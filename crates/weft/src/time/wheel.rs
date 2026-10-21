//! A hierarchical timing wheel.
//!
//! Time is measured in integer *ticks* (1 tick = 1 ms in the runtime). The
//! wheel has [`NUM_LEVELS`] levels of [`SLOTS`] slots each; a slot at level
//! `L` covers `64^L` ticks, so level 0 has 1 ms resolution over a 64 ms
//! window, level 1 covers 4.096 s with 64 ms slots, and so on up to level 5,
//! which spans ~2.2 years (`2^36` ms).
//!
//! An entry is stored at the lowest level whose slot granularity separates its
//! deadline from the wheel's current time: the level is derived from the
//! highest bit in which `elapsed` and `deadline` differ. When time reaches the
//! start of a higher-level slot, that slot's entries are *cascaded*: each is
//! either fired (if due) or re-inserted at a lower, finer level.
//!
//! Complexity:
//! * insert / remove: O(1) (bit tricks + `Vec::swap_remove`)
//! * finding the next deadline: O(levels) using a 64-bit occupancy bitmap per
//!   level and `trailing_zeros`
//! * each entry is cascaded at most `NUM_LEVELS - 1` times over its lifetime
//!
//! Compared with a binary heap this gives O(1) cancellation, which matters
//! because most timers in a server (I/O timeouts) are cancelled rather than
//! fired.

use crate::util::slab::{Key, Slab};

/// Bits of the tick consumed by each level.
const LEVEL_BITS: u32 = 6;
/// Slots per level.
pub(crate) const SLOTS: usize = 1 << LEVEL_BITS;
const SLOT_MASK: u64 = (SLOTS as u64) - 1;
/// Number of levels in the hierarchy.
pub(crate) const NUM_LEVELS: usize = 6;
/// Largest representable distance between `elapsed` and a deadline.
pub(crate) const MAX_DURATION: u64 = 1 << (LEVEL_BITS as u64 * NUM_LEVELS as u64);

/// Handle to a timer stored in the wheel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TimerKey(Key);

#[derive(Debug)]
struct Entry<T> {
    deadline: u64,
    value: T,
    level: u8,
    slot: u8,
    /// Position of this entry's index inside its slot vector.
    pos: u32,
}

#[derive(Debug)]
struct Level {
    /// Bit `i` is set iff `slots[i]` is non-empty.
    occupied: u64,
    slots: [Vec<u32>; SLOTS],
}

/// The next slot that needs processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Expiration {
    level: usize,
    slot: usize,
    /// Tick at which the slot starts.
    pub(crate) deadline: u64,
}

/// Number of ticks covered by one slot at `level`.
fn slot_range(level: usize) -> u64 {
    1 << (LEVEL_BITS as usize * level)
}

/// Number of ticks covered by a whole `level`.
fn level_range(level: usize) -> u64 {
    slot_range(level + 1)
}

/// The level at which a timer for `deadline` lives when the wheel is at
/// `elapsed`.
fn level_for(elapsed: u64, deadline: u64) -> usize {
    let mut masked = (elapsed ^ deadline) | SLOT_MASK;
    if masked >= MAX_DURATION {
        masked = MAX_DURATION - 1;
    }
    let significant = 63 - masked.leading_zeros() as usize;
    significant / LEVEL_BITS as usize
}

impl Level {
    fn new() -> Level {
        Level {
            occupied: 0,
            slots: std::array::from_fn(|_| Vec::new()),
        }
    }

    fn next_expiration(&self, level: usize, now: u64) -> Option<Expiration> {
        if self.occupied == 0 {
            return None;
        }
        // Rotate the bitmap so that bit 0 corresponds to the current slot and
        // find the first occupied slot at or after it.
        let now_slot = (now / slot_range(level)) & SLOT_MASK;
        let rotated = self.occupied.rotate_right(now_slot as u32);
        let slot = ((rotated.trailing_zeros() as u64 + now_slot) & SLOT_MASK) as usize;

        let range = level_range(level);
        let level_start = now & !(range - 1);
        let mut deadline = level_start + slot as u64 * slot_range(level);
        if deadline <= now && level > 0 {
            // Only the top level can hold a slot "behind" the current one: a
            // deadline beyond the end of the hierarchy wraps around it.
            debug_assert_eq!(level, NUM_LEVELS - 1);
            deadline += range;
        }
        Some(Expiration {
            level,
            slot,
            deadline,
        })
    }
}

/// A hierarchical timing wheel storing values of type `T` (wakers, in the
/// runtime).
#[derive(Debug)]
pub(crate) struct Wheel<T> {
    /// All timers with a deadline `<= elapsed` have been fired.
    elapsed: u64,
    levels: Box<[Level]>,
    entries: Slab<Entry<T>>,
}

impl<T> Default for Wheel<T> {
    fn default() -> Self {
        Wheel::new()
    }
}

impl<T> Wheel<T> {
    pub(crate) fn new() -> Wheel<T> {
        Wheel {
            elapsed: 0,
            levels: (0..NUM_LEVELS).map(|_| Level::new()).collect(),
            entries: Slab::new(),
        }
    }

    /// Number of pending timers.
    #[cfg(all(test, not(loom)))]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(all(test, not(loom)))]
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.len() == 0
    }

    /// Inserts a timer. Returns `Err(value)` if `deadline` has already been
    /// reached (`deadline <= elapsed`), in which case the caller should treat
    /// the timer as fired. Deadlines further than [`MAX_DURATION`] in the
    /// future are clamped.
    pub(crate) fn insert(&mut self, deadline: u64, value: T) -> Result<TimerKey, T> {
        if deadline <= self.elapsed {
            return Err(value);
        }
        let deadline = deadline.min(self.elapsed + MAX_DURATION - 1);
        let key = self.entries.insert(Entry {
            deadline,
            value,
            level: 0,
            slot: 0,
            pos: 0,
        });
        self.link(key.index() as u32);
        Ok(TimerKey(key))
    }

    /// Cancels a timer, returning its value if it had not fired yet.
    pub(crate) fn remove(&mut self, key: TimerKey) -> Option<T> {
        let entry = self.entries.get(key.0)?;
        let (level, slot, pos) = (entry.level as usize, entry.slot as usize, entry.pos);
        self.unlink(level, slot, pos);
        self.entries.remove(key.0).map(|e| e.value)
    }

    /// Returns a mutable reference to a pending timer's value.
    pub(crate) fn get_mut(&mut self, key: TimerKey) -> Option<&mut T> {
        self.entries.get_mut(key.0).map(|e| &mut e.value)
    }

    /// The tick at which the wheel next needs to be polled, if any timers are
    /// pending. This may be earlier than the earliest deadline (when a higher
    /// level slot needs cascading) but never later.
    pub(crate) fn next_expiration(&self) -> Option<Expiration> {
        self.levels
            .iter()
            .enumerate()
            .find_map(|(level, l)| l.next_expiration(level, self.elapsed))
    }

    /// Advances the wheel to `now`, calling `fire` for every timer whose
    /// deadline is `<= now`, in deadline order (ties in arbitrary order).
    pub(crate) fn poll(&mut self, now: u64, mut fire: impl FnMut(T)) {
        while let Some(exp) = self.next_expiration() {
            if exp.deadline > now {
                break;
            }
            self.process_expiration(exp, &mut fire);
        }
        self.elapsed = self.elapsed.max(now);
    }

    fn process_expiration(&mut self, exp: Expiration, fire: &mut impl FnMut(T)) {
        let level = &mut self.levels[exp.level];
        let indices = std::mem::take(&mut level.slots[exp.slot]);
        level.occupied &= !(1 << exp.slot);
        self.elapsed = exp.deadline;
        for &index in &indices {
            let entry = self
                .entries
                .get_by_index_mut(index as usize)
                .expect("wheel slot references a vacant entry");
            if entry.deadline <= self.elapsed {
                let key = self.entries.key_at(index as usize).expect("occupied");
                let entry = self.entries.remove(key).expect("occupied");
                fire(entry.value);
            } else {
                // Cascade into a finer level.
                self.link(index);
            }
        }
        // Reuse the allocation if the slot is still empty.
        let slot = &mut self.levels[exp.level].slots[exp.slot];
        if slot.is_empty() {
            let mut indices = indices;
            indices.clear();
            *slot = indices;
        }
    }

    /// Places entry `index` into the slot matching its deadline.
    fn link(&mut self, index: u32) {
        let elapsed = self.elapsed;
        let entry = self
            .entries
            .get_by_index_mut(index as usize)
            .expect("linking a vacant entry");
        let level = level_for(elapsed, entry.deadline);
        let slot = ((entry.deadline >> (LEVEL_BITS as usize * level)) & SLOT_MASK) as usize;
        let vec = &mut self.levels[level].slots[slot];
        entry.level = level as u8;
        entry.slot = slot as u8;
        entry.pos = vec.len() as u32;
        vec.push(index);
        self.levels[level].occupied |= 1 << slot;
    }

    fn unlink(&mut self, level: usize, slot: usize, pos: u32) {
        let vec = &mut self.levels[level].slots[slot];
        vec.swap_remove(pos as usize);
        if let Some(&moved) = vec.get(pos as usize) {
            self.entries
                .get_by_index_mut(moved as usize)
                .expect("moved entry is occupied")
                .pos = pos;
        }
        if vec.is_empty() {
            self.levels[level].occupied &= !(1 << slot);
        }
    }

    /// Removes every pending timer, returning their values.
    pub(crate) fn drain(&mut self) -> Vec<T> {
        for level in self.levels.iter_mut() {
            level.occupied = 0;
            level.slots.iter_mut().for_each(Vec::clear);
        }
        self.entries.drain().into_iter().map(|e| e.value).collect()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::util::rand::FastRand;

    fn fired_at(wheel: &mut Wheel<u64>, now: u64) -> Vec<u64> {
        let mut out = Vec::new();
        wheel.poll(now, |v| out.push(v));
        out
    }

    #[test]
    fn level_selection() {
        assert_eq!(level_for(0, 1), 0);
        assert_eq!(level_for(0, 63), 0);
        assert_eq!(level_for(0, 64), 1);
        assert_eq!(level_for(0, 4095), 1);
        assert_eq!(level_for(0, 4096), 2);
        assert_eq!(level_for(0, MAX_DURATION - 1), NUM_LEVELS - 1);
        // Same 64-tick block => level 0 even for large absolute values.
        assert_eq!(level_for(1_000_000, 1_000_001), 0);
    }

    #[test]
    fn fires_at_exact_tick() {
        let mut wheel = Wheel::new();
        wheel.insert(5, 5).unwrap();
        assert!(fired_at(&mut wheel, 4).is_empty());
        assert_eq!(fired_at(&mut wheel, 5), vec![5]);
        assert!(wheel.is_empty());
    }

    #[test]
    fn past_deadline_is_rejected() {
        let mut wheel = Wheel::new();
        wheel.poll(100, |_: u64| {});
        assert_eq!(wheel.insert(100, 1), Err(1));
        assert_eq!(wheel.insert(50, 2), Err(2));
        assert!(wheel.insert(101, 3).is_ok());
    }

    #[test]
    fn cascades_through_levels() {
        let mut wheel = Wheel::new();
        let deadlines = [70, 4_100, 300_000, 20_000_000, 3_000_000_000];
        for &d in &deadlines {
            wheel.insert(d, d).unwrap();
        }
        for &d in &deadlines {
            assert!(fired_at(&mut wheel, d - 1).is_empty(), "{d} fired early");
            assert_eq!(fired_at(&mut wheel, d), vec![d]);
        }
    }

    #[test]
    fn remove_cancels() {
        let mut wheel = Wheel::new();
        let a = wheel.insert(10, 10).unwrap();
        let b = wheel.insert(10, 11).unwrap();
        let c = wheel.insert(5_000, 12).unwrap();
        assert_eq!(wheel.remove(a), Some(10));
        assert_eq!(wheel.remove(a), None, "double remove is a no-op");
        assert_eq!(wheel.remove(c), Some(12));
        assert_eq!(fired_at(&mut wheel, 10_000), vec![11]);
        assert_eq!(wheel.remove(b), None, "fired timers cannot be removed");
    }

    #[test]
    fn next_expiration_never_later_than_earliest_deadline() {
        let mut wheel = Wheel::new();
        wheel.insert(1_000, 0).unwrap();
        wheel.insert(200, 0).unwrap();
        let exp = wheel.next_expiration().unwrap();
        assert!(exp.deadline <= 200);
    }

    #[test]
    fn huge_deadlines_are_clamped() {
        let mut wheel = Wheel::new();
        wheel.insert(u64::MAX, 1).unwrap();
        assert!(fired_at(&mut wheel, MAX_DURATION - 2).is_empty());
        assert_eq!(fired_at(&mut wheel, MAX_DURATION), vec![1]);
    }

    /// Randomised model check against a sorted reference: every timer fires
    /// exactly once, never before its deadline, and at the first poll whose
    /// `now` reaches it.
    #[test]
    fn randomized_against_reference() {
        let rng = FastRand::new(0xdead_beef);
        let mut wheel = Wheel::new();
        let mut reference: Vec<(u64, u64, TimerKey)> = Vec::new();
        let mut now = 0u64;
        let mut next_id = 0u64;
        for _ in 0..20_000 {
            match rng.below(10) {
                0..=4 => {
                    let span = [10, 1_000, 100_000, 10_000_000][rng.below(4)];
                    let deadline = now + 1 + rng.next_u64() % span;
                    let key = wheel.insert(deadline, next_id).unwrap();
                    reference.push((deadline, next_id, key));
                    next_id += 1;
                }
                5 if !reference.is_empty() => {
                    let i = rng.below(reference.len());
                    let (_, id, key) = reference.swap_remove(i);
                    assert_eq!(wheel.remove(key), Some(id));
                }
                _ => {
                    let step = [1, 7, 300, 50_000][rng.below(4)];
                    now += rng.next_u64() % step;
                    let mut fired = fired_at(&mut wheel, now);
                    let mut expected: Vec<u64> = reference
                        .iter()
                        .filter(|(d, _, _)| *d <= now)
                        .map(|&(_, id, _)| id)
                        .collect();
                    reference.retain(|(d, _, _)| *d > now);
                    fired.sort_unstable();
                    expected.sort_unstable();
                    assert_eq!(fired, expected, "mismatch at now={now}");
                }
            }
            assert_eq!(wheel.len(), reference.len());
        }
    }

    #[test]
    fn fires_in_deadline_order() {
        let mut wheel = Wheel::new();
        let rng = FastRand::new(7);
        for _ in 0..1_000 {
            let d = 1 + rng.next_u64() % 1_000_000;
            wheel.insert(d, d).unwrap();
        }
        let fired = fired_at(&mut wheel, 1_000_000);
        assert_eq!(fired.len(), 1_000);
        assert!(fired.windows(2).all(|w| w[0] <= w[1]));
    }
}
