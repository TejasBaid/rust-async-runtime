//! A generational slab allocator.
//!
//! Keys pack a slot index together with a generation counter that is bumped
//! every time a slot is freed. A stale key (one whose entry was removed and
//! whose slot was reused) therefore never aliases the new occupant; this is
//! what makes it safe to hand keys to the kernel as event tokens and to keep
//! them inside timer handles.

/// A key into a [`Slab`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Key {
    index: u32,
    generation: u32,
}

impl Key {
    /// Packs the key into a `u64` (generation in the high half).
    pub(crate) fn to_u64(self) -> u64 {
        ((self.generation as u64) << 32) | self.index as u64
    }

    /// Inverse of [`Key::to_u64`].
    pub(crate) fn from_u64(v: u64) -> Key {
        Key {
            index: v as u32,
            generation: (v >> 32) as u32,
        }
    }

    pub(crate) fn index(self) -> usize {
        self.index as usize
    }
}

#[derive(Debug)]
enum Entry<T> {
    Occupied { generation: u32, value: T },
    Vacant { generation: u32, next_free: usize },
}

/// A vector-backed slab with O(1) insert/remove and a free list.
#[derive(Debug)]
pub(crate) struct Slab<T> {
    entries: Vec<Entry<T>>,
    next_free: usize,
    len: usize,
}

impl<T> Default for Slab<T> {
    fn default() -> Self {
        Slab::new()
    }
}

impl<T> Slab<T> {
    pub(crate) const fn new() -> Slab<T> {
        Slab {
            entries: Vec::new(),
            next_free: 0,
            len: 0,
        }
    }

    #[cfg(all(test, not(loom)))]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[cfg(all(test, not(loom)))]
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn insert(&mut self, value: T) -> Key {
        self.len += 1;
        let index = self.next_free;
        if index == self.entries.len() {
            assert!(index < u32::MAX as usize, "slab overflow");
            self.entries.push(Entry::Occupied {
                generation: 0,
                value,
            });
            self.next_free = index + 1;
            return Key {
                index: index as u32,
                generation: 0,
            };
        }
        let entry = &mut self.entries[index];
        let (generation, next_free) = match *entry {
            Entry::Vacant {
                generation,
                next_free,
            } => (generation, next_free),
            Entry::Occupied { .. } => unreachable!("free list points at an occupied slot"),
        };
        *entry = Entry::Occupied { generation, value };
        self.next_free = next_free;
        Key {
            index: index as u32,
            generation,
        }
    }

    pub(crate) fn get(&self, key: Key) -> Option<&T> {
        match self.entries.get(key.index())? {
            Entry::Occupied { generation, value } if *generation == key.generation => Some(value),
            _ => None,
        }
    }

    pub(crate) fn get_mut(&mut self, key: Key) -> Option<&mut T> {
        match self.entries.get_mut(key.index())? {
            Entry::Occupied { generation, value } if *generation == key.generation => Some(value),
            _ => None,
        }
    }

    /// Access by raw index, ignoring generations (used for internal links).
    pub(crate) fn get_by_index_mut(&mut self, index: usize) -> Option<&mut T> {
        match self.entries.get_mut(index)? {
            Entry::Occupied { value, .. } => Some(value),
            Entry::Vacant { .. } => None,
        }
    }

    /// Returns the key currently stored at `index`, if occupied.
    pub(crate) fn key_at(&self, index: usize) -> Option<Key> {
        match self.entries.get(index)? {
            Entry::Occupied { generation, .. } => Some(Key {
                index: index as u32,
                generation: *generation,
            }),
            Entry::Vacant { .. } => None,
        }
    }

    pub(crate) fn remove(&mut self, key: Key) -> Option<T> {
        let entry = self.entries.get_mut(key.index())?;
        match entry {
            Entry::Occupied { generation, .. } if *generation == key.generation => {}
            _ => return None,
        }
        let vacant = Entry::Vacant {
            generation: key.generation.wrapping_add(1),
            next_free: self.next_free,
        };
        let Entry::Occupied { value, .. } = std::mem::replace(entry, vacant) else {
            unreachable!()
        };
        self.next_free = key.index();
        self.len -= 1;
        Some(value)
    }

    /// Removes and returns every value, leaving the slab empty.
    pub(crate) fn drain(&mut self) -> Vec<T> {
        let entries = std::mem::take(&mut self.entries);
        self.next_free = 0;
        self.len = 0;
        entries
            .into_iter()
            .filter_map(|e| match e {
                Entry::Occupied { value, .. } => Some(value),
                Entry::Vacant { .. } => None,
            })
            .collect()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn insert_get_remove() {
        let mut slab = Slab::new();
        let a = slab.insert("a");
        let b = slab.insert("b");
        assert_eq!(slab.get(a), Some(&"a"));
        assert_eq!(slab.get(b), Some(&"b"));
        assert_eq!(slab.remove(a), Some("a"));
        assert_eq!(slab.get(a), None);
        assert_eq!(slab.len(), 1);
    }

    #[test]
    fn stale_keys_do_not_alias_reused_slots() {
        let mut slab = Slab::new();
        let a = slab.insert(1);
        slab.remove(a);
        let b = slab.insert(2);
        assert_eq!(a.index(), b.index(), "slot is reused");
        assert_ne!(a, b);
        assert_eq!(slab.get(a), None);
        assert_eq!(slab.remove(a), None);
        assert_eq!(slab.get(b), Some(&2));
    }

    #[test]
    fn key_round_trips_through_u64() {
        let mut slab = Slab::new();
        for i in 0..10 {
            let k = slab.insert(i);
            if i % 2 == 0 {
                slab.remove(k);
            }
        }
        let k = slab.insert(99);
        assert_eq!(Key::from_u64(k.to_u64()), k);
        assert_eq!(slab.get(Key::from_u64(k.to_u64())), Some(&99));
    }

    #[test]
    fn drain_empties() {
        let mut slab = Slab::new();
        for i in 0..5 {
            slab.insert(i);
        }
        let mut all = slab.drain();
        all.sort();
        assert_eq!(all, vec![0, 1, 2, 3, 4]);
        assert!(slab.is_empty());
        let k = slab.insert(7);
        assert_eq!(slab.get(k), Some(&7));
    }
}
