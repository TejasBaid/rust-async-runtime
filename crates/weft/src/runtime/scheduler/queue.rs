//! Per-worker run queue: a bounded, lock-free, single-producer /
//! multi-consumer ring buffer with "steal half" semantics.
//!
//! # Why not `crossbeam-deque`?
//!
//! crossbeam's Chase-Lev deque is unbounded and resizes its buffer, which
//! requires epoch-based reclamation to free old buffers safely. A scheduler
//! does not need an unbounded local queue: when the ring is full, half of it
//! is moved to the global injector in one batch. A fixed-size array removes
//! the need for memory reclamation entirely, keeps the queue in a single
//! cache-friendly allocation, and makes the algorithm small enough to model
//! check exhaustively with loom (see `loom_tests`).
//!
//! # Algorithm
//!
//! The design follows the one used by Go's runtime and Tokio:
//!
//! * `tail` is written only by the owner (push).
//! * `head` packs two 32-bit cursors: `real` (the next slot to pop/steal) and
//!   `steal` (the first slot still being copied by an in-progress stealer).
//!   Normally `steal == real`.
//! * The owner pops from the head with a CAS (FIFO order; the LIFO behaviour
//!   lives in the worker's separate LIFO slot).
//! * A stealer first CASes `real` forward by `n` (claiming `[steal, real+n)`
//!   while leaving `steal` behind so the owner cannot overwrite those slots),
//!   copies the tasks into its own queue, then CASes `steal` up to `real`.
//! * Only one steal can be in flight per queue; concurrent stealers back off.
//!
//! Positions are `u32` and wrap; slot indices are `pos & MASK`.

use std::mem::MaybeUninit;
use std::ptr;

use crate::loom::cell::UnsafeCell;
use crate::loom::sync::Arc;
use crate::loom::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Capacity of each local queue (a power of two).
#[cfg(not(loom))]
pub(crate) const LOCAL_QUEUE_CAPACITY: usize = 256;
#[cfg(loom)]
pub(crate) const LOCAL_QUEUE_CAPACITY: usize = 4;

const MASK: usize = LOCAL_QUEUE_CAPACITY - 1;

struct Inner<T> {
    /// `(steal << 32) | real`.
    head: AtomicU64,
    tail: AtomicU32,
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

// SAFETY: tasks are moved between threads through the buffer; access to each
// slot is serialised by the head/tail protocol described in the module docs.
unsafe impl<T: Send> Send for Inner<T> {}
// SAFETY: as above.
unsafe impl<T: Send> Sync for Inner<T> {}

/// The owner's handle: push and pop.
pub(crate) struct Local<T: 'static> {
    inner: Arc<Inner<T>>,
}

/// A handle other workers use to steal from this queue.
pub(crate) struct Steal<T: 'static>(Arc<Inner<T>>);

/// Destination for tasks that do not fit in the local queue.
pub(crate) trait Overflow<T> {
    fn push(&self, task: T);
    fn push_batch(&self, tasks: impl Iterator<Item = T>);
}

/// Creates a new queue.
pub(crate) fn local<T: 'static>() -> (Steal<T>, Local<T>) {
    let buffer = (0..LOCAL_QUEUE_CAPACITY)
        .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
        .collect();
    let inner = Arc::new(Inner {
        head: AtomicU64::new(0),
        tail: AtomicU32::new(0),
        buffer,
    });
    (Steal(inner.clone()), Local { inner })
}

fn unpack(n: u64) -> (u32, u32) {
    ((n >> 32) as u32, n as u32)
}

fn pack(steal: u32, real: u32) -> u64 {
    ((steal as u64) << 32) | real as u64
}

impl<T> Inner<T> {
    /// # Safety
    /// The caller must have exclusive access to slot `pos` and it must
    /// contain an initialised value.
    unsafe fn read(&self, pos: u32) -> T {
        self.buffer[pos as usize & MASK].with(|ptr| {
            // SAFETY: forwarded caller contract.
            unsafe { ptr::read(ptr).assume_init() }
        })
    }

    /// # Safety
    /// The caller must have exclusive access to slot `pos`, which must be
    /// logically empty.
    unsafe fn write(&self, pos: u32, task: T) {
        self.buffer[pos as usize & MASK].with_mut(|ptr| {
            // SAFETY: forwarded caller contract.
            unsafe { ptr::write(ptr, MaybeUninit::new(task)) }
        })
    }

    fn len(&self) -> u32 {
        let (_, real) = unpack(self.head.load(Ordering::Acquire));
        let tail = self.tail.load(Ordering::Acquire);
        tail.wrapping_sub(real)
    }
}

impl<T> Local<T> {
    /// Number of tasks currently in the queue.
    #[cfg(all(test, not(loom)))]
    pub(crate) fn len(&self) -> usize {
        self.inner.len() as usize
    }

    /// Free slots, accounting for slots still held by an in-flight steal.
    pub(crate) fn remaining_slots(&self) -> usize {
        let (steal, _) = unpack(self.inner.head.load(Ordering::Acquire));
        let tail = self.inner.tail.load(Ordering::Relaxed);
        LOCAL_QUEUE_CAPACITY - tail.wrapping_sub(steal) as usize
    }

    /// Pushes a batch that is known to fit (`len <= remaining_slots()`).
    pub(crate) fn push_back_batch(&mut self, tasks: impl ExactSizeIterator<Item = T>) {
        let len = tasks.len();
        assert!(len <= self.remaining_slots(), "local queue batch overflow");
        let mut tail = self.inner.tail.load(Ordering::Relaxed);
        for task in tasks {
            // SAFETY: slots in `[tail, steal + CAPACITY)` are free and only
            // the owner writes them; they are published by the Release store
            // of `tail` below.
            unsafe { self.inner.write(tail, task) };
            tail = tail.wrapping_add(1);
        }
        self.inner.tail.store(tail, Ordering::Release);
    }

    /// Pushes a task, moving half of the queue to `overflow` if it is full.
    pub(crate) fn push_back_or_overflow(&mut self, mut task: T, overflow: &impl Overflow<T>) {
        let tail = loop {
            let head = self.inner.head.load(Ordering::Acquire);
            let (steal, real) = unpack(head);
            // Only the owner writes `tail`.
            let tail = self.inner.tail.load(Ordering::Relaxed);
            if tail.wrapping_sub(steal) < LOCAL_QUEUE_CAPACITY as u32 {
                break tail;
            } else if steal != real {
                // A stealer is about to free up half the queue; don't wait for
                // it, just send this one task to the global queue.
                overflow.push(task);
                return;
            } else {
                match self.push_overflow(task, real, tail, overflow) {
                    Ok(()) => return,
                    // Lost a race with a stealer; there may be room now.
                    Err(t) => task = t,
                }
            }
        };
        // SAFETY: `tail - steal < CAPACITY`, so slot `tail` is free and not
        // visible to any stealer until the Release store below.
        unsafe { self.inner.write(tail, task) };
        self.inner
            .tail
            .store(tail.wrapping_add(1), Ordering::Release);
    }

    /// Moves half of a full queue, plus `task`, to the overflow queue.
    fn push_overflow(
        &mut self,
        task: T,
        head: u32,
        tail: u32,
        overflow: &impl Overflow<T>,
    ) -> Result<(), T> {
        const NUM_TASKS_TAKEN: u32 = (LOCAL_QUEUE_CAPACITY / 2) as u32;
        assert_eq!(
            tail.wrapping_sub(head) as usize,
            LOCAL_QUEUE_CAPACITY,
            "queue is not full"
        );
        let prev = pack(head, head);
        let next = head.wrapping_add(NUM_TASKS_TAKEN);
        // Claim the oldest half in one CAS. Failure means a stealer got there
        // first.
        if self
            .inner
            .head
            .compare_exchange(prev, pack(next, next), Ordering::Release, Ordering::Relaxed)
            .is_err()
        {
            return Err(task);
        }
        let inner = &*self.inner;
        let taken = (0..NUM_TASKS_TAKEN).map(|i| {
            // SAFETY: the successful CAS above transferred ownership of the
            // slots `[head, head + NUM_TASKS_TAKEN)` to us; the owner never
            // writes them again until `tail` wraps around past them.
            unsafe { inner.read(head.wrapping_add(i)) }
        });
        overflow.push_batch(taken.chain(std::iter::once(task)));
        Ok(())
    }

    /// Pops the oldest task.
    pub(crate) fn pop(&mut self) -> Option<T> {
        let mut head = self.inner.head.load(Ordering::Acquire);
        let pos = loop {
            let (steal, real) = unpack(head);
            let tail = self.inner.tail.load(Ordering::Relaxed);
            if real == tail {
                return None;
            }
            let next_real = real.wrapping_add(1);
            // If no steal is in flight, keep `steal` in lock-step with `real`.
            let next = if steal == real {
                pack(next_real, next_real)
            } else {
                assert_ne!(steal, next_real);
                pack(steal, next_real)
            };
            match self
                .inner
                .head
                .compare_exchange(head, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break real,
                Err(actual) => head = actual,
            }
        };
        // SAFETY: the CAS advanced `real` past `pos`, giving us exclusive
        // ownership of that slot; it was initialised by a push that
        // happened-before (we read `tail` after it was stored, and we are the
        // owner thread that performed the push).
        Some(unsafe { self.inner.read(pos) })
    }
}

impl<T> Drop for Local<T> {
    fn drop(&mut self) {
        // Drop any tasks still queued so they are not leaked.
        while self.pop().is_some() {}
    }
}

impl<T> Steal<T> {
    pub(crate) fn is_empty(&self) -> bool {
        self.0.len() == 0
    }

    /// Steals half of this queue into `dst`, returning one of the stolen tasks
    /// to run immediately.
    pub(crate) fn steal_into(&self, dst: &mut Local<T>) -> Option<T> {
        let dst_tail = dst.inner.tail.load(Ordering::Relaxed);
        let (dst_steal, _) = unpack(dst.inner.head.load(Ordering::Acquire));
        // Only steal into a queue that is at most half full, so the stolen
        // half always fits.
        if dst_tail.wrapping_sub(dst_steal) > (LOCAL_QUEUE_CAPACITY / 2) as u32 {
            return None;
        }
        let mut n = self.steal_into2(dst, dst_tail);
        if n == 0 {
            return None;
        }
        // Keep the last stolen task for the caller.
        n -= 1;
        let ret_pos = dst_tail.wrapping_add(n);
        // SAFETY: `steal_into2` wrote slots `[dst_tail, dst_tail + n]` of our
        // own queue; none of them has been published yet.
        let ret = unsafe { dst.inner.read(ret_pos) };
        if n > 0 {
            dst.inner
                .tail
                .store(dst_tail.wrapping_add(n), Ordering::Release);
        }
        Some(ret)
    }

    /// Copies up to half of the source queue into `dst` starting at
    /// `dst_tail`, without publishing them. Returns the number copied.
    fn steal_into2(&self, dst: &mut Local<T>, dst_tail: u32) -> u32 {
        let mut prev_packed = self.0.head.load(Ordering::Acquire);
        let mut next_packed;

        // Phase 1: claim `n` tasks by advancing `real` (but not `steal`).
        let n = loop {
            let (src_steal, src_real) = unpack(prev_packed);
            let src_tail = self.0.tail.load(Ordering::Acquire);
            if src_steal != src_real {
                // Another worker is stealing from this queue right now.
                return 0;
            }
            let available = src_tail.wrapping_sub(src_real);
            let n = available - available / 2;
            if n == 0 {
                return 0;
            }
            let steal_to = src_real.wrapping_add(n);
            assert_ne!(src_steal, steal_to);
            next_packed = pack(src_steal, steal_to);
            match self.0.head.compare_exchange(
                prev_packed,
                next_packed,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break n,
                Err(actual) => prev_packed = actual,
            }
        };
        assert!(n as usize <= LOCAL_QUEUE_CAPACITY / 2, "steal too large");

        // Phase 2: copy. The owner cannot overwrite these slots because its
        // capacity check is relative to `steal`, which we have not moved.
        let (first, _) = unpack(next_packed);
        for i in 0..n {
            let src_pos = first.wrapping_add(i);
            let dst_pos = dst_tail.wrapping_add(i);
            // SAFETY: phase 1 gave us exclusive ownership of the source slot,
            // whose initialisation happened-before our Acquire load of the
            // source `tail`. The destination slot is beyond `dst`'s published
            // tail and within capacity (checked in `steal_into`), and `dst` is
            // exclusively borrowed.
            unsafe {
                let task = self.0.read(src_pos);
                dst.inner.write(dst_pos, task);
            }
        }

        // Phase 3: release the claimed slots by moving `steal` up to `real`.
        // The owner may have popped (advanced `real`) concurrently.
        let mut prev_packed = next_packed;
        loop {
            let (_, real) = unpack(prev_packed);
            next_packed = pack(real, real);
            match self.0.head.compare_exchange(
                prev_packed,
                next_packed,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return n,
                Err(actual) => {
                    let (actual_steal, actual_real) = unpack(actual);
                    assert_ne!(actual_steal, actual_real);
                    prev_packed = actual;
                }
            }
        }
    }
}

impl<T> Clone for Steal<T> {
    fn clone(&self) -> Self {
        Steal(self.0.clone())
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::sync::atomic::AtomicUsize;

    struct VecOverflow(RefCell<Vec<usize>>);

    impl Overflow<usize> for VecOverflow {
        fn push(&self, task: usize) {
            self.0.borrow_mut().push(task);
        }
        fn push_batch(&self, tasks: impl Iterator<Item = usize>) {
            self.0.borrow_mut().extend(tasks);
        }
    }

    #[test]
    fn fifo_push_pop() {
        let (_, mut local) = local::<usize>();
        let of = VecOverflow(RefCell::new(vec![]));
        for i in 0..10 {
            local.push_back_or_overflow(i, &of);
        }
        assert_eq!(local.len(), 10);
        for i in 0..10 {
            assert_eq!(local.pop(), Some(i));
        }
        assert_eq!(local.pop(), None);
    }

    #[test]
    fn overflow_moves_half() {
        let (_, mut local) = local::<usize>();
        let of = VecOverflow(RefCell::new(vec![]));
        for i in 0..LOCAL_QUEUE_CAPACITY + 1 {
            local.push_back_or_overflow(i, &of);
        }
        let overflowed = of.0.borrow().clone();
        assert_eq!(overflowed.len(), LOCAL_QUEUE_CAPACITY / 2 + 1);
        assert_eq!(overflowed[0], 0, "oldest tasks overflow first");
        assert_eq!(local.len(), LOCAL_QUEUE_CAPACITY / 2);
    }

    #[test]
    fn steal_half() {
        let (steal, mut a) = local::<usize>();
        let (_, mut b) = local::<usize>();
        let of = VecOverflow(RefCell::new(vec![]));
        for i in 0..10 {
            a.push_back_or_overflow(i, &of);
        }
        let first = steal.steal_into(&mut b).unwrap();
        assert_eq!(first, 4, "last stolen task is returned");
        assert_eq!(b.len(), 4);
        assert_eq!(a.len(), 5);
        let mut all: Vec<usize> = std::iter::from_fn(|| a.pop())
            .chain(std::iter::from_fn(|| b.pop()))
            .collect();
        all.push(first);
        all.sort();
        assert_eq!(all, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn wraparound() {
        let (_, mut local) = local::<usize>();
        let of = VecOverflow(RefCell::new(vec![]));
        for round in 0..5 * LOCAL_QUEUE_CAPACITY {
            local.push_back_or_overflow(round, &of);
            assert_eq!(local.pop(), Some(round));
        }
        assert!(of.0.borrow().is_empty());
    }

    #[test]
    fn drop_releases_queued_tasks() {
        let counter = std::sync::Arc::new(AtomicUsize::new(0));
        struct D(std::sync::Arc<AtomicUsize>);
        impl Drop for D {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct NoOverflow;
        impl Overflow<D> for NoOverflow {
            fn push(&self, _: D) {
                unreachable!()
            }
            fn push_batch(&self, _: impl Iterator<Item = D>) {
                unreachable!()
            }
        }
        let (_, mut local) = local::<D>();
        for _ in 0..5 {
            local.push_back_or_overflow(D(counter.clone()), &NoOverflow);
        }
        drop(local);
        assert_eq!(counter.load(Ordering::SeqCst), 5);
    }

    /// Owner pushes and pops while several threads steal; every task must be
    /// consumed exactly once.
    #[test]
    fn concurrent_steal_stress() {
        use std::sync::Mutex;
        use std::sync::atomic::AtomicBool;

        const N: usize = 200_000;
        const STEALERS: usize = 3;

        struct MutexOverflow(Mutex<Vec<usize>>);
        impl Overflow<usize> for MutexOverflow {
            fn push(&self, task: usize) {
                self.0.lock().unwrap().push(task);
            }
            fn push_batch(&self, tasks: impl Iterator<Item = usize>) {
                self.0.lock().unwrap().extend(tasks);
            }
        }

        let (steal, mut owner) = local::<usize>();
        let done = std::sync::Arc::new(AtomicBool::new(false));
        let seen: std::sync::Arc<Vec<AtomicUsize>> =
            std::sync::Arc::new((0..N).map(|_| AtomicUsize::new(0)).collect());

        let stealers: Vec<_> = (0..STEALERS)
            .map(|_| {
                let steal = steal.clone();
                let done = done.clone();
                let seen = seen.clone();
                std::thread::spawn(move || {
                    let (_, mut mine) = local::<usize>();
                    loop {
                        let finished = done.load(Ordering::Acquire);
                        if let Some(t) = steal.steal_into(&mut mine) {
                            seen[t].fetch_add(1, Ordering::Relaxed);
                        }
                        while let Some(t) = mine.pop() {
                            seen[t].fetch_add(1, Ordering::Relaxed);
                        }
                        if finished && steal.is_empty() {
                            break;
                        }
                    }
                })
            })
            .collect();

        let overflow = MutexOverflow(Mutex::new(Vec::new()));
        for i in 0..N {
            owner.push_back_or_overflow(i, &overflow);
            if i % 3 == 0 {
                if let Some(t) = owner.pop() {
                    seen[t].fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        while let Some(t) = owner.pop() {
            seen[t].fetch_add(1, Ordering::Relaxed);
        }
        done.store(true, Ordering::Release);
        for s in stealers {
            s.join().unwrap();
        }
        for t in overflow.0.into_inner().unwrap() {
            seen[t].fetch_add(1, Ordering::Relaxed);
        }
        for (i, count) in seen.iter().enumerate() {
            assert_eq!(count.load(Ordering::Relaxed), 1, "task {i}");
        }
    }
}
