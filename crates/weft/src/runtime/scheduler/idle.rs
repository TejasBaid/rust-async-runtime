//! Bookkeeping for idle workers.
//!
//! Waking a parked thread costs a syscall, so the scheduler tries hard not to
//! wake workers needlessly:
//!
//! * At most one worker is woken per notification, and only if no worker is
//!   currently *searching* (looking for work to steal). A searching worker
//!   will find the new task itself.
//! * At most half of the workers may search at once.
//! * When the last searching worker finds work, it wakes another one to keep
//!   the search going (work tends to arrive in bursts).
//! * When the last searching worker gives up, it re-checks every queue before
//!   sleeping. Together with the `SeqCst` fences, this closes the race where a
//!   producer saw "someone is searching" and skipped the wake-up just as that
//!   searcher was giving up.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering, fence};

pub(crate) struct Idle {
    num_searching: AtomicUsize,
    num_unparked: AtomicUsize,
    num_workers: usize,
    /// Indices of parked workers.
    sleepers: Mutex<Vec<usize>>,
}

impl Idle {
    pub(crate) fn new(num_workers: usize) -> Idle {
        Idle {
            num_searching: AtomicUsize::new(0),
            num_unparked: AtomicUsize::new(num_workers),
            num_workers,
            sleepers: Mutex::new(Vec::with_capacity(num_workers)),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<usize>> {
        self.sleepers.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Picks a parked worker to wake for newly available work, if waking one
    /// is worthwhile. The chosen worker is accounted as unparked *and*
    /// searching.
    pub(crate) fn worker_to_notify(&self) -> Option<usize> {
        // Pairs with the fence in the parking path (see module docs).
        fence(Ordering::SeqCst);
        if !self.notify_should_wakeup() {
            return None;
        }
        let mut sleepers = self.lock();
        if !self.notify_should_wakeup() {
            return None;
        }
        let worker = sleepers.pop()?;
        self.num_searching.fetch_add(1, Ordering::SeqCst);
        self.num_unparked.fetch_add(1, Ordering::SeqCst);
        Some(worker)
    }

    fn notify_should_wakeup(&self) -> bool {
        self.num_searching.load(Ordering::SeqCst) == 0
            && self.num_unparked.load(Ordering::SeqCst) < self.num_workers
    }

    /// Registers `worker` as parked. Returns `true` if it was the last
    /// searching worker, in which case the caller must re-check all queues.
    pub(crate) fn transition_worker_to_parked(&self, worker: usize, is_searching: bool) -> bool {
        let mut sleepers = self.lock();
        let last_searcher = is_searching && self.num_searching.fetch_sub(1, Ordering::SeqCst) == 1;
        self.num_unparked.fetch_sub(1, Ordering::SeqCst);
        sleepers.push(worker);
        drop(sleepers);
        fence(Ordering::SeqCst);
        last_searcher
    }

    /// Tries to become a searching worker (throttled to half the workers).
    pub(crate) fn transition_worker_to_searching(&self) -> bool {
        if 2 * self.num_searching.load(Ordering::SeqCst) >= self.num_workers {
            return false;
        }
        self.num_searching.fetch_add(1, Ordering::SeqCst);
        true
    }

    /// Returns `true` if this was the last searching worker.
    pub(crate) fn transition_worker_from_searching(&self) -> bool {
        self.num_searching.fetch_sub(1, Ordering::SeqCst) == 1
    }

    /// Called by a worker that woke up. If it is still listed as a sleeper,
    /// it woke for another reason (I/O, timer, spurious) and removes itself;
    /// returns `true` in that case. Returns `false` if a notifier already
    /// removed it (and accounted it as searching).
    pub(crate) fn unpark_worker_by_id(&self, worker: usize) -> bool {
        let mut sleepers = self.lock();
        match sleepers.iter().position(|&w| w == worker) {
            Some(i) => {
                sleepers.swap_remove(i);
                self.num_unparked.fetch_add(1, Ordering::SeqCst);
                true
            }
            None => false,
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::Idle;

    #[test]
    fn notify_wakes_one_sleeper_and_marks_it_searching() {
        let idle = Idle::new(2);
        assert_eq!(idle.worker_to_notify(), None, "nobody is parked");
        assert!(!idle.transition_worker_to_parked(1, false));
        assert_eq!(idle.worker_to_notify(), Some(1));
        // Worker 1 is now searching, so further notifications are skipped.
        assert!(!idle.transition_worker_to_parked(0, false));
        assert_eq!(idle.worker_to_notify(), None);
        assert!(!idle.unpark_worker_by_id(1), "already removed by notifier");
        assert!(idle.transition_worker_from_searching());
        assert_eq!(idle.worker_to_notify(), Some(0));
    }

    #[test]
    fn last_searcher_is_reported() {
        let idle = Idle::new(4);
        assert!(idle.transition_worker_to_searching());
        assert!(idle.transition_worker_to_searching());
        assert!(!idle.transition_worker_to_searching(), "throttled at half");
        assert!(!idle.transition_worker_to_parked(0, true));
        assert!(idle.transition_worker_to_parked(1, true));
    }

    #[test]
    fn self_wakeup_removes_from_sleepers() {
        let idle = Idle::new(1);
        idle.transition_worker_to_parked(0, false);
        assert!(idle.unpark_worker_by_id(0));
        assert_eq!(idle.worker_to_notify(), None);
    }
}
