//! The task state machine.
//!
//! Every task has a single `AtomicUsize` that packs lifecycle flags in the low
//! bits and a reference count in the high bits. All transitions are
//! compare-and-swap loops (or single RMW operations) on that word, so a task
//! can be woken, polled, cancelled and joined from different threads without a
//! lock.
//!
//! ```text
//!  bit:   63 .......... 6     5        4          3           2        1        0
//!        [  ref count    | CANCELLED | JOIN_WAKER | JOIN_INTEREST | NOTIFIED | COMPLETE | RUNNING ]
//! ```
//!
//! * `RUNNING`  – a worker is currently polling the future (exclusive access
//!   to the future/output cell).
//! * `COMPLETE` – the future finished (or was cancelled); the output cell is
//!   owned by the `JoinHandle`.
//! * `NOTIFIED` – a `Notified` handle for this task is in (or on its way to)
//!   a run queue. Guarantees at most one queue entry per task.
//! * `JOIN_INTEREST` – a `JoinHandle` exists.
//! * `JOIN_WAKER` – the `JoinHandle` has stored a waker in the trailer; the
//!   bit hands ownership of that slot to the task.
//! * `CANCELLED` – the task should be cancelled the next time it is polled.

use crate::loom::sync::atomic::{AtomicUsize, Ordering};

const RUNNING: usize = 0b00_0001;
const COMPLETE: usize = 0b00_0010;
const NOTIFIED: usize = 0b00_0100;
const JOIN_INTEREST: usize = 0b00_1000;
const JOIN_WAKER: usize = 0b01_0000;
const CANCELLED: usize = 0b10_0000;
const LIFECYCLE_MASK: usize = RUNNING | COMPLETE;
const REF_SHIFT: usize = 6;
const REF_ONE: usize = 1 << REF_SHIFT;

/// A freshly spawned task holds three references (owned-tasks list, the
/// initial `Notified` and the `JoinHandle`), is notified, and has a join
/// handle.
const INITIAL_STATE: usize = (REF_ONE * 3) | JOIN_INTEREST | NOTIFIED;

pub(crate) struct State {
    val: AtomicUsize,
}

/// A copy of the state word at some instant.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Snapshot(usize);

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TransitionToRunning {
    /// The caller now has exclusive access to the future.
    Success,
    /// The caller has exclusive access and must cancel the task.
    Cancelled,
    /// The task is already running or complete; the caller's reference was
    /// dropped.
    Failed,
    /// As `Failed`, and the caller dropped the last reference.
    Dealloc,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TransitionToIdle {
    /// The task is idle; the run reference was dropped.
    Ok,
    /// The task was woken while running; the run reference is transferred to
    /// a new `Notified` that the caller must schedule.
    OkNotified,
    /// The run reference was the last one.
    OkDealloc,
    /// The task was cancelled while running; the caller keeps exclusive
    /// access and must cancel it.
    Cancelled,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TransitionToNotifiedByVal {
    DoNothing,
    /// A reference was created for a new `Notified`; the caller must schedule
    /// it and then drop the waker's reference.
    Submit,
    Dealloc,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TransitionToNotifiedByRef {
    DoNothing,
    /// A new reference was created for the `Notified`.
    Submit,
}

impl State {
    pub(crate) fn new() -> State {
        State {
            val: AtomicUsize::new(INITIAL_STATE),
        }
    }

    pub(crate) fn load(&self) -> Snapshot {
        Snapshot(self.val.load(Ordering::Acquire))
    }

    /// Runs a CAS loop where `f` computes both the result and (optionally) the
    /// next state. Returning `None` leaves the state untouched.
    fn fetch_update_action<T>(&self, mut f: impl FnMut(Snapshot) -> (T, Option<Snapshot>)) -> T {
        let mut curr = self.load();
        loop {
            let (output, next) = f(curr);
            let Some(next) = next else {
                return output;
            };
            match self.val.compare_exchange_weak(
                curr.0,
                next.0,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return output,
                Err(actual) => curr = Snapshot(actual),
            }
        }
    }

    /// CAS loop returning `Ok(next)` on success or `Err(curr)` if `f`
    /// declined to transition.
    fn fetch_update(
        &self,
        mut f: impl FnMut(Snapshot) -> Option<Snapshot>,
    ) -> Result<Snapshot, Snapshot> {
        let mut curr = self.load();
        loop {
            let Some(next) = f(curr) else {
                return Err(curr);
            };
            match self.val.compare_exchange_weak(
                curr.0,
                next.0,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(next),
                Err(actual) => curr = Snapshot(actual),
            }
        }
    }

    /// Called by a worker before polling. Consumes the `Notified` reference.
    pub(crate) fn transition_to_running(&self) -> TransitionToRunning {
        self.fetch_update_action(|mut next| {
            assert!(next.is_notified(), "polling a task that is not notified");
            if !next.is_idle() {
                // Already running or complete (e.g. cancelled by shutdown while
                // queued). Drop the Notified reference instead.
                next.ref_dec();
                let action = if next.ref_count() == 0 {
                    TransitionToRunning::Dealloc
                } else {
                    TransitionToRunning::Failed
                };
                return (action, Some(next));
            }
            next.0 |= RUNNING;
            next.0 &= !NOTIFIED;
            let action = if next.is_cancelled() {
                TransitionToRunning::Cancelled
            } else {
                TransitionToRunning::Success
            };
            (action, Some(next))
        })
    }

    /// Called after a poll returned `Pending`.
    pub(crate) fn transition_to_idle(&self) -> TransitionToIdle {
        self.fetch_update_action(|curr| {
            assert!(curr.is_running());
            if curr.is_cancelled() {
                return (TransitionToIdle::Cancelled, None);
            }
            let mut next = curr;
            next.0 &= !RUNNING;
            let action = if next.is_notified() {
                TransitionToIdle::OkNotified
            } else {
                next.ref_dec();
                if next.ref_count() == 0 {
                    TransitionToIdle::OkDealloc
                } else {
                    TransitionToIdle::Ok
                }
            };
            (action, Some(next))
        })
    }

    /// RUNNING -> COMPLETE. Returns the new state.
    pub(crate) fn transition_to_complete(&self) -> Snapshot {
        const DELTA: usize = RUNNING | COMPLETE;
        let prev = Snapshot(self.val.fetch_xor(DELTA, Ordering::AcqRel));
        assert!(prev.is_running());
        assert!(!prev.is_complete());
        Snapshot(prev.0 ^ DELTA)
    }

    /// Drops `count` references after completion. Returns `true` if the task
    /// must be deallocated.
    pub(crate) fn transition_to_terminal(&self, count: usize) -> bool {
        let prev = Snapshot(self.val.fetch_sub(count * REF_ONE, Ordering::AcqRel));
        assert!(
            prev.ref_count() >= count,
            "task ref count underflow: {} < {count}",
            prev.ref_count()
        );
        prev.ref_count() == count
    }

    /// `Waker::wake` (consumes the waker's reference).
    pub(crate) fn transition_to_notified_by_val(&self) -> TransitionToNotifiedByVal {
        self.fetch_update_action(|mut snapshot| {
            let action = if snapshot.is_running() {
                // The running worker will reschedule after the poll; it holds
                // a reference, so this cannot be the last one.
                snapshot.0 |= NOTIFIED;
                snapshot.ref_dec();
                assert!(snapshot.ref_count() > 0);
                TransitionToNotifiedByVal::DoNothing
            } else if snapshot.is_complete() || snapshot.is_notified() {
                snapshot.ref_dec();
                if snapshot.ref_count() == 0 {
                    TransitionToNotifiedByVal::Dealloc
                } else {
                    TransitionToNotifiedByVal::DoNothing
                }
            } else {
                // Idle: create a reference for the Notified. The caller keeps
                // the waker's reference alive until `schedule` returns (the
                // task may otherwise be freed while the scheduler is still
                // borrowed from it) and drops it afterwards.
                snapshot.0 |= NOTIFIED;
                snapshot.ref_inc();
                TransitionToNotifiedByVal::Submit
            };
            (action, Some(snapshot))
        })
    }

    /// `Waker::wake_by_ref`.
    pub(crate) fn transition_to_notified_by_ref(&self) -> TransitionToNotifiedByRef {
        self.fetch_update_action(|mut snapshot| {
            if snapshot.is_complete() || snapshot.is_notified() {
                (TransitionToNotifiedByRef::DoNothing, None)
            } else if snapshot.is_running() {
                snapshot.0 |= NOTIFIED;
                (TransitionToNotifiedByRef::DoNothing, Some(snapshot))
            } else {
                snapshot.0 |= NOTIFIED;
                snapshot.ref_inc();
                (TransitionToNotifiedByRef::Submit, Some(snapshot))
            }
        })
    }

    /// `JoinHandle::abort`. Returns `true` if the caller must schedule a new
    /// `Notified` (a reference has been created for it).
    pub(crate) fn transition_to_notified_and_cancel(&self) -> bool {
        self.fetch_update_action(|mut snapshot| {
            if snapshot.is_cancelled() || snapshot.is_complete() {
                (false, None)
            } else if snapshot.is_running() {
                // The worker will observe NOTIFIED after the poll, reschedule,
                // and cancel on the next poll.
                snapshot.0 |= NOTIFIED | CANCELLED;
                (false, Some(snapshot))
            } else if snapshot.is_notified() {
                // Already queued; the worker will cancel instead of polling.
                snapshot.0 |= CANCELLED;
                (false, Some(snapshot))
            } else {
                snapshot.0 |= NOTIFIED | CANCELLED;
                snapshot.ref_inc();
                (true, Some(snapshot))
            }
        })
    }

    /// Runtime shutdown: marks the task cancelled and, if it is idle, claims
    /// it (sets RUNNING). Returns `true` if the caller must cancel it.
    pub(crate) fn transition_to_shutdown(&self) -> bool {
        let mut prev = Snapshot(0);
        let _ = self.fetch_update(|mut snapshot| {
            prev = snapshot;
            if snapshot.is_idle() {
                snapshot.0 |= RUNNING;
            }
            snapshot.0 |= CANCELLED;
            Some(snapshot)
        });
        prev.is_idle()
    }

    /// Called when the `JoinHandle` is dropped. Fails if the task already
    /// completed, in which case the handle must drop the output.
    pub(crate) fn unset_join_interested(&self) -> Result<Snapshot, Snapshot> {
        self.fetch_update(|curr| {
            assert!(curr.is_join_interested());
            if curr.is_complete() {
                return None;
            }
            let mut next = curr;
            next.0 &= !JOIN_INTEREST;
            Some(next)
        })
    }

    /// Publishes the join waker. Fails if the task already completed.
    pub(crate) fn set_join_waker(&self) -> Result<Snapshot, Snapshot> {
        self.fetch_update(|curr| {
            assert!(curr.is_join_interested());
            assert!(!curr.is_join_waker_set());
            if curr.is_complete() {
                return None;
            }
            let mut next = curr;
            next.0 |= JOIN_WAKER;
            Some(next)
        })
    }

    /// Reclaims the join waker slot so it can be overwritten. Fails if the
    /// task already completed.
    pub(crate) fn unset_waker(&self) -> Result<Snapshot, Snapshot> {
        self.fetch_update(|curr| {
            assert!(curr.is_join_interested());
            assert!(curr.is_join_waker_set());
            if curr.is_complete() {
                return None;
            }
            let mut next = curr;
            next.0 &= !JOIN_WAKER;
            Some(next)
        })
    }

    pub(crate) fn ref_inc(&self) {
        // Relaxed is enough: a new reference can only be created from an
        // existing one, which already synchronises with the task's creation.
        let prev = self.val.fetch_add(REF_ONE, Ordering::Relaxed);
        if prev > isize::MAX as usize {
            std::process::abort();
        }
    }

    /// Returns `true` if this was the last reference.
    pub(crate) fn ref_dec(&self) -> bool {
        let prev = Snapshot(self.val.fetch_sub(REF_ONE, Ordering::AcqRel));
        assert!(prev.ref_count() >= 1, "task ref count underflow");
        prev.ref_count() == 1
    }
}

impl Snapshot {
    pub(crate) fn is_idle(self) -> bool {
        self.0 & LIFECYCLE_MASK == 0
    }
    pub(crate) fn is_running(self) -> bool {
        self.0 & RUNNING != 0
    }
    pub(crate) fn is_complete(self) -> bool {
        self.0 & COMPLETE != 0
    }
    pub(crate) fn is_notified(self) -> bool {
        self.0 & NOTIFIED != 0
    }
    pub(crate) fn is_cancelled(self) -> bool {
        self.0 & CANCELLED != 0
    }
    pub(crate) fn is_join_interested(self) -> bool {
        self.0 & JOIN_INTEREST != 0
    }
    pub(crate) fn is_join_waker_set(self) -> bool {
        self.0 & JOIN_WAKER != 0
    }
    pub(crate) fn ref_count(self) -> usize {
        self.0 >> REF_SHIFT
    }
    fn ref_inc(&mut self) {
        self.0 += REF_ONE;
    }
    fn ref_dec(&mut self) {
        assert!(self.ref_count() > 0);
        self.0 -= REF_ONE;
    }
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("running", &self.is_running())
            .field("complete", &self.is_complete())
            .field("notified", &self.is_notified())
            .field("cancelled", &self.is_cancelled())
            .field("join_interested", &self.is_join_interested())
            .field("join_waker", &self.is_join_waker_set())
            .field("refs", &self.ref_count())
            .finish()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn initial_state() {
        let s = State::new().load();
        assert!(s.is_notified() && s.is_idle() && s.is_join_interested());
        assert_eq!(s.ref_count(), 3);
    }

    #[test]
    fn run_then_idle_drops_run_ref() {
        let state = State::new();
        assert_eq!(state.transition_to_running(), TransitionToRunning::Success);
        assert!(state.load().is_running() && !state.load().is_notified());
        assert_eq!(state.transition_to_idle(), TransitionToIdle::Ok);
        assert_eq!(state.load().ref_count(), 2);
    }

    #[test]
    fn wake_while_running_reschedules() {
        let state = State::new();
        state.transition_to_running();
        assert_eq!(
            state.transition_to_notified_by_ref(),
            TransitionToNotifiedByRef::DoNothing
        );
        assert_eq!(state.transition_to_idle(), TransitionToIdle::OkNotified);
        // The run reference moved to the new Notified.
        assert_eq!(state.load().ref_count(), 3);
        assert!(state.load().is_notified());
    }

    #[test]
    fn wake_idle_task_submits_once() {
        let state = State::new();
        state.transition_to_running();
        state.transition_to_idle();
        assert_eq!(
            state.transition_to_notified_by_ref(),
            TransitionToNotifiedByRef::Submit
        );
        // Second wake is coalesced.
        assert_eq!(
            state.transition_to_notified_by_ref(),
            TransitionToNotifiedByRef::DoNothing
        );
        assert_eq!(state.load().ref_count(), 3);
    }

    #[test]
    fn wake_by_val_on_complete_drops_ref() {
        let state = State::new();
        state.transition_to_running();
        state.transition_to_complete();
        state.ref_inc(); // a waker
        assert_eq!(
            state.transition_to_notified_by_val(),
            TransitionToNotifiedByVal::DoNothing
        );
        assert_eq!(state.load().ref_count(), 3);
    }

    #[test]
    fn abort_idle_task() {
        let state = State::new();
        state.transition_to_running();
        state.transition_to_idle();
        assert!(state.transition_to_notified_and_cancel());
        assert!(!state.transition_to_notified_and_cancel(), "idempotent");
        assert_eq!(
            state.transition_to_running(),
            TransitionToRunning::Cancelled
        );
    }

    #[test]
    fn abort_running_task_cancels_after_poll() {
        let state = State::new();
        state.transition_to_running();
        assert!(!state.transition_to_notified_and_cancel());
        assert_eq!(state.transition_to_idle(), TransitionToIdle::Cancelled);
    }

    #[test]
    fn join_interest_vs_completion() {
        let state = State::new();
        state.transition_to_running();
        state.transition_to_complete();
        assert!(state.unset_join_interested().is_err());
        assert!(state.set_join_waker().is_err());
    }

    #[test]
    fn shutdown_claims_idle_tasks_only() {
        let state = State::new();
        assert!(state.transition_to_shutdown(), "idle+notified is claimable");
        assert_eq!(state.transition_to_running(), TransitionToRunning::Failed);

        let running = State::new();
        running.transition_to_running();
        assert!(!running.transition_to_shutdown());
        assert!(running.load().is_cancelled());
    }

    #[test]
    fn terminal_reports_dealloc() {
        let state = State::new();
        state.transition_to_running();
        state.transition_to_complete();
        assert!(!state.transition_to_terminal(2));
        assert!(state.transition_to_terminal(1));
    }
}
