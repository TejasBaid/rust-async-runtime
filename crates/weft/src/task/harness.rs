//! Typed operations on a task, invoked through the vtable.

use std::any::Any;
use std::future::Future;
use std::mem::ManuallyDrop;
use std::panic::{self, AssertUnwindSafe};
use std::ptr::NonNull;
use std::task::{Context, Poll, Waker};

use super::core::{Cell, Core, Header, Trailer};
use super::raw::RawTask;
use super::state::{Snapshot, State, TransitionToIdle, TransitionToRunning};
use super::waker::waker_ref;
use super::{JoinError, Notified, Schedule, Task};

pub(super) struct Harness<T: Future, S: 'static> {
    cell: NonNull<Cell<T, S>>,
}

impl<T: Future, S: 'static> Harness<T, S> {
    /// # Safety
    /// `ptr` must point to the header of a live `Cell<T, S>`.
    pub(super) unsafe fn from_raw(ptr: NonNull<Header>) -> Harness<T, S> {
        Harness { cell: ptr.cast() }
    }

    fn cell(&self) -> &Cell<T, S> {
        // SAFETY: `from_raw`'s contract guarantees the cell is alive for as
        // long as the caller holds the reference it is operating on.
        unsafe { self.cell.as_ref() }
    }

    fn header_ptr(&self) -> NonNull<Header> {
        self.cell.cast()
    }

    fn header(&self) -> &Header {
        &self.cell().header
    }

    fn state(&self) -> &State {
        &self.header().state
    }

    fn core(&self) -> &Core<T, S> {
        &self.cell().core
    }

    fn trailer(&self) -> &Trailer {
        &self.cell().trailer
    }

    fn raw(&self) -> RawTask {
        // SAFETY: the cell is alive (see `cell`).
        unsafe { RawTask::from_raw(self.header_ptr()) }
    }

    fn dealloc(self) {
        self.raw().dealloc();
    }

    fn drop_reference(self) {
        self.raw().drop_reference();
    }
}

impl<T: Future, S: Schedule> Harness<T, S> {
    /// Polls the task once. Consumes the `Notified` reference.
    pub(super) fn poll(self) {
        match self.state().transition_to_running() {
            TransitionToRunning::Success => {
                let header = self.header_ptr();
                let waker = waker_ref(&header);
                let mut cx = Context::from_waker(&waker);
                if poll_future(self.core(), self.header().id, &mut cx).is_ready() {
                    self.complete();
                    return;
                }
                match self.state().transition_to_idle() {
                    TransitionToIdle::Ok => {}
                    TransitionToIdle::OkNotified => {
                        // Woken during the poll: re-queue behind other work
                        // (this is what makes `yield_now` fair). The new
                        // Notified gets its own reference; ours keeps the
                        // cell (and thus the borrowed scheduler) alive until
                        // `yield_now` returns, because another worker may run
                        // the task to completion in the meantime.
                        self.raw().ref_inc();
                        let notified = Notified(Task::from_raw(self.raw()));
                        self.core().scheduler.yield_now(notified);
                        self.drop_reference();
                    }
                    TransitionToIdle::OkDealloc => self.dealloc(),
                    TransitionToIdle::Cancelled => {
                        cancel_task(self.core(), self.header().id);
                        self.complete();
                    }
                }
            }
            TransitionToRunning::Cancelled => {
                cancel_task(self.core(), self.header().id);
                self.complete();
            }
            TransitionToRunning::Failed => {}
            TransitionToRunning::Dealloc => self.dealloc(),
        }
    }

    /// Schedules a `Notified` whose reference the caller already created.
    pub(super) fn schedule_notified(self) {
        let notified = Notified(Task::from_raw(self.raw()));
        self.core().scheduler.schedule(notified);
    }

    /// Cancels the task on runtime shutdown. Consumes one reference.
    pub(super) fn shutdown(self) {
        if !self.state().transition_to_shutdown() {
            // Running elsewhere (it will observe CANCELLED) or complete.
            self.drop_reference();
            return;
        }
        cancel_task(self.core(), self.header().id);
        self.complete();
    }

    /// Finishes a task whose output (or error) has been stored. The caller
    /// holds the RUNNING bit and one reference.
    fn complete(self) {
        let snapshot = self.state().transition_to_complete();
        // Neither branch may unwind into the scheduler.
        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            if !snapshot.is_join_interested() {
                // Nobody will read the output: drop it now.
                // SAFETY: COMPLETE is set and the JoinHandle is gone, so the
                // task has exclusive access to the stage.
                unsafe { self.core().drop_future_or_output() };
            } else if snapshot.is_join_waker_set() {
                // SAFETY: JOIN_WAKER and COMPLETE are both set, so the
                // JoinHandle can no longer modify the waker slot.
                unsafe { self.trailer().wake_join() };
            }
        }));

        // Remove the task from the owned list; that releases the list's
        // reference in the same atomic operation as ours.
        let me = ManuallyDrop::new(Task::<S>::from_raw(self.raw()));
        let num_release = match self.core().scheduler.release(&me) {
            Some(task) => {
                std::mem::forget(task);
                2
            }
            None => 1,
        };
        if self.state().transition_to_terminal(num_release) {
            self.dealloc();
        }
    }

    /// # Safety
    /// `dst` must be a valid `*mut Poll<Result<T::Output, JoinError>>`.
    pub(super) unsafe fn try_read_output(
        self,
        dst: *mut Poll<Result<T::Output, JoinError>>,
        waker: &Waker,
    ) {
        if can_read_output(self.state(), self.trailer(), waker) {
            // SAFETY: COMPLETE was observed with Acquire ordering, so the
            // JoinHandle has exclusive access to the output; `dst` is valid
            // per the caller's contract and holds a `Poll::Pending` (no drop
            // glue runs on overwrite).
            unsafe { *dst = Poll::Ready(self.core().take_output()) };
        }
    }

    pub(super) fn drop_join_handle_slow(self) {
        if self.state().unset_join_interested().is_err() {
            // The task completed before we could withdraw interest, so the
            // output is ours to drop. A panicking destructor must not unwind
            // out of `JoinHandle::drop`.
            let _ = panic::catch_unwind(AssertUnwindSafe(|| {
                // SAFETY: COMPLETE is set and we are the JoinHandle.
                unsafe { self.core().drop_future_or_output() };
            }));
        }
        self.drop_reference();
    }
}

/// Decides whether the `JoinHandle` may read the output; otherwise stores the
/// waker so that the task wakes the handle on completion.
fn can_read_output(state: &State, trailer: &Trailer, waker: &Waker) -> bool {
    let snapshot = state.load();
    debug_assert!(snapshot.is_join_interested());
    if snapshot.is_complete() {
        return true;
    }
    let res = if snapshot.is_join_waker_set() {
        // SAFETY: while JOIN_WAKER is set, the JoinHandle may read the slot.
        if unsafe { trailer.will_wake(waker) } {
            return false;
        }
        // Reclaim the slot, then publish the new waker.
        state
            .unset_waker()
            .and_then(|snapshot| set_join_waker(state, trailer, waker.clone(), snapshot))
    } else {
        set_join_waker(state, trailer, waker.clone(), snapshot)
    };
    match res {
        Ok(_) => false,
        Err(snapshot) => {
            assert!(snapshot.is_complete());
            true
        }
    }
}

fn set_join_waker(
    state: &State,
    trailer: &Trailer,
    waker: Waker,
    snapshot: Snapshot,
) -> Result<Snapshot, Snapshot> {
    assert!(snapshot.is_join_interested());
    assert!(!snapshot.is_join_waker_set());
    // SAFETY: JOIN_WAKER is unset, so the JoinHandle owns the slot.
    unsafe { trailer.set_waker(Some(waker)) };
    let res = state.set_join_waker();
    if res.is_err() {
        // The task completed first and never looked at the slot; clear it.
        // SAFETY: JOIN_WAKER is still unset, we still own the slot.
        unsafe { trailer.set_waker(None) };
    }
    res
}

/// Polls the future, catching panics. Returns `Ready` once the output (or a
/// panic error) has been stored.
fn poll_future<T: Future, S>(core: &Core<T, S>, id: super::Id, cx: &mut Context<'_>) -> Poll<()> {
    let output = match panic::catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: the caller transitioned the task to RUNNING.
        unsafe { core.poll(cx) }
    })) {
        Ok(Poll::Pending) => return Poll::Pending,
        Ok(Poll::Ready(output)) => Ok(output),
        Err(panic) => {
            let _ = drop_future_catching(core);
            Err(JoinError::panic(id, panic))
        }
    };
    // SAFETY: still RUNNING, so we have exclusive access.
    unsafe { core.store_output(output) };
    Poll::Ready(())
}

/// Drops the future and stores a cancellation error.
fn cancel_task<T: Future, S>(core: &Core<T, S>, id: super::Id) {
    let err = match drop_future_catching(core) {
        Ok(()) => JoinError::cancelled(id),
        Err(panic) => JoinError::panic(id, panic),
    };
    // SAFETY: the caller holds the RUNNING bit.
    unsafe { core.store_output(Err(err)) };
}

fn drop_future_catching<T: Future, S>(core: &Core<T, S>) -> Result<(), Box<dyn Any + Send>> {
    panic::catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: the caller holds the RUNNING bit.
        unsafe { core.drop_future_or_output() }
    }))
}
