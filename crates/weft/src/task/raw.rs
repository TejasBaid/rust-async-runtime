//! Type-erased task pointer and its vtable.

use std::future::Future;
use std::ptr::NonNull;
use std::task::Waker;

use super::core::{Cell, Header};
use super::harness::Harness;
use super::state::{State, TransitionToNotifiedByRef, TransitionToNotifiedByVal};
use super::{Id, Schedule};

/// Monomorphised operations for a concrete `Cell<T, S>`.
pub(crate) struct Vtable {
    /// Polls the future (consumes a `Notified` reference).
    pub(super) poll: unsafe fn(NonNull<Header>),
    /// Hands a `Notified` (whose reference the caller has already created)
    /// to the task's scheduler.
    pub(super) schedule: unsafe fn(NonNull<Header>),
    /// Frees the allocation.
    pub(super) dealloc: unsafe fn(NonNull<Header>),
    /// Reads the output into `*mut Poll<Result<T::Output, JoinError>>`, or
    /// registers the waker.
    pub(super) try_read_output: unsafe fn(NonNull<Header>, *mut (), &Waker),
    /// Drops the `JoinHandle`'s interest (and the output if already complete).
    pub(super) drop_join_handle_slow: unsafe fn(NonNull<Header>),
    /// Cancels the task during runtime shutdown (consumes a reference).
    pub(super) shutdown: unsafe fn(NonNull<Header>),
}

pub(super) fn vtable<T: Future, S: Schedule>() -> &'static Vtable {
    &Vtable {
        poll: poll::<T, S>,
        schedule: schedule::<T, S>,
        dealloc: dealloc::<T, S>,
        try_read_output: try_read_output::<T, S>,
        drop_join_handle_slow: drop_join_handle_slow::<T, S>,
        shutdown: shutdown::<T, S>,
    }
}

/// A non-owning, copyable pointer to a task. Reference counting is done
/// explicitly by the owning wrappers (`Task`, `Notified`, `JoinHandle`,
/// wakers).
#[derive(Clone, Copy)]
pub(crate) struct RawTask {
    ptr: NonNull<Header>,
}

impl RawTask {
    pub(super) fn new<T, S>(future: T, scheduler: S, id: Id) -> RawTask
    where
        T: Future,
        S: Schedule,
    {
        let cell = Cell::new(future, scheduler, vtable::<T, S>(), id);
        let ptr = NonNull::from(Box::leak(cell)).cast::<Header>();
        RawTask { ptr }
    }

    /// # Safety
    /// `ptr` must point to the header of a live task.
    pub(super) unsafe fn from_raw(ptr: NonNull<Header>) -> RawTask {
        RawTask { ptr }
    }

    pub(super) fn header_ptr(self) -> NonNull<Header> {
        self.ptr
    }

    pub(super) fn header(&self) -> &Header {
        // SAFETY: a RawTask is only constructed for live tasks and every user
        // of one holds a reference that keeps the allocation alive.
        unsafe { self.ptr.as_ref() }
    }

    pub(super) fn state(&self) -> &State {
        &self.header().state
    }

    pub(super) fn id(&self) -> Id {
        self.header().id
    }

    /// Polls the task. The caller gives up one `Notified` reference.
    pub(super) fn poll(self) {
        // SAFETY: the vtable entry matches the concrete type of this task.
        unsafe { (self.header().vtable.poll)(self.ptr) }
    }

    fn schedule(self) {
        // SAFETY: see `poll`.
        unsafe { (self.header().vtable.schedule)(self.ptr) }
    }

    pub(super) fn dealloc(self) {
        // SAFETY: see `poll`; callers only invoke this after the last
        // reference is gone.
        unsafe { (self.header().vtable.dealloc)(self.ptr) }
    }

    /// # Safety
    /// `dst` must point to a `Poll<Result<T::Output, JoinError>>` for this
    /// task's output type `T::Output`.
    pub(super) unsafe fn try_read_output(self, dst: *mut (), waker: &Waker) {
        // SAFETY: forwarded caller contract; vtable matches the task type.
        unsafe { (self.header().vtable.try_read_output)(self.ptr, dst, waker) }
    }

    pub(super) fn drop_join_handle_slow(self) {
        // SAFETY: see `poll`.
        unsafe { (self.header().vtable.drop_join_handle_slow)(self.ptr) }
    }

    pub(super) fn shutdown(self) {
        // SAFETY: see `poll`.
        unsafe { (self.header().vtable.shutdown)(self.ptr) }
    }

    pub(super) fn ref_inc(self) {
        self.state().ref_inc();
    }

    pub(super) fn drop_reference(self) {
        if self.state().ref_dec() {
            self.dealloc();
        }
    }

    /// `Waker::wake`: consumes the waker's reference.
    pub(super) fn wake_by_val(self) {
        match self.state().transition_to_notified_by_val() {
            TransitionToNotifiedByVal::Submit => {
                self.schedule();
                self.drop_reference();
            }
            TransitionToNotifiedByVal::Dealloc => self.dealloc(),
            TransitionToNotifiedByVal::DoNothing => {}
        }
    }

    /// `Waker::wake_by_ref`.
    pub(super) fn wake_by_ref(self) {
        match self.state().transition_to_notified_by_ref() {
            TransitionToNotifiedByRef::Submit => self.schedule(),
            TransitionToNotifiedByRef::DoNothing => {}
        }
    }

    /// `JoinHandle::abort`.
    pub(super) fn remote_abort(self) {
        if self.state().transition_to_notified_and_cancel() {
            self.schedule();
        }
    }
}

// SAFETY: every operation on the shared header goes through the atomic state
// word; access to the future/output is serialised by the state machine.
unsafe impl Send for RawTask {}
// SAFETY: as above.
unsafe impl Sync for RawTask {}

unsafe fn poll<T: Future, S: Schedule>(ptr: NonNull<Header>) {
    // SAFETY: `ptr` points to a `Cell<T, S>` (vtable is monomorphised).
    unsafe { Harness::<T, S>::from_raw(ptr) }.poll();
}

unsafe fn schedule<T: Future, S: Schedule>(ptr: NonNull<Header>) {
    // SAFETY: `ptr` points to a `Cell<T, S>`; the caller created the
    // reference that the new `Notified` takes ownership of.
    unsafe { Harness::<T, S>::from_raw(ptr) }.schedule_notified();
}

unsafe fn dealloc<T: Future, S: Schedule>(ptr: NonNull<Header>) {
    // SAFETY: called exactly once, after the last reference was dropped;
    // `ptr` was produced by `Box::leak` of a `Box<Cell<T, S>>`.
    drop(unsafe { Box::from_raw(ptr.cast::<Cell<T, S>>().as_ptr()) });
}

unsafe fn try_read_output<T: Future, S: Schedule>(
    ptr: NonNull<Header>,
    dst: *mut (),
    waker: &Waker,
) {
    // SAFETY: `ptr` points to a `Cell<T, S>`; `dst` has the right type per
    // `RawTask::try_read_output`'s contract.
    unsafe { Harness::<T, S>::from_raw(ptr).try_read_output(dst.cast(), waker) }
}

unsafe fn drop_join_handle_slow<T: Future, S: Schedule>(ptr: NonNull<Header>) {
    // SAFETY: `ptr` points to a `Cell<T, S>`.
    unsafe { Harness::<T, S>::from_raw(ptr) }.drop_join_handle_slow();
}

unsafe fn shutdown<T: Future, S: Schedule>(ptr: NonNull<Header>) {
    // SAFETY: `ptr` points to a `Cell<T, S>`.
    unsafe { Harness::<T, S>::from_raw(ptr) }.shutdown();
}
