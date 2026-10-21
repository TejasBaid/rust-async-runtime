//! The task `Waker` implementation.
//!
//! A task waker is a `RawWaker` whose data pointer is the task's `Header`.
//! Cloning increments the task's reference count, dropping decrements it, and
//! waking runs the `NOTIFIED` state transition and, if the task was idle,
//! pushes it onto a run queue through the header's vtable. A single static
//! `RawWakerVTable` serves every task type; the type-specific work is
//! dispatched through the task's own vtable.

use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ptr::NonNull;
use std::task::{RawWaker, RawWakerVTable, Waker};

use super::core::Header;
use super::raw::RawTask;

static WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_waker, wake_by_val, wake_by_ref, drop_waker);

/// A `Waker` borrowed from a running task without touching its reference
/// count. Only valid while the borrow of the header is.
pub(super) struct WakerRef<'a> {
    waker: ManuallyDrop<Waker>,
    _p: PhantomData<&'a Header>,
}

impl Deref for WakerRef<'_> {
    type Target = Waker;

    fn deref(&self) -> &Waker {
        &self.waker
    }
}

/// Creates a waker for the task without incrementing its reference count.
/// The caller (the worker polling the task) holds a reference for the whole
/// lifetime of the returned value; `ManuallyDrop` guarantees the matching
/// decrement never happens.
pub(super) fn waker_ref(header: &NonNull<Header>) -> WakerRef<'_> {
    // SAFETY: the data pointer is a live task header and the vtable functions
    // below implement the RawWaker contract for it. Dropping is suppressed by
    // `ManuallyDrop`, matching the reference we did not take.
    let waker = unsafe { Waker::from_raw(raw_waker(*header)) };
    WakerRef {
        waker: ManuallyDrop::new(waker),
        _p: PhantomData,
    }
}

fn raw_waker(header: NonNull<Header>) -> RawWaker {
    RawWaker::new(header.as_ptr() as *const (), &WAKER_VTABLE)
}

/// # Safety
/// `ptr` must be the data pointer of a waker created by this module.
unsafe fn task(ptr: *const ()) -> RawTask {
    // SAFETY: wakers are only constructed from non-null header pointers of
    // live tasks, and each waker owns (or borrows) a reference.
    unsafe { RawTask::from_raw(NonNull::new_unchecked(ptr as *mut Header)) }
}

unsafe fn clone_waker(ptr: *const ()) -> RawWaker {
    // SAFETY: called by `Waker::clone` on a waker from this module.
    let task = unsafe { task(ptr) };
    task.ref_inc();
    raw_waker(task.header_ptr())
}

unsafe fn wake_by_val(ptr: *const ()) {
    // SAFETY: called by `Waker::wake`; consumes the waker's reference.
    unsafe { task(ptr) }.wake_by_val();
}

unsafe fn wake_by_ref(ptr: *const ()) {
    // SAFETY: called by `Waker::wake_by_ref`; the waker keeps its reference.
    unsafe { task(ptr) }.wake_by_ref();
}

unsafe fn drop_waker(ptr: *const ()) {
    // SAFETY: called by `Waker::drop`; releases the waker's reference.
    unsafe { task(ptr) }.drop_reference();
}
