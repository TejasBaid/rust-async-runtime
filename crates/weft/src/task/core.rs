//! Memory layout of a task.
//!
//! A spawned task is a **single heap allocation** (`Cell<T, S>`) holding the
//! header, the scheduler handle, the future (later replaced by its output) and
//! the join waker. Type-erased handles (`RawTask`, wakers, `JoinHandle`) point
//! at the `Header`, which is the first field of a `#[repr(C)]` struct, so a
//! `*mut Header` can be cast back to `*mut Cell<T, S>` by the monomorphised
//! functions in the header's vtable.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use super::raw::Vtable;
use super::state::State;
use super::{Id, JoinError};
use crate::loom::cell::UnsafeCell;

#[repr(C)]
pub(super) struct Cell<T: Future, S> {
    /// Hot data touched by every handle; must be first (see module docs).
    pub(super) header: Header,
    pub(super) core: Core<T, S>,
    pub(super) trailer: Trailer,
}

pub(crate) struct Header {
    pub(super) state: State,
    pub(super) vtable: &'static Vtable,
    pub(super) id: Id,
}

pub(super) struct Core<T: Future, S> {
    pub(super) scheduler: S,
    /// Accessed only by the holder of the RUNNING bit (future) or, after
    /// COMPLETE, by the `JoinHandle` (output).
    pub(super) stage: UnsafeCell<Stage<T>>,
}

pub(super) struct Trailer {
    /// Owned by the `JoinHandle` while `JOIN_WAKER` is unset and by the task
    /// while it is set.
    pub(super) waker: UnsafeCell<Option<Waker>>,
}

pub(super) enum Stage<T: Future> {
    Running(T),
    Finished(Result<T::Output, JoinError>),
    Consumed,
}

impl<T: Future, S> Cell<T, S> {
    pub(super) fn new(future: T, scheduler: S, vtable: &'static Vtable, id: Id) -> Box<Self> {
        Box::new(Cell {
            header: Header {
                state: State::new(),
                vtable,
                id,
            },
            core: Core {
                scheduler,
                stage: UnsafeCell::new(Stage::Running(future)),
            },
            trailer: Trailer {
                waker: UnsafeCell::new(None),
            },
        })
    }
}

impl<T: Future, S> Core<T, S> {
    /// Polls the future.
    ///
    /// # Safety
    /// The caller must hold the RUNNING bit (exclusive access to `stage`) and
    /// the stage must be `Running`.
    pub(super) unsafe fn poll(&self, cx: &mut Context<'_>) -> Poll<T::Output> {
        let res = self.stage.with_mut(|ptr| {
            // SAFETY: exclusive access is guaranteed by the caller.
            let Stage::Running(future) = (unsafe { &mut *ptr }) else {
                unreachable!("polled a task that is not in the running stage");
            };
            // SAFETY: the future lives inside a heap allocation that is never
            // moved until it is dropped in place, so it is structurally pinned.
            let future = unsafe { Pin::new_unchecked(future) };
            future.poll(cx)
        });
        if res.is_ready() {
            // SAFETY: still exclusive (caller contract).
            unsafe { self.drop_future_or_output() };
        }
        res
    }

    /// # Safety
    /// The caller must have exclusive access to `stage`.
    pub(super) unsafe fn drop_future_or_output(&self) {
        // SAFETY: forwarded caller contract.
        unsafe { self.set_stage(Stage::Consumed) };
    }

    /// # Safety
    /// The caller must have exclusive access to `stage`.
    pub(super) unsafe fn store_output(&self, output: Result<T::Output, JoinError>) {
        // SAFETY: forwarded caller contract.
        unsafe { self.set_stage(Stage::Finished(output)) };
    }

    /// # Safety
    /// The caller must have exclusive access to `stage` (task COMPLETE and the
    /// caller is the `JoinHandle`).
    pub(super) unsafe fn take_output(&self) -> Result<T::Output, JoinError> {
        self.stage.with_mut(|ptr| {
            // SAFETY: exclusive access is guaranteed by the caller.
            match std::mem::replace(unsafe { &mut *ptr }, Stage::Consumed) {
                Stage::Finished(output) => output,
                _ => panic!("JoinHandle polled after completion"),
            }
        })
    }

    /// Replaces the stage *before* dropping the old value, so a panicking
    /// destructor can never leave a half-dropped value behind.
    ///
    /// # Safety
    /// The caller must have exclusive access to `stage`.
    unsafe fn set_stage(&self, stage: Stage<T>) {
        // SAFETY: exclusive access is guaranteed by the caller.
        let old = self
            .stage
            .with_mut(|ptr| std::mem::replace(unsafe { &mut *ptr }, stage));
        drop(old);
    }
}

impl Trailer {
    /// # Safety
    /// The caller must own the waker slot (see field docs).
    pub(super) unsafe fn set_waker(&self, waker: Option<Waker>) {
        // SAFETY: forwarded caller contract.
        self.waker.with_mut(|ptr| unsafe { *ptr = waker });
    }

    /// # Safety
    /// The caller must have shared read access to the slot.
    pub(super) unsafe fn will_wake(&self, waker: &Waker) -> bool {
        self.waker.with(|ptr| {
            // SAFETY: forwarded caller contract.
            unsafe { &*ptr }
                .as_ref()
                .is_some_and(|w| w.will_wake(waker))
        })
    }

    /// # Safety
    /// The task must own the slot (`JOIN_WAKER` set and task COMPLETE).
    pub(super) unsafe fn wake_join(&self) {
        self.waker.with(|ptr| {
            // SAFETY: forwarded caller contract.
            match unsafe { &*ptr } {
                Some(waker) => waker.wake_by_ref(),
                None => panic!("join waker missing"),
            }
        });
    }
}
