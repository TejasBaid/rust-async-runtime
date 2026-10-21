//! Thread-local runtime context.
//!
//! Each thread that runs runtime code (workers, blocking-pool threads, and
//! threads inside `Runtime::block_on`) has the runtime's [`Handle`] installed
//! here so that free functions like [`spawn`](crate::spawn) and I/O
//! constructors can find it.

use std::cell::{Cell, RefCell};

use super::Handle;
use crate::driver;

thread_local! {
    static CURRENT: RefCell<Option<Handle>> = const { RefCell::new(None) };
    static IN_WORKER: Cell<bool> = const { Cell::new(false) };
}

/// Runs `f` with the current handle, if any.
pub(crate) fn with_current<R>(f: impl FnOnce(&Handle) -> R) -> Option<R> {
    CURRENT
        .try_with(|c| c.borrow().as_ref().map(f))
        .ok()
        .flatten()
}

/// Returns the current runtime's driver handle.
///
/// # Panics
/// Panics if there is no runtime context on this thread.
#[track_caller]
pub(crate) fn driver_handle() -> driver::Handle {
    match with_current(|h| h.shared.driver.clone()) {
        Some(handle) => handle,
        None => panic!(
            "there is no reactor running: this must be called from the context of a weft runtime"
        ),
    }
}

/// Restores the previous context when dropped.
pub(crate) struct EnterGuard {
    prev: Option<Handle>,
}

/// Installs `handle` as the current runtime for this thread.
pub(crate) fn enter(handle: Handle) -> EnterGuard {
    let prev = CURRENT.with(|c| c.borrow_mut().replace(handle));
    EnterGuard { prev }
}

impl Drop for EnterGuard {
    fn drop(&mut self) {
        let prev = self.prev.take();
        let _ = CURRENT.try_with(|c| *c.borrow_mut() = prev);
    }
}

/// Marks the current thread as a worker thread for the guard's lifetime.
pub(crate) struct WorkerGuard(());

pub(crate) fn enter_worker() -> WorkerGuard {
    IN_WORKER.with(|w| w.set(true));
    WorkerGuard(())
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        let _ = IN_WORKER.try_with(|w| w.set(false));
    }
}

/// Returns `true` on a runtime worker thread.
pub(crate) fn is_worker_thread() -> bool {
    IN_WORKER.try_with(Cell::get).unwrap_or(false)
}
