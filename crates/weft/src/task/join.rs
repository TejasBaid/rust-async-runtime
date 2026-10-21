use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::raw::RawTask;
use super::{Id, JoinError};

/// An owned permission to await a spawned task's output.
///
/// Dropping a `JoinHandle` *detaches* the task: it keeps running, and its
/// output is dropped when it completes. Use [`JoinHandle::abort`] to cancel
/// it instead.
pub struct JoinHandle<T> {
    raw: RawTask,
    _p: PhantomData<T>,
}

// SAFETY: the handle only moves `T` out of the task (from whichever thread
// polls it), so `T: Send` is sufficient; all shared state is synchronised by
// the task's atomic state word.
unsafe impl<T: Send> Send for JoinHandle<T> {}
// SAFETY: `&JoinHandle` only exposes `abort`, `is_finished` and `id`, which
// touch the atomic state word exclusively.
unsafe impl<T: Send> Sync for JoinHandle<T> {}

impl<T> Unpin for JoinHandle<T> {}

impl<T> JoinHandle<T> {
    pub(super) fn new(raw: RawTask) -> JoinHandle<T> {
        JoinHandle {
            raw,
            _p: PhantomData,
        }
    }

    /// Requests cancellation of the task. If the task has not completed, the
    /// handle resolves to a [`JoinError`] for which
    /// [`is_cancelled`](JoinError::is_cancelled) returns `true`. A task that is
    /// currently being polled is cancelled once that poll returns.
    pub fn abort(&self) {
        self.raw.remote_abort();
    }

    /// Returns `true` if the task has finished (completed, panicked or been
    /// cancelled).
    pub fn is_finished(&self) -> bool {
        self.raw.state().load().is_complete()
    }

    /// The task's unique id.
    pub fn id(&self) -> Id {
        self.raw.id()
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut ret = Poll::Pending;
        // SAFETY: `ret` has type `Poll<Result<T, JoinError>>`, which matches
        // the task's output type because `T` was fixed when the task was
        // spawned.
        unsafe {
            self.raw.try_read_output(
                (&mut ret as *mut Poll<Result<T, JoinError>>).cast(),
                cx.waker(),
            )
        };
        ret
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        self.raw.drop_join_handle_slow();
    }
}

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle")
            .field("id", &self.id())
            .field("finished", &self.is_finished())
            .finish()
    }
}
