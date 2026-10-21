//! Tasks: spawning, joining, cancellation and cooperative yielding.
//!
//! # Design
//!
//! A task is a single heap allocation containing a header (atomic state word
//! and vtable pointer), the scheduler handle, the future/output cell and the
//! join waker (see `task/core.rs`). All handles to the task are thin pointers to
//! the header and carry one reference each:
//!
//! | handle         | role                                                     |
//! |----------------|----------------------------------------------------------|
//! | `Task<S>`      | membership in the runtime's owned-tasks list (shutdown)  |
//! | `Notified<S>`  | an entry in a run queue; the right to poll the task once |
//! | `JoinHandle<T>`| the right to read the output                              |
//! | `Waker`        | the right to re-schedule the task                         |
//!
//! The lock-free state machine in `task/state.rs` guarantees that a task is in at
//! most one run queue at a time, that only one thread polls it at a time, and
//! that the output is handed to the `JoinHandle` exactly once.

mod core;
mod error;
mod harness;
mod join;
mod owned;
mod raw;
mod state;
mod waker;

pub use self::error::JoinError;
pub use self::join::JoinHandle;
pub(crate) use self::owned::OwnedTasks;

use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use self::raw::RawTask;

/// A unique identifier for a task, stable for the task's lifetime.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Id(u64);

impl Id {
    pub(crate) fn next() -> Id {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Id(NEXT.fetch_add(1, Ordering::Relaxed))
    }

    pub(crate) fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Id({})", self.0)
    }
}

/// The interface between a task and the scheduler that runs it.
pub(crate) trait Schedule: Send + Sync + Sized + 'static {
    /// Pushes a woken task onto a run queue.
    fn schedule(&self, task: Notified<Self>);

    /// Re-queues a task that woke itself while being polled. Defaults to
    /// `schedule`; schedulers override it to bypass LIFO optimisations.
    fn yield_now(&self, task: Notified<Self>) {
        self.schedule(task);
    }

    /// Removes a completed task from the owned-tasks list, returning the
    /// list's reference.
    fn release(&self, task: &Task<Self>) -> Option<Task<Self>>;
}

/// An owned reference to a task (used by the owned-tasks list).
pub(crate) struct Task<S: 'static> {
    raw: RawTask,
    _p: PhantomData<S>,
}

// SAFETY: spawned futures and outputs are `Send`; all shared state of the task
// is synchronised by its atomic state word.
unsafe impl<S> Send for Task<S> {}
// SAFETY: as above.
unsafe impl<S> Sync for Task<S> {}

impl<S: 'static> Task<S> {
    fn from_raw(raw: RawTask) -> Task<S> {
        Task {
            raw,
            _p: PhantomData,
        }
    }

    pub(crate) fn id(&self) -> Id {
        self.raw.id()
    }

    /// Cancels the task as part of runtime shutdown, consuming this
    /// reference.
    pub(crate) fn shutdown(self) {
        let raw = self.raw;
        std::mem::forget(self);
        raw.shutdown();
    }
}

impl<S: 'static> Drop for Task<S> {
    fn drop(&mut self) {
        self.raw.drop_reference();
    }
}

impl<S: 'static> fmt::Debug for Task<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task").field("id", &self.id()).finish()
    }
}

/// A task that is ready to be polled. Exactly one exists per queued task.
pub(crate) struct Notified<S: 'static>(Task<S>);

impl<S: 'static> Notified<S> {
    /// Polls the task once.
    pub(crate) fn run(self) {
        let raw = self.0.raw;
        std::mem::forget(self);
        raw.poll();
    }

    /// Cancels a queued task without polling it.
    pub(crate) fn shutdown(self) {
        let raw = self.0.raw;
        std::mem::forget(self);
        raw.shutdown();
    }

    #[cfg(all(test, not(loom)))]
    pub(crate) fn id(&self) -> Id {
        self.0.id()
    }
}

impl<S: 'static> fmt::Debug for Notified<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Notified").field(&self.0.id()).finish()
    }
}

/// Allocates a new task. Returns the owned-list reference, the initial run
/// queue entry and the join handle.
pub(crate) fn new_task<T, S>(
    future: T,
    scheduler: S,
    id: Id,
) -> (Task<S>, Notified<S>, JoinHandle<T::Output>)
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
    S: Schedule,
{
    let raw = RawTask::new::<T, S>(future, scheduler, id);
    (
        Task::from_raw(raw),
        Notified(Task::from_raw(raw)),
        JoinHandle::new(raw),
    )
}

/// Spawns a future onto the current runtime.
///
/// The future starts running immediately in the background on one of the
/// runtime's worker threads; awaiting the returned [`JoinHandle`] yields its
/// output.
///
/// # Panics
/// Panics if called outside the context of a weft runtime.
///
/// ```
/// let rt = weft::Runtime::new().unwrap();
/// let out = rt.block_on(async {
///     let handle = weft::spawn(async { 6 * 7 });
///     handle.await.unwrap()
/// });
/// assert_eq!(out, 42);
/// ```
#[track_caller]
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match crate::runtime::context::with_current(|handle| handle.spawn_inner(future)) {
        Some(join) => join,
        None => panic!("`weft::spawn` must be called from the context of a weft runtime"),
    }
}

/// Runs a blocking closure on a dedicated thread pool so that it does not
/// stall the async worker threads.
///
/// # Panics
/// Panics if called outside the context of a weft runtime.
#[track_caller]
pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    match crate::runtime::context::with_current(|handle| handle.spawn_blocking(f)) {
        Some(join) => join,
        None => panic!("`weft::spawn_blocking` must be called from the context of a weft runtime"),
    }
}

/// Yields execution back to the scheduler, letting other tasks run before this
/// one is polled again.
pub async fn yield_now() {
    struct YieldNow {
        yielded: bool,
    }

    impl Future for YieldNow {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.yielded {
                return Poll::Ready(());
            }
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    YieldNow { yielded: false }.await
}

#[cfg(all(test, not(loom)))]
mod tests;
