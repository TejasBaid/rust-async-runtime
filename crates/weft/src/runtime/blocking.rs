//! A thread pool for blocking work ([`spawn_blocking`](crate::spawn_blocking)).
//!
//! Blocking closures are wrapped in a future that runs the closure on its
//! first poll and are spawned as regular tasks with a dedicated scheduler, so
//! they reuse the task machinery (and the same `JoinHandle` type). Threads are
//! created on demand up to a limit and exit after an idle keep-alive period.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;

use super::{Handle, context};
use crate::task::{self, Id, JoinHandle, Notified, Schedule, Task};

const KEEP_ALIVE: Duration = Duration::from_secs(10);

/// Scheduler for blocking tasks. A blocking task completes on its first poll,
/// so it is never re-scheduled and never needs an owned-tasks entry.
#[derive(Clone, Copy)]
pub(crate) struct BlockingSchedule;

impl Schedule for BlockingSchedule {
    fn schedule(&self, _task: Notified<Self>) {
        // Unreachable in practice: the task is complete before anyone could
        // wake it. Dropping the Notified is always safe.
    }

    fn release(&self, _task: &Task<Self>) -> Option<Task<Self>> {
        None
    }
}

/// Future adapter running a closure once.
struct BlockingTask<F> {
    func: Option<F>,
}

impl<F> Unpin for BlockingTask<F> {}

impl<F: FnOnce() -> R, R> Future for BlockingTask<F> {
    type Output = R;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<R> {
        let func = self
            .func
            .take()
            .expect("blocking task polled after completion");
        Poll::Ready(func())
    }
}

pub(crate) struct BlockingPool {
    inner: Arc<Inner>,
}

struct Inner {
    state: Mutex<State>,
    condvar: Condvar,
    max_threads: usize,
    thread_name: String,
}

struct State {
    queue: VecDeque<Notified<BlockingSchedule>>,
    num_threads: usize,
    num_idle: usize,
    /// Notifications sent but not yet consumed by a waiting thread.
    num_notify: usize,
    shutdown: bool,
}

impl BlockingPool {
    pub(crate) fn new(max_threads: usize, thread_name: String) -> BlockingPool {
        BlockingPool {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    queue: VecDeque::new(),
                    num_threads: 0,
                    num_idle: 0,
                    num_notify: 0,
                    shutdown: false,
                }),
                condvar: Condvar::new(),
                max_threads: max_threads.max(1),
                thread_name,
            }),
        }
    }

    pub(crate) fn spawn<F, R>(&self, handle: &Handle, func: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let future = BlockingTask { func: Some(func) };
        let (task, notified, join) = task::new_task(future, BlockingSchedule, Id::next());
        // Blocking tasks are not tracked in the owned-tasks list.
        drop(task);

        let mut state = self.inner.lock();
        if state.shutdown {
            drop(state);
            notified.shutdown();
            return join;
        }
        state.queue.push_back(notified);
        if state.num_idle > 0 {
            state.num_idle -= 1;
            state.num_notify += 1;
            self.inner.condvar.notify_one();
        } else if state.num_threads < self.inner.max_threads {
            state.num_threads += 1;
            drop(state);
            let inner = self.inner.clone();
            let handle = handle.clone();
            let spawned = std::thread::Builder::new()
                .name(self.inner.thread_name.clone())
                .spawn(move || inner.run(handle));
            if spawned.is_err() {
                // Could not create a thread: leave the task queued for an
                // existing thread (if any) and undo the accounting.
                self.inner.lock().num_threads -= 1;
            }
        }
        join
    }

    /// Stops accepting work and cancels queued (not yet started) tasks.
    /// Threads running a closure finish it and exit.
    pub(crate) fn shutdown(&self) {
        let queued: Vec<_> = {
            let mut state = self.inner.lock();
            state.shutdown = true;
            state.queue.drain(..).collect()
        };
        self.inner.condvar.notify_all();
        for task in queued {
            task.shutdown();
        }
    }
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn run(&self, handle: Handle) {
        let _enter = context::enter(handle);
        let mut state = self.lock();
        loop {
            if let Some(task) = state.queue.pop_front() {
                drop(state);
                task.run();
                state = self.lock();
                continue;
            }
            if state.shutdown {
                break;
            }
            state.num_idle += 1;
            let (guard, timeout) = self
                .condvar
                .wait_timeout(state, KEEP_ALIVE)
                .unwrap_or_else(|e| e.into_inner());
            state = guard;
            if state.num_notify > 0 {
                // A spawner already moved us out of the idle count.
                state.num_notify -= 1;
            } else {
                state.num_idle -= 1;
                if timeout.timed_out() && state.queue.is_empty() && !state.shutdown {
                    break;
                }
            }
        }
        state.num_threads -= 1;
    }
}
