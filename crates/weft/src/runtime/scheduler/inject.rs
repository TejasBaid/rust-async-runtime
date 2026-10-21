//! The global injection queue.
//!
//! Receives tasks spawned or woken from outside the worker threads, and the
//! overflow of full local queues. It is a mutex-protected `VecDeque` with an
//! atomic length so that the common "is there anything?" check is lock-free.
//! Workers take tasks from it in batches (see `pop_batch_into`) to amortise
//! the lock, so the mutex is rarely contended in practice.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::queue::{Local, Overflow};

pub(crate) struct Inject<T> {
    state: Mutex<State<T>>,
    len: AtomicUsize,
}

struct State<T> {
    queue: VecDeque<T>,
    closed: bool,
}

impl<T> Inject<T> {
    pub(crate) fn new() -> Inject<T> {
        Inject {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                closed: false,
            }),
            len: AtomicUsize::new(0),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    /// Pushes a task. Returns it back if the queue has been closed.
    pub(crate) fn try_push(&self, task: T) -> Result<(), T> {
        let mut state = self.lock();
        if state.closed {
            return Err(task);
        }
        state.queue.push_back(task);
        self.len.store(state.queue.len(), Ordering::Release);
        Ok(())
    }

    pub(crate) fn pop(&self) -> Option<T> {
        if self.is_empty() {
            return None;
        }
        let mut state = self.lock();
        let task = state.queue.pop_front();
        self.len.store(state.queue.len(), Ordering::Release);
        task
    }

    /// Pops up to `max` tasks: returns the first and pushes the rest into
    /// `local`, which must have room for them.
    pub(crate) fn pop_batch_into(&self, max: usize, local: &mut Local<T>) -> Option<T> {
        if self.is_empty() || max == 0 {
            return None;
        }
        let mut state = self.lock();
        let n = max.min(state.queue.len());
        let first = state.queue.pop_front()?;
        if n > 1 {
            local.push_back_batch(state.queue.drain(..n - 1));
        }
        self.len.store(state.queue.len(), Ordering::Release);
        Some(first)
    }

    /// Closes the queue and returns everything left in it.
    pub(crate) fn close(&self) -> Vec<T> {
        let mut state = self.lock();
        state.closed = true;
        self.len.store(0, Ordering::Release);
        state.queue.drain(..).collect()
    }
}

impl<T> Overflow<T> for Inject<T> {
    fn push(&self, task: T) {
        // Overflow only happens on worker threads while the runtime is
        // running; if it raced with shutdown the task is simply dropped.
        let _ = self.try_push(task);
    }

    fn push_batch(&self, tasks: impl Iterator<Item = T>) {
        let mut state = self.lock();
        if state.closed {
            drop(state);
            tasks.for_each(drop);
            return;
        }
        state.queue.extend(tasks);
        self.len.store(state.queue.len(), Ordering::Release);
    }
}
