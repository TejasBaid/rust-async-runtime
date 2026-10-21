//! The set of all live tasks owned by a runtime.
//!
//! Needed for shutdown: an idle task is referenced only by wakers stored in
//! I/O resources, timers or channels, and those can form reference cycles
//! with the runtime itself. On shutdown every remaining task is cancelled
//! (its future dropped), which breaks the cycles.
//!
//! The list is sharded by task id to keep spawn/complete contention low.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use super::Task;

type Shard<S> = Mutex<HashMap<u64, Task<S>>>;

pub(crate) struct OwnedTasks<S: 'static> {
    shards: Box<[Shard<S>]>,
    closed: AtomicBool,
}

impl<S: 'static> OwnedTasks<S> {
    pub(crate) fn new(num_shards: usize) -> OwnedTasks<S> {
        let n = num_shards.next_power_of_two().max(1);
        OwnedTasks {
            shards: (0..n).map(|_| Mutex::new(HashMap::new())).collect(),
            closed: AtomicBool::new(false),
        }
    }

    fn shard(&self, id: u64) -> &Shard<S> {
        &self.shards[id as usize & (self.shards.len() - 1)]
    }

    /// Adds a task. Fails (returning it) once the list has been closed.
    pub(crate) fn bind(&self, task: Task<S>) -> Result<(), Task<S>> {
        let id = task.id().as_u64();
        let mut shard = self.shard(id).lock().unwrap_or_else(|e| e.into_inner());
        // Checked under the shard lock: `close_and_shutdown_all` sets the flag
        // before draining each shard, so a task is either drained or rejected.
        if self.closed.load(Ordering::Acquire) {
            return Err(task);
        }
        shard.insert(id, task);
        Ok(())
    }

    pub(crate) fn remove(&self, task: &Task<S>) -> Option<Task<S>> {
        let id = task.id().as_u64();
        self.shard(id)
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id)
    }

    /// Closes the list and cancels every task in it.
    pub(crate) fn close_and_shutdown_all(&self) {
        self.closed.store(true, Ordering::Release);
        for shard in self.shards.iter() {
            // Release the lock before cancelling: completing a task calls
            // `remove`, which locks the shard again.
            let tasks: Vec<Task<S>> = shard
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .drain()
                .map(|(_, t)| t)
                .collect();
            for task in tasks {
                task.shutdown();
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap_or_else(|e| e.into_inner()).len())
            .sum()
    }
}
