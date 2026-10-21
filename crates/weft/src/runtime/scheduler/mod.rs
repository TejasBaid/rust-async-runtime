//! The multi-threaded work-stealing scheduler.
//!
//! ```text
//!                      spawn / wake from outside a worker
//!                                   │
//!                                   ▼
//!                       ┌──────────────────────┐
//!                       │  global inject queue │◄──── overflow (half a local queue)
//!                       └──────────┬───────────┘
//!            batch pop ┌───────────┼─────────────┐
//!                      ▼           ▼             ▼
//!   worker 0 ┌────┬──────────┐  worker 1 ...  worker N
//!            │LIFO│ local    │◄──── steal half ────┐
//!            └────┴──────────┘                     │
//!              ▲  wake from a task on this worker  │
//!              └───────────────────────────────────┘
//! ```
//!
//! Each worker loops:
//! 1. every `event_interval` ticks, poll the I/O driver without blocking so
//!    that I/O and timers make progress even when all workers are busy;
//! 2. run the LIFO slot task, then local tasks; every
//!    `global_queue_interval` ticks check the global queue first (fairness);
//! 3. if the local queue is empty, refill from the global queue in a batch;
//! 4. otherwise try to steal half of a random victim's queue;
//! 5. otherwise park (in the I/O driver if it is free, else on a condvar).

mod idle;
mod inject;
mod park;
pub(crate) mod queue;
mod worker;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use self::idle::Idle;
use self::inject::Inject;
use self::park::Parker;
use self::queue::{Local, Steal};
use crate::driver::{self, Driver};
use crate::runtime::blocking::BlockingPool;
use crate::task::{self, OwnedTasks, Schedule};

pub(crate) use self::worker::Core;
pub(crate) use self::worker::run as worker_run;

type Notified = task::Notified<Arc<Shared>>;

/// Tunables (see [`Builder`](crate::runtime::Builder)).
#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) event_interval: u32,
    pub(crate) global_queue_interval: u32,
    pub(crate) lifo_enabled: bool,
}

/// Per-worker state reachable from other threads.
struct Remote {
    steal: Steal<Notified>,
    parker: Parker,
}

/// State shared by all workers and handles.
pub(crate) struct Shared {
    remotes: Box<[Remote]>,
    inject: Inject<Notified>,
    idle: Idle,
    pub(crate) owned: OwnedTasks<Arc<Shared>>,
    pub(crate) driver: driver::Handle,
    driver_lock: Mutex<Driver>,
    shutdown: AtomicBool,
    pub(crate) blocking: BlockingPool,
    config: Config,
}

impl Shared {
    /// Creates the shared state plus one `Core` per worker.
    // Cores are boxed because they are moved between the worker loop and the
    // thread-local context on every task poll; moving a pointer is cheaper.
    #[allow(clippy::vec_box)]
    pub(crate) fn new(
        num_workers: usize,
        driver: Driver,
        driver_handle: driver::Handle,
        blocking: BlockingPool,
        config: Config,
    ) -> (Arc<Shared>, Vec<Box<Core>>) {
        let mut remotes = Vec::with_capacity(num_workers);
        let mut locals: Vec<Local<Notified>> = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let (steal, local) = queue::local();
            remotes.push(Remote {
                steal,
                parker: Parker::new(),
            });
            locals.push(local);
        }
        let shared = Arc::new(Shared {
            remotes: remotes.into_boxed_slice(),
            inject: Inject::new(),
            idle: Idle::new(num_workers),
            owned: OwnedTasks::new(num_workers * 4),
            driver: driver_handle,
            driver_lock: Mutex::new(driver),
            shutdown: AtomicBool::new(false),
            blocking,
            config,
        });
        let cores = locals
            .into_iter()
            .enumerate()
            .map(|(index, local)| Core::new(index, local))
            .collect();
        (shared, cores)
    }

    pub(crate) fn num_workers(&self) -> usize {
        self.remotes.len()
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Pushes a task onto the global queue and wakes a worker if needed.
    fn schedule_remote(&self, task: Notified) {
        match self.inject.try_push(task) {
            Ok(()) => self.notify_parked(),
            // Shut down: dropping the Notified releases its reference; the
            // task itself is cancelled through the owned-tasks list.
            Err(task) => drop(task),
        }
    }

    /// Wakes one parked worker if there is reason to.
    fn notify_parked(&self) {
        if let Some(index) = self.idle.worker_to_notify() {
            self.remotes[index].parker.unpark(&self.driver);
        }
    }

    /// Wakes a worker if any queue has work (used by the last searcher
    /// before it parks).
    fn notify_if_work_pending(&self) {
        let pending = !self.inject.is_empty() || self.remotes.iter().any(|r| !r.steal.is_empty());
        if pending {
            self.notify_parked();
        }
    }

    /// Signals all workers to exit.
    pub(crate) fn shutdown(&self) {
        if self.shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        for remote in self.remotes.iter() {
            remote.parker.unpark(&self.driver);
        }
    }

    /// Final cleanup once all worker threads have exited.
    pub(crate) fn shutdown_tasks(&self) {
        self.blocking.shutdown();
        // Queued tasks: dropping their Notified releases the queue reference.
        drop(self.inject.close());
        // Every task still alive is cancelled (future dropped in place).
        self.owned.close_and_shutdown_all();
        // Finally wake anything still registered with the driver.
        self.driver.shutdown();
    }
}

impl Schedule for Arc<Shared> {
    fn schedule(&self, task: Notified) {
        worker::schedule(self, task, false);
    }

    fn yield_now(&self, task: Notified) {
        worker::schedule(self, task, true);
    }

    fn release(&self, task: &task::Task<Self>) -> Option<task::Task<Self>> {
        self.owned.remove(task)
    }
}
