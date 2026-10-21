//! The runtime: worker threads, the I/O/time driver, and the blocking pool.
//!
//! ```
//! let rt = weft::Runtime::builder().worker_threads(2).build().unwrap();
//! let sum = rt.block_on(async {
//!     let handles: Vec<_> = (0..10u64).map(|i| weft::spawn(async move { i * i })).collect();
//!     let mut sum = 0;
//!     for h in handles {
//!         sum += h.await.unwrap();
//!     }
//!     sum
//! });
//! assert_eq!(sum, 285);
//! ```

pub(crate) mod blocking;
pub(crate) mod context;
pub(crate) mod scheduler;

use std::fmt;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

use self::blocking::BlockingPool;
use self::scheduler::{Config, Shared};
use crate::driver::Driver;
use crate::task::{self, Id, JoinHandle};

/// Configures and builds a [`Runtime`].
#[derive(Debug, Clone)]
pub struct Builder {
    worker_threads: Option<usize>,
    max_blocking_threads: usize,
    thread_name: String,
    thread_stack_size: Option<usize>,
    event_interval: u32,
    global_queue_interval: u32,
    lifo_slot: bool,
}

impl Default for Builder {
    fn default() -> Self {
        Builder::new()
    }
}

impl Builder {
    /// Creates a builder with default settings.
    pub fn new() -> Builder {
        Builder {
            worker_threads: None,
            max_blocking_threads: 512,
            thread_name: "weft".to_string(),
            thread_stack_size: None,
            event_interval: 61,
            global_queue_interval: 31,
            lifo_slot: true,
        }
    }

    /// Number of worker threads. Defaults to the number of CPUs.
    pub fn worker_threads(&mut self, n: usize) -> &mut Self {
        assert!(n > 0, "worker_threads must be at least 1");
        self.worker_threads = Some(n);
        self
    }

    /// Upper bound on threads used by [`spawn_blocking`](crate::spawn_blocking).
    pub fn max_blocking_threads(&mut self, n: usize) -> &mut Self {
        self.max_blocking_threads = n.max(1);
        self
    }

    /// Prefix for thread names.
    pub fn thread_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.thread_name = name.into();
        self
    }

    /// Stack size of worker and blocking threads.
    pub fn thread_stack_size(&mut self, size: usize) -> &mut Self {
        self.thread_stack_size = Some(size);
        self
    }

    /// How many scheduler ticks pass between non-blocking polls of the I/O
    /// driver by a busy worker. Default 61.
    pub fn event_interval(&mut self, ticks: u32) -> &mut Self {
        self.event_interval = ticks.max(1);
        self
    }

    /// How many scheduler ticks pass between forced checks of the global
    /// queue. Default 31.
    pub fn global_queue_interval(&mut self, ticks: u32) -> &mut Self {
        self.global_queue_interval = ticks.max(1);
        self
    }

    /// Disables the LIFO slot optimisation (useful for benchmarking it).
    pub fn disable_lifo_slot(&mut self) -> &mut Self {
        self.lifo_slot = false;
        self
    }

    /// Builds the runtime and starts its worker threads.
    pub fn build(&self) -> io::Result<Runtime> {
        let num_workers = self
            .worker_threads
            .unwrap_or_else(|| thread::available_parallelism().map_or(4, |n| n.get()));
        let (driver, driver_handle) = Driver::new()?;
        let blocking = BlockingPool::new(
            self.max_blocking_threads,
            format!("{}-blocking", self.thread_name),
        );
        let config = Config {
            event_interval: self.event_interval,
            global_queue_interval: self.global_queue_interval,
            lifo_enabled: self.lifo_slot,
        };
        let (shared, cores) = Shared::new(num_workers, driver, driver_handle, blocking, config);
        let mut runtime = Runtime {
            handle: Handle { shared },
            workers: Vec::with_capacity(num_workers),
        };
        for (index, core) in cores.into_iter().enumerate() {
            let shared = runtime.handle.shared.clone();
            let mut builder =
                thread::Builder::new().name(format!("{}-worker-{index}", self.thread_name));
            if let Some(size) = self.thread_stack_size {
                builder = builder.stack_size(size);
            }
            // On failure, dropping `runtime` shuts down the workers started
            // so far.
            let worker = builder.spawn(move || scheduler::worker_run(shared, core))?;
            runtime.workers.push(worker);
        }
        Ok(runtime)
    }
}

/// A handle to a running runtime. Cheap to clone; can be sent to other
/// threads to spawn tasks onto the runtime.
#[derive(Clone)]
pub struct Handle {
    pub(crate) shared: Arc<Shared>,
}

impl Handle {
    /// Returns the handle of the runtime this thread is running in.
    ///
    /// # Panics
    /// Panics if called outside a runtime.
    #[track_caller]
    pub fn current() -> Handle {
        Handle::try_current()
            .expect("`Handle::current` must be called from the context of a weft runtime")
    }

    /// Returns the current runtime's handle, if any.
    pub fn try_current() -> Option<Handle> {
        context::with_current(Handle::clone)
    }

    /// Spawns a future onto this runtime.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawn_inner(future)
    }

    pub(crate) fn spawn_inner<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (task, notified, join) = task::new_task(future, self.shared.clone(), Id::next());
        match self.shared.owned.bind(task) {
            Ok(()) => crate::task::Schedule::schedule(&self.shared, notified),
            Err(task) => {
                // The runtime is shutting down: cancel immediately.
                drop(notified);
                task.shutdown();
            }
        }
        join
    }

    /// Runs `f` on the blocking thread pool.
    pub fn spawn_blocking<F, R>(&self, f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.shared.blocking.spawn(self, f)
    }

    /// Number of worker threads.
    pub fn num_workers(&self) -> usize {
        self.shared.num_workers()
    }

    /// Number of tasks that have been spawned and not yet completed.
    pub fn num_alive_tasks(&self) -> usize {
        self.shared.owned.len()
    }
}

impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handle")
            .field("workers", &self.num_workers())
            .finish_non_exhaustive()
    }
}

/// A multi-threaded, work-stealing async runtime.
///
/// Dropping the runtime shuts it down: worker threads are stopped and joined,
/// and every task that has not completed is cancelled (its future is dropped).
pub struct Runtime {
    handle: Handle,
    workers: Vec<thread::JoinHandle<()>>,
}

impl Runtime {
    /// Creates a runtime with default settings.
    pub fn new() -> io::Result<Runtime> {
        Builder::new().build()
    }

    /// Returns a [`Builder`].
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// The runtime's handle.
    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    /// Spawns a future onto the runtime.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    /// Runs a future to completion on the current thread, with this runtime
    /// as the context (so it can spawn tasks, use timers and do I/O). The
    /// future itself does not need to be `Send`.
    ///
    /// # Panics
    /// Panics if called from a runtime worker thread, where blocking would
    /// stall the scheduler.
    #[track_caller]
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        if context::is_worker_thread() {
            panic!("cannot call `block_on` from within a runtime worker thread");
        }
        let _enter = context::enter(self.handle.clone());
        block_on(future)
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        let shared = &self.handle.shared;
        shared.shutdown();
        if !context::is_worker_thread() {
            for worker in self.workers.drain(..) {
                let _ = worker.join();
            }
        }
        shared.shutdown_tasks();
    }
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("handle", &self.handle)
            .finish_non_exhaustive()
    }
}

/// Wakes a thread blocked in [`block_on`].
struct ThreadWaker {
    thread: thread::Thread,
    notified: AtomicBool,
}

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if !self.notified.swap(true, Ordering::Release) {
            self.thread.unpark();
        }
    }
}

/// Drives a future on the current thread, parking between polls.
fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let thread_waker = Arc::new(ThreadWaker {
        thread: thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = Waker::from(thread_waker.clone());
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(out) = future.as_mut().poll(&mut cx) {
            return out;
        }
        // `thread::park` may return spuriously; only a wake clears the loop.
        while !thread_waker.notified.swap(false, Ordering::Acquire) {
            thread::park();
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests;
