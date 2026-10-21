//! The worker thread main loop.

use std::cell::{Cell, RefCell};
use std::ptr;
use std::sync::Arc;
use std::time::Duration;

use super::queue::{LOCAL_QUEUE_CAPACITY, Local};
use super::{Notified, Shared};
use crate::runtime::{Handle, context};
use crate::util::rand::FastRand;

/// After this many consecutive LIFO-slot polls, the LIFO task is moved to the
/// back of the local queue so that two tasks pinging each other cannot starve
/// the rest of the queue.
const MAX_LIFO_POLLS_PER_TICK: usize = 3;

/// The worker-owned part of the scheduler. It moves between the worker loop
/// (a `Box<Core>` on the stack) and the thread-local context (while a task is
/// being polled, so that wake-ups from that task can push to the local queue
/// without synchronisation).
pub(crate) struct Core {
    index: usize,
    local: Local<Notified>,
    /// The most recently woken task. Running it next exploits cache locality
    /// in message-passing patterns (task A sends to B, B runs immediately on
    /// the same core while the message is still hot).
    lifo: Option<Notified>,
    tick: u32,
    is_searching: bool,
    rand: FastRand,
}

impl Core {
    pub(super) fn new(index: usize, local: Local<Notified>) -> Box<Core> {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        Box::new(Core {
            index,
            local,
            lifo: None,
            tick: 0,
            is_searching: false,
            rand: FastRand::new(seed ^ (index as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15)),
        })
    }
}

/// Per-thread worker context, referenced from a thread-local while the
/// worker runs.
struct Context {
    shared: Arc<Shared>,
    index: usize,
    core: RefCell<Option<Box<Core>>>,
}

thread_local! {
    static CURRENT: Cell<*const Context> = const { Cell::new(ptr::null()) };
}

/// Entry point of a worker thread.
pub(crate) fn run(shared: Arc<Shared>, core: Box<Core>) {
    let cx = Context {
        index: core.index,
        shared,
        core: RefCell::new(None),
    };
    let _worker = context::enter_worker();
    let _enter = context::enter(Handle {
        shared: cx.shared.clone(),
    });

    struct ResetCurrent;
    impl Drop for ResetCurrent {
        fn drop(&mut self) {
            let _ = CURRENT.try_with(|c| c.set(ptr::null()));
        }
    }
    CURRENT.with(|c| c.set(&cx));
    let _reset = ResetCurrent;

    let mut core = cx.run(core);
    // Drop queued tasks' run-queue references; the tasks themselves are
    // cancelled by `Shared::shutdown_tasks`.
    drop(core.lifo.take());
    while let Some(task) = core.local.pop() {
        drop(task);
    }
}

/// Schedules a task, using the current worker's local queue when called from
/// one of this runtime's workers.
pub(super) fn schedule(shared: &Arc<Shared>, task: Notified, is_yield: bool) {
    let mut task = Some(task);
    let _ = CURRENT.try_with(|current| {
        let ptr = current.get();
        if ptr.is_null() {
            return;
        }
        // SAFETY: `CURRENT` is non-null only between `CURRENT.set(&cx)` and
        // `ResetCurrent::drop` in `run`, during which `cx` is alive on this
        // thread's stack. The pointer is never shared with other threads.
        let cx = unsafe { &*ptr };
        if !Arc::ptr_eq(&cx.shared, shared) {
            return;
        }
        // The core is absent while the worker loop itself holds it (e.g.
        // while stealing); fall back to the global queue then.
        let Ok(mut core) = cx.core.try_borrow_mut() else {
            return;
        };
        if let Some(core) = core.as_mut() {
            schedule_local(shared, core, task.take().expect("task present"), is_yield);
        }
    });
    if let Some(task) = task {
        shared.schedule_remote(task);
    }
}

fn schedule_local(shared: &Shared, core: &mut Core, task: Notified, is_yield: bool) {
    let should_notify = if is_yield || !shared.config.lifo_enabled {
        core.local.push_back_or_overflow(task, &shared.inject);
        true
    } else {
        match core.lifo.replace(task) {
            Some(prev) => {
                core.local.push_back_or_overflow(prev, &shared.inject);
                true
            }
            // Only the LIFO slot changed, which other workers cannot steal
            // from, so there is no point waking one.
            None => false,
        }
    };
    if should_notify {
        shared.notify_parked();
    }
}

impl Context {
    fn run(&self, mut core: Box<Core>) -> Box<Core> {
        let shared = &*self.shared;
        while !shared.is_shutdown() {
            core.tick = core.tick.wrapping_add(1);
            if core.tick % shared.config.event_interval == 0 {
                core = self.maintenance(core);
            }
            if let Some(task) = self.next_task(&mut core) {
                core = self.run_task(task, core);
                continue;
            }
            if let Some(task) = self.steal_work(&mut core) {
                core = self.run_task(task, core);
                continue;
            }
            core = self.park(core);
        }
        core
    }

    fn next_task(&self, core: &mut Core) -> Option<Notified> {
        let shared = &*self.shared;
        // Periodically prefer the global queue so it cannot be starved by
        // tasks that keep re-scheduling each other locally.
        if core.tick % shared.config.global_queue_interval == 0 {
            if let Some(task) = shared.inject.pop() {
                return Some(task);
            }
        }
        if let Some(task) = core.lifo.take() {
            return Some(task);
        }
        if let Some(task) = core.local.pop() {
            return Some(task);
        }
        self.refill_from_inject(core)
    }

    /// Takes a fair share of the global queue into the local queue.
    fn refill_from_inject(&self, core: &mut Core) -> Option<Notified> {
        let shared = &*self.shared;
        if shared.inject.is_empty() {
            return None;
        }
        let n = (shared.inject.len() / shared.num_workers() + 1)
            .min(core.local.remaining_slots())
            .min(LOCAL_QUEUE_CAPACITY / 2);
        shared.inject.pop_batch_into(n, &mut core.local)
    }

    fn steal_work(&self, core: &mut Core) -> Option<Notified> {
        let shared = &*self.shared;
        if !core.is_searching {
            if !shared.idle.transition_worker_to_searching() {
                return None;
            }
            core.is_searching = true;
        }
        let n = shared.remotes.len();
        let start = core.rand.below(n);
        for i in 0..n {
            let victim = (start + i) % n;
            if victim == core.index {
                continue;
            }
            if let Some(task) = shared.remotes[victim].steal.steal_into(&mut core.local) {
                return Some(task);
            }
        }
        // Work may have been injected while we were stealing.
        self.refill_from_inject(core)
    }

    fn transition_from_searching(&self, core: &mut Core) {
        if !core.is_searching {
            return;
        }
        core.is_searching = false;
        // The last searcher to find work wakes another worker to keep
        // looking: work tends to arrive in bursts.
        if self.shared.idle.transition_worker_from_searching() {
            self.shared.notify_parked();
        }
    }

    fn run_task(&self, task: Notified, mut core: Box<Core>) -> Box<Core> {
        self.transition_from_searching(&mut core);
        *self.core.borrow_mut() = Some(core);
        task.run();

        let mut lifo_polls = 0;
        loop {
            let mut core = self
                .core
                .borrow_mut()
                .take()
                .expect("worker core missing after poll");
            let Some(task) = core.lifo.take() else {
                return core;
            };
            if lifo_polls >= MAX_LIFO_POLLS_PER_TICK || self.shared.is_shutdown() {
                core.local.push_back_or_overflow(task, &self.shared.inject);
                self.shared.notify_parked();
                return core;
            }
            lifo_polls += 1;
            *self.core.borrow_mut() = Some(core);
            task.run();
        }
    }

    /// Polls the driver without blocking, if no other worker is using it.
    fn maintenance(&self, core: Box<Core>) -> Box<Core> {
        let mut driver = match self.shared.driver_lock.try_lock() {
            Ok(driver) => driver,
            Err(std::sync::TryLockError::Poisoned(p)) => p.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return core,
        };
        // Install the core so that tasks woken by the driver go to our local
        // queue rather than the global one.
        *self.core.borrow_mut() = Some(core);
        driver.park(&self.shared.driver, Some(Duration::ZERO));
        drop(driver);
        self.core
            .borrow_mut()
            .take()
            .expect("worker core missing after maintenance")
    }

    fn park(&self, mut core: Box<Core>) -> Box<Core> {
        let shared = &*self.shared;
        let was_searching = std::mem::replace(&mut core.is_searching, false);
        if shared
            .idle
            .transition_worker_to_parked(core.index, was_searching)
        {
            // We were the last searcher: nobody else will notice work that
            // arrived while we were giving up.
            shared.notify_if_work_pending();
        } else if !shared.inject.is_empty() {
            shared.notify_parked();
        }

        *self.core.borrow_mut() = Some(core);
        shared.remotes[self.index]
            .parker
            .park(&shared.driver_lock, &shared.driver);
        let mut core = self
            .core
            .borrow_mut()
            .take()
            .expect("worker core missing after park");

        if !shared.idle.unpark_worker_by_id(core.index) {
            // A notifier removed us from the sleepers and counted us as
            // searching.
            core.is_searching = true;
        }
        core
    }
}
