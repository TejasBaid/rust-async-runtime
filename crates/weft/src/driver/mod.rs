//! The I/O and time driver (the "reactor").
//!
//! The driver is split in two:
//!
//! * [`Handle`] (shared, `Arc`): the poller, the registration slab, and the
//!   timer wheel. Any thread can register I/O resources or timers through it.
//! * [`Driver`] (exclusive, behind the scheduler's driver mutex): the event
//!   buffer. Whichever worker holds it may block in the OS poller.
//!
//! One turn of the driver ([`Driver::park`]):
//!
//! 1. compute the poll timeout from the earliest timer deadline,
//! 2. block in `kevent`/`epoll_wait`,
//! 3. for each event, update the resource's readiness and collect wakers,
//! 4. advance the timer wheel and collect expired timers' wakers,
//! 5. wake everything *after* releasing all locks.

pub(crate) mod io;

use std::io as stdio;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::Waker;
use std::time::{Duration, Instant};

use self::io::{Ready, ScheduledIo};
use crate::sys::{DefaultPoller, Events, Interest, Poller, Token};
use crate::time::wheel::{TimerKey, Wheel};
use crate::util::slab::{Key, Slab};

/// Events fetched per poll call.
const EVENTS_CAPACITY: usize = 1024;

/// Shared handle to the driver.
#[derive(Clone)]
pub(crate) struct Handle {
    inner: Arc<Inner>,
}

struct Inner {
    poller: DefaultPoller,
    resources: Mutex<Slab<Arc<ScheduledIo>>>,
    timers: Mutex<Timers>,
    /// Time origin: tick 0.
    start: Instant,
    is_shutdown: AtomicBool,
}

struct Timers {
    wheel: Wheel<Waker>,
    /// Tick until which the driving thread is blocked in the poller, so that
    /// inserting an earlier timer knows it must interrupt it. 0 means "not
    /// parked"; `u64::MAX` means "parked with no timer".
    parked_until: u64,
}

/// The exclusive half of the driver.
pub(crate) struct Driver {
    events: Events,
    wakers: Vec<Waker>,
}

impl Driver {
    pub(crate) fn new() -> stdio::Result<(Driver, Handle)> {
        let poller = DefaultPoller::new()?;
        let handle = Handle {
            inner: Arc::new(Inner {
                poller,
                resources: Mutex::new(Slab::new()),
                timers: Mutex::new(Timers {
                    wheel: Wheel::new(),
                    parked_until: 0,
                }),
                start: Instant::now(),
                is_shutdown: AtomicBool::new(false),
            }),
        };
        let driver = Driver {
            events: Events::with_capacity(EVENTS_CAPACITY),
            wakers: Vec::with_capacity(64),
        };
        Ok((driver, handle))
    }

    /// Runs one turn of the driver, blocking for at most `timeout` (`None`:
    /// until an event, a timer or an explicit unpark).
    pub(crate) fn park(&mut self, handle: &Handle, timeout: Option<Duration>) {
        let inner = &*handle.inner;
        if inner.is_shutdown.load(Ordering::Acquire) {
            return;
        }
        let timeout = handle.prepare_park(timeout);
        if let Err(err) = inner.poller.poll(&mut self.events, timeout) {
            // Only EBADF/EFAULT/EINVAL are possible here, all of which mean
            // the poller itself is broken.
            panic!("I/O driver poll failed: {err}");
        }
        handle.lock_timers().parked_until = 0;

        // Dispatch I/O readiness.
        if !self.events.is_empty() {
            let resources = handle.lock_resources();
            for event in self.events.iter() {
                let key = Key::from_u64(event.token.0);
                if let Some(io) = resources.get(key) {
                    let ready = Ready::from_event(&event);
                    io.set_readiness(ready);
                    io.take_wakers(ready, &mut self.wakers);
                }
            }
        }

        // Fire expired timers.
        {
            let now = handle.now_tick();
            let mut timers = handle.lock_timers();
            let wakers = &mut self.wakers;
            timers.wheel.poll(now, |waker| wakers.push(waker));
        }

        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }
}

impl Handle {
    fn lock_resources(&self) -> MutexGuard<'_, Slab<Arc<ScheduledIo>>> {
        self.inner
            .resources
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    fn lock_timers(&self) -> MutexGuard<'_, Timers> {
        self.inner.timers.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Interrupts a thread blocked in [`Driver::park`].
    pub(crate) fn unpark(&self) {
        // A failing wake-up would mean the kernel object is gone, which can
        // only happen after shutdown; nothing useful can be done about it.
        let _ = self.inner.poller.wake();
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.inner.is_shutdown.load(Ordering::Acquire)
    }

    /// Computes the poll timeout and records how long the driver will sleep.
    fn prepare_park(&self, timeout: Option<Duration>) -> Option<Duration> {
        let mut timers = self.lock_timers();
        let next = timers.wheel.next_expiration().map(|exp| exp.deadline);
        let timer_timeout = next.map(|tick| {
            self.tick_to_instant(tick)
                .saturating_duration_since(Instant::now())
        });
        let timeout = match (timeout, timer_timeout) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        timers.parked_until = match timeout {
            Some(d) if d.is_zero() => 0,
            _ => next.unwrap_or(u64::MAX),
        };
        timeout
    }

    // ===== time =====

    /// Milliseconds since the driver started, rounded down.
    pub(crate) fn now_tick(&self) -> u64 {
        self.inner.start.elapsed().as_millis() as u64
    }

    /// Converts a deadline to a tick, rounding *up* so a timer never fires
    /// before its deadline.
    fn instant_to_tick(&self, t: Instant) -> u64 {
        let d = t.saturating_duration_since(self.inner.start);
        d.as_nanos().div_ceil(1_000_000).min(u64::MAX as u128) as u64
    }

    fn tick_to_instant(&self, tick: u64) -> Instant {
        self.inner
            .start
            .checked_add(Duration::from_millis(tick))
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400 * 365))
    }

    /// Registers a timer. Returns `Err` if the deadline has already passed
    /// according to the wheel (the caller should complete immediately).
    pub(crate) fn insert_timer(&self, deadline: Instant, waker: Waker) -> Result<TimerKey, Waker> {
        let tick = self.instant_to_tick(deadline);
        let mut timers = self.lock_timers();
        if self.is_shutdown() {
            return Err(waker);
        }
        let key = timers.wheel.insert(tick, waker)?;
        // Decided under the timers lock: `prepare_park` records
        // `parked_until` under the same lock, so either it sees our timer or
        // we see its deadline.
        let must_wake = tick < timers.parked_until;
        drop(timers);
        if must_wake {
            self.unpark();
        }
        Ok(key)
    }

    /// Updates the waker of a pending timer. Returns `false` if the timer has
    /// already fired.
    pub(crate) fn update_timer(&self, key: TimerKey, waker: &Waker) -> bool {
        let mut timers = self.lock_timers();
        let old = match timers.wheel.get_mut(key) {
            Some(existing) if existing.will_wake(waker) => return true,
            Some(existing) => std::mem::replace(existing, waker.clone()),
            None => return false,
        };
        // Never drop a waker while holding a lock: dropping the last
        // reference to a foreign task could re-enter the driver.
        drop(timers);
        drop(old);
        true
    }

    pub(crate) fn remove_timer(&self, key: TimerKey) {
        let waker = self.lock_timers().wheel.remove(key);
        // Drop the waker outside the lock: dropping a task waker can free the
        // task, which may in turn drop other timers.
        drop(waker);
    }

    // ===== I/O =====

    pub(crate) fn add_source(
        &self,
        fd: RawFd,
        interest: Interest,
    ) -> stdio::Result<(Key, Arc<ScheduledIo>)> {
        if self.is_shutdown() {
            return Err(shutdown_error());
        }
        let io = Arc::new(ScheduledIo::new());
        let key = self.lock_resources().insert(io.clone());
        if let Err(err) = self
            .inner
            .poller
            .register(fd, Token(key.to_u64()), interest)
        {
            self.lock_resources().remove(key);
            return Err(err);
        }
        Ok((key, io))
    }

    pub(crate) fn deregister_source(&self, fd: RawFd) -> stdio::Result<()> {
        self.inner.poller.deregister(fd)
    }

    pub(crate) fn remove_source(&self, key: Key) {
        let io = self.lock_resources().remove(key);
        drop(io);
    }

    /// Shuts the driver down: every I/O resource reports an error and every
    /// pending timer is woken.
    pub(crate) fn shutdown(&self) {
        if self.inner.is_shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        let resources = self.lock_resources().drain();
        for io in resources {
            io.shutdown();
        }
        let timers = self.lock_timers().wheel.drain();
        for waker in timers {
            waker.wake();
        }
        self.unpark();
    }

    #[cfg(all(test, not(loom)))]
    pub(crate) fn num_sources(&self) -> usize {
        self.lock_resources().len()
    }

    #[cfg(all(test, not(loom)))]
    pub(crate) fn num_timers(&self) -> usize {
        self.lock_timers().wheel.len()
    }
}

pub(crate) fn shutdown_error() -> stdio::Error {
    stdio::Error::other("the weft runtime has been shut down")
}
