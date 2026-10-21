//! Per-resource readiness tracking.
//!
//! Each registered file descriptor has a [`ScheduledIo`] holding:
//!
//! * a readiness word: ready bits in the low byte, a *tick* counter above,
//!   incremented on every event delivered by the driver;
//! * one reader waker and one writer waker (so a socket can be read by one
//!   task and written by another concurrently).
//!
//! Because the poller is edge-triggered, readiness is cleared by the *task*
//! when an operation returns `EWOULDBLOCK`. The tick prevents a lost wake-up
//! in this race:
//!
//! ```text
//! task: read() -> EWOULDBLOCK
//!                                  driver: new data arrives, sets READABLE
//! task: clear READABLE             <- would erase the new edge!
//! ```
//!
//! The task only clears the bits if the tick still matches the one it
//! observed before attempting the operation.

use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use super::{Handle, shutdown_error};
use crate::sys::{Event, Interest};
use crate::util::slab::Key;

const READY_MASK: usize = 0xFF;
const SHUTDOWN: usize = 1 << 7;
const TICK_SHIFT: u32 = 8;
const TICK_MASK: usize = usize::MAX >> TICK_SHIFT;

/// A set of readiness flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Ready(usize);

impl Ready {
    pub(crate) const READABLE: Ready = Ready(1);
    pub(crate) const WRITABLE: Ready = Ready(1 << 1);
    pub(crate) const READ_CLOSED: Ready = Ready(1 << 2);
    pub(crate) const WRITE_CLOSED: Ready = Ready(1 << 3);
    pub(crate) const ERROR: Ready = Ready(1 << 4);
    const ALL: Ready = Ready(0x1F);

    pub(crate) fn from_event(event: &Event) -> Ready {
        let mut r = 0;
        if event.readable {
            r |= Ready::READABLE.0;
        }
        if event.writable {
            r |= Ready::WRITABLE.0;
        }
        if event.read_closed {
            r |= Ready::READ_CLOSED.0;
        }
        if event.write_closed {
            r |= Ready::WRITE_CLOSED.0;
        }
        if event.error {
            r |= Ready::ERROR.0;
        }
        Ready(r)
    }

    pub(crate) fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub(crate) fn intersects(self, other: Ready) -> bool {
        self.0 & other.0 != 0
    }

    fn intersection(self, other: Ready) -> Ready {
        Ready(self.0 & other.0)
    }
}

/// Which half of a resource an operation uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    Read,
    Write,
}

impl Direction {
    /// Readiness bits that should wake a task waiting in this direction. An
    /// error or hang-up must wake both so the operation can observe it.
    fn mask(self) -> Ready {
        match self {
            Direction::Read => Ready(Ready::READABLE.0 | Ready::READ_CLOSED.0 | Ready::ERROR.0),
            Direction::Write => Ready(Ready::WRITABLE.0 | Ready::WRITE_CLOSED.0 | Ready::ERROR.0),
        }
    }
}

/// Readiness observed by a task, tagged with the driver tick.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReadyEvent {
    tick: usize,
    ready: Ready,
}

#[derive(Default)]
struct Waiters {
    reader: Option<Waker>,
    writer: Option<Waker>,
}

pub(crate) struct ScheduledIo {
    readiness: AtomicUsize,
    waiters: Mutex<Waiters>,
}

impl ScheduledIo {
    pub(crate) fn new() -> ScheduledIo {
        ScheduledIo {
            readiness: AtomicUsize::new(0),
            waiters: Mutex::new(Waiters::default()),
        }
    }

    /// Called by the driver for each event.
    pub(crate) fn set_readiness(&self, ready: Ready) {
        let mut curr = self.readiness.load(Ordering::Acquire);
        loop {
            let tick = ((curr >> TICK_SHIFT) + 1) & TICK_MASK;
            let next = (tick << TICK_SHIFT) | (curr & READY_MASK) | ready.0;
            match self.readiness.compare_exchange_weak(
                curr,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => curr = actual,
            }
        }
    }

    /// Clears the readiness observed in `event`, unless a newer event has
    /// arrived since.
    pub(crate) fn clear_readiness(&self, event: ReadyEvent) {
        let mut curr = self.readiness.load(Ordering::Acquire);
        loop {
            if (curr >> TICK_SHIFT) & TICK_MASK != event.tick {
                return;
            }
            let next = curr & !event.ready.0;
            match self.readiness.compare_exchange_weak(
                curr,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => curr = actual,
            }
        }
    }

    /// Moves the wakers interested in `ready` into `out`.
    pub(crate) fn take_wakers(&self, ready: Ready, out: &mut Vec<Waker>) {
        let mut waiters = self.waiters.lock().unwrap_or_else(|e| e.into_inner());
        if ready.intersects(Direction::Read.mask()) {
            out.extend(waiters.reader.take());
        }
        if ready.intersects(Direction::Write.mask()) {
            out.extend(waiters.writer.take());
        }
    }

    fn snapshot(&self, direction: Direction) -> Result<Option<ReadyEvent>, ()> {
        let curr = self.readiness.load(Ordering::Acquire);
        if curr & SHUTDOWN != 0 {
            return Err(());
        }
        let ready = Ready(curr & READY_MASK).intersection(direction.mask());
        if ready.is_empty() {
            Ok(None)
        } else {
            Ok(Some(ReadyEvent {
                tick: (curr >> TICK_SHIFT) & TICK_MASK,
                ready,
            }))
        }
    }

    /// Returns the current readiness for `direction`, or registers the task's
    /// waker and returns `Pending`.
    pub(crate) fn poll_readiness(
        &self,
        cx: &mut Context<'_>,
        direction: Direction,
    ) -> Poll<io::Result<ReadyEvent>> {
        match self.snapshot(direction) {
            Err(()) => return Poll::Ready(Err(shutdown_error())),
            Ok(Some(ev)) => return Poll::Ready(Ok(ev)),
            Ok(None) => {}
        }
        let mut waiters = self.waiters.lock().unwrap_or_else(|e| e.into_inner());
        let slot = match direction {
            Direction::Read => &mut waiters.reader,
            Direction::Write => &mut waiters.writer,
        };
        let old = match slot {
            Some(existing) if existing.will_wake(cx.waker()) => None,
            _ => slot.replace(cx.waker().clone()),
        };
        drop(waiters);
        // Dropped outside the lock (it may be the last reference to a task).
        drop(old);
        // Re-check after registering: the driver sets readiness *before*
        // taking the waiters lock, so either it saw our waker or we see its
        // readiness.
        match self.snapshot(direction) {
            Err(()) => Poll::Ready(Err(shutdown_error())),
            Ok(Some(ev)) => Poll::Ready(Ok(ev)),
            Ok(None) => Poll::Pending,
        }
    }

    /// Marks the resource as shut down and wakes both directions.
    pub(crate) fn shutdown(&self) {
        self.readiness
            .fetch_or(SHUTDOWN | Ready::ALL.0, Ordering::AcqRel);
        let mut wakers = Vec::new();
        self.take_wakers(Ready::ALL, &mut wakers);
        wakers.into_iter().for_each(Waker::wake);
    }
}

/// A registration of one file descriptor with the driver.
pub(crate) struct Registration {
    handle: Handle,
    key: Key,
    shared: Arc<ScheduledIo>,
}

impl Registration {
    /// Registers `fd` with the current runtime's driver.
    ///
    /// # Panics
    /// Panics if called outside a weft runtime.
    #[track_caller]
    pub(crate) fn new(fd: RawFd, interest: Interest) -> io::Result<Registration> {
        let handle = crate::runtime::context::driver_handle();
        let (key, shared) = handle.add_source(fd, interest)?;
        Ok(Registration {
            handle,
            key,
            shared,
        })
    }

    pub(crate) fn poll_ready(
        &self,
        cx: &mut Context<'_>,
        direction: Direction,
    ) -> Poll<io::Result<ReadyEvent>> {
        self.shared.poll_readiness(cx, direction)
    }

    pub(crate) fn clear_readiness(&self, event: ReadyEvent) {
        self.shared.clear_readiness(event);
    }

    /// Attempts a non-blocking operation, waiting for readiness and retrying
    /// on `WouldBlock`.
    pub(crate) fn poll_io<R>(
        &self,
        cx: &mut Context<'_>,
        direction: Direction,
        mut op: impl FnMut() -> io::Result<R>,
    ) -> Poll<io::Result<R>> {
        loop {
            let event = match self.poll_ready(cx, direction) {
                Poll::Ready(Ok(ev)) => ev,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };
            match op() {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => self.clear_readiness(event),
                res => return Poll::Ready(res),
            }
        }
    }

    pub(crate) fn deregister(&self, fd: RawFd) -> io::Result<()> {
        self.handle.deregister_source(fd)
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.handle.remove_source(self.key);
    }
}

/// An I/O object registered with the driver. Deregisters before the
/// underlying descriptor is closed.
pub(crate) struct PollEvented<E: AsRawFd> {
    io: Option<E>,
    registration: Registration,
}

impl<E: AsRawFd> PollEvented<E> {
    #[track_caller]
    pub(crate) fn new(io: E, interest: Interest) -> io::Result<PollEvented<E>> {
        let registration = Registration::new(io.as_raw_fd(), interest)?;
        Ok(PollEvented {
            io: Some(io),
            registration,
        })
    }

    pub(crate) fn get_ref(&self) -> &E {
        self.io.as_ref().expect("I/O object already taken")
    }

    pub(crate) fn registration(&self) -> &Registration {
        &self.registration
    }

    /// Deregisters and returns the inner I/O object.
    pub(crate) fn into_inner(mut self) -> io::Result<E> {
        let io = self.io.take().expect("I/O object already taken");
        self.registration.deregister(io.as_raw_fd())?;
        Ok(io)
    }
}

impl<E: AsRawFd> Drop for PollEvented<E> {
    fn drop(&mut self) {
        if let Some(io) = self.io.take() {
            // Deregister explicitly before the fd is closed: with epoll, a
            // duplicated descriptor would otherwise keep delivering events.
            let _ = self.registration.deregister(io.as_raw_fd());
            drop(io);
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::task::Wake;

    struct Flag(std::sync::atomic::AtomicBool);
    impl Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn event(readable: bool, writable: bool) -> Event {
        Event {
            token: crate::sys::Token(0),
            readable,
            writable,
            read_closed: false,
            write_closed: false,
            error: false,
        }
    }

    #[test]
    fn readiness_then_clear() {
        let io = ScheduledIo::new();
        let flag = Arc::new(Flag(Default::default()));
        let waker = Waker::from(flag.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(io.poll_readiness(&mut cx, Direction::Read).is_pending());

        let ready = Ready::from_event(&event(true, false));
        io.set_readiness(ready);
        let mut wakers = vec![];
        io.take_wakers(ready, &mut wakers);
        assert_eq!(wakers.len(), 1);
        wakers.into_iter().for_each(Waker::wake);
        assert!(flag.0.load(Ordering::SeqCst));

        let Poll::Ready(Ok(ev)) = io.poll_readiness(&mut cx, Direction::Read) else {
            panic!("expected readiness");
        };
        // Write direction is unaffected.
        assert!(io.poll_readiness(&mut cx, Direction::Write).is_pending());
        io.clear_readiness(ev);
        assert!(io.poll_readiness(&mut cx, Direction::Read).is_pending());
    }

    #[test]
    fn stale_clear_does_not_erase_new_event() {
        let io = ScheduledIo::new();
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        io.set_readiness(Ready::READABLE);
        let Poll::Ready(Ok(ev)) = io.poll_readiness(&mut cx, Direction::Read) else {
            panic!()
        };
        // A new edge arrives between the failed read and the clear.
        io.set_readiness(Ready::READABLE);
        io.clear_readiness(ev);
        assert!(io.poll_readiness(&mut cx, Direction::Read).is_ready());
    }

    #[test]
    fn shutdown_errors_both_directions() {
        let io = ScheduledIo::new();
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        io.shutdown();
        assert!(matches!(
            io.poll_readiness(&mut cx, Direction::Read),
            Poll::Ready(Err(_))
        ));
        assert!(matches!(
            io.poll_readiness(&mut cx, Direction::Write),
            Poll::Ready(Err(_))
        ));
    }
}
