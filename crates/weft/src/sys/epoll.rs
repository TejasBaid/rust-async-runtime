//! epoll(7) backend for Linux and Android.
//!
//! Descriptors are registered with `EPOLLET` (edge-triggered) for both
//! directions at once. Cross-thread wake-ups go through an `eventfd`
//! registered under a reserved token (`u64::MAX`).

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::time::Duration;

use super::{Event, Events, Interest, Poller, Token, WAKE_TOKEN, cvt};

pub(crate) type RawEvent = libc::epoll_event;

/// The poller type exported as [`DefaultPoller`](super::DefaultPoller).
pub type Selector = Epoll;

/// An epoll instance plus the eventfd used for wake-ups.
#[derive(Debug)]
pub struct Epoll {
    epfd: OwnedFd,
    eventfd: OwnedFd,
}

impl Epoll {
    fn ctl(&self, op: libc::c_int, fd: RawFd, flags: u32, token: Token) -> io::Result<()> {
        let mut ev = libc::epoll_event {
            events: flags,
            u64: token.0,
        };
        // SAFETY: `epfd` is a valid epoll descriptor and `ev` is a valid,
        // initialised epoll_event that outlives the call (EPOLL_CTL_DEL
        // ignores it but older kernels require a non-null pointer).
        cvt(unsafe { libc::epoll_ctl(self.epfd.as_raw_fd(), op, fd, &mut ev) })?;
        Ok(())
    }

    /// Resets the eventfd counter so future wake-ups produce a new edge.
    fn drain_eventfd(&self) {
        let mut buf = 0u64;
        // SAFETY: `buf` is a valid 8-byte buffer, which is exactly the size an
        // eventfd read requires. The fd is non-blocking, so EAGAIN (nothing to
        // drain) is returned instead of blocking; the result is ignored.
        let _ = unsafe {
            libc::read(
                self.eventfd.as_raw_fd(),
                (&mut buf as *mut u64).cast(),
                std::mem::size_of::<u64>(),
            )
        };
    }
}

impl Poller for Epoll {
    fn new() -> io::Result<Self> {
        // SAFETY: epoll_create1 has no pointer arguments.
        let epfd = cvt(unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) })?;
        // SAFETY: freshly created descriptor, owned by nobody else.
        let epfd = unsafe { OwnedFd::from_raw_fd(epfd) };
        // SAFETY: eventfd has no pointer arguments.
        let efd = cvt(unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) })?;
        // SAFETY: freshly created descriptor, owned by nobody else.
        let eventfd = unsafe { OwnedFd::from_raw_fd(efd) };
        let this = Epoll { epfd, eventfd };
        this.ctl(
            libc::EPOLL_CTL_ADD,
            this.eventfd.as_raw_fd(),
            (libc::EPOLLIN | libc::EPOLLET) as u32,
            WAKE_TOKEN,
        )?;
        Ok(this)
    }

    fn register(&self, fd: RawFd, token: Token, interest: Interest) -> io::Result<()> {
        let mut flags = libc::EPOLLET | libc::EPOLLRDHUP;
        if interest.is_readable() {
            flags |= libc::EPOLLIN;
        }
        if interest.is_writable() {
            flags |= libc::EPOLLOUT;
        }
        self.ctl(libc::EPOLL_CTL_ADD, fd, flags as u32, token)
    }

    fn deregister(&self, fd: RawFd) -> io::Result<()> {
        self.ctl(libc::EPOLL_CTL_DEL, fd, 0, Token(0))
    }

    fn poll(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
        // Round sub-millisecond timeouts *up* so that a pending timer never
        // turns into a busy loop of zero-timeout polls.
        let timeout_ms = match timeout {
            None => -1,
            Some(d) => {
                let ms = d.as_nanos().div_ceil(1_000_000);
                ms.min(libc::c_int::MAX as u128) as libc::c_int
            }
        };
        events.raw.clear();
        let cap = events.raw.capacity().min(libc::c_int::MAX as usize) as libc::c_int;
        // SAFETY: the output buffer has room for `cap` events (spare capacity
        // of `events.raw`); the kernel writes at most `cap` entries.
        let n = unsafe {
            libc::epoll_wait(
                self.epfd.as_raw_fd(),
                events.raw.as_mut_ptr(),
                cap,
                timeout_ms,
            )
        };
        if n == -1 {
            let err = io::Error::last_os_error();
            return if err.kind() == io::ErrorKind::Interrupted {
                Ok(())
            } else {
                Err(err)
            };
        }
        // SAFETY: epoll_wait initialised the first `n` (<= cap) entries.
        unsafe { events.raw.set_len(n as usize) };
        if events.raw.iter().any(|ev| ev.u64 == WAKE_TOKEN.0) {
            self.drain_eventfd();
        }
        Ok(())
    }

    fn wake(&self) -> io::Result<()> {
        let one = 1u64;
        // SAFETY: writing 8 bytes from a valid u64 to an eventfd is the
        // documented way to increment its counter.
        let n = unsafe {
            libc::write(
                self.eventfd.as_raw_fd(),
                (&one as *const u64).cast(),
                std::mem::size_of::<u64>(),
            )
        };
        if n == -1 {
            let err = io::Error::last_os_error();
            // EAGAIN means the counter is saturated, i.e. a wake-up is
            // already pending, which is all we need.
            if err.kind() != io::ErrorKind::WouldBlock {
                return Err(err);
            }
        }
        Ok(())
    }
}

/// Converts a native epoll_event into a normalised [`Event`].
pub(crate) fn to_event(ev: &libc::epoll_event) -> Option<Event> {
    // Copy the fields out first: `epoll_event` is `repr(packed)` on x86_64,
    // so taking references to its fields is not allowed.
    let data = ev.u64;
    let flags = ev.events as libc::c_int;
    if data == WAKE_TOKEN.0 {
        return None;
    }
    let hup = flags & libc::EPOLLHUP != 0;
    Some(Event {
        token: Token(data),
        readable: flags & (libc::EPOLLIN | libc::EPOLLPRI) != 0,
        writable: flags & libc::EPOLLOUT != 0,
        read_closed: hup || flags & libc::EPOLLRDHUP != 0,
        write_closed: hup,
        error: flags & libc::EPOLLERR != 0,
    })
}
