//! kqueue(2) backend for macOS and the BSDs.
//!
//! Each registered descriptor gets two kernel filters (`EVFILT_READ` and/or
//! `EVFILT_WRITE`) added with `EV_CLEAR`, which gives edge-triggered
//! semantics. Cross-thread wake-ups use an `EVFILT_USER` event, so no extra
//! file descriptor (pipe) is needed.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::ptr;
use std::time::Duration;

use super::{Event, Events, Interest, Poller, Token, WAKE_TOKEN, cvt};

pub(crate) type RawEvent = libc::kevent;

/// The poller type exported as [`DefaultPoller`](super::DefaultPoller).
pub type Selector = Kqueue;

/// Identifier of the `EVFILT_USER` event used for wake-ups.
const WAKE_IDENT: usize = 0;

/// A kqueue instance.
#[derive(Debug)]
pub struct Kqueue {
    kq: OwnedFd,
}

/// Builds a `kevent` change record.
fn change(ident: usize, filter: i16, flags: u16, fflags: u32, token: Token) -> libc::kevent {
    // SAFETY: `kevent` is a plain-old-data C struct; the all-zero bit pattern
    // is a valid value for every field (integers and a nullable pointer).
    let mut ev: libc::kevent = unsafe { std::mem::zeroed() };
    ev.ident = ident as _;
    ev.filter = filter as _;
    ev.flags = flags as _;
    ev.fflags = fflags as _;
    ev.udata = token.0 as usize as _;
    ev
}

impl Kqueue {
    /// Submits `changes` and checks the per-change receipts.
    ///
    /// Every change must carry `EV_RECEIPT`; the kernel then echoes each one
    /// back with `EV_ERROR` set and `data` holding the errno (0 on success),
    /// which lets us attribute failures to individual filters. Errors listed
    /// in `ignored` are treated as success.
    fn apply(&self, changes: &mut [libc::kevent], ignored: &[i64]) -> io::Result<()> {
        let len = changes.len() as libc::c_int;
        let buf = changes.as_mut_ptr();
        let timeout = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `buf` points to `len` initialised kevent structs that are
        // valid for both reads (changelist) and writes (eventlist); kevent(2)
        // explicitly allows the two lists to alias. The timeout is a valid
        // pointer to a zero timespec, so the call never blocks.
        let n = cvt(unsafe { libc::kevent(self.kq.as_raw_fd(), buf, len, buf, len, &timeout) })?;
        for ev in &changes[..n as usize] {
            let errno = ev.data as i64;
            if ev.flags & libc::EV_ERROR != 0 && errno != 0 && !ignored.contains(&errno) {
                return Err(io::Error::from_raw_os_error(errno as i32));
            }
        }
        Ok(())
    }
}

impl Poller for Kqueue {
    fn new() -> io::Result<Self> {
        // SAFETY: kqueue(2) takes no arguments and returns a new descriptor
        // or -1.
        let fd = cvt(unsafe { libc::kqueue() })?;
        // SAFETY: `fd` was just returned by the kernel and is owned by nobody
        // else, so transferring ownership to `OwnedFd` is sound.
        let kq = unsafe { OwnedFd::from_raw_fd(fd) };
        // SAFETY: `fd` is a valid open descriptor; F_SETFD only toggles flags.
        cvt(unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) })?;
        let this = Kqueue { kq };
        let mut wake = [change(
            WAKE_IDENT,
            libc::EVFILT_USER,
            libc::EV_ADD | libc::EV_CLEAR | libc::EV_RECEIPT,
            0,
            WAKE_TOKEN,
        )];
        this.apply(&mut wake, &[])?;
        Ok(this)
    }

    fn register(&self, fd: RawFd, token: Token, interest: Interest) -> io::Result<()> {
        let flags = libc::EV_ADD | libc::EV_CLEAR | libc::EV_RECEIPT;
        let mut changes = [change(0, 0, 0, 0, token); 2];
        let mut n = 0;
        if interest.is_readable() {
            changes[n] = change(fd as usize, libc::EVFILT_READ, flags, 0, token);
            n += 1;
        }
        if interest.is_writable() {
            changes[n] = change(fd as usize, libc::EVFILT_WRITE, flags, 0, token);
            n += 1;
        }
        // EPIPE is reported when adding EVFILT_WRITE to a pipe whose reader
        // is gone; the registration still succeeds and EOF will be reported.
        self.apply(&mut changes[..n], &[libc::EPIPE as i64])
    }

    fn deregister(&self, fd: RawFd) -> io::Result<()> {
        let flags = libc::EV_DELETE | libc::EV_RECEIPT;
        let mut changes = [
            change(fd as usize, libc::EVFILT_READ, flags, 0, Token(0)),
            change(fd as usize, libc::EVFILT_WRITE, flags, 0, Token(0)),
        ];
        // ENOENT: the filter was never added (e.g. read-only interest).
        self.apply(&mut changes, &[libc::ENOENT as i64])
    }

    fn poll(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
        let ts = timeout.map(|d| libc::timespec {
            tv_sec: d.as_secs().min(libc::time_t::MAX as u64) as libc::time_t,
            tv_nsec: d.subsec_nanos() as libc::c_long,
        });
        let ts_ptr = ts
            .as_ref()
            .map_or(ptr::null(), |t| t as *const libc::timespec);
        events.raw.clear();
        let cap = events.raw.capacity() as libc::c_int;
        // SAFETY: the eventlist points to `cap` slots of spare capacity owned
        // by `events.raw`; the kernel writes at most `cap` entries. The
        // changelist is empty, and `ts_ptr` is null or points to a live
        // timespec on our stack.
        let n = unsafe {
            libc::kevent(
                self.kq.as_raw_fd(),
                ptr::null(),
                0,
                events.raw.as_mut_ptr(),
                cap,
                ts_ptr,
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
        // SAFETY: kevent(2) initialised the first `n` (<= cap) entries.
        unsafe { events.raw.set_len(n as usize) };
        Ok(())
    }

    fn wake(&self) -> io::Result<()> {
        let mut ev = [change(
            WAKE_IDENT,
            libc::EVFILT_USER,
            libc::EV_ADD | libc::EV_RECEIPT,
            libc::NOTE_TRIGGER,
            WAKE_TOKEN,
        )];
        self.apply(&mut ev, &[])
    }
}

/// Converts a native kevent into a normalised [`Event`].
pub(crate) fn to_event(ev: &libc::kevent) -> Option<Event> {
    if ev.filter == libc::EVFILT_USER {
        return None;
    }
    let token = Token(ev.udata as usize as u64);
    let eof = ev.flags & libc::EV_EOF != 0;
    // On EOF, `fflags` carries the pending socket error (if any).
    let error = ev.flags & libc::EV_ERROR != 0 || (eof && ev.fflags != 0);
    let mut event = Event {
        token,
        readable: false,
        writable: false,
        read_closed: false,
        write_closed: false,
        error,
    };
    match ev.filter {
        libc::EVFILT_READ => {
            event.readable = true;
            event.read_closed = eof;
        }
        libc::EVFILT_WRITE => {
            event.writable = true;
            event.write_closed = eof;
        }
        _ => return None,
    }
    Some(event)
}
