//! Operating-system readiness notification backends.
//!
//! The reactor is written against the [`Poller`] trait, which abstracts over
//! the platform's readiness API:
//!
//! * **kqueue** on macOS / iOS / the BSDs (`kqueue::Kqueue`)
//! * **epoll** on Linux and Android (`epoll::Epoll`)
//!
//! Both backends are implemented directly on top of raw system calls exposed
//! by the `libc` crate and register file descriptors in **edge-triggered**
//! mode: an event is reported once per readiness *transition*, and the
//! consumer is responsible for draining the descriptor until it returns
//! `EWOULDBLOCK`. Edge triggering means a descriptor only has to be registered
//! once for its whole lifetime, which keeps the hot path free of
//! re-registration syscalls.

use std::io;
use std::os::fd::RawFd;
use std::time::Duration;

#[cfg(any(target_os = "linux", target_os = "android"))]
pub mod epoll;
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "dragonfly"
))]
pub mod kqueue;

#[cfg(any(target_os = "linux", target_os = "android"))]
use epoll as backend;
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "dragonfly"
))]
use kqueue as backend;

/// The poller used by the runtime on the current platform.
pub type DefaultPoller = backend::Selector;

/// An opaque value associated with a registered file descriptor and handed
/// back with every event for that descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Token(pub u64);

/// Token reserved for the poller's internal wake-up mechanism. Events carrying
/// it are consumed by the backend and never surface through [`Events::iter`].
pub(crate) const WAKE_TOKEN: Token = Token(u64::MAX);

/// The readiness directions a registration is interested in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interest {
    readable: bool,
    writable: bool,
}

impl Interest {
    /// Interest in read readiness.
    pub const READABLE: Interest = Interest {
        readable: true,
        writable: false,
    };
    /// Interest in write readiness.
    pub const WRITABLE: Interest = Interest {
        readable: false,
        writable: true,
    };
    /// Interest in both read and write readiness.
    pub const BOTH: Interest = Interest {
        readable: true,
        writable: true,
    };

    /// Returns `true` if this interest includes read readiness.
    pub fn is_readable(self) -> bool {
        self.readable
    }

    /// Returns `true` if this interest includes write readiness.
    pub fn is_writable(self) -> bool {
        self.writable
    }
}

/// A single readiness event, normalised across backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Event {
    /// Token supplied when the descriptor was registered.
    pub token: Token,
    /// The descriptor became readable.
    pub readable: bool,
    /// The descriptor became writable.
    pub writable: bool,
    /// The peer closed its write half (EOF will be observed by reads).
    pub read_closed: bool,
    /// The descriptor can no longer be written to.
    pub write_closed: bool,
    /// An error condition is pending on the descriptor.
    pub error: bool,
}

/// A reusable buffer of native events filled by [`Poller::poll`].
///
/// The buffer stores the platform's native event structures and converts them
/// lazily, so polling never allocates once the buffer has been created.
pub struct Events {
    pub(crate) raw: Vec<backend::RawEvent>,
}

// SAFETY: the native event structs contain only plain integers, except for
// kqueue's `udata` field which is a `*mut c_void`. We never dereference it: it
// carries a `Token` encoded as an integer, so sending the buffer across
// threads is sound.
unsafe impl Send for Events {}
// SAFETY: see the `Send` impl above; shared access only reads integers.
unsafe impl Sync for Events {}

impl Events {
    /// Creates a buffer that can receive up to `capacity` native events per
    /// call to [`Poller::poll`].
    pub fn with_capacity(capacity: usize) -> Events {
        Events {
            raw: Vec::with_capacity(capacity.max(1)),
        }
    }

    /// Maximum number of native events a single poll can return.
    pub fn capacity(&self) -> usize {
        self.raw.capacity()
    }

    /// Number of native events received by the last poll.
    pub fn len(&self) -> usize {
        self.raw.len()
    }

    /// Returns `true` if the last poll returned no events.
    pub fn is_empty(&self) -> bool {
        self.raw.is_empty()
    }

    /// Discards all buffered events.
    pub fn clear(&mut self) {
        self.raw.clear();
    }

    /// Iterates over the events received by the last poll, skipping the
    /// poller's internal wake-up notifications.
    pub fn iter(&self) -> impl Iterator<Item = Event> + '_ {
        self.raw.iter().filter_map(backend::to_event)
    }
}

impl std::fmt::Debug for Events {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

/// A readiness notification facility (kqueue, epoll, ...).
///
/// All methods take `&self` so that one poller can be shared between the
/// thread blocked in [`Poller::poll`] and the threads registering descriptors
/// or calling [`Poller::wake`]; the kernel objects are themselves thread-safe.
pub trait Poller: Send + Sync + Sized {
    /// Creates a new poller instance.
    fn new() -> io::Result<Self>;

    /// Registers `fd` in edge-triggered mode for the given interest.
    fn register(&self, fd: RawFd, token: Token, interest: Interest) -> io::Result<()>;

    /// Removes `fd` from the interest list.
    fn deregister(&self, fd: RawFd) -> io::Result<()>;

    /// Blocks until at least one event is available, `timeout` elapses, or
    /// [`Poller::wake`] is called. `None` blocks indefinitely.
    ///
    /// `events` is cleared before new events are written into it. An
    /// interrupted wait (`EINTR`) returns `Ok` with no events.
    fn poll(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()>;

    /// Wakes up a thread blocked in [`Poller::poll`]. If no thread is
    /// currently blocked, the next call to `poll` returns immediately.
    fn wake(&self) -> io::Result<()>;
}

/// Converts a `-1` return value from a syscall into the current `errno`.
pub(crate) fn cvt(ret: libc::c_int) -> io::Result<libc::c_int> {
    if ret == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(ret)
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    use std::sync::Arc;
    use std::time::Instant;

    fn poll_tokens(poller: &DefaultPoller, timeout: Duration) -> Vec<Event> {
        let mut events = Events::with_capacity(16);
        poller.poll(&mut events, Some(timeout)).unwrap();
        events.iter().collect()
    }

    #[test]
    fn reports_readable_and_writable() {
        let poller = DefaultPoller::new().unwrap();
        let (mut a, b) = UnixStream::pair().unwrap();
        a.set_nonblocking(true).unwrap();
        b.set_nonblocking(true).unwrap();
        poller
            .register(b.as_raw_fd(), Token(7), Interest::BOTH)
            .unwrap();

        // A fresh socket is immediately writable.
        let events = poll_tokens(&poller, Duration::from_millis(100));
        assert!(events.iter().any(|e| e.token == Token(7) && e.writable));

        a.write_all(b"hello").unwrap();
        let events = poll_tokens(&poller, Duration::from_millis(1000));
        assert!(events.iter().any(|e| e.token == Token(7) && e.readable));

        poller.deregister(b.as_raw_fd()).unwrap();
    }

    #[test]
    fn edge_triggered_reports_once() {
        let poller = DefaultPoller::new().unwrap();
        let (mut a, mut b) = UnixStream::pair().unwrap();
        b.set_nonblocking(true).unwrap();
        poller
            .register(b.as_raw_fd(), Token(1), Interest::READABLE)
            .unwrap();
        a.write_all(b"x").unwrap();
        let first = poll_tokens(&poller, Duration::from_millis(1000));
        assert!(first.iter().any(|e| e.readable));
        // Without draining or new data, an edge-triggered poller stays quiet.
        let second = poll_tokens(&poller, Duration::from_millis(20));
        assert!(second.iter().all(|e| !e.readable));
        let mut buf = [0u8; 8];
        assert_eq!(b.read(&mut buf).unwrap(), 1);
    }

    #[test]
    fn reports_peer_close() {
        let poller = DefaultPoller::new().unwrap();
        let (a, b) = UnixStream::pair().unwrap();
        b.set_nonblocking(true).unwrap();
        poller
            .register(b.as_raw_fd(), Token(3), Interest::READABLE)
            .unwrap();
        drop(a);
        let events = poll_tokens(&poller, Duration::from_millis(1000));
        assert!(events.iter().any(|e| e.token == Token(3) && e.read_closed));
    }

    #[test]
    fn wake_interrupts_poll() {
        let poller = Arc::new(DefaultPoller::new().unwrap());
        let p2 = poller.clone();
        let start = Instant::now();
        let t = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            p2.wake().unwrap();
        });
        let mut events = Events::with_capacity(4);
        poller
            .poll(&mut events, Some(Duration::from_secs(10)))
            .unwrap();
        assert!(start.elapsed() < Duration::from_secs(5));
        assert_eq!(events.iter().count(), 0, "wake events are internal");
        t.join().unwrap();
    }

    #[test]
    fn wake_before_poll_is_not_lost() {
        let poller = DefaultPoller::new().unwrap();
        poller.wake().unwrap();
        let start = Instant::now();
        let mut events = Events::with_capacity(4);
        poller
            .poll(&mut events, Some(Duration::from_secs(10)))
            .unwrap();
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn timeout_elapses() {
        let poller = DefaultPoller::new().unwrap();
        let start = Instant::now();
        let mut events = Events::with_capacity(4);
        poller
            .poll(&mut events, Some(Duration::from_millis(30)))
            .unwrap();
        assert!(start.elapsed() >= Duration::from_millis(25));
        assert!(events.is_empty());
    }
}
