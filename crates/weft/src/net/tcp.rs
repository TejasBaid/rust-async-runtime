use std::fmt;
use std::future::poll_fn;
use std::io::{self, IoSlice, Read, Write};
use std::mem;
use std::net::{self, Shutdown, SocketAddr, ToSocketAddrs};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::driver::io::{Direction, PollEvented};
use crate::io::{AsyncRead, AsyncWrite};
use crate::sys::{Interest, cvt};

/// A TCP socket server, listening for connections.
///
/// ```no_run
/// use weft::io::{AsyncReadExt, AsyncWriteExt};
/// use weft::net::TcpListener;
///
/// let rt = weft::Runtime::new().unwrap();
/// rt.block_on(async {
///     let listener = TcpListener::bind("127.0.0.1:0").unwrap();
///     loop {
///         let (mut socket, _) = listener.accept().await.unwrap();
///         weft::spawn(async move {
///             let mut buf = [0u8; 1024];
///             while let Ok(n) = socket.read(&mut buf).await {
///                 if n == 0 || socket.write_all(&buf[..n]).await.is_err() {
///                     break;
///                 }
///             }
///         });
///     }
/// });
/// ```
pub struct TcpListener {
    io: PollEvented<net::TcpListener>,
}

impl TcpListener {
    /// Binds to `addr` (use port 0 for an OS-assigned port).
    ///
    /// # Panics
    /// Panics outside a weft runtime.
    #[track_caller]
    pub fn bind(addr: impl ToSocketAddrs) -> io::Result<TcpListener> {
        let listener = net::TcpListener::bind(addr)?;
        TcpListener::from_std(listener)
    }

    /// Wraps a standard library listener (it is switched to non-blocking).
    #[track_caller]
    pub fn from_std(listener: net::TcpListener) -> io::Result<TcpListener> {
        listener.set_nonblocking(true)?;
        Ok(TcpListener {
            io: PollEvented::new(listener, Interest::READABLE)?,
        })
    }

    /// Accepts a new connection.
    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        poll_fn(|cx| self.poll_accept(cx)).await
    }

    /// Polls for a new connection.
    pub fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<(TcpStream, SocketAddr)>> {
        let (stream, addr) = match self
            .io
            .registration()
            .poll_io(cx, Direction::Read, || self.io.get_ref().accept())
        {
            Poll::Ready(Ok(pair)) => pair,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        };
        Poll::Ready(TcpStream::from_std(stream).map(|s| (s, addr)))
    }

    /// The local address the listener is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().local_addr()
    }

    /// Deregisters the listener and returns the standard library type.
    pub fn into_std(self) -> io::Result<net::TcpListener> {
        self.io.into_inner()
    }
}

impl fmt::Debug for TcpListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpListener")
            .field("addr", &self.local_addr().ok())
            .field("fd", &self.io.get_ref().as_raw_fd())
            .finish()
    }
}

/// A TCP connection.
///
/// `&TcpStream` implements [`AsyncRead`] and [`AsyncWrite`], so a stream can
/// be read and written concurrently from two tasks (see
/// [`into_split`](TcpStream::into_split)).
pub struct TcpStream {
    io: PollEvented<net::TcpStream>,
}

impl TcpStream {
    /// Opens a connection to `addr`, trying each resolved address in turn.
    ///
    /// The connect itself is non-blocking (`connect(2)` returning
    /// `EINPROGRESS`, then waiting for writability). Note that name resolution
    /// via `ToSocketAddrs` is blocking; pass a `SocketAddr` to avoid it.
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<TcpStream> {
        let mut last_err = None;
        for addr in addr.to_socket_addrs()? {
            match TcpStream::connect_addr(addr).await {
                Ok(stream) => return Ok(stream),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "no addresses to connect to")
        }))
    }

    async fn connect_addr(addr: SocketAddr) -> io::Result<TcpStream> {
        let stream = TcpStream::from_std(nonblocking_connect(addr)?)?;
        // Writability signals that the connection attempt finished.
        poll_fn(|cx| stream.io.registration().poll_ready(cx, Direction::Write)).await?;
        if let Some(err) = stream.io.get_ref().take_error()? {
            return Err(err);
        }
        // A refused connection can also surface as "not connected".
        stream.io.get_ref().peer_addr()?;
        Ok(stream)
    }

    /// Wraps a connected standard library stream (switched to non-blocking).
    #[track_caller]
    pub fn from_std(stream: net::TcpStream) -> io::Result<TcpStream> {
        stream.set_nonblocking(true)?;
        Ok(TcpStream {
            io: PollEvented::new(stream, Interest::BOTH)?,
        })
    }

    /// Deregisters the stream and returns the standard library type.
    pub fn into_std(self) -> io::Result<net::TcpStream> {
        self.io.into_inner()
    }

    /// The local address of this socket.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().local_addr()
    }

    /// The remote address of this socket.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().peer_addr()
    }

    /// Sets `TCP_NODELAY` (disables Nagle's algorithm).
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.io.get_ref().set_nodelay(nodelay)
    }

    /// Shuts down the read, write, or both halves.
    pub fn shutdown_std(&self, how: Shutdown) -> io::Result<()> {
        self.io.get_ref().shutdown(how)
    }

    /// Polls a read into `buf`.
    fn poll_read_priv(&self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        self.io
            .registration()
            .poll_io(cx, Direction::Read, || self.io.get_ref().read(buf))
    }

    fn poll_write_priv(&self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.io
            .registration()
            .poll_io(cx, Direction::Write, || self.io.get_ref().write(buf))
    }

    fn poll_write_vectored_priv(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.io.registration().poll_io(cx, Direction::Write, || {
            self.io.get_ref().write_vectored(bufs)
        })
    }

    /// Waits until the socket is readable.
    pub async fn readable(&self) -> io::Result<()> {
        poll_fn(|cx| self.io.registration().poll_ready(cx, Direction::Read))
            .await
            .map(|_| ())
    }

    /// Waits until the socket is writable.
    pub async fn writable(&self) -> io::Result<()> {
        poll_fn(|cx| self.io.registration().poll_ready(cx, Direction::Write))
            .await
            .map(|_| ())
    }

    /// Splits the stream into owned read and write halves that can be moved
    /// to different tasks.
    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        let arc = Arc::new(self);
        (OwnedReadHalf(arc.clone()), OwnedWriteHalf(arc))
    }
}

impl fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpStream")
            .field("local", &self.local_addr().ok())
            .field("peer", &self.peer_addr().ok())
            .field("fd", &self.io.get_ref().as_raw_fd())
            .finish()
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_read_priv(cx, buf)
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_priv(cx, buf)
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored_priv(cx, bufs)
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // TCP sockets have no user-space buffer.
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.shutdown_std(Shutdown::Write))
    }
}

impl AsyncRead for &TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_read_priv(cx, buf)
    }
}

impl AsyncWrite for &TcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_priv(cx, buf)
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored_priv(cx, bufs)
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.shutdown_std(Shutdown::Write))
    }
}

/// The read half of a [`TcpStream`], from [`TcpStream::into_split`].
#[derive(Debug)]
pub struct OwnedReadHalf(Arc<TcpStream>);

/// The write half of a [`TcpStream`], from [`TcpStream::into_split`].
/// Dropping it shuts down the write direction.
#[derive(Debug)]
pub struct OwnedWriteHalf(Arc<TcpStream>);

impl AsyncRead for OwnedReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.0.poll_read_priv(cx, buf)
    }
}

impl AsyncWrite for OwnedWriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.poll_write_priv(cx, buf)
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.0.poll_write_vectored_priv(cx, bufs)
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.0.shutdown_std(Shutdown::Write))
    }
}

impl Drop for OwnedWriteHalf {
    fn drop(&mut self) {
        let _ = self.0.shutdown_std(Shutdown::Write);
    }
}

/// Creates a non-blocking, close-on-exec TCP socket and starts connecting it.
fn nonblocking_connect(addr: SocketAddr) -> io::Result<net::TcpStream> {
    let domain = match addr {
        SocketAddr::V4(_) => libc::AF_INET,
        SocketAddr::V6(_) => libc::AF_INET6,
    };
    // SAFETY: socket(2) takes no pointers.
    let fd = cvt(unsafe { libc::socket(domain, libc::SOCK_STREAM, 0) })?;
    // SAFETY: `fd` is a freshly created socket that nothing else owns; the
    // std type takes ownership and will close it on every path below.
    let stream = unsafe { net::TcpStream::from_raw_fd(fd) };
    set_cloexec(fd)?;
    stream.set_nonblocking(true)?;
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    set_nosigpipe(fd)?;

    let (storage, len) = socket_addr_to_raw(&addr);
    // SAFETY: `storage` is a properly initialised sockaddr of `len` bytes for
    // the socket's address family.
    let ret = unsafe {
        libc::connect(
            fd,
            (&storage as *const libc::sockaddr_storage).cast::<libc::sockaddr>(),
            len,
        )
    };
    if ret == -1 {
        let err = io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EINPROGRESS) {
            return Err(err);
        }
    }
    Ok(stream)
}

fn set_cloexec(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fd` is a valid descriptor; F_SETFD only changes flags.
    cvt(unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) })?;
    Ok(())
}

/// macOS has no `MSG_NOSIGNAL`; ask the socket itself not to raise SIGPIPE.
#[cfg(any(target_os = "macos", target_os = "ios"))]
fn set_nosigpipe(fd: RawFd) -> io::Result<()> {
    let one: libc::c_int = 1;
    // SAFETY: `one` is a valid c_int and its size is passed correctly.
    cvt(unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_NOSIGPIPE,
            (&one as *const libc::c_int).cast(),
            mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    })?;
    Ok(())
}

/// Converts a `SocketAddr` into a C `sockaddr_storage`.
fn socket_addr_to_raw(addr: &SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    // SAFETY: sockaddr_storage is plain-old-data; all-zero is a valid value.
    let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
    let len = match addr {
        SocketAddr::V4(a) => {
            // SAFETY: sockaddr_storage is large enough and suitably aligned
            // for any socket address type, including sockaddr_in.
            let sin = unsafe {
                &mut *(&mut storage as *mut libc::sockaddr_storage).cast::<libc::sockaddr_in>()
            };
            sin.sin_family = libc::AF_INET as libc::sa_family_t;
            sin.sin_port = a.port().to_be();
            sin.sin_addr = libc::in_addr {
                s_addr: u32::from_ne_bytes(a.ip().octets()),
            };
            #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
            {
                sin.sin_len = mem::size_of::<libc::sockaddr_in>() as u8;
            }
            mem::size_of::<libc::sockaddr_in>()
        }
        SocketAddr::V6(a) => {
            // SAFETY: as above, for sockaddr_in6.
            let sin6 = unsafe {
                &mut *(&mut storage as *mut libc::sockaddr_storage).cast::<libc::sockaddr_in6>()
            };
            sin6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sin6.sin6_port = a.port().to_be();
            sin6.sin6_addr = libc::in6_addr {
                s6_addr: a.ip().octets(),
            };
            sin6.sin6_flowinfo = a.flowinfo();
            sin6.sin6_scope_id = a.scope_id();
            #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
            {
                sin6.sin6_len = mem::size_of::<libc::sockaddr_in6>() as u8;
            }
            mem::size_of::<libc::sockaddr_in6>()
        }
    };
    (storage, len as libc::socklen_t)
}
