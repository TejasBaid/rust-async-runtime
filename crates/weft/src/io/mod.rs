//! Asynchronous I/O traits and helpers.
//!
//! [`AsyncRead`] and [`AsyncWrite`] are the poll-based counterparts of
//! `std::io::Read`/`Write`. They are deliberately minimal (byte slices rather
//! than an uninitialised-buffer abstraction); the extension traits
//! [`AsyncReadExt`] and [`AsyncWriteExt`] provide `async fn`-style helpers.

use std::future::Future;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Reads bytes asynchronously.
pub trait AsyncRead {
    /// Attempts to read into `buf`. Returns `Ready(Ok(0))` at end of stream.
    /// On `Pending`, the current task will be woken when the source may have
    /// become readable.
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>>;
}

/// Writes bytes asynchronously.
pub trait AsyncWrite {
    /// Attempts to write from `buf`, returning how many bytes were written.
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>>;

    /// Like [`poll_write`](AsyncWrite::poll_write), from several buffers. The
    /// default implementation writes the first non-empty buffer.
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let buf = bufs
            .iter()
            .find(|b| !b.is_empty())
            .map_or(&[][..], |b| &**b);
        self.poll_write(cx, buf)
    }

    /// Flushes buffered data.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    /// Shuts down the write side (e.g. sends a TCP FIN).
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
}

impl<T: AsyncRead + Unpin + ?Sized> AsyncRead for &mut T {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut **self).poll_read(cx, buf)
    }
}

impl<T: AsyncRead + Unpin + ?Sized> AsyncRead for Box<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut **self).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin + ?Sized> AsyncWrite for &mut T {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut **self).poll_write(cx, buf)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut **self).poll_write_vectored(cx, bufs)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut **self).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut **self).poll_shutdown(cx)
    }
}

impl<T: AsyncWrite + Unpin + ?Sized> AsyncWrite for Box<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut **self).poll_write(cx, buf)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut **self).poll_write_vectored(cx, bufs)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut **self).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut **self).poll_shutdown(cx)
    }
}

/// `async` helpers for [`AsyncRead`].
pub trait AsyncReadExt: AsyncRead {
    /// Reads some bytes into `buf`.
    fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> Read<'a, Self>
    where
        Self: Unpin,
    {
        Read { reader: self, buf }
    }

    /// Reads exactly `buf.len()` bytes, failing with `UnexpectedEof` if the
    /// stream ends first.
    fn read_exact<'a>(&'a mut self, buf: &'a mut [u8]) -> ReadExact<'a, Self>
    where
        Self: Unpin,
    {
        ReadExact {
            reader: self,
            buf,
            filled: 0,
        }
    }

    /// Reads until end of stream, appending to `buf`.
    fn read_to_end<'a>(&'a mut self, buf: &'a mut Vec<u8>) -> ReadToEnd<'a, Self>
    where
        Self: Unpin,
    {
        let start = buf.len();
        ReadToEnd {
            reader: self,
            buf,
            start,
        }
    }
}

impl<R: AsyncRead + ?Sized> AsyncReadExt for R {}

/// `async` helpers for [`AsyncWrite`].
pub trait AsyncWriteExt: AsyncWrite {
    /// Writes some bytes from `buf`.
    fn write<'a>(&'a mut self, buf: &'a [u8]) -> Write<'a, Self>
    where
        Self: Unpin,
    {
        Write { writer: self, buf }
    }

    /// Writes the whole buffer.
    fn write_all<'a>(&'a mut self, buf: &'a [u8]) -> WriteAll<'a, Self>
    where
        Self: Unpin,
    {
        WriteAll { writer: self, buf }
    }

    /// Flushes the writer.
    fn flush(&mut self) -> Flush<'_, Self>
    where
        Self: Unpin,
    {
        Flush { writer: self }
    }

    /// Shuts down the write half.
    fn shutdown(&mut self) -> Shutdown<'_, Self>
    where
        Self: Unpin,
    {
        Shutdown { writer: self }
    }
}

impl<W: AsyncWrite + ?Sized> AsyncWriteExt for W {}

/// Future for [`AsyncReadExt::read`].
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Read<'a, R: ?Sized> {
    reader: &'a mut R,
    buf: &'a mut [u8],
}

impl<R: AsyncRead + Unpin + ?Sized> Future for Read<'_, R> {
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        Pin::new(&mut *this.reader).poll_read(cx, this.buf)
    }
}

/// Future for [`AsyncReadExt::read_exact`].
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReadExact<'a, R: ?Sized> {
    reader: &'a mut R,
    buf: &'a mut [u8],
    filled: usize,
}

impl<R: AsyncRead + Unpin + ?Sized> Future for ReadExact<'_, R> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        while this.filled < this.buf.len() {
            let n = match Pin::new(&mut *this.reader).poll_read(cx, &mut this.buf[this.filled..]) {
                Poll::Ready(Ok(n)) => n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
            }
            this.filled += n;
        }
        Poll::Ready(Ok(()))
    }
}

/// Future for [`AsyncReadExt::read_to_end`].
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReadToEnd<'a, R: ?Sized> {
    reader: &'a mut R,
    buf: &'a mut Vec<u8>,
    start: usize,
}

impl<R: AsyncRead + Unpin + ?Sized> Future for ReadToEnd<'_, R> {
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let mut chunk = [0u8; 8 * 1024];
        loop {
            match Pin::new(&mut *this.reader).poll_read(cx, &mut chunk) {
                Poll::Ready(Ok(0)) => return Poll::Ready(Ok(this.buf.len() - this.start)),
                Poll::Ready(Ok(n)) => this.buf.extend_from_slice(&chunk[..n]),
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Future for [`AsyncWriteExt::write`].
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Write<'a, W: ?Sized> {
    writer: &'a mut W,
    buf: &'a [u8],
}

impl<W: AsyncWrite + Unpin + ?Sized> Future for Write<'_, W> {
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        Pin::new(&mut *this.writer).poll_write(cx, this.buf)
    }
}

/// Future for [`AsyncWriteExt::write_all`].
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct WriteAll<'a, W: ?Sized> {
    writer: &'a mut W,
    buf: &'a [u8],
}

impl<W: AsyncWrite + Unpin + ?Sized> Future for WriteAll<'_, W> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        while !this.buf.is_empty() {
            let n = match Pin::new(&mut *this.writer).poll_write(cx, this.buf) {
                Poll::Ready(Ok(n)) => n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            this.buf = &this.buf[n..];
        }
        Poll::Ready(Ok(()))
    }
}

/// Future for [`AsyncWriteExt::flush`].
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Flush<'a, W: ?Sized> {
    writer: &'a mut W,
}

impl<W: AsyncWrite + Unpin + ?Sized> Future for Flush<'_, W> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut *self.writer).poll_flush(cx)
    }
}

/// Future for [`AsyncWriteExt::shutdown`].
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Shutdown<'a, W: ?Sized> {
    writer: &'a mut W,
}

impl<W: AsyncWrite + Unpin + ?Sized> Future for Shutdown<'_, W> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut *self.writer).poll_shutdown(cx)
    }
}
