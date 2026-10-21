//! The connection loop.

use std::future::{Future, poll_fn};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use weft::io::{AsyncRead, AsyncWriteExt};
use weft::net::{TcpListener, TcpStream};
use weft::time::{Sleep, sleep};

use crate::message::{Body, Method, Request, Response, StatusCode, Version, write_head};
use crate::parser::{Limits, Parser};

/// Something that turns a [`Request`] into a [`Response`].
///
/// Implemented for any `Fn(Request) -> impl Future<Output = Response>`, and
/// by [`Router`](crate::Router) and [`StaticFiles`](crate::StaticFiles).
pub trait Handler: Send + Sync + 'static {
    /// The response future.
    type Future: Future<Output = Response> + Send + 'static;

    /// Handles one request.
    fn call(&self, req: Request) -> Self::Future;
}

impl<F, Fut> Handler for F
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response> + Send + 'static,
{
    type Future = Fut;

    fn call(&self, req: Request) -> Fut {
        self(req)
    }
}

/// Server settings.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Parser limits (head size, header count, body size).
    pub limits: Limits,
    /// How long an idle keep-alive connection is kept open.
    pub keep_alive_timeout: Duration,
    /// How long a client may take to send a complete request once it has
    /// started sending one.
    pub request_timeout: Duration,
    /// Set `TCP_NODELAY` on accepted sockets.
    pub nodelay: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            limits: Limits::default(),
            keep_alive_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            nodelay: true,
        }
    }
}

/// An HTTP/1.1 server.
///
/// ```no_run
/// use weft_http::{Request, Response, Server};
///
/// let rt = weft::Runtime::new().unwrap();
/// rt.block_on(async {
///     let server = Server::bind("127.0.0.1:8080").unwrap();
///     server
///         .serve(|_req: Request| async { Response::text("hello\n") })
///         .await
///         .unwrap();
/// });
/// ```
#[derive(Debug)]
pub struct Server {
    listener: TcpListener,
    config: ServerConfig,
}

const READ_CHUNK: usize = 16 * 1024;
/// Bodies larger than this are written directly instead of being copied
/// into the output buffer.
const COPY_THRESHOLD: usize = 16 * 1024;

impl Server {
    /// Binds a listening socket. Must be called inside a weft runtime.
    pub fn bind(addr: impl ToSocketAddrs) -> io::Result<Server> {
        Ok(Server::from_listener(TcpListener::bind(addr)?))
    }

    /// Uses an existing listener.
    pub fn from_listener(listener: TcpListener) -> Server {
        Server {
            listener,
            config: ServerConfig::default(),
        }
    }

    /// Replaces the configuration.
    pub fn with_config(mut self, config: ServerConfig) -> Server {
        self.config = config;
        self
    }

    /// The bound address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accepts connections forever, spawning one task per connection.
    pub async fn serve<H: Handler>(self, handler: H) -> io::Result<()> {
        self.serve_until(handler, std::future::pending()).await
    }

    /// Like [`serve`](Server::serve), but stops accepting new connections
    /// once `shutdown` completes. Connections already accepted keep running
    /// until they finish.
    pub async fn serve_until<H, S>(self, handler: H, shutdown: S) -> io::Result<()>
    where
        H: Handler,
        S: Future<Output = ()>,
    {
        let handler = Arc::new(handler);
        let config = Arc::new(self.config);
        let mut shutdown = pin!(shutdown);
        loop {
            let accepted = poll_fn(|cx| {
                if shutdown.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(None);
                }
                self.listener.poll_accept(cx).map(Some)
            })
            .await;
            match accepted {
                None => return Ok(()),
                Some(Ok((stream, _peer))) => {
                    let handler = handler.clone();
                    let config = config.clone();
                    weft::spawn(async move {
                        // Connection-level I/O errors (resets, broken pipes)
                        // only affect that client.
                        let _ = serve_connection(stream, &*handler, &config).await;
                    });
                }
                Some(Err(e)) if is_transient_accept_error(&e) => {
                    // E.g. out of file descriptors: back off briefly instead
                    // of spinning.
                    sleep(Duration::from_millis(10)).await;
                }
                Some(Err(e)) => return Err(e),
            }
        }
    }
}

fn is_transient_accept_error(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::Interrupted
    ) || matches!(e.raw_os_error(), Some(23 | 24)) // ENFILE, EMFILE
}

/// Per-connection read buffer: `buf[start..filled]` holds unparsed bytes.
struct ReadBuf {
    buf: Vec<u8>,
    start: usize,
    filled: usize,
}

impl ReadBuf {
    fn new() -> ReadBuf {
        ReadBuf {
            buf: vec![0; READ_CHUNK],
            start: 0,
            filled: 0,
        }
    }

    fn unparsed(&self) -> &[u8] {
        &self.buf[self.start..self.filled]
    }

    /// Moves unparsed bytes to the front and makes room for a read.
    fn prepare_read(&mut self) -> &mut [u8] {
        if self.start == self.filled {
            self.start = 0;
            self.filled = 0;
        } else if self.start > 0 {
            self.buf.copy_within(self.start..self.filled, 0);
            self.filled -= self.start;
            self.start = 0;
        }
        if self.buf.len() - self.filled < READ_CHUNK / 4 {
            let new_len = (self.buf.len() * 2).max(self.filled + READ_CHUNK);
            self.buf.resize(new_len, 0);
        }
        &mut self.buf[self.filled..]
    }
}

/// Serves one connection: parses pipelined requests in order, runs the
/// handler for each, and batches the responses into as few writes as
/// possible.
pub(crate) async fn serve_connection<H: Handler + ?Sized>(
    mut stream: TcpStream,
    handler: &H,
    config: &ServerConfig,
) -> io::Result<()> {
    if config.nodelay {
        let _ = stream.set_nodelay(true);
    }
    let mut parser = Parser::new(config.limits);
    let mut rbuf = ReadBuf::new();
    let mut out: Vec<u8> = Vec::with_capacity(4096);
    // One timer per connection, re-armed lazily (see `read_with_deadline`).
    let mut timer = sleep(config.keep_alive_timeout);

    loop {
        // Serve every complete request that is already buffered. With
        // pipelining several may be available; responses are appended to
        // `out` in request order and flushed together.
        loop {
            match parser.parse(rbuf.unparsed()) {
                Ok(Some((req, consumed))) => {
                    rbuf.start += consumed;
                    let close = !req.keep_alive();
                    let is_head = req.method == Method::Head;
                    let version = req.version;
                    let response = handler.call(req).await;
                    encode_response(&mut stream, &mut out, response, is_head, version, close)
                        .await?;
                    if close {
                        stream.write_all(&out).await?;
                        let _ = stream.shutdown().await;
                        return Ok(());
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    let response = Response::error(StatusCode(err.status_code()));
                    encode_response(
                        &mut stream,
                        &mut out,
                        response,
                        false,
                        Version::Http11,
                        true,
                    )
                    .await?;
                    stream.write_all(&out).await?;
                    let _ = stream.shutdown().await;
                    return Ok(());
                }
            }
        }
        if !out.is_empty() {
            stream.write_all(&out).await?;
            out.clear();
        }

        let mid_request = parser.is_mid_request(rbuf.unparsed().len());
        let limit = if mid_request {
            config.request_timeout
        } else {
            config.keep_alive_timeout
        };
        let deadline = Instant::now() + limit;
        let space = rbuf.prepare_read();
        match read_with_deadline(&mut stream, space, &mut timer, deadline).await {
            None => {
                if mid_request {
                    let response = Response::error(StatusCode::REQUEST_TIMEOUT);
                    encode_response(
                        &mut stream,
                        &mut out,
                        response,
                        false,
                        Version::Http11,
                        true,
                    )
                    .await?;
                    let _ = stream.write_all(&out).await;
                }
                return Ok(());
            }
            Some(Ok(0)) => return Ok(()),
            Some(Ok(n)) => rbuf.filled += n,
            Some(Err(e)) => return Err(e),
        }
    }
}

/// Reads into `buf`, giving up at `deadline` (returns `None`).
///
/// Registering and cancelling a timer on every read would put the timer
/// wheel's lock on the hot path. Instead the connection keeps one `Sleep`
/// whose registration is only touched when it fires early (the deadline has
/// moved since it was armed) or must fire earlier than currently armed.
async fn read_with_deadline(
    stream: &mut TcpStream,
    buf: &mut [u8],
    timer: &mut Sleep,
    deadline: Instant,
) -> Option<io::Result<usize>> {
    if timer.deadline() > deadline {
        timer.reset(deadline);
    }
    poll_fn(|cx| {
        if let Poll::Ready(res) = Pin::new(&mut *stream).poll_read(cx, buf) {
            return Poll::Ready(Some(res));
        }
        loop {
            match Pin::new(&mut *timer).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) if Instant::now() >= deadline => return Poll::Ready(None),
                // Fired for an older, earlier deadline: re-arm and poll again
                // to register the new one.
                Poll::Ready(()) => timer.reset(deadline),
            }
        }
    })
    .await
}

/// Serialises a response. Small bodies are appended to `out`; large or
/// streamed bodies flush `out` and are written directly.
async fn encode_response(
    stream: &mut TcpStream,
    out: &mut Vec<u8>,
    response: Response,
    is_head: bool,
    version: Version,
    close: bool,
) -> io::Result<()> {
    let Response {
        status,
        headers,
        body,
    } = response;
    let bytes = match body {
        Body::Empty => Vec::new(),
        Body::Full(bytes) => bytes,
        Body::Stream(mut rx) if version == Version::Http10 => {
            // HTTP/1.0 has no chunked coding: buffer the stream.
            let mut all = Vec::new();
            while let Some(chunk) = rx.recv().await {
                all.extend_from_slice(&chunk);
            }
            all
        }
        Body::Stream(mut rx) => {
            write_head(
                out,
                status,
                &headers,
                Some(("transfer-encoding", "chunked")),
                close,
            );
            if is_head || !status.allows_body() {
                return Ok(());
            }
            stream.write_all(out).await?;
            out.clear();
            while let Some(chunk) = rx.recv().await {
                if chunk.is_empty() {
                    continue;
                }
                out.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
                out.extend_from_slice(&chunk);
                out.extend_from_slice(b"\r\n");
                stream.write_all(out).await?;
                out.clear();
            }
            out.extend_from_slice(b"0\r\n\r\n");
            return Ok(());
        }
    };
    if !status.allows_body() {
        write_head(out, status, &headers, None, close);
        return Ok(());
    }
    let len = bytes.len().to_string();
    write_head(out, status, &headers, Some(("content-length", &len)), close);
    if is_head {
        return Ok(());
    }
    if bytes.len() > COPY_THRESHOLD {
        stream.write_all(out).await?;
        out.clear();
        stream.write_all(&bytes).await?;
    } else {
        out.extend_from_slice(&bytes);
    }
    Ok(())
}
