//! End-to-end tests: a real server on an ephemeral port, driven by a small
//! blocking HTTP client so the client side is independent of the runtime.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use weft::Runtime;
use weft::sync::oneshot;
use weft_http::{
    Body, Method, Request, Response, Router, Server, ServerConfig, StaticFiles, StatusCode,
};

struct TestServer {
    addr: SocketAddr,
    _shutdown: oneshot::Sender<()>,
    rt: Runtime,
}

fn app() -> Router {
    Router::new()
        .get("/", |_: Request| async { Response::text("hello") })
        .get("/json", |_: Request| async {
            Response::json(r#"{"message":"Hello, World!"}"#)
        })
        .get("/users/:id", |req: Request| async move {
            Response::text(format!("user={}", req.param("id").unwrap()))
        })
        .post("/echo", |req: Request| async move {
            Response::bytes("application/octet-stream", req.body)
        })
        .get("/stream", |_: Request| async {
            let (tx, body) = Body::channel(4);
            weft::spawn(async move {
                for i in 0..5 {
                    if tx.send(format!("chunk-{i};")).await.is_err() {
                        return;
                    }
                    weft::time::sleep(Duration::from_millis(2)).await;
                }
            });
            Response::stream("text/plain", body)
        })
        .get("/slow", |_: Request| async {
            weft::time::sleep(Duration::from_millis(50)).await;
            Response::text("slow")
        })
        .get("/no-content", |_: Request| async {
            Response::new(StatusCode::NO_CONTENT)
        })
}

fn start_with(router: Router, config: ServerConfig) -> TestServer {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    let (tx, rx) = oneshot::channel::<()>();
    let addr = rt.block_on(async move {
        let server = Server::bind("127.0.0.1:0").unwrap().with_config(config);
        let addr = server.local_addr().unwrap();
        weft::spawn(async move {
            server
                .serve_until(router, async move {
                    let _ = rx.await;
                })
                .await
                .unwrap();
        });
        addr
    });
    TestServer {
        addr,
        _shutdown: tx,
        rt,
    }
}

fn start() -> TestServer {
    start_with(app(), ServerConfig::default())
}

#[derive(Debug)]
struct Resp {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Resp {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
    fn text(&self) -> String {
        String::from_utf8(self.body.clone()).unwrap()
    }
}

/// Reads one response; `head_only` for responses to HEAD requests.
fn read_response(r: &mut BufReader<TcpStream>, head_only: bool) -> std::io::Result<Resp> {
    let mut line = String::new();
    if r.read_line(&mut line)? == 0 {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    let status: u16 = line.split(' ').nth(1).unwrap().parse().unwrap();
    let mut headers = Vec::new();
    loop {
        line.clear();
        r.read_line(&mut line)?;
        let l = line.trim_end();
        if l.is_empty() {
            break;
        }
        let (n, v) = l.split_once(':').unwrap();
        headers.push((n.trim().to_string(), v.trim().to_string()));
    }
    let mut resp = Resp {
        status,
        headers,
        body: Vec::new(),
    };
    if head_only || status == 204 || status == 304 {
        return Ok(resp);
    }
    if resp
        .header("transfer-encoding")
        .is_some_and(|v| v.eq_ignore_ascii_case("chunked"))
    {
        loop {
            line.clear();
            r.read_line(&mut line)?;
            let size = usize::from_str_radix(line.trim_end(), 16).unwrap();
            let mut chunk = vec![0; size + 2];
            r.read_exact(&mut chunk)?;
            if size == 0 {
                break;
            }
            resp.body.extend_from_slice(&chunk[..size]);
        }
    } else if let Some(len) = resp.header("content-length") {
        let mut body = vec![0; len.parse().unwrap()];
        r.read_exact(&mut body)?;
        resp.body = body;
    } else {
        r.read_to_end(&mut resp.body)?;
    }
    Ok(resp)
}

fn connect(addr: SocketAddr) -> (TcpStream, BufReader<TcpStream>) {
    let s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let r = BufReader::new(s.try_clone().unwrap());
    (s, r)
}

fn get(addr: SocketAddr, path: &str) -> Resp {
    let (mut s, mut r) = connect(addr);
    write!(s, "GET {path} HTTP/1.1\r\nHost: test\r\n\r\n").unwrap();
    read_response(&mut r, false).unwrap()
}

/// Returns `true` if the server closed the connection.
fn closed_by_server(r: &mut BufReader<TcpStream>) -> bool {
    let mut buf = [0u8; 1];
    matches!(r.read(&mut buf), Ok(0))
}

#[test]
fn basic_get() {
    let srv = start();
    let resp = get(srv.addr, "/");
    assert_eq!(resp.status, 200);
    assert_eq!(resp.text(), "hello");
    assert_eq!(resp.header("content-length"), Some("5"));
    assert!(resp.header("date").unwrap().ends_with("GMT"));
    assert_eq!(
        resp.header("content-type"),
        Some("text/plain; charset=utf-8")
    );
}

#[test]
fn json_and_params() {
    let srv = start();
    let resp = get(srv.addr, "/json");
    assert_eq!(resp.header("content-type"), Some("application/json"));
    assert_eq!(resp.text(), r#"{"message":"Hello, World!"}"#);
    assert_eq!(get(srv.addr, "/users/42").text(), "user=42");
}

#[test]
fn not_found_and_method_not_allowed() {
    let srv = start();
    assert_eq!(get(srv.addr, "/nope").status, 404);
    let (mut s, mut r) = connect(srv.addr);
    write!(s, "DELETE /json HTTP/1.1\r\nHost: t\r\n\r\n").unwrap();
    let resp = read_response(&mut r, false).unwrap();
    assert_eq!(resp.status, 405);
    assert_eq!(resp.header("allow"), Some("GET"));
}

#[test]
fn keep_alive_reuses_connection() {
    let srv = start();
    let (mut s, mut r) = connect(srv.addr);
    for i in 0..20 {
        write!(s, "GET /users/{i} HTTP/1.1\r\nHost: t\r\n\r\n").unwrap();
        let resp = read_response(&mut r, false).unwrap();
        assert_eq!(resp.text(), format!("user={i}"));
        assert_eq!(resp.header("connection"), None);
    }
}

#[test]
fn pipelined_requests_answered_in_order() {
    let srv = start();
    let (mut s, mut r) = connect(srv.addr);
    // A slow request first: its response must still come first.
    let mut batch = String::from("GET /slow HTTP/1.1\r\nHost: t\r\n\r\n");
    for i in 0..10 {
        batch.push_str(&format!("GET /users/{i} HTTP/1.1\r\nHost: t\r\n\r\n"));
    }
    batch.push_str("POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: 4\r\n\r\nping");
    s.write_all(batch.as_bytes()).unwrap();
    assert_eq!(read_response(&mut r, false).unwrap().text(), "slow");
    for i in 0..10 {
        assert_eq!(
            read_response(&mut r, false).unwrap().text(),
            format!("user={i}")
        );
    }
    assert_eq!(read_response(&mut r, false).unwrap().text(), "ping");
}

#[test]
fn request_split_across_many_writes() {
    let srv = start();
    let (mut s, mut r) = connect(srv.addr);
    s.set_nodelay(true).unwrap();
    let req = b"POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: 11\r\n\r\nhello world";
    for chunk in req.chunks(3) {
        s.write_all(chunk).unwrap();
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(read_response(&mut r, false).unwrap().text(), "hello world");
}

#[test]
fn connection_close_is_honoured() {
    let srv = start();
    let (mut s, mut r) = connect(srv.addr);
    write!(s, "GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n").unwrap();
    let resp = read_response(&mut r, false).unwrap();
    assert_eq!(resp.header("connection"), Some("close"));
    assert!(closed_by_server(&mut r));
}

#[test]
fn http10_closes_by_default() {
    let srv = start();
    let (mut s, mut r) = connect(srv.addr);
    write!(s, "GET / HTTP/1.0\r\n\r\n").unwrap();
    let resp = read_response(&mut r, false).unwrap();
    assert_eq!(resp.text(), "hello");
    assert!(closed_by_server(&mut r));
}

#[test]
fn chunked_request_body() {
    let srv = start();
    let (mut s, mut r) = connect(srv.addr);
    s.write_all(
        b"POST /echo HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n",
    )
    .unwrap();
    assert_eq!(read_response(&mut r, false).unwrap().text(), "hello world");
}

#[test]
fn chunked_streaming_response() {
    let srv = start();
    let resp = get(srv.addr, "/stream");
    assert_eq!(resp.header("transfer-encoding"), Some("chunked"));
    assert_eq!(resp.text(), "chunk-0;chunk-1;chunk-2;chunk-3;chunk-4;");
    // HTTP/1.0 clients get the same body without chunking.
    let (mut s, mut r) = connect(srv.addr);
    write!(s, "GET /stream HTTP/1.0\r\n\r\n").unwrap();
    let resp = read_response(&mut r, false).unwrap();
    assert_eq!(resp.header("transfer-encoding"), None);
    assert_eq!(resp.text(), "chunk-0;chunk-1;chunk-2;chunk-3;chunk-4;");
}

#[test]
fn head_has_no_body() {
    let srv = start();
    let (mut s, mut r) = connect(srv.addr);
    write!(
        s,
        "HEAD / HTTP/1.1\r\nHost: t\r\n\r\nGET / HTTP/1.1\r\nHost: t\r\n\r\n"
    )
    .unwrap();
    let head = read_response(&mut r, true).unwrap();
    assert_eq!(head.status, 200);
    assert_eq!(head.header("content-length"), Some("5"));
    // The next response starts right after the head: no stray body bytes.
    assert_eq!(read_response(&mut r, false).unwrap().text(), "hello");
}

#[test]
fn no_content_has_no_framing() {
    let srv = start();
    let resp = get(srv.addr, "/no-content");
    assert_eq!(resp.status, 204);
    assert_eq!(resp.header("content-length"), None);
}

#[test]
fn malformed_request_gets_400_and_close() {
    let srv = start();
    let (mut s, mut r) = connect(srv.addr);
    s.write_all(b"GARBAGE\r\n\r\n").unwrap();
    let resp = read_response(&mut r, false).unwrap();
    assert_eq!(resp.status, 400);
    assert_eq!(resp.header("connection"), Some("close"));
    assert!(closed_by_server(&mut r));
}

#[test]
fn smuggling_attempt_rejected() {
    let srv = start();
    let (mut s, mut r) = connect(srv.addr);
    s.write_all(
        b"POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
    )
    .unwrap();
    assert_eq!(read_response(&mut r, false).unwrap().status, 400);
}

#[test]
fn oversized_body_gets_413() {
    let srv = start();
    let (mut s, mut r) = connect(srv.addr);
    write!(
        s,
        "POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: 999999999\r\n\r\n"
    )
    .unwrap();
    assert_eq!(read_response(&mut r, false).unwrap().status, 413);
}

#[test]
fn oversized_head_gets_431() {
    let srv = start();
    let (mut s, mut r) = connect(srv.addr);
    let big = "a".repeat(16 * 1024);
    let _ = write!(s, "GET / HTTP/1.1\r\nHost: t\r\nX-Big: {big}\r\n\r\n");
    assert_eq!(read_response(&mut r, false).unwrap().status, 431);
}

#[test]
fn idle_keep_alive_connection_is_closed() {
    let config = ServerConfig {
        keep_alive_timeout: Duration::from_millis(100),
        ..ServerConfig::default()
    };
    let srv = start_with(app(), config);
    let (mut s, mut r) = connect(srv.addr);
    write!(s, "GET / HTTP/1.1\r\nHost: t\r\n\r\n").unwrap();
    read_response(&mut r, false).unwrap();
    let start = Instant::now();
    assert!(closed_by_server(&mut r));
    let waited = start.elapsed();
    assert!(waited >= Duration::from_millis(80), "{waited:?}");
    assert!(waited < Duration::from_secs(3), "{waited:?}");
}

#[test]
fn incomplete_request_times_out_with_408() {
    let config = ServerConfig {
        request_timeout: Duration::from_millis(100),
        ..ServerConfig::default()
    };
    let srv = start_with(app(), config);
    let (mut s, mut r) = connect(srv.addr);
    s.write_all(b"GET / HTTP/1.1\r\nHost: t\r\n").unwrap(); // no blank line
    let resp = read_response(&mut r, false).unwrap();
    assert_eq!(resp.status, 408);
}

#[test]
fn static_files_and_traversal() {
    let dir = std::env::temp_dir().join(format!("weft-http-test-{}", std::process::id()));
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    std::fs::write(dir.join("index.html"), "<h1>index</h1>").unwrap();
    std::fs::write(dir.join("sub/app.js"), "console.log(1)").unwrap();
    let router = Router::new().get("/static/*path", StaticFiles::new(&dir).unwrap());
    let srv = start_with(router, ServerConfig::default());

    let resp = get(srv.addr, "/static/sub/app.js");
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.header("content-type"),
        Some("text/javascript; charset=utf-8")
    );
    assert_eq!(resp.text(), "console.log(1)");
    assert_eq!(get(srv.addr, "/static/").text(), "<h1>index</h1>");
    assert_eq!(get(srv.addr, "/static/missing.txt").status, 404);
    assert_eq!(get(srv.addr, "/static/../Cargo.toml").status, 403);
    assert_eq!(
        get(srv.addr, "/static/%2e%2e/%2e%2e/etc/passwd").status,
        403
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn many_concurrent_clients() {
    let srv = start();
    let addr = srv.addr;
    let ok = Arc::new(AtomicUsize::new(0));
    let threads: Vec<_> = (0..32)
        .map(|t| {
            let ok = ok.clone();
            std::thread::spawn(move || {
                let (mut s, mut r) = connect(addr);
                for i in 0..50 {
                    write!(s, "GET /users/{t}-{i} HTTP/1.1\r\nHost: t\r\n\r\n").unwrap();
                    let resp = read_response(&mut r, false).unwrap();
                    assert_eq!(resp.text(), format!("user={t}-{i}"));
                    ok.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(ok.load(Ordering::Relaxed), 32 * 50);
}

#[test]
fn handler_closure_without_router() {
    let rt = Runtime::builder().worker_threads(2).build().unwrap();
    let addr = rt.block_on(async {
        let server = Server::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();
        weft::spawn(server.serve(|req: Request| async move {
            let m = req.method.clone();
            Response::text(format!("{} {}", m, req.path()))
        }));
        addr
    });
    assert_eq!(get(addr, "/a/b?q=1").text(), "GET /a/b");
    drop(rt);
}

#[test]
fn shutdown_stops_accepting() {
    let srv = start();
    let addr = srv.addr;
    assert_eq!(get(addr, "/").status, 200);
    let TestServer { _shutdown, rt, .. } = srv;
    _shutdown.send(()).unwrap();
    // Give the accept loop a moment to observe the shutdown signal.
    std::thread::sleep(Duration::from_millis(50));
    drop(rt);
    assert!(
        TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_err() || {
            // On some platforms the connect can race with the close;
            // a request must then fail.
            let (mut s, mut r) = connect(addr);
            let _ = write!(s, "GET / HTTP/1.1\r\nHost: t\r\n\r\n");
            read_response(&mut r, false).is_err()
        }
    );
    let _ = Method::Get;
}
