//! Baseline for the HTTP benchmark: the *same* weft-http parser, router and
//! response encoder, driven by Tokio instead of weft. Differences in
//! throughput between this and `weft-http`'s example server therefore come
//! from the runtime (scheduler, reactor, timers), not the HTTP code.
//!
//! ```text
//! cargo run --release -p weft-bench --example tokio_http -- 127.0.0.1:8081 [WORKERS]
//! ```

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use weft_http::{Handler, Parser, Request, Response, Router, StatusCode, encode_full_response};

fn app() -> Router {
    Router::new()
        .get("/plaintext", |_: Request| async {
            Response::text("Hello, World!")
        })
        .get("/json", |_: Request| async {
            Response::json(r#"{"message":"Hello, World!"}"#)
        })
}

async fn serve(mut stream: TcpStream, app: Arc<Router>) -> std::io::Result<()> {
    stream.set_nodelay(true)?;
    let mut parser = Parser::default();
    let mut buf = vec![0u8; 16 * 1024];
    let (mut start, mut filled) = (0usize, 0usize);
    let mut out = Vec::with_capacity(4096);
    loop {
        loop {
            match parser.parse(&buf[start..filled]) {
                Ok(Some((req, n))) => {
                    start += n;
                    let keep_alive = req.keep_alive();
                    let res = app.call(req).await;
                    encode_full_response(&res, keep_alive, &mut out);
                    if !keep_alive {
                        stream.write_all(&out).await?;
                        return Ok(());
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let res = Response::error(StatusCode(e.status_code()));
                    encode_full_response(&res, false, &mut out);
                    stream.write_all(&out).await?;
                    return Ok(());
                }
            }
        }
        if !out.is_empty() {
            stream.write_all(&out).await?;
            out.clear();
        }
        if start == filled {
            start = 0;
            filled = 0;
        } else if start > 0 {
            buf.copy_within(start..filled, 0);
            filled -= start;
            start = 0;
        }
        if filled == buf.len() {
            buf.resize(buf.len() * 2, 0);
        }
        let n = stream.read(&mut buf[filled..]).await?;
        if n == 0 {
            return Ok(());
        }
        filled += n;
    }
}

fn main() -> std::io::Result<()> {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:8081".into());
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    if let Some(w) = args.next().and_then(|w| w.parse().ok()) {
        builder.worker_threads(w);
    }
    let rt = builder.enable_all().build()?;
    rt.block_on(async move {
        let listener = TcpListener::bind(&addr).await?;
        eprintln!(
            "tokio baseline listening on http://{}",
            listener.local_addr()?
        );
        let app = Arc::new(app());
        loop {
            let (stream, _) = listener.accept().await?;
            let app = app.clone();
            tokio::spawn(async move {
                let _ = serve(stream, app).await;
            });
        }
    })
}
