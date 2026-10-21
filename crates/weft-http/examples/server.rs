//! Example HTTP server: JSON endpoints, path parameters, a chunked stream,
//! and static files.
//!
//! ```text
//! cargo run --release -p weft-http --example server -- [ADDR] [WORKERS]
//! curl http://127.0.0.1:8080/json
//! ```

use std::time::{Duration, Instant};

use weft_http::{Body, Request, Response, Router, Server, StaticFiles, json_escape};

fn main() -> std::io::Result<()> {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:8080".to_string());
    let mut builder = weft::Runtime::builder();
    if let Some(workers) = args.next().and_then(|w| w.parse().ok()) {
        builder.worker_threads(workers);
    }
    let rt = builder.build()?;

    let static_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/static");
    let started = Instant::now();

    rt.block_on(async move {
        let app = Router::new()
            .get("/", |_: Request| async {
                Response::new(weft_http::StatusCode(302)).with_header("location", "/static/")
            })
            .get("/plaintext", |_: Request| async {
                Response::text("Hello, World!")
            })
            .get("/json", |_: Request| async {
                Response::json(r#"{"message":"Hello, World!"}"#)
            })
            .get("/users/:id", |req: Request| async move {
                let id = req.param("id").unwrap_or_default();
                Response::json(format!(
                    r#"{{"id":"{}","name":"user-{}"}}"#,
                    json_escape(id),
                    json_escape(id)
                ))
            })
            .post("/echo", |req: Request| async move {
                let ct = req
                    .header("content-type")
                    .unwrap_or("application/octet-stream")
                    .to_string();
                Response::bytes(&ct, req.body)
            })
            .get("/stream", |_: Request| async {
                // A chunked response produced by a separate task, paced by
                // the runtime's timers.
                let (tx, body) = Body::channel(8);
                weft::spawn(async move {
                    for i in 1..=5 {
                        let line = format!("{{\"tick\":{i}}}\n");
                        if tx.send(line).await.is_err() {
                            return; // client went away
                        }
                        weft::time::sleep(Duration::from_millis(200)).await;
                    }
                });
                Response::stream("application/x-ndjson", body)
            })
            .get("/stats", move |_: Request| async move {
                // Deliberately no global request counter: a shared atomic
                // incremented by every worker would be a contended cache line
                // on the hot path.
                let handle = weft::Handle::current();
                Response::json(format!(
                    r#"{{"uptime_secs":{:.1},"workers":{},"alive_tasks":{}}}"#,
                    started.elapsed().as_secs_f64(),
                    handle.num_workers(),
                    handle.num_alive_tasks()
                ))
            })
            .get(
                "/static/*path",
                StaticFiles::new(static_dir).expect("static dir"),
            );

        let server = Server::bind(&addr)?;
        eprintln!(
            "weft-http listening on http://{} ({} workers)",
            server.local_addr()?,
            weft::Handle::current().num_workers()
        );
        server.serve(app).await
    })
}
