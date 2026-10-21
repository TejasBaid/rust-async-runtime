//! # weft-http
//!
//! An HTTP/1.1 server built on the [`weft`] runtime.
//!
//! * [`parser`]: an incremental, allocation-light request parser with
//!   chunked transfer-coding support and smuggling-resistant framing rules.
//! * [`Server`]: one task per connection, keep-alive with idle and request
//!   timeouts, pipelining (responses are written in request order and
//!   batched into one write), and chunked streaming responses.
//! * [`Router`] and [`StaticFiles`]: routing with path parameters and
//!   traversal-safe static file serving.
//!
//! ```no_run
//! use weft_http::{Request, Response, Router, Server};
//!
//! let rt = weft::Runtime::new().unwrap();
//! rt.block_on(async {
//!     let app = Router::new().get("/", |_: Request| async { Response::text("hi\n") });
//!     Server::bind("127.0.0.1:8080").unwrap().serve(app).await.unwrap();
//! });
//! ```

#![warn(missing_docs, missing_debug_implementations)]

mod date;
mod fs;
mod message;
pub mod parser;
mod router;
mod server;

pub use fs::{StaticFiles, mime_type};
pub use message::{Body, BodySender, Headers, Method, Request, Response, StatusCode, Version};
pub use parser::{Limits, ParseError, Parser};
pub use router::{BoxFuture, Router};
pub use server::{Handler, Server, ServerConfig};

/// Escapes a string for inclusion in a JSON string literal.
pub fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// Serialises a complete response (head and body) into bytes. Exposed so the
/// same encoder can be reused by other front-ends (e.g. the Tokio comparison
/// server in the benchmarks); streamed bodies are not supported here.
pub fn encode_full_response(res: &Response, keep_alive: bool, out: &mut Vec<u8>) {
    let body: &[u8] = match &res.body {
        Body::Full(b) => b,
        _ => &[],
    };
    let len = body.len().to_string();
    message::write_head(
        out,
        res.status,
        &res.headers,
        Some(("content-length", &len)),
        !keep_alive,
    );
    out.extend_from_slice(body);
}
