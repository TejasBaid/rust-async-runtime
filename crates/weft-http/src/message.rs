//! Request and response types.

use std::fmt;

use weft::sync::mpsc;

/// An HTTP request method.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Method {
    /// `GET`
    Get,
    /// `HEAD`
    Head,
    /// `POST`
    Post,
    /// `PUT`
    Put,
    /// `DELETE`
    Delete,
    /// `PATCH`
    Patch,
    /// `OPTIONS`
    Options,
    /// Any other (extension) method.
    Other(String),
}

impl Method {
    pub(crate) fn from_bytes(b: &[u8]) -> Method {
        match b {
            b"GET" => Method::Get,
            b"HEAD" => Method::Head,
            b"POST" => Method::Post,
            b"PUT" => Method::Put,
            b"DELETE" => Method::Delete,
            b"PATCH" => Method::Patch,
            b"OPTIONS" => Method::Options,
            other => Method::Other(String::from_utf8_lossy(other).into_owned()),
        }
    }

    /// The method name.
    pub fn as_str(&self) -> &str {
        match self {
            Method::Get => "GET",
            Method::Head => "HEAD",
            Method::Post => "POST",
            Method::Put => "PUT",
            Method::Delete => "DELETE",
            Method::Patch => "PATCH",
            Method::Options => "OPTIONS",
            Method::Other(s) => s,
        }
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// HTTP protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    /// HTTP/1.0
    Http10,
    /// HTTP/1.1
    Http11,
}

/// An ordered, case-insensitive multimap of header fields.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Headers {
    entries: Vec<(String, String)>,
}

impl Headers {
    /// Creates an empty header map.
    pub fn new() -> Headers {
        Headers::default()
    }

    /// Number of fields.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if there are no fields.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// First value of `name`.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// All values of `name`, in order.
    pub fn get_all<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.entries
            .iter()
            .filter(move |(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// Returns `true` if any value of `name`, split on commas, equals `token`
    /// (case-insensitively). Used for `Connection: close` and similar.
    pub fn has_token(&self, name: &str, token: &str) -> bool {
        self.get_all(name)
            .flat_map(|v| v.split(','))
            .any(|t| t.trim().eq_ignore_ascii_case(token))
    }

    /// Adds a field, keeping existing ones with the same name.
    pub fn append(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.entries.push((name.into(), value.into()));
    }

    /// Sets a field, replacing existing ones with the same name.
    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<String>) {
        let name = name.into();
        self.remove(&name);
        self.entries.push((name, value.into()));
    }

    /// Removes all fields named `name`.
    pub fn remove(&mut self, name: &str) {
        self.entries.retain(|(n, _)| !n.eq_ignore_ascii_case(name));
    }

    /// Returns `true` if a field named `name` exists.
    pub fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    /// Iterates over `(name, value)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.entries.iter().map(|(n, v)| (n.as_str(), v.as_str()))
    }
}

/// A parsed HTTP request with a fully buffered body.
#[derive(Debug, Clone)]
pub struct Request {
    /// The method.
    pub method: Method,
    /// The request target as sent (origin-form: path plus optional query).
    pub target: String,
    /// The protocol version.
    pub version: Version,
    /// Header fields.
    pub headers: Headers,
    /// The decoded body.
    pub body: Vec<u8>,
    /// Path parameters captured by the [`Router`](crate::Router).
    pub params: Vec<(String, String)>,
}

impl Request {
    /// The path component of the target.
    pub fn path(&self) -> &str {
        self.target.split('?').next().unwrap_or("")
    }

    /// The query string, if any.
    pub fn query(&self) -> Option<&str> {
        self.target.split_once('?').map(|(_, q)| q)
    }

    /// First value of a header.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name)
    }

    /// A path parameter captured by the router (e.g. `:id`).
    pub fn param(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    /// Whether the client wants the connection kept open after this request
    /// (HTTP/1.1 defaults to yes, HTTP/1.0 to no).
    pub fn keep_alive(&self) -> bool {
        match self.version {
            Version::Http11 => !self.headers.has_token("connection", "close"),
            Version::Http10 => self.headers.has_token("connection", "keep-alive"),
        }
    }
}

/// An HTTP status code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StatusCode(pub u16);

impl StatusCode {
    /// 200 OK
    pub const OK: StatusCode = StatusCode(200);
    /// 201 Created
    pub const CREATED: StatusCode = StatusCode(201);
    /// 204 No Content
    pub const NO_CONTENT: StatusCode = StatusCode(204);
    /// 304 Not Modified
    pub const NOT_MODIFIED: StatusCode = StatusCode(304);
    /// 400 Bad Request
    pub const BAD_REQUEST: StatusCode = StatusCode(400);
    /// 403 Forbidden
    pub const FORBIDDEN: StatusCode = StatusCode(403);
    /// 404 Not Found
    pub const NOT_FOUND: StatusCode = StatusCode(404);
    /// 405 Method Not Allowed
    pub const METHOD_NOT_ALLOWED: StatusCode = StatusCode(405);
    /// 408 Request Timeout
    pub const REQUEST_TIMEOUT: StatusCode = StatusCode(408);
    /// 500 Internal Server Error
    pub const INTERNAL_SERVER_ERROR: StatusCode = StatusCode(500);

    /// The canonical reason phrase.
    pub fn reason(self) -> &'static str {
        match self.0 {
            100 => "Continue",
            200 => "OK",
            201 => "Created",
            202 => "Accepted",
            204 => "No Content",
            301 => "Moved Permanently",
            302 => "Found",
            304 => "Not Modified",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            405 => "Method Not Allowed",
            408 => "Request Timeout",
            411 => "Length Required",
            413 => "Content Too Large",
            414 => "URI Too Long",
            431 => "Request Header Fields Too Large",
            500 => "Internal Server Error",
            501 => "Not Implemented",
            503 => "Service Unavailable",
            505 => "HTTP Version Not Supported",
            _ => "",
        }
    }

    /// 1xx, 204 and 304 responses never carry a body.
    pub fn allows_body(self) -> bool {
        !(self.0 < 200 || self.0 == 204 || self.0 == 304)
    }
}

/// A response body.
pub enum Body {
    /// No body.
    Empty,
    /// A complete body, sent with `Content-Length`.
    Full(Vec<u8>),
    /// A streamed body, sent with `Transfer-Encoding: chunked` (or buffered
    /// for HTTP/1.0 clients). Produced by [`Body::channel`].
    Stream(mpsc::Receiver<Vec<u8>>),
}

impl Body {
    /// Creates a streaming body and the sender that feeds it. Each message
    /// becomes one chunk; dropping the sender ends the body.
    pub fn channel(capacity: usize) -> (BodySender, Body) {
        let (tx, rx) = mpsc::channel(capacity);
        (BodySender { tx }, Body::Stream(rx))
    }
}

impl fmt::Debug for Body {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Body::Empty => f.write_str("Body::Empty"),
            Body::Full(b) => write!(f, "Body::Full({} bytes)", b.len()),
            Body::Stream(_) => f.write_str("Body::Stream"),
        }
    }
}

/// Feeds a streamed [`Body`].
#[derive(Debug, Clone)]
pub struct BodySender {
    tx: mpsc::Sender<Vec<u8>>,
}

impl BodySender {
    /// Sends one chunk. Fails if the client has gone away. Empty chunks are
    /// skipped (a zero-length chunk would terminate the body).
    pub async fn send(&self, chunk: impl Into<Vec<u8>>) -> Result<(), mpsc::SendError<Vec<u8>>> {
        let chunk = chunk.into();
        if chunk.is_empty() {
            return Ok(());
        }
        self.tx.send(chunk).await
    }
}

/// An HTTP response.
#[derive(Debug)]
pub struct Response {
    /// Status code.
    pub status: StatusCode,
    /// Header fields (framing headers are added automatically).
    pub headers: Headers,
    /// The body.
    pub body: Body,
}

impl Response {
    /// A response with the given status and no body.
    pub fn new(status: StatusCode) -> Response {
        Response {
            status,
            headers: Headers::new(),
            body: Body::Empty,
        }
    }

    /// A `200 OK` response with a body and content type.
    pub fn bytes(content_type: &str, body: impl Into<Vec<u8>>) -> Response {
        let mut res = Response::new(StatusCode::OK);
        res.headers.insert("content-type", content_type);
        res.body = Body::Full(body.into());
        res
    }

    /// `200 OK`, `text/plain`.
    pub fn text(body: impl Into<String>) -> Response {
        Response::bytes("text/plain; charset=utf-8", body.into().into_bytes())
    }

    /// `200 OK`, `text/html`.
    pub fn html(body: impl Into<String>) -> Response {
        Response::bytes("text/html; charset=utf-8", body.into().into_bytes())
    }

    /// `200 OK`, `application/json` (the body must already be JSON).
    pub fn json(body: impl Into<String>) -> Response {
        Response::bytes("application/json", body.into().into_bytes())
    }

    /// A streamed `200 OK` response.
    pub fn stream(content_type: &str, body: Body) -> Response {
        let mut res = Response::new(StatusCode::OK);
        res.headers.insert("content-type", content_type);
        res.body = body;
        res
    }

    /// A plain-text error response with the given status.
    pub fn error(status: StatusCode) -> Response {
        let mut res = Response::text(format!("{} {}\n", status.0, status.reason()));
        res.status = status;
        res
    }

    /// `404 Not Found`.
    pub fn not_found() -> Response {
        Response::error(StatusCode::NOT_FOUND)
    }

    /// Builder-style status setter.
    pub fn with_status(mut self, status: StatusCode) -> Response {
        self.status = status;
        self
    }

    /// Builder-style header setter.
    pub fn with_header(mut self, name: &str, value: impl Into<String>) -> Response {
        self.headers.insert(name, value);
        self
    }
}

/// Writes a status line and header block (ending with the blank line) into
/// `out`. `framing` is the `Content-Length`/`Transfer-Encoding` header.
pub(crate) fn write_head(
    out: &mut Vec<u8>,
    status: StatusCode,
    headers: &Headers,
    framing: Option<(&str, &str)>,
    close: bool,
) {
    use std::io::Write;
    let _ = write!(out, "HTTP/1.1 {} {}\r\n", status.0, status.reason());
    for (name, value) in headers.iter() {
        if name.eq_ignore_ascii_case("content-length")
            || name.eq_ignore_ascii_case("transfer-encoding")
            || name.eq_ignore_ascii_case("connection")
        {
            continue;
        }
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(value.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    if !headers.contains("date") {
        out.extend_from_slice(b"date: ");
        crate::date::with_http_date(|d| out.extend_from_slice(d.as_bytes()));
        out.extend_from_slice(b"\r\n");
    }
    if !headers.contains("server") {
        out.extend_from_slice(b"server: weft\r\n");
    }
    if let Some((name, value)) = framing {
        let _ = write!(out, "{name}: {value}\r\n");
    }
    if close {
        out.extend_from_slice(b"connection: close\r\n");
    }
    out.extend_from_slice(b"\r\n");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headers_are_case_insensitive() {
        let mut h = Headers::new();
        h.append("Content-Type", "a");
        h.append("X-Multi", "1");
        h.append("x-multi", "2");
        assert_eq!(h.get("content-type"), Some("a"));
        assert_eq!(h.get_all("X-MULTI").collect::<Vec<_>>(), ["1", "2"]);
        h.insert("X-Multi", "3");
        assert_eq!(h.get_all("x-multi").collect::<Vec<_>>(), ["3"]);
    }

    #[test]
    fn connection_tokens() {
        let mut h = Headers::new();
        h.append("Connection", "Upgrade, Close");
        assert!(h.has_token("connection", "close"));
        assert!(!h.has_token("connection", "keep-alive"));
    }

    #[test]
    fn head_serialization() {
        let mut out = Vec::new();
        let mut h = Headers::new();
        h.insert("content-type", "text/plain");
        h.insert("date", "fixed");
        h.insert("content-length", "999"); // overridden by framing
        write_head(
            &mut out,
            StatusCode::OK,
            &h,
            Some(("content-length", "2")),
            true,
        );
        let s = String::from_utf8(out).unwrap();
        assert_eq!(
            s,
            "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ndate: fixed\r\nserver: weft\r\ncontent-length: 2\r\nconnection: close\r\n\r\n"
        );
    }
}
