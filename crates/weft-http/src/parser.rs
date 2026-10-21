//! Incremental HTTP/1.1 request parser.
//!
//! The parser is fed the *unconsumed* bytes of a connection buffer each time
//! more data arrives. It never re-scans bytes it has already examined: it
//! remembers where the search for the end of the head stopped and, once the
//! head is parsed, how far body decoding (including the chunked
//! transfer-coding state machine) has progressed. A request is returned only
//! when it is complete, together with the number of bytes it occupied, so the
//! caller can immediately try to parse the next (pipelined) request from the
//! remaining bytes.
//!
//! Hardening:
//! * size limits on the head, the number of headers, and the body;
//! * strict token validation for methods and header names, and rejection of
//!   control characters in header values;
//! * requests carrying both `Content-Length` and `Transfer-Encoding`, or
//!   conflicting `Content-Length` values, are rejected (request smuggling);
//! * obsolete header line folding is rejected.

use std::fmt;

use crate::message::{Headers, Method, Request, Version};

/// Parser limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Maximum size of the request line plus headers, in bytes.
    pub max_head_size: usize,
    /// Maximum number of header fields.
    pub max_headers: usize,
    /// Maximum decoded body size, in bytes.
    pub max_body_size: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_head_size: 8 * 1024,
            max_headers: 64,
            max_body_size: 1024 * 1024,
        }
    }
}

/// Why a request could not be parsed. Each maps to an HTTP status code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// Malformed syntax (400).
    BadRequest(&'static str),
    /// Head larger than [`Limits::max_head_size`] or too many headers (431).
    HeadTooLarge,
    /// Body larger than [`Limits::max_body_size`] (413).
    BodyTooLarge,
    /// Unsupported HTTP version (505).
    UnsupportedVersion,
    /// Unsupported transfer coding (501).
    UnsupportedTransferEncoding,
}

impl ParseError {
    /// The status code to respond with.
    pub fn status_code(&self) -> u16 {
        match self {
            ParseError::BadRequest(_) => 400,
            ParseError::HeadTooLarge => 431,
            ParseError::BodyTooLarge => 413,
            ParseError::UnsupportedVersion => 505,
            ParseError::UnsupportedTransferEncoding => 501,
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::BadRequest(why) => write!(f, "bad request: {why}"),
            ParseError::HeadTooLarge => f.write_str("request header fields too large"),
            ParseError::BodyTooLarge => f.write_str("payload too large"),
            ParseError::UnsupportedVersion => f.write_str("HTTP version not supported"),
            ParseError::UnsupportedTransferEncoding => f.write_str("transfer coding not supported"),
        }
    }
}

impl std::error::Error for ParseError {}

#[derive(Debug)]
enum State {
    /// Looking for the blank line that ends the head. `scanned` bytes have
    /// already been searched.
    Head { scanned: usize },
    /// Head parsed; waiting for `len` body bytes starting at `body_start`.
    Fixed {
        request: Request,
        body_start: usize,
        len: usize,
    },
    /// Head parsed; decoding a chunked body. `pos` is the offset of the next
    /// undecoded byte.
    Chunked {
        request: Request,
        pos: usize,
        chunk: ChunkState,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkState {
    /// Expecting a chunk-size line.
    Size,
    /// Inside chunk data, `remaining` bytes left.
    Data { remaining: usize },
    /// Expecting the CRLF after chunk data.
    DataEnd,
    /// After the last (zero-size) chunk: skipping trailer fields.
    Trailers,
}

/// An incremental request parser. One per connection; reusable across
/// requests.
#[derive(Debug)]
pub struct Parser {
    limits: Limits,
    state: State,
}

impl Default for Parser {
    fn default() -> Self {
        Parser::new(Limits::default())
    }
}

impl Parser {
    /// Creates a parser with the given limits.
    pub fn new(limits: Limits) -> Parser {
        Parser {
            limits,
            state: State::Head { scanned: 0 },
        }
    }

    /// Returns `true` if part of a request has been received (used to pick
    /// between the idle and the header-read timeout).
    pub fn is_mid_request(&self, buffered: usize) -> bool {
        !matches!(self.state, State::Head { .. }) || buffered > 0
    }

    /// Parses `buf`, which must start at the first byte of the current
    /// request and contain every byte received since (callers append to it
    /// between calls and only drop bytes reported as consumed).
    ///
    /// Returns `Ok(Some((request, consumed)))` for a complete request,
    /// `Ok(None)` if more data is needed.
    pub fn parse(&mut self, buf: &[u8]) -> Result<Option<(Request, usize)>, ParseError> {
        loop {
            match &mut self.state {
                State::Head { scanned } => {
                    // Back up 3 bytes so a terminator split across reads is
                    // still found.
                    let from = scanned.saturating_sub(3);
                    let Some(end) = find_head_end(&buf[from..]).map(|i| from + i) else {
                        if buf.len() > self.limits.max_head_size {
                            return Err(ParseError::HeadTooLarge);
                        }
                        *scanned = buf.len();
                        return Ok(None);
                    };
                    let head_len = end + 4;
                    if head_len > self.limits.max_head_size {
                        return Err(ParseError::HeadTooLarge);
                    }
                    // Include the CRLF of the last header line, not the blank line.
                    let (request, framing) = parse_head(&buf[..end + 1], &self.limits)?;
                    self.state = match framing {
                        Framing::Length(len) => State::Fixed {
                            request,
                            body_start: head_len,
                            len,
                        },
                        Framing::Chunked => State::Chunked {
                            request,
                            pos: head_len,
                            chunk: ChunkState::Size,
                        },
                    };
                }
                State::Fixed {
                    body_start, len, ..
                } => {
                    let end = *body_start + *len;
                    if buf.len() < end {
                        return Ok(None);
                    }
                    let start = *body_start;
                    let State::Fixed { mut request, .. } = self.reset() else {
                        unreachable!()
                    };
                    request.body = buf[start..end].to_vec();
                    return Ok(Some((request, end)));
                }
                State::Chunked {
                    request,
                    pos,
                    chunk,
                } => match decode_chunked(buf, pos, chunk, &mut request.body, &self.limits)? {
                    false => return Ok(None),
                    true => {
                        let consumed = *pos;
                        let State::Chunked { request, .. } = self.reset() else {
                            unreachable!()
                        };
                        return Ok(Some((request, consumed)));
                    }
                },
            }
        }
    }

    fn reset(&mut self) -> State {
        std::mem::replace(&mut self.state, State::Head { scanned: 0 })
    }
}

enum Framing {
    Length(usize),
    Chunked,
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// RFC 9110 `tchar`.
fn is_token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

fn is_token(s: &[u8]) -> bool {
    !s.is_empty() && s.iter().all(|&b| is_token_byte(b))
}

/// Parses the request line and header fields. `head` ends with the `\r` of
/// the last line's CRLF.
fn parse_head(head: &[u8], limits: &Limits) -> Result<(Request, Framing), ParseError> {
    let mut lines = head.split(|&b| b == b'\n').map(|line| {
        line.strip_suffix(b"\r")
            .ok_or(ParseError::BadRequest("line not terminated by CRLF"))
    });

    let request_line = lines
        .next()
        .ok_or(ParseError::BadRequest("empty request"))??;
    let mut parts = request_line.split(|&b| b == b' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(ParseError::BadRequest("malformed request line"));
    };
    if !is_token(method) {
        return Err(ParseError::BadRequest("invalid method"));
    }
    if target.is_empty() || target.iter().any(|&b| b <= b' ' || b == 0x7f) {
        return Err(ParseError::BadRequest("invalid request target"));
    }
    let version = match version {
        b"HTTP/1.1" => Version::Http11,
        b"HTTP/1.0" => Version::Http10,
        v if v.starts_with(b"HTTP/") => return Err(ParseError::UnsupportedVersion),
        _ => return Err(ParseError::BadRequest("invalid version")),
    };
    let method = Method::from_bytes(method);
    let target = String::from_utf8(target.to_vec())
        .map_err(|_| ParseError::BadRequest("non-UTF-8 request target"))?;

    let mut headers = Headers::new();
    for line in lines {
        let line = line?;
        if line.first().is_some_and(|&b| b == b' ' || b == b'\t') {
            return Err(ParseError::BadRequest("obsolete line folding"));
        }
        let colon = line
            .iter()
            .position(|&b| b == b':')
            .ok_or(ParseError::BadRequest("header without colon"))?;
        let (name, value) = (&line[..colon], &line[colon + 1..]);
        if !is_token(name) {
            return Err(ParseError::BadRequest("invalid header name"));
        }
        let value = value.trim_ascii();
        if value.iter().any(|&b| (b < b' ' && b != b'\t') || b == 0x7f) {
            return Err(ParseError::BadRequest("invalid header value"));
        }
        if headers.len() >= limits.max_headers {
            return Err(ParseError::HeadTooLarge);
        }
        // Header names are ASCII tokens; values may contain obs-text, which
        // we map lossily rather than reject.
        let name = std::str::from_utf8(name).expect("token is ASCII");
        headers.append(name, String::from_utf8_lossy(value).into_owned());
    }

    if version == Version::Http11 && headers.get("host").is_none() {
        return Err(ParseError::BadRequest("missing Host header"));
    }

    let framing = body_framing(&headers, limits)?;
    let request = Request {
        method,
        target,
        version,
        headers,
        body: Vec::new(),
        params: Vec::new(),
    };
    Ok((request, framing))
}

/// Determines how the body is delimited (RFC 9112 §6.3).
fn body_framing(headers: &Headers, limits: &Limits) -> Result<Framing, ParseError> {
    let te = headers.get_all("transfer-encoding").collect::<Vec<_>>();
    let mut cls = headers.get_all("content-length");
    if !te.is_empty() {
        if cls.next().is_some() {
            return Err(ParseError::BadRequest(
                "both Content-Length and Transfer-Encoding",
            ));
        }
        let codings: Vec<&str> = te
            .iter()
            .flat_map(|v| v.split(','))
            .map(str::trim)
            .filter(|c| !c.is_empty())
            .collect();
        return match codings.as_slice() {
            [.., last] if last.eq_ignore_ascii_case("chunked") => {
                if codings.len() > 1 {
                    // gzip etc. under chunked: not supported.
                    Err(ParseError::UnsupportedTransferEncoding)
                } else {
                    Ok(Framing::Chunked)
                }
            }
            _ => Err(ParseError::UnsupportedTransferEncoding),
        };
    }
    let mut length: Option<usize> = None;
    for value in cls {
        for part in value.split(',').map(str::trim) {
            if part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()) {
                return Err(ParseError::BadRequest("invalid Content-Length"));
            }
            let n: usize = part.parse().map_err(|_| ParseError::BodyTooLarge)?;
            match length {
                Some(prev) if prev != n => {
                    return Err(ParseError::BadRequest("conflicting Content-Length"));
                }
                _ => length = Some(n),
            }
        }
    }
    let len = length.unwrap_or(0);
    if len > limits.max_body_size {
        return Err(ParseError::BodyTooLarge);
    }
    Ok(Framing::Length(len))
}

/// Advances the chunked decoder over `buf[*pos..]`, appending data to `body`.
/// Returns `Ok(true)` when the body (including trailers) is complete.
fn decode_chunked(
    buf: &[u8],
    pos: &mut usize,
    state: &mut ChunkState,
    body: &mut Vec<u8>,
    limits: &Limits,
) -> Result<bool, ParseError> {
    loop {
        match *state {
            ChunkState::Size => {
                let Some(line_end) = find_crlf(&buf[*pos..]) else {
                    if buf.len() - *pos > 1024 {
                        return Err(ParseError::BadRequest("chunk size line too long"));
                    }
                    return Ok(false);
                };
                let line = &buf[*pos..*pos + line_end];
                // Ignore chunk extensions.
                let size_str = line
                    .split(|&b| b == b';')
                    .next()
                    .unwrap_or(&[])
                    .trim_ascii();
                if size_str.is_empty() || size_str.len() > 16 {
                    return Err(ParseError::BadRequest("invalid chunk size"));
                }
                let size = std::str::from_utf8(size_str)
                    .ok()
                    .and_then(|s| usize::from_str_radix(s, 16).ok())
                    .ok_or(ParseError::BadRequest("invalid chunk size"))?;
                *pos += line_end + 2;
                if size == 0 {
                    *state = ChunkState::Trailers;
                } else {
                    if body.len().saturating_add(size) > limits.max_body_size {
                        return Err(ParseError::BodyTooLarge);
                    }
                    *state = ChunkState::Data { remaining: size };
                }
            }
            ChunkState::Data { remaining } => {
                let available = (buf.len() - *pos).min(remaining);
                body.extend_from_slice(&buf[*pos..*pos + available]);
                *pos += available;
                if available < remaining {
                    *state = ChunkState::Data {
                        remaining: remaining - available,
                    };
                    return Ok(false);
                }
                *state = ChunkState::DataEnd;
            }
            ChunkState::DataEnd => {
                if buf.len() - *pos < 2 {
                    return Ok(false);
                }
                if &buf[*pos..*pos + 2] != b"\r\n" {
                    return Err(ParseError::BadRequest("missing CRLF after chunk"));
                }
                *pos += 2;
                *state = ChunkState::Size;
            }
            ChunkState::Trailers => {
                let Some(line_end) = find_crlf(&buf[*pos..]) else {
                    return Ok(false);
                };
                *pos += line_end + 2;
                if line_end == 0 {
                    return Ok(true);
                }
                // Trailer fields are accepted and discarded.
            }
        }
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_all(input: &[u8]) -> Result<Vec<Request>, ParseError> {
        let mut parser = Parser::default();
        let mut out = Vec::new();
        let mut start = 0;
        while let Some((req, n)) = parser.parse(&input[start..])? {
            out.push(req);
            start += n;
        }
        assert_eq!(start, input.len(), "trailing bytes left unparsed");
        Ok(out)
    }

    #[test]
    fn simple_get() {
        let reqs =
            parse_all(b"GET /hello?x=1 HTTP/1.1\r\nHost: a\r\nUser-Agent: t\r\n\r\n").unwrap();
        assert_eq!(reqs.len(), 1);
        let r = &reqs[0];
        assert_eq!(r.method, Method::Get);
        assert_eq!(r.path(), "/hello");
        assert_eq!(r.query(), Some("x=1"));
        assert_eq!(r.header("user-agent"), Some("t"));
        assert_eq!(r.header("HOST"), Some("a"));
        assert!(r.body.is_empty());
    }

    #[test]
    fn content_length_body() {
        let reqs =
            parse_all(b"POST /p HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhello").unwrap();
        assert_eq!(reqs[0].body, b"hello");
    }

    #[test]
    fn pipelined_requests() {
        let input = b"GET /a HTTP/1.1\r\nHost: x\r\n\r\nPOST /b HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabcGET /c HTTP/1.1\r\nHost: x\r\n\r\n";
        let reqs = parse_all(input).unwrap();
        let paths: Vec<_> = reqs.iter().map(|r| r.path().to_string()).collect();
        assert_eq!(paths, ["/a", "/b", "/c"]);
        assert_eq!(reqs[1].body, b"abc");
    }

    #[test]
    fn byte_at_a_time() {
        let input = b"POST /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n4;ext=1\r\nWiki\r\n5\r\npedia\r\n0\r\nTrailer: v\r\n\r\nGET /y HTTP/1.1\r\nHost: h\r\n\r\n";
        let mut parser = Parser::default();
        let mut buf = Vec::new();
        let mut got = Vec::new();
        for &b in input.iter() {
            buf.push(b);
            while let Some((req, n)) = parser.parse(&buf).unwrap() {
                buf.drain(..n);
                got.push(req);
            }
        }
        assert!(buf.is_empty());
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].body, b"Wikipedia");
        assert_eq!(got[1].path(), "/y");
    }

    #[test]
    fn every_split_point_yields_same_result() {
        let input: &[u8] = b"PUT /k HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n";
        for split in 0..input.len() {
            let mut parser = Parser::default();
            assert!(
                parser.parse(&input[..split]).unwrap().is_none(),
                "split {split}"
            );
            let (req, n) = parser.parse(input).unwrap().expect("complete");
            assert_eq!(n, input.len());
            assert_eq!(req.body, b"abc");
        }
    }

    #[test]
    fn http10_without_host() {
        let reqs = parse_all(b"GET / HTTP/1.0\r\n\r\n").unwrap();
        assert_eq!(reqs[0].version, Version::Http10);
        assert!(!reqs[0].keep_alive());
    }

    #[test]
    fn keep_alive_rules() {
        let r = &parse_all(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n").unwrap()[0];
        assert!(r.keep_alive());
        let r = &parse_all(b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n").unwrap()[0];
        assert!(!r.keep_alive());
        let r = &parse_all(b"GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n").unwrap()[0];
        assert!(r.keep_alive());
    }

    #[test]
    fn rejects_malformed_input() {
        let cases: &[&[u8]] = &[
            b"GET /\r\n\r\n",
            b"GET / HTTP/1.1\r\n\r\n", // missing Host
            b"G@T / HTTP/1.1\r\nHost: a\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: a\r\nBad Header: x\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: a\r\n folded\r\n\r\n",
            b"GET / HTTP/1.1\nHost: a\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nab",
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\nTransfer-Encoding: chunked\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: -1\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n",
            b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nabc",
        ];
        for case in cases {
            let res = Parser::default().parse(case);
            assert!(
                matches!(res, Err(ParseError::BadRequest(_))),
                "{:?} => {res:?}",
                String::from_utf8_lossy(case)
            );
        }
    }

    #[test]
    fn version_and_coding_errors() {
        assert_eq!(
            Parser::default()
                .parse(b"GET / HTTP/2.0\r\n\r\n")
                .unwrap_err(),
            ParseError::UnsupportedVersion
        );
        assert_eq!(
            Parser::default()
                .parse(b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: gzip\r\n\r\n")
                .unwrap_err(),
            ParseError::UnsupportedTransferEncoding
        );
    }

    #[test]
    fn enforces_limits() {
        let limits = Limits {
            max_head_size: 64,
            max_headers: 2,
            max_body_size: 4,
        };
        let big_head = format!(
            "GET / HTTP/1.1\r\nHost: a\r\nX: {}\r\n\r\n",
            "y".repeat(100)
        );
        assert_eq!(
            Parser::new(limits).parse(big_head.as_bytes()).unwrap_err(),
            ParseError::HeadTooLarge
        );
        // An unterminated head that is already too big fails early.
        assert_eq!(
            Parser::new(limits).parse(&[b'a'; 100]).unwrap_err(),
            ParseError::HeadTooLarge
        );
        assert_eq!(
            Parser::new(limits)
                .parse(b"GET / HTTP/1.1\r\nHost: a\r\nA: 1\r\nB: 2\r\n\r\n")
                .unwrap_err(),
            ParseError::HeadTooLarge
        );
        assert_eq!(
            Parser::new(limits)
                .parse(b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\n")
                .unwrap_err(),
            ParseError::BodyTooLarge
        );
        assert_eq!(
            Parser::new(limits)
                .parse(b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n5\r\n")
                .unwrap_err(),
            ParseError::BodyTooLarge
        );
    }

    #[test]
    fn head_scan_is_incremental() {
        // Feed a long head in pieces; the terminator straddles two reads.
        let mut input = b"GET / HTTP/1.1\r\nHost: a\r\n".to_vec();
        input.extend_from_slice(b"X-Pad: 0123456789\r\n\r");
        let mut parser = Parser::default();
        assert!(parser.parse(&input).unwrap().is_none());
        input.extend_from_slice(b"\n");
        let (req, n) = parser.parse(&input).unwrap().unwrap();
        assert_eq!(n, input.len());
        assert_eq!(req.header("x-pad"), Some("0123456789"));
    }
}
