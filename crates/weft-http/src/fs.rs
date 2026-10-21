//! Static file serving.

use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use crate::message::{Request, Response, StatusCode};
use crate::router::BoxFuture;
use crate::server::Handler;

/// Serves files from a directory.
///
/// The file path is taken from the router's `path` wildcard parameter if
/// present (mount it as `/static/*path`), otherwise from the full request
/// path. Requests are percent-decoded and then checked twice against
/// directory traversal: `..` components are rejected outright, and the
/// canonicalised path (after following symlinks) must stay inside the root.
/// Files are read on the blocking pool so workers never block on disk I/O.
#[derive(Debug, Clone)]
pub struct StaticFiles {
    root: Arc<PathBuf>,
}

impl StaticFiles {
    /// Serves files below `root`.
    pub fn new(root: impl AsRef<Path>) -> io::Result<StaticFiles> {
        Ok(StaticFiles {
            root: Arc::new(root.as_ref().canonicalize()?),
        })
    }
}

/// Decodes `%XX` escapes. Returns `None` for malformed escapes or non-UTF-8.
fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3)?;
            let hex = std::str::from_utf8(hex).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Turns a URL path into a relative filesystem path, rejecting anything that
/// could escape the root.
fn sanitize(path: &str) -> Option<PathBuf> {
    let decoded = percent_decode(path)?;
    if decoded.contains('\0') || decoded.contains('\\') {
        return None;
    }
    let mut out = PathBuf::new();
    for component in Path::new(decoded.trim_start_matches('/')).components() {
        match component {
            Component::Normal(c) => out.push(c),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    Some(out)
}

/// Content type from the file extension.
pub fn mime_type(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("html" | "htm") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("json") => "application/json",
        Some("txt") => "text/plain; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("ico") => "image/x-icon",
        Some("wasm") => "application/wasm",
        Some("pdf") => "application/pdf",
        _ => "application/octet-stream",
    }
}

fn read_file(root: &Path, rel: &Path) -> Result<(PathBuf, Vec<u8>), StatusCode> {
    let mut path = root.join(rel);
    if path.is_dir() {
        path.push("index.html");
    }
    let canonical = path.canonicalize().map_err(|_| StatusCode::NOT_FOUND)?;
    if !canonical.starts_with(root) {
        // A symlink pointing outside the root.
        return Err(StatusCode::FORBIDDEN);
    }
    let data = std::fs::read(&canonical).map_err(|e| match e.kind() {
        io::ErrorKind::PermissionDenied => StatusCode::FORBIDDEN,
        _ => StatusCode::NOT_FOUND,
    })?;
    Ok((canonical, data))
}

impl Handler for StaticFiles {
    type Future = BoxFuture;

    fn call(&self, req: Request) -> BoxFuture {
        let root = self.root.clone();
        let raw = req
            .param("path")
            .map(str::to_owned)
            .unwrap_or_else(|| req.path().to_owned());
        Box::pin(async move {
            let Some(rel) = sanitize(&raw) else {
                return Response::error(StatusCode::FORBIDDEN);
            };
            let result = weft::spawn_blocking(move || read_file(&root, &rel)).await;
            match result {
                Ok(Ok((path, data))) => Response::bytes(mime_type(&path), data)
                    .with_header("cache-control", "public, max-age=60"),
                Ok(Err(status)) => Response::error(status),
                Err(_) => Response::error(StatusCode::INTERNAL_SERVER_ERROR),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_rejects_traversal() {
        assert_eq!(sanitize("/a/b.txt"), Some(PathBuf::from("a/b.txt")));
        assert_eq!(sanitize("./a/./b"), Some(PathBuf::from("a/b")));
        assert_eq!(sanitize("%61.txt"), Some(PathBuf::from("a.txt")));
        assert_eq!(sanitize("../etc/passwd"), None);
        assert_eq!(sanitize("a/../../x"), None);
        assert_eq!(sanitize("%2e%2e/secret"), None);
        assert_eq!(sanitize("a%00b"), None);
        assert_eq!(sanitize("a\\..\\b"), None);
        assert_eq!(sanitize("%zz"), None);
    }

    #[test]
    fn mime_types() {
        assert_eq!(mime_type(Path::new("x.HTML")), "text/html; charset=utf-8");
        assert_eq!(
            mime_type(Path::new("x.unknown")),
            "application/octet-stream"
        );
    }
}
