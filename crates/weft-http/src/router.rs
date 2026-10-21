//! A small path router with `:param` and `*wildcard` segments.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::message::{Method, Request, Response, StatusCode};
use crate::server::Handler;

/// A boxed response future.
pub type BoxFuture = Pin<Box<dyn Future<Output = Response> + Send>>;

trait ErasedHandler: Send + Sync {
    fn call(&self, req: Request) -> BoxFuture;
}

impl<H: Handler> ErasedHandler for H {
    fn call(&self, req: Request) -> BoxFuture {
        Box::pin(Handler::call(self, req))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Static(String),
    Param(String),
    /// Matches the rest of the path (possibly empty).
    Wildcard(String),
}

struct Route {
    method: Method,
    segments: Vec<Segment>,
    handler: Arc<dyn ErasedHandler>,
}

/// Dispatches requests to handlers by method and path.
///
/// * `/users/:id` captures one segment as `id` ([`Request::param`]).
/// * `/static/*path` captures the remainder of the path as `path`.
/// * Routes are tried in registration order.
/// * `HEAD` falls back to the matching `GET` route.
/// * A path that matches with the wrong method yields `405` with an `Allow`
///   header; no match yields the fallback handler or `404`.
///
/// ```
/// use weft_http::{Method, Request, Response, Router};
///
/// let router = Router::new()
///     .get("/", |_req: Request| async { Response::text("home") })
///     .get("/users/:id", |req: Request| async move {
///         Response::text(format!("user {}", req.param("id").unwrap()))
///     })
///     .route(Method::Post, "/echo", |req: Request| async move {
///         Response::bytes("application/octet-stream", req.body)
///     });
/// # let _ = router;
/// ```
#[derive(Default)]
pub struct Router {
    routes: Vec<Route>,
    fallback: Option<Arc<dyn ErasedHandler>>,
}

impl std::fmt::Debug for Router {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let routes: Vec<String> = self
            .routes
            .iter()
            .map(|r| format!("{} {:?}", r.method, r.segments))
            .collect();
        f.debug_struct("Router").field("routes", &routes).finish()
    }
}

fn parse_pattern(pattern: &str) -> Vec<Segment> {
    assert!(
        pattern.starts_with('/'),
        "route patterns must start with '/'"
    );
    let mut segments = Vec::new();
    let parts: Vec<&str> = pattern.split('/').filter(|s| !s.is_empty()).collect();
    for (i, part) in parts.iter().enumerate() {
        if let Some(name) = part.strip_prefix(':') {
            segments.push(Segment::Param(name.to_string()));
        } else if let Some(name) = part.strip_prefix('*') {
            assert!(i == parts.len() - 1, "wildcard must be the last segment");
            segments.push(Segment::Wildcard(name.to_string()));
        } else {
            segments.push(Segment::Static(part.to_string()));
        }
    }
    segments
}

fn match_path(segments: &[Segment], path: &str) -> Option<Vec<(String, String)>> {
    let mut params = Vec::new();
    let mut rest = path.trim_start_matches('/');
    for segment in segments {
        if let Segment::Wildcard(name) = segment {
            params.push((name.clone(), rest.to_string()));
            return Some(params);
        }
        if rest.is_empty() {
            return None;
        }
        let (part, tail) = rest.split_once('/').unwrap_or((rest, ""));
        match segment {
            Segment::Static(s) if s == part => {}
            Segment::Param(name) => params.push((name.clone(), part.to_string())),
            _ => return None,
        }
        rest = tail;
    }
    rest.trim_end_matches('/').is_empty().then_some(params)
}

impl Router {
    /// Creates an empty router.
    pub fn new() -> Router {
        Router::default()
    }

    /// Adds a route.
    pub fn route(mut self, method: Method, pattern: &str, handler: impl Handler) -> Router {
        self.routes.push(Route {
            method,
            segments: parse_pattern(pattern),
            handler: Arc::new(handler),
        });
        self
    }

    /// Adds a `GET` route.
    pub fn get(self, pattern: &str, handler: impl Handler) -> Router {
        self.route(Method::Get, pattern, handler)
    }

    /// Adds a `POST` route.
    pub fn post(self, pattern: &str, handler: impl Handler) -> Router {
        self.route(Method::Post, pattern, handler)
    }

    /// Adds a `PUT` route.
    pub fn put(self, pattern: &str, handler: impl Handler) -> Router {
        self.route(Method::Put, pattern, handler)
    }

    /// Adds a `DELETE` route.
    pub fn delete(self, pattern: &str, handler: impl Handler) -> Router {
        self.route(Method::Delete, pattern, handler)
    }

    /// Sets the handler used when no route matches.
    pub fn fallback(mut self, handler: impl Handler) -> Router {
        self.fallback = Some(Arc::new(handler));
        self
    }

    fn dispatch(&self, mut req: Request) -> BoxFuture {
        let path = req.path().to_string();
        let mut allowed: Vec<&Method> = Vec::new();
        for route in &self.routes {
            let Some(params) = match_path(&route.segments, &path) else {
                continue;
            };
            let method_ok = route.method == req.method
                || (req.method == Method::Head && route.method == Method::Get);
            if method_ok {
                req.params = params;
                return route.handler.call(req);
            }
            allowed.push(&route.method);
        }
        if !allowed.is_empty() {
            let allow = allowed
                .iter()
                .map(|m| m.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let res = Response::error(StatusCode::METHOD_NOT_ALLOWED).with_header("allow", allow);
            return Box::pin(async move { res });
        }
        match &self.fallback {
            Some(handler) => handler.call(req),
            None => Box::pin(async { Response::not_found() }),
        }
    }
}

impl Handler for Router {
    type Future = BoxFuture;

    fn call(&self, req: Request) -> BoxFuture {
        self.dispatch(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching() {
        let p = parse_pattern("/users/:id/posts/:post");
        let params = match_path(&p, "/users/7/posts/42").unwrap();
        assert_eq!(
            params,
            vec![("id".into(), "7".into()), ("post".into(), "42".into())]
        );
        assert!(match_path(&p, "/users/7/posts").is_none());
        assert!(match_path(&p, "/users/7/posts/42/extra").is_none());

        let w = parse_pattern("/static/*path");
        assert_eq!(
            match_path(&w, "/static/css/site.css").unwrap(),
            vec![("path".into(), "css/site.css".into())]
        );
        assert_eq!(match_path(&w, "/static").unwrap()[0].1, "");
        assert!(match_path(&parse_pattern("/"), "/").is_some());
        assert!(match_path(&parse_pattern("/a"), "/a/").is_some());
        assert!(match_path(&parse_pattern("/a"), "/b").is_none());
    }
}
