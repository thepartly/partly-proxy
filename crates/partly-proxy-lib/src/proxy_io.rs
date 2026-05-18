//! In-memory request and response types passed through the middleware chain.
//!
//! See `SPECIFICATION.md` §6.1 — bodies are fully-materialised `Bytes`, not a
//! streaming `Body`. Middleware can mutate every field in place.

use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode, Uri, Version};

/// Fully-materialised proxy request as seen by middleware.
#[derive(Debug, Clone)]
pub struct ProxyRequest {
    pub method: Method,
    pub uri: Uri,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub version: Version,
}

impl ProxyRequest {
    /// Build from raw fields. The most common construction path; the
    /// listener calls this once the inbound body has been collected.
    pub fn new(method: Method, uri: Uri, headers: HeaderMap, body: Bytes) -> Self {
        Self {
            method,
            uri,
            headers,
            body,
            version: Version::HTTP_11,
        }
    }

    /// Convenience: mutable handle on the body bytes.
    pub fn body_mut(&mut self) -> &mut Bytes {
        &mut self.body
    }

    /// Replace the body and return the old one — useful when the middleware
    /// wants to inspect the original before writing a new payload.
    pub fn take_body(&mut self) -> Bytes {
        std::mem::take(&mut self.body)
    }

    /// Replace the body wholesale.
    pub fn set_body(&mut self, body: impl Into<Bytes>) {
        self.body = body.into();
    }
}

/// Fully-materialised proxy response as seen by middleware.
#[derive(Debug, Clone)]
pub struct ProxyResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub version: Version,
}

impl ProxyResponse {
    /// Build an empty-body response with `Content-Length: 0` semantics.
    pub fn new(status: StatusCode) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
            body: Bytes::new(),
            version: Version::HTTP_11,
        }
    }

    /// Fluent header setter.
    pub fn with_header(
        mut self,
        name: impl http::header::IntoHeaderName,
        value: impl Into<bytes::Bytes>,
    ) -> Self {
        let bytes = value.into();
        if let Ok(v) = http::HeaderValue::from_bytes(&bytes) {
            self.headers.insert(name, v);
        }
        self
    }

    /// Fluent body setter.
    pub fn with_body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = body.into();
        self
    }

    /// Convenience: mutable handle on the body bytes.
    pub fn body_mut(&mut self) -> &mut Bytes {
        &mut self.body
    }

    /// Take and replace the body, returning the previous value.
    pub fn take_body(&mut self) -> Bytes {
        std::mem::take(&mut self.body)
    }

    /// Replace the body wholesale.
    pub fn set_body(&mut self, body: impl Into<Bytes>) {
        self.body = body.into();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_body_helpers_round_trip() {
        let mut r = ProxyRequest::new(
            Method::POST,
            "/x".parse().unwrap(),
            HeaderMap::new(),
            Bytes::from_static(b"a"),
        );
        assert_eq!(r.body, Bytes::from_static(b"a"));
        r.set_body(Bytes::from_static(b"b"));
        assert_eq!(r.body, Bytes::from_static(b"b"));
        let prev = r.take_body();
        assert_eq!(prev, Bytes::from_static(b"b"));
        assert!(r.body.is_empty());
        *r.body_mut() = Bytes::from_static(b"c");
        assert_eq!(r.body, Bytes::from_static(b"c"));
    }

    #[test]
    fn response_builder_chains_status_headers_body() {
        let r = ProxyResponse::new(StatusCode::CREATED)
            .with_header("x-test", Bytes::from_static(b"yes"))
            .with_body(Bytes::from_static(b"hello"));
        assert_eq!(r.status, StatusCode::CREATED);
        assert_eq!(
            r.headers.get("x-test").and_then(|v| v.to_str().ok()),
            Some("yes")
        );
        assert_eq!(r.body, Bytes::from_static(b"hello"));
    }
}
