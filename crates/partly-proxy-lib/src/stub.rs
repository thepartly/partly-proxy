//! Stub matcher, stubbed response, and the per-upstream stub store.
//!
//! See `SPECIFICATION.md` §7. A stub binds a [`RequestMatcher`] to a
//! [`StubbedResponse`]. When a stub fires, the proxy honours its optional
//! delay, decrements its fire count, and auto-removes the stub when the
//! counter reaches zero. `times: None` means "unlimited".

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::header::{HeaderName, HeaderValue};
use http::{HeaderMap, Method, StatusCode, Version};
use regex::Regex;
use tokio::sync::RwLock;

use crate::proxy_io::{ProxyRequest, ProxyResponse};

/// Matcher predicate. All set fields must agree for the stub to fire. Unset
/// fields are ignored. See §7.1.
#[derive(Debug, Default, Clone)]
pub struct RequestMatcher {
    method: Option<Method>,
    path_pattern: Option<PathPattern>,
    header_contains: HashMap<String, String>,
    body_contains: Option<Bytes>,
}

/// Either a compiled regex (matched against `uri.path()`) or an exact-string
/// path. If the pattern compiles as a regex it is treated as such; otherwise
/// the proxy falls back to literal-equality comparison (per spec §7.1).
#[derive(Debug, Clone)]
enum PathPattern {
    Regex(Regex),
    Exact(String),
}

impl RequestMatcher {
    /// Fresh matcher — matches every request until at least one field is
    /// constrained.
    pub fn new() -> Self {
        Self::default()
    }

    /// Constrain by HTTP method (exact match).
    pub fn method(mut self, method: Method) -> Self {
        self.method = Some(method);
        self
    }

    /// Constrain by path. Parsed as a regex; if the regex fails to compile,
    /// the raw string is used as an exact-equality match.
    pub fn path(mut self, pattern: impl Into<String>) -> Self {
        let raw = pattern.into();
        let parsed = match Regex::new(&raw) {
            Ok(r) => PathPattern::Regex(r),
            Err(_) => PathPattern::Exact(raw),
        };
        self.path_pattern = Some(parsed);
        self
    }

    /// Require a header to be present with the given substring. Multiple
    /// calls accumulate; every header constraint must pass.
    pub fn header(mut self, name: impl Into<String>, value_substr: impl Into<String>) -> Self {
        self.header_contains
            .insert(name.into().to_ascii_lowercase(), value_substr.into());
        self
    }

    /// Require the request body to contain `needle` as a UTF-8-lossy
    /// substring.
    pub fn body_contains(mut self, needle: impl Into<Bytes>) -> Self {
        self.body_contains = Some(needle.into());
        self
    }

    /// Whether `req` satisfies this matcher.
    pub fn matches(&self, req: &ProxyRequest) -> bool {
        if let Some(m) = &self.method {
            if req.method != *m {
                return false;
            }
        }
        if let Some(p) = &self.path_pattern {
            let path = req.uri.path();
            let matched = match p {
                PathPattern::Regex(r) => r.is_match(path),
                PathPattern::Exact(s) => path == s,
            };
            if !matched {
                return false;
            }
        }
        for (name, needle) in &self.header_contains {
            let found = req
                .headers
                .get(name.as_str())
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.contains(needle.as_str()));
            if !found {
                return false;
            }
        }
        if let Some(needle) = &self.body_contains {
            let haystack = String::from_utf8_lossy(&req.body);
            let needle_str = String::from_utf8_lossy(needle);
            if !haystack.contains(needle_str.as_ref()) {
                return false;
            }
        }
        true
    }
}

/// A canned response — see §7.
#[derive(Debug, Clone)]
pub struct StubbedResponse {
    pub status: StatusCode,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
    pub delay: Option<Duration>,
}

impl StubbedResponse {
    pub fn new(status: StatusCode) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Bytes::new(),
            delay: None,
        }
    }

    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = body.into();
        self
    }

    pub fn delay(mut self, d: Duration) -> Self {
        self.delay = Some(d);
        self
    }

    /// Materialise into a `ProxyResponse`. Invalid header names/values are
    /// silently dropped — stubs are caller-controlled test fixtures, not
    /// untrusted input.
    pub fn into_proxy(self) -> ProxyResponse {
        let mut headers = HeaderMap::new();
        for (n, v) in self.headers {
            if let (Ok(name), Ok(val)) = (
                HeaderName::from_bytes(n.as_bytes()),
                HeaderValue::from_bytes(v.as_bytes()),
            ) {
                headers.insert(name, val);
            }
        }
        ProxyResponse {
            status: self.status,
            headers,
            body: self.body,
            version: Version::HTTP_11,
        }
    }
}

/// One stub entry in the per-upstream store.
#[derive(Debug, Clone)]
pub struct StubEntry {
    pub matcher: RequestMatcher,
    pub response: StubbedResponse,
    /// `None` ⇒ unlimited fires; `Some(n)` ⇒ fires at most `n` times before
    /// auto-removal.
    pub times: Option<u32>,
}

/// Per-upstream stub store. Cheap to clone — wraps a single async `RwLock`.
#[derive(Debug, Clone, Default)]
pub struct StubStore {
    inner: Arc<RwLock<Vec<StubEntry>>>,
}

impl StubStore {
    /// Append a stub. Stubs are tried in insertion order; the first match
    /// fires.
    pub async fn add(&self, entry: StubEntry) {
        self.inner.write().await.push(entry);
    }

    /// Remove every stub.
    pub async fn clear(&self) {
        self.inner.write().await.clear();
    }

    /// Number of stubs currently registered.
    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }

    /// Whether the store is currently empty.
    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.is_empty()
    }

    /// Try to find a stub that matches `req`. If found, decrement the fire
    /// counter (auto-removing the stub when it reaches zero) and return the
    /// matching response and its delay.
    ///
    /// The delay is *not* applied here — the caller is responsible for
    /// `sleep(delay)` after releasing the lock, so we don't hold the write
    /// lock during the artificial wait.
    pub async fn take_match(
        &self,
        req: &ProxyRequest,
    ) -> Option<(StubbedResponse, Option<Duration>)> {
        let mut stubs = self.inner.write().await;
        let idx = stubs.iter().position(|e| e.matcher.matches(req))?;
        let response = stubs[idx].response.clone();
        let delay = response.delay;
        let exhaust = if let Some(remaining) = &mut stubs[idx].times {
            *remaining = remaining.saturating_sub(1);
            *remaining == 0
        } else {
            false
        };
        if exhaust {
            stubs.remove(idx);
        }
        Some((response, delay))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn req(path: &str) -> ProxyRequest {
        ProxyRequest::new(
            Method::GET,
            path.parse().unwrap(),
            HeaderMap::new(),
            Bytes::new(),
        )
    }

    #[test]
    fn empty_matcher_matches_everything() {
        let m = RequestMatcher::new();
        assert!(m.matches(&req("/anything")));
    }

    #[test]
    fn method_constraint_is_exact() {
        let m = RequestMatcher::new().method(Method::POST);
        assert!(!m.matches(&req("/x")));
        let mut r = req("/x");
        r.method = Method::POST;
        assert!(m.matches(&r));
    }

    #[test]
    fn path_regex_matches() {
        let m = RequestMatcher::new().path(r"^/orders/\d+/refund$");
        assert!(m.matches(&req("/orders/123/refund")));
        assert!(!m.matches(&req("/orders/abc/refund")));
        assert!(!m.matches(&req("/refund")));
    }

    #[test]
    fn path_falls_back_to_exact_when_regex_invalid() {
        // `[` is an unfinished character class.
        let m = RequestMatcher::new().path("[broken");
        assert!(!m.matches(&req("/anything")));
        // The literal exact string match still works.
        let mut r = req("/");
        r.uri = "[broken".parse().unwrap_or_else(|_| "/".parse().unwrap());
        // Most platforms will reject "[broken" as a URI; ensure the test
        // is at least well-formed by exercising the path-string equality.
        // We cannot construct a Uri whose path is "[broken" via the
        // standard parser, so the regex-invalid case is asserted via the
        // negative case above.
    }

    #[test]
    fn header_substring_constraint() {
        let mut r = req("/x");
        r.headers.insert("x-tenant", "acme-prod".parse().unwrap());
        let m = RequestMatcher::new().header("x-tenant", "acme");
        assert!(m.matches(&r));
        let m = RequestMatcher::new().header("x-tenant", "globex");
        assert!(!m.matches(&r));
    }

    #[test]
    fn body_substring_constraint() {
        let mut r = req("/x");
        r.body = Bytes::from_static(b"{\"reason\":\"chargeback\"}");
        let m = RequestMatcher::new().body_contains(Bytes::from_static(b"chargeback"));
        assert!(m.matches(&r));
        let m = RequestMatcher::new().body_contains(Bytes::from_static(b"refund"));
        assert!(!m.matches(&r));
    }

    #[tokio::test]
    async fn stub_store_returns_first_match_and_decrements() {
        let store = StubStore::default();
        store
            .add(StubEntry {
                matcher: RequestMatcher::new().path("/a"),
                response: StubbedResponse::new(StatusCode::OK).body(Bytes::from_static(b"first")),
                times: Some(2),
            })
            .await;
        store
            .add(StubEntry {
                matcher: RequestMatcher::new().path("/a"),
                response: StubbedResponse::new(StatusCode::OK).body(Bytes::from_static(b"second")),
                times: None,
            })
            .await;

        let r = req("/a");
        let (a, _) = store.take_match(&r).await.unwrap();
        assert_eq!(a.body, Bytes::from_static(b"first"));
        let (b, _) = store.take_match(&r).await.unwrap();
        assert_eq!(b.body, Bytes::from_static(b"first"));
        // First stub now exhausted; next match falls through to second.
        let (c, _) = store.take_match(&r).await.unwrap();
        assert_eq!(c.body, Bytes::from_static(b"second"));
        assert_eq!(store.len().await, 1);
    }

    #[tokio::test]
    async fn unlimited_stub_keeps_firing() {
        let store = StubStore::default();
        store
            .add(StubEntry {
                matcher: RequestMatcher::new().path("/a"),
                response: StubbedResponse::new(StatusCode::OK),
                times: None,
            })
            .await;
        for _ in 0..1000 {
            assert!(store.take_match(&req("/a")).await.is_some());
        }
        assert_eq!(store.len().await, 1);
    }

    #[tokio::test]
    async fn no_match_leaves_store_unchanged() {
        let store = StubStore::default();
        store
            .add(StubEntry {
                matcher: RequestMatcher::new().path("/exact"),
                response: StubbedResponse::new(StatusCode::OK),
                times: Some(1),
            })
            .await;
        assert!(store.take_match(&req("/other")).await.is_none());
        assert_eq!(store.len().await, 1);
    }

    #[test]
    fn stubbed_response_into_proxy_carries_headers_and_body() {
        let r = StubbedResponse::new(StatusCode::CREATED)
            .header("content-type", "application/json")
            .body(Bytes::from_static(b"{\"ok\":true}"));
        let pr = r.into_proxy();
        assert_eq!(pr.status, StatusCode::CREATED);
        assert_eq!(
            pr.headers.get("content-type").and_then(|v| v.to_str().ok()),
            Some("application/json")
        );
        assert_eq!(pr.body, Bytes::from_static(b"{\"ok\":true}"));
    }
}
