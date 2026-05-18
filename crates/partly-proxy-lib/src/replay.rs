//! Replay source — see `SPECIFICATION.md` §8.
//!
//! A `ReplaySource` is an immutable bundle of recorded exchanges plus a
//! match strategy. The two supported strategies (per §8.1) are:
//!
//! - `MethodPathAndBodyHash` — O(1) hash-indexed lookup, built once at
//!   construction. The default.
//! - `Custom(closure)` — linear scan with a user predicate. Use sparingly
//!   on large snapshots (§8.1.1).
//!
//! Lookups go through every middleware's `redact_request_for_snapshot`
//! before the lookup key is computed (§8.2.1), so a request that carried
//! a live `Authorization` header still matches a snapshot recorded with
//! that header stripped.

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;

use crate::error::{ProxyError, Result};
use crate::middleware::{self, SharedMiddleware};
use crate::proxy_io::{ProxyRequest, ProxyResponse};
use crate::recorded::{sha256_hex, ExchangeOutcome, RecordedExchange, RecordedRequest};

/// Predicate type used by [`MatchStrategy::Custom`]. Defined as a type alias
/// so the trait-object type doesn't trip clippy's `type_complexity` lint.
pub type CustomMatcher = Arc<dyn Fn(&RecordedRequest, &ProxyRequest) -> bool + Send + Sync>;

/// Match strategy for a [`ReplaySource`].
#[derive(Clone, Default)]
pub enum MatchStrategy {
    /// `(method, uri.path(), sha256_hex(body))`. Hash-indexed; O(1) lookup.
    #[default]
    MethodPathAndBodyHash,
    /// User-supplied predicate; falls back to a linear scan over every
    /// exchange. The closure is invoked with the on-disk
    /// [`RecordedRequest`] and the live (already-redacted) [`ProxyRequest`].
    Custom(CustomMatcher),
}

impl std::fmt::Debug for MatchStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MethodPathAndBodyHash => f.write_str("MethodPathAndBodyHash"),
            Self::Custom(_) => f.write_str("Custom(<closure>)"),
        }
    }
}

/// Key used by `MethodPathAndBodyHash`: (method, path, body sha-256 hex).
type IndexKey = (String, String, String);

/// Cheap-to-clone replay source. Behind an `Arc`, so several listeners can
/// share one source.
#[derive(Clone)]
pub struct ReplaySource {
    inner: Arc<ReplaySourceInner>,
}

struct ReplaySourceInner {
    strategy: MatchStrategy,
    exchanges: Vec<RecordedExchange>,
    /// Populated only for `MethodPathAndBodyHash`. Maps the lookup key to an
    /// index into `exchanges`. The *first* exchange written for a given key
    /// wins on collision — that way replay is deterministic across reloads.
    index: HashMap<IndexKey, usize>,
}

impl std::fmt::Debug for ReplaySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplaySource")
            .field("strategy", &self.inner.strategy)
            .field("exchanges", &self.inner.exchanges.len())
            .field("index", &self.inner.index.len())
            .finish_non_exhaustive()
    }
}

impl ReplaySource {
    /// Build a replay source from an in-memory list of exchanges.
    pub fn new(exchanges: Vec<RecordedExchange>, strategy: MatchStrategy) -> Self {
        let index = build_index(&exchanges, &strategy);
        Self {
            inner: Arc::new(ReplaySourceInner {
                strategy,
                exchanges,
                index,
            }),
        }
    }

    /// Stream an NDJSON file line-by-line into a replay source.
    ///
    /// The loader reads one exchange per line and never materialises the
    /// whole file as a single string (per §8.1.1's 100k-exchange scale
    /// target). Each malformed line yields a `ProxyError::Recording`.
    pub fn from_jsonl(path: impl AsRef<Path>, strategy: MatchStrategy) -> Result<Self> {
        let file = std::fs::File::open(path).map_err(ProxyError::Recording)?;
        let reader = BufReader::new(file);
        let mut exchanges = Vec::new();
        for (lineno, line) in reader.lines().enumerate() {
            let line = line.map_err(ProxyError::Recording)?;
            if line.trim().is_empty() {
                continue;
            }
            let exchange: RecordedExchange = serde_json::from_str(&line).map_err(|e| {
                ProxyError::Recording(std::io::Error::other(format!("line {}: {e}", lineno + 1)))
            })?;
            exchanges.push(exchange);
        }
        Ok(Self::new(exchanges, strategy))
    }

    /// Number of exchanges in the source.
    pub fn len(&self) -> usize {
        self.inner.exchanges.len()
    }

    /// Whether the source is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.exchanges.is_empty()
    }

    /// Look up a response for `req`. Returns `None` on miss, on a hit with an
    /// `Error` outcome (errors are intentionally not replayed — use stubs for
    /// that), or when the match strategy refuses the request.
    ///
    /// `chain` is the effective middleware list — its
    /// `redact_request_for_snapshot` hooks fire on a working copy of `req`
    /// before the lookup key is computed.
    pub fn lookup(&self, req: &ProxyRequest, chain: &[SharedMiddleware]) -> Option<ProxyResponse> {
        let mut redacted = req.clone();
        middleware::redact_request(chain, &mut redacted);
        let matched = match &self.inner.strategy {
            MatchStrategy::MethodPathAndBodyHash => {
                let key = (
                    redacted.method.as_str().to_owned(),
                    redacted.uri.path().to_owned(),
                    sha256_hex(&redacted.body),
                );
                self.inner
                    .index
                    .get(&key)
                    .and_then(|&i| self.inner.exchanges.get(i))
            }
            MatchStrategy::Custom(f) => self
                .inner
                .exchanges
                .iter()
                .find(|e| f(&e.request, &redacted)),
        };

        let exchange = matched?;
        match &exchange.outcome {
            ExchangeOutcome::Response(r) => Some(ProxyResponse {
                status: r.status(),
                headers: build_header_map(&r.headers),
                body: r.body.clone(),
                version: http::Version::HTTP_11,
            }),
            ExchangeOutcome::Error { .. } => None,
        }
    }
}

fn build_index(
    exchanges: &[RecordedExchange],
    strategy: &MatchStrategy,
) -> HashMap<IndexKey, usize> {
    if !matches!(strategy, MatchStrategy::MethodPathAndBodyHash) {
        return HashMap::new();
    }
    let mut index = HashMap::with_capacity(exchanges.len());
    for (i, e) in exchanges.iter().enumerate() {
        if !matches!(e.outcome, ExchangeOutcome::Response(_)) {
            continue;
        }
        let path = path_of(&e.request.uri);
        let key = (
            e.request.method.clone(),
            path,
            e.request.body_sha256.clone(),
        );
        index.entry(key).or_insert(i);
    }
    index
}

/// Extract the path component of a recorded URI string. Tolerant of both
/// origin-form (`/orders`) and absolute-form (`http://host/orders`).
fn path_of(uri: &str) -> String {
    if let Ok(u) = uri.parse::<http::Uri>() {
        return u.path().to_owned();
    }
    // Strip query, then strip scheme+authority if present.
    let no_query = uri.split('?').next().unwrap_or(uri);
    if let Some(idx) = no_query.find("://") {
        if let Some(slash) = no_query[idx + 3..].find('/') {
            return no_query[idx + 3 + slash..].to_owned();
        }
        return "/".to_owned();
    }
    no_query.to_owned()
}

fn build_header_map(pairs: &[(String, String)]) -> http::HeaderMap {
    let mut map = http::HeaderMap::with_capacity(pairs.len());
    for (k, v) in pairs {
        if let (Ok(name), Ok(value)) = (
            http::HeaderName::from_bytes(k.as_bytes()),
            http::HeaderValue::from_bytes(v.as_bytes()),
        ) {
            map.insert(name, value);
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recorded::{RecordedRequest, RecordedResponse};
    use bytes::Bytes;
    use http::{HeaderMap, Method, StatusCode};
    use std::time::Duration;

    fn make_exchange(method: Method, path: &str, body: &[u8], status: u16) -> RecordedExchange {
        let req = RecordedRequest::from_parts(
            &method,
            &path.parse().unwrap(),
            &HeaderMap::new(),
            Bytes::copy_from_slice(body),
        );
        let resp = RecordedResponse {
            status,
            headers: vec![("x-replay".into(), "true".into())],
            body: Bytes::from(format!("body-for-{path}").into_bytes()),
        };
        RecordedExchange::new(
            Some("api".into()),
            req,
            ExchangeOutcome::Response(resp),
            Duration::from_millis(1),
        )
    }

    fn live(method: Method, path: &str, body: &[u8]) -> ProxyRequest {
        ProxyRequest::new(
            method,
            path.parse().unwrap(),
            HeaderMap::new(),
            Bytes::copy_from_slice(body),
        )
    }

    #[test]
    fn hash_strategy_finds_exact_match() {
        let src = ReplaySource::new(
            vec![
                make_exchange(Method::GET, "/health", b"", 200),
                make_exchange(Method::POST, "/orders", b"{\"n\":1}", 201),
            ],
            MatchStrategy::MethodPathAndBodyHash,
        );
        let resp = src.lookup(&live(Method::GET, "/health", b""), &[]).unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(
            resp.headers.get("x-replay").and_then(|v| v.to_str().ok()),
            Some("true")
        );
        assert_eq!(resp.body, Bytes::from_static(b"body-for-/health"));
    }

    #[test]
    fn hash_strategy_distinguishes_by_body() {
        let src = ReplaySource::new(
            vec![
                make_exchange(Method::POST, "/orders", b"{\"n\":1}", 201),
                make_exchange(Method::POST, "/orders", b"{\"n\":2}", 202),
            ],
            MatchStrategy::MethodPathAndBodyHash,
        );
        let r1 = src
            .lookup(&live(Method::POST, "/orders", b"{\"n\":1}"), &[])
            .unwrap();
        assert_eq!(r1.status, StatusCode::CREATED);
        let r2 = src
            .lookup(&live(Method::POST, "/orders", b"{\"n\":2}"), &[])
            .unwrap();
        assert_eq!(r2.status, StatusCode::ACCEPTED);
    }

    #[test]
    fn hash_strategy_misses_when_method_or_path_or_body_differs() {
        let src = ReplaySource::new(
            vec![make_exchange(Method::GET, "/health", b"", 200)],
            MatchStrategy::MethodPathAndBodyHash,
        );
        assert!(src
            .lookup(&live(Method::POST, "/health", b""), &[])
            .is_none());
        assert!(src.lookup(&live(Method::GET, "/other", b""), &[]).is_none());
        assert!(src
            .lookup(&live(Method::GET, "/health", b"x"), &[])
            .is_none());
    }

    #[test]
    fn replay_skips_error_outcomes() {
        let req = RecordedRequest::from_parts(
            &Method::GET,
            &"/oops".parse().unwrap(),
            &HeaderMap::new(),
            Bytes::new(),
        );
        let ex = RecordedExchange::new(
            Some("api".into()),
            req,
            ExchangeOutcome::Error {
                message: "boom".into(),
            },
            Duration::from_millis(1),
        );
        let src = ReplaySource::new(vec![ex], MatchStrategy::MethodPathAndBodyHash);
        assert!(src.lookup(&live(Method::GET, "/oops", b""), &[]).is_none());
    }

    #[test]
    fn custom_strategy_runs_predicate() {
        let src = ReplaySource::new(
            vec![
                make_exchange(Method::GET, "/a", b"", 200),
                make_exchange(Method::GET, "/b", b"", 201),
                make_exchange(Method::GET, "/c", b"", 202),
            ],
            MatchStrategy::Custom(Arc::new(|recorded, live| {
                recorded.uri.contains("/b") && live.method == Method::GET
            })),
        );
        let resp = src
            .lookup(&live(Method::GET, "/anything", b""), &[])
            .unwrap();
        assert_eq!(resp.status, StatusCode::CREATED);
    }

    #[tokio::test]
    async fn from_jsonl_round_trips() {
        // Build a recorder with persist enabled, drive some exchanges
        // through it, then load that NDJSON file via ReplaySource.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trace.ndjson");
        let recorder = crate::recorder::Recorder::new(crate::config::RecordingConfig::persisted(
            100,
            path.clone(),
        ))
        .await
        .unwrap();
        for n in 0..3 {
            let req = RecordedRequest::from_parts(
                &Method::GET,
                &format!("/n/{n}").parse().unwrap(),
                &HeaderMap::new(),
                Bytes::new(),
            );
            let resp = RecordedResponse {
                status: 200,
                headers: vec![],
                body: Bytes::from(format!("body-{n}")),
            };
            recorder
                .record(RecordedExchange::new(
                    Some("api".into()),
                    req,
                    ExchangeOutcome::Response(resp),
                    Duration::from_millis(1),
                ))
                .await
                .unwrap();
        }

        let src = ReplaySource::from_jsonl(&path, MatchStrategy::MethodPathAndBodyHash).unwrap();
        assert_eq!(src.len(), 3);
        let resp = src.lookup(&live(Method::GET, "/n/1", b""), &[]).unwrap();
        assert_eq!(resp.body, Bytes::from_static(b"body-1"));
    }
}
