//! Replay source — see `SPECIFICATION.md` §8.
//!
//! `ReplaySource` is a crate-internal, immutable bundle of recorded
//! exchanges indexed for O(1) lookup. The lookup key is `(method,
//! origin-form URI (path + query string), body SHA-256)`, built once at
//! construction (§8.1).
//!
//! It is not part of the public API: callers attach a
//! [`SnapshotStorage`](crate::SnapshotStorage) backend to an upstream
//! (e.g. `JsonlStorage` or [`InMemoryStorage`](crate::InMemoryStorage)),
//! and the cluster builds the `ReplaySource` from that backend's `load()`
//! stream at `run()`.
//!
//! Lookups go through every middleware's `redact_request_for_snapshot`
//! before the lookup key is computed (§8.2.1), so a request that carried
//! a live `Authorization` header still matches a snapshot recorded with
//! that header stripped.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

// `ProxyError` is only constructed in the storage-error test below.
#[cfg(test)]
use partly_proxy_types::ProxyError;
use partly_proxy_types::{
    ExchangeOutcome, RecordedExchange, Result, SnapshotStorage, hash::sha256_hex,
};

use crate::{
    middleware::{self, SharedMiddleware},
    proxy_io::{ProxyRequest, ProxyResponse},
};

/// Lookup key: (method, path+query, body sha-256 hex).
type IndexKey = (String, String, String);

/// Cheap-to-clone replay source. Behind an `Arc`, so several listeners can
/// share one source. Crate-internal — built from an attached
/// [`SnapshotStorage`] at cluster `run()`, never constructed by callers.
#[derive(Clone)]
pub(crate) struct ReplaySource {
    inner: Arc<ReplaySourceInner>,
}

struct ReplaySourceInner {
    /// Guarded so `Mode::Record` can promote freshly forwarded exchanges into
    /// the index mid-run (see [`ReplaySource::insert`]). Critical sections are
    /// short and never `.await`, so a `std::sync::RwLock` is appropriate even
    /// on the async hot path.
    state: RwLock<ReplayState>,
}

struct ReplayState {
    exchanges: Vec<RecordedExchange>,
    /// Maps the lookup key to an index into `exchanges`. The *first*
    /// exchange written for a given key wins on collision — that way
    /// replay is deterministic across reloads.
    index: HashMap<IndexKey, usize>,
}

impl std::fmt::Debug for ReplaySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.inner.state.read().expect("replay lock poisoned");
        f.debug_struct("ReplaySource")
            .field("exchanges", &state.exchanges.len())
            .field("index", &state.index.len())
            .finish_non_exhaustive()
    }
}

impl ReplaySource {
    /// Build a replay source from an in-memory list of exchanges.
    pub(crate) fn new(exchanges: Vec<RecordedExchange>) -> Self {
        let index = build_index(&exchanges);
        Self {
            inner: Arc::new(ReplaySourceInner {
                state: RwLock::new(ReplayState { exchanges, index }),
            }),
        }
    }

    /// Drain a `SnapshotStorage`'s `load()` stream into a replay source.
    ///
    /// Generic over any storage backend. Peak memory during construction is
    /// bounded by the largest single exchange — the stream is consumed
    /// one item at a time, then the assembled `Vec` feeds `build_index`
    /// for O(1) lookups.
    pub(crate) async fn from_storage(storage: &dyn SnapshotStorage) -> Result<Self> {
        use futures::StreamExt;
        let mut stream = storage.load();
        let mut exchanges = Vec::new();
        while let Some(item) = stream.next().await {
            exchanges.push(item?);
        }
        Ok(Self::new(exchanges))
    }

    /// Number of exchanges in the source. Test-only introspection.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner
            .state
            .read()
            .expect("replay lock poisoned")
            .exchanges
            .len()
    }

    /// Add a freshly recorded exchange to the live index so a subsequent
    /// identical request replays it instead of re-forwarding and re-recording.
    ///
    /// This is what makes `Mode::Record`'s deduplicating cache
    /// (`SPECIFICATION.md` §8.3/§20.1) work *within* a single run — including
    /// one that started from an empty snapshot file: the first forward of a
    /// request is recorded and promoted here, and every later identical request
    /// becomes a replay hit. Only `Response` outcomes are indexed (errors are
    /// never replayed), and the first entry for a key wins, matching
    /// [`build_index`].
    pub(crate) fn insert(&self, exchange: RecordedExchange) {
        if !matches!(exchange.outcome, ExchangeOutcome::Response(_)) {
            return;
        }
        let key = (
            exchange.request.method.clone(),
            path_and_query_of_str(&exchange.request.uri),
            exchange.request.body_sha256.clone(),
        );
        let mut state = self.inner.state.write().expect("replay lock poisoned");
        if state.index.contains_key(&key) {
            // First write wins — an entry for this key already replays.
            return;
        }
        let idx = state.exchanges.len();
        state.exchanges.push(exchange);
        state.index.insert(key, idx);
    }

    /// Look up a response for `req`. Returns `None` on miss or on a hit with
    /// an `Error` outcome (errors are intentionally not replayed — use stubs
    /// for that).
    ///
    /// `chain` is the effective middleware list — its
    /// `redact_request_for_snapshot` hooks fire on a working copy of `req`
    /// before the lookup key is computed.
    pub(crate) fn lookup(
        &self,
        req: &ProxyRequest,
        chain: &[SharedMiddleware],
    ) -> Option<ProxyResponse> {
        let mut redacted = req.clone();
        middleware::redact_request(chain, &mut redacted);
        let key = (
            redacted.method.as_str().to_owned(),
            path_and_query_of_uri(&redacted.uri),
            sha256_hex(&redacted.body),
        );
        let state = self.inner.state.read().expect("replay lock poisoned");
        let exchange = state
            .index
            .get(&key)
            .and_then(|&i| state.exchanges.get(i))?;
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

fn build_index(exchanges: &[RecordedExchange]) -> HashMap<IndexKey, usize> {
    let mut index = HashMap::with_capacity(exchanges.len());
    for (i, e) in exchanges.iter().enumerate() {
        if !matches!(e.outcome, ExchangeOutcome::Response(_)) {
            continue;
        }
        let path_and_query = path_and_query_of_str(&e.request.uri);
        let key = (
            e.request.method.clone(),
            path_and_query,
            e.request.body_sha256.clone(),
        );
        index.entry(key).or_insert(i);
    }
    index
}

/// Extract path+query from a live `http::Uri`. Falls back to the path
/// alone when the URI has no `PathAndQuery` (e.g. asterisk-form `*`).
fn path_and_query_of_uri(uri: &http::Uri) -> String {
    uri.path_and_query()
        .map_or_else(|| uri.path().to_owned(), |pq| pq.as_str().to_owned())
}

/// Extract path+query from a recorded URI string. Tolerant of both
/// origin-form (`/orders?n=1`) and absolute-form
/// (`http://host/orders?n=1`).
fn path_and_query_of_str(uri: &str) -> String {
    if let Ok(u) = uri.parse::<http::Uri>() {
        return path_and_query_of_uri(&u);
    }
    // Tolerant fallback for non-spec-compliant recorded URIs: drop the
    // scheme+authority prefix if present, keep path and query verbatim.
    if let Some(idx) = uri.find("://") {
        if let Some(slash) = uri[idx + 3..].find('/') {
            return uri[idx + 3 + slash..].to_owned();
        }
        return "/".to_owned();
    }
    uri.to_owned()
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
    use std::time::Duration;

    use bytes::Bytes;
    use http::{HeaderMap, Method, StatusCode};
    use partly_proxy_types::{RecordedRequest, RecordedResponse};

    use super::*;

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
    fn lookup_finds_exact_match() {
        let src = ReplaySource::new(vec![
            make_exchange(Method::GET, "/health", b"", 200),
            make_exchange(Method::POST, "/orders", b"{\"n\":1}", 201),
        ]);
        let resp = src.lookup(&live(Method::GET, "/health", b""), &[]).unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(
            resp.headers.get("x-replay").and_then(|v| v.to_str().ok()),
            Some("true")
        );
        assert_eq!(resp.body, Bytes::from_static(b"body-for-/health"));
    }

    #[test]
    fn lookup_distinguishes_by_body() {
        let src = ReplaySource::new(vec![
            make_exchange(Method::POST, "/orders", b"{\"n\":1}", 201),
            make_exchange(Method::POST, "/orders", b"{\"n\":2}", 202),
        ]);
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
    fn lookup_misses_when_method_or_path_or_body_differs() {
        let src = ReplaySource::new(vec![make_exchange(Method::GET, "/health", b"", 200)]);
        assert!(
            src.lookup(&live(Method::POST, "/health", b""), &[])
                .is_none()
        );
        assert!(src.lookup(&live(Method::GET, "/other", b""), &[]).is_none());
        assert!(
            src.lookup(&live(Method::GET, "/health", b"x"), &[])
                .is_none()
        );
    }

    #[test]
    fn lookup_distinguishes_by_query_string() {
        // Regression: query-string-driven APIs (data in the query, empty
        // body) used to collapse to the first snapshot at a given path
        // because the lookup key dropped the query. The key now includes
        // path+query, so each distinct query string is its own entry.
        let src = ReplaySource::new(vec![
            make_exchange(Method::GET, "/vehicle?plate=ABC123", b"", 200),
            make_exchange(Method::GET, "/vehicle?plate=XYZ999", b"", 201),
        ]);
        let abc = src
            .lookup(&live(Method::GET, "/vehicle?plate=ABC123", b""), &[])
            .unwrap();
        assert_eq!(abc.status, StatusCode::OK);
        let xyz = src
            .lookup(&live(Method::GET, "/vehicle?plate=XYZ999", b""), &[])
            .unwrap();
        assert_eq!(xyz.status, StatusCode::CREATED);
        // Same path with no matching query string must miss, not fall
        // back to either recorded entry.
        assert!(
            src.lookup(&live(Method::GET, "/vehicle?plate=OTHER", b""), &[])
                .is_none()
        );
        assert!(
            src.lookup(&live(Method::GET, "/vehicle", b""), &[])
                .is_none()
        );
    }

    #[test]
    fn lookup_tolerates_absolute_form_recorded_uri() {
        // Live requests arrive in origin-form; recorder may store an
        // absolute-form URI (`http://host/path?query`). The index must
        // strip scheme+authority but keep the query.
        let recorded = make_exchange(
            Method::GET,
            "http://api.example.com/vehicle?plate=ABC123",
            b"",
            200,
        );
        let src = ReplaySource::new(vec![recorded]);
        let hit = src
            .lookup(&live(Method::GET, "/vehicle?plate=ABC123", b""), &[])
            .unwrap();
        assert_eq!(hit.status, StatusCode::OK);
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
        let src = ReplaySource::new(vec![ex]);
        assert!(src.lookup(&live(Method::GET, "/oops", b""), &[]).is_none());
    }

    #[cfg(feature = "storage-jsonl")]
    #[tokio::test]
    async fn jsonl_storage_round_trips() {
        // Build a recorder backed by an explicit JsonlStorage, drive
        // some exchanges through it, then load that NDJSON file back into a
        // ReplaySource via `from_storage`.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trace.ndjson");
        let storage: partly_proxy_types::SharedStorage = Arc::new(
            crate::jsonl::JsonlStorage::open(&path)
                .await
                .expect("open jsonl"),
        );
        // Route the "api" upstream (stamped on each exchange below) to the
        // JSONL medium so records land on disk. Keep an `Arc` clone to load
        // from afterwards.
        let routes = std::collections::HashMap::from([("api".to_owned(), storage.clone())]);
        let recorder = crate::recorder::Recorder::with_routes(
            crate::config::RecordingConfig::in_memory(100),
            routes,
        );
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

        let src = ReplaySource::from_storage(storage.as_ref()).await.unwrap();
        assert_eq!(src.len(), 3);
        let resp = src.lookup(&live(Method::GET, "/n/1", b""), &[]).unwrap();
        assert_eq!(resp.body, Bytes::from_static(b"body-1"));
    }

    /// Mock `SnapshotStorage` that replays a pre-baked exchange list.
    /// Used to validate `ReplaySource::from_storage` in isolation from any
    /// concrete backend.
    #[derive(Debug)]
    struct MockStorage {
        exchanges: Vec<RecordedExchange>,
    }

    #[async_trait::async_trait]
    impl SnapshotStorage for MockStorage {
        async fn append(&self, _exchange: &RecordedExchange) -> Result<()> {
            Ok(())
        }

        async fn flush(&self) -> Result<()> {
            Ok(())
        }

        fn load(&self) -> futures::stream::BoxStream<'_, Result<RecordedExchange>> {
            let items: Vec<Result<RecordedExchange>> =
                self.exchanges.iter().cloned().map(Ok).collect();
            Box::pin(futures::stream::iter(items))
        }
    }

    #[tokio::test]
    async fn from_storage_drains_load_into_replay_source() {
        let exchanges = vec![
            make_exchange(Method::GET, "/a", b"", 200),
            make_exchange(Method::POST, "/b", b"{\"n\":1}", 201),
        ];
        let storage = MockStorage {
            exchanges: exchanges.clone(),
        };
        let src = ReplaySource::from_storage(&storage).await.unwrap();
        assert_eq!(src.len(), 2);
        let resp = src
            .lookup(&live(Method::POST, "/b", b"{\"n\":1}"), &[])
            .unwrap();
        assert_eq!(resp.status, StatusCode::CREATED);
    }

    #[tokio::test]
    async fn from_storage_propagates_load_error() {
        #[derive(Debug)]
        struct BadStorage;
        #[async_trait::async_trait]
        impl SnapshotStorage for BadStorage {
            async fn append(&self, _: &RecordedExchange) -> Result<()> {
                Ok(())
            }

            async fn flush(&self) -> Result<()> {
                Ok(())
            }

            fn load(&self) -> futures::stream::BoxStream<'_, Result<RecordedExchange>> {
                Box::pin(futures::stream::iter([Err(ProxyError::Recording(
                    std::io::Error::other("synthetic"),
                ))]))
            }
        }
        let err = ReplaySource::from_storage(&BadStorage).await.unwrap_err();
        assert!(matches!(err, ProxyError::Recording(_)));
    }
}
