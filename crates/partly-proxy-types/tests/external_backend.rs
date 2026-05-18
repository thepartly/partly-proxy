//! Proves that a `SnapshotStorage` backend can be built using only the
//! `partly-proxy-types` crate — no dependency on `partly-proxy-lib` or
//! any first-party backend crate.
//!
//! This is the contract for third-party backends: the imports below are
//! the entire public surface an implementer needs to learn. For an
//! example of running the shared conformance battery against a custom
//! backend, see `partly-proxy-storage-jsonl/tests/conformance.rs` or
//! the equivalent in `partly-proxy-storage-sqlite` — both opt in via
//! `partly-proxy-types = { ..., features = ["testing"] }`.

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use http::{HeaderMap, Method};
use partly_proxy_types::storage::{BoxStream, ExchangeStream, SnapshotStorage};
use partly_proxy_types::{
    ExchangeOutcome, RecordedExchange, RecordedRequest, RecordedResponse, Result,
};

/// Minimal in-memory backend — the example from the trait docstring,
/// reproduced here as runnable code so doc drift would surface as a
/// test failure.
#[derive(Debug, Default)]
struct InMemoryStorage {
    exchanges: Mutex<Vec<RecordedExchange>>,
}

#[async_trait]
impl SnapshotStorage for InMemoryStorage {
    async fn append(&self, exchange: &RecordedExchange) -> Result<()> {
        self.exchanges.lock().unwrap().push(exchange.clone());
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    fn load(&self) -> ExchangeStream<'_> {
        let snapshot = self.exchanges.lock().unwrap().clone();
        Box::pin(futures::stream::iter(snapshot.into_iter().map(Ok)))
    }
}

fn make_exchange(path: &str, body: &[u8]) -> RecordedExchange {
    let req = RecordedRequest::from_parts(
        &Method::POST,
        &path.parse().unwrap(),
        &HeaderMap::new(),
        Bytes::copy_from_slice(body),
    );
    let resp = RecordedResponse {
        status: 200,
        headers: Vec::new(),
        body: Bytes::from_static(b"ok"),
    };
    RecordedExchange::new(
        Some("api".to_owned()),
        req,
        ExchangeOutcome::Response(resp),
        Duration::from_millis(1),
    )
}

#[tokio::test]
async fn external_backend_round_trips() {
    let storage = InMemoryStorage::default();
    let a = make_exchange("/a", b"first");
    let b = make_exchange("/b", b"second");
    storage.append(&a).await.unwrap();
    storage.append(&b).await.unwrap();
    storage.flush().await.unwrap();

    let loaded: Vec<RecordedExchange> = storage
        .load()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(loaded, vec![a, b]);
}

#[test]
fn box_stream_re_export_resolves() {
    // Compile-time check that `BoxStream` is reachable via the types
    // crate alone — implementers should not have to add `futures` as a
    // direct dep just to spell the trait's return type.
    fn _accepts(_: BoxStream<'static, Result<RecordedExchange>>) {}
}
