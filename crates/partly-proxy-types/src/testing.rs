//! Conformance test suite for [`SnapshotStorage`] implementations.
//!
//! Gated on the `testing` Cargo feature so it doesn't bloat normal
//! builds. Each backend crate's test file calls
//! [`run_conformance`] with a closure that produces a fresh
//! `SharedStorage` per sub-case, and gets a battery of assertions
//! covering the durability/ordering contract the trait promises.

use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use http::{HeaderMap, Method};

use crate::error::Result;
use crate::recorded::{ExchangeOutcome, RecordedExchange, RecordedRequest, RecordedResponse};
use crate::storage::SharedStorage;

/// Run the full battery against any [`SharedStorage`] factory.
///
/// `make_storage` is called once per sub-case so each test gets a clean
/// medium — no cross-contamination between e.g. the "ordering" and
/// "empty load" cases. Backends that hold expensive resources (DB
/// connections, network clients) should still implement the factory as
/// a thin async builder.
///
/// Panics with `assert!` on the first failure, so a failed sub-case
/// surfaces as a normal test failure in the calling crate.
pub async fn run_conformance<F, Fut>(make_storage: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SharedStorage>,
{
    round_trip_single_exchange(&make_storage).await;
    insertion_order_preserved(&make_storage).await;
    load_after_flush_yields_everything(&make_storage).await;
    error_outcome_round_trips(&make_storage).await;
    interleaved_append_and_flush(&make_storage).await;
    empty_storage_load_yields_nothing(&make_storage).await;
}

/// `append → flush → load` round-trips one exchange byte-equally.
pub async fn round_trip_single_exchange<F, Fut>(make: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SharedStorage>,
{
    let storage = make().await;
    let ex = make_exchange("/round-trip", b"single", 200);
    storage.append(&ex).await.expect("append");
    storage.flush().await.expect("flush");

    let loaded = collect_load(&storage).await.expect("load");
    assert_eq!(
        loaded.len(),
        1,
        "round-trip should yield exactly one exchange"
    );
    assert_eq!(
        loaded[0], ex,
        "round-tripped exchange must equal the original"
    );
}

/// Insertion order is preserved across `flush`.
pub async fn insertion_order_preserved<F, Fut>(make: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SharedStorage>,
{
    let storage = make().await;
    let mut originals = Vec::new();
    for i in 0u8..5 {
        let ex = make_exchange(&format!("/n/{i}"), &[i], 200);
        storage.append(&ex).await.expect("append");
        originals.push(ex);
    }
    storage.flush().await.expect("flush");
    let loaded = collect_load(&storage).await.expect("load");
    assert_eq!(loaded, originals, "loaded order must match insertion order");
}

/// `load` is callable after `flush` returns and yields every appended
/// exchange.
pub async fn load_after_flush_yields_everything<F, Fut>(make: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SharedStorage>,
{
    let storage = make().await;
    for i in 0..7usize {
        let ex = make_exchange(&format!("/load/{i}"), b"x", 200);
        storage.append(&ex).await.expect("append");
    }
    storage.flush().await.expect("flush");
    let loaded = collect_load(&storage).await.expect("load");
    assert_eq!(loaded.len(), 7, "load should yield every appended exchange");
}

/// Exchanges with an `Error` outcome round-trip just like response
/// outcomes.
pub async fn error_outcome_round_trips<F, Fut>(make: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SharedStorage>,
{
    let storage = make().await;
    let req = RecordedRequest::from_parts(
        &Method::GET,
        &"/oops".parse().unwrap(),
        &HeaderMap::new(),
        Bytes::new(),
    );
    let ex = RecordedExchange::new(
        Some("api".to_owned()),
        req,
        ExchangeOutcome::Error {
            message: "synthetic upstream-connect error".to_owned(),
        },
        Duration::from_millis(3),
    );
    storage.append(&ex).await.expect("append");
    storage.flush().await.expect("flush");
    let loaded = collect_load(&storage).await.expect("load");
    assert_eq!(loaded.len(), 1, "error-outcome exchange should round-trip");
    assert_eq!(loaded[0], ex, "error-outcome bytes must be byte-equal");
}

/// Multiple `append`s interleaved with `flush` calls produce no
/// duplicates and no losses.
pub async fn interleaved_append_and_flush<F, Fut>(make: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SharedStorage>,
{
    let storage = make().await;
    let mut originals = Vec::new();
    for i in 0u8..6 {
        let ex = make_exchange(&format!("/inter/{i}"), &[i], 200);
        storage.append(&ex).await.expect("append");
        originals.push(ex);
        if i % 2 == 0 {
            storage.flush().await.expect("flush mid-stream");
        }
    }
    storage.flush().await.expect("final flush");
    let loaded = collect_load(&storage).await.expect("load");
    assert_eq!(
        loaded, originals,
        "interleaved flushes must not duplicate or drop exchanges"
    );
}

/// Empty storage: `load` yields zero items without error.
pub async fn empty_storage_load_yields_nothing<F, Fut>(make: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SharedStorage>,
{
    let storage = make().await;
    let loaded = collect_load(&storage).await.expect("load on empty storage");
    assert!(
        loaded.is_empty(),
        "empty storage should yield zero exchanges"
    );
}

/// Helper: drain a `SharedStorage::load()` stream into a Vec, collecting
/// the first error if any.
async fn collect_load(storage: &SharedStorage) -> Result<Vec<RecordedExchange>> {
    let mut stream = storage.load();
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item?);
    }
    Ok(out)
}

/// Deterministic test exchange. Body bytes are caller-provided so each
/// sub-case can produce distinguishable payloads.
pub fn make_exchange(path: &str, body: &[u8], status: u16) -> RecordedExchange {
    let req = RecordedRequest::from_parts(
        &Method::POST,
        &path.parse().unwrap(),
        &HeaderMap::new(),
        Bytes::copy_from_slice(body),
    );
    let resp = RecordedResponse {
        status,
        headers: vec![("x-test".to_owned(), "true".to_owned())],
        body: Bytes::from(format!("response-for-{path}").into_bytes()),
    };
    RecordedExchange {
        // Deterministic id derived from `path` — unique per call within
        // a sub-case, identical across re-runs so PartialEq comparisons
        // stay stable.
        id: deterministic_uuid(path),
        upstream: Some("test".to_owned()),
        timestamp: chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        duration: Duration::from_millis(5),
        request: req,
        outcome: ExchangeOutcome::Response(resp),
        labels: BTreeMap::new(),
    }
}

fn deterministic_uuid(seed: &str) -> uuid::Uuid {
    let mut bytes = [0u8; 16];
    let src = seed.as_bytes();
    let n = src.len().min(16);
    bytes[..n].copy_from_slice(&src[..n]);
    uuid::Uuid::from_bytes(bytes)
}
