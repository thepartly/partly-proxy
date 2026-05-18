//! Conformance suite + a few backend-specific assertions for the
//! object-store backend. Runs against `object_store::memory::InMemory`
//! so no network is required.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use http::{HeaderMap, Method};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use partly_proxy_storage_object::{ObjectStorage, DEFAULT_BATCH_BYTES};
use partly_proxy_types::recorded::{
    ExchangeOutcome, RecordedExchange, RecordedRequest, RecordedResponse,
};
use partly_proxy_types::storage::{SharedStorage, SnapshotStorage};
use partly_proxy_types::testing::run_conformance;

fn make_store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

#[tokio::test]
async fn object_store_conformance_default_batch() {
    let store = make_store();
    let counter = Arc::new(AtomicUsize::new(0));
    run_conformance(move || {
        let store = store.clone();
        let counter = counter.clone();
        async move {
            // Unique prefix per sub-case so state from one case doesn't
            // leak into the next.
            let n = counter.fetch_add(1, Ordering::SeqCst);
            let prefix = ObjectPath::from(format!("runs/case-{n}"));
            let storage = ObjectStorage::with_defaults(store, prefix);
            let shared: SharedStorage = Arc::new(storage);
            shared
        }
    })
    .await;
}

/// A small `batch_bytes` forces multiple parts during a single run, which
/// the conformance suite alone wouldn't exercise reliably.
#[tokio::test]
async fn small_batch_emits_multiple_parts() {
    let store = make_store();
    let prefix = ObjectPath::from("runs/multi-part");
    // 128 bytes is well below one exchange's JSON, so each append flushes
    // a part.
    let storage = ObjectStorage::new(store.clone(), prefix.clone(), 128);

    for n in 0u8..3 {
        let req = RecordedRequest::from_parts(
            &Method::GET,
            &format!("/x/{n}").parse().unwrap(),
            &HeaderMap::new(),
            Bytes::new(),
        );
        let resp = RecordedResponse {
            status: 200,
            headers: vec![],
            body: Bytes::from(format!("b{n}").into_bytes()),
        };
        let ex = RecordedExchange::new(
            Some("api".to_owned()),
            req,
            ExchangeOutcome::Response(resp),
            Duration::from_millis(1),
        );
        storage.append(&ex).await.unwrap();
    }
    storage.flush().await.unwrap();

    // After flush we should see >= 2 part-* objects plus a manifest.
    let mut listing = store.list(Some(&prefix));
    let mut paths = Vec::new();
    while let Some(item) = listing.next().await {
        paths.push(item.unwrap().location);
    }
    let part_count = paths
        .iter()
        .filter(|p| p.as_ref().contains("part-"))
        .count();
    assert!(
        part_count >= 2,
        "expected >=2 part objects, found {part_count} (paths: {paths:?})"
    );
    assert!(paths.iter().any(|p| p.as_ref().ends_with("manifest.json")));

    // And load returns everything in order.
    let shared: SharedStorage = Arc::new(storage);
    let mut stream2 = shared.load();
    let mut count = 0;
    while let Some(item) = stream2.next().await {
        let _ = item.unwrap();
        count += 1;
    }
    assert_eq!(count, 3);
}

#[test]
fn default_batch_bytes_is_4mib() {
    assert_eq!(DEFAULT_BATCH_BYTES, 4 * 1024 * 1024);
}
