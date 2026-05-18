//! Drives the shared `SnapshotStorage` conformance battery against
//! `JsonlStorage`. The suite lives in `partly-proxy-types::testing` so
//! every backend crate runs the same assertions on the same fixtures.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use partly_proxy_storage_jsonl::JsonlStorage;
use partly_proxy_types::storage::SharedStorage;
use partly_proxy_types::testing::run_conformance;

#[tokio::test]
async fn jsonl_conformance() {
    // Each sub-case in the suite calls the factory once. Give each its
    // own file in a single tempdir so the files don't collide and so
    // they're all cleaned up when this test function returns.
    let dir = tempfile::tempdir().unwrap();
    let dir_path = dir.path().to_path_buf();
    let counter = Arc::new(AtomicUsize::new(0));

    run_conformance(move || {
        let dir_path = dir_path.clone();
        let counter = counter.clone();
        async move {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            let path = dir_path.join(format!("trace-{n}.ndjson"));
            let storage = JsonlStorage::open(path).await.expect("open");
            let shared: SharedStorage = Arc::new(storage);
            shared
        }
    })
    .await;

    drop(dir);
}
