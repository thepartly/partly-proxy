//! Conformance suite + memory-mode smoke for `SqliteStorage`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use partly_proxy_storage_sqlite::SqliteStorage;
use partly_proxy_types::storage::SharedStorage;
use partly_proxy_types::testing::run_conformance;

#[tokio::test]
async fn sqlite_file_conformance() {
    let dir = tempfile::tempdir().unwrap();
    let dir_path = dir.path().to_path_buf();
    let counter = Arc::new(AtomicUsize::new(0));

    run_conformance(move || {
        let dir_path = dir_path.clone();
        let counter = counter.clone();
        async move {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            let path = dir_path.join(format!("trace-{n}.sqlite"));
            let storage = SqliteStorage::open(path).await.expect("open");
            let shared: SharedStorage = Arc::new(storage);
            shared
        }
    })
    .await;

    drop(dir);
}

#[tokio::test]
async fn sqlite_memory_conformance() {
    run_conformance(|| async {
        let storage = SqliteStorage::in_memory().await.expect("open");
        let shared: SharedStorage = Arc::new(storage);
        shared
    })
    .await;
}
