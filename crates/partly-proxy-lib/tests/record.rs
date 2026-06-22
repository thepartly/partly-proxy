//! End-to-end recording through a real listener + forwarder.

use std::{sync::Arc, time::Duration};

use http::Method;
use partly_proxy_lib::{
    ClusterHandle, ExchangeOutcome, ProxyClusterBuilder, ProxyConfig, RecordedExchange,
    RecordingConfig, SharedStorage, UpstreamTarget, jsonl::JsonlStorage,
};

mod common;
use common::{cfg, http_client, ndjson_line_count, seed_snapshot, spawn_echo, unreachable_addr};

async fn spawn_proxy(upstream_url: String, recording: RecordingConfig) -> ClusterHandle {
    ProxyClusterBuilder::new()
        .recording(recording)
        .add_upstream("upstream", cfg(upstream_url))
        .run()
        .await
        .expect("cluster builds")
}

#[tokio::test]
async fn successful_exchange_is_recorded_in_memory() {
    let (echo_addr, _echo_task) = spawn_echo().await;
    let cluster = spawn_proxy(
        format!("http://{echo_addr}"),
        RecordingConfig::in_memory(100),
    )
    .await;
    let proxy_addr = cluster.addr("upstream").unwrap();

    let resp = http_client()
        .post(format!("http://{proxy_addr}/orders"))
        .header("x-trace", "abc")
        .body("hello")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let _ = resp.text().await.unwrap();

    let recorder = cluster.recorder();
    // Recording happens after the response is returned to the client; allow
    // a few iterations of the runtime to let the record() task finish.
    let exchanges = wait_for_exchanges(recorder, 1).await;
    assert_eq!(exchanges.len(), 1);
    let ex = &exchanges[0];
    assert_eq!(ex.upstream.as_deref(), Some("upstream"));
    assert_eq!(ex.request.method, "POST");
    assert_eq!(ex.request.uri, "/orders");
    assert_eq!(ex.request.body, bytes::Bytes::from_static(b"hello"));
    // Body hash is deterministic; "hello" -> 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
    assert_eq!(
        ex.request.body_sha256,
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    );
    assert!(
        ex.request
            .headers
            .iter()
            .any(|(k, v)| k == "x-trace" && v == "abc")
    );
    let recorded_resp = match &ex.outcome {
        ExchangeOutcome::Response(r) => r,
        ExchangeOutcome::Error { message } => panic!("unexpected error outcome: {message}"),
    };
    assert_eq!(recorded_resp.status, 200);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn unreachable_upstream_records_error_outcome() {
    let unreachable = unreachable_addr();
    let cluster = spawn_proxy(
        format!("http://{unreachable}"),
        RecordingConfig::in_memory(10),
    )
    .await;
    let proxy_addr = cluster.addr("upstream").unwrap();

    let resp = http_client()
        .get(format!("http://{proxy_addr}/oops"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 502);

    let recorded = wait_for_exchanges(cluster.recorder(), 1).await;
    assert_eq!(recorded.len(), 1);
    match &recorded[0].outcome {
        ExchangeOutcome::Error { message } => {
            assert!(
                message.contains("upstream"),
                "error message should mention upstream: {message}"
            );
        }
        ExchangeOutcome::Response(_) => panic!("expected error outcome"),
    }

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn ndjson_persist_file_is_replayable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trace.ndjson");
    let (echo_addr, _echo_task) = spawn_echo().await;

    let storage: partly_proxy_lib::SharedStorage = std::sync::Arc::new(
        partly_proxy_lib::jsonl::JsonlStorage::open(&path)
            .await
            .unwrap(),
    );
    let cfg = ProxyConfig::http(
        "127.0.0.1:0".parse().unwrap(),
        UpstreamTarget::new(format!("http://{echo_addr}"))
            .with_connect_timeout(Duration::from_secs(1))
            .with_request_timeout(Duration::from_secs(5)),
    );
    let cluster = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(100))
        .add_upstream_with("upstream", cfg, Vec::new(), Some(storage))
        .run()
        .await
        .unwrap();
    let proxy_addr = cluster.addr("upstream").unwrap();

    for n in 0..3 {
        let _ = http_client()
            .post(format!("http://{proxy_addr}/items/{n}"))
            .body(format!("body-{n}"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
    }
    let _ = wait_for_exchanges(cluster.recorder(), 3).await;
    cluster.shutdown().await.unwrap();

    let raw = tokio::fs::read_to_string(&path).await.unwrap();
    let lines: Vec<_> = raw.lines().collect();
    assert_eq!(lines.len(), 3);
    let parsed: Vec<RecordedExchange> = lines
        .iter()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(parsed[0].request.uri, "/items/0");
    assert_eq!(parsed[2].request.uri, "/items/2");
    assert_eq!(parsed[0].request.body, bytes::Bytes::from_static(b"body-0"));
}

/// Storage backend that counts every `append` and `flush` it sees and
/// keeps the exchanges in memory. Used to verify the per-upstream
/// `add_upstream_with(storage)` plumbing.
#[derive(Debug, Default)]
struct TrackingStorage {
    appended: tokio::sync::Mutex<Vec<RecordedExchange>>,
    append_count: std::sync::atomic::AtomicUsize,
    flush_count: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl partly_proxy_lib::SnapshotStorage for TrackingStorage {
    async fn append(&self, exchange: &RecordedExchange) -> partly_proxy_lib::Result<()> {
        self.append_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.appended.lock().await.push(exchange.clone());
        Ok(())
    }

    async fn flush(&self) -> partly_proxy_lib::Result<()> {
        self.flush_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    fn load(&self) -> futures::stream::BoxStream<'_, partly_proxy_lib::Result<RecordedExchange>> {
        let snapshot = self
            .appended
            .try_lock()
            .map(|g| g.clone())
            .unwrap_or_default();
        Box::pin(futures::stream::iter(snapshot.into_iter().map(Ok)))
    }
}

#[tokio::test]
async fn custom_storage_via_per_upstream_snapshots() {
    let (echo_addr, _t) = spawn_echo().await;
    let storage = std::sync::Arc::new(TrackingStorage::default());
    let cfg = ProxyConfig::http(
        "127.0.0.1:0".parse().unwrap(),
        UpstreamTarget::new(format!("http://{echo_addr}"))
            .with_connect_timeout(Duration::from_secs(1))
            .with_request_timeout(Duration::from_secs(5)),
    );
    let cluster = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(100))
        .add_upstream_with("upstream", cfg, Vec::new(), Some(storage.clone()))
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("upstream").unwrap();

    // Distinct paths so each request is genuinely new: in `Mode::Record` the
    // snapshot is a deduplicating cache, so three *identical* requests would
    // collapse to a single append (the later two replay the first).
    for n in 0..3 {
        let _ = http_client()
            .get(format!("http://{proxy}/x/{n}"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
    }
    let _ = wait_for_exchanges(cluster.recorder(), 3).await;
    cluster.shutdown().await.unwrap();

    // Three appends through the trait, one flush from the shutdown fence.
    assert_eq!(
        storage
            .append_count
            .load(std::sync::atomic::Ordering::SeqCst),
        3
    );
    assert_eq!(
        storage
            .flush_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    let saved = storage.appended.lock().await.clone();
    assert_eq!(saved.len(), 3);
    for (i, ex) in saved.iter().enumerate() {
        assert_eq!(ex.request.uri, format!("/x/{i}"), "exchange {i}");
    }
}

#[tokio::test]
async fn disabled_recording_keeps_buffer_empty() {
    let (echo_addr, _echo_task) = spawn_echo().await;
    let cluster = spawn_proxy(format!("http://{echo_addr}"), RecordingConfig::disabled()).await;
    let proxy_addr = cluster.addr("upstream").unwrap();
    let _ = http_client()
        .get(format!("http://{proxy_addr}/x"))
        .send()
        .await
        .unwrap();
    // Even after a generous yield window, no exchanges should land.
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(cluster.recorder().len().await, 0);
    }
    cluster.shutdown().await.unwrap();
}

/// A request already present in the snapshot file is served from the snapshot
/// (proved here by pointing the upstream at an unreachable address: a forward
/// would 502), but the recorder appended it a second time, so the file grew
/// from one line to two. Per SPECIFICATION.md §8.3/§20.1 the snapshot is a
/// deduplicating cache, so it must stay at one line.
#[tokio::test]
async fn record_mode_does_not_re_record_request_already_in_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trace.ndjson");
    seed_snapshot(&path, Method::POST, "/existing", b"hello", b"FROM-SNAPSHOT").await;
    assert_eq!(
        ndjson_line_count(&path).await,
        1,
        "seed should write one line"
    );

    let storage: SharedStorage = Arc::new(JsonlStorage::open(&path).await.unwrap());
    let cfg = ProxyConfig::http(
        "127.0.0.1:0".parse().unwrap(),
        UpstreamTarget::new(format!("http://{}", unreachable_addr()))
            .with_connect_timeout(Duration::from_millis(500))
            .with_request_timeout(Duration::from_secs(2)),
    );
    let cluster = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(100))
        .add_upstream_with("upstream", cfg, Vec::new(), Some(storage))
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("upstream").unwrap();

    let resp = http_client()
        .post(format!("http://{proxy}/existing"))
        .body("hello")
        .send()
        .await
        .unwrap();
    // 200 + the snapshot body proves the request was replayed, not forwarded
    // (the unreachable upstream would have produced a 502).
    assert_eq!(resp.status(), 200, "request in snapshot must be replayed");
    assert_eq!(resp.text().await.unwrap(), "FROM-SNAPSHOT");

    // Give the recorder ample time to (wrongly) append a duplicate.
    tokio::time::sleep(Duration::from_millis(300)).await;
    cluster.shutdown().await.unwrap();

    assert_eq!(
        ndjson_line_count(&path).await,
        1,
        "a request already present in the snapshot must NOT be re-recorded \
         (SPECIFICATION.md §8.3/§20.1: the snapshot is a deduplicating cache)"
    );
}

/// Starting from an existing-but-empty snapshot file, the same request is sent
/// twice. The first forwards to the upstream and is recorded; the second must
/// hit that just-recorded snapshot and be served without a second recording.
/// If the freshly recorded exchange is not promoted into the live replay
/// index, the second request is treated as new and recorded again — leaving
/// two copies in the file instead of one.
#[tokio::test]
async fn record_mode_from_empty_file_dedupes_repeated_request() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trace.ndjson");
    // File exists but is empty.
    tokio::fs::File::create(&path).await.unwrap();
    assert_eq!(ndjson_line_count(&path).await, 0);

    let (echo_addr, _echo_task) = spawn_echo().await;
    let storage: SharedStorage = Arc::new(JsonlStorage::open(&path).await.unwrap());
    let cfg = ProxyConfig::http(
        "127.0.0.1:0".parse().unwrap(),
        UpstreamTarget::new(format!("http://{echo_addr}"))
            .with_connect_timeout(Duration::from_secs(1))
            .with_request_timeout(Duration::from_secs(5)),
    );
    let cluster = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(100))
        .add_upstream_with("upstream", cfg, Vec::new(), Some(storage))
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("upstream").unwrap();

    for _ in 0..2 {
        let _ = http_client()
            .get(format!("http://{proxy}/dup"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    cluster.shutdown().await.unwrap();

    assert_eq!(
        ndjson_line_count(&path).await,
        1,
        "the second identical request must be served from the freshly recorded \
         snapshot and not recorded again (SPECIFICATION.md §8.3 deduplicating cache)"
    );
}

/// Guard (passes today): an existing-but-empty snapshot file must not suppress
/// recording of a genuinely new request. This pins the *other* half of the
/// dedup contract so a fix for the two regressions above cannot over-correct
/// into dropping new records.
#[tokio::test]
async fn record_mode_from_empty_file_still_records_new_request() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trace.ndjson");
    tokio::fs::File::create(&path).await.unwrap();

    let (echo_addr, _echo_task) = spawn_echo().await;
    let storage: SharedStorage = Arc::new(JsonlStorage::open(&path).await.unwrap());
    let cfg = ProxyConfig::http(
        "127.0.0.1:0".parse().unwrap(),
        UpstreamTarget::new(format!("http://{echo_addr}"))
            .with_connect_timeout(Duration::from_secs(1))
            .with_request_timeout(Duration::from_secs(5)),
    );
    let cluster = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(100))
        .add_upstream_with("upstream", cfg, Vec::new(), Some(storage))
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("upstream").unwrap();

    let _ = http_client()
        .post(format!("http://{proxy}/brand-new"))
        .body("payload")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let _ = wait_for_exchanges(cluster.recorder(), 1).await;
    cluster.shutdown().await.unwrap();

    assert_eq!(
        ndjson_line_count(&path).await,
        1,
        "a new request against an empty snapshot file must be recorded"
    );
}

/// Poll the recorder until it reaches at least `target` exchanges or times
/// out. The lifecycle records exchanges *after* the response has been sent,
/// so the client-side `await` returning is not sufficient by itself.
async fn wait_for_exchanges(
    recorder: &partly_proxy_lib::Recorder,
    target: usize,
) -> Vec<RecordedExchange> {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let len = recorder.len().await;
        if len >= target {
            return recorder.exchanges().await;
        }
        if std::time::Instant::now() >= deadline {
            return recorder.exchanges().await;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
