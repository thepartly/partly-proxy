//! End-to-end recording through a real listener + forwarder.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use http::{HeaderMap, Method};
use partly_proxy_echo as echo;
use partly_proxy_lib::{
    ClusterHandle, ExchangeOutcome, ProxyClusterBuilder, ProxyConfig, RecordedExchange,
    RecordedRequest, RecordedResponse, RecordingConfig, SharedStorage, SnapshotStorage,
    UpstreamTarget, jsonl::JsonlStorage,
};
use tokio::task::JoinHandle;

async fn spawn_echo() -> (SocketAddr, JoinHandle<()>) {
    let (addr, listener) = echo::bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let task = tokio::spawn(async move {
        let _ = echo::serve(listener).await;
    });
    (addr, task)
}

async fn spawn_proxy(upstream_url: String, recording: RecordingConfig) -> ClusterHandle {
    let cfg = ProxyConfig::http(
        "127.0.0.1:0".parse().unwrap(),
        UpstreamTarget::new(upstream_url)
            .with_connect_timeout(Duration::from_secs(1))
            .with_request_timeout(Duration::from_secs(5)),
    );
    ProxyClusterBuilder::new()
        .recording(recording)
        .add_upstream("upstream", cfg)
        .run()
        .await
        .expect("cluster builds")
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client builds")
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
    let unreachable = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = l.local_addr().unwrap();
        drop(l);
        a
    };
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

    for _ in 0..3 {
        let _ = http_client()
            .get(format!("http://{proxy}/x"))
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
        assert_eq!(ex.request.uri, "/x", "exchange {i}");
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

// ---------------------------------------------------------------------------
// Regression test for the `Mode::Record` deduplicating-snapshot cache.
//
// SPECIFICATION.md §8.3 and §20.1 require that, in `Mode::Record`, a request
// already present in the snapshot is *replayed* and **not re-recorded** — "the
// snapshot acts as a deduplicating cache so already-seen requests do not re-hit
// the upstream", and a re-run "replays any request already in the file rather
// than re-recording it". The lifecycle previously recorded every served
// exchange unconditionally, so a replay hit appended a duplicate on every hit.
// ---------------------------------------------------------------------------

/// Number of non-blank NDJSON lines currently on disk at `path`.
async fn ndjson_line_count(path: &std::path::Path) -> usize {
    let raw = tokio::fs::read_to_string(path).await.unwrap_or_default();
    raw.lines().filter(|l| !l.trim().is_empty()).count()
}

/// Write a single recorded exchange into a fresh NDJSON file at `path`.
async fn seed_snapshot(
    path: &std::path::Path,
    method: Method,
    uri: &str,
    req_body: &[u8],
    resp_body: &[u8],
) {
    let req = RecordedRequest::from_parts(
        &method,
        &uri.parse().unwrap(),
        &HeaderMap::new(),
        Bytes::copy_from_slice(req_body),
    );
    let resp = RecordedResponse {
        status: 200,
        headers: Vec::new(),
        body: Bytes::copy_from_slice(resp_body),
    };
    let storage = JsonlStorage::open(path).await.unwrap();
    storage
        .append(&RecordedExchange::new(
            Some("upstream".to_owned()),
            req,
            ExchangeOutcome::Response(resp),
            Duration::from_millis(1),
        ))
        .await
        .unwrap();
    storage.flush().await.unwrap();
    drop(storage);
}

/// An address that nothing is listening on — any forward to it fails, which
/// lets a test prove a response came from the replay snapshot (not the
/// upstream).
async fn unreachable_addr() -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

/// Bug: "It multiplies records for existing requests."
///
/// A request already present in the snapshot file is served from the snapshot
/// (proved here by pointing the upstream at an unreachable address: a forward
/// would 502), but the recorder appended it a second time, so the file grew
/// from one line to two. Per §8.3/§20.1 it must stay at one line.
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
        UpstreamTarget::new(format!("http://{}", unreachable_addr().await))
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
