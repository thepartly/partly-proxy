//! Shared helpers for the integration test binaries.
//!
//! Lives under `tests/common/` (a subdirectory, not a top-level `tests/*.rs`)
//! so Cargo treats it as a module to include via `mod common;` rather than as
//! its own test binary. Each including binary only exercises a subset of these,
//! so unused-helper warnings are expected and silenced.
#![allow(dead_code)]

use std::{net::SocketAddr, path::Path, time::Duration};

use bytes::Bytes;
use http::{HeaderMap, Method};
use partly_proxy_echo as echo;
use partly_proxy_lib::{
    ExchangeOutcome, ProxyConfig, RecordedExchange, RecordedRequest, RecordedResponse,
    SnapshotStorage, UpstreamTarget, jsonl::JsonlStorage,
};
use tokio::{io::AsyncBufReadExt, task::JoinHandle};

/// Bind an in-process echo upstream on an ephemeral port and serve it on a
/// background task. Returns the bound address and the task handle.
pub async fn spawn_echo() -> (SocketAddr, JoinHandle<()>) {
    let (addr, listener) = echo::bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let task = tokio::spawn(async move {
        let _ = echo::serve(listener).await;
    });
    (addr, task)
}

/// A reqwest client that ignores any ambient proxy env vars and times out
/// after 5s — the standard client for driving the proxy from a test.
pub fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client builds")
}

/// A `ProxyConfig` bound to an ephemeral port, forwarding to `url` with short
/// (1s connect / 5s request) test timeouts.
pub fn cfg(url: String) -> ProxyConfig {
    ProxyConfig::http(
        "127.0.0.1:0".parse().unwrap(),
        UpstreamTarget::new(url)
            .with_connect_timeout(Duration::from_secs(1))
            .with_request_timeout(Duration::from_secs(5)),
    )
}

/// An address that nothing is listening on — bind an ephemeral port, capture
/// it, then drop the listener. Any forward to it fails, which lets a test
/// prove a response came from a stub/replay rather than the upstream.
pub fn unreachable_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

/// Count non-blank NDJSON lines at `path`, streaming line-by-line so it stays
/// cheap on large snapshot files. A missing file counts as zero.
pub async fn ndjson_line_count(path: &Path) -> usize {
    let Ok(file) = tokio::fs::File::open(path).await else {
        return 0;
    };
    let mut lines = tokio::io::BufReader::new(file).lines();
    let mut count = 0;
    while let Some(line) = lines.next_line().await.expect("read NDJSON line") {
        if !line.trim().is_empty() {
            count += 1;
        }
    }
    count
}

/// Write a single recorded exchange into a fresh NDJSON file at `path`.
pub async fn seed_snapshot(
    path: &Path,
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
