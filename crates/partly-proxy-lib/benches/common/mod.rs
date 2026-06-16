//! Shared helpers for the end-to-end proxy benchmarks.
//!
//! Each bench file calls into here to bring up an in-process echo upstream,
//! a `partly-proxy-lib` listener configured with the requested recording
//! flavour, and a hyper-util HTTP/1 client pointed at the listener. The
//! returned [`ProxyHandle`] owns the echo task, the cluster, and the
//! optional tempdir backing the storage file — drop order tears the whole
//! stack down cleanly when the bench finishes.

#![allow(dead_code)]

use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::Request;
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use partly_proxy_echo as echo;
use partly_proxy_lib::{
    ClusterHandle, ProxyClusterBuilder, ProxyConfig, RecordingConfig, SharedStorage, Snapshots,
    UpstreamTarget,
};
use tempfile::TempDir;
use tokio::task::JoinHandle;

/// Hyper-util client used by every bench. `Full<Bytes>` works for both
/// empty GETs (`Full::new(Bytes::new())`) and large POSTs without needing
/// a second client type.
pub type BenchClient = Client<HttpConnector, Full<Bytes>>;

pub fn http_client() -> BenchClient {
    let mut connector = HttpConnector::new();
    connector.set_nodelay(true);
    // `pool_idle_timeout` defaults to 90s, plenty for benches.
    Client::builder(TokioExecutor::new()).build(connector)
}

/// Spawn the echo upstream on 127.0.0.1:0 and return (addr, task).
pub async fn spawn_echo() -> (SocketAddr, JoinHandle<()>) {
    let (addr, listener) = echo::bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("echo bind");
    let task = tokio::spawn(async move {
        let _ = echo::serve(listener).await;
    });
    (addr, task)
}

/// Recording flavour for the proxy under test.
#[derive(Clone, Copy, Debug)]
pub enum Recording {
    /// Recording fully disabled — `Recorder::record` is a no-op.
    Disabled,
    /// In-memory ring only, no `SnapshotStorage` backend.
    InMemory,
    /// In-memory ring + NDJSON file (`partly-proxy-storage-jsonl`).
    Jsonl,
    /// In-memory ring + `SQLite` file (`partly-proxy-storage-sqlite`).
    Sqlite,
}

impl Recording {
    pub fn label(self) -> &'static str {
        match self {
            Recording::Disabled => "disabled",
            Recording::InMemory => "in_memory",
            Recording::Jsonl => "jsonl",
            Recording::Sqlite => "sqlite",
        }
    }

    /// Iteration order used by every matrix bench. Listed here so adding
    /// a new flavour automatically widens the matrix instead of having
    /// to update each bench file.
    pub fn all() -> &'static [Recording] {
        &[
            Recording::Disabled,
            Recording::InMemory,
            Recording::Jsonl,
            Recording::Sqlite,
        ]
    }
}

/// Owns every resource a bench needs while it runs. Dropping (or calling
/// [`ProxyHandle::shutdown`]) tears down the cluster, the echo task, and
/// the tempdir backing the persisted storage in that order.
pub struct ProxyHandle {
    pub cluster: ClusterHandle,
    pub proxy_addr: SocketAddr,
    pub upstream_addr: SocketAddr,
    pub echo_task: JoinHandle<()>,
    pub _tempdir: Option<TempDir>,
}

impl ProxyHandle {
    pub async fn shutdown(self) {
        let _ = self.cluster.shutdown().await;
        self.echo_task.abort();
    }
}

pub async fn spawn_proxy(recording: Recording) -> ProxyHandle {
    let (echo_addr, echo_task) = spawn_echo().await;

    let (storage, tempdir): (Option<SharedStorage>, Option<TempDir>) = match recording {
        Recording::Disabled | Recording::InMemory => (None, None),
        Recording::Jsonl => {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("bench.ndjson");
            let storage: SharedStorage = Arc::new(
                partly_proxy_storage_jsonl::JsonlStorage::open(&path)
                    .await
                    .expect("open jsonl"),
            );
            (Some(storage), Some(dir))
        }
        Recording::Sqlite => {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("bench.sqlite");
            let storage: SharedStorage = Arc::new(
                partly_proxy_storage_sqlite::SqliteStorage::open(&path)
                    .await
                    .expect("open sqlite"),
            );
            (Some(storage), Some(dir))
        }
    };

    let cfg = RecordingConfig {
        enabled: !matches!(recording, Recording::Disabled),
        max_in_memory: 10_000,
    };

    let snapshots = storage.map(Snapshots::from_storage);
    let builder = ProxyClusterBuilder::new().recording(cfg).add_upstream_with(
        "upstream",
        ProxyConfig::http(
            "127.0.0.1:0".parse().unwrap(),
            UpstreamTarget::new(format!("http://{echo_addr}")),
        ),
        Vec::new(),
        snapshots,
    );
    let cluster = builder.run().await.expect("cluster build");
    let proxy_addr = cluster.addr("upstream").expect("bound addr");

    ProxyHandle {
        cluster,
        proxy_addr,
        upstream_addr: echo_addr,
        echo_task,
        _tempdir: tempdir,
    }
}

/// Issue a single GET against the proxy and drain the response body. The
/// drain matters: without it the connection isn't released back to the
/// pool and subsequent requests pay a fresh-connect cost.
pub async fn do_get(client: &BenchClient, url: &hyper::Uri) {
    let req = Request::get(url.clone())
        .body(Full::new(Bytes::new()))
        .expect("request build");
    let resp = client.request(req).await.expect("request send");
    let _ = resp.into_body().collect().await.expect("response drain");
}

/// Issue a single POST against the proxy with `body`. Cloning a `Bytes`
/// is cheap — every iteration shares the same allocation.
pub async fn do_post(client: &BenchClient, url: &hyper::Uri, body: Bytes) {
    let req = Request::post(url.clone())
        .header("content-type", "application/octet-stream")
        .body(Full::new(body))
        .expect("request build");
    let resp = client.request(req).await.expect("request send");
    let _ = resp.into_body().collect().await.expect("response drain");
}

/// Build a multi-thread tokio runtime sized to the host's logical cores.
/// Used by throughput / large-body benches where we want real concurrency.
pub fn multi_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Build a current-thread runtime — used by latency benches so the
/// reported per-iteration time isn't smeared across worker threads.
pub fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}
