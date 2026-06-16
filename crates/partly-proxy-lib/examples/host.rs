//! Minimal example of hosting the proxy in a binary.
//!
//! Run with:
//!
//! ```sh
//! PARTLY_PROXY_BIND=127.0.0.1:8080 \
//! PARTLY_PROXY_UPSTREAM=http://127.0.0.1:8000 \
//! PARTLY_PROXY_TCP_CONTROL_BIND=127.0.0.1:4500 \
//! cargo run --example host -p partly-proxy-lib
//! ```
//!
//! Set `PARTLY_PROXY_RECORDING_PATH=/tmp/trace.ndjson` to additionally
//! persist exchanges to NDJSON via the `partly-proxy-storage-jsonl`
//! backend.
//!
//! This is a starting point — production deployments should write their own
//! binary that adds whatever framing their infrastructure needs (health
//! probes, metrics, config-file parsing, etc.). The point of the example
//! is to show how short that binary is when it doesn't have to do those
//! things.

use std::{net::SocketAddr, sync::Arc};

use partly_proxy_lib::{
    ProxyClusterBuilder, ProxyConfig, RecordingConfig, Result, SharedStorage, Snapshots,
    UpstreamTarget,
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let proxy_bind: SocketAddr = std::env::var("PARTLY_PROXY_BIND")
        .unwrap_or_else(|_| "0.0.0.0:8080".into())
        .parse()
        .expect("PARTLY_PROXY_BIND must be a valid SocketAddr");
    let upstream_url =
        std::env::var("PARTLY_PROXY_UPSTREAM").unwrap_or_else(|_| "http://127.0.0.1:8000".into());
    let tcp_control_bind: Option<SocketAddr> = std::env::var("PARTLY_PROXY_TCP_CONTROL_BIND")
        .ok()
        .map(|v| {
            v.parse()
                .expect("PARTLY_PROXY_TCP_CONTROL_BIND must be a valid SocketAddr")
        });

    // Storage is configured per upstream: the same medium is loaded for
    // replay and appended to while recording. Here the single "upstream"
    // gets its own JSONL file when PARTLY_PROXY_RECORDING_PATH is set.
    let snapshots: Option<Snapshots> = match std::env::var("PARTLY_PROXY_RECORDING_PATH").ok() {
        Some(path) => {
            let storage: SharedStorage =
                Arc::new(partly_proxy_lib::jsonl::JsonlStorage::open(path).await?);
            Some(Snapshots::from_storage(storage))
        }
        None => None,
    };

    let mut builder = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(10_000))
        .add_upstream_with(
            "upstream",
            ProxyConfig::http(proxy_bind, UpstreamTarget::new(upstream_url)),
            Vec::new(),
            snapshots,
        );
    if let Some(addr) = tcp_control_bind {
        builder = builder.tcp_control_plane(addr);
    }

    let cluster = builder.run().await?;
    tracing::info!(
        proxy = %cluster.addr("upstream").unwrap(),
        control = ?cluster.tcp_control_addr(),
        "proxy started; Ctrl+C to shut down",
    );

    tokio::signal::ctrl_c().await.ok();
    tracing::info!("Ctrl+C received; shutting down");
    cluster.shutdown().await
}
