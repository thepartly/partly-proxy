//! Reusable wiring for the `partly-proxy-runner` binary — exposed so
//! integration tests can drive it without spawning a child process.
//!
//! See `SPECIFICATION.md` §18. Production deployments are expected to
//! replace this binary with their own wiring; the library exists so the
//! end-to-end "build a cluster, run it, shut it down" path is reproducible.

use std::net::SocketAddr;

use partly_proxy_lib::{
    ClusterHandle, ProxyClusterBuilder, ProxyConfig, RecordingConfig, Result as ProxyResult,
    UpstreamTarget,
};

/// Configuration for the runner. Constructed from env vars by
/// [`RunnerOptions::from_env`] or programmatically by tests.
#[derive(Debug, Clone)]
pub struct RunnerOptions {
    pub proxy_bind: SocketAddr,
    pub upstream_url: String,
    pub tcp_control_bind: Option<SocketAddr>,
    pub recording: RecordingConfig,
}

impl Default for RunnerOptions {
    fn default() -> Self {
        Self {
            proxy_bind: "0.0.0.0:8080".parse().unwrap(),
            upstream_url: "http://127.0.0.1:8000".to_string(),
            tcp_control_bind: None,
            recording: RecordingConfig::default(),
        }
    }
}

impl RunnerOptions {
    /// Build options from environment variables. Unset variables fall back
    /// to [`RunnerOptions::default`].
    pub fn from_env() -> Result<Self, RunnerError> {
        let mut opts = Self::default();
        if let Ok(v) = std::env::var("PARTLY_PROXY_BIND") {
            opts.proxy_bind = v
                .parse()
                .map_err(|e| RunnerError::Config(format!("PARTLY_PROXY_BIND: {e}")))?;
        }
        if let Ok(v) = std::env::var("PARTLY_PROXY_UPSTREAM") {
            opts.upstream_url = v;
        }
        if let Ok(v) = std::env::var("PARTLY_PROXY_TCP_CONTROL_BIND") {
            opts.tcp_control_bind =
                Some(v.parse().map_err(|e| {
                    RunnerError::Config(format!("PARTLY_PROXY_TCP_CONTROL_BIND: {e}"))
                })?);
        }
        if let Ok(v) = std::env::var("PARTLY_PROXY_RECORDING_PATH") {
            opts.recording = RecordingConfig::persisted(10_000, std::path::PathBuf::from(v));
        }
        Ok(opts)
    }
}

/// Result of bringing up the runner. Holds the cluster handle and the
/// proxy's bound address (handy when the caller passed `127.0.0.1:0`).
pub struct RunningRunner {
    pub cluster: ClusterHandle,
    pub proxy_addr: SocketAddr,
}

impl RunningRunner {
    /// Graceful shutdown — delegates to the underlying [`ClusterHandle`].
    pub async fn shutdown(self) -> ProxyResult<()> {
        self.cluster.shutdown().await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RunnerError {
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("proxy error: {0}")]
    Proxy(#[from] partly_proxy_lib::ProxyError),
}

/// Bring up the proxy listener (and optional TCP control plane).
pub async fn run(opts: RunnerOptions) -> Result<RunningRunner, RunnerError> {
    let proxy_cfg = ProxyConfig::http(opts.proxy_bind, UpstreamTarget::new(opts.upstream_url));

    let mut builder = ProxyClusterBuilder::new()
        .recording(opts.recording)
        .add_upstream("upstream", proxy_cfg);
    if let Some(addr) = opts.tcp_control_bind {
        builder = builder.tcp_control_plane(addr);
    }
    let cluster = builder.run().await?;
    let proxy_addr = cluster
        .addr("upstream")
        .expect("upstream is registered above");

    Ok(RunningRunner {
        cluster,
        proxy_addr,
    })
}
