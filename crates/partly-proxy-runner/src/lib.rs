//! Reusable wiring for the `partly-proxy-runner` binary — exposed so
//! integration tests can drive it without spawning a child process.
//!
//! See `SPECIFICATION.md` §18. Production deployments are expected to
//! replace this binary with their own wiring; the library exists so that
//! "is the proxy up?" probes (`/health`, `/ready`) are reproducible.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http::header::CONTENT_TYPE;
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use partly_proxy_lib::{
    ClusterHandle, ProxyClusterBuilder, ProxyConfig, ProxyError, RecordingConfig,
    Result as ProxyResult, UpstreamTarget,
};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Configuration for the runner. Constructed from env vars by
/// [`RunnerOptions::from_env`] or programmatically by tests.
#[derive(Debug, Clone)]
pub struct RunnerOptions {
    pub proxy_bind: SocketAddr,
    pub upstream_url: String,
    pub health_bind: SocketAddr,
    pub tcp_control_bind: Option<SocketAddr>,
    pub recording: RecordingConfig,
}

impl Default for RunnerOptions {
    fn default() -> Self {
        Self {
            proxy_bind: "0.0.0.0:8080".parse().unwrap(),
            upstream_url: "http://127.0.0.1:8000".to_string(),
            health_bind: "0.0.0.0:9090".parse().unwrap(),
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
        if let Ok(v) = std::env::var("PARTLY_PROXY_HEALTH_BIND") {
            opts.health_bind = v
                .parse()
                .map_err(|e| RunnerError::Config(format!("PARTLY_PROXY_HEALTH_BIND: {e}")))?;
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

/// Result of bringing up the runner. Holds the shared cluster handle
/// (`Arc`-wrapped so the health server can read it concurrently) and the
/// addresses of both listeners.
pub struct RunningRunner {
    pub cluster: Arc<ClusterHandle>,
    pub proxy_addr: SocketAddr,
    pub health_addr: SocketAddr,
    health_task: JoinHandle<()>,
    health_shutdown: watch::Sender<bool>,
}

impl RunningRunner {
    /// Graceful shutdown: stop the health server first (which drops its
    /// Arc reference), then the cluster. Returns the cluster shutdown error
    /// if any.
    pub async fn shutdown(self) -> ProxyResult<()> {
        let _ = self.health_shutdown.send(true);
        let _ = self.health_task.await;
        // After the health task has exited, we should be the sole holder of
        // the cluster Arc. If not, something else is keeping a reference and
        // we cannot consume the handle.
        let cluster = Arc::try_unwrap(self.cluster).map_err(|_| {
            ProxyError::Shutdown("cluster handle is still referenced after health task exit".into())
        })?;
        cluster.shutdown().await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RunnerError {
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("proxy error: {0}")]
    Proxy(#[from] partly_proxy_lib::ProxyError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Bring up the proxy listener and the health server.
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
    let cluster = Arc::new(cluster);

    let (health_addr, health_task, health_shutdown) =
        spawn_health(opts.health_bind, cluster.clone()).await?;

    Ok(RunningRunner {
        cluster,
        proxy_addr,
        health_addr,
        health_task,
        health_shutdown,
    })
}

/// Spawn the health HTTP server.
///
/// Per-connection tasks are tracked in a `JoinSet` so the accept loop can
/// drain them on shutdown — once the loop returns, no task is still
/// holding a clone of the cluster `Arc`, which is what `RunningRunner::shutdown`
/// needs in order to call `Arc::try_unwrap` and consume the handle.
async fn spawn_health(
    bind: SocketAddr,
    cluster: Arc<ClusterHandle>,
) -> Result<(SocketAddr, JoinHandle<()>, watch::Sender<bool>), RunnerError> {
    let listener = TcpListener::bind(bind).await?;
    let bound = listener.local_addr()?;
    let (tx, mut rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut connections: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                biased;
                res = rx.changed() => {
                    if res.is_err() || *rx.borrow() {
                        // Drain in-flight connections so every Arc clone is
                        // dropped before we return.
                        while connections.join_next().await.is_some() {}
                        return;
                    }
                }
                accepted = listener.accept() => {
                    let Ok((stream, _peer)) = accepted else { continue };
                    let cluster = cluster.clone();
                    connections.spawn(async move {
                        let io = TokioIo::new(stream);
                        let svc = service_fn(move |req| {
                            let c = cluster.clone();
                            async move { Ok::<_, Infallible>(handle_health(req, c).await) }
                        });
                        let builder = auto::Builder::new(TokioExecutor::new());
                        let _ = builder.serve_connection(io, svc).await;
                    });
                }
                Some(_) = connections.join_next() => {
                    // Reap completed connections so the set doesn't grow
                    // unboundedly under long-lived deployments.
                }
            }
        }
    });
    Ok((bound, task, tx))
}

async fn handle_health(
    req: Request<Incoming>,
    cluster: Arc<ClusterHandle>,
) -> Response<Full<Bytes>> {
    match req.uri().path() {
        "/health" | "/healthz" => simple_response(StatusCode::OK, b"ok"),
        "/ready" | "/readyz" => readiness_response(cluster).await,
        _ => simple_response(StatusCode::NOT_FOUND, b"not found"),
    }
}

async fn readiness_response(cluster: Arc<ClusterHandle>) -> Response<Full<Bytes>> {
    let statuses = cluster.upstream_statuses().await;
    let all_ready = !statuses.is_empty() && statuses.iter().all(|s| s.ready);

    let payload = ReadinessPayload {
        ready: all_ready,
        upstreams: statuses
            .iter()
            .map(|s| ReadinessUpstream {
                name: &s.name,
                bound_addr: s.bound_addr.to_string(),
                ready: s.ready,
                exchange_count: s.exchange_count,
            })
            .collect(),
    };
    let body = serde_json::to_vec_pretty(&payload).unwrap_or_default();
    let status = if all_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))
        .expect("static response is valid")
}

#[derive(serde::Serialize)]
struct ReadinessPayload<'a> {
    ready: bool,
    upstreams: Vec<ReadinessUpstream<'a>>,
}

#[derive(serde::Serialize)]
struct ReadinessUpstream<'a> {
    name: &'a str,
    bound_addr: String,
    ready: bool,
    exchange_count: usize,
}

fn simple_response(status: StatusCode, body: &'static [u8]) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from_static(body)))
        .expect("static response is valid")
}
