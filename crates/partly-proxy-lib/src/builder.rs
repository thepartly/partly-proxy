//! Cluster builder — see `SPECIFICATION.md` §4.
//!
//! `ProxyClusterBuilder` is the single entry point for constructing a cluster.
//! It accumulates configuration via fluent methods and then binds every
//! listener in `run()`, returning a [`ClusterHandle`](crate::ClusterHandle).
//! Duplicate upstream names are rejected by `run()`, not at registration
//! time — this makes the builder side-effect-free and the validation
//! deterministic.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};

use bytes::Bytes;
use http::{StatusCode, header::CONTENT_TYPE};
use partly_proxy_types::{ProxyError, Result, SharedStorage};
use tokio::sync::watch;

use crate::{
    cluster::{ClusterHandle, RunningUpstream},
    command,
    config::{Mode, ProxyConfig, RecordingConfig, UpstreamTarget},
    control_plane, listener,
    middleware::{ProxyMiddleware, SharedMiddleware},
    proxy_io::{ProxyRequest, ProxyResponse},
    recorder::Recorder,
    replay::ReplaySource,
    upstream::UpstreamRegistry,
};

/// Closure invoked when a request in [`Mode::Replay`] finds no matching stub
/// and no matching snapshot. Receives the unmatched request and returns the
/// response to send back to the caller.
pub type ReplayMissHandler = Arc<dyn Fn(ProxyRequest) -> ProxyResponse + Send + Sync>;

pub(crate) fn default_replay_miss_handler() -> ReplayMissHandler {
    Arc::new(|_req| {
        ProxyResponse::new(StatusCode::SERVICE_UNAVAILABLE)
            .with_header(CONTENT_TYPE, Bytes::from_static(b"application/json"))
            .with_body(Bytes::from_static(b"{}"))
    })
}

/// Builder for a [`ClusterHandle`](crate::ClusterHandle).
pub struct ProxyClusterBuilder {
    recording: RecordingConfig,
    default_mode: Mode,
    upstreams: Vec<UpstreamSpec>,
    global_middleware: Vec<SharedMiddleware>,
    tcp_control_addr: Option<SocketAddr>,
    replay_miss_handler: ReplayMissHandler,
}

impl Default for ProxyClusterBuilder {
    fn default() -> Self {
        Self {
            recording: RecordingConfig::default(),
            default_mode: Mode::Record,
            upstreams: Vec::new(),
            global_middleware: Vec::new(),
            tcp_control_addr: None,
            replay_miss_handler: default_replay_miss_handler(),
        }
    }
}

impl std::fmt::Debug for ProxyClusterBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyClusterBuilder")
            .field("recording", &self.recording)
            .field(
                "upstreams",
                &self.upstreams.iter().map(|u| &u.name).collect::<Vec<_>>(),
            )
            .field("global_middleware", &self.global_middleware.len())
            .field("tcp_control_addr", &self.tcp_control_addr)
            .finish_non_exhaustive()
    }
}

/// One registered upstream and the configuration that describes it.
pub(crate) struct UpstreamSpec {
    pub name: String,
    pub config: ProxyConfig,
    pub middleware: Vec<SharedMiddleware>,
    /// Per-upstream storage backend — loaded for replay and (in `Record`)
    /// appended to as the recording sink. Resolved at `run()`.
    pub storage: Option<SharedStorage>,
    pub mode: Mode,
    pub replay_miss_handler: ReplayMissHandler,
}

impl std::fmt::Debug for UpstreamSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpstreamSpec")
            .field("name", &self.name)
            .field("middleware", &self.middleware.len())
            .field("storage", &self.storage.is_some())
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

impl ProxyClusterBuilder {
    /// Fresh builder with defaults — recording on, 10k cap, no upstreams.
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the recording configuration. The last call wins.
    pub fn recording(mut self, cfg: RecordingConfig) -> Self {
        self.recording = cfg;
        self
    }

    /// Set the default mode for all subsequently-added upstreams and update
    /// the recording config accordingly (`Record` → enabled, `Replay` →
    /// disabled). Eliminates the need to pass the same mode to every
    /// `add_upstream_with*` call and to independently configure recording.
    pub fn default_mode(mut self, mode: Mode) -> Self {
        self.recording = match mode {
            Mode::Record => RecordingConfig::default(),
            Mode::Replay => RecordingConfig::disabled(),
        };
        self.default_mode = mode;
        self
    }

    /// Override the handler invoked when a request in [`Mode::Replay`] finds
    /// no matching stub and no matching snapshot. The closure receives the
    /// unmatched [`ProxyRequest`] and returns the response sent to the caller.
    ///
    /// The default handler returns `503 {}` with `Content-Type: application/json`.
    /// Use this to change the status, body, headers, or trigger a side-effect
    /// (e.g. a structured log or metric) on miss. Applies to all
    /// subsequently-added upstreams.
    pub fn on_replay_miss<F>(mut self, f: F) -> Self
    where
        F: Fn(ProxyRequest) -> ProxyResponse + Send + Sync + 'static,
    {
        self.replay_miss_handler = Arc::new(f);
        self
    }

    /// Register an upstream with no per-upstream middleware and no replay
    /// source. Names should be unique; duplicates are surfaced by `run()`.
    /// Uses the builder's current [`default_mode`](Self::default_mode).
    pub fn add_upstream(mut self, name: impl Into<String>, config: ProxyConfig) -> Self {
        self.upstreams.push(UpstreamSpec {
            name: name.into(),
            config,
            middleware: Vec::new(),
            storage: None,
            mode: self.default_mode,
            replay_miss_handler: Arc::clone(&self.replay_miss_handler),
        });
        self
    }

    /// Register an upstream with a list of per-upstream middleware. The
    /// effective chain for that upstream becomes `global ++ per_upstream`.
    /// Uses the builder's current [`default_mode`](Self::default_mode).
    pub fn add_upstream_with_middleware(
        mut self,
        name: impl Into<String>,
        config: ProxyConfig,
        middleware: Vec<SharedMiddleware>,
    ) -> Self {
        self.upstreams.push(UpstreamSpec {
            name: name.into(),
            config,
            middleware,
            storage: None,
            mode: self.default_mode,
            replay_miss_handler: Arc::clone(&self.replay_miss_handler),
        });
        self
    }

    /// Register an upstream with both per-upstream middleware and an
    /// optional [`SnapshotStorage`](crate::SnapshotStorage) backend. Uses the
    /// builder's current [`default_mode`](Self::default_mode).
    ///
    /// The `storage` backend is the single per-upstream storage knob: at
    /// [`run()`](Self::run) its existing contents are loaded into the replay
    /// source, and in [`Mode::Record`] every new exchange for this upstream
    /// is appended back to it. Construct a backend
    /// (e.g. `JsonlStorage::open(path)` or
    /// [`InMemoryStorage`](crate::InMemoryStorage)), wrap it in an `Arc`, and
    /// pass it here. Give each upstream its own backend to keep recordings
    /// separate.
    ///
    /// See `SPECIFICATION.md` §8.3: in `Record` mode, stubs take priority
    /// over replay, which takes priority over the upstream forward. To
    /// replay snapshots without ever forwarding to the upstream, call
    /// [`Self::default_mode`] with [`Mode::Replay`] first.
    pub fn add_upstream_with(
        mut self,
        name: impl Into<String>,
        config: ProxyConfig,
        middleware: Vec<SharedMiddleware>,
        storage: Option<SharedStorage>,
    ) -> Self {
        self.upstreams.push(UpstreamSpec {
            name: name.into(),
            config,
            middleware,
            storage,
            mode: self.default_mode,
            replay_miss_handler: Arc::clone(&self.replay_miss_handler),
        });
        self
    }

    /// Register an upstream with an explicit [`Mode`], overriding
    /// [`default_mode`](Self::default_mode) for this entry.
    ///
    /// In [`Mode::Replay`] the terminal never forwards to the upstream — a
    /// missing snapshot yields the replay-miss response (default `503 {}`).
    /// In [`Mode::Record`] the terminal falls through to the upstream on
    /// miss and (when recording is enabled) appends the exchange to the
    /// upstream's `storage` backend.
    pub fn add_upstream_with_mode(
        mut self,
        name: impl Into<String>,
        config: ProxyConfig,
        middleware: Vec<SharedMiddleware>,
        storage: Option<SharedStorage>,
        mode: Mode,
    ) -> Self {
        self.upstreams.push(UpstreamSpec {
            name: name.into(),
            config,
            middleware,
            storage,
            mode,
            replay_miss_handler: Arc::clone(&self.replay_miss_handler),
        });
        self
    }

    /// Register a stub — an upstream that never forwards to a real backend.
    /// All requests are handled by `middleware`; anything that falls through
    /// the chain invokes the replay-miss handler (default `503 {}`).
    ///
    /// Equivalent to `add_upstream_with_mode` with a dummy upstream target
    /// and `Mode::Replay`, but without requiring callers to supply a
    /// `ProxyConfig` with a meaningless upstream URL.
    pub fn add_stub(
        mut self,
        name: impl Into<String>,
        bind_addr: SocketAddr,
        middleware: Vec<SharedMiddleware>,
    ) -> Self {
        let config = ProxyConfig::http(bind_addr, UpstreamTarget::new("http://stub.internal:0"));
        self.upstreams.push(UpstreamSpec {
            name: name.into(),
            config,
            middleware,
            storage: None,
            mode: Mode::Replay,
            replay_miss_handler: Arc::clone(&self.replay_miss_handler),
        });
        self
    }

    /// Append a middleware to the global chain. Global middleware applies to
    /// every upstream and runs before any per-upstream middleware.
    pub fn add_middleware<M: ProxyMiddleware>(mut self, mw: M) -> Self {
        self.global_middleware.push(Arc::new(mw));
        self
    }

    /// Append a pre-`Arc`-wrapped middleware to the global chain. Useful when
    /// the same instance is shared with other code paths.
    pub fn add_shared_middleware(mut self, mw: SharedMiddleware) -> Self {
        self.global_middleware.push(mw);
        self
    }

    /// Inspect the recording configuration the builder will use.
    pub fn recording_config(&self) -> &RecordingConfig {
        &self.recording
    }

    /// Inspect the upstream names registered so far, in registration order.
    pub fn upstream_names(&self) -> Vec<&str> {
        self.upstreams.iter().map(|u| u.name.as_str()).collect()
    }

    /// Number of registered upstreams.
    pub fn upstream_count(&self) -> usize {
        self.upstreams.len()
    }

    /// Number of global middleware registered so far.
    pub fn global_middleware_count(&self) -> usize {
        self.global_middleware.len()
    }

    /// Enable the TCP JSON-Lines control plane on `addr`. See
    /// `SPECIFICATION.md` §12.2.
    pub fn tcp_control_plane(mut self, addr: SocketAddr) -> Self {
        self.tcp_control_addr = Some(addr);
        self
    }

    /// Bind every listener and start its accept loop.
    ///
    /// Returns a [`ClusterHandle`](crate::ClusterHandle) once all listeners
    /// are bound. If any bind fails, every already-bound listener is shut
    /// down before the error is returned, so partial bring-up never leaks
    /// listening sockets.
    pub async fn run(self) -> Result<ClusterHandle> {
        let mut seen = HashSet::new();
        for spec in &self.upstreams {
            if !seen.insert(spec.name.as_str()) {
                return Err(ProxyError::Command(format!(
                    "duplicate upstream name in cluster: {}",
                    spec.name
                )));
            }
        }

        // Resolve each upstream's storage backend up front: load its
        // contents into a replay source for the hot path, and register the
        // backend in a per-upstream routing map so the recorder appends new
        // exchanges back to it. Loading is async (it streams the backend),
        // so it happens here in `run()` rather than in the synchronous
        // `add_upstream_*` builders.
        let mut routes: HashMap<String, SharedStorage> = HashMap::new();
        let mut resolved = Vec::with_capacity(self.upstreams.len());
        for mut spec in self.upstreams {
            let replay = match spec.storage.take() {
                Some(storage) => {
                    let replay = ReplaySource::from_storage(storage.as_ref()).await?;
                    routes.insert(spec.name.clone(), storage);
                    Some(replay)
                }
                None => None,
            };
            resolved.push((spec, replay));
        }

        let recorder = Recorder::with_routes(self.recording.clone(), routes);
        let (shutdown_tx, shutdown_rx) = watch::channel::<Option<std::time::Duration>>(None);
        let mut upstreams = BTreeMap::new();
        let mut registry = UpstreamRegistry::default();

        let global_middleware = self.global_middleware;

        for (spec, replay) in resolved {
            let name = spec.name.clone();
            match listener::spawn_listener(
                spec,
                replay,
                global_middleware.clone(),
                recorder.clone(),
                shutdown_rx.clone(),
            )
            .await
            {
                Ok(running) => {
                    registry.insert(running.runtime);
                    upstreams.insert(
                        name,
                        RunningUpstream {
                            bound_addr: running.bound_addr,
                            task: running.task,
                        },
                    );
                }
                Err(e) => {
                    // Tear down whatever we managed to bring up.
                    let _ = shutdown_tx.send(Some(std::time::Duration::ZERO));
                    for (_, up) in upstreams {
                        let _ = up.task.await;
                    }
                    return Err(e);
                }
            }
        }

        let registry = Arc::new(registry);
        let (command_sender, command_task) =
            command::spawn_processor(registry, recorder.clone(), shutdown_rx.clone());

        let tcp_control = if let Some(addr) = self.tcp_control_addr {
            match control_plane::spawn_tcp_control_plane(addr, command_sender.clone(), shutdown_rx)
                .await
            {
                Ok(rc) => Some(rc),
                Err(e) => {
                    let _ = shutdown_tx.send(Some(std::time::Duration::ZERO));
                    for (_, up) in upstreams {
                        let _ = up.task.await;
                    }
                    let _ = command_task.await;
                    return Err(e);
                }
            }
        } else {
            None
        };

        Ok(ClusterHandle::new(
            upstreams,
            shutdown_tx,
            self.recording,
            recorder,
            command_sender,
            command_task,
            tcp_control,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;
    use crate::config::UpstreamTarget;

    fn addr() -> SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }

    #[test]
    fn new_builder_has_defaults() {
        let b = ProxyClusterBuilder::new();
        assert_eq!(b.upstream_count(), 0);
        let r = b.recording_config();
        assert!(r.enabled);
        assert_eq!(r.max_in_memory, 10_000);
    }

    #[test]
    fn recording_override_takes_last() {
        let b = ProxyClusterBuilder::new()
            .recording(RecordingConfig::in_memory(50))
            .recording(RecordingConfig::in_memory(99));
        assert_eq!(b.recording_config().max_in_memory, 99);
    }

    #[test]
    fn add_upstream_preserves_registration_order() {
        let cfg_a = ProxyConfig::http(addr(), UpstreamTarget::new("http://a"));
        let cfg_b = ProxyConfig::http(addr(), UpstreamTarget::new("http://b"));
        let cfg_c = ProxyConfig::http(addr(), UpstreamTarget::new("http://c"));

        let b = ProxyClusterBuilder::new()
            .add_upstream("a", cfg_a)
            .add_upstream("b", cfg_b)
            .add_upstream("c", cfg_c);

        assert_eq!(b.upstream_names(), vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn run_rejects_duplicate_upstream_names() {
        let cfg = || ProxyConfig::http(addr(), UpstreamTarget::new("http://x"));
        let err = ProxyClusterBuilder::new()
            .add_upstream("api", cfg())
            .add_upstream("api", cfg())
            .run()
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("duplicate upstream name"), "got: {msg}");
    }

    #[tokio::test]
    async fn run_with_no_upstreams_yields_empty_handle() {
        let h = ProxyClusterBuilder::new().run().await.unwrap();
        assert!(h.upstream_names().is_empty());
        h.shutdown().await.unwrap();
    }
}
