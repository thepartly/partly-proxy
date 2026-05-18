//! Cluster builder — see `SPECIFICATION.md` §4.
//!
//! `ProxyClusterBuilder` is the single entry point for constructing a cluster.
//! It accumulates configuration via fluent methods and then binds every
//! listener in `run()`, returning a [`ClusterHandle`](crate::ClusterHandle).
//! Duplicate upstream names are rejected by `run()`, not at registration
//! time — this makes the builder side-effect-free and the validation
//! deterministic.

use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};

use partly_proxy_types::{ProxyError, Result, SharedStorage};
use tokio::sync::watch;

use crate::{
    cluster::{ClusterHandle, RunningUpstream},
    command,
    config::{ProxyConfig, RecordingConfig},
    control_plane, listener,
    middleware::{ProxyMiddleware, SharedMiddleware},
    recorder::Recorder,
    replay::ReplaySource,
    upstream::UpstreamRegistry,
};

/// Builder for a [`ClusterHandle`](crate::ClusterHandle).
#[derive(Default)]
pub struct ProxyClusterBuilder {
    recording: RecordingConfig,
    upstreams: Vec<UpstreamSpec>,
    global_middleware: Vec<SharedMiddleware>,
    tcp_control_addr: Option<SocketAddr>,
    storage: Option<SharedStorage>,
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
            .field("storage", &self.storage.is_some())
            .finish()
    }
}

/// One registered upstream and the configuration that describes it.
pub(crate) struct UpstreamSpec {
    pub name: String,
    pub config: ProxyConfig,
    pub middleware: Vec<SharedMiddleware>,
    pub replay: Option<ReplaySource>,
}

impl std::fmt::Debug for UpstreamSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpstreamSpec")
            .field("name", &self.name)
            .field("middleware", &self.middleware.len())
            .field("replay", &self.replay.is_some())
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

    /// Register an upstream with no per-upstream middleware and no replay
    /// source. Names should be unique; duplicates are surfaced by `run()`.
    pub fn add_upstream(mut self, name: impl Into<String>, config: ProxyConfig) -> Self {
        self.upstreams.push(UpstreamSpec {
            name: name.into(),
            config,
            middleware: Vec::new(),
            replay: None,
        });
        self
    }

    /// Register an upstream with a list of per-upstream middleware. The
    /// effective chain for that upstream becomes `global ++ per_upstream`.
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
            replay: None,
        });
        self
    }

    /// Register an upstream with both per-upstream middleware and an
    /// optional replay source — the most general per-upstream registration.
    ///
    /// See `SPECIFICATION.md` §8.3: replay is always layered with middleware
    /// and stubs; stubs (registered later over the command plane) take
    /// priority over replay, which takes priority over the upstream forward.
    pub fn add_upstream_with(
        mut self,
        name: impl Into<String>,
        config: ProxyConfig,
        middleware: Vec<SharedMiddleware>,
        replay: Option<ReplaySource>,
    ) -> Self {
        self.upstreams.push(UpstreamSpec {
            name: name.into(),
            config,
            middleware,
            replay,
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

    /// Override the recorder's storage backend.
    ///
    /// When set, `run()` builds the recorder via
    /// [`Recorder::with_storage`](crate::Recorder::with_storage) and the
    /// provided `SharedStorage` is used for every recorded exchange. When
    /// unset, the recorder falls back to opening the default backend from
    /// `RecordingConfig::persist_path` (NDJSON when the `storage-jsonl`
    /// feature is on, in-memory only otherwise).
    pub fn storage(mut self, storage: SharedStorage) -> Self {
        self.storage = Some(storage);
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

        let recorder = match self.storage.clone() {
            Some(storage) => Recorder::with_storage(self.recording.clone(), Some(storage)),
            None => Recorder::new(self.recording.clone()),
        };
        let (shutdown_tx, shutdown_rx) = watch::channel::<Option<std::time::Duration>>(None);
        let mut upstreams = BTreeMap::new();
        let mut registry = UpstreamRegistry::default();

        let global_middleware = self.global_middleware;

        for spec in self.upstreams {
            let name = spec.name.clone();
            match listener::spawn_listener(
                spec,
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
