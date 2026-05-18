//! Cluster builder — see `SPECIFICATION.md` §4.
//!
//! `ProxyClusterBuilder` is the single entry point for constructing a cluster.
//! It accumulates configuration via fluent methods and then binds every
//! listener in `run()`, returning a [`ClusterHandle`](crate::ClusterHandle).
//! Duplicate upstream names are rejected by `run()`, not at registration
//! time — this makes the builder side-effect-free and the validation
//! deterministic.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use tokio::sync::watch;

use crate::cluster::{ClusterHandle, RunningUpstream};
use crate::config::{ProxyConfig, RecordingConfig};
use crate::error::{ProxyError, Result};
use crate::listener;
use crate::middleware::{ProxyMiddleware, SharedMiddleware};
use crate::recorder::Recorder;

/// Builder for a [`ClusterHandle`](crate::ClusterHandle).
#[derive(Default)]
pub struct ProxyClusterBuilder {
    recording: RecordingConfig,
    upstreams: Vec<UpstreamSpec>,
    global_middleware: Vec<SharedMiddleware>,
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
            .finish()
    }
}

/// One registered upstream and the configuration that describes it.
///
/// Kept public-in-crate so later slices can extend it (replay source, etc.)
/// without rewriting the builder.
pub(crate) struct UpstreamSpec {
    pub name: String,
    pub config: ProxyConfig,
    pub middleware: Vec<SharedMiddleware>,
}

impl std::fmt::Debug for UpstreamSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpstreamSpec")
            .field("name", &self.name)
            .field("middleware", &self.middleware.len())
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

        let recorder = Recorder::new(self.recording.clone()).await?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut upstreams = BTreeMap::new();

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
                    let _ = shutdown_tx.send(true);
                    for (_, up) in upstreams {
                        let _ = up.task.await;
                    }
                    return Err(e);
                }
            }
        }

        Ok(ClusterHandle::new(
            upstreams,
            shutdown_tx,
            self.recording,
            recorder,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::UpstreamTarget;
    use std::net::SocketAddr;

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
