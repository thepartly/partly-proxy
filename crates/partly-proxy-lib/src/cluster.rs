//! Cluster handle — returned by [`ProxyClusterBuilder::run`](crate::ProxyClusterBuilder::run).
//!
//! Owns the shutdown signal and the join handles for every listener. Dropping
//! the handle without calling `shutdown` leaves listeners running until the
//! process exits.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::command::CommandSender;
use crate::config::RecordingConfig;
use crate::control_plane::RunningControlPlane;
use crate::error::{ProxyError, Result};
use crate::recorder::Recorder;

/// Per-upstream metadata tracked by the handle.
pub(crate) struct RunningUpstream {
    pub bound_addr: SocketAddr,
    pub task: JoinHandle<()>,
}

/// Snapshot of one upstream's runtime status — see
/// [`ClusterHandle::upstream_statuses`].
#[derive(Debug, Clone)]
pub struct UpstreamStatus {
    pub name: String,
    pub bound_addr: SocketAddr,
    /// `true` while the accept loop task is still running.
    pub ready: bool,
    /// Number of exchanges currently held in the recorder buffer for this
    /// upstream.
    pub exchange_count: usize,
}

/// Handle to a running cluster — see `SPECIFICATION.md` §4.
///
/// Not `Clone`: `shutdown` consumes the handle. To use the cluster from
/// multiple call sites, share addresses or wrap the handle in `Arc` before
/// the final shutdown call.
pub struct ClusterHandle {
    upstreams: BTreeMap<String, RunningUpstream>,
    shutdown_tx: watch::Sender<bool>,
    recording: RecordingConfig,
    recorder: Recorder,
    command_sender: CommandSender,
    command_task: Option<JoinHandle<()>>,
    tcp_control: Option<RunningControlPlane>,
}

impl std::fmt::Debug for ClusterHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterHandle")
            .field(
                "upstreams",
                &self
                    .upstreams
                    .iter()
                    .map(|(k, u)| (k.as_str(), u.bound_addr))
                    .collect::<Vec<_>>(),
            )
            .field("recording", &self.recording)
            .finish_non_exhaustive()
    }
}

impl ClusterHandle {
    pub(crate) fn new(
        upstreams: BTreeMap<String, RunningUpstream>,
        shutdown_tx: watch::Sender<bool>,
        recording: RecordingConfig,
        recorder: Recorder,
        command_sender: CommandSender,
        command_task: JoinHandle<()>,
        tcp_control: Option<RunningControlPlane>,
    ) -> Self {
        Self {
            upstreams,
            shutdown_tx,
            recording,
            recorder,
            command_sender,
            command_task: Some(command_task),
            tcp_control,
        }
    }

    /// Bound address of the TCP JSON-Lines control plane, if it was enabled
    /// via [`ProxyClusterBuilder::tcp_control_plane`].
    pub fn tcp_control_addr(&self) -> Option<SocketAddr> {
        self.tcp_control.as_ref().map(|c| c.bound_addr)
    }

    /// Bound address for a named upstream, or `None` if unknown.
    pub fn addr(&self, name: &str) -> Option<SocketAddr> {
        self.upstreams.get(name).map(|u| u.bound_addr)
    }

    /// All registered upstream names, sorted lexicographically.
    pub fn upstream_names(&self) -> Vec<&str> {
        self.upstreams.keys().map(String::as_str).collect()
    }

    /// View the recording configuration this cluster was started with.
    pub fn recording_config(&self) -> &RecordingConfig {
        &self.recording
    }

    /// Shared recorder — cheap to clone. Holds the in-memory ring and
    /// optionally appends to the configured NDJSON file. See
    /// `SPECIFICATION.md` §9.
    pub fn recorder(&self) -> &Recorder {
        &self.recorder
    }

    /// In-process command channel — see `SPECIFICATION.md` §12.1. Cheap to
    /// clone if multiple call sites need to issue commands.
    pub fn command_sender(&self) -> &CommandSender {
        &self.command_sender
    }

    /// Per-upstream snapshot for `/ready` endpoints. An upstream is `ready`
    /// while its accept-loop task is still alive. `exchange_count` is the
    /// number of recorded exchanges currently in the in-memory buffer
    /// scoped to this upstream.
    pub async fn upstream_statuses(&self) -> Vec<UpstreamStatus> {
        let mut out = Vec::with_capacity(self.upstreams.len());
        for (name, up) in &self.upstreams {
            let exchange_count = self
                .recorder
                .count_matching(|e| e.upstream.as_deref() == Some(name.as_str()))
                .await;
            out.push(UpstreamStatus {
                name: name.clone(),
                bound_addr: up.bound_addr,
                ready: !up.task.is_finished(),
                exchange_count,
            });
        }
        out
    }

    /// Whether the cluster as a whole is ready: at least one upstream
    /// configured AND every upstream's accept loop still running.
    pub fn is_ready(&self) -> bool {
        if self.upstreams.is_empty() {
            return false;
        }
        self.upstreams.values().all(|u| !u.task.is_finished())
    }

    /// Broadcast a shutdown signal to every listener and await its accept
    /// loop. Returns `Ok(())` once every accept loop task has joined.
    ///
    /// In-flight connections receive the same signal and are dropped on the
    /// next yield point. There is no graceful drain — slice 16 (lifecycle)
    /// will refine this if drain semantics are needed.
    pub async fn shutdown(self) -> Result<()> {
        self.shutdown_with_timeout(Duration::from_secs(5)).await
    }

    /// Like [`shutdown`](Self::shutdown), but with an explicit per-task join
    /// timeout. Tasks that exceed the timeout are aborted.
    pub async fn shutdown_with_timeout(mut self, timeout: Duration) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        let mut errors = Vec::new();
        for (name, up) in self.upstreams {
            match tokio::time::timeout(timeout, up.task).await {
                Ok(Ok(())) => {}
                Ok(Err(join_err)) => {
                    errors.push(format!("upstream {name} task panic: {join_err}"));
                }
                Err(_) => {
                    errors.push(format!(
                        "upstream {name} accept loop did not exit within {timeout:?}"
                    ));
                }
            }
        }
        if let Some(task) = self.command_task.take() {
            match tokio::time::timeout(timeout, task).await {
                Ok(Ok(())) => {}
                Ok(Err(join_err)) => {
                    errors.push(format!("command processor panic: {join_err}"));
                }
                Err(_) => {
                    errors.push(format!("command processor did not exit within {timeout:?}"));
                }
            }
        }
        if let Some(rc) = self.tcp_control.take() {
            match tokio::time::timeout(timeout, rc.task).await {
                Ok(Ok(())) => {}
                Ok(Err(join_err)) => {
                    errors.push(format!("TCP control plane panic: {join_err}"));
                }
                Err(_) => {
                    errors.push(format!("TCP control plane did not exit within {timeout:?}"));
                }
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(ProxyError::Shutdown(errors.join("; ")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command;
    use crate::upstream::UpstreamRegistry;

    async fn empty() -> ClusterHandle {
        let (tx, rx) = watch::channel(false);
        let recorder = Recorder::new(RecordingConfig::default()).await.unwrap();
        let registry = std::sync::Arc::new(UpstreamRegistry::default());
        let (sender, task) = command::spawn_processor(registry, recorder.clone(), rx);
        ClusterHandle::new(
            BTreeMap::new(),
            tx,
            RecordingConfig::default(),
            recorder,
            sender,
            task,
            None,
        )
    }

    #[tokio::test]
    async fn empty_handle_reports_no_upstreams() {
        let h = empty().await;
        assert!(h.upstream_names().is_empty());
        assert!(h.addr("nope").is_none());
    }

    #[tokio::test]
    async fn shutdown_empty_cluster_succeeds_immediately() {
        let h = empty().await;
        h.shutdown().await.unwrap();
    }
}
