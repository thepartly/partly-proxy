//! Cluster handle — returned by [`ProxyClusterBuilder::run`](crate::ProxyClusterBuilder::run).
//!
//! Owns the shutdown signal and the join handles for every listener. Dropping
//! the handle without calling `shutdown` leaves listeners running until the
//! process exits.

use std::{collections::BTreeMap, net::SocketAddr, time::Duration};

use partly_proxy_types::{ProxyError, Result};
use tokio::{sync::watch, task::JoinHandle};

use crate::{
    command::CommandSender, config::RecordingConfig, control_plane::RunningControlPlane,
    recorder::Recorder,
};

/// Per-upstream metadata tracked by the handle.
pub(crate) struct RunningUpstream {
    pub bound_addr: SocketAddr,
    pub task: JoinHandle<()>,
}

/// Handle to a running cluster — see `SPECIFICATION.md` §4.
///
/// Not `Clone`: `shutdown` consumes the handle. To use the cluster from
/// multiple call sites, share addresses or wrap the handle in `Arc` before
/// the final shutdown call.
pub struct ClusterHandle {
    upstreams: BTreeMap<String, RunningUpstream>,
    shutdown_tx: watch::Sender<Option<Duration>>,
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
        shutdown_tx: watch::Sender<Option<Duration>>,
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

    /// Shared recorder — cheap to clone. Holds the cluster-wide in-memory
    /// ring and routes each exchange to its upstream's durable storage
    /// backend (if one was attached). See `SPECIFICATION.md` §9.
    pub fn recorder(&self) -> &Recorder {
        &self.recorder
    }

    /// In-process command channel — see `SPECIFICATION.md` §12.1. Cheap to
    /// clone if multiple call sites need to issue commands.
    pub fn command_sender(&self) -> &CommandSender {
        &self.command_sender
    }

    /// Stop accepting new connections, drain in-flight HTTP exchanges, and
    /// hard-abort anything still running after the drain budget elapses.
    /// Returns `Ok(())` once every accept loop has joined.
    ///
    /// Equivalent to [`shutdown_with_timeout`](Self::shutdown_with_timeout)
    /// with a 5-second budget.
    pub async fn shutdown(self) -> Result<()> {
        self.shutdown_with_timeout(Duration::from_secs(5)).await
    }

    /// Graceful shutdown with an explicit drain budget. See
    /// `SPECIFICATION.md` §16.
    ///
    /// Phases:
    /// 1. Stop accepting — the per-listener accept loop and the command /
    ///    TCP control accept loops exit immediately on the shutdown signal.
    /// 2. Drain — each listener asks every in-flight connection to finish
    ///    via `auto::Connection::graceful_shutdown` (HTTP/1: `Connection:
    ///    close`; HTTP/2: GOAWAY) and waits for the connection futures to
    ///    resolve.
    /// 3. Hard abort — connections that have not drained within `timeout`
    ///    have their futures dropped.
    ///
    /// Returns within `timeout + 1s` (a small outer slack catches the case
    /// where the listener task itself is wedged; on the outer timeout the
    /// task is aborted).
    ///
    /// Exchanges that are still mid-request when the hard-abort fires are
    /// **not** recorded — `record(...)` is the last step of the request
    /// lifecycle and only runs for exchanges that completed gracefully.
    pub async fn shutdown_with_timeout(mut self, timeout: Duration) -> Result<()> {
        let _ = self.shutdown_tx.send(Some(timeout));
        let mut errors = Vec::new();
        // Drain listener tasks first so any in-flight requests reach the
        // recorder before we fence the storage. Order: shutdown signal →
        // listener join (which itself drains then optionally aborts) →
        // recorder flush → command/control task join.
        let outer = timeout + Duration::from_secs(1);
        for (name, up) in self.upstreams {
            // Hold an abort handle so a wedged listener task (it should
            // bound itself by `timeout`, but bugs happen) gets terminated
            // instead of leaking past `outer`.
            let abort_handle = up.task.abort_handle();
            match tokio::time::timeout(outer, up.task).await {
                Ok(Ok(())) => {}
                Ok(Err(join_err)) => {
                    errors.push(format!("upstream {name} task panic: {join_err}"));
                }
                Err(_) => {
                    abort_handle.abort();
                    errors.push(format!(
                        "upstream {name} accept loop did not exit within {outer:?}"
                    ));
                }
            }
        }
        // Fence the storage backend before joining the command task. For
        // line-buffered backends (JSONL) this is an extra fsync; for
        // batched backends (object store) it's when the part files +
        // manifest are written. Errors are recorded but don't abort the
        // rest of the shutdown.
        if let Err(e) = self.recorder.flush().await {
            errors.push(format!("recorder flush failed: {e}"));
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
    use crate::{command, upstream::UpstreamRegistry};

    fn empty() -> ClusterHandle {
        let (tx, rx) = watch::channel::<Option<Duration>>(None);
        let recorder = Recorder::new(RecordingConfig::default());
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
        let h = empty();
        assert!(h.upstream_names().is_empty());
        assert!(h.addr("nope").is_none());
    }

    #[tokio::test]
    async fn shutdown_empty_cluster_succeeds_immediately() {
        let h = empty();
        h.shutdown().await.unwrap();
    }
}
