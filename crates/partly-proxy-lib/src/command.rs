//! In-process command channel — see `SPECIFICATION.md` §12.1 and §14.
//!
//! Implements `Stub`, `ClearStubs`, `Pause`, `Resume`, `QueryTraffic`,
//! `ClearRecordings`, `AssertSeen`, and `AssertCount`. The two assertion
//! variants block until the predicate transitions or the supplied
//! `timeout` elapses (wait-for semantics, §14.1) — overshoot also
//! terminates `AssertCount` early.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use partly_proxy_types::{ProxyError, RecordedExchange, Result};
use tokio::sync::{mpsc, oneshot};

use crate::{
    assertions::TrafficFilter,
    recorder::Recorder,
    stub::{RequestMatcher, StubEntry, StubbedResponse},
    upstream::SharedUpstreamRegistry,
};

/// One control-plane command.
#[derive(Debug)]
#[non_exhaustive]
pub enum Command {
    /// Register a new stub against one (or, when there is exactly one
    /// upstream, the only) upstream.
    Stub {
        upstream: Option<String>,
        matcher: RequestMatcher,
        response: StubbedResponse,
        times: Option<u32>,
    },
    /// Clear stubs for one upstream, or for all when `upstream` is `None`.
    ClearStubs { upstream: Option<String> },
    /// Pause an upstream (or all). Requests already mid-flight finish; new
    /// inbound requests block on lifecycle stage 3 until a `Resume` arrives.
    Pause { upstream: Option<String> },
    /// Resume a paused upstream (or all).
    Resume { upstream: Option<String> },
    /// Snapshot the recorder, optionally filtered.
    QueryTraffic { filter: TrafficFilter },
    /// Drop every exchange held in memory.
    ClearRecordings,
    /// Block until the filter matches at least one exchange or `timeout`
    /// elapses. See `SPECIFICATION.md` §14.1 for the wait-for semantics.
    AssertSeen {
        filter: TrafficFilter,
        timeout: Duration,
    },
    /// Block until the filter matches exactly `expected` exchanges, the
    /// match count overshoots, or `timeout` elapses. See `SPECIFICATION.md`
    /// §14.1 for the wait-for semantics; overshoot terminates fast.
    AssertCount {
        filter: TrafficFilter,
        expected: usize,
        timeout: Duration,
    },
}

/// One response to a command.
#[derive(Debug)]
#[non_exhaustive]
pub enum CommandResponse {
    /// Command succeeded with no payload.
    Ok,
    /// Command failed; the caller-visible reason is in `message`.
    Error { message: String },
    /// Recorded exchanges returned by `QueryTraffic`.
    Exchanges(Vec<RecordedExchange>),
    /// Verdict for `AssertSeen` / `AssertCount`.
    AssertionResult { passed: bool, message: String },
}

impl CommandResponse {
    /// Convenience: error response from any `Display` message.
    pub fn error(msg: impl std::fmt::Display) -> Self {
        Self::Error {
            message: msg.to_string(),
        }
    }
}

/// Envelope routed across the mpsc channel — the command plus a oneshot to
/// return the reply on.
pub(crate) struct CommandEnvelope {
    pub cmd: Command,
    pub reply: oneshot::Sender<CommandResponse>,
}

impl std::fmt::Debug for CommandEnvelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandEnvelope")
            .field("cmd", &self.cmd)
            .finish_non_exhaustive()
    }
}

/// Cheap-to-clone sender for the in-process command channel.
#[derive(Clone)]
pub struct CommandSender {
    tx: mpsc::Sender<CommandEnvelope>,
}

impl std::fmt::Debug for CommandSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandSender")
            .field("capacity_remaining", &self.tx.capacity())
            .finish()
    }
}

impl CommandSender {
    pub(crate) fn new(tx: mpsc::Sender<CommandEnvelope>) -> Self {
        Self { tx }
    }

    /// Send a command and await its response.
    pub async fn send(&self, cmd: Command) -> Result<CommandResponse> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(CommandEnvelope {
                cmd,
                reply: reply_tx,
            })
            .await
            .map_err(|_| ProxyError::Command("command channel closed".into()))?;
        reply_rx
            .await
            .map_err(|_| ProxyError::Command("command reply dropped".into()))
    }
}

/// Spawn the command-processor task. Returns the [`CommandSender`].
///
/// The task runs until the channel is closed (all senders dropped) or the
/// `shutdown` watch flips true.
pub(crate) fn spawn_processor(
    upstreams: SharedUpstreamRegistry,
    recorder: Recorder,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> (CommandSender, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel::<CommandEnvelope>(64);
    let task = tokio::spawn(process_loop(rx, upstreams, recorder, shutdown));
    (CommandSender::new(tx), task)
}

async fn process_loop(
    mut rx: mpsc::Receiver<CommandEnvelope>,
    upstreams: SharedUpstreamRegistry,
    recorder: Recorder,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            biased;
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() {
                    tracing::debug!("command processor shutting down");
                    return;
                }
            }
            envelope = rx.recv() => {
                let Some(envelope) = envelope else {
                    tracing::debug!("command channel closed; processor exiting");
                    return;
                };
                let response = dispatch(&envelope.cmd, &upstreams, &recorder).await;
                let _ = envelope.reply.send(response);
            }
        }
    }
}

async fn dispatch(
    cmd: &Command,
    upstreams: &SharedUpstreamRegistry,
    recorder: &Recorder,
) -> CommandResponse {
    match cmd {
        Command::Stub {
            upstream,
            matcher,
            response,
            times,
        } => match resolve_upstream(upstreams, upstream.as_deref()) {
            Ok(up) => {
                up.stubs
                    .add(StubEntry {
                        matcher: matcher.clone(),
                        response: response.clone(),
                        times: *times,
                    })
                    .await;
                CommandResponse::Ok
            }
            Err(e) => CommandResponse::error(e),
        },
        Command::ClearStubs { upstream } => {
            match upstream {
                Some(name) => match upstreams.get(name) {
                    Some(up) => up.stubs.clear().await,
                    None => {
                        return CommandResponse::error(ProxyError::UnknownUpstream(name.clone()));
                    }
                },
                None => {
                    for up in upstreams.iter() {
                        up.stubs.clear().await;
                    }
                }
            }
            CommandResponse::Ok
        }
        Command::Pause { upstream } => set_pause(upstreams, upstream.as_deref(), true),
        Command::Resume { upstream } => set_pause(upstreams, upstream.as_deref(), false),
        Command::QueryTraffic { filter } => {
            let all = recorder.exchanges().await;
            let filtered = all.into_iter().filter(|e| filter.matches(e)).collect();
            CommandResponse::Exchanges(filtered)
        }
        Command::ClearRecordings => {
            recorder.clear().await;
            CommandResponse::Ok
        }
        Command::AssertSeen { filter, timeout } => {
            wait_for_assertion(recorder, filter, *timeout, AssertionShape::Seen).await
        }
        Command::AssertCount {
            filter,
            expected,
            timeout,
        } => wait_for_assertion(recorder, filter, *timeout, AssertionShape::Count(*expected)).await,
    }
}

/// Shape of the predicate driving the wait-for loop.
#[derive(Debug, Clone, Copy)]
enum AssertionShape {
    Seen,
    Count(usize),
}

#[derive(Debug)]
enum AssertionVerdict {
    Pending,
    Passed(String),
    Failed(String),
}

fn evaluate(shape: AssertionShape, count: usize) -> AssertionVerdict {
    match shape {
        AssertionShape::Seen => {
            if count >= 1 {
                AssertionVerdict::Passed(format!("matched {count} exchanges"))
            } else {
                AssertionVerdict::Pending
            }
        }
        AssertionShape::Count(expected) => match count.cmp(&expected) {
            std::cmp::Ordering::Equal => {
                AssertionVerdict::Passed(format!("matched exactly {expected}"))
            }
            std::cmp::Ordering::Greater => {
                // Overshoot is terminal — further traffic cannot bring the
                // count back down, so fail fast instead of waiting out the
                // clock (§14.1).
                AssertionVerdict::Failed(format!(
                    "matched {count} exchanges; expected exactly {expected} (overshoot)"
                ))
            }
            std::cmp::Ordering::Less => AssertionVerdict::Pending,
        },
    }
}

/// Block until `shape` is satisfied or `timeout` elapses. A `timeout` of
/// zero collapses to a single immediate evaluation (§14.1).
async fn wait_for_assertion(
    recorder: &Recorder,
    filter: &TrafficFilter,
    timeout: Duration,
    shape: AssertionShape,
) -> CommandResponse {
    let deadline = Instant::now().checked_add(timeout);

    loop {
        // Register the waker BEFORE checking the predicate so a record()
        // that lands between snapshot and await still wakes us.
        let notify = recorder.on_insert();
        let waiter = notify.notified();
        tokio::pin!(waiter);
        waiter.as_mut().enable();

        let count = recorder.count_matching(|e| filter.matches(e)).await;
        match evaluate(shape, count) {
            AssertionVerdict::Passed(message) => {
                return CommandResponse::AssertionResult {
                    passed: true,
                    message,
                };
            }
            AssertionVerdict::Failed(message) => {
                return CommandResponse::AssertionResult {
                    passed: false,
                    message,
                };
            }
            AssertionVerdict::Pending => {}
        }

        // Timeout of zero — one evaluation, then fail.
        if timeout.is_zero() {
            return CommandResponse::AssertionResult {
                passed: false,
                message: format!("matched {count}; timeout=0 collapsed to immediate eval"),
            };
        }

        let remaining = match deadline {
            Some(d) => d.saturating_duration_since(Instant::now()),
            None => Duration::from_secs(0),
        };
        if remaining.is_zero() {
            return CommandResponse::AssertionResult {
                passed: false,
                message: format!("timeout after {timeout:?}; matched {count} exchanges at expiry"),
            };
        }

        // Wait for either a fresh insertion or the deadline.
        if tokio::time::timeout(remaining, waiter).await.is_err() {
            let final_count = recorder.count_matching(|e| filter.matches(e)).await;
            return CommandResponse::AssertionResult {
                passed: false,
                message: format!(
                    "timeout after {timeout:?}; matched {final_count} exchanges at expiry"
                ),
            };
        }
        // Re-check on the next iteration.
    }
}

fn set_pause(
    upstreams: &SharedUpstreamRegistry,
    name: Option<&str>,
    paused: bool,
) -> CommandResponse {
    if let Some(n) = name {
        if let Some(up) = upstreams.get(n) {
            up.pause.send_replace(paused);
            CommandResponse::Ok
        } else {
            CommandResponse::error(ProxyError::UnknownUpstream(n.to_owned()))
        }
    } else {
        for up in upstreams.iter() {
            up.pause.send_replace(paused);
        }
        CommandResponse::Ok
    }
}

/// Resolve `target` to a runtime — `None` is the implicit "only upstream"
/// shorthand (per spec §12.1: "With multiple upstreams, scoped commands
/// (notably `Stub`) require an explicit name.").
fn resolve_upstream(
    upstreams: &SharedUpstreamRegistry,
    target: Option<&str>,
) -> Result<Arc<crate::upstream::UpstreamRuntime>> {
    if let Some(name) = target {
        return upstreams
            .get(name)
            .ok_or_else(|| ProxyError::UnknownUpstream(name.to_owned()));
    }
    let names = upstreams.names();
    if names.len() == 1 {
        Ok(upstreams.get(&names[0]).expect("single upstream resolves"))
    } else {
        Err(ProxyError::Command(format!(
            "upstream not specified and cluster has {} upstreams",
            names.len()
        )))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use http::{HeaderMap, Method, StatusCode};
    use partly_proxy_types::{ExchangeOutcome, RecordedRequest, RecordedResponse};

    use super::*;
    use crate::{
        config::RecordingConfig,
        upstream::{UpstreamRegistry, UpstreamRuntime},
    };

    fn fixture() -> (
        SharedUpstreamRegistry,
        Recorder,
        tokio::sync::watch::Sender<bool>,
    ) {
        let runtime = Arc::new(UpstreamRuntime::test_only("api"));
        let mut registry = UpstreamRegistry::default();
        registry.insert(runtime);
        let shared = Arc::new(registry);
        let recorder = Recorder::new(RecordingConfig::in_memory(10));
        let (tx, _rx) = tokio::sync::watch::channel(false);
        (shared, recorder, tx)
    }

    #[tokio::test]
    async fn stub_command_inserts_into_store() {
        let (registry, recorder, _shutdown_tx) = fixture();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (sender, task) = spawn_processor(registry.clone(), recorder, shutdown_rx);

        let resp = sender
            .send(Command::Stub {
                upstream: Some("api".into()),
                matcher: RequestMatcher::new().path("/x"),
                response: StubbedResponse::new(StatusCode::OK),
                times: Some(1),
            })
            .await
            .unwrap();
        assert!(matches!(resp, CommandResponse::Ok));

        let up = registry.get("api").unwrap();
        assert_eq!(up.stubs.len().await, 1);

        let _ = shutdown_tx.send(true);
        let _ = task.await;
    }

    #[tokio::test]
    async fn stub_against_unknown_upstream_yields_error_response() {
        let (registry, recorder, _) = fixture();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (sender, task) = spawn_processor(registry, recorder, shutdown_rx);

        let resp = sender
            .send(Command::Stub {
                upstream: Some("does-not-exist".into()),
                matcher: RequestMatcher::new(),
                response: StubbedResponse::new(StatusCode::OK),
                times: None,
            })
            .await
            .unwrap();
        assert!(
            matches!(&resp, CommandResponse::Error { message } if message.contains("unknown upstream")),
            "got {resp:?}"
        );

        let _ = shutdown_tx.send(true);
        let _ = task.await;
    }

    #[tokio::test]
    async fn stub_with_none_upstream_resolves_when_single() {
        let (registry, recorder, _) = fixture();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (sender, task) = spawn_processor(registry.clone(), recorder, shutdown_rx);

        let resp = sender
            .send(Command::Stub {
                upstream: None,
                matcher: RequestMatcher::new().path("/x"),
                response: StubbedResponse::new(StatusCode::OK),
                times: None,
            })
            .await
            .unwrap();
        assert!(matches!(resp, CommandResponse::Ok));
        assert_eq!(registry.get("api").unwrap().stubs.len().await, 1);

        let _ = shutdown_tx.send(true);
        let _ = task.await;
    }

    #[tokio::test]
    async fn pause_and_resume_flip_the_watch() {
        let (registry, recorder, _) = fixture();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (sender, task) = spawn_processor(registry.clone(), recorder, shutdown_rx);

        assert!(!*registry.get("api").unwrap().pause.borrow());
        let _ = sender
            .send(Command::Pause {
                upstream: Some("api".into()),
            })
            .await
            .unwrap();
        assert!(*registry.get("api").unwrap().pause.borrow());
        let _ = sender
            .send(Command::Resume {
                upstream: Some("api".into()),
            })
            .await
            .unwrap();
        assert!(!*registry.get("api").unwrap().pause.borrow());

        let _ = shutdown_tx.send(true);
        let _ = task.await;
    }

    #[tokio::test]
    async fn clear_recordings_empties_the_recorder() {
        let (registry, recorder, _) = fixture();
        // Seed a couple of fake exchanges.
        let make = || {
            let req = RecordedRequest::from_parts(
                &Method::GET,
                &"/".parse().unwrap(),
                &HeaderMap::new(),
                Bytes::new(),
            );
            let resp = RecordedResponse {
                status: 200,
                headers: vec![],
                body: Bytes::new(),
            };
            RecordedExchange::new(
                Some("api".into()),
                req,
                ExchangeOutcome::Response(resp),
                Duration::from_millis(1),
            )
        };
        recorder.record(make()).await.unwrap();
        recorder.record(make()).await.unwrap();
        assert_eq!(recorder.len().await, 2);

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (sender, task) = spawn_processor(registry, recorder.clone(), shutdown_rx);

        let resp = sender.send(Command::ClearRecordings).await.unwrap();
        assert!(matches!(resp, CommandResponse::Ok));
        assert_eq!(recorder.len().await, 0);

        let _ = shutdown_tx.send(true);
        let _ = task.await;
    }
}
