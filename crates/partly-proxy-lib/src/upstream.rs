//! Per-upstream runtime state, plus the shared registry the command
//! processor reads to dispatch commands.
//!
//! Lives in its own module (rather than `listener.rs`) so the stub store,
//! pause flag, recorder, middleware list and forwarder are reachable from
//! both the listener path and the command-processor path without a
//! circular dependency.

use std::{collections::BTreeMap, sync::Arc};

use tokio::sync::watch;

use crate::{
    config::Mode, forwarder::Forwarder, middleware::SharedMiddleware, recorder::Recorder,
    replay::ReplaySource, stub::StubStore,
};

/// OTEL fields cached on the runtime so the request path can build server
/// spans without re-reading `ProxyConfig`. Only present when an
/// `otel_0_*` feature is on.
#[cfg(feature = "_otel_any")]
#[derive(Clone)]
pub(crate) struct OtelRuntime {
    pub bind_addr: std::net::SocketAddr,
    pub scheme: &'static str,
    pub extract: bool,
    pub filter: Option<crate::config::OtelRequestFilter>,
}

#[cfg(feature = "_otel_any")]
impl Default for OtelRuntime {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:0".parse().expect("static addr parses"),
            scheme: "http",
            extract: true,
            filter: None,
        }
    }
}

/// Per-upstream runtime state shared with every accepted connection and with
/// the command processor.
pub(crate) struct UpstreamRuntime {
    pub name: String,
    pub forwarder: Forwarder,
    pub recorder: Recorder,
    /// Effective middleware chain — `global ++ per_upstream`, computed at
    /// cluster `run()` time.
    pub middleware: Vec<SharedMiddleware>,
    /// Per-upstream stub list.
    pub stubs: StubStore,
    /// Pause flag — `true` while the upstream is paused. Use the `watch`
    /// sender to flip it; receivers awaiting `pause.changed()` will wake.
    pub pause: watch::Sender<bool>,
    /// Optional replay source consulted between stub scan and forward.
    pub replay: Option<ReplaySource>,
    /// What happens on a replay miss — see [`Mode`].
    pub mode: Mode,
    /// OTEL-only fields. Populated via [`UpstreamRuntime::with_otel`].
    #[cfg(feature = "_otel_any")]
    pub otel: OtelRuntime,
}

impl UpstreamRuntime {
    /// Construct from the listener-bound state. Used by `listener::spawn_listener`.
    pub(crate) fn new(
        name: String,
        forwarder: Forwarder,
        recorder: Recorder,
        middleware: Vec<SharedMiddleware>,
        replay: Option<ReplaySource>,
        mode: Mode,
    ) -> Self {
        let (pause, _rx) = watch::channel(false);
        Self {
            name,
            forwarder,
            recorder,
            middleware,
            stubs: StubStore::default(),
            pause,
            replay,
            mode,
            #[cfg(feature = "_otel_any")]
            otel: OtelRuntime::default(),
        }
    }

    /// Attach the listener-specific OTEL configuration. Called once per
    /// upstream during `spawn_listener` after the bound address is known.
    #[cfg(feature = "_otel_any")]
    pub(crate) fn with_otel(mut self, otel: OtelRuntime) -> Self {
        self.otel = otel;
        self
    }

    /// Borrow a fresh `pause` receiver — every accept-loop task uses one to
    /// await resume signals on lifecycle stage 3.
    pub(crate) fn pause_receiver(&self) -> watch::Receiver<bool> {
        self.pause.subscribe()
    }

    /// Cheap test constructor that fills the runtime with placeholder
    /// values. Used only by unit tests in the `command` module.
    #[cfg(test)]
    pub(crate) fn test_only(name: &str) -> Self {
        use crate::config::{RecordingConfig, UpstreamTarget};
        let recorder = Recorder::new(RecordingConfig::disabled());
        let forwarder =
            Forwarder::new(UpstreamTarget::new("http://127.0.0.1:1")).expect("forwarder builds");
        Self::new(
            name.to_owned(),
            forwarder,
            recorder,
            Vec::new(),
            None,
            Mode::Record,
        )
    }
}

/// Read-only registry of upstreams keyed by name. Held behind an `Arc` so
/// the command processor and the cluster handle both share the same map.
#[derive(Default)]
pub(crate) struct UpstreamRegistry {
    upstreams: BTreeMap<String, Arc<UpstreamRuntime>>,
}

impl UpstreamRegistry {
    pub(crate) fn insert(&mut self, runtime: Arc<UpstreamRuntime>) {
        self.upstreams.insert(runtime.name.clone(), runtime);
    }

    pub(crate) fn get(&self, name: &str) -> Option<Arc<UpstreamRuntime>> {
        self.upstreams.get(name).cloned()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Arc<UpstreamRuntime>> {
        self.upstreams.values()
    }

    pub(crate) fn names(&self) -> Vec<String> {
        self.upstreams.keys().cloned().collect()
    }
}

/// Shared, immutable-after-build reference to the registry.
pub(crate) type SharedUpstreamRegistry = Arc<UpstreamRegistry>;
