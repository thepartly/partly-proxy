//! Per-upstream runtime state, plus the shared registry the command
//! processor reads to dispatch commands.
//!
//! Lives in its own module (rather than `listener.rs`) so the stub store,
//! pause flag, recorder, middleware list and forwarder are reachable from
//! both the listener path and the command-processor path without a
//! circular dependency.

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::watch;

use crate::forwarder::Forwarder;
use crate::middleware::SharedMiddleware;
use crate::recorder::Recorder;
use crate::replay::ReplaySource;
use crate::stub::StubStore;

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
}

impl UpstreamRuntime {
    /// Construct from the listener-bound state. Used by `listener::spawn_listener`.
    pub(crate) fn new(
        name: String,
        forwarder: Forwarder,
        recorder: Recorder,
        middleware: Vec<SharedMiddleware>,
        replay: Option<ReplaySource>,
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
        }
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
        Self::new(name.to_owned(), forwarder, recorder, Vec::new(), None)
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
