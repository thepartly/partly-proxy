//! Configuration types — see `SPECIFICATION.md` §3.
//!
//! These are plain owned structs. They are constructed by callers and consumed
//! by [`ProxyClusterBuilder`](crate::ProxyClusterBuilder). The library does not
//! validate the contents at construction; semantic validation happens at the
//! point where each field is used (when the listener binds, when the outbound
//! client is built, etc.).

use std::{net::SocketAddr, path::PathBuf, time::Duration};

/// One listener bound to one upstream — see `SPECIFICATION.md` §3.1.
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// Address the listener binds to.
    pub bind_addr: SocketAddr,
    /// Upstream this listener forwards to.
    pub upstream: UpstreamTarget,
    /// If set, the listener terminates inbound TLS.
    pub inbound_tls: Option<InboundTlsConfig>,
}

impl ProxyConfig {
    /// Plain-HTTP listener — convenience for the most common case.
    pub fn http(bind_addr: SocketAddr, upstream: UpstreamTarget) -> Self {
        Self {
            bind_addr,
            upstream,
            inbound_tls: None,
        }
    }
}

/// Outbound target description — see `SPECIFICATION.md` §3.2.
#[derive(Debug, Clone)]
pub struct UpstreamTarget {
    /// Scheme + host (+ optional port and path prefix) of the upstream.
    pub base_url: String,
    /// If set, overrides the `Host` header sent to the upstream.
    pub host_header: Option<String>,
    /// Connect timeout — default 10 s.
    pub connect_timeout: Duration,
    /// Per-request timeout — default 30 s.
    pub request_timeout: Duration,
    /// Outbound TLS settings, if the upstream is HTTPS.
    pub tls: Option<UpstreamTlsConfig>,
}

impl UpstreamTarget {
    /// Build an `UpstreamTarget` with the default timeouts and no TLS overrides.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            ..Self::default()
        }
    }

    /// Override the `Host` header sent to the upstream.
    pub fn with_host_header(mut self, host_header: impl Into<String>) -> Self {
        self.host_header = Some(host_header.into());
        self
    }

    /// Override the connect timeout.
    pub fn with_connect_timeout(mut self, t: Duration) -> Self {
        self.connect_timeout = t;
        self
    }

    /// Override the per-request timeout.
    pub fn with_request_timeout(mut self, t: Duration) -> Self {
        self.request_timeout = t;
        self
    }

    /// Attach an outbound TLS config.
    pub fn with_tls(mut self, tls: UpstreamTlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }
}

impl Default for UpstreamTarget {
    fn default() -> Self {
        Self {
            base_url: String::new(),
            host_header: None,
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            tls: None,
        }
    }
}

/// Recording configuration — see `SPECIFICATION.md` §3.3.
///
/// Controls the recorder's in-memory ring buffer only. Persistence —
/// NDJSON file, `SQLite` database, or anything else implementing
/// [`SnapshotStorage`](crate::SnapshotStorage) — is configured
/// separately via
/// [`Recorder::with_storage`](crate::Recorder::with_storage) or
/// [`ProxyClusterBuilder::storage`](crate::ProxyClusterBuilder::storage).
#[derive(Debug, Clone)]
pub struct RecordingConfig {
    /// Whether exchanges are recorded at all.
    pub enabled: bool,
    /// Cap for the in-memory ring buffer; FIFO eviction once exceeded.
    pub max_in_memory: usize,
}

impl Default for RecordingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_in_memory: 10_000,
        }
    }
}

impl RecordingConfig {
    /// Recording disabled — exchanges are not retained anywhere.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }

    /// In-memory only with the given capacity.
    pub fn in_memory(max_in_memory: usize) -> Self {
        Self {
            enabled: true,
            max_in_memory,
        }
    }
}

/// Per-upstream mode — see `SPECIFICATION.md` §8.3.
///
/// Determines what happens when the terminal stages find no matching stub
/// and no replay hit:
///
/// - [`Mode::Record`] forwards to the upstream and records the exchange.
///   When a [`ReplaySource`](crate::ReplaySource) is also configured, replay
///   hits short-circuit before the forward (so previously-seen requests
///   don't re-hit the upstream).
/// - [`Mode::Replay`] never touches the upstream. A miss yields a `503` with
///   an empty-JSON-object body (`{}`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Mode {
    /// Forward to upstream on a replay miss and record the exchange.
    #[default]
    Record,
    /// Refuse to forward; replay misses return `503 {}`.
    Replay,
}

impl std::str::FromStr for Mode {
    type Err = partly_proxy_types::ProxyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "record" => Ok(Mode::Record),
            "replay" => Ok(Mode::Replay),
            _ => Err(partly_proxy_types::ProxyError::Command(format!(
                "invalid proxy mode {s:?}: expected \"record\" or \"replay\""
            ))),
        }
    }
}

/// Outbound TLS settings — see `SPECIFICATION.md` §3.4.
#[derive(Debug, Clone, Default)]
pub struct UpstreamTlsConfig {
    /// Skip every certificate check — use only for self-signed test upstreams.
    /// When `true`, `custom_ca_cert` is ignored.
    pub accept_invalid_certs: bool,
    /// Additional trust anchors merged with the system root store.
    pub custom_ca_cert: Option<PathBuf>,
}

/// Inbound TLS settings — see `SPECIFICATION.md` §3.5.
#[derive(Debug, Clone)]
pub struct InboundTlsConfig {
    /// PEM-encoded certificate chain.
    pub cert_path: PathBuf,
    /// PEM-encoded private key (PKCS#8, PKCS#1, or SEC1).
    pub key_path: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr() -> SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }

    #[test]
    fn upstream_target_defaults_match_spec() {
        let t = UpstreamTarget::new("http://localhost:1234");
        assert_eq!(t.base_url, "http://localhost:1234");
        assert!(t.host_header.is_none());
        assert_eq!(t.connect_timeout, Duration::from_secs(10));
        assert_eq!(t.request_timeout, Duration::from_secs(30));
        assert!(t.tls.is_none());
    }

    #[test]
    fn upstream_target_builders_are_fluent() {
        let t = UpstreamTarget::new("https://api.example.com")
            .with_host_header("internal.api")
            .with_connect_timeout(Duration::from_millis(500))
            .with_request_timeout(Duration::from_secs(5))
            .with_tls(UpstreamTlsConfig {
                accept_invalid_certs: true,
                ..UpstreamTlsConfig::default()
            });
        assert_eq!(t.host_header.as_deref(), Some("internal.api"));
        assert_eq!(t.connect_timeout, Duration::from_millis(500));
        assert_eq!(t.request_timeout, Duration::from_secs(5));
        assert!(t.tls.as_ref().unwrap().accept_invalid_certs);
    }

    #[test]
    fn recording_config_defaults_match_spec() {
        let r = RecordingConfig::default();
        assert!(r.enabled);
        assert_eq!(r.max_in_memory, 10_000);
    }

    #[test]
    fn recording_disabled_keeps_other_fields_sensible() {
        let r = RecordingConfig::disabled();
        assert!(!r.enabled);
        assert_eq!(r.max_in_memory, 10_000);
    }

    #[test]
    fn recording_in_memory_carries_cap() {
        let r = RecordingConfig::in_memory(500);
        assert!(r.enabled);
        assert_eq!(r.max_in_memory, 500);
    }

    #[test]
    fn proxy_config_http_constructor_clears_inbound_tls() {
        let cfg = ProxyConfig::http(addr(), UpstreamTarget::new("http://x"));
        assert!(cfg.inbound_tls.is_none());
    }
}
