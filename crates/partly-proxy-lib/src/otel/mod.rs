//! OpenTelemetry helpers — context extraction, span construction, and
//! response/request header injection.
//!
//! Call sites in `listener.rs` and `forwarder.rs` use the functions
//! re-exported here without `cfg` gates of their own; the per-version
//! impl modules (`v0_27`, future `v0_28`, …) provide the active
//! implementation, and a stub module under `#[cfg(not(feature =
//! "_otel_any"))]` provides no-ops so the surface compiles whether or
//! not any OTEL minor is enabled.
//!
//! The crate does not initialise a tracer provider, exporter,
//! propagator, or `tracing-subscriber`. The host binary owns all of
//! that. These helpers consult `opentelemetry::global` for whatever
//! propagator the host has installed.
//!
//! # Adding a new OTEL minor
//!
//! 1. Add `opentelemetry_0_X` / `opentelemetry-http_0_X` / etc. blocks
//!    to the workspace `Cargo.toml`.
//! 2. Add an `otel_0_X = ["_otel_any", ...]` feature in
//!    `partly-proxy-lib/Cargo.toml`.
//! 3. Copy `v0_27.rs` to `v0_X.rs` and update import paths.
//! 4. Add `#[cfg(feature = "otel_0_X")] #[path = "v0_X.rs"] mod inner;`
//!    here, and extend the `compile_error!` guard in `lib.rs`.
//! 5. Add a `tests/otel_v0_X.rs` patterned on the existing one.

use std::net::SocketAddr;

use http::{Method, Uri, Version};

/// Inputs needed to build the server span for one inbound request.
///
/// Lives in `mod.rs` so the version-specific impl modules and the
/// no-op stub share the same shape and the call site in `listener.rs`
/// constructs it once.
#[allow(dead_code)]
pub(crate) struct ServerSpanInputs<'a> {
    pub method: &'a Method,
    pub uri: &'a Uri,
    pub version: Version,
    pub peer: SocketAddr,
    pub bind_addr: SocketAddr,
    pub scheme: &'static str,
    pub user_agent: Option<&'a str>,
    pub upstream_name: &'a str,
}

#[cfg(feature = "otel_0_27")]
#[path = "v0_27.rs"]
mod inner;

#[cfg(not(feature = "_otel_any"))]
#[allow(dead_code)]
mod inner {
    //! No-op stub. Compiled when no `otel_0_*` feature is on.
    //!
    //! Most fns aren't called from the no-OTEL request path (the call
    //! sites are themselves inside `#[cfg(feature = "_otel_any")]`
    //! blocks). They exist so the API surface in `mod.rs` is stable and
    //! the future stubs/version impls stay symmetric.

    use http::{HeaderMap, Method, StatusCode, Uri};
    use tracing::Span;

    use super::ServerSpanInputs;

    /// Opaque parent context. Empty when the feature is off.
    #[derive(Debug, Default)]
    pub(crate) struct ParentContext;

    pub(crate) fn extract_parent_context(_headers: &HeaderMap) -> ParentContext {
        ParentContext
    }

    pub(crate) fn make_server_span(_inputs: &ServerSpanInputs<'_>) -> Span {
        Span::none()
    }

    pub(crate) fn apply_parent(_span: &Span, _parent: ParentContext) {}

    pub(crate) fn inject_into_response_headers(_span: &Span, _headers: &mut HeaderMap) {}

    pub(crate) fn record_response_status(_span: &Span, _status: StatusCode) {}

    pub(crate) fn make_client_span(_method: &Method, _uri: &Uri, _upstream_name: &str) -> Span {
        Span::none()
    }

    pub(crate) fn inject_into_request_headers(_span: &Span, _headers: &mut HeaderMap) {}
}

#[allow(unused_imports)]
pub(crate) use inner::{
    ParentContext, apply_parent, extract_parent_context, inject_into_request_headers,
    inject_into_response_headers, make_client_span, make_server_span, record_response_status,
};
