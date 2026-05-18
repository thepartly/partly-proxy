//! OpenTelemetry 0.27 implementation. Mirrors the API in `mod.rs`.
//!
//! Imports use the version-suffixed crate renames (`opentelemetry_0_27`,
//! `opentelemetry_http_0_27`, …) declared in the workspace `Cargo.toml`
//! so multiple OTEL minors can coexist.

use std::net::SocketAddr;

use http::{HeaderMap, Method, StatusCode, Uri, Version};
use opentelemetry_0_27::{
    Context, global,
    trace::{Status, TraceContextExt},
};
use opentelemetry_http_0_27::{HeaderExtractor, HeaderInjector};
use opentelemetry_semantic_conventions_0_27::attribute::{
    CLIENT_ADDRESS, CLIENT_PORT, HTTP_REQUEST_METHOD, HTTP_RESPONSE_STATUS_CODE, HTTP_ROUTE,
    NETWORK_PROTOCOL_VERSION, SERVER_ADDRESS, SERVER_PORT, URL_FULL, URL_PATH, URL_QUERY,
    URL_SCHEME, USER_AGENT_ORIGINAL,
};
use tracing::Span;
use tracing_opentelemetry_0_28::OpenTelemetrySpanExt;

const PARTLY_PROXY_UPSTREAM: &str = "partly.proxy.upstream";

/// Parent context extracted from inbound headers. Round-tripped through
/// `apply_parent` rather than handed to callers as an `opentelemetry`
/// type to keep the surface in `mod.rs` version-agnostic.
pub(crate) struct ParentContext(Context);

impl Default for ParentContext {
    fn default() -> Self {
        Self(Context::new())
    }
}

pub(crate) fn extract_parent_context(headers: &HeaderMap) -> ParentContext {
    let cx = global::get_text_map_propagator(|prop| prop.extract(&HeaderExtractor(headers)));
    ParentContext(cx)
}

pub(crate) fn make_server_span(
    method: &Method,
    uri: &Uri,
    version: Version,
    peer: SocketAddr,
    bind_addr: SocketAddr,
    scheme: &'static str,
    user_agent: Option<&str>,
    upstream_name: &str,
) -> Span {
    let display_name = format!("{} {}", method.as_str(), upstream_name);
    let span = tracing::info_span!(
        "http.server.request",
        otel.name = %display_name,
        otel.kind = "server",
    );

    span.set_attribute(HTTP_REQUEST_METHOD, method.as_str().to_owned());
    span.set_attribute(HTTP_ROUTE, upstream_name.to_owned());
    span.set_attribute(URL_PATH, uri.path().to_owned());
    if let Some(q) = uri.query() {
        span.set_attribute(URL_QUERY, q.to_owned());
    }
    span.set_attribute(URL_SCHEME, scheme);
    span.set_attribute(SERVER_ADDRESS, bind_addr.ip().to_string());
    span.set_attribute(SERVER_PORT, i64::from(bind_addr.port()));
    span.set_attribute(CLIENT_ADDRESS, peer.ip().to_string());
    span.set_attribute(CLIENT_PORT, i64::from(peer.port()));
    if let Some(ua) = user_agent {
        span.set_attribute(USER_AGENT_ORIGINAL, ua.to_owned());
    }
    if let Some(v) = http_version_str(version) {
        span.set_attribute(NETWORK_PROTOCOL_VERSION, v);
    }
    span.set_attribute(PARTLY_PROXY_UPSTREAM, upstream_name.to_owned());

    span
}

pub(crate) fn apply_parent(span: &Span, parent: ParentContext) {
    if parent.0.span().span_context().is_valid() {
        span.set_parent(parent.0);
    }
}

pub(crate) fn inject_into_response_headers(span: &Span, headers: &mut HeaderMap) {
    let cx = span.context();
    global::get_text_map_propagator(|prop| {
        prop.inject_context(&cx, &mut HeaderInjector(headers));
    });
}

pub(crate) fn record_response_status(span: &Span, status: StatusCode) {
    span.set_attribute(HTTP_RESPONSE_STATUS_CODE, i64::from(status.as_u16()));
    if status.is_server_error() {
        span.set_status(Status::error(
            status.canonical_reason().unwrap_or("error").to_owned(),
        ));
    }
}

pub(crate) fn make_client_span(method: &Method, uri: &Uri, upstream_name: &str) -> Span {
    let display_name = method.as_str().to_owned();
    let span = tracing::info_span!(
        "http.client.request",
        otel.name = %display_name,
        otel.kind = "client",
    );
    span.set_attribute(HTTP_REQUEST_METHOD, method.as_str().to_owned());
    span.set_attribute(URL_FULL, uri.to_string());
    if let Some(authority) = uri.authority() {
        span.set_attribute(SERVER_ADDRESS, authority.host().to_owned());
        if let Some(port) = authority.port_u16() {
            span.set_attribute(SERVER_PORT, i64::from(port));
        }
    }
    span.set_attribute(PARTLY_PROXY_UPSTREAM, upstream_name.to_owned());
    span
}

pub(crate) fn inject_into_request_headers(span: &Span, headers: &mut HeaderMap) {
    let cx = span.context();
    global::get_text_map_propagator(|prop| {
        prop.inject_context(&cx, &mut HeaderInjector(headers));
    });
}

fn http_version_str(v: Version) -> Option<&'static str> {
    match v {
        Version::HTTP_09 => Some("0.9"),
        Version::HTTP_10 => Some("1.0"),
        Version::HTTP_11 => Some("1.1"),
        Version::HTTP_2 => Some("2"),
        Version::HTTP_3 => Some("3"),
        _ => None,
    }
}
