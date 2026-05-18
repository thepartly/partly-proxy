//! OpenTelemetry contract tests against the 0.27 stack.
//!
//! Builds: only with `--features otel_0_27`. The test installs the
//! `TraceContextPropagator`, a `TracerProvider` with an
//! `InMemorySpanExporter`, and a `tracing-opentelemetry` layer that
//! turns the proxy's `tracing::Span`s into recorded OTEL spans.
//!
//! Globals are set once per process (the OTEL `global::*` registries
//! are non-reentrant), so tests share a single exporter and serialise
//! through a `Mutex`. Each test resets the exporter before running.

#![cfg(feature = "otel_0_27")]

use std::{
    net::SocketAddr,
    sync::{Mutex, MutexGuard, OnceLock},
    time::Duration,
};

use http::Method;
use opentelemetry_0_27::{
    global,
    trace::{SpanKind, Status, TraceContextExt, TracerProvider as _},
};
use opentelemetry_sdk_0_27::{
    propagation::TraceContextPropagator,
    testing::trace::InMemorySpanExporter,
    trace::TracerProvider,
};
use partly_proxy_echo as echo;
use partly_proxy_lib::{ProxyClusterBuilder, ProxyConfig, UpstreamTarget};
use serde_json::Value;
use tokio::task::JoinHandle;
use tracing_subscriber::layer::SubscriberExt;

const TRACE_ID: &str = "0af7651916cd43dd8448eb211c80319c";
const PARENT_SPAN_ID: &str = "b7ad6b7169203331";

struct Harness {
    exporter: InMemorySpanExporter,
    serial: Mutex<()>,
}

static HARNESS: OnceLock<Harness> = OnceLock::new();

fn init() -> &'static Harness {
    HARNESS.get_or_init(|| {
        global::set_text_map_propagator(TraceContextPropagator::new());

        let exporter = InMemorySpanExporter::default();
        let provider = TracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("partly-proxy-lib-test");
        let _ = global::set_tracer_provider(provider);

        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry_0_28::layer().with_tracer(tracer));
        // Ignore the error — another test binary or earlier installation
        // may have already set the global default. The propagator and
        // provider above are what we actually rely on.
        let _ = tracing::subscriber::set_global_default(subscriber);

        Harness {
            exporter,
            serial: Mutex::new(()),
        }
    })
}

/// Acquire the serial lock and reset the in-memory exporter so each
/// scenario sees only its own spans.
fn scenario() -> (&'static Harness, MutexGuard<'static, ()>) {
    let h = init();
    // PoisonError from a prior failed test shouldn't stop the next one
    // from running — recover the inner guard.
    let guard = h.serial.lock().unwrap_or_else(|p| p.into_inner());
    h.exporter.reset();
    (h, guard)
}

async fn spawn_echo() -> (SocketAddr, JoinHandle<()>) {
    let (addr, listener) = echo::bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let task = tokio::spawn(async move {
        let _ = echo::serve(listener).await;
    });
    (addr, task)
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}

fn base_cfg(echo_addr: SocketAddr) -> ProxyConfig {
    ProxyConfig::http(
        "127.0.0.1:0".parse().unwrap(),
        UpstreamTarget::new(format!("http://{echo_addr}"))
            .with_connect_timeout(Duration::from_secs(2))
            .with_request_timeout(Duration::from_secs(5)),
    )
}

fn server_spans(spans: &[opentelemetry_sdk_0_27::export::trace::SpanData]) -> Vec<&opentelemetry_sdk_0_27::export::trace::SpanData> {
    spans
        .iter()
        .filter(|s| s.span_kind == SpanKind::Server)
        .collect()
}

fn attr<'a>(
    span: &'a opentelemetry_sdk_0_27::export::trace::SpanData,
    key: &str,
) -> Option<&'a opentelemetry_0_27::Value> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| &kv.value)
}

#[tokio::test]
async fn inbound_traceparent_is_extracted() {
    let (h, _g) = scenario();
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .add_upstream("api", base_cfg(echo_addr))
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    let traceparent = format!("00-{TRACE_ID}-{PARENT_SPAN_ID}-01");
    let resp = http_client()
        .get(format!("http://{proxy}/orders/42"))
        .header("traceparent", &traceparent)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    // Drain the body so the server-side `handle_request` future completes
    // and the OTEL span gets exported before we read the exporter.
    let _ = resp.bytes().await.unwrap();
    cluster.shutdown().await.unwrap();

    let spans = h.exporter.get_finished_spans().unwrap();
    let server = server_spans(&spans);
    let server = server.first().expect("server span recorded");

    assert_eq!(server.span_context.trace_id().to_string(), TRACE_ID);
    assert_eq!(server.parent_span_id.to_string(), PARENT_SPAN_ID);
    assert_eq!(
        attr(server, "http.route").and_then(opt_str),
        Some("api".to_string())
    );
    assert_eq!(
        attr(server, "http.request.method").and_then(opt_str),
        Some("GET".to_string())
    );
    assert_eq!(
        attr(server, "url.path").and_then(opt_str),
        Some("/orders/42".to_string())
    );
}

#[tokio::test]
async fn response_carries_traceparent_matching_trace_id() {
    let (_h, _g) = scenario();
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .add_upstream("api", base_cfg(echo_addr))
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    let traceparent = format!("00-{TRACE_ID}-{PARENT_SPAN_ID}-01");
    let resp = http_client()
        .get(format!("http://{proxy}/"))
        .header("traceparent", &traceparent)
        .send()
        .await
        .unwrap();

    let resp_traceparent = resp
        .headers()
        .get("traceparent")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .expect("response carries traceparent");
    let _ = resp.bytes().await.unwrap();
    cluster.shutdown().await.unwrap();

    let segments: Vec<&str> = resp_traceparent.split('-').collect();
    assert_eq!(segments.len(), 4, "well-formed traceparent");
    assert_eq!(segments[0], "00");
    assert_eq!(segments[1], TRACE_ID);
    // span_id is the proxy's server span, not the caller's.
    assert_ne!(segments[2], PARENT_SPAN_ID);
}

#[tokio::test]
async fn outbound_injection_off_by_default() {
    let (_h, _g) = scenario();
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .add_upstream("api", base_cfg(echo_addr))
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    // No traceparent on the inbound side: if the proxy minted one, the
    // upstream would still see it. We expect none, since propagation is
    // off by default.
    let body: Value = http_client()
        .get(format!("http://{proxy}/"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    cluster.shutdown().await.unwrap();

    let echoed = echoed_header(&body, "traceparent");
    assert!(
        echoed.is_none(),
        "expected no traceparent forwarded upstream, got {echoed:?}"
    );
}

#[tokio::test]
async fn outbound_injection_when_enabled() {
    let (_h, _g) = scenario();
    let (echo_addr, _t) = spawn_echo().await;
    let cfg = base_cfg(echo_addr).with_otel_propagation_to_upstream();
    let cluster = ProxyClusterBuilder::new()
        .add_upstream("api", cfg)
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    let traceparent = format!("00-{TRACE_ID}-{PARENT_SPAN_ID}-01");
    let body: Value = http_client()
        .get(format!("http://{proxy}/"))
        .header("traceparent", &traceparent)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    cluster.shutdown().await.unwrap();

    let echoed = echoed_header(&body, "traceparent").expect("upstream sees traceparent");
    let segments: Vec<&str> = echoed.split('-').collect();
    assert_eq!(segments.len(), 4);
    assert_eq!(segments[1], TRACE_ID, "trace_id preserved across the proxy");
}

#[tokio::test]
async fn extraction_can_be_disabled() {
    let (h, _g) = scenario();
    let (echo_addr, _t) = spawn_echo().await;
    let cfg = base_cfg(echo_addr).without_otel_extraction();
    let cluster = ProxyClusterBuilder::new()
        .add_upstream("api", cfg)
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    let traceparent = format!("00-{TRACE_ID}-{PARENT_SPAN_ID}-01");
    let resp = http_client()
        .get(format!("http://{proxy}/"))
        .header("traceparent", &traceparent)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let has_traceparent = resp.headers().get("traceparent").is_some();
    let _ = resp.bytes().await.unwrap();
    cluster.shutdown().await.unwrap();

    assert!(
        !has_traceparent,
        "no traceparent injected when extraction is disabled"
    );

    let spans = h.exporter.get_finished_spans().unwrap();
    assert!(
        server_spans(&spans).is_empty(),
        "no server span recorded when extraction is disabled"
    );
}

#[tokio::test]
async fn filter_skips_selected_requests() {
    let (h, _g) = scenario();
    let (echo_addr, _t) = spawn_echo().await;
    let cfg = base_cfg(echo_addr)
        .with_otel_filter(|_m: &Method, uri: &http::Uri| uri.path() != "/healthz");
    let cluster = ProxyClusterBuilder::new()
        .add_upstream("api", cfg)
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    let traceparent = format!("00-{TRACE_ID}-{PARENT_SPAN_ID}-01");
    let resp = http_client()
        .get(format!("http://{proxy}/healthz"))
        .header("traceparent", &traceparent)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let has_traceparent = resp.headers().get("traceparent").is_some();
    let _ = resp.bytes().await.unwrap();
    cluster.shutdown().await.unwrap();

    assert!(!has_traceparent);
    let spans = h.exporter.get_finished_spans().unwrap();
    assert!(server_spans(&spans).is_empty());
}

#[tokio::test]
async fn server_error_maps_to_error_status() {
    let (h, _g) = scenario();
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .add_upstream("api", base_cfg(echo_addr))
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    let resp = http_client()
        .get(format!("http://{proxy}/_status/500"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 500);
    let _ = resp.bytes().await.unwrap();
    cluster.shutdown().await.unwrap();

    let spans = h.exporter.get_finished_spans().unwrap();
    let server = server_spans(&spans);
    let server = server
        .first()
        .expect("server span recorded for 5xx response");
    assert!(
        matches!(server.status, Status::Error { .. }),
        "expected Status::Error, got {:?}",
        server.status
    );
    assert_eq!(
        attr(server, "http.response.status_code").and_then(opt_i64),
        Some(500)
    );
}

fn echoed_header(body: &Value, name: &str) -> Option<String> {
    body.get("headers")?
        .as_array()?
        .iter()
        .find_map(|kv| {
            let arr = kv.as_array()?;
            let key = arr.first()?.as_str()?;
            if key.eq_ignore_ascii_case(name) {
                arr.get(1)?.as_str().map(str::to_owned)
            } else {
                None
            }
        })
}

fn opt_str(v: &opentelemetry_0_27::Value) -> Option<String> {
    match v {
        opentelemetry_0_27::Value::String(s) => Some(s.to_string()),
        _ => None,
    }
}

fn opt_i64(v: &opentelemetry_0_27::Value) -> Option<i64> {
    match v {
        opentelemetry_0_27::Value::I64(i) => Some(*i),
        _ => None,
    }
}

// Silence unused-warning from the unused TraceContextExt import; it's
// needed only for trait disambiguation in some configurations.
#[allow(dead_code)]
fn _trace_context_ext_in_scope(cx: &opentelemetry_0_27::Context) -> bool {
    cx.span().span_context().is_valid()
}
