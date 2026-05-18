//! Inbound TCP accept loop and per-request service handler.
//!
//! Implements the request lifecycle from `SPECIFICATION.md` §5: TLS
//! handshake (when `inbound_tls` is configured) → HTTP/1.1 or HTTP/2
//! negotiation → pause gate → body collection → middleware chain →
//! terminal (stub scan, then replay lookup, then forward) → record
//! (with snapshot-boundary redaction).

use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode, Uri, Version, header::CONTENT_TYPE};
use http_body_util::Full;
use hyper::{Request, Response, body::Incoming, service::service_fn};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use partly_proxy_types::{
    ExchangeOutcome, ProxyError, RecordedExchange, RecordedRequest, RecordedResponse, Result,
};
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};
use tokio_rustls::TlsAcceptor;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    builder::UpstreamSpec,
    context::RequestContext,
    forwarder::Forwarder,
    middleware::{self, SharedMiddleware, Terminal, TerminalFuture},
    proxy_io::{ProxyRequest, ProxyResponse},
    recorder::Recorder,
    tls::build_tls_acceptor,
    upstream::UpstreamRuntime,
};

/// One running listener — the bound address plus the task handle of the
/// accept loop.
pub(crate) struct RunningListener {
    pub bound_addr: SocketAddr,
    pub runtime: Arc<UpstreamRuntime>,
    pub task: JoinHandle<()>,
}

/// Bind the listener for one upstream spec and spawn its accept loop.
pub(crate) async fn spawn_listener(
    spec: UpstreamSpec,
    global_middleware: Vec<SharedMiddleware>,
    recorder: Recorder,
    shutdown: watch::Receiver<Option<Duration>>,
) -> Result<RunningListener> {
    let tls_acceptor = if let Some(cfg) = &spec.config.inbound_tls {
        Some(build_tls_acceptor(cfg)?)
    } else {
        None
    };

    let listener = TcpListener::bind(spec.config.bind_addr)
        .await
        .map_err(ProxyError::Bind)?;
    let bound_addr = listener.local_addr().map_err(ProxyError::Bind)?;

    // OTEL inputs are read before `spec.config.upstream` is moved into the
    // Forwarder. `bind_addr`/`scheme` are also captured here so the
    // listener can populate the OTEL runtime once the socket is bound.
    #[cfg(feature = "_otel_any")]
    let otel_runtime = crate::upstream::OtelRuntime {
        bind_addr: bound_addr,
        scheme: if spec.config.inbound_tls.is_some() {
            "https"
        } else {
            "http"
        },
        extract: spec.config.otel_extract,
        filter: spec.config.otel_filter.clone(),
    };
    #[cfg(feature = "_otel_any")]
    let propagate_upstream = spec.config.otel_propagate_upstream;

    let forwarder = Forwarder::new(spec.config.upstream)?;
    #[cfg(feature = "_otel_any")]
    let forwarder = forwarder.with_otel_propagation(propagate_upstream);
    let mut middleware = global_middleware;
    middleware.extend(spec.middleware);

    let runtime = UpstreamRuntime::new(spec.name, forwarder, recorder, middleware, spec.replay);
    #[cfg(feature = "_otel_any")]
    let runtime = runtime.with_otel(otel_runtime);
    let runtime = Arc::new(runtime);

    let task = tokio::spawn(accept_loop(
        listener,
        runtime.clone(),
        tls_acceptor,
        shutdown,
    ));
    Ok(RunningListener {
        bound_addr,
        runtime,
        task,
    })
}

async fn accept_loop(
    listener: TcpListener,
    runtime: Arc<UpstreamRuntime>,
    tls_acceptor: Option<TlsAcceptor>,
    mut shutdown: watch::Receiver<Option<Duration>>,
) {
    tracing::debug!(name = %runtime.name, tls = tls_acceptor.is_some(), "accept loop started");

    // Per-connection lifetime tracker + two-stage cancellation tokens. See
    // `SPECIFICATION.md` §16: shutdown stops accepting, asks in-flight
    // connections to drain via `auto::Connection::graceful_shutdown`, then
    // hard-aborts whatever is still running after the deadline.
    let tracker = TaskTracker::new();
    let drain_token = CancellationToken::new();
    let abort_token = CancellationToken::new();

    loop {
        tokio::select! {
            biased;
            res = shutdown.changed() => {
                if res.is_err() || shutdown.borrow().is_some() {
                    tracing::debug!(name = %runtime.name, "accept loop shutting down");
                    break;
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        let runtime = runtime.clone();
                        let tls = tls_acceptor.clone();
                        let drain = drain_token.clone();
                        let abort = abort_token.clone();
                        tracker.spawn(async move {
                            serve_one(stream, peer, runtime, tls, drain, abort).await;
                        });
                    }
                    Err(e) => {
                        if is_fatal_accept(&e) {
                            tracing::error!("fatal accept error, exiting accept loop: {e}");
                            break;
                        }
                        tracing::warn!("transient accept error: {e}");
                    }
                }
            }
        }
    }

    // Drop the listener so the kernel stops queueing new connections.
    drop(listener);

    // Unblock any pause-gated requests so they can finish their exchange
    // within the drain window. With pause cleared, the only way a request
    // sits indefinitely is the upstream forward itself, which is cut by the
    // abort step below.
    let _ = runtime.pause.send_replace(false);

    let deadline = shutdown.borrow().unwrap_or(Duration::from_secs(5));

    drain_token.cancel();
    tracker.close();

    if tokio::time::timeout(deadline, tracker.wait())
        .await
        .is_err()
    {
        tracing::debug!(
            name = %runtime.name,
            "drain deadline {deadline:?} exceeded; hard-aborting in-flight connections"
        );
        abort_token.cancel();
        tracker.wait().await;
    }

    tracing::debug!(name = %runtime.name, "accept loop exited");
}

async fn serve_one(
    stream: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    runtime: Arc<UpstreamRuntime>,
    tls_acceptor: Option<TlsAcceptor>,
    drain_token: CancellationToken,
    abort_token: CancellationToken,
) {
    let svc = service_fn({
        let runtime = runtime.clone();
        move |req| {
            let r = runtime.clone();
            async move { handle_request(req, r, peer).await }
        }
    });
    let builder = auto::Builder::new(TokioExecutor::new());

    // `serve_with_io!` runs the connection future against the two
    // cancellation tokens: drain → ask hyper to send Connection: close
    // (H1) / GOAWAY (H2) and finish in-flight exchanges; abort → drop the
    // connection future entirely.
    macro_rules! serve_with_io {
        ($io:expr) => {{
            let conn = builder.serve_connection($io, svc);
            let mut conn = std::pin::pin!(conn);
            tokio::select! {
                res = conn.as_mut() => {
                    if let Err(e) = res {
                        tracing::debug!(%peer, "connection ended with error: {e}");
                    }
                }
                () = drain_token.cancelled() => {
                    conn.as_mut().graceful_shutdown();
                    tokio::select! {
                        res = conn.as_mut() => {
                            if let Err(e) = res {
                                tracing::debug!(%peer, "graceful drain ended with error: {e}");
                            }
                        }
                        () = abort_token.cancelled() => {
                            tracing::debug!(%peer, "connection aborted on drain timeout");
                        }
                    }
                }
            }
        }};
    }

    if let Some(acceptor) = tls_acceptor {
        // TLS handshake races drain_token so a half-finished handshake
        // doesn't keep the tracker alive past the drain window.
        let tls_stream = tokio::select! {
            res = acceptor.accept(stream) => match res {
                Ok(s) => s,
                Err(e) => {
                    tracing::debug!(%peer, "TLS handshake failed: {e}");
                    return;
                }
            },
            () = drain_token.cancelled() => {
                tracing::debug!(%peer, "TLS handshake aborted on shutdown");
                return;
            }
        };
        let io = TokioIo::new(tls_stream);
        serve_with_io!(io);
    } else {
        let io = TokioIo::new(stream);
        serve_with_io!(io);
    }
}

fn is_fatal_accept(e: &std::io::Error) -> bool {
    !matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::Interrupted
    )
}

async fn handle_request(
    req: Request<Incoming>,
    runtime: Arc<UpstreamRuntime>,
    peer: SocketAddr,
) -> std::result::Result<Response<Full<Bytes>>, Infallible> {
    use tracing::Instrument;

    // Lifecycle stage 3: pause gate.
    pause_gate(&runtime).await;

    let started = Instant::now();
    let (parts, body) = req.into_parts();

    // Build the OTEL server span before the body is read so it spans the
    // full inbound processing. Returns `Span::none()` when no `otel_0_*`
    // feature is on, when `otel_extract` is false for this listener, or
    // when the per-request filter rejected the request.
    let span = build_server_span(&parts, peer, &runtime);

    let runtime_for_block = runtime.clone();
    let mut response = async move {
        use http_body_util::BodyExt;

        // Lifecycle stage 4: body collection.
        let body_bytes = match body.collect().await {
            Ok(c) => c.to_bytes(),
            Err(e) => {
                let err = ProxyError::upstream_request_with("inbound body read failed", e);
                record_error_exchange(
                    &runtime_for_block,
                    &parts.method,
                    &parts.uri,
                    &parts.headers,
                    Bytes::new(),
                    &err,
                    started.elapsed(),
                )
                .await;
                return bad_gateway(&err);
            }
        };

        let original_request = ProxyRequest {
            method: parts.method,
            uri: parts.uri,
            headers: parts.headers,
            body: body_bytes,
            version: parts.version,
        };

        let mut ctx = RequestContext::new();

        // Lifecycle stages 5–8: middleware chain wrapping the stub→forward terminal.
        let terminal = LiveTerminal {
            runtime: runtime_for_block.as_ref(),
        };
        let chain_input = original_request.clone();
        let outcome = middleware::run_chain(
            &runtime_for_block.middleware,
            &terminal,
            chain_input,
            &mut ctx,
        )
        .await;

        match outcome {
            Ok(resp) => {
                record_success_exchange(
                    &runtime_for_block,
                    &original_request,
                    &resp,
                    started.elapsed(),
                )
                .await;
                into_hyper(resp)
            }
            Err(err) => {
                record_error_exchange(
                    &runtime_for_block,
                    &original_request.method,
                    &original_request.uri,
                    &original_request.headers,
                    original_request.body.clone(),
                    &err,
                    started.elapsed(),
                )
                .await;
                bad_gateway(&err)
            }
        }
    }
    .instrument(span.clone())
    .await;

    // Lifecycle stage 9: emit response. With OTEL on, also inject the trace
    // context into the response headers and record the status on the span.
    // Both helpers are no-ops when no `otel_0_*` feature is enabled or when
    // `span` is `Span::none()`.
    crate::otel::inject_into_response_headers(&span, response.headers_mut());
    crate::otel::record_response_status(&span, response.status());
    let _ = runtime; // ensure we hold the Arc until response is built
    Ok(response)
}

/// Build the OTEL server span for an inbound request, or `Span::none()`
/// when no OTEL feature is enabled, extraction is disabled for this
/// listener, or the per-request filter rejected the request.
#[allow(unused_variables)]
fn build_server_span(
    parts: &http::request::Parts,
    peer: SocketAddr,
    runtime: &UpstreamRuntime,
) -> tracing::Span {
    #[cfg(feature = "_otel_any")]
    {
        if !runtime.otel.extract {
            return tracing::Span::none();
        }
        if let Some(filter) = &runtime.otel.filter {
            if !filter(&parts.method, &parts.uri) {
                return tracing::Span::none();
            }
        }
        let parent = crate::otel::extract_parent_context(&parts.headers);
        let user_agent = parts
            .headers
            .get(http::header::USER_AGENT)
            .and_then(|v| v.to_str().ok());
        let span = crate::otel::make_server_span(&crate::otel::ServerSpanInputs {
            method: &parts.method,
            uri: &parts.uri,
            version: parts.version,
            peer,
            bind_addr: runtime.otel.bind_addr,
            scheme: runtime.otel.scheme,
            user_agent,
            upstream_name: &runtime.name,
        });
        crate::otel::apply_parent(&span, parent);
        span
    }
    #[cfg(not(feature = "_otel_any"))]
    {
        tracing::Span::none()
    }
}

/// Lifecycle stage 3: block while `runtime.pause` is true.
async fn pause_gate(runtime: &UpstreamRuntime) {
    let mut rx = runtime.pause_receiver();
    while *rx.borrow() {
        // `changed()` returns Err when the sender is dropped — in that
        // case we treat it as "no longer paused" and proceed.
        if rx.changed().await.is_err() {
            return;
        }
    }
}

/// Concrete terminal that drives the stub scan, replay lookup, and
/// outbound forward in spec order.
struct LiveTerminal<'a> {
    runtime: &'a UpstreamRuntime,
}

impl Terminal for LiveTerminal<'_> {
    fn invoke<'b>(&'b self, req: ProxyRequest, _ctx: &'b mut RequestContext) -> TerminalFuture<'b> {
        Box::pin(async move {
            // Lifecycle stage 6: stub scan. The first matching stub wins.
            if let Some((response, delay)) = self.runtime.stubs.take_match(&req).await {
                if let Some(d) = delay {
                    tokio::time::sleep(d).await;
                }
                return Ok(response.into_proxy());
            }
            // Lifecycle stage 7: replay lookup. The lookup applies
            // `redact_request_for_snapshot` to a working copy before hashing.
            if let Some(source) = &self.runtime.replay {
                if let Some(resp) = source.lookup(&req, &self.runtime.middleware) {
                    return Ok(resp);
                }
            }
            // Lifecycle stage 8: forward.
            self.runtime
                .forwarder
                .forward(req, &self.runtime.name)
                .await
        })
    }
}

fn into_hyper(resp: ProxyResponse) -> Response<Full<Bytes>> {
    let mut builder = Response::builder()
        .status(resp.status)
        .version(resp.version);
    if let Some(headers) = builder.headers_mut() {
        *headers = resp.headers;
        // Hyper recomputes Content-Length from the Full<Bytes> body. Any
        // value supplied by middleware or carried over from the upstream
        // would conflict with the actual body length when middleware has
        // rewritten the body, so strip framing headers here.
        headers.remove(http::header::CONTENT_LENGTH);
        headers.remove(http::header::TRANSFER_ENCODING);
    }
    builder
        .body(Full::new(resp.body))
        .expect("response build is infallible with valid headers")
}

async fn record_success_exchange(
    runtime: &UpstreamRuntime,
    original_request: &ProxyRequest,
    final_response: &ProxyResponse,
    duration: std::time::Duration,
) {
    if !runtime.recorder.is_enabled() {
        return;
    }
    let (recorded_req, recorded_resp) = build_recorded(runtime, original_request, final_response);
    persist_exchange(
        runtime,
        recorded_req,
        ExchangeOutcome::Response(recorded_resp),
        duration,
    )
    .await;
}

async fn record_error_exchange(
    runtime: &UpstreamRuntime,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Bytes,
    err: &ProxyError,
    duration: std::time::Duration,
) {
    if !runtime.recorder.is_enabled() {
        return;
    }
    let original = ProxyRequest {
        method: method.clone(),
        uri: uri.clone(),
        headers: headers.clone(),
        body,
        version: Version::HTTP_11,
    };
    let mut redacted = original.clone();
    middleware::redact_request(&runtime.middleware, &mut redacted);
    let recorded_req = RecordedRequest::from_parts(
        &redacted.method,
        &redacted.uri,
        &redacted.headers,
        redacted.body,
    );
    persist_exchange(
        runtime,
        recorded_req,
        ExchangeOutcome::Error {
            message: err.to_string(),
        },
        duration,
    )
    .await;
}

fn build_recorded(
    runtime: &UpstreamRuntime,
    original_request: &ProxyRequest,
    final_response: &ProxyResponse,
) -> (RecordedRequest, RecordedResponse) {
    let mut redacted_req = original_request.clone();
    middleware::redact_request(&runtime.middleware, &mut redacted_req);
    let recorded_req = RecordedRequest::from_parts(
        &redacted_req.method,
        &redacted_req.uri,
        &redacted_req.headers,
        redacted_req.body,
    );

    let mut redacted_resp = final_response.clone();
    middleware::redact_response(&runtime.middleware, &mut redacted_resp);
    let recorded_resp = RecordedResponse::from_parts(
        redacted_resp.status,
        &redacted_resp.headers,
        redacted_resp.body,
    );

    (recorded_req, recorded_resp)
}

async fn persist_exchange(
    runtime: &UpstreamRuntime,
    request: RecordedRequest,
    outcome: ExchangeOutcome,
    duration: std::time::Duration,
) {
    let exchange = RecordedExchange::new(Some(runtime.name.clone()), request, outcome, duration);
    if let Err(e) = runtime.recorder.record(exchange).await {
        tracing::warn!(name = %runtime.name, "recorder rejected exchange: {e}");
    }
}

fn bad_gateway(err: &ProxyError) -> Response<Full<Bytes>> {
    tracing::warn!("returning 502 to client: {err}");
    let body = format!("{err}");
    Response::builder()
        .status(StatusCode::BAD_GATEWAY)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .header("x-proxy-error", error_kind(err))
        .body(Full::new(Bytes::from(body)))
        .expect("static response is valid")
}

fn error_kind(err: &ProxyError) -> &'static str {
    match err {
        ProxyError::Bind(_) => "bind",
        ProxyError::UpstreamConnect { .. } => "upstream-connect",
        ProxyError::UpstreamRequest { .. } => "upstream-request",
        ProxyError::Middleware(_) => "middleware",
        ProxyError::Command(_) => "command",
        ProxyError::Recording(_) => "recording",
        ProxyError::Tls(_) => "tls",
        ProxyError::UnknownUpstream(_) => "unknown-upstream",
        ProxyError::Shutdown(_) => "shutdown",
        // ProxyError is #[non_exhaustive] and now lives in another crate
        // (partly-proxy-types), so the match needs a fall-through arm.
        // Bucket `Other` + any future variants as "other".
        _ => "other",
    }
}
