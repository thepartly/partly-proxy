//! Inbound TCP accept loop and per-request service handler.
//!
//! Lifecycle stages from `SPECIFICATION.md` §5 wired up so far:
//!   - 3: pause gate (slice 5)
//!   - 4: body collection
//!   - 5: middleware chain (slice 4)
//!   - 6: stub scan terminal (slice 5)
//!   - 8: forward to upstream
//!   - 9: record (with snapshot-boundary redaction)
//!
//! Stages 1 (TLS) and 7 (replay terminal) land in later slices.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, Method, StatusCode, Uri, Version};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::builder::UpstreamSpec;
use crate::context::RequestContext;
use crate::error::{ProxyError, Result};
use crate::forwarder::Forwarder;
use crate::middleware::{self, SharedMiddleware, Terminal, TerminalFuture};
use crate::proxy_io::{ProxyRequest, ProxyResponse};
use crate::recorded::{ExchangeOutcome, RecordedExchange, RecordedRequest, RecordedResponse};
use crate::recorder::Recorder;
use crate::upstream::UpstreamRuntime;

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
    shutdown: watch::Receiver<bool>,
) -> Result<RunningListener> {
    let listener = TcpListener::bind(spec.config.bind_addr)
        .await
        .map_err(ProxyError::Bind)?;
    let bound_addr = listener.local_addr().map_err(ProxyError::Bind)?;

    let forwarder = Forwarder::new(spec.config.upstream)?;
    let mut middleware = global_middleware;
    middleware.extend(spec.middleware);

    let runtime = Arc::new(UpstreamRuntime::new(
        spec.name,
        forwarder,
        recorder,
        middleware,
        spec.replay,
    ));

    let task = tokio::spawn(accept_loop(listener, runtime.clone(), shutdown));
    Ok(RunningListener {
        bound_addr,
        runtime,
        task,
    })
}

async fn accept_loop(
    listener: TcpListener,
    runtime: Arc<UpstreamRuntime>,
    mut shutdown: watch::Receiver<bool>,
) {
    tracing::debug!(name = %runtime.name, "accept loop started");
    loop {
        tokio::select! {
            biased;
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() {
                    tracing::debug!(name = %runtime.name, "accept loop shutting down");
                    return;
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        let runtime = runtime.clone();
                        let mut conn_shutdown = shutdown.clone();
                        tokio::spawn(async move {
                            let io = TokioIo::new(stream);
                            let svc = service_fn(move |req| {
                                let r = runtime.clone();
                                async move { handle_request(req, r).await }
                            });
                            let builder = auto::Builder::new(TokioExecutor::new());
                            let conn = builder.serve_connection(io, svc);
                            tokio::select! {
                                res = conn => {
                                    if let Err(e) = res {
                                        tracing::debug!(%peer, "connection ended with error: {e}");
                                    }
                                }
                                _ = conn_shutdown.changed() => {
                                    tracing::debug!(%peer, "connection dropped on shutdown");
                                }
                            }
                        });
                    }
                    Err(e) => {
                        if is_fatal_accept(&e) {
                            tracing::error!("fatal accept error, exiting accept loop: {e}");
                            return;
                        }
                        tracing::warn!("transient accept error: {e}");
                    }
                }
            }
        }
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
) -> std::result::Result<Response<Full<Bytes>>, Infallible> {
    use http_body_util::BodyExt;

    // Lifecycle stage 3: pause gate.
    pause_gate(&runtime).await;

    let started = Instant::now();
    let (parts, body) = req.into_parts();

    // Lifecycle stage 4: body collection.
    let body_bytes = match body.collect().await {
        Ok(c) => c.to_bytes(),
        Err(e) => {
            let err = ProxyError::UpstreamRequest(format!("inbound body read failed: {e}"));
            record_error_exchange(
                &runtime,
                &parts.method,
                &parts.uri,
                &parts.headers,
                Bytes::new(),
                &err,
                started.elapsed(),
            )
            .await;
            return Ok(bad_gateway(&err));
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
        runtime: runtime.as_ref(),
    };
    let chain_input = original_request.clone();
    let outcome =
        middleware::run_chain(&runtime.middleware, &terminal, chain_input, &mut ctx).await;

    match outcome {
        Ok(resp) => {
            record_success_exchange(&runtime, &original_request, &resp, started.elapsed()).await;
            Ok(into_hyper(resp))
        }
        Err(err) => {
            record_error_exchange(
                &runtime,
                &original_request.method,
                &original_request.uri,
                &original_request.headers,
                original_request.body.clone(),
                &err,
                started.elapsed(),
            )
            .await;
            Ok(bad_gateway(&err))
        }
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

/// Concrete terminal that drives the stub scan and outbound forward. Replay
/// (slice 6) will plug in between the stub scan and the forward.
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
            self.runtime.forwarder.forward(req).await
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
        ProxyError::UpstreamConnect(_) => "upstream-connect",
        ProxyError::UpstreamRequest(_) => "upstream-request",
        ProxyError::Middleware(_) => "middleware",
        ProxyError::Command(_) => "command",
        ProxyError::Recording(_) => "recording",
        ProxyError::Tls(_) => "tls",
        ProxyError::UnknownUpstream(_) => "unknown-upstream",
        ProxyError::Shutdown(_) => "shutdown",
        ProxyError::Other(_) => "other",
    }
}
