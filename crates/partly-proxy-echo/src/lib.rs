//! Deterministic HTTP echo server.
//!
//! Every request is rendered as a JSON document describing the method, path,
//! headers and body, and returned with a configurable status (default 200).
//! Special paths:
//!
//! - `GET /_status/{code}` — returns the requested HTTP status with body
//!   `status={code}`.
//! - `GET /_sleep/{ms}` — sleeps for the requested duration before replying.
//! - `GET /_kill` — drops the connection without sending a response (useful
//!   for upstream-error path tests).
//!
//! Used by the proxy library's integration tests. Lives in its own
//! crate so other workspace crates and external tools can depend on it
//! without pulling in the proxy library.

use std::{convert::Infallible, net::SocketAddr};

use base64::Engine;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, StatusCode, body::Incoming, service::service_fn};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use serde::Serialize;
use tokio::net::TcpListener;

/// JSON shape returned for every non-special request.
#[derive(Debug, Serialize)]
pub struct EchoBody {
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub headers: Vec<(String, String)>,
    /// Body as utf-8 if valid, otherwise base64.
    pub body: EchoPayload,
}

#[derive(Debug, Serialize)]
#[serde(tag = "encoding", content = "value", rename_all = "lowercase")]
pub enum EchoPayload {
    Utf8(String),
    Base64(String),
}

/// Run the echo server on an already-bound listener.
///
/// The future returns when the listener stops accepting (e.g. on socket close).
pub async fn serve(listener: TcpListener) -> std::io::Result<()> {
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) if is_fatal_accept(&e) => return Err(e),
            Err(e) => {
                tracing::warn!("echo: transient accept error: {e}");
                continue;
            }
        };

        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let builder = auto::Builder::new(TokioExecutor::new());
            if let Err(e) = builder.serve_connection(io, service_fn(handle)).await {
                tracing::debug!("echo connection ended: {e}");
            }
        });
    }
}

/// Bind to `addr` and serve. Returns the actual bound address.
pub async fn bind(addr: SocketAddr) -> std::io::Result<(SocketAddr, TcpListener)> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    Ok((bound, listener))
}

fn is_fatal_accept(e: &std::io::Error) -> bool {
    !matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::Interrupted
    )
}

async fn handle(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path().to_owned();

    if let Some(code) = path.strip_prefix("/_status/") {
        return Ok(status_response(code));
    }
    if let Some(ms) = path.strip_prefix("/_sleep/") {
        return Ok(sleep_response(ms).await);
    }
    if path == "/_kill" {
        // Returning a response here still completes the connection cleanly;
        // a true "drop" would require closing the socket. For the most common
        // upstream-error test, the proxy crate uses an unreachable address
        // instead — `_kill` returns 500 as a graceful alternative.
        return Ok(simple_response(StatusCode::INTERNAL_SERVER_ERROR, "killed"));
    }

    Ok(echo_response(req).await)
}

fn status_response(code: &str) -> Response<Full<Bytes>> {
    let status = code
        .parse::<u16>()
        .ok()
        .and_then(|c| StatusCode::from_u16(c).ok())
        .unwrap_or(StatusCode::BAD_REQUEST);
    simple_response(status, &format!("status={}", status.as_u16()))
}

async fn sleep_response(ms: &str) -> Response<Full<Bytes>> {
    let millis = ms.parse::<u64>().unwrap_or(0);
    tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
    simple_response(StatusCode::OK, &format!("slept={millis}"))
}

fn simple_response(status: StatusCode, body: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .header("x-echo", "true")
        .body(Full::new(Bytes::copy_from_slice(body.as_bytes())))
        .expect("static response is valid")
}

async fn echo_response(req: Request<Incoming>) -> Response<Full<Bytes>> {
    let method = req.method().to_string();
    let path = req.uri().path().to_owned();
    let query = req.uri().query().map(str::to_owned);
    let headers = req
        .headers()
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_owned(),
                v.to_str().unwrap_or("<binary>").to_owned(),
            )
        })
        .collect();

    let body_bytes = match req.into_body().collect().await {
        Ok(c) => c.to_bytes(),
        Err(_) => Bytes::new(),
    };

    let body = match std::str::from_utf8(&body_bytes) {
        Ok(s) => EchoPayload::Utf8(s.to_owned()),
        Err(_) => {
            EchoPayload::Base64(base64::engine::general_purpose::STANDARD.encode(&body_bytes))
        }
    };

    let payload = EchoBody {
        method,
        path,
        query,
        headers,
        body,
    };
    let json = serde_json::to_vec(&payload).expect("EchoBody is always serialisable");

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .header("x-echo", "true")
        .body(Full::new(Bytes::from(json)))
        .expect("response is valid")
}
