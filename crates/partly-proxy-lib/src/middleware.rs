//! `ProxyMiddleware` trait, `Next<'_>` cursor, and chain composition.
//!
//! See `SPECIFICATION.md` §6. There is exactly one middleware chain per
//! request: global middleware first (in registration order), then per-upstream
//! middleware (in the order passed to `add_upstream_with_middleware`), then
//! the terminal stages (stub scan → replay lookup → outbound forward).

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;

use partly_proxy_types::Result;

use crate::context::RequestContext;
use crate::proxy_io::{ProxyRequest, ProxyResponse};

/// Object-safe middleware trait. Implementors live behind
/// `Arc<dyn ProxyMiddleware>`. The two `redact_*_for_snapshot` hooks are
/// optional (default no-op) — they fire only at the recording/replay
/// boundary, never on the live request path.
#[async_trait]
pub trait ProxyMiddleware: Send + Sync + 'static {
    /// Live request handler. Implementations decide whether to call
    /// `next.run(req, ctx).await`.
    async fn handle(
        &self,
        req: ProxyRequest,
        ctx: &mut RequestContext,
        next: Next<'_>,
    ) -> Result<ProxyResponse>;

    /// Pure rewrite applied immediately before a request crosses the
    /// snapshot boundary (recording or replay lookup). Default: no-op.
    fn redact_request_for_snapshot(&self, _req: &mut ProxyRequest) {}

    /// Pure rewrite applied immediately before a response is persisted to a
    /// snapshot. Default: no-op.
    fn redact_response_for_snapshot(&self, _resp: &mut ProxyResponse) {}
}

/// Shared, cheaply-clonable middleware reference.
pub type SharedMiddleware = Arc<dyn ProxyMiddleware>;

/// Boxed future returned by [`Terminal::invoke`].
pub type TerminalFuture<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<ProxyResponse>> + Send + 'a>>;

/// Terminal stage at the end of the middleware chain. Production code
/// supplies a `LiveTerminal` in `listener.rs` that runs the stub scan,
/// replay lookup, and outbound forward in order. Tests can supply their
/// own implementation to inspect chain composition in isolation.
pub trait Terminal: Send + Sync {
    fn invoke<'a>(&'a self, req: ProxyRequest, ctx: &'a mut RequestContext) -> TerminalFuture<'a>;
}

/// Cursor over the remaining middleware and the terminal stage.
///
/// `Next` carries a borrow of the surrounding chain plus a reference to a
/// trait-object terminal. Calling [`Next::run`] either advances one step or,
/// when the chain is exhausted, drives the terminal.
pub struct Next<'a> {
    remaining: &'a [SharedMiddleware],
    terminal: &'a dyn Terminal,
}

impl std::fmt::Debug for Next<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Next")
            .field("remaining", &self.remaining.len())
            .finish_non_exhaustive()
    }
}

impl<'a> Next<'a> {
    pub(crate) fn new(remaining: &'a [SharedMiddleware], terminal: &'a dyn Terminal) -> Self {
        Self {
            remaining,
            terminal,
        }
    }

    /// Advance one step. If there is more middleware, the next layer's
    /// `handle` runs with a `Next` covering the remaining tail. When the
    /// chain is exhausted, the terminal stages run (stub → replay → forward).
    pub async fn run(self, req: ProxyRequest, ctx: &mut RequestContext) -> Result<ProxyResponse> {
        if let Some((first, rest)) = self.remaining.split_first() {
            let inner = Next::new(rest, self.terminal);
            first.handle(req, ctx, inner).await
        } else {
            self.terminal.invoke(req, ctx).await
        }
    }
}

/// Apply every middleware's snapshot redaction in registration order.
pub(crate) fn redact_request(chain: &[SharedMiddleware], req: &mut ProxyRequest) {
    for mw in chain {
        mw.redact_request_for_snapshot(req);
    }
}

/// Apply every middleware's snapshot redaction in registration order.
pub(crate) fn redact_response(chain: &[SharedMiddleware], resp: &mut ProxyResponse) {
    for mw in chain {
        mw.redact_response_for_snapshot(resp);
    }
}

/// Drive the middleware chain for one request and return the final response.
///
/// Borrows `chain` and `terminal` for the duration of the call.
pub(crate) async fn run_chain(
    chain: &[SharedMiddleware],
    terminal: &dyn Terminal,
    req: ProxyRequest,
    ctx: &mut RequestContext,
) -> Result<ProxyResponse> {
    let next = Next::new(chain, terminal);
    next.run(req, ctx).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{HeaderMap, Method, StatusCode, Uri};
    use partly_proxy_types::ProxyError;

    /// Test terminal: returns a fixed 404 with the request body echoed.
    struct FakeTerminal;

    impl Terminal for FakeTerminal {
        fn invoke<'a>(
            &'a self,
            req: ProxyRequest,
            _ctx: &'a mut RequestContext,
        ) -> TerminalFuture<'a> {
            Box::pin(
                async move { Ok(ProxyResponse::new(StatusCode::NOT_FOUND).with_body(req.body)) },
            )
        }
    }

    /// Test-only: middleware that records its own observation order via a
    /// shared `Arc<Mutex<Vec<&'static str>>>`.
    struct Logger {
        tag: &'static str,
        log: Arc<std::sync::Mutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl ProxyMiddleware for Logger {
        async fn handle(
            &self,
            req: ProxyRequest,
            ctx: &mut RequestContext,
            next: Next<'_>,
        ) -> Result<ProxyResponse> {
            self.log.lock().unwrap().push(self.tag);
            next.run(req, ctx).await
        }
    }

    /// Test-only: middleware that short-circuits with a synthetic response.
    struct ShortCircuit {
        status: StatusCode,
        body: &'static [u8],
    }

    #[async_trait]
    impl ProxyMiddleware for ShortCircuit {
        async fn handle(
            &self,
            _req: ProxyRequest,
            _ctx: &mut RequestContext,
            _next: Next<'_>,
        ) -> Result<ProxyResponse> {
            Ok(ProxyResponse::new(self.status).with_body(Bytes::copy_from_slice(self.body)))
        }
    }

    /// Test terminal substitute: produces a fixed response without touching
    /// a real `Forwarder`. We can't construct a `Forwarder` without a base
    /// URL, so the chain-composition tests run `Next` against a custom
    /// terminal via the public `Next::new`+`run` path indirectly through
    /// `ShortCircuit` middleware.
    fn proxy_req() -> ProxyRequest {
        ProxyRequest::new(
            Method::GET,
            Uri::from_static("/"),
            HeaderMap::new(),
            Bytes::new(),
        )
    }

    #[tokio::test]
    async fn chain_runs_middleware_in_registration_order() {
        let log: Arc<std::sync::Mutex<Vec<&'static str>>> = Arc::new(std::sync::Mutex::default());
        let chain: Vec<SharedMiddleware> = vec![
            Arc::new(Logger {
                tag: "a",
                log: log.clone(),
            }),
            Arc::new(Logger {
                tag: "b",
                log: log.clone(),
            }),
            Arc::new(ShortCircuit {
                status: StatusCode::OK,
                body: b"x",
            }),
        ];

        let terminal = FakeTerminal;
        let mut ctx = RequestContext::new();
        let resp = run_chain(&chain, &terminal, proxy_req(), &mut ctx)
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(resp.body, Bytes::from_static(b"x"));

        let observed = log.lock().unwrap().clone();
        assert_eq!(observed, vec!["a", "b"]);
    }

    #[tokio::test]
    async fn short_circuit_skips_inner_middleware() {
        let log: Arc<std::sync::Mutex<Vec<&'static str>>> = Arc::new(std::sync::Mutex::default());
        let chain: Vec<SharedMiddleware> = vec![
            Arc::new(ShortCircuit {
                status: StatusCode::IM_A_TEAPOT,
                body: b"nope",
            }),
            Arc::new(Logger {
                tag: "should-not-run",
                log: log.clone(),
            }),
        ];

        let terminal = FakeTerminal;
        let mut ctx = RequestContext::new();
        let resp = run_chain(&chain, &terminal, proxy_req(), &mut ctx)
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::IM_A_TEAPOT);
        assert!(log.lock().unwrap().is_empty());
    }

    /// Test-only: middleware that rewrites the request body and returns
    /// whatever the inner stages produce, then rewrites the response body.
    struct Wrapper;

    #[async_trait]
    impl ProxyMiddleware for Wrapper {
        async fn handle(
            &self,
            mut req: ProxyRequest,
            ctx: &mut RequestContext,
            next: Next<'_>,
        ) -> Result<ProxyResponse> {
            req.body = Bytes::from_static(b"<wrapped>");
            let mut resp = next.run(req, ctx).await?;
            // Prefix the response body in place.
            let mut new = b"prefix:".to_vec();
            new.extend_from_slice(&resp.body);
            resp.body = Bytes::from(new);
            Ok(resp)
        }
    }

    /// Echo terminal: synthetic middleware that mirrors the request body.
    struct EchoTerminal;

    #[async_trait]
    impl ProxyMiddleware for EchoTerminal {
        async fn handle(
            &self,
            req: ProxyRequest,
            _ctx: &mut RequestContext,
            _next: Next<'_>,
        ) -> Result<ProxyResponse> {
            Ok(ProxyResponse::new(StatusCode::OK).with_body(req.body))
        }
    }

    #[tokio::test]
    async fn wrapper_rewrites_request_and_response_bodies() {
        let chain: Vec<SharedMiddleware> = vec![Arc::new(Wrapper), Arc::new(EchoTerminal)];
        let terminal = FakeTerminal;
        let mut ctx = RequestContext::new();
        let resp = run_chain(&chain, &terminal, proxy_req(), &mut ctx)
            .await
            .unwrap();
        assert_eq!(resp.body, Bytes::from_static(b"prefix:<wrapped>"));
    }

    /// Test-only: middleware that errors out.
    struct Boom;

    #[async_trait]
    impl ProxyMiddleware for Boom {
        async fn handle(
            &self,
            _req: ProxyRequest,
            _ctx: &mut RequestContext,
            _next: Next<'_>,
        ) -> Result<ProxyResponse> {
            Err(ProxyError::Middleware("boom".into()))
        }
    }

    /// Test-only: catches any `Err` from `next.run` and returns a recovery
    /// response.
    struct Recover;

    #[async_trait]
    impl ProxyMiddleware for Recover {
        async fn handle(
            &self,
            req: ProxyRequest,
            ctx: &mut RequestContext,
            next: Next<'_>,
        ) -> Result<ProxyResponse> {
            match next.run(req, ctx).await {
                Ok(r) => Ok(r),
                Err(_) => {
                    Ok(ProxyResponse::new(StatusCode::OK)
                        .with_body(Bytes::from_static(b"recovered")))
                }
            }
        }
    }

    #[tokio::test]
    async fn outer_middleware_can_recover_from_inner_error() {
        let chain: Vec<SharedMiddleware> = vec![Arc::new(Recover), Arc::new(Boom)];
        let terminal = FakeTerminal;
        let mut ctx = RequestContext::new();
        let resp = run_chain(&chain, &terminal, proxy_req(), &mut ctx)
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(resp.body, Bytes::from_static(b"recovered"));
    }

    #[tokio::test]
    async fn error_propagates_when_no_middleware_catches() {
        let chain: Vec<SharedMiddleware> = vec![Arc::new(Boom)];
        let terminal = FakeTerminal;
        let mut ctx = RequestContext::new();
        let err = run_chain(&chain, &terminal, proxy_req(), &mut ctx)
            .await
            .unwrap_err();
        assert!(matches!(err, ProxyError::Middleware(_)));
    }

    struct StripAuth;

    #[async_trait]
    impl ProxyMiddleware for StripAuth {
        async fn handle(
            &self,
            req: ProxyRequest,
            ctx: &mut RequestContext,
            next: Next<'_>,
        ) -> Result<ProxyResponse> {
            next.run(req, ctx).await
        }

        fn redact_request_for_snapshot(&self, req: &mut ProxyRequest) {
            req.headers.remove("authorization");
        }

        fn redact_response_for_snapshot(&self, resp: &mut ProxyResponse) {
            resp.headers.remove("set-cookie");
        }
    }

    #[test]
    fn redact_helpers_apply_all_in_order() {
        let mut req = proxy_req();
        req.headers
            .insert("authorization", "Bearer xyz".parse().unwrap());
        req.headers.insert("x-keep", "ok".parse().unwrap());
        let chain: Vec<SharedMiddleware> = vec![Arc::new(StripAuth)];
        redact_request(&chain, &mut req);
        assert!(req.headers.get("authorization").is_none());
        assert_eq!(
            req.headers.get("x-keep").and_then(|v| v.to_str().ok()),
            Some("ok")
        );

        let mut resp = ProxyResponse::new(StatusCode::OK);
        resp.headers
            .insert("set-cookie", "sid=abc".parse().unwrap());
        resp.headers.insert("x-keep", "ok".parse().unwrap());
        redact_response(&chain, &mut resp);
        assert!(resp.headers.get("set-cookie").is_none());
        assert_eq!(
            resp.headers.get("x-keep").and_then(|v| v.to_str().ok()),
            Some("ok")
        );
    }
}
