//! Replay layered with middleware, stubs and the live forwarder.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};
use partly_proxy_lib::{
    Command, ExchangeOutcome, InMemoryStorage, Mode, Next, ProxyClusterBuilder, ProxyMiddleware,
    ProxyRequest, ProxyResponse, RecordedExchange, RecordedRequest, RecordedResponse,
    RecordingConfig, RequestContext, RequestMatcher, ResponseSource, Result as ProxyResult,
    SharedMiddleware, SharedStorage, StubbedResponse,
};

mod common;
use common::{cfg, http_client, spawn_echo, unreachable_addr};

fn in_memory_store(exchanges: Vec<RecordedExchange>) -> SharedStorage {
    Arc::new(InMemoryStorage::from(exchanges))
}

fn make_recorded(
    method: Method,
    path: &str,
    body: &[u8],
    status: u16,
    body_resp: &[u8],
) -> RecordedExchange {
    let req = RecordedRequest::from_parts(
        &method,
        &path.parse().unwrap(),
        &HeaderMap::new(),
        Bytes::copy_from_slice(body),
    );
    let resp = RecordedResponse {
        status,
        headers: vec![("content-type".into(), "application/json".into())],
        body: Bytes::copy_from_slice(body_resp),
    };
    RecordedExchange::new(
        Some("api".into()),
        req,
        ExchangeOutcome::Response(resp),
        Duration::from_millis(1),
    )
}

#[tokio::test]
async fn replay_hit_serves_recorded_response_without_touching_upstream() {
    // Bind an unreachable address as the upstream so any forward attempt
    // would clearly fail. Replay must succeed without ever reaching it.
    let unreachable = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = l.local_addr().unwrap();
        drop(l);
        a
    };

    let replay = in_memory_store(vec![make_recorded(
        Method::GET,
        "/health",
        b"",
        200,
        b"{\"ok\":true}",
    )]);
    let cluster = ProxyClusterBuilder::new()
        .add_upstream_with_mode(
            "api",
            cfg(format!("http://{unreachable}")),
            Vec::new(),
            Some(replay),
            Mode::Replay,
        )
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    let resp = http_client()
        .get(format!("http://{proxy}/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "{\"ok\":true}");

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn replay_mode_miss_returns_503_without_touching_upstream() {
    // SPECIFICATION.md §8.3: in Mode::Replay a replay miss never forwards to
    // the upstream — it returns 503 with body `{}`. Point the upstream at an
    // unreachable address so any accidental forward would surface as a 502.
    let unreachable = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = l.local_addr().unwrap();
        drop(l);
        a
    };
    let replay = in_memory_store(vec![make_recorded(
        Method::GET,
        "/health",
        b"",
        200,
        b"replayed",
    )]);
    let cluster = ProxyClusterBuilder::new()
        .add_upstream_with_mode(
            "api",
            cfg(format!("http://{unreachable}")),
            Vec::new(),
            Some(replay),
            Mode::Replay,
        )
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    // /health is in the snapshot — replay hits.
    let r = http_client()
        .get(format!("http://{proxy}/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "replayed");

    // /other is not — Mode::Replay returns 503 {} rather than forwarding.
    let r = http_client()
        .get(format!("http://{proxy}/other"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        r.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/json")
    );
    assert_eq!(r.text().await.unwrap(), "{}");

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn record_mode_miss_falls_through_to_upstream() {
    // SPECIFICATION.md §8.3: in Mode::Record a replay miss falls through to
    // the upstream so the new exchange can be recorded.
    let (echo_addr, _t) = spawn_echo().await;
    let replay = in_memory_store(vec![make_recorded(
        Method::GET,
        "/health",
        b"",
        200,
        b"replayed",
    )]);
    let cluster = ProxyClusterBuilder::new()
        .add_upstream_with_mode(
            "api",
            cfg(format!("http://{echo_addr}")),
            Vec::new(),
            Some(replay),
            Mode::Record,
        )
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    // /health is in the snapshot — replay returns "replayed".
    let r = http_client()
        .get(format!("http://{proxy}/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.text().await.unwrap(), "replayed");

    // /other is not — falls through to echo (returns JSON).
    let r = http_client()
        .get(format!("http://{proxy}/other"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["path"], "/other");

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn stub_takes_priority_over_replay() {
    let unreachable = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = l.local_addr().unwrap();
        drop(l);
        a
    };
    let replay = in_memory_store(vec![make_recorded(
        Method::GET,
        "/x",
        b"",
        200,
        b"from-replay",
    )]);
    let cluster = ProxyClusterBuilder::new()
        .add_upstream_with(
            "api",
            cfg(format!("http://{unreachable}")),
            Vec::new(),
            Some(replay),
        )
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    cluster
        .command_sender()
        .send(Command::Stub {
            upstream: None,
            matcher: RequestMatcher::new().method(Method::GET).path("/x"),
            response: StubbedResponse::new(StatusCode::IM_A_TEAPOT)
                .body(Bytes::from_static(b"from-stub")),
            times: Some(1),
        })
        .await
        .unwrap();

    // First call: stub fires.
    let r = http_client()
        .get(format!("http://{proxy}/x"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::IM_A_TEAPOT);
    assert_eq!(r.text().await.unwrap(), "from-stub");

    // Second call: stub exhausted, replay takes over.
    let r = http_client()
        .get(format!("http://{proxy}/x"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "from-replay");

    cluster.shutdown().await.unwrap();
}

/// Auth-stripping redactor used by the redaction test below.
struct StripAuthRedactor;

#[async_trait]
impl ProxyMiddleware for StripAuthRedactor {
    async fn handle(
        &self,
        req: ProxyRequest,
        ctx: &mut RequestContext,
        next: Next<'_>,
    ) -> ProxyResult<ProxyResponse> {
        next.run(req, ctx).await
    }

    fn redact_request_for_snapshot(&self, req: &mut ProxyRequest) {
        req.headers.remove("authorization");
    }
}

#[tokio::test]
async fn replay_lookup_uses_redact_request_for_snapshot() {
    // Snapshot was recorded with auth stripped (the recorded body is just
    // the literal "BODY", and the URI/method match). When the live request
    // arrives carrying an Authorization header, the redactor on the middleware
    // chain runs on a working copy before the lookup hashes the body, so the
    // snapshot hit succeeds.
    //
    // The body hash must match the *redacted* live body. Here the redactor
    // doesn't touch the body — the test exercises that the redaction step
    // runs at all, since the spec mandates that recorded snapshots that
    // had authorization stripped should still match incoming requests that
    // carry an authorization header.
    let unreachable = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = l.local_addr().unwrap();
        drop(l);
        a
    };
    let snapshot = make_recorded(Method::GET, "/secure", b"", 200, b"ok");
    let replay = in_memory_store(vec![snapshot]);
    let cluster = ProxyClusterBuilder::new()
        .add_upstream_with(
            "api",
            cfg(format!("http://{unreachable}")),
            vec![Arc::new(StripAuthRedactor) as SharedMiddleware],
            Some(replay),
        )
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    let r = http_client()
        .get(format!("http://{proxy}/secure"))
        .header("authorization", "Bearer live-token")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "ok");

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn record_mode_does_not_re_record_a_replay_hit() {
    // SPECIFICATION.md §8.3/§20.1: in `Mode::Record` the snapshot is a
    // deduplicating cache — a request already present is replayed "rather than
    // re-recording it". Serving a replay hit must therefore NOT append another
    // copy to the recorder (which would multiply entries for an already-seen
    // request). The upstream is unreachable, so a 200 + the snapshot body also
    // proves the request was replayed, not forwarded.
    let unreachable = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = l.local_addr().unwrap();
        drop(l);
        a
    };
    let replay = in_memory_store(vec![make_recorded(
        Method::GET,
        "/x",
        b"",
        200,
        b"replay-body",
    )]);
    let cluster = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(10))
        .add_upstream_with(
            "api",
            cfg(format!("http://{unreachable}")),
            Vec::new(),
            Some(replay),
        )
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    let resp = http_client()
        .get(format!("http://{proxy}/x"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "replay hit, not a forward to the dead upstream"
    );
    assert_eq!(resp.text().await.unwrap(), "replay-body");

    // Give the recorder ample time to (wrongly) append the replayed exchange.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        cluster.recorder().len().await,
        0,
        "a replayed request is already on record and must not be recorded again"
    );

    cluster.shutdown().await.unwrap();
}

/// Captures `ctx.response_source()` after `next.run` returns. Used by the
/// `ResponseSource` tests to assert which terminal branch produced the response.
struct CaptureSource(Arc<Mutex<Option<ResponseSource>>>);

#[async_trait]
impl ProxyMiddleware for CaptureSource {
    async fn handle(
        &self,
        req: ProxyRequest,
        ctx: &mut RequestContext,
        next: Next<'_>,
    ) -> ProxyResult<ProxyResponse> {
        let resp = next.run(req, ctx).await;
        *self.0.lock().unwrap() = ctx.response_source();
        resp
    }
}

/// Short-circuits without ever calling `next.run`.
struct ShortCircuit;

#[async_trait]
impl ProxyMiddleware for ShortCircuit {
    async fn handle(
        &self,
        _req: ProxyRequest,
        _ctx: &mut RequestContext,
        _next: Next<'_>,
    ) -> ProxyResult<ProxyResponse> {
        Ok(ProxyResponse::new(StatusCode::IM_A_TEAPOT).with_body(Bytes::from_static(b"short")))
    }
}

#[tokio::test]
async fn response_source_stub_marks_ctx() {
    let captured = Arc::new(Mutex::new(None));
    let cluster = ProxyClusterBuilder::new()
        .add_upstream_with(
            "api",
            cfg(format!("http://{}", unreachable_addr())),
            vec![Arc::new(CaptureSource(captured.clone())) as SharedMiddleware],
            None,
        )
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    cluster
        .command_sender()
        .send(Command::Stub {
            upstream: None,
            matcher: RequestMatcher::new().method(Method::GET).path("/x"),
            response: StubbedResponse::new(StatusCode::OK).body(Bytes::from_static(b"ok")),
            times: Some(1),
        })
        .await
        .unwrap();

    let r = http_client()
        .get(format!("http://{proxy}/x"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(*captured.lock().unwrap(), Some(ResponseSource::Stub));

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn response_source_snapshot_marks_ctx() {
    let captured = Arc::new(Mutex::new(None));
    let replay = in_memory_store(vec![make_recorded(
        Method::GET,
        "/x",
        b"",
        200,
        b"replayed",
    )]);
    let cluster = ProxyClusterBuilder::new()
        .add_upstream_with_mode(
            "api",
            cfg(format!("http://{}", unreachable_addr())),
            vec![Arc::new(CaptureSource(captured.clone())) as SharedMiddleware],
            Some(replay),
            Mode::Replay,
        )
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    let r = http_client()
        .get(format!("http://{proxy}/x"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(*captured.lock().unwrap(), Some(ResponseSource::Snapshot));

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn response_source_replay_miss_marks_ctx() {
    let captured = Arc::new(Mutex::new(None));
    let replay = in_memory_store(vec![make_recorded(
        Method::GET,
        "/x",
        b"",
        200,
        b"replayed",
    )]);
    let cluster = ProxyClusterBuilder::new()
        .add_upstream_with_mode(
            "api",
            cfg(format!("http://{}", unreachable_addr())),
            vec![Arc::new(CaptureSource(captured.clone())) as SharedMiddleware],
            Some(replay),
            Mode::Replay,
        )
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    let r = http_client()
        .get(format!("http://{proxy}/miss"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(*captured.lock().unwrap(), Some(ResponseSource::ReplayMiss));

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn response_source_upstream_marks_ctx() {
    let (echo_addr, _t) = spawn_echo().await;
    let captured = Arc::new(Mutex::new(None));
    let cluster = ProxyClusterBuilder::new()
        .add_upstream_with_mode(
            "api",
            cfg(format!("http://{echo_addr}")),
            vec![Arc::new(CaptureSource(captured.clone())) as SharedMiddleware],
            None,
            Mode::Record,
        )
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    let r = http_client()
        .get(format!("http://{proxy}/anything"))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    assert_eq!(*captured.lock().unwrap(), Some(ResponseSource::Upstream));

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn response_source_absent_when_middleware_short_circuits() {
    let captured = Arc::new(Mutex::new(None));
    let cluster = ProxyClusterBuilder::new()
        .add_upstream_with(
            "api",
            cfg(format!("http://{}", unreachable_addr())),
            vec![
                Arc::new(CaptureSource(captured.clone())) as SharedMiddleware,
                Arc::new(ShortCircuit) as SharedMiddleware,
            ],
            None,
        )
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    let r = http_client()
        .get(format!("http://{proxy}/anything"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::IM_A_TEAPOT);
    assert_eq!(*captured.lock().unwrap(), None);

    cluster.shutdown().await.unwrap();
}
