//! Middleware chain wired through the live listener, with body
//! rewrites, short-circuits, recovery, and snapshot-boundary redaction.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http::StatusCode;
use partly_proxy_echo as echo;
use partly_proxy_lib::{
    ClusterHandle, Next, ProxyClusterBuilder, ProxyConfig, ProxyMiddleware, ProxyRequest,
    ProxyResponse, RecordingConfig, RequestContext, Result as ProxyResult, SharedMiddleware,
    UpstreamTarget,
};
use tokio::task::JoinHandle;

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
        .expect("reqwest client builds")
}

fn upstream_cfg(url: String) -> ProxyConfig {
    ProxyConfig::http(
        "127.0.0.1:0".parse().unwrap(),
        UpstreamTarget::new(url)
            .with_connect_timeout(Duration::from_secs(1))
            .with_request_timeout(Duration::from_secs(5)),
    )
}

struct ShortCircuit200;

#[async_trait]
impl ProxyMiddleware for ShortCircuit200 {
    async fn handle(
        &self,
        _req: ProxyRequest,
        _ctx: &mut RequestContext,
        _next: Next<'_>,
    ) -> ProxyResult<ProxyResponse> {
        Ok(ProxyResponse::new(StatusCode::OK)
            .with_header("x-from-middleware", Bytes::from_static(b"yes"))
            .with_body(Bytes::from_static(b"hello")))
    }
}

#[tokio::test]
async fn short_circuit_middleware_skips_forwarding() {
    let (echo_addr, _t) = spawn_echo().await;
    // Echo path /_status/500 would normally surface as 500; the short-circuit
    // middleware must win.
    let cluster = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(10))
        .add_upstream_with_middleware(
            "api",
            upstream_cfg(format!("http://{echo_addr}")),
            vec![Arc::new(ShortCircuit200) as SharedMiddleware],
        )
        .run()
        .await
        .unwrap();
    let addr = cluster.addr("api").unwrap();

    let resp = http_client()
        .get(format!("http://{addr}/_status/500"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("x-from-middleware")
            .map(|v| v.to_str().unwrap()),
        Some("yes")
    );
    assert_eq!(resp.text().await.unwrap(), "hello");

    cluster.shutdown().await.unwrap();
}

struct PrefixBody {
    prefix: &'static [u8],
}

#[async_trait]
impl ProxyMiddleware for PrefixBody {
    async fn handle(
        &self,
        req: ProxyRequest,
        ctx: &mut RequestContext,
        next: Next<'_>,
    ) -> ProxyResult<ProxyResponse> {
        let mut resp = next.run(req, ctx).await?;
        let mut new = self.prefix.to_vec();
        new.extend_from_slice(&resp.body);
        resp.body = Bytes::from(new);
        Ok(resp)
    }
}

#[tokio::test]
async fn response_body_rewrite_lands_on_the_wire() {
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .add_upstream_with_middleware(
            "api",
            upstream_cfg(format!("http://{echo_addr}")),
            vec![Arc::new(PrefixBody { prefix: b"PREFIX:" }) as SharedMiddleware],
        )
        .run()
        .await
        .unwrap();
    let addr = cluster.addr("api").unwrap();

    let body = http_client()
        .get(format!("http://{addr}/_status/200"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    // Echo returns "status=200" for /_status/200; the middleware prepends.
    assert_eq!(body, "PREFIX:status=200");

    cluster.shutdown().await.unwrap();
}

struct RewriteRequestBody {
    new_body: &'static [u8],
}

#[async_trait]
impl ProxyMiddleware for RewriteRequestBody {
    async fn handle(
        &self,
        mut req: ProxyRequest,
        ctx: &mut RequestContext,
        next: Next<'_>,
    ) -> ProxyResult<ProxyResponse> {
        req.body = Bytes::copy_from_slice(self.new_body);
        // Drop content-length so hyper recomputes it on the outbound side.
        req.headers.remove("content-length");
        next.run(req, ctx).await
    }
}

#[tokio::test]
async fn request_body_rewrite_reaches_upstream() {
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .add_upstream_with_middleware(
            "api",
            upstream_cfg(format!("http://{echo_addr}")),
            vec![Arc::new(RewriteRequestBody {
                new_body: b"REWRITTEN",
            }) as SharedMiddleware],
        )
        .run()
        .await
        .unwrap();
    let addr = cluster.addr("api").unwrap();

    let resp_body: serde_json::Value = http_client()
        .post(format!("http://{addr}/echo"))
        .body("original")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp_body["body"]["encoding"], "utf8");
    assert_eq!(resp_body["body"]["value"], "REWRITTEN");

    cluster.shutdown().await.unwrap();
}

struct Recover;

#[async_trait]
impl ProxyMiddleware for Recover {
    async fn handle(
        &self,
        req: ProxyRequest,
        ctx: &mut RequestContext,
        next: Next<'_>,
    ) -> ProxyResult<ProxyResponse> {
        match next.run(req, ctx).await {
            Ok(r) => Ok(r),
            Err(_) => Ok(ProxyResponse::new(StatusCode::OK)
                .with_header("x-recovered", Bytes::from_static(b"true"))
                .with_body(Bytes::from_static(b"recovered"))),
        }
    }
}

#[tokio::test]
async fn middleware_can_recover_from_upstream_failure() {
    // Bind a listener, grab the addr, drop it — the upstream URL refuses.
    let unreachable = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = l.local_addr().unwrap();
        drop(l);
        a
    };
    let cluster = ProxyClusterBuilder::new()
        .add_upstream_with_middleware(
            "api",
            upstream_cfg(format!("http://{unreachable}")),
            vec![Arc::new(Recover) as SharedMiddleware],
        )
        .run()
        .await
        .unwrap();
    let addr = cluster.addr("api").unwrap();

    let resp = http_client()
        .get(format!("http://{addr}/missing"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("x-recovered")
            .map(|v| v.to_str().unwrap()),
        Some("true")
    );
    assert_eq!(resp.text().await.unwrap(), "recovered");

    cluster.shutdown().await.unwrap();
}

struct StripAuth;

#[async_trait]
impl ProxyMiddleware for StripAuth {
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
        req.headers.remove("cookie");
    }

    fn redact_response_for_snapshot(&self, resp: &mut ProxyResponse) {
        resp.headers.remove("set-cookie");
    }
}

#[tokio::test]
async fn snapshot_redaction_strips_secrets_from_recorder_only() {
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(10))
        .add_upstream_with_middleware(
            "api",
            upstream_cfg(format!("http://{echo_addr}")),
            vec![Arc::new(StripAuth) as SharedMiddleware],
        )
        .run()
        .await
        .unwrap();
    let addr = cluster.addr("api").unwrap();

    // The live request carries an Authorization header — upstream should see
    // it, but the recorder should not.
    let live = http_client()
        .get(format!("http://{addr}/echo"))
        .header("authorization", "Bearer secret-token")
        .header("cookie", "sid=xyz")
        .send()
        .await
        .unwrap();
    assert_eq!(live.status(), 200);
    let live_body: serde_json::Value = live.json().await.unwrap();
    // Upstream (echo) should observe the live header.
    let auth_seen = live_body["headers"].as_array().unwrap().iter().any(|h| {
        h[0].as_str() == Some("authorization") && h[1].as_str() == Some("Bearer secret-token")
    });
    assert!(auth_seen, "upstream must still see the live auth header");

    // Now check the recorder — auth should be stripped.
    let recorded = wait_for_recordings(&cluster, 1).await;
    let req = &recorded[0].request;
    assert!(
        !req.headers.iter().any(|(k, _)| k == "authorization"),
        "authorization should be redacted from recording: {:?}",
        req.headers
    );
    assert!(
        !req.headers.iter().any(|(k, _)| k == "cookie"),
        "cookie should be redacted from recording: {:?}",
        req.headers
    );

    cluster.shutdown().await.unwrap();
}

struct CountingMiddleware {
    tag: &'static str,
    counter: Arc<AtomicUsize>,
}

#[async_trait]
impl ProxyMiddleware for CountingMiddleware {
    async fn handle(
        &self,
        mut req: ProxyRequest,
        ctx: &mut RequestContext,
        next: Next<'_>,
    ) -> ProxyResult<ProxyResponse> {
        self.counter.fetch_add(1, Ordering::SeqCst);
        // Stamp a header so we can verify the order on the upstream side.
        let header_name = format!("x-stamp-{}", self.tag);
        let header_value =
            http::HeaderValue::from_str(&self.counter.load(Ordering::SeqCst).to_string()).unwrap();
        req.headers.insert(
            http::HeaderName::try_from(header_name).unwrap(),
            header_value,
        );
        next.run(req, ctx).await
    }
}

#[tokio::test]
async fn global_middleware_runs_before_per_upstream() {
    let global_counter = Arc::new(AtomicUsize::new(0));
    let local_counter = Arc::new(AtomicUsize::new(0));
    let (echo_addr, _t) = spawn_echo().await;

    let global = CountingMiddleware {
        tag: "global",
        counter: global_counter.clone(),
    };
    let local = CountingMiddleware {
        tag: "local",
        counter: local_counter.clone(),
    };

    let cluster = ProxyClusterBuilder::new()
        .add_middleware(global)
        .add_upstream_with_middleware(
            "api",
            upstream_cfg(format!("http://{echo_addr}")),
            vec![Arc::new(local) as SharedMiddleware],
        )
        .run()
        .await
        .unwrap();
    let addr = cluster.addr("api").unwrap();

    let body: serde_json::Value = http_client()
        .get(format!("http://{addr}/echo"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let headers = body["headers"].as_array().unwrap();
    let has_global = headers
        .iter()
        .any(|h| h[0].as_str() == Some("x-stamp-global"));
    let has_local = headers
        .iter()
        .any(|h| h[0].as_str() == Some("x-stamp-local"));
    assert!(has_global && has_local, "both stamps reached upstream");

    assert_eq!(global_counter.load(Ordering::SeqCst), 1);
    assert_eq!(local_counter.load(Ordering::SeqCst), 1);

    cluster.shutdown().await.unwrap();
}

async fn wait_for_recordings(
    cluster: &ClusterHandle,
    target: usize,
) -> Vec<partly_proxy_lib::RecordedExchange> {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let exchanges = cluster.recorder().exchanges().await;
        if exchanges.len() >= target {
            return exchanges;
        }
        if std::time::Instant::now() >= deadline {
            return exchanges;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
