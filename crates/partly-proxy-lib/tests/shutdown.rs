//! Integration tests for `ClusterHandle::shutdown_with_timeout` —
//! see `SPECIFICATION.md` §16.
//!
//! Uses the stub `delay` knob (see `stub::StubbedResponse::delay`) to
//! simulate slow in-flight requests without standing up a slow upstream.

use std::{net::SocketAddr, time::Duration};

use bytes::Bytes;
use http::StatusCode;
use partly_proxy_lib::{
    Command, ProxyClusterBuilder, ProxyConfig, RequestMatcher, StubbedResponse, UpstreamTarget,
};

fn cfg() -> ProxyConfig {
    ProxyConfig::http(
        "127.0.0.1:0".parse().unwrap(),
        UpstreamTarget::new("http://127.0.0.1:1")
            .with_connect_timeout(Duration::from_secs(1))
            .with_request_timeout(Duration::from_secs(5)),
    )
}

fn h1_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap()
}

fn h2_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .http2_prior_knowledge()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap()
}

/// Register a stub that responds with `body` after `delay`.
async fn stub_slow(
    cluster: &partly_proxy_lib::ClusterHandle,
    path: &str,
    body: &'static [u8],
    delay: Duration,
) {
    cluster
        .command_sender()
        .send(Command::Stub {
            upstream: None,
            matcher: RequestMatcher::new().path(path),
            response: StubbedResponse::new(StatusCode::OK)
                .delay(delay)
                .body(Bytes::from_static(body)),
            times: None,
        })
        .await
        .unwrap();
}

/// 1. A request that finishes inside the drain budget completes successfully,
///    and `shutdown_with_timeout` returns `Ok(())`.
#[tokio::test]
async fn drain_completes_before_deadline() {
    let cluster = ProxyClusterBuilder::new()
        .add_upstream("api", cfg())
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();
    stub_slow(&cluster, "/slow", b"drained", Duration::from_millis(200)).await;

    let req = tokio::spawn(async move {
        h1_client()
            .get(format!("http://{proxy}/slow"))
            .send()
            .await
            .unwrap()
    });
    // Give the request a moment to land on the listener.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let started = std::time::Instant::now();
    cluster
        .shutdown_with_timeout(Duration::from_secs(3))
        .await
        .unwrap();
    let shutdown_elapsed = started.elapsed();

    let resp = req.await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"drained");
    // Drain only had to wait ~150ms more after the request landed; well under 3s.
    assert!(
        shutdown_elapsed < Duration::from_secs(2),
        "expected shutdown to drain quickly, took {shutdown_elapsed:?}"
    );
}

/// 2. A request slower than the drain budget is hard-aborted; the client sees a
///    connection error rather than the stubbed response, and shutdown still
///    returns within the deadline plus the documented 1-second outer slack
///    (with measurement slop for slow CI runners).
#[tokio::test]
async fn deadline_aborts_slow_request() {
    let cluster = ProxyClusterBuilder::new()
        .add_upstream("api", cfg())
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();
    stub_slow(&cluster, "/forever", b"never", Duration::from_secs(60)).await;

    let req = tokio::spawn(async move {
        h1_client()
            .get(format!("http://{proxy}/forever"))
            .send()
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let started = std::time::Instant::now();
    cluster
        .shutdown_with_timeout(Duration::from_millis(500))
        .await
        .unwrap();
    let shutdown_elapsed = started.elapsed();

    let result = req.await.unwrap();
    assert!(
        result.is_err(),
        "expected the client to see a connection error, got {result:?}"
    );
    // 500ms drain + 1s outer slack = 1.5s expected; allow generous slop for CI.
    assert!(
        shutdown_elapsed < Duration::from_secs(4),
        "expected shutdown to return promptly, took {shutdown_elapsed:?}"
    );
}

/// 3. A cluster with no in-flight requests shuts down well under the budget.
#[tokio::test]
async fn no_traffic_shutdown_is_fast() {
    let cluster = ProxyClusterBuilder::new()
        .add_upstream("api", cfg())
        .run()
        .await
        .unwrap();

    let started = std::time::Instant::now();
    cluster
        .shutdown_with_timeout(Duration::from_secs(30))
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "no-traffic shutdown should be near-instant, took {elapsed:?}"
    );
}

/// 4. After shutdown fires, the listener stops accepting connections —
///    fresh TCP connects either fail outright or are torn down without a
///    response. (The kernel's accept backlog may hold one or two stale
///    sockets briefly after `drop(listener)`, so we assert "eventually
///    rejected within a short polling window", not "first attempt rejected".)
#[tokio::test]
async fn listener_stops_accepting() {
    let cluster = ProxyClusterBuilder::new()
        .add_upstream("api", cfg())
        .run()
        .await
        .unwrap();
    let proxy: SocketAddr = cluster.addr("api").unwrap();

    // Pause shutdown completion by holding a slow in-flight request open.
    stub_slow(&cluster, "/hold", b"held", Duration::from_millis(800)).await;
    let _holder = tokio::spawn(async move {
        let _ = h1_client().get(format!("http://{proxy}/hold")).send().await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let shutdown_task =
        tokio::spawn(async move { cluster.shutdown_with_timeout(Duration::from_secs(3)).await });

    // Poll connect attempts for up to 500ms; assert at least one of the
    // last attempts is rejected or immediately closed.
    let deadline = std::time::Instant::now() + Duration::from_millis(500);
    let mut saw_rejection = false;
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(
            Duration::from_millis(50),
            tokio::net::TcpStream::connect(proxy),
        )
        .await
        {
            Ok(Err(_)) | Err(_) => {
                saw_rejection = true;
                break;
            }
            Ok(Ok(mut s)) => {
                // Connect succeeded — verify the listener is no longer
                // serving by trying to read; an immediate EOF/error is
                // equivalent to "not accepting".
                use tokio::io::AsyncReadExt;
                let mut buf = [0u8; 1];
                if let Ok(Ok(0)) =
                    tokio::time::timeout(Duration::from_millis(50), s.read(&mut buf)).await
                {
                    saw_rejection = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    assert!(
        saw_rejection,
        "listener kept accepting after shutdown was requested"
    );

    shutdown_task.await.unwrap().unwrap();
}

/// 5. The drain path also works for HTTP/2 connections — `auto::Builder`
///    negotiates H2 via prior-knowledge and hyper's graceful close sends
///    GOAWAY before the connection finishes.
#[tokio::test]
async fn h2_drain_completes_before_deadline() {
    let cluster = ProxyClusterBuilder::new()
        .add_upstream("api", cfg())
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();
    stub_slow(
        &cluster,
        "/slow2",
        b"drained-h2",
        Duration::from_millis(200),
    )
    .await;

    let req = tokio::spawn(async move {
        h2_client()
            .get(format!("http://{proxy}/slow2"))
            .send()
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    cluster
        .shutdown_with_timeout(Duration::from_secs(3))
        .await
        .unwrap();

    let resp = req.await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.version(), http::Version::HTTP_2);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"drained-h2");
}
