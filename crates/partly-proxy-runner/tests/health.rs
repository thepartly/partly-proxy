//! Integration tests for the runner's health endpoints.

use std::net::SocketAddr;
use std::time::Duration;

use partly_proxy_runner::{run, RunnerOptions};

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}

fn opts(upstream_url: String) -> RunnerOptions {
    RunnerOptions {
        proxy_bind: "127.0.0.1:0".parse().unwrap(),
        health_bind: "127.0.0.1:0".parse().unwrap(),
        upstream_url,
        tcp_control_bind: None,
        recording: partly_proxy_lib::RecordingConfig::in_memory(100),
    }
}

/// Spawn an in-process echo upstream for the runner to point at.
async fn spawn_echo() -> SocketAddr {
    let (addr, listener) = partly_proxy_echo::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = partly_proxy_echo::serve(listener).await;
    });
    addr
}

#[tokio::test]
async fn health_endpoints_are_always_200() {
    let echo = spawn_echo().await;
    let runner = run(opts(format!("http://{echo}"))).await.unwrap();

    for path in ["/health", "/healthz"] {
        let r = http_client()
            .get(format!("http://{}{path}", runner.health_addr))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "{path}");
        assert_eq!(r.text().await.unwrap(), "ok");
    }

    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn ready_endpoint_reports_upstream_status() {
    let echo = spawn_echo().await;
    let runner = run(opts(format!("http://{echo}"))).await.unwrap();

    // Drive one request through the proxy so the exchange counter is non-zero.
    let _ = http_client()
        .get(format!("http://{}/x", runner.proxy_addr))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    // Allow the recorder to catch up.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while runner.cluster.recorder().len().await < 1 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    for path in ["/ready", "/readyz"] {
        let r = http_client()
            .get(format!("http://{}{path}", runner.health_addr))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "{path}");
        let body: serde_json::Value = r.json().await.unwrap();
        assert_eq!(body["ready"], true);
        let upstreams = body["upstreams"].as_array().unwrap();
        assert_eq!(upstreams.len(), 1);
        assert_eq!(upstreams[0]["name"], "upstream");
        assert_eq!(upstreams[0]["ready"], true);
        assert!(upstreams[0]["exchange_count"].as_u64().unwrap() >= 1);
    }

    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_path_returns_404() {
    let echo = spawn_echo().await;
    let runner = run(opts(format!("http://{echo}"))).await.unwrap();
    let r = http_client()
        .get(format!("http://{}/nope", runner.health_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    runner.shutdown().await.unwrap();
}
