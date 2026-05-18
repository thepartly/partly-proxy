//! Integration tests for the runner — proves it binds the proxy listener
//! and forwards through to a real in-process upstream.

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
async fn runner_forwards_through_to_upstream() {
    let echo = spawn_echo().await;
    let runner = run(opts(format!("http://{echo}"))).await.unwrap();

    let body: serde_json::Value = http_client()
        .get(format!("http://{}/forwarded?x=1", runner.proxy_addr))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["path"], "/forwarded");

    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn runner_with_tcp_control_plane_binds_both_listeners() {
    let echo = spawn_echo().await;
    let mut runner_opts = opts(format!("http://{echo}"));
    runner_opts.tcp_control_bind = Some("127.0.0.1:0".parse().unwrap());
    let runner = run(runner_opts).await.unwrap();

    let ctrl = runner
        .cluster
        .tcp_control_addr()
        .expect("TCP control plane bound");
    // Smoke check: connection accepted.
    let stream = tokio::net::TcpStream::connect(ctrl).await;
    assert!(
        stream.is_ok(),
        "failed to connect to TCP control: {stream:?}"
    );

    runner.shutdown().await.unwrap();
}
