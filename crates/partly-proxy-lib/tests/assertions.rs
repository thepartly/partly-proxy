//! Wait-for semantics of `AssertSeen` and `AssertCount` (see
//! `SPECIFICATION.md` §14.1).

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use partly_proxy_echo as echo;
use partly_proxy_lib::{
    Command, CommandResponse, ProxyClusterBuilder, ProxyConfig, RecordingConfig, TrafficFilter,
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
        .unwrap()
}

fn cfg(url: String) -> ProxyConfig {
    ProxyConfig::http(
        "127.0.0.1:0".parse().unwrap(),
        UpstreamTarget::new(url)
            .with_connect_timeout(Duration::from_secs(1))
            .with_request_timeout(Duration::from_secs(5)),
    )
}

#[tokio::test]
async fn assert_seen_blocks_until_traffic_arrives() {
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(10))
        .add_upstream("api", cfg(format!("http://{echo_addr}")))
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();
    let sender = cluster.command_sender().clone();

    // Spawn the assertion; it must block.
    let assertion = tokio::spawn(async move {
        sender
            .send(Command::AssertSeen {
                filter: TrafficFilter::new().path_pattern("^/marker$"),
                timeout: Duration::from_secs(5),
            })
            .await
    });

    // Sleep, then drive the matching traffic.
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        !assertion.is_finished(),
        "assertion should still be blocked"
    );
    let _ = http_client()
        .get(format!("http://{proxy}/marker"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(2), assertion)
        .await
        .expect("assertion finishes")
        .expect("task ok")
        .expect("command ok");
    assert!(
        matches!(
            result,
            CommandResponse::AssertionResult { passed: true, .. }
        ),
        "got {result:?}"
    );

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn assert_seen_times_out_when_no_match() {
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(10))
        .add_upstream("api", cfg(format!("http://{echo_addr}")))
        .run()
        .await
        .unwrap();

    let start = Instant::now();
    let resp = cluster
        .command_sender()
        .send(Command::AssertSeen {
            filter: TrafficFilter::new().path_pattern("^/never-happens$"),
            timeout: Duration::from_millis(200),
        })
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert!(
        matches!(
            &resp,
            CommandResponse::AssertionResult {
                passed: false,
                message
            } if message.contains("timeout")
        ),
        "got {resp:?}"
    );
    assert!(elapsed >= Duration::from_millis(180));
    assert!(elapsed < Duration::from_secs(1));

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn assert_seen_with_zero_timeout_evaluates_immediately() {
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(10))
        .add_upstream("api", cfg(format!("http://{echo_addr}")))
        .run()
        .await
        .unwrap();

    let start = Instant::now();
    let resp = cluster
        .command_sender()
        .send(Command::AssertSeen {
            filter: TrafficFilter::new(),
            timeout: Duration::from_millis(0),
        })
        .await
        .unwrap();
    let elapsed = start.elapsed();
    // Should complete almost immediately; no waiting.
    assert!(elapsed < Duration::from_millis(100));
    assert!(matches!(
        resp,
        CommandResponse::AssertionResult { passed: false, .. }
    ));

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn assert_count_passes_when_exact_match() {
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(10))
        .add_upstream("api", cfg(format!("http://{echo_addr}")))
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();
    let sender = cluster.command_sender().clone();

    let assertion = tokio::spawn(async move {
        sender
            .send(Command::AssertCount {
                filter: TrafficFilter::new().path_pattern("^/orders$"),
                expected: 2,
                timeout: Duration::from_secs(2),
            })
            .await
    });

    for _ in 0..2 {
        let _ = http_client()
            .get(format!("http://{proxy}/orders"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
    }

    let result = tokio::time::timeout(Duration::from_secs(3), assertion)
        .await
        .expect("assertion finishes")
        .expect("task ok")
        .expect("command ok");
    assert!(
        matches!(
            result,
            CommandResponse::AssertionResult { passed: true, .. }
        ),
        "got {result:?}"
    );

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn assert_count_fails_fast_on_overshoot() {
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(10))
        .add_upstream("api", cfg(format!("http://{echo_addr}")))
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    // Pre-populate three matching exchanges *before* sending AssertCount.
    for _ in 0..3 {
        let _ = http_client()
            .get(format!("http://{proxy}/x"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
    }
    // Wait for the recorder.
    let deadline = Instant::now() + Duration::from_secs(2);
    while cluster.recorder().len().await < 3 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let start = Instant::now();
    let resp = cluster
        .command_sender()
        .send(Command::AssertCount {
            filter: TrafficFilter::new().path_pattern("^/x$"),
            expected: 2,
            timeout: Duration::from_secs(10),
        })
        .await
        .unwrap();
    let elapsed = start.elapsed();

    // Should fail fast, not wait the full 10 seconds.
    assert!(
        matches!(
            &resp,
            CommandResponse::AssertionResult { passed: false, message } if message.contains("overshoot")
        ),
        "got {resp:?}"
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "fail-fast took {elapsed:?}"
    );

    cluster.shutdown().await.unwrap();
}
