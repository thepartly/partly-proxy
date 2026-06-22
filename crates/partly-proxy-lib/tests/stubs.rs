//! Stubs, pause/resume, and the in-process command plane driving the
//! live listener.

use std::time::Duration;

use bytes::Bytes;
use http::StatusCode;
use partly_proxy_lib::{
    Command, CommandResponse, ProxyClusterBuilder, RecordingConfig, RequestMatcher,
    StubbedResponse, TrafficFilter,
};

mod common;
use common::{cfg, http_client, spawn_echo};

#[tokio::test]
async fn stub_overrides_upstream_response() {
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .add_upstream("api", cfg(format!("http://{echo_addr}")))
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    let resp = cluster
        .command_sender()
        .send(Command::Stub {
            upstream: Some("api".into()),
            matcher: RequestMatcher::new()
                .method(http::Method::POST)
                .path(r"^/orders/\d+/refund$"),
            response: StubbedResponse::new(StatusCode::CREATED)
                .header("content-type", "application/json")
                .body(Bytes::from_static(b"{\"ok\":true}")),
            times: Some(2),
        })
        .await
        .unwrap();
    assert!(matches!(resp, CommandResponse::Ok));

    // First two hits get the stub.
    for _ in 0..2 {
        let r = http_client()
            .post(format!("http://{proxy}/orders/123/refund"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 201);
        assert_eq!(
            r.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/json")
        );
        assert_eq!(r.text().await.unwrap(), "{\"ok\":true}");
    }

    // Third hit falls through to the echo upstream (200).
    let r = http_client()
        .post(format!("http://{proxy}/orders/123/refund"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn stub_delay_is_honoured() {
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .add_upstream("api", cfg(format!("http://{echo_addr}")))
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    cluster
        .command_sender()
        .send(Command::Stub {
            upstream: None, // single upstream → implicit
            matcher: RequestMatcher::new().path("/slow"),
            response: StubbedResponse::new(StatusCode::OK)
                .delay(Duration::from_millis(150))
                .body(Bytes::from_static(b"ok")),
            times: None,
        })
        .await
        .unwrap();

    let start = std::time::Instant::now();
    let r = http_client()
        .get(format!("http://{proxy}/slow"))
        .send()
        .await
        .unwrap();
    let elapsed = start.elapsed();
    assert_eq!(r.status(), 200);
    assert!(
        elapsed >= Duration::from_millis(140),
        "expected at least 140ms, got {elapsed:?}"
    );

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn clear_stubs_removes_all_registered() {
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .add_upstream("api", cfg(format!("http://{echo_addr}")))
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    cluster
        .command_sender()
        .send(Command::Stub {
            upstream: None,
            matcher: RequestMatcher::new().path("/x"),
            response: StubbedResponse::new(StatusCode::IM_A_TEAPOT),
            times: None,
        })
        .await
        .unwrap();
    cluster
        .command_sender()
        .send(Command::ClearStubs { upstream: None })
        .await
        .unwrap();

    let r = http_client()
        .get(format!("http://{proxy}/x"))
        .send()
        .await
        .unwrap();
    // Stub cleared — falls through to echo (200).
    assert_eq!(r.status(), 200);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn pause_blocks_until_resume() {
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .add_upstream("api", cfg(format!("http://{echo_addr}")))
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    cluster
        .command_sender()
        .send(Command::Pause { upstream: None })
        .await
        .unwrap();

    // Kick off a request that should block on the pause gate.
    let pending = tokio::spawn(async move {
        http_client()
            .get(format!("http://{proxy}/x"))
            .send()
            .await
            .map(|r| r.status())
    });

    // Give the request a moment to enter the pause gate, then verify it
    // hasn't completed.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !pending.is_finished(),
        "request must remain blocked while upstream paused"
    );

    // Resume — the request should complete promptly.
    cluster
        .command_sender()
        .send(Command::Resume { upstream: None })
        .await
        .unwrap();

    let status = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .expect("request resumes within timeout")
        .expect("task does not panic")
        .expect("reqwest succeeds");
    assert_eq!(status, 200);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn query_traffic_returns_filtered_exchanges() {
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(100))
        .add_upstream("api", cfg(format!("http://{echo_addr}")))
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    for path in ["/orders", "/orders", "/health"] {
        let _ = http_client()
            .get(format!("http://{proxy}{path}"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
    }

    // Wait for the recorder to catch up.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        if cluster.recorder().len().await >= 3 {
            break;
        }
        if std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let resp = cluster
        .command_sender()
        .send(Command::QueryTraffic {
            filter: TrafficFilter::new().path_pattern("^/orders$"),
        })
        .await
        .unwrap();
    match resp {
        CommandResponse::Exchanges(exchanges) => {
            assert_eq!(exchanges.len(), 2);
            for e in &exchanges {
                assert!(e.request.uri.ends_with("/orders"));
            }
        }
        other => panic!("expected Exchanges, got {other:?}"),
    }

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn stub_against_unknown_upstream_returns_error_in_band() {
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .add_upstream("api", cfg(format!("http://{echo_addr}")))
        .run()
        .await
        .unwrap();

    let resp = cluster
        .command_sender()
        .send(Command::Stub {
            upstream: Some("missing".into()),
            matcher: RequestMatcher::new(),
            response: StubbedResponse::new(StatusCode::OK),
            times: None,
        })
        .await
        .unwrap();
    assert!(
        matches!(&resp, CommandResponse::Error { message } if message.contains("missing")),
        "got {resp:?}"
    );

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn clear_recordings_empties_the_buffer() {
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(50))
        .add_upstream("api", cfg(format!("http://{echo_addr}")))
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

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
    // Give the recorder time to land all three.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while cluster.recorder().len().await < 3 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(cluster.recorder().len().await >= 3);

    cluster
        .command_sender()
        .send(Command::ClearRecordings)
        .await
        .unwrap();
    assert_eq!(cluster.recorder().len().await, 0);

    cluster.shutdown().await.unwrap();
}
