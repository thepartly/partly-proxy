//! End-to-end coverage of the JSON-Lines TCP control plane.

use std::{net::SocketAddr, time::Duration};

use partly_proxy_echo as echo;
use partly_proxy_lib::{ProxyClusterBuilder, ProxyConfig, RecordingConfig, UpstreamTarget};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    task::JoinHandle,
};

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

/// Send one JSON line, return the next response line.
async fn rt(addr: SocketAddr, line: &str) -> serde_json::Value {
    let mut conn = TcpStream::connect(addr).await.expect("connect TCP control");
    conn.write_all(line.as_bytes())
        .await
        .expect("write request line");
    if !line.ends_with('\n') {
        conn.write_all(b"\n").await.expect("write newline");
    }
    conn.flush().await.expect("flush");
    let (r, _w) = conn.into_split();
    let mut lines = BufReader::new(r).lines();
    let next = tokio::time::timeout(Duration::from_secs(2), lines.next_line())
        .await
        .expect("response within timeout")
        .expect("io ok")
        .expect("at least one response line");
    serde_json::from_str(&next).expect("response is JSON")
}

#[tokio::test]
async fn tcp_stub_and_hit_via_wire() {
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(10))
        .add_upstream(
            "api",
            ProxyConfig::http(
                "127.0.0.1:0".parse().unwrap(),
                UpstreamTarget::new(format!("http://{echo_addr}"))
                    .with_connect_timeout(Duration::from_secs(1))
                    .with_request_timeout(Duration::from_secs(5)),
            ),
        )
        .tcp_control_plane("127.0.0.1:0".parse().unwrap())
        .run()
        .await
        .unwrap();
    let tcp = cluster.tcp_control_addr().expect("TCP control bound");
    let proxy = cluster.addr("api").unwrap();

    // Register a stub over the wire.
    let resp = rt(
        tcp,
        r#"{"type":"Stub","upstream":"api","method":"GET","path_pattern":"^/health$","status":200,"body":"ok"}"#,
    )
    .await;
    assert_eq!(resp["type"], "Ok");

    // The stub now serves the upstream.
    let body = http_client()
        .get(format!("http://{proxy}/health"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body, "ok");

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn tcp_query_traffic_returns_filtered_exchanges() {
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(50))
        .add_upstream(
            "api",
            ProxyConfig::http(
                "127.0.0.1:0".parse().unwrap(),
                UpstreamTarget::new(format!("http://{echo_addr}"))
                    .with_request_timeout(Duration::from_secs(5)),
            ),
        )
        .tcp_control_plane("127.0.0.1:0".parse().unwrap())
        .run()
        .await
        .unwrap();
    let tcp = cluster.tcp_control_addr().unwrap();
    let proxy = cluster.addr("api").unwrap();

    // Drive some traffic through the proxy.
    for path in ["/x", "/x", "/y"] {
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
    while cluster.recorder().len().await < 3 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let resp = rt(tcp, r#"{"type":"QueryTraffic","path_pattern":"^/x$"}"#).await;
    assert_eq!(resp["type"], "Exchanges");
    let exchanges = resp["exchanges"].as_array().expect("exchanges array");
    assert_eq!(exchanges.len(), 2);
    for e in exchanges {
        assert!(e["request"]["uri"].as_str().unwrap().contains("/x"));
    }

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn tcp_returns_error_on_invalid_json() {
    let cluster = ProxyClusterBuilder::new()
        .add_upstream(
            "api",
            ProxyConfig::http(
                "127.0.0.1:0".parse().unwrap(),
                UpstreamTarget::new("http://127.0.0.1:1"),
            ),
        )
        .tcp_control_plane("127.0.0.1:0".parse().unwrap())
        .run()
        .await
        .unwrap();
    let tcp = cluster.tcp_control_addr().unwrap();

    let resp = rt(tcp, "not json at all").await;
    assert_eq!(resp["type"], "Error");
    assert!(resp["message"].as_str().unwrap().contains("invalid JSON"));

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn tcp_pipelines_multiple_commands_per_connection() {
    let (echo_addr, _t) = spawn_echo().await;
    let cluster = ProxyClusterBuilder::new()
        .add_upstream(
            "api",
            ProxyConfig::http(
                "127.0.0.1:0".parse().unwrap(),
                UpstreamTarget::new(format!("http://{echo_addr}")),
            ),
        )
        .tcp_control_plane("127.0.0.1:0".parse().unwrap())
        .run()
        .await
        .unwrap();
    let tcp = cluster.tcp_control_addr().unwrap();

    let mut conn = TcpStream::connect(tcp).await.unwrap();
    conn.write_all(
        b"{\"type\":\"Stub\",\"upstream\":\"api\",\"path_pattern\":\"^/a$\",\"status\":200,\"body\":\"first\"}\n",
    )
    .await
    .unwrap();
    conn.write_all(b"{\"type\":\"ClearStubs\"}\n")
        .await
        .unwrap();
    conn.flush().await.unwrap();

    let (r, _w) = conn.into_split();
    let mut lines = BufReader::new(r).lines();
    let first: serde_json::Value =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    let second: serde_json::Value =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(first["type"], "Ok");
    assert_eq!(second["type"], "Ok");

    cluster.shutdown().await.unwrap();
}
