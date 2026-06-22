//! Plain-HTTP forwarding from a real listener to a real (in-process)
//! echo upstream.
//!
//! Every test starts the echo server on an ephemeral port, builds a single
//! proxy listener pointed at that echo, then exercises behaviour through a
//! reqwest client. We never reach the public internet — the upstream is
//! always in the same tokio runtime.

use std::time::Duration;

use partly_proxy_lib::{ClusterHandle, ProxyClusterBuilder, ProxyConfig, UpstreamTarget};

mod common;
use common::{cfg, http_client, spawn_echo, unreachable_addr};

/// Spawn the proxy in front of an upstream URL and return the cluster handle.
async fn spawn_proxy(upstream_url: String) -> ClusterHandle {
    ProxyClusterBuilder::new()
        .add_upstream("upstream", cfg(upstream_url))
        .run()
        .await
        .expect("cluster builds")
}

#[tokio::test]
async fn forwards_get_to_upstream_and_returns_body() {
    let (echo_addr, _echo_task) = spawn_echo().await;
    let cluster = spawn_proxy(format!("http://{echo_addr}")).await;
    let proxy_addr = cluster.addr("upstream").expect("upstream addr");

    let resp = http_client()
        .get(format!("http://{proxy_addr}/hello/world?x=1"))
        .header("x-test", "abc")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["method"], "GET");
    assert_eq!(body["path"], "/hello/world");
    assert_eq!(body["query"], "x=1");

    let headers = body["headers"].as_array().expect("headers array");
    let has_x_test = headers
        .iter()
        .any(|h| h[0].as_str() == Some("x-test") && h[1].as_str() == Some("abc"));
    assert!(has_x_test, "x-test header reached upstream: {headers:#?}");

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn forwards_post_body_unchanged() {
    let (echo_addr, _echo_task) = spawn_echo().await;
    let cluster = spawn_proxy(format!("http://{echo_addr}")).await;
    let proxy_addr = cluster.addr("upstream").unwrap();

    let payload = serde_json::json!({"hello": "world", "n": 42});
    let resp = http_client()
        .post(format!("http://{proxy_addr}/orders"))
        .json(&payload)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["method"], "POST");
    assert_eq!(body["path"], "/orders");
    assert_eq!(body["body"]["encoding"], "utf8");
    let value = body["body"]["value"].as_str().unwrap();
    let echoed: serde_json::Value = serde_json::from_str(value).unwrap();
    assert_eq!(echoed, payload);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn upstream_status_is_proxied_verbatim() {
    let (echo_addr, _echo_task) = spawn_echo().await;
    let cluster = spawn_proxy(format!("http://{echo_addr}")).await;
    let proxy_addr = cluster.addr("upstream").unwrap();

    let resp = http_client()
        .get(format!("http://{proxy_addr}/_status/418"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 418);
    let txt = resp.text().await.unwrap();
    assert_eq!(txt, "status=418");

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn unreachable_upstream_yields_502() {
    let unreachable = unreachable_addr();
    let cluster = spawn_proxy(format!("http://{unreachable}")).await;
    let proxy_addr = cluster.addr("upstream").unwrap();

    let resp = http_client()
        .get(format!("http://{proxy_addr}/anything"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 502);
    assert_eq!(
        resp.headers()
            .get("x-proxy-error")
            .map(|v| v.to_str().unwrap()),
        Some("upstream-connect")
    );

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn host_header_override_reaches_upstream() {
    let (echo_addr, _echo_task) = spawn_echo().await;
    let cfg = ProxyConfig::http(
        "127.0.0.1:0".parse().unwrap(),
        UpstreamTarget::new(format!("http://{echo_addr}"))
            .with_host_header("internal.example")
            .with_connect_timeout(Duration::from_secs(1))
            .with_request_timeout(Duration::from_secs(5)),
    );
    let cluster = ProxyClusterBuilder::new()
        .add_upstream("upstream", cfg)
        .run()
        .await
        .unwrap();
    let proxy_addr = cluster.addr("upstream").unwrap();

    let body: serde_json::Value = http_client()
        .get(format!("http://{proxy_addr}/"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let headers = body["headers"].as_array().expect("headers array");
    let host_value = headers
        .iter()
        .find(|h| h[0].as_str() == Some("host"))
        .and_then(|h| h[1].as_str())
        .expect("host header forwarded");
    assert_eq!(host_value, "internal.example");

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn path_prefix_in_base_url_is_prepended() {
    let (echo_addr, _echo_task) = spawn_echo().await;
    let cluster = spawn_proxy(format!("http://{echo_addr}/api/v1")).await;
    let proxy_addr = cluster.addr("upstream").unwrap();

    let body: serde_json::Value = http_client()
        .get(format!("http://{proxy_addr}/users/7"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["path"], "/api/v1/users/7");

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_unblocks_in_flight_listener() {
    let (echo_addr, _echo_task) = spawn_echo().await;
    let cluster = spawn_proxy(format!("http://{echo_addr}")).await;
    let names = cluster.upstream_names();
    assert_eq!(names, vec!["upstream"]);
    cluster.shutdown().await.unwrap();
}
