//! Inbound and outbound TLS coverage.
//!
//! Test certs are generated on the fly with `rcgen` so the suite has no
//! external file dependencies. Each test writes the PEMs to a `tempfile::tempdir`
//! and points the proxy / upstream at those paths.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use partly_proxy_lib::{
    InboundTlsConfig, ProxyClusterBuilder, ProxyConfig, UpstreamTarget, UpstreamTlsConfig,
};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

/// Materialised test certificates written to a tempdir.
struct TestCerts {
    _dir: TempDir,
    ca_cert_pem: PathBuf,
    leaf_cert_pem: PathBuf,
    leaf_key_pem: PathBuf,
}

fn ensure_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::CryptoProvider::install_default(
            rustls::crypto::ring::default_provider(),
        );
    });
}

fn make_test_certs(subject: &str) -> TestCerts {
    ensure_crypto_provider();
    let dir = tempfile::tempdir().unwrap();

    // Root CA.
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.distinguished_name = {
        let mut dn = rcgen::DistinguishedName::new();
        dn.push(rcgen::DnType::CommonName, "partly-proxy test CA");
        dn
    };
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    // Leaf cert signed by the CA.
    let mut leaf_params = rcgen::CertificateParams::new(vec![subject.to_string()]).unwrap();
    leaf_params.distinguished_name = {
        let mut dn = rcgen::DistinguishedName::new();
        dn.push(rcgen::DnType::CommonName, subject);
        dn
    };
    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();

    let ca_path = dir.path().join("ca.pem");
    let leaf_path = dir.path().join("leaf.pem");
    let leaf_key_path = dir.path().join("leaf.key.pem");
    std::fs::write(&ca_path, ca_cert.pem()).unwrap();
    std::fs::write(&leaf_path, leaf_cert.pem()).unwrap();
    std::fs::write(&leaf_key_path, leaf_key.serialize_pem()).unwrap();

    TestCerts {
        _dir: dir,
        ca_cert_pem: ca_path,
        leaf_cert_pem: leaf_path,
        leaf_key_pem: leaf_key_path,
    }
}

/// Spawn a tiny HTTPS upstream that returns "https-ok" for every request.
async fn spawn_https_upstream(certs: &TestCerts) -> (SocketAddr, JoinHandle<()>) {
    ensure_crypto_provider();
    let cert_chain: Vec<CertificateDer<'static>> =
        CertificateDer::pem_file_iter(&certs.leaf_cert_pem)
            .unwrap()
            .map(|c| c.unwrap())
            .collect();
    let key = PrivateKeyDer::from_pem_file(&certs.leaf_key_pem).unwrap();
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_config));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let task = tokio::spawn(async move {
        loop {
            let Ok((tcp, _peer)) = listener.accept().await else {
                continue;
            };
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(tls) = acceptor.accept(tcp).await else {
                    return;
                };
                let io = TokioIo::new(tls);
                let svc = service_fn(|_req: Request<Incoming>| async move {
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .body(Full::new(Bytes::from_static(b"https-ok")))
                            .unwrap(),
                    )
                });
                let _ = auto::Builder::new(TokioExecutor::new())
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });

    (addr, task)
}

fn proxy_config(upstream_url: String, tls: Option<UpstreamTlsConfig>) -> ProxyConfig {
    let mut target = UpstreamTarget::new(upstream_url)
        .with_connect_timeout(Duration::from_secs(2))
        .with_request_timeout(Duration::from_secs(5));
    if let Some(t) = tls {
        target = target.with_tls(t);
    }
    ProxyConfig::http("127.0.0.1:0".parse().unwrap(), target)
}

#[tokio::test]
async fn outbound_https_with_custom_ca_succeeds() {
    let certs = make_test_certs("localhost");
    let (upstream_addr, _t) = spawn_https_upstream(&certs).await;

    let cluster = ProxyClusterBuilder::new()
        .add_upstream(
            "api",
            proxy_config(
                format!("https://localhost:{}", upstream_addr.port()),
                Some(UpstreamTlsConfig {
                    accept_invalid_certs: false,
                    custom_ca_cert: Some(certs.ca_cert_pem.clone()),
                }),
            ),
        )
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let body = client
        .get(format!("http://{proxy}/anything"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body, "https-ok");

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn outbound_https_without_trust_fails() {
    let certs = make_test_certs("localhost");
    let (upstream_addr, _t) = spawn_https_upstream(&certs).await;

    // No custom CA, no accept_invalid_certs — system roots don't trust our test CA.
    let cluster = ProxyClusterBuilder::new()
        .add_upstream(
            "api",
            proxy_config(format!("https://localhost:{}", upstream_addr.port()), None),
        )
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let resp = client
        .get(format!("http://{proxy}/anything"))
        .send()
        .await
        .unwrap();
    // The forward fails → proxy returns 502.
    assert_eq!(resp.status(), 502);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn outbound_https_with_accept_invalid_certs_succeeds() {
    let certs = make_test_certs("localhost");
    let (upstream_addr, _t) = spawn_https_upstream(&certs).await;

    let cluster = ProxyClusterBuilder::new()
        .add_upstream(
            "api",
            proxy_config(
                format!("https://localhost:{}", upstream_addr.port()),
                Some(UpstreamTlsConfig {
                    accept_invalid_certs: true,
                    custom_ca_cert: None,
                }),
            ),
        )
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let body = client
        .get(format!("http://{proxy}/anything"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body, "https-ok");

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn inbound_tls_serves_requests_over_https() {
    let certs = make_test_certs("localhost");

    // Plain-HTTP echo upstream so the proxy has something to forward to.
    let (echo_addr, echo_listener) = partly_proxy_echo::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let _echo_task = tokio::spawn(async move {
        let _ = partly_proxy_echo::serve(echo_listener).await;
    });

    let cfg = ProxyConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        upstream: UpstreamTarget::new(format!("http://{echo_addr}"))
            .with_connect_timeout(Duration::from_secs(2))
            .with_request_timeout(Duration::from_secs(5)),
        inbound_tls: Some(InboundTlsConfig {
            cert_path: certs.leaf_cert_pem.clone(),
            key_path: certs.leaf_key_pem.clone(),
        }),
    };

    let cluster = ProxyClusterBuilder::new()
        .add_upstream("api", cfg)
        .run()
        .await
        .unwrap();
    let proxy = cluster.addr("api").unwrap();

    // Build a reqwest client that trusts our test CA so HTTPS to the proxy works.
    let ca_pem = std::fs::read(&certs.ca_cert_pem).unwrap();
    let ca = reqwest::Certificate::from_pem(&ca_pem).unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(ca)
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    // The proxy is bound on 127.0.0.1 but our leaf cert names "localhost",
    // so target via the SAN. The hostname needs to match what the cert
    // attests; the port is whatever the proxy bound.
    let url = format!("https://localhost:{}/check", proxy.port());
    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["path"], "/check");

    cluster.shutdown().await.unwrap();
}
