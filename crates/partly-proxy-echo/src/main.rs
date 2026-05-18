//! `partly-proxy-echo` — bind on an address and serve the echo handler.
//!
//! Used as the test upstream by the proxy library's Docker-based smoke tests.

use std::net::SocketAddr;

use partly_proxy_echo::{bind, serve};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let addr_str = std::env::var("ECHO_BIND").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let addr: SocketAddr = addr_str
        .parse()
        .unwrap_or_else(|e| panic!("invalid ECHO_BIND {addr_str:?}: {e}"));

    let (bound, listener) = bind(addr).await?;
    tracing::info!(%bound, "echo upstream listening");

    serve(listener).await
}
