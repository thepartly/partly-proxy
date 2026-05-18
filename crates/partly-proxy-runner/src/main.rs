//! `partly-proxy-runner` — host the proxy + health endpoints.
//!
//! Configuration via environment variables (see `RunnerOptions::from_env`).
//! Logging via `RUST_LOG`. Graceful shutdown on Ctrl+C.

use partly_proxy_runner::{run, RunnerOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let opts = RunnerOptions::from_env()?;
    tracing::info!(?opts.proxy_bind, ?opts.health_bind, upstream = %opts.upstream_url, "starting partly-proxy-runner");

    let runner = run(opts).await?;
    tracing::info!(proxy = %runner.proxy_addr, health = %runner.health_addr, "ready");

    tokio::signal::ctrl_c().await.ok();
    tracing::info!("Ctrl+C received; shutting down");
    runner.shutdown().await?;
    Ok(())
}
