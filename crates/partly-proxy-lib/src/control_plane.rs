//! TCP JSON-Lines control plane adapter — see `SPECIFICATION.md` §12.2.
//!
//! Listens on a configured address, accepts one or more connections, and for
//! each newline-delimited JSON command dispatches via the in-process
//! [`CommandSender`] and writes a single JSON response line back. Each
//! connection is independent; clients can pipeline commands by writing
//! multiple lines.

use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::command::CommandSender;
use crate::error::{ProxyError, Result};
use crate::wire::{WireCommand, WireResponse};

/// Result of [`spawn_tcp_control_plane`].
pub(crate) struct RunningControlPlane {
    pub bound_addr: SocketAddr,
    pub task: JoinHandle<()>,
}

/// Bind on `addr` and spawn the accept loop. Returns the actual bound
/// address (port 0 is resolved on bind) and the join handle for the loop.
pub(crate) async fn spawn_tcp_control_plane(
    addr: SocketAddr,
    sender: CommandSender,
    shutdown: watch::Receiver<bool>,
) -> Result<RunningControlPlane> {
    let listener = TcpListener::bind(addr).await.map_err(ProxyError::Bind)?;
    let bound_addr = listener.local_addr().map_err(ProxyError::Bind)?;
    let task = tokio::spawn(accept_loop(listener, sender, shutdown));
    Ok(RunningControlPlane { bound_addr, task })
}

async fn accept_loop(
    listener: TcpListener,
    sender: CommandSender,
    mut shutdown: watch::Receiver<bool>,
) {
    tracing::debug!("TCP control plane accept loop started");
    loop {
        tokio::select! {
            biased;
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() {
                    tracing::debug!("TCP control plane shutting down");
                    return;
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        let sender = sender.clone();
                        let shutdown = shutdown.clone();
                        tokio::spawn(async move {
                            if let Err(e) = serve_connection(stream, sender, shutdown).await {
                                tracing::debug!(%peer, "TCP control connection error: {e}");
                            }
                        });
                    }
                    Err(e) => {
                        if !matches!(
                            e.kind(),
                            std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::ConnectionAborted
                                | std::io::ErrorKind::Interrupted
                        ) {
                            tracing::error!("fatal TCP control accept error: {e}");
                            return;
                        }
                        tracing::warn!("transient TCP control accept error: {e}");
                    }
                }
            }
        }
    }
}

async fn serve_connection(
    stream: TcpStream,
    sender: CommandSender,
    mut shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    loop {
        let next = tokio::select! {
            biased;
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            l = lines.next_line() => l?,
        };
        let Some(line) = next else { return Ok(()) };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let response = handle_line(trimmed, &sender).await;
        let mut buf = serde_json::to_vec(&response).unwrap_or_else(|_| {
            br#"{"type":"Error","message":"internal serialization failure"}"#.to_vec()
        });
        buf.push(b'\n');
        writer.write_all(&buf).await?;
        writer.flush().await?;
    }
}

async fn handle_line(line: &str, sender: &CommandSender) -> WireResponse {
    let wire: WireCommand = match serde_json::from_str(line) {
        Ok(c) => c,
        Err(e) => {
            return WireResponse::Error {
                message: format!("invalid JSON: {e}"),
            };
        }
    };
    let cmd = match wire.into_command() {
        Ok(c) => c,
        Err(e) => {
            return WireResponse::Error {
                message: e.to_string(),
            };
        }
    };
    match sender.send(cmd).await {
        Ok(resp) => WireResponse::from_response(resp),
        Err(e) => WireResponse::Error {
            message: e.to_string(),
        },
    }
}
