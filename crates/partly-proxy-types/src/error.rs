//! Error model — see `SPECIFICATION.md` §15.

use std::io;

/// The single error type returned by every fallible operation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProxyError {
    #[error("listener bind failed: {0}")]
    Bind(#[source] io::Error),

    /// Outbound TCP/TLS handshake or request setup failed before any bytes
    /// were exchanged.
    #[error("upstream connection failed: {0}")]
    UpstreamConnect(String),

    /// Connection succeeded but the request or response then failed
    /// (timeout, mid-stream EOF, body collect error, etc.).
    #[error("upstream request failed: {0}")]
    UpstreamRequest(String),

    #[error("middleware error: {0}")]
    Middleware(String),

    #[error("command channel error: {0}")]
    Command(String),

    #[error("recording I/O failed: {0}")]
    Recording(#[source] io::Error),

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("unknown upstream: {0}")]
    UnknownUpstream(String),

    #[error("shutdown failed: {0}")]
    Shutdown(String),

    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl ProxyError {
    pub fn other<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Other(Box::new(err))
    }
}

pub type Result<T, E = ProxyError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_upstream_renders() {
        let err = ProxyError::UnknownUpstream("billing".into());
        assert_eq!(err.to_string(), "unknown upstream: billing");
    }

    #[test]
    fn bind_keeps_source_chain() {
        let io_err = io::Error::new(io::ErrorKind::AddrInUse, "port 80 busy");
        let err = ProxyError::Bind(io_err);
        assert_eq!(err.to_string(), "listener bind failed: port 80 busy");
        let source = std::error::Error::source(&err).expect("source attached");
        assert_eq!(source.to_string(), "port 80 busy");
    }

    #[test]
    fn other_wraps_arbitrary_error() {
        #[derive(Debug, thiserror::Error)]
        #[error("custom")]
        struct Custom;

        let err = ProxyError::other(Custom);
        assert!(matches!(err, ProxyError::Other(_)));
        assert_eq!(err.to_string(), "custom");
    }
}
