//! Error model — see `SPECIFICATION.md` §15.

use std::io;

/// The single error type returned by every fallible operation in the crate.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProxyError {
    /// A listener could not bind to its configured address.
    #[error("listener bind failed: {0}")]
    Bind(#[source] io::Error),

    /// Outbound TCP or TLS handshake — or request setup — failed before bytes
    /// could be exchanged with the upstream.
    #[error("upstream connection failed: {0}")]
    UpstreamConnect(String),

    /// Connection to the upstream was established, but the request or response
    /// then failed (timeout, mid-stream EOF, body collection error, etc.).
    #[error("upstream request failed: {0}")]
    UpstreamRequest(String),

    /// A middleware returned an error from `handle`.
    #[error("middleware error: {0}")]
    Middleware(String),

    /// The command channel was closed, a response channel was dropped, or a
    /// command was invalid in the current cluster state.
    #[error("command channel error: {0}")]
    Command(String),

    /// Recording persistence failed (NDJSON file write, flush, etc.).
    #[error("recording I/O failed: {0}")]
    Recording(#[source] io::Error),

    /// TLS configuration or PEM loading failed.
    #[error("TLS error: {0}")]
    Tls(String),

    /// A command referenced an upstream name that is not registered.
    #[error("unknown upstream: {0}")]
    UnknownUpstream(String),

    /// Shutdown sequence failed (listener join failure, etc.).
    #[error("shutdown failed: {0}")]
    Shutdown(String),

    /// Catch-all for converted external errors.
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl ProxyError {
    /// Wrap an arbitrary error into [`ProxyError::Other`].
    pub fn other<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Other(Box::new(err))
    }
}

/// Convenience alias used throughout the crate.
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
        // The display message is the variant's own message...
        assert_eq!(err.to_string(), "listener bind failed: port 80 busy");
        // ...and the underlying io::Error is exposed via `source()`.
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
