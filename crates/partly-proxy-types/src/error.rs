//! Error model — see `SPECIFICATION.md` §15.

use std::io;

/// Owned, type-erased error suitable for the `source` chain.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The single error type returned by every fallible operation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProxyError {
    #[error("listener bind failed: {0}")]
    Bind(#[source] io::Error),

    /// Outbound TCP/TLS handshake or request setup failed before any bytes
    /// were exchanged. `source()` typically resolves to a
    /// `hyper_util::client::legacy::Error`.
    #[error("upstream connection failed: {context}")]
    UpstreamConnect {
        context: String,
        #[source]
        source: Option<BoxError>,
    },

    /// Connection succeeded but the request or response then failed
    /// (timeout, mid-stream EOF, body collect error, etc.). `source()`
    /// preserves the concrete underlying error when one is available.
    #[error("upstream request failed: {context}")]
    UpstreamRequest {
        context: String,
        #[source]
        source: Option<BoxError>,
    },

    /// Returned by a middleware's `handle`. Display + source delegate to
    /// the wrapped error.
    #[error(transparent)]
    Middleware(BoxError),

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
    Other(BoxError),
}

impl ProxyError {
    pub fn other<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Other(Box::new(err))
    }

    pub fn middleware<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Middleware(Box::new(err))
    }

    pub fn upstream_connect(context: impl Into<String>) -> Self {
        Self::UpstreamConnect {
            context: context.into(),
            source: None,
        }
    }

    pub fn upstream_connect_with<E: std::error::Error + Send + Sync + 'static>(
        context: impl Into<String>,
        source: E,
    ) -> Self {
        Self::UpstreamConnect {
            context: context.into(),
            source: Some(Box::new(source)),
        }
    }

    pub fn upstream_request(context: impl Into<String>) -> Self {
        Self::UpstreamRequest {
            context: context.into(),
            source: None,
        }
    }

    pub fn upstream_request_with<E: std::error::Error + Send + Sync + 'static>(
        context: impl Into<String>,
        source: E,
    ) -> Self {
        Self::UpstreamRequest {
            context: context.into(),
            source: Some(Box::new(source)),
        }
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
    fn upstream_connect_preserves_source() {
        let io_err = io::Error::new(io::ErrorKind::ConnectionRefused, "refused");
        let err = ProxyError::upstream_connect_with("connect to http://x", io_err);
        assert_eq!(
            err.to_string(),
            "upstream connection failed: connect to http://x"
        );
        let source = std::error::Error::source(&err).expect("source attached");
        assert_eq!(source.to_string(), "refused");
    }

    #[test]
    fn upstream_connect_without_source_renders() {
        let err = ProxyError::upstream_connect("base_url missing scheme: //x");
        assert_eq!(
            err.to_string(),
            "upstream connection failed: base_url missing scheme: //x"
        );
        assert!(std::error::Error::source(&err).is_none());
    }

    #[test]
    fn middleware_takes_display_from_inner() {
        #[derive(Debug, thiserror::Error)]
        #[error("boom")]
        struct Boom;

        let err = ProxyError::middleware(Boom);
        // `#[error(transparent)]` — Display passes through.
        assert_eq!(err.to_string(), "boom");
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
