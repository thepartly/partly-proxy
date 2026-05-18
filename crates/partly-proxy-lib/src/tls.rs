//! TLS helpers — outbound `rustls::ClientConfig` and inbound `TlsAcceptor`.
//! See `SPECIFICATION.md` §11.
//!
//! The crypto provider (`ring`) is installed lazily by [`ensure_crypto_installed`]
//! the first time TLS configuration is constructed, so callers don't need to
//! arrange a one-shot at startup.

use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::ServerConfig;
use rustls::{ClientConfig, DigitallySignedStruct, Error as RustlsError, RootCertStore};
use tokio_rustls::TlsAcceptor;

use crate::config::{InboundTlsConfig, UpstreamTlsConfig};
use crate::error::{ProxyError, Result};

/// Idempotently install the `ring` crypto provider as the rustls default.
/// rustls 0.23 requires an explicit provider before any config is built; if
/// callers have already installed one (e.g. `aws-lc-rs`), the first install
/// wins and ours is silently dropped.
pub(crate) fn ensure_crypto_installed() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // Best-effort: ignore the result, since another caller may already
        // have installed a provider.
        let _ = rustls::crypto::CryptoProvider::install_default(
            rustls::crypto::ring::default_provider(),
        );
    });
}

/// Build the outbound `ClientConfig` for one upstream. Honours
/// `accept_invalid_certs` (replaces verification with a permissive verifier)
/// and `custom_ca_cert` (appended to the Mozilla root store).
pub(crate) fn build_client_config(tls: Option<&UpstreamTlsConfig>) -> Result<ClientConfig> {
    ensure_crypto_installed();
    let builder = ClientConfig::builder();

    let config = if tls.is_some_and(|t| t.accept_invalid_certs) {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoopVerifier))
            .with_no_client_auth()
    } else {
        let mut roots = RootCertStore::empty();
        // Bake-in Mozilla roots (webpki-roots). The iterator yields the same
        // certs the spec calls out as the default trust anchors.
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        if let Some(cfg) = tls {
            if let Some(path) = &cfg.custom_ca_cert {
                let pem = std::fs::read(path).map_err(|e| {
                    ProxyError::Tls(format!("read custom_ca_cert at {}: {e}", path.display()))
                })?;
                let mut slice = pem.as_slice();
                for cert in rustls_pemfile::certs(&mut slice) {
                    let cert =
                        cert.map_err(|e| ProxyError::Tls(format!("parse custom_ca_cert: {e}")))?;
                    roots
                        .add(cert)
                        .map_err(|e| ProxyError::Tls(format!("add custom_ca_cert: {e}")))?;
                }
            }
        }
        builder.with_root_certificates(roots).with_no_client_auth()
    };

    Ok(config)
}

/// Build the inbound `TlsAcceptor` for one listener.
pub(crate) fn build_tls_acceptor(cfg: &InboundTlsConfig) -> Result<TlsAcceptor> {
    ensure_crypto_installed();
    let certs = load_certs(&cfg.cert_path)?;
    let key = load_private_key(&cfg.key_path)?;
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| ProxyError::Tls(format!("invalid cert/key pair: {e}")))?;
    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let pem = std::fs::read(path)
        .map_err(|e| ProxyError::Tls(format!("read cert at {}: {e}", path.display())))?;
    rustls_pemfile::certs(&mut pem.as_slice())
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| ProxyError::Tls(format!("parse cert chain at {}: {e}", path.display())))
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let pem = std::fs::read(path)
        .map_err(|e| ProxyError::Tls(format!("read key at {}: {e}", path.display())))?;
    rustls_pemfile::private_key(&mut pem.as_slice())
        .map_err(|e| ProxyError::Tls(format!("parse private key at {}: {e}", path.display())))?
        .ok_or_else(|| ProxyError::Tls(format!("no private key in PEM at {}", path.display())))
}

/// Permissive `ServerCertVerifier` used when `accept_invalid_certs = true`.
/// Accepts every certificate without checking the chain — only for testing
/// against self-signed upstreams.
#[derive(Debug)]
struct NoopVerifier;

impl rustls::client::danger::ServerCertVerifier for NoopVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, RustlsError> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, RustlsError> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, RustlsError> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        use rustls::SignatureScheme as S;
        vec![
            S::RSA_PKCS1_SHA256,
            S::RSA_PKCS1_SHA384,
            S::RSA_PKCS1_SHA512,
            S::ECDSA_NISTP256_SHA256,
            S::ECDSA_NISTP384_SHA384,
            S::RSA_PSS_SHA256,
            S::RSA_PSS_SHA384,
            S::RSA_PSS_SHA512,
            S::ED25519,
        ]
    }
}
