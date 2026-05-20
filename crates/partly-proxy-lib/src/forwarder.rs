//! Outbound forwarder — owns the hyper-util client and the URI-rewrite rules
//! for one upstream. See `SPECIFICATION.md` §10 + §11.1.
//!
//! Plain HTTP and HTTPS upstreams use the same `HttpsConnector<HttpConnector>`,
//! configured per-upstream from [`UpstreamTlsConfig`]. The scheme of
//! `base_url` decides which is actually used; the connector serves both.

use std::time::Duration;

use bytes::Bytes;
use http::{
    HeaderValue, Request, Uri,
    header::HOST,
    uri::{Authority, PathAndQuery, Scheme},
};
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnector;
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use partly_proxy_types::{ProxyError, Result};

use crate::{
    config::UpstreamTarget,
    proxy_io::{ProxyRequest, ProxyResponse},
    tls::build_client_config,
};

type Connector = HttpsConnector<HttpConnector>;
type HyperClient = Client<Connector, Full<Bytes>>;

/// Per-upstream outbound client and pre-parsed base URI.
pub(crate) struct Forwarder {
    client: HyperClient,
    target: UpstreamTarget,
    base: BaseUri,
}

#[derive(Debug, Clone)]
struct BaseUri {
    scheme: Scheme,
    authority: Authority,
    /// Path prefix from the base URL, with any trailing `/` stripped.
    /// May be empty.
    path_prefix: String,
}

impl Forwarder {
    pub(crate) fn new(target: UpstreamTarget) -> Result<Self> {
        let base = parse_base_url(&target.base_url)?;

        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_connect_timeout(Some(target.connect_timeout));

        let tls_config = build_client_config(target.tls.as_ref())?;
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls_config)
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .wrap_connector(http);

        let client = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(30))
            .build(connector);

        Ok(Self {
            client,
            target,
            base,
        })
    }

    /// Forward a `ProxyRequest` to the upstream and return a `ProxyResponse`.
    /// Connect failures map to `UpstreamConnect`; timeouts and post-handshake
    /// failures map to `UpstreamRequest`.
    pub(crate) async fn forward(&self, mut req: ProxyRequest) -> Result<ProxyResponse> {
        let outbound_uri = self.build_outbound_uri(&req.uri)?;

        // Recompute the Host header. hyper-util will set one based on the
        // outbound URI authority if none is supplied, but the spec also lets
        // callers override it explicitly via `host_header`.
        if let Some(override_host) = &self.target.host_header {
            let value = HeaderValue::from_str(override_host).map_err(|e| {
                ProxyError::upstream_request_with("invalid host_header override", e)
            })?;
            req.headers.insert(HOST, value);
        } else {
            // Drop the inbound Host header so hyper computes one from the
            // outbound authority; otherwise a Host pointing at the proxy
            // would be forwarded verbatim and confuse the upstream.
            req.headers.remove(HOST);
        }

        let mut builder = Request::builder()
            .method(req.method.clone())
            .uri(outbound_uri.clone())
            .version(req.version);

        if let Some(headers) = builder.headers_mut() {
            *headers = req.headers.clone();
            // hyper-util sets Content-Length from the Full<Bytes> body. If
            // middleware has rewritten the body, the original Content-Length
            // is stale and would clash with hyper's recomputed value.
            headers.remove(http::header::CONTENT_LENGTH);
            headers.remove(http::header::TRANSFER_ENCODING);
        }

        let outbound = builder
            .body(Full::new(req.body))
            .map_err(|e| ProxyError::upstream_request_with("request build failed", e))?;

        let fut = self.client.request(outbound);
        let resp = tokio::time::timeout(self.target.request_timeout, fut)
            .await
            .map_err(|_| {
                ProxyError::upstream_request(format!(
                    "request to {outbound_uri} timed out after {:?}",
                    self.target.request_timeout
                ))
            })?
            .map_err(|e| classify_legacy_error(&outbound_uri, e))?;

        let (resp_parts, resp_body) = resp.into_parts();
        let collected = resp_body
            .collect()
            .await
            .map_err(|e| ProxyError::upstream_request_with("response body read failed", e))?
            .to_bytes();

        Ok(ProxyResponse {
            status: resp_parts.status,
            headers: resp_parts.headers,
            body: collected,
            version: resp_parts.version,
        })
    }

    fn build_outbound_uri(&self, inbound: &Uri) -> Result<Uri> {
        let path_and_query = inbound.path_and_query().map_or("/", PathAndQuery::as_str);
        let combined_path = format!("{}{}", self.base.path_prefix, path_and_query);

        Uri::builder()
            .scheme(self.base.scheme.clone())
            .authority(self.base.authority.clone())
            .path_and_query(combined_path)
            .build()
            .map_err(|e| ProxyError::upstream_request_with("URI build failed", e))
    }
}

fn parse_base_url(s: &str) -> Result<BaseUri> {
    let uri: Uri = s.parse().map_err(ProxyError::other)?;
    let scheme = uri
        .scheme()
        .cloned()
        .ok_or_else(|| ProxyError::upstream_connect(format!("base_url missing scheme: {s}")))?;
    if scheme != Scheme::HTTP && scheme != Scheme::HTTPS {
        return Err(ProxyError::upstream_connect(format!(
            "unsupported scheme {scheme} in base_url: {s}"
        )));
    }
    let authority = uri
        .authority()
        .cloned()
        .ok_or_else(|| ProxyError::upstream_connect(format!("base_url missing authority: {s}")))?;
    let path_prefix = uri.path().trim_end_matches('/').to_owned();
    Ok(BaseUri {
        scheme,
        authority,
        path_prefix,
    })
}

fn classify_legacy_error(outbound_uri: &Uri, e: hyper_util::client::legacy::Error) -> ProxyError {
    if e.is_connect() {
        ProxyError::upstream_connect_with(format!("connect to {outbound_uri}"), e)
    } else {
        ProxyError::upstream_request_with(format!("request to {outbound_uri}"), e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(base: &str) -> UpstreamTarget {
        UpstreamTarget::new(base)
    }

    #[test]
    fn parses_plain_base_url() {
        let b = parse_base_url("http://upstream:9000").unwrap();
        assert_eq!(b.scheme, Scheme::HTTP);
        assert_eq!(b.authority.as_str(), "upstream:9000");
        assert_eq!(b.path_prefix, "");
    }

    #[test]
    fn keeps_path_prefix_without_trailing_slash() {
        let b = parse_base_url("http://upstream/v1").unwrap();
        assert_eq!(b.path_prefix, "/v1");
    }

    #[test]
    fn strips_trailing_slash_from_path_prefix() {
        let b = parse_base_url("http://upstream/v1/").unwrap();
        assert_eq!(b.path_prefix, "/v1");
    }

    #[test]
    fn https_base_url_is_accepted() {
        let b = parse_base_url("https://upstream").unwrap();
        assert_eq!(b.scheme, Scheme::HTTPS);
    }

    #[test]
    fn rejects_missing_scheme() {
        let e = parse_base_url("//upstream").unwrap_err();
        assert!(
            matches!(e, ProxyError::Other(_) | ProxyError::UpstreamConnect { .. }),
            "got {e:?}"
        );
    }

    #[tokio::test]
    async fn build_outbound_uri_combines_prefix_and_inbound_path() {
        let fwd = Forwarder::new(target("http://upstream/v1")).unwrap();
        let inbound: Uri = "/orders/123?x=1".parse().unwrap();
        let out = fwd.build_outbound_uri(&inbound).unwrap();
        assert_eq!(out.to_string(), "http://upstream/v1/orders/123?x=1");
    }

    #[tokio::test]
    async fn build_outbound_uri_handles_empty_path() {
        let fwd = Forwarder::new(target("http://upstream")).unwrap();
        let inbound: Uri = "/".parse().unwrap();
        let out = fwd.build_outbound_uri(&inbound).unwrap();
        assert_eq!(out.to_string(), "http://upstream/");
    }
}
