//! On-the-wire data model for recorded exchanges — see `SPECIFICATION.md` §9.1.

use std::{collections::BTreeMap, time::Duration};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use http::{HeaderMap, Method, Response, StatusCode, Uri};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::hash::sha256_hex;

/// One recorded request.
///
/// `body_sha256` is computed *after* any snapshot-boundary redaction
/// (§6.4) — it's the match key for `MethodUriAndBodyHash` replay lookups.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordedRequest {
    pub method: String,
    pub uri: String,
    pub headers: Vec<(String, String)>,
    #[serde(with = "crate::encoding::base64_bytes")]
    pub body: Bytes,
    pub body_sha256: String,
}

impl RecordedRequest {
    pub fn from_parts(method: &Method, uri: &Uri, headers: &HeaderMap, body: Bytes) -> Self {
        let body_sha256 = sha256_hex(&body);
        Self {
            method: method.as_str().to_owned(),
            uri: uri.to_string(),
            headers: header_pairs(headers),
            body,
            body_sha256,
        }
    }

    /// Rehash after mutating `body` in place (e.g. snapshot redaction).
    pub fn recompute_hash(&mut self) {
        self.body_sha256 = sha256_hex(&self.body);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordedResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    #[serde(with = "crate::encoding::base64_bytes")]
    pub body: Bytes,
}

impl RecordedResponse {
    pub fn from_parts(status: StatusCode, headers: &HeaderMap, body: Bytes) -> Self {
        Self {
            status: status.as_u16(),
            headers: header_pairs(headers),
            body,
        }
    }

    pub fn status(&self) -> StatusCode {
        StatusCode::from_u16(self.status).unwrap_or(StatusCode::BAD_GATEWAY)
    }

    pub fn from_hyper(resp: &Response<Bytes>) -> Self {
        Self::from_parts(resp.status(), resp.headers(), resp.body().clone())
    }
}

/// Outcome of an exchange — a recorded response, or a transport-level error.
///
/// A non-2xx response is still `Response(...)`; only transport failures
/// (connect refused, body truncated, etc.) land as `Error { ... }`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExchangeOutcome {
    Response(RecordedResponse),
    Error { message: String },
}

impl ExchangeOutcome {
    pub fn as_response(&self) -> Option<&RecordedResponse> {
        match self {
            Self::Response(r) => Some(r),
            Self::Error { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordedExchange {
    pub id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream: Option<String>,
    pub timestamp: DateTime<Utc>,
    /// Serialised as integer `duration_ms` for cross-language friendliness.
    #[serde(rename = "duration_ms", with = "crate::encoding::duration_ms")]
    pub duration: Duration,
    pub request: RecordedRequest,
    pub outcome: ExchangeOutcome,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

impl RecordedExchange {
    /// Construct with a fresh UUID and wall-clock timestamp. Tests typically
    /// build the struct literal directly.
    pub fn new(
        upstream: Option<String>,
        request: RecordedRequest,
        outcome: ExchangeOutcome,
        duration: Duration,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            upstream,
            timestamp: Utc::now(),
            duration,
            request,
            outcome,
            labels: BTreeMap::new(),
        }
    }
}

fn header_pairs(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|(k, v)| {
            let value = v
                .to_str()
                .map_or_else(|_| "<binary>".to_owned(), str::to_owned);
            (k.as_str().to_owned(), value)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use http::header::HeaderValue;

    use super::*;

    #[test]
    fn recorded_request_hashes_body_on_construct() {
        let req = RecordedRequest::from_parts(
            &Method::POST,
            &"/orders".parse().unwrap(),
            &HeaderMap::new(),
            Bytes::from_static(b"hello"),
        );
        assert_eq!(req.body_sha256, sha256_hex(b"hello"));
        assert_eq!(req.method, "POST");
        assert_eq!(req.uri, "/orders");
    }

    #[test]
    fn header_pairs_stringify_binary_values() {
        let mut h = HeaderMap::new();
        h.insert("ok", HeaderValue::from_static("yes"));
        h.insert(
            "bin",
            HeaderValue::from_bytes(b"\xff\xfe").expect("HeaderValue accepts bytes"),
        );
        let pairs = header_pairs(&h);
        assert!(pairs.contains(&("ok".to_owned(), "yes".to_owned())));
        assert!(pairs.contains(&("bin".to_owned(), "<binary>".to_owned())));
    }

    #[test]
    fn roundtrips_through_json() {
        let req = RecordedRequest::from_parts(
            &Method::POST,
            &"/orders".parse().unwrap(),
            &HeaderMap::new(),
            Bytes::from_static(b"{}"),
        );
        let resp = RecordedResponse {
            status: 201,
            headers: vec![("content-type".to_owned(), "application/json".to_owned())],
            body: Bytes::from_static(b"{\"ok\":true}"),
        };
        let ex = RecordedExchange {
            id: Uuid::new_v4(),
            upstream: Some("api".to_owned()),
            timestamp: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            duration: Duration::from_millis(125),
            request: req,
            outcome: ExchangeOutcome::Response(resp),
            labels: {
                let mut m = BTreeMap::new();
                m.insert("test".to_owned(), "roundtrip".to_owned());
                m
            },
        };

        let json = serde_json::to_string(&ex).unwrap();
        let back: RecordedExchange = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ex);
    }

    #[test]
    fn outcome_error_round_trips() {
        let ex = RecordedExchange {
            id: Uuid::nil(),
            upstream: None,
            timestamp: DateTime::from_timestamp(0, 0).unwrap(),
            duration: Duration::from_millis(0),
            request: RecordedRequest::from_parts(
                &Method::GET,
                &"/".parse().unwrap(),
                &HeaderMap::new(),
                Bytes::new(),
            ),
            outcome: ExchangeOutcome::Error {
                message: "boom".to_owned(),
            },
            labels: BTreeMap::new(),
        };
        let json = serde_json::to_string(&ex).unwrap();
        assert!(json.contains("\"kind\":\"error\""));
        let back: RecordedExchange = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ex);
    }

    #[test]
    fn recompute_hash_picks_up_body_changes() {
        let mut req = RecordedRequest::from_parts(
            &Method::GET,
            &"/".parse().unwrap(),
            &HeaderMap::new(),
            Bytes::from_static(b"a"),
        );
        let before = req.body_sha256.clone();
        req.body = Bytes::from_static(b"b");
        req.recompute_hash();
        assert_ne!(before, req.body_sha256);
        assert_eq!(req.body_sha256, sha256_hex(b"b"));
    }
}
