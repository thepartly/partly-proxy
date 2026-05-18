//! On-the-wire data model for recorded exchanges — see `SPECIFICATION.md` §9.1.
//!
//! These types are owned (no borrows), `Serialize` + `Deserialize`, and
//! round-trip cleanly through NDJSON. The recorder stores them; replay
//! sources read them back; the JSON-Lines control plane carries them in
//! response payloads.

use std::{collections::BTreeMap, time::Duration};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use http::{HeaderMap, Method, Response, StatusCode, Uri};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// One recorded request — see §9.1.
///
/// `body_sha256` is the lowercase hex SHA-256 of `body` *after* any
/// snapshot-boundary redaction has been applied (§6.4). The hash is the
/// match key for `MethodPathAndBodyHash` replay lookups.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordedRequest {
    pub method: String,
    pub uri: String,
    pub headers: Vec<(String, String)>,
    #[serde(with = "base64_bytes")]
    pub body: Bytes,
    pub body_sha256: String,
}

impl RecordedRequest {
    /// Build a recorded request from its raw parts and compute the body hash.
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

    /// Rehash the body — used after mutating `body` in place, e.g. when
    /// applying snapshot-boundary redaction.
    pub fn recompute_hash(&mut self) {
        self.body_sha256 = sha256_hex(&self.body);
    }
}

/// One recorded response — see §9.1.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordedResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    #[serde(with = "base64_bytes")]
    pub body: Bytes,
}

impl RecordedResponse {
    /// Build a recorded response from a fully-collected hyper response.
    pub fn from_parts(status: StatusCode, headers: &HeaderMap, body: Bytes) -> Self {
        Self {
            status: status.as_u16(),
            headers: header_pairs(headers),
            body,
        }
    }

    /// Convenience: drop the body and return just the status. Useful for
    /// `TrafficFilter::status` matching without cloning bytes.
    pub fn status(&self) -> StatusCode {
        StatusCode::from_u16(self.status).unwrap_or(StatusCode::BAD_GATEWAY)
    }

    /// Build a `RecordedResponse` from a hyper `Response<Bytes>` (the shape
    /// the forwarder returns).
    pub fn from_hyper(resp: &Response<Bytes>) -> Self {
        Self::from_parts(resp.status(), resp.headers(), resp.body().clone())
    }
}

/// Either a recorded response or a stringified error description — see §9.1.
///
/// The error case is distinct from "non-2xx response" — a 500 response *is*
/// an `Outcome::Response(...)`; only transport-level failures (connection
/// refused, body read truncated, etc.) become `Outcome::Error(...)`.
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

/// One complete recorded exchange — see §9.1.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordedExchange {
    pub id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream: Option<String>,
    pub timestamp: DateTime<Utc>,
    /// Wall-clock duration of the exchange. Serialised as `duration_ms`
    /// (an integer millisecond count) for cross-language friendliness.
    #[serde(rename = "duration_ms", with = "duration_ms")]
    pub duration: Duration,
    pub request: RecordedRequest,
    pub outcome: ExchangeOutcome,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

impl RecordedExchange {
    /// Construct an exchange with a fresh UUID and the current wall-clock
    /// timestamp. Convenience for the lifecycle code; tests typically build
    /// the struct literal directly.
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

/// Lowercase hex SHA-256 — exported for use by replay-source lookup.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
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

/// Serde adapter for `Bytes` <-> base64 string.
mod base64_bytes {
    use base64::Engine;
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &Bytes, s: S) -> Result<S::Ok, S::Error> {
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
        s.serialize_str(&encoded)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Bytes, D::Error> {
        let s = String::deserialize(d)?;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(s.as_bytes())
            .map_err(serde::de::Error::custom)?;
        Ok(Bytes::from(decoded))
    }
}

/// Serde adapter for `Duration` <-> integer milliseconds (u64).
mod duration_ms {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        // Saturating cast keeps very long durations representable; the
        // realistic ceiling (u64 millis = ~584 million years) never matters.
        let ms = u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
        s.serialize_u64(ms)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let ms = u64::deserialize(d)?;
        Ok(Duration::from_millis(ms))
    }
}

#[cfg(test)]
mod tests {
    use http::header::HeaderValue;

    use super::*;

    #[test]
    fn sha256_hex_matches_known_value() {
        // From `echo -n hello | sha256sum`.
        assert_eq!(
            sha256_hex(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

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
        // Manually insert a binary value.
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
        // Smoke-test the discriminator name lands as expected.
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
