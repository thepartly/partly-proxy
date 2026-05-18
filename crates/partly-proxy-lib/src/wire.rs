//! JSON-Lines wire format for the TCP control plane — see `SPECIFICATION.md`
//! §12.2.
//!
//! The wire types in this module are deliberately separate from the typed
//! Rust [`Command`](crate::Command) / [`CommandResponse`](crate::CommandResponse)
//! enums. The wire format flattens fields for easy hand-writing
//! (`{"type":"Stub","upstream":"api","path_pattern":"^/x$",…}`), and stub
//! bodies are UTF-8 strings rather than base64 so test authors don't need
//! to encode JSON payloads by hand. Recorded exchange bodies, in contrast,
//! ride on the existing base64-via-serde plumbing in `recorded.rs` —
//! they must round-trip arbitrary bytes.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, StatusCode};
use serde::{Deserialize, Serialize};

use crate::assertions::TrafficFilter;
use crate::command::{Command, CommandResponse};
use crate::recorded::RecordedExchange;
use crate::stub::{RequestMatcher, StubbedResponse};

/// Filter portion of every assertion / query command, flattened.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct WireFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_pattern: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

impl WireFilter {
    fn into_filter(self) -> TrafficFilter {
        let mut f = TrafficFilter::new();
        if let Some(u) = self.upstream {
            f = f.upstream(u);
        }
        if let Some(m) = self.method {
            f = f.method(m);
        }
        if let Some(p) = self.path_pattern {
            f = f.path_pattern(p);
        }
        if let Some(s) = self.status {
            f = f.status(s);
        }
        for (k, v) in self.labels {
            f = f.label(k, v);
        }
        f
    }
}

/// One JSON-Lines command on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[non_exhaustive]
pub enum WireCommand {
    Stub(StubFields),
    ClearStubs {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        upstream: Option<String>,
    },
    Pause {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        upstream: Option<String>,
    },
    Resume {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        upstream: Option<String>,
    },
    AssertSeen {
        #[serde(flatten)]
        filter: WireFilter,
        timeout_ms: u64,
    },
    AssertCount {
        #[serde(flatten)]
        filter: WireFilter,
        expected: usize,
        timeout_ms: u64,
    },
    QueryTraffic {
        #[serde(flatten)]
        filter: WireFilter,
    },
    ClearRecordings,
}

/// Stub command payload. Bodies are UTF-8 strings on the wire — see the
/// module docs for the rationale.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StubFields {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream: Option<String>,

    // --- Matcher ---
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_pattern: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub header_contains: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_contains: Option<String>,

    // --- Response ---
    #[serde(default = "default_status")]
    pub status: u16,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub response_headers: BTreeMap<String, String>,
    /// UTF-8 response body.
    #[serde(default)]
    pub body: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delay_ms: Option<u64>,

    /// Fire-count limit. `None` ⇒ unlimited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub times: Option<u32>,
}

fn default_status() -> u16 {
    200
}

impl WireCommand {
    /// Lower a wire command into the in-process [`Command`] type.
    pub fn into_command(self) -> Result<Command, WireError> {
        match self {
            Self::Stub(s) => {
                let mut matcher = RequestMatcher::new();
                if let Some(m) = s.method {
                    let m: Method = m.parse().map_err(|e: http::method::InvalidMethod| {
                        WireError(format!("invalid method: {e}"))
                    })?;
                    matcher = matcher.method(m);
                }
                if let Some(p) = s.path_pattern {
                    matcher = matcher.path(p);
                }
                for (k, v) in s.header_contains {
                    matcher = matcher.header(k, v);
                }
                if let Some(b) = s.body_contains {
                    matcher = matcher.body_contains(Bytes::from(b));
                }

                let status = StatusCode::from_u16(s.status)
                    .map_err(|e| WireError(format!("invalid status: {e}")))?;
                let mut response = StubbedResponse::new(status).body(Bytes::from(s.body));
                for (k, v) in s.response_headers {
                    response = response.header(k, v);
                }
                if let Some(ms) = s.delay_ms {
                    response = response.delay(Duration::from_millis(ms));
                }
                Ok(Command::Stub {
                    upstream: s.upstream,
                    matcher,
                    response,
                    times: s.times,
                })
            }
            Self::ClearStubs { upstream } => Ok(Command::ClearStubs { upstream }),
            Self::Pause { upstream } => Ok(Command::Pause { upstream }),
            Self::Resume { upstream } => Ok(Command::Resume { upstream }),
            Self::AssertSeen { filter, timeout_ms } => Ok(Command::AssertSeen {
                filter: filter.into_filter(),
                timeout: Duration::from_millis(timeout_ms),
            }),
            Self::AssertCount {
                filter,
                expected,
                timeout_ms,
            } => Ok(Command::AssertCount {
                filter: filter.into_filter(),
                expected,
                timeout: Duration::from_millis(timeout_ms),
            }),
            Self::QueryTraffic { filter } => Ok(Command::QueryTraffic {
                filter: filter.into_filter(),
            }),
            Self::ClearRecordings => Ok(Command::ClearRecordings),
        }
    }
}

/// One JSON-Lines response on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[non_exhaustive]
pub enum WireResponse {
    Ok,
    Error { message: String },
    Exchanges { exchanges: Vec<RecordedExchange> },
    AssertionResult { passed: bool, message: String },
}

impl WireResponse {
    /// Lift an in-process [`CommandResponse`] onto the wire.
    pub fn from_response(resp: CommandResponse) -> Self {
        match resp {
            CommandResponse::Ok => Self::Ok,
            CommandResponse::Error { message } => Self::Error { message },
            CommandResponse::Exchanges(exchanges) => Self::Exchanges { exchanges },
            CommandResponse::AssertionResult { passed, message } => {
                Self::AssertionResult { passed, message }
            }
        }
    }
}

/// Failure decoding a wire command into the typed [`Command`].
#[derive(Debug, thiserror::Error)]
#[error("wire decode: {0}")]
pub struct WireError(pub String);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_command_round_trips_through_json() {
        let raw = serde_json::json!({
            "type": "Stub",
            "upstream": "api",
            "method": "POST",
            "path_pattern": "^/orders/\\d+/refund$",
            "header_contains": {"x-tenant": "acme"},
            "body_contains": "\"reason\":\"chargeback\"",
            "status": 201,
            "response_headers": {"content-type": "application/json"},
            "body": "{\"ok\":true}",
            "delay_ms": 50,
            "times": 3
        });
        let wire: WireCommand = serde_json::from_value(raw).unwrap();
        let cmd = wire.into_command().unwrap();
        let Command::Stub {
            upstream,
            response,
            times,
            ..
        } = cmd
        else {
            panic!("expected Stub")
        };
        assert_eq!(upstream.as_deref(), Some("api"));
        assert_eq!(response.status, StatusCode::CREATED);
        assert_eq!(response.body, Bytes::from_static(b"{\"ok\":true}"));
        assert_eq!(response.delay, Some(Duration::from_millis(50)));
        assert_eq!(times, Some(3));
    }

    #[test]
    fn assert_count_round_trips() {
        let raw = serde_json::json!({
            "type": "AssertCount",
            "upstream": "api",
            "path_pattern": "^/health$",
            "expected": 1,
            "timeout_ms": 5000
        });
        let wire: WireCommand = serde_json::from_value(raw).unwrap();
        match wire.into_command().unwrap() {
            Command::AssertCount {
                expected,
                timeout,
                filter,
            } => {
                assert_eq!(expected, 1);
                assert_eq!(timeout, Duration::from_secs(5));
                assert_eq!(filter.upstream.as_deref(), Some("api"));
                assert_eq!(filter.path_pattern.as_deref(), Some("^/health$"));
            }
            other => panic!("expected AssertCount, got {other:?}"),
        }
    }

    #[test]
    fn unknown_method_returns_wire_error() {
        let wire = WireCommand::Stub(StubFields {
            upstream: None,
            method: Some("BAD METHOD".into()),
            path_pattern: None,
            header_contains: BTreeMap::new(),
            body_contains: None,
            status: 200,
            response_headers: BTreeMap::new(),
            body: String::new(),
            delay_ms: None,
            times: None,
        });
        let err = wire.into_command().unwrap_err();
        assert!(err.to_string().contains("invalid method"));
    }

    #[test]
    fn response_lifts_from_internal_type() {
        let r = WireResponse::from_response(CommandResponse::Ok);
        assert!(matches!(r, WireResponse::Ok));
        let r = WireResponse::from_response(CommandResponse::Error {
            message: "x".into(),
        });
        assert!(matches!(r, WireResponse::Error { message } if message == "x"));
    }

    #[test]
    fn ok_response_serialises_compactly() {
        let r = WireResponse::Ok;
        assert_eq!(serde_json::to_string(&r).unwrap(), r#"{"type":"Ok"}"#);
    }
}
