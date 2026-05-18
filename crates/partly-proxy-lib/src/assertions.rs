//! `TrafficFilter` — the common predicate used by assertions and traffic
//! queries. See `SPECIFICATION.md` §13.
//!
//! All set conditions AND together; unset conditions match anything. The
//! `path_pattern` field is a regex if it compiles, otherwise an exact-string
//! match (same fallback as [`crate::stub::RequestMatcher`]).

use std::collections::BTreeMap;

use regex::Regex;

use crate::recorded::RecordedExchange;

/// Predicate used by `AssertSeen`, `AssertCount`, and `QueryTraffic`.
#[derive(Debug, Default, Clone)]
pub struct TrafficFilter {
    pub upstream: Option<String>,
    pub method: Option<String>,
    pub path_pattern: Option<String>,
    pub status: Option<u16>,
    pub labels: BTreeMap<String, String>,
}

impl TrafficFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upstream(mut self, name: impl Into<String>) -> Self {
        self.upstream = Some(name.into());
        self
    }

    pub fn method(mut self, m: impl Into<String>) -> Self {
        self.method = Some(m.into());
        self
    }

    pub fn path_pattern(mut self, p: impl Into<String>) -> Self {
        self.path_pattern = Some(p.into());
        self
    }

    pub fn status(mut self, s: u16) -> Self {
        self.status = Some(s);
        self
    }

    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// Whether `exchange` matches every constraint in this filter.
    pub fn matches(&self, exchange: &RecordedExchange) -> bool {
        if let Some(name) = &self.upstream {
            if exchange.upstream.as_deref() != Some(name.as_str()) {
                return false;
            }
        }
        if let Some(m) = &self.method {
            if exchange.request.method != *m {
                return false;
            }
        }
        if let Some(p) = &self.path_pattern {
            // Recompile each time. The filter is typically used once per
            // command, so the cost is negligible and we avoid threading
            // a cached `Regex` through the wire format. If profiling ever
            // shows this is hot we can cache lazily.
            let path = parse_path(&exchange.request.uri);
            let hit = match Regex::new(p) {
                Ok(r) => r.is_match(path.as_str()),
                Err(_) => path == *p,
            };
            if !hit {
                return false;
            }
        }
        if let Some(want) = self.status {
            match exchange.outcome.as_response() {
                Some(r) if r.status == want => {}
                _ => return false,
            }
        }
        for (k, v) in &self.labels {
            if exchange.labels.get(k) != Some(v) {
                return false;
            }
        }
        true
    }
}

/// Extract just the path component of a recorded URI, robust to both full
/// URIs (`http://host/x?y=1`) and path-only forms (`/x?y=1`).
fn parse_path(uri: &str) -> String {
    if let Ok(parsed) = uri.parse::<http::Uri>() {
        return parsed.path().to_owned();
    }
    // Fallback: strip query, then strip scheme/authority if present.
    let no_query = uri.split('?').next().unwrap_or("/");
    if let Some(idx) = no_query.find("://") {
        if let Some(slash) = no_query[idx + 3..].find('/') {
            return no_query[idx + 3 + slash..].to_owned();
        }
        return "/".to_owned();
    }
    no_query.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recorded::{ExchangeOutcome, RecordedExchange, RecordedRequest, RecordedResponse};
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use std::time::Duration;

    fn ex(method: Method, path: &str, status: u16) -> RecordedExchange {
        let req = RecordedRequest::from_parts(
            &method,
            &path.parse().unwrap(),
            &HeaderMap::new(),
            Bytes::new(),
        );
        let resp = RecordedResponse {
            status,
            headers: vec![],
            body: Bytes::new(),
        };
        RecordedExchange::new(
            Some("api".into()),
            req,
            ExchangeOutcome::Response(resp),
            Duration::from_millis(1),
        )
    }

    #[test]
    fn empty_filter_matches_everything() {
        assert!(TrafficFilter::new().matches(&ex(Method::GET, "/x", 200)));
    }

    #[test]
    fn upstream_constraint_is_exact() {
        let e = ex(Method::GET, "/x", 200);
        assert!(TrafficFilter::new().upstream("api").matches(&e));
        assert!(!TrafficFilter::new().upstream("billing").matches(&e));
    }

    #[test]
    fn method_and_status_constraints() {
        let e = ex(Method::POST, "/orders", 201);
        assert!(TrafficFilter::new().method("POST").matches(&e));
        assert!(!TrafficFilter::new().method("GET").matches(&e));
        assert!(TrafficFilter::new().status(201).matches(&e));
        assert!(!TrafficFilter::new().status(200).matches(&e));
    }

    #[test]
    fn path_regex_matches_when_compileable() {
        let e = ex(Method::GET, "/orders/123/refund", 200);
        assert!(TrafficFilter::new()
            .path_pattern(r"^/orders/\d+/refund$")
            .matches(&e));
        assert!(!TrafficFilter::new()
            .path_pattern(r"^/orders/\d+$")
            .matches(&e));
    }

    #[test]
    fn label_constraint_requires_exact_match() {
        let mut e = ex(Method::GET, "/x", 200);
        e.labels.insert("tier".into(), "gold".into());
        assert!(TrafficFilter::new().label("tier", "gold").matches(&e));
        assert!(!TrafficFilter::new().label("tier", "silver").matches(&e));
        assert!(!TrafficFilter::new().label("missing", "x").matches(&e));
    }

    #[test]
    fn status_does_not_match_error_outcome() {
        let req = RecordedRequest::from_parts(
            &Method::GET,
            &"/x".parse().unwrap(),
            &HeaderMap::new(),
            Bytes::new(),
        );
        let exchange = RecordedExchange::new(
            Some("api".into()),
            req,
            ExchangeOutcome::Error {
                message: "boom".into(),
            },
            Duration::from_millis(1),
        );
        assert!(!TrafficFilter::new().status(200).matches(&exchange));
        // Filters without `status` still match.
        assert!(TrafficFilter::new().method("GET").matches(&exchange));
    }
}
