//! Shared type surface for the partly-proxy ecosystem.
//!
//! Both `partly-proxy-lib` and every storage backend crate depend on this
//! crate. Splitting these types out is what lets the lib expose an
//! optional dep on a backend crate while the backend crate's dep on
//! "the trait" doesn't create a Cargo package-graph cycle.
//!
//! `partly-proxy-lib` re-exports every public item from here under its
//! original module path, so downstream callers can continue to write
//! `partly_proxy_lib::ProxyError` / `RecordedExchange` / `SnapshotStorage`
//! without knowing the types live in a sibling crate.

pub mod error;
pub mod recorded;
pub mod storage;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use error::{ProxyError, Result};
pub use recorded::{ExchangeOutcome, RecordedExchange, RecordedRequest, RecordedResponse};
pub use storage::{SharedStorage, SnapshotStorage};
