//! Shared type surface for the partly-proxy ecosystem.
//!
//! `partly-proxy-lib` and every storage backend crate depend on this
//! crate. Splitting these types out is what avoids a Cargo package-graph
//! cycle: the lib has an optional dep on each backend, and each backend
//! depends only on this crate.
//!
//! `partly-proxy-lib` re-exports every public item from here at its
//! own crate root, so callers can still write `partly_proxy_lib::ProxyError`
//! etc.
//!
//! See [`storage`] for the [`SnapshotStorage`] trait and an
//! implementation walkthrough.

pub mod error;
pub mod recorded;
pub mod storage;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use error::{ProxyError, Result};
pub use recorded::{ExchangeOutcome, RecordedExchange, RecordedRequest, RecordedResponse};
pub use storage::{SharedStorage, SnapshotStorage};
