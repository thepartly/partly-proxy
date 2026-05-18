//! Shared type surface for the partly-proxy ecosystem.
//!
//! `partly-proxy-lib` and every storage backend crate depend on this
//! crate. Splitting these types out is what lets the lib expose an
//! optional dep on a backend crate while the backend crate's dep on
//! "the trait" doesn't create a Cargo package-graph cycle.
//!
//! `partly-proxy-lib` re-exports every public item from here under its
//! original module path, so downstream callers can continue to write
//! `partly_proxy_lib::ProxyError` / `RecordedExchange` / `SnapshotStorage`
//! without knowing the types live in a sibling crate.
//!
//! # Implementing a custom storage backend
//!
//! [`SnapshotStorage`] is a public trait — any crate can implement it.
//! See the [`storage`] module docs for a worked example. The crates
//! `partly-proxy-storage-jsonl` and `partly-proxy-storage-sqlite` are
//! first-party implementations; either makes a fine starting point if
//! you want to write your own.
//!
//! The public surface for implementers is intentionally small:
//!
//! - [`SnapshotStorage`] — the trait itself.
//! - [`SharedStorage`] — `Arc<dyn SnapshotStorage>`; what the recorder and
//!   builder accept.
//! - [`storage::ExchangeStream`] / [`storage::BoxStream`] — the stream
//!   type returned by [`SnapshotStorage::load`].
//! - [`RecordedExchange`], [`RecordedRequest`], [`RecordedResponse`],
//!   [`ExchangeOutcome`] — the on-the-wire data model. Serde-derived,
//!   so backends can persist them via any format their storage layer
//!   supports.
//! - [`Result`] / [`ProxyError`] — every fallible trait method uses these.
//! - [`testing::run_conformance`](testing::run_conformance) — opt-in via
//!   the `testing` Cargo feature; runs a backend through the same
//!   battery the first-party backends use.

pub mod error;
pub mod recorded;
pub mod storage;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use error::{ProxyError, Result};
pub use recorded::{ExchangeOutcome, RecordedExchange, RecordedRequest, RecordedResponse};
pub use storage::{SharedStorage, SnapshotStorage};
