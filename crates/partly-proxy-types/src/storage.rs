//! Snapshot storage abstraction — pluggable medium for recorded exchanges.
//!
//! The recorder calls into a [`SnapshotStorage`] for every exchange that
//! survives the snapshot-boundary redaction pipeline (§9.2). The trait is
//! deliberately small: every backend supports the full record → replay
//! round-trip, and shutdown is the single durability fence.
//!
//! First-party backends live in their own workspace crates
//! (`partly-proxy-storage-jsonl`, `partly-proxy-storage-sqlite`). The
//! lib's `storage-jsonl` / `storage-sqlite` Cargo features additively
//! pull them in and re-export them at `partly_proxy_lib::jsonl` /
//! `partly_proxy_lib::sqlite`.
//!
//! # Implementing a custom backend
//!
//! `SnapshotStorage` is a normal public trait — any crate can implement
//! it. The trait surface uses only types exported from
//! `partly-proxy-types`, so a custom backend can depend on this crate
//! alone (no need to pull in the heavyweight `partly-proxy-lib`).
//!
//! Required dependencies in the backend crate's `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! partly-proxy-types = "0.1"
//! async-trait = "0.1"
//! ```
//!
//! Minimal example — an in-memory backend that's effectively a thin
//! wrapper around a `Vec`:
//!
//! ```
//! use std::sync::Mutex;
//!
//! use async_trait::async_trait;
//! use partly_proxy_types::storage::{BoxStream, SnapshotStorage};
//! use partly_proxy_types::{RecordedExchange, Result};
//!
//! #[derive(Debug, Default)]
//! pub struct InMemoryStorage {
//!     exchanges: Mutex<Vec<RecordedExchange>>,
//! }
//!
//! #[async_trait]
//! impl SnapshotStorage for InMemoryStorage {
//!     async fn append(&self, exchange: &RecordedExchange) -> Result<()> {
//!         self.exchanges.lock().unwrap().push(exchange.clone());
//!         Ok(())
//!     }
//!
//!     async fn flush(&self) -> Result<()> {
//!         Ok(())
//!     }
//!
//!     fn load(&self) -> BoxStream<'_, Result<RecordedExchange>> {
//!         let snapshot = self.exchanges.lock().unwrap().clone();
//!         Box::pin(futures::stream::iter(snapshot.into_iter().map(Ok)))
//!     }
//! }
//! ```
//!
//! Wrap the backend in [`SharedStorage`] and hand it to the recorder via
//! `Recorder::with_storage(config, Some(storage))` or
//! `ProxyClusterBuilder::storage(storage)`.
//!
//! ## Conformance testing
//!
//! Backend crates can opt into the shared conformance battery by adding
//! `partly-proxy-types = { version = "0.1", features = ["testing"] }`
//! as a dev-dep and calling
//! [`testing::run_conformance`](crate::testing::run_conformance) with
//! a factory closure that produces a fresh `SharedStorage` per
//! sub-case.

use std::sync::Arc;

use async_trait::async_trait;
// Re-exported so external implementers can spell the return type of
// `load()` without adding `futures` to their own Cargo.toml.
pub use futures::stream::BoxStream;

use crate::error::Result;
use crate::recorded::RecordedExchange;

/// Convenience alias for the stream type returned by [`SnapshotStorage::load`].
///
/// `BoxStream<'a, Result<RecordedExchange>>` is the canonical spelling;
/// implementations can use either form interchangeably.
pub type ExchangeStream<'a> = BoxStream<'a, Result<RecordedExchange>>;

/// Pluggable storage medium for recorded exchanges. See
/// `SPECIFICATION.md` §9 for the design rationale and the module-level
/// docs above for an implementation example.
///
/// Implementations are typically wrapped in [`SharedStorage`] so they can
/// be cloned cheaply across the recorder, the cluster handle, and any
/// later replay sources.
#[async_trait]
pub trait SnapshotStorage: Send + Sync + std::fmt::Debug {
    /// Persist one exchange. Called from `Recorder::record` *after*
    /// redaction (§9.2) and *before* the in-memory ring is touched, so a
    /// disk / network error stops the exchange from becoming visible to
    /// predicate scans.
    ///
    /// The contract is "queued for durability": backends MAY return
    /// before the bytes hit stable storage. Callers that need a fence
    /// call [`flush`](Self::flush). Per-line file backends (NDJSON) keep
    /// the per-`append` flush they had before this trait existed;
    /// batched backends buffer until `flush`.
    async fn append(&self, exchange: &RecordedExchange) -> Result<()>;

    /// Make every previously appended exchange durable. Called from
    /// `Recorder::flush` on demand and from `ClusterHandle::shutdown`.
    /// For batching backends this is when bytes leave memory; for
    /// line-buffered backends it is an additional fsync.
    async fn flush(&self) -> Result<()>;

    /// Streaming read in insertion order. Used by
    /// `ReplaySource::from_storage` to materialise a snapshot.
    ///
    /// Returning a stream — not a `Vec` — preserves the spec's
    /// "100k-exchange single streaming pass" property (§8.1.1). Peak
    /// memory is bounded by the largest single exchange, not the file
    /// size.
    fn load(&self) -> ExchangeStream<'_>;
}

/// Cheap-to-clone handle on a [`SnapshotStorage`]. Cluster code threads
/// this around instead of taking generic type parameters everywhere.
pub type SharedStorage = Arc<dyn SnapshotStorage>;
