//! Snapshot storage abstraction — pluggable medium for recorded exchanges.
//!
//! The recorder calls into a [`SnapshotStorage`] for every exchange that
//! survives the snapshot-boundary redaction pipeline (§9.2). The trait is
//! deliberately small: every backend supports the full record → replay
//! round-trip, and shutdown is the single durability fence.
//!
//! Backends live in their own workspace crates (`partly-proxy-storage-jsonl`,
//! `partly-proxy-storage-sqlite`). The lib's `storage-jsonl` /
//! `storage-sqlite` Cargo features additively pull them in and re-export
//! them at `partly_proxy_lib::jsonl` / `partly_proxy_lib::sqlite`, so
//! callers can either depend on the backend crate directly or enable the
//! feature on the lib.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::error::Result;
use crate::recorded::RecordedExchange;

/// Pluggable storage medium for recorded exchanges. See `SPECIFICATION.md`
/// §9 and `.scratch/MULTI_BACKEND_PLAN.md` for the design rationale.
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
    /// The contract is "queued for durability": backends MAY return before
    /// the byte hits stable storage. Callers that need a fence call
    /// [`flush`](Self::flush). Per-line file backends (NDJSON) keep the
    /// per-`append` flush they had before this trait existed; batched
    /// backends (object store) buffer until `flush`.
    async fn append(&self, exchange: &RecordedExchange) -> Result<()>;

    /// Make every previously appended exchange durable. Called from
    /// `Recorder::flush` on demand and from `ClusterHandle::shutdown`.
    /// For batching backends (object store) this is when bytes leave
    /// memory; for line-buffered backends it is an additional fsync.
    async fn flush(&self) -> Result<()>;

    /// Streaming read in insertion order. Used by
    /// `ReplaySource::from_storage` to materialise a snapshot.
    ///
    /// Returning a stream — not a `Vec` — preserves the spec's
    /// "100k-exchange single streaming pass" property (§8.1.1). Peak
    /// memory is bounded by the largest single exchange, not the file
    /// size.
    fn load(&self) -> BoxStream<'_, Result<RecordedExchange>>;
}

/// Cheap-to-clone handle on a [`SnapshotStorage`]. Cluster code threads
/// this around instead of taking generic type parameters everywhere.
pub type SharedStorage = Arc<dyn SnapshotStorage>;
