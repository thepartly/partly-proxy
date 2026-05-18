//! Snapshot storage abstraction — see `SPECIFICATION.md` §9.
//!
//! Any crate can implement [`SnapshotStorage`]; the trait surface uses
//! only types from this crate, so backends don't need `partly-proxy-lib`.
//! First-party backends: `partly-proxy-storage-jsonl`,
//! `partly-proxy-storage-sqlite`.
//!
//! # Example
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
//! Backends can opt into the shared conformance battery by enabling the
//! `testing` Cargo feature and calling
//! [`testing::run_conformance`](crate::testing::run_conformance).

use std::sync::Arc;

use async_trait::async_trait;
// Re-exported so implementers don't have to add `futures` themselves.
pub use futures::stream::BoxStream;

use crate::{error::Result, recorded::RecordedExchange};

pub type ExchangeStream<'a> = BoxStream<'a, Result<RecordedExchange>>;

/// Pluggable storage medium for recorded exchanges.
#[async_trait]
pub trait SnapshotStorage: Send + Sync + std::fmt::Debug {
    /// Persist one exchange. Called *after* redaction and *before* the
    /// in-memory ring is touched, so an error here stops the exchange from
    /// becoming visible to predicate scans.
    ///
    /// "Queued for durability": backends MAY return before bytes hit stable
    /// storage. Callers needing a fence call [`flush`](Self::flush).
    async fn append(&self, exchange: &RecordedExchange) -> Result<()>;

    /// Make every previously appended exchange durable.
    async fn flush(&self) -> Result<()>;

    /// Streaming read in insertion order. A stream (not `Vec`) keeps peak
    /// memory bounded by the largest single exchange (§8.1.1).
    fn load(&self) -> ExchangeStream<'_>;
}

/// Cheap-to-clone handle on a [`SnapshotStorage`].
pub type SharedStorage = Arc<dyn SnapshotStorage>;
