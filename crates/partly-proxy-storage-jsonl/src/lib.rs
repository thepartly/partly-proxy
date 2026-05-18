//! NDJSON [`SnapshotStorage`] backend.
//!
//! One file, one exchange per line. Bodies serialise as base64 via the
//! `recorded` module's serde plumbing, so the on-disk format matches
//! what `RecordingConfig::persisted` produces when the lib's
//! `storage-jsonl` feature is on.
//!
//! Durability model:
//!
//! - [`JsonlStorage::append`] writes the line and calls `BufWriter::flush`
//!   before returning. The bytes are in the OS write cache; a `tail -f`
//!   on the file sees them immediately and a process crash preserves
//!   them — per-line durability is part of the contract.
//! - [`SnapshotStorage::flush`] additionally calls `File::sync_data` for
//!   callers (typically `ClusterHandle::shutdown`) that want
//!   committed-to-disk semantics.
//!
//! [`parse_ndjson_line`] is exported so `partly-proxy-lib`'s
//! `ReplaySource::from_jsonl` can share the parsing path verbatim, with
//! no duplicated implementation.

use std::path::{Path, PathBuf};

use async_stream::try_stream;
use async_trait::async_trait;
use futures::stream::BoxStream;
use partly_proxy_types::error::{ProxyError, Result};
use partly_proxy_types::recorded::RecordedExchange;
use partly_proxy_types::storage::SnapshotStorage;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::Mutex;

/// NDJSON-backed [`SnapshotStorage`]. Cheap to wrap in `Arc` and share.
#[derive(Debug)]
pub struct JsonlStorage {
    /// Held under an async mutex because `append` writes asynchronously
    /// and we must serialise writes against the shared `BufWriter`.
    state: Mutex<BufWriter<File>>,
    path: PathBuf,
}

impl JsonlStorage {
    /// Open `path` in append mode, creating it if missing. Uses the same
    /// `OpenOptions::new().append(true).create(true)` invocation
    /// `Recorder::new` uses when it auto-builds a `JsonlStorage` from
    /// `RecordingConfig::persist_path`.
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)
            .await
            .map_err(ProxyError::Recording)?;
        Ok(Self {
            state: Mutex::new(BufWriter::new(file)),
            path,
        })
    }

    /// Path the storage was opened on. Surface for callers that want to
    /// pass the same location to `ReplaySource::from_jsonl` later.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl SnapshotStorage for JsonlStorage {
    async fn append(&self, exchange: &RecordedExchange) -> Result<()> {
        let mut line =
            serde_json::to_vec(exchange).map_err(|e| ProxyError::Recording(io_other(e)))?;
        line.push(b'\n');
        let mut guard = self.state.lock().await;
        guard
            .write_all(&line)
            .await
            .map_err(ProxyError::Recording)?;
        // BufWriter::flush so `tail -f` and post-crash readers see the
        // line. The trait's `flush` is reserved for the additional
        // fsync.
        guard.flush().await.map_err(ProxyError::Recording)?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let mut guard = self.state.lock().await;
        guard.flush().await.map_err(ProxyError::Recording)?;
        guard
            .get_ref()
            .sync_data()
            .await
            .map_err(ProxyError::Recording)?;
        Ok(())
    }

    fn load(&self) -> BoxStream<'_, Result<RecordedExchange>> {
        let path = self.path.clone();
        Box::pin(try_stream! {
            let file = File::open(&path).await.map_err(ProxyError::Recording)?;
            let reader = BufReader::new(file);
            let mut lines = reader.lines();
            let mut lineno: usize = 0;
            while let Some(line) = lines
                .next_line()
                .await
                .map_err(ProxyError::Recording)?
            {
                if !line.trim().is_empty() {
                    let exchange = parse_ndjson_line(&line, lineno)?;
                    yield exchange;
                }
                lineno += 1;
            }
        })
    }
}

/// Parse one NDJSON line into a [`RecordedExchange`].
///
/// Exposed as `pub` so `partly-proxy-lib::ReplaySource::from_jsonl` can
/// share the implementation verbatim instead of carrying its own copy.
/// The `lineno` is zero-indexed; the error message offsets it by 1 for
/// human-friendly `line N` reports.
pub fn parse_ndjson_line(line: &str, lineno: usize) -> Result<RecordedExchange> {
    serde_json::from_str(line).map_err(|e| {
        ProxyError::Recording(std::io::Error::other(format!("line {}: {e}", lineno + 1)))
    })
}

fn io_other<E: Into<Box<dyn std::error::Error + Send + Sync>>>(e: E) -> std::io::Error {
    std::io::Error::other(e)
}
