//! Object-store [`SnapshotStorage`] backend (S3 / GCS / Minio).
//!
//! Built on Apache Arrow's `object_store` crate so one implementation
//! covers every major cloud provider. The crate's [`ObjectStorage`] type
//! takes a `Arc<dyn ObjectStore>` you bring yourself, plus a prefix
//! (e.g. `s3://bucket/runs/2024-01-01`) and an in-memory batch size.
//!
//! ## Durability model
//!
//! - [`SnapshotStorage::append`] buffers serialised NDJSON bytes in
//!   memory. When the buffer crosses `batch_bytes` (default 4 MiB) the
//!   accumulated bytes are PUT as `{prefix}/part-NNNNNNNN.ndjson` and
//!   the buffer is reset.
//! - [`SnapshotStorage::flush`] PUTs the residual buffer (if any) as the
//!   next part, then writes a `manifest.json` listing the parts in
//!   order. The manifest is what [`SnapshotStorage::load`] reads to
//!   replay.
//! - Crash semantics: exchanges sitting in the in-memory buffer at
//!   process abort are lost. The object backend is positioned as
//!   "record-then-save-the-snapshot"; per-exchange durability is the
//!   JSONL or `SQLite` backend's job.
//!
//! ## Wire format
//!
//! Each part is plain NDJSON of `RecordedExchange` — byte-equal to what
//! `partly-proxy-storage-jsonl` produces. A reader can fetch a part and
//! parse it as standard JSON-Lines without any object-backend logic.

use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use parking_lot::Mutex;
use partly_proxy_types::error::{ProxyError, Result};
use partly_proxy_types::recorded::RecordedExchange;
use partly_proxy_types::storage::SnapshotStorage;
use serde::{Deserialize, Serialize};

/// Default in-memory batch size: 4 MiB.
pub const DEFAULT_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// Object-store-backed [`SnapshotStorage`].
pub struct ObjectStorage {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    batch_bytes: usize,
    state: Mutex<BatchState>,
}

impl std::fmt::Debug for ObjectStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStorage")
            .field("prefix", &self.prefix.to_string())
            .field("batch_bytes", &self.batch_bytes)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct BatchState {
    buffer: Vec<u8>,
    next_part: u32,
    total_exchanges: u64,
}

/// JSON shape of the manifest object written on `flush`.
#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    schema_version: u32,
    parts: Vec<String>,
    total_exchanges: u64,
}

impl ObjectStorage {
    /// Build an object-storage backend.
    ///
    /// `store` is the underlying `object_store` instance (S3, GCS,
    /// Minio, in-memory — any `ObjectStore` implementation works).
    /// `prefix` is the path inside the store that scopes this run's
    /// parts and manifest. `batch_bytes` is the in-memory soft limit
    /// before an automatic part upload.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: ObjectPath, batch_bytes: usize) -> Self {
        Self {
            store,
            prefix,
            batch_bytes,
            state: Mutex::new(BatchState::default()),
        }
    }

    /// Convenience constructor that uses [`DEFAULT_BATCH_BYTES`].
    pub fn with_defaults(store: Arc<dyn ObjectStore>, prefix: ObjectPath) -> Self {
        Self::new(store, prefix, DEFAULT_BATCH_BYTES)
    }

    fn part_path(&self, part_no: u32) -> ObjectPath {
        self.prefix.child(format!("part-{part_no:08}.ndjson"))
    }

    fn manifest_path(&self) -> ObjectPath {
        self.prefix.child("manifest.json")
    }

    async fn put_part(&self, part_no: u32, buf: Vec<u8>) -> Result<()> {
        let path = self.part_path(part_no);
        let payload: PutPayload = Bytes::from(buf).into();
        self.store
            .put(&path, payload)
            .await
            .map_err(into_recording)?;
        Ok(())
    }
}

#[async_trait]
impl SnapshotStorage for ObjectStorage {
    async fn append(&self, exchange: &RecordedExchange) -> Result<()> {
        let mut line = serde_json::to_vec(exchange).map_err(into_recording)?;
        line.push(b'\n');

        // Critical section: extend buffer + decide whether to upload.
        // We never await while holding the parking_lot mutex.
        let to_upload = {
            let mut state = self.state.lock();
            state.buffer.extend_from_slice(&line);
            state.total_exchanges += 1;
            if state.buffer.len() >= self.batch_bytes {
                let buf = std::mem::take(&mut state.buffer);
                let part = state.next_part;
                state.next_part += 1;
                Some((part, buf))
            } else {
                None
            }
        };

        if let Some((part_no, buf)) = to_upload {
            self.put_part(part_no, buf).await?;
        }
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        // First: upload any residual buffer as the next part.
        let residual = {
            let mut state = self.state.lock();
            if state.buffer.is_empty() {
                None
            } else {
                let buf = std::mem::take(&mut state.buffer);
                let part = state.next_part;
                state.next_part += 1;
                Some((part, buf))
            }
        };
        if let Some((part_no, buf)) = residual {
            self.put_part(part_no, buf).await?;
        }

        // Second: snapshot the counters and write the manifest.
        let (total, parts_count) = {
            let state = self.state.lock();
            (state.total_exchanges, state.next_part)
        };
        let parts: Vec<String> = (0..parts_count)
            .map(|i| format!("part-{i:08}.ndjson"))
            .collect();
        let manifest = Manifest {
            schema_version: 1,
            parts,
            total_exchanges: total,
        };
        let manifest_bytes = serde_json::to_vec(&manifest).map_err(into_recording)?;
        let payload: PutPayload = Bytes::from(manifest_bytes).into();
        self.store
            .put(&self.manifest_path(), payload)
            .await
            .map_err(into_recording)?;
        Ok(())
    }

    fn load(&self) -> BoxStream<'_, Result<RecordedExchange>> {
        let store = self.store.clone();
        let manifest_path = self.manifest_path();
        let prefix = self.prefix.clone();
        Box::pin(try_stream! {
            // No flush yet → no manifest → empty stream. Treating
            // missing-manifest as zero exchanges is the user-friendly
            // contract; an attempt to load before any `flush()` should
            // not surface as an error.
            let Some(manifest) = fetch_manifest(&store, &manifest_path).await? else {
                return;
            };

            // Stream each part in order.
            for part_name in &manifest.parts {
                let part_path = prefix.child(part_name.as_str());
                let result = store.get(&part_path).await.map_err(into_recording)?;
                let bytes = result.bytes().await.map_err(into_recording)?;
                let text = std::str::from_utf8(&bytes).map_err(into_recording)?;
                for (lineno, line) in text.lines().enumerate() {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let exchange = parse_part_line(part_name, lineno, line)?;
                    yield exchange;
                }
            }
        })
    }
}

async fn fetch_manifest(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
) -> Result<Option<Manifest>> {
    match store.get(path).await {
        Ok(result) => {
            let bytes = result.bytes().await.map_err(into_recording)?;
            let manifest = serde_json::from_slice(&bytes).map_err(into_recording)?;
            Ok(Some(manifest))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(into_recording(e)),
    }
}

fn parse_part_line(part_name: &str, lineno: usize, line: &str) -> Result<RecordedExchange> {
    serde_json::from_str(line).map_err(|e| {
        ProxyError::Recording(std::io::Error::other(format!(
            "part {part_name}, line {}: {e}",
            lineno + 1
        )))
    })
}

fn into_recording<E: std::fmt::Display>(e: E) -> ProxyError {
    ProxyError::Recording(std::io::Error::other(e.to_string()))
}
