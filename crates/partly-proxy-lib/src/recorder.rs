//! Shared traffic recorder — see `SPECIFICATION.md` §9.2.
//!
//! The recorder owns an in-memory ring buffer of `RecordedExchange`s and,
//! optionally, an append-only NDJSON file. It is cheaply cloneable
//! (`Arc`-backed); every listener task and the future control plane share
//! one instance per cluster.
//!
//! Redaction (`redact_request_for_snapshot` / `redact_response_for_snapshot`,
//! §6.4) happens in the lifecycle code *before* this recorder is called —
//! the recorder hashes and stores whatever it is handed.

use std::collections::VecDeque;
use std::sync::Arc;

use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::{Notify, RwLock};

use crate::config::RecordingConfig;
use crate::error::{ProxyError, Result};
use crate::recorded::RecordedExchange;

/// Cheap-to-clone handle on the shared recorder.
#[derive(Clone)]
pub struct Recorder {
    inner: Arc<RecorderInner>,
}

impl std::fmt::Debug for Recorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recorder")
            .field("enabled", &self.inner.config.enabled)
            .field("max_in_memory", &self.inner.config.max_in_memory)
            .field("persist_path", &self.inner.config.persist_path)
            .finish_non_exhaustive()
    }
}

struct RecorderInner {
    config: RecordingConfig,
    state: RwLock<RecorderState>,
    /// Fired (via `notify_waiters`) every time a new exchange is recorded.
    /// The wait-for assertion loop registers a waiter before each predicate
    /// check, so notifications that arrive between checks are not lost.
    on_insert: Notify,
}

struct RecorderState {
    buffer: VecDeque<RecordedExchange>,
    file: Option<BufWriter<tokio::fs::File>>,
}

impl Recorder {
    /// Build a recorder. If `config.persist_path` is set, the file is opened
    /// in append mode and any error is surfaced as `ProxyError::Recording`.
    pub async fn new(config: RecordingConfig) -> Result<Self> {
        let file = match &config.persist_path {
            Some(path) => {
                let f = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(path)
                    .await
                    .map_err(ProxyError::Recording)?;
                Some(BufWriter::new(f))
            }
            None => None,
        };
        let initial_capacity = config.max_in_memory.min(1024);
        let state = RecorderState {
            buffer: VecDeque::with_capacity(initial_capacity),
            file,
        };
        Ok(Self {
            inner: Arc::new(RecorderInner {
                config,
                state: RwLock::new(state),
                on_insert: Notify::new(),
            }),
        })
    }

    /// Borrow the per-insert notifier. Callers that need to wait for new
    /// exchanges should register a waiter via `notified()` *before* taking
    /// their first predicate snapshot — that way a `record()` that lands
    /// between snapshot and await still wakes them.
    pub(crate) fn on_insert(&self) -> &Notify {
        &self.inner.on_insert
    }

    /// Whether recording is enabled for this recorder.
    pub fn is_enabled(&self) -> bool {
        self.inner.config.enabled
    }

    /// Cap on the in-memory ring (FIFO eviction).
    pub fn max_in_memory(&self) -> usize {
        self.inner.config.max_in_memory
    }

    /// Insert an exchange. If the buffer is at capacity, the oldest entry is
    /// evicted first (FIFO). When the recorder has a `persist_path`, the
    /// exchange is appended to disk *before* it lands in memory — that way a
    /// disk error stops the exchange from being visible to predicate scans.
    ///
    /// When recording is disabled, this is a no-op.
    pub async fn record(&self, exchange: RecordedExchange) -> Result<()> {
        if !self.inner.config.enabled {
            return Ok(());
        }
        let mut state = self.inner.state.write().await;

        if let Some(file) = state.file.as_mut() {
            let mut line = serde_json::to_vec(&exchange)
                .map_err(|e| ProxyError::Recording(std::io::Error::other(e)))?;
            line.push(b'\n');
            file.write_all(&line).await.map_err(ProxyError::Recording)?;
            file.flush().await.map_err(ProxyError::Recording)?;
        }

        if self.inner.config.max_in_memory == 0 {
            // Cap of zero means "do not retain in memory" — disk-only mode.
            // We still notify waiters because a disk-only recorder may be
            // monitored by an external process tailing the NDJSON file.
            drop(state);
            self.inner.on_insert.notify_waiters();
            return Ok(());
        }
        while state.buffer.len() >= self.inner.config.max_in_memory {
            state.buffer.pop_front();
        }
        state.buffer.push_back(exchange);
        // Drop the lock *before* notifying so the waiter that wakes up can
        // grab the read lock without contending with us.
        drop(state);
        self.inner.on_insert.notify_waiters();
        Ok(())
    }

    /// Snapshot every exchange currently in the ring buffer, in insertion
    /// order. Returns a fresh `Vec` — does not borrow internal state.
    pub async fn exchanges(&self) -> Vec<RecordedExchange> {
        self.inner
            .state
            .read()
            .await
            .buffer
            .iter()
            .cloned()
            .collect()
    }

    /// Number of exchanges currently retained in memory.
    pub async fn len(&self) -> usize {
        self.inner.state.read().await.buffer.len()
    }

    /// Whether the buffer is empty.
    pub async fn is_empty(&self) -> bool {
        self.inner.state.read().await.buffer.is_empty()
    }

    /// Drop every exchange currently held in memory. The on-disk NDJSON is
    /// not touched — clearing is purely an in-memory operation.
    pub async fn clear(&self) {
        self.inner.state.write().await.buffer.clear();
    }

    /// Whether any exchange in the buffer matches `pred`.
    pub async fn any_matching<F>(&self, pred: F) -> bool
    where
        F: Fn(&RecordedExchange) -> bool,
    {
        self.inner.state.read().await.buffer.iter().any(pred)
    }

    /// Count exchanges in the buffer matching `pred`.
    pub async fn count_matching<F>(&self, pred: F) -> usize
    where
        F: Fn(&RecordedExchange) -> bool,
    {
        self.inner
            .state
            .read()
            .await
            .buffer
            .iter()
            .filter(|e| pred(e))
            .count()
    }

    /// First exchange in the buffer matching `pred`, cloned.
    pub async fn find_matching<F>(&self, pred: F) -> Option<RecordedExchange>
    where
        F: Fn(&RecordedExchange) -> bool,
    {
        self.inner
            .state
            .read()
            .await
            .buffer
            .iter()
            .find(|e| pred(e))
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recorded::{ExchangeOutcome, RecordedRequest, RecordedResponse};
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use std::time::Duration;

    fn make_exchange(path: &str, body: &[u8]) -> RecordedExchange {
        let req = RecordedRequest::from_parts(
            &Method::POST,
            &path.parse().unwrap(),
            &HeaderMap::new(),
            Bytes::copy_from_slice(body),
        );
        let resp = RecordedResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(b"ok"),
        };
        RecordedExchange::new(
            Some("api".to_owned()),
            req,
            ExchangeOutcome::Response(resp),
            Duration::from_millis(1),
        )
    }

    #[tokio::test]
    async fn records_and_reads_back_in_order() {
        let recorder = Recorder::new(RecordingConfig::in_memory(10)).await.unwrap();
        recorder.record(make_exchange("/a", b"1")).await.unwrap();
        recorder.record(make_exchange("/b", b"2")).await.unwrap();
        let all = recorder.exchanges().await;
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].request.uri, "/a");
        assert_eq!(all[1].request.uri, "/b");
    }

    #[tokio::test]
    async fn disabled_recorder_is_a_noop() {
        let recorder = Recorder::new(RecordingConfig::disabled()).await.unwrap();
        recorder.record(make_exchange("/a", b"")).await.unwrap();
        assert_eq!(recorder.len().await, 0);
    }

    #[tokio::test]
    async fn fifo_eviction_drops_oldest_at_capacity() {
        let recorder = Recorder::new(RecordingConfig::in_memory(3)).await.unwrap();
        for i in 0..5 {
            recorder
                .record(make_exchange(&format!("/{i}"), b""))
                .await
                .unwrap();
        }
        let all = recorder.exchanges().await;
        assert_eq!(all.len(), 3);
        let paths: Vec<_> = all.iter().map(|e| e.request.uri.clone()).collect();
        assert_eq!(paths, vec!["/2", "/3", "/4"]);
    }

    #[tokio::test]
    async fn clear_empties_buffer() {
        let recorder = Recorder::new(RecordingConfig::in_memory(10)).await.unwrap();
        recorder.record(make_exchange("/a", b"")).await.unwrap();
        assert_eq!(recorder.len().await, 1);
        recorder.clear().await;
        assert!(recorder.is_empty().await);
    }

    #[tokio::test]
    async fn predicate_scans_filter_correctly() {
        let recorder = Recorder::new(RecordingConfig::in_memory(10)).await.unwrap();
        recorder.record(make_exchange("/a", b"")).await.unwrap();
        recorder.record(make_exchange("/b", b"")).await.unwrap();
        recorder.record(make_exchange("/b", b"")).await.unwrap();

        assert!(recorder.any_matching(|e| e.request.uri == "/b").await);
        assert!(!recorder.any_matching(|e| e.request.uri == "/c").await);
        assert_eq!(recorder.count_matching(|e| e.request.uri == "/b").await, 2);
        let found = recorder
            .find_matching(|e| e.request.uri == "/a")
            .await
            .expect("present");
        assert_eq!(found.request.uri, "/a");
    }

    #[tokio::test]
    async fn persist_path_writes_ndjson_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trace.ndjson");
        let recorder = Recorder::new(RecordingConfig::persisted(10, path.clone()))
            .await
            .unwrap();
        recorder
            .record(make_exchange("/a", b"hello"))
            .await
            .unwrap();
        recorder
            .record(make_exchange("/b", b"world"))
            .await
            .unwrap();

        let raw = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<_> = raw.lines().collect();
        assert_eq!(lines.len(), 2);
        let parsed: RecordedExchange = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed.request.uri, "/a");
        let parsed: RecordedExchange = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(parsed.request.uri, "/b");
    }
}
