//! `SQLite` [`SnapshotStorage`] backend, built on [`sqlx`] 0.8 with the
//! `sqlite + runtime-tokio` features.
//!
//! Durability model:
//!
//! - `append` is one `INSERT` per exchange. WAL + `synchronous=NORMAL`
//!   gives durable autocommit per statement, so [`SnapshotStorage::flush`]
//!   is a no-op.
//! - `load` streams `SELECT payload FROM exchanges ORDER BY seq` via
//!   `sqlx::query(...).fetch(&pool)`, mapping each `Vec<u8>` row through
//!   `serde_json::from_slice` to a [`RecordedExchange`]. Insertion order
//!   is preserved by the autoincrement `seq` column.
//!
//! The `payload` column carries the canonical JSON encoding of the
//! exchange so cross-backend bytes are identical. Dedicated `method`,
//! `uri`, and `body_sha256` columns sit alongside `payload` for a future
//! push-down of `MethodUriAndBodyHash` replay lookups into SQL — out
//! of scope here.

use std::{path::Path, str::FromStr};

use async_trait::async_trait;
use futures::{TryStreamExt, stream::BoxStream};
use partly_proxy_types::{
    error::{ProxyError, Result},
    recorded::RecordedExchange,
    storage::SnapshotStorage,
};
use sqlx::{
    Row,
    sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteSynchronous,
    },
};

/// `SQLite`-backed [`SnapshotStorage`]. Cheap to wrap in `Arc` and share.
#[derive(Debug)]
pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    /// Open (creating if missing) a file-backed `SQLite` database at `path`.
    /// WAL journaling and `synchronous=NORMAL` (via
    /// [`SqliteSynchronous::Normal`]) are enabled on first connect; the
    /// migration runs once on open.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let opts = SqliteConnectOptions::new()
            .filename(path.as_ref())
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(opts)
            .await
            .map_err(into_recording)?;
        run_migration(&pool).await?;
        Ok(Self { pool })
    }

    /// Open a transient in-memory database. Limited to one connection
    /// because each in-memory connection would otherwise see a different
    /// database.
    pub async fn in_memory() -> Result<Self> {
        let opts = SqliteConnectOptions::from_str("sqlite::memory:")
            .map_err(into_recording)?
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await
            .map_err(into_recording)?;
        run_migration(&pool).await?;
        Ok(Self { pool })
    }

    /// Borrow the underlying connection pool. Useful for advanced tests
    /// or migrations layered on top of the backend.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

async fn run_migration(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS exchanges (
            seq         INTEGER PRIMARY KEY AUTOINCREMENT,
            id          TEXT NOT NULL,
            upstream    TEXT,
            timestamp   TEXT NOT NULL,
            duration_ms INTEGER NOT NULL,
            method      TEXT NOT NULL,
            uri         TEXT NOT NULL,
            body_sha256 TEXT NOT NULL,
            payload     BLOB NOT NULL
        )",
    )
    .execute(pool)
    .await
    .map_err(into_recording)?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_lookup ON exchanges(method, uri, body_sha256)")
        .execute(pool)
        .await
        .map_err(into_recording)?;
    Ok(())
}

#[async_trait]
impl SnapshotStorage for SqliteStorage {
    async fn append(&self, exchange: &RecordedExchange) -> Result<()> {
        let payload = serde_json::to_vec(exchange).map_err(into_recording)?;
        let duration_ms = i64::try_from(exchange.duration.as_millis()).unwrap_or(i64::MAX);
        sqlx::query(
            "INSERT INTO exchanges (id, upstream, timestamp, duration_ms, method, uri, body_sha256, payload)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(exchange.id.to_string())
        .bind(exchange.upstream.as_deref())
        .bind(exchange.timestamp.to_rfc3339())
        .bind(duration_ms)
        .bind(&exchange.request.method)
        .bind(&exchange.request.uri)
        .bind(&exchange.request.body_sha256)
        .bind(payload)
        .execute(&self.pool)
        .await
        .map_err(into_recording)?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        // WAL + autocommit-per-statement means every append is already
        // durable. Nothing to fence.
        Ok(())
    }

    fn load(&self) -> BoxStream<'_, Result<RecordedExchange>> {
        let stream = sqlx::query("SELECT payload FROM exchanges ORDER BY seq")
            .fetch(&self.pool)
            .map_err(into_recording)
            .and_then(|row| async move {
                let payload: Vec<u8> = row.try_get("payload").map_err(into_recording)?;
                serde_json::from_slice::<RecordedExchange>(&payload).map_err(into_recording)
            });
        Box::pin(stream)
    }
}

fn into_recording<E: std::fmt::Display>(e: E) -> ProxyError {
    ProxyError::Recording(std::io::Error::other(e.to_string()))
}
