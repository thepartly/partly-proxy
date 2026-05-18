//! `partly-proxy-lib` — programmable HTTP/HTTPS proxy for integration testing.
//!
//! See `SPECIFICATION.md` in the workspace root for the full design.
//!
//! This is slice 1 of an incremental rollout: only the configuration types,
//! the error model, and a non-functional builder/cluster skeleton are present.
//! Networking, middleware, recording, replay, and the control plane land in
//! later slices.

#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

pub mod assertions;
pub mod builder;
pub mod cluster;
pub mod command;
pub mod config;
pub mod context;
mod control_plane;
pub mod error;
mod forwarder;
mod listener;
pub mod middleware;
pub mod proxy_io;
pub mod recorded;
pub mod recorder;
pub mod replay;
pub mod storage;
pub mod stub;
mod tls;
mod upstream;
pub mod wire;

pub use assertions::TrafficFilter;
pub use builder::ProxyClusterBuilder;
pub use cluster::ClusterHandle;
pub use command::{Command, CommandResponse, CommandSender};
pub use config::{
    InboundTlsConfig, ProxyConfig, RecordingConfig, UpstreamTarget, UpstreamTlsConfig,
};
pub use context::RequestContext;
pub use error::{ProxyError, Result};
pub use middleware::{Next, ProxyMiddleware, SharedMiddleware, Terminal, TerminalFuture};
pub use proxy_io::{ProxyRequest, ProxyResponse};
pub use recorded::{ExchangeOutcome, RecordedExchange, RecordedRequest, RecordedResponse};
pub use recorder::Recorder;
pub use replay::{MatchStrategy, ReplaySource};
pub use storage::{SharedStorage, SnapshotStorage};
pub use stub::{RequestMatcher, StubEntry, StubStore, StubbedResponse};

/// Re-export of the JSON-Lines snapshot backend, available when the
/// `storage-jsonl` feature is on (which it is by default).
#[cfg(feature = "storage-jsonl")]
pub use partly_proxy_storage_jsonl as jsonl;

/// Re-export of the SQLite snapshot backend, available when the
/// `storage-sqlite` feature is on.
#[cfg(feature = "storage-sqlite")]
pub use partly_proxy_storage_sqlite as sqlite;

/// Re-export of the object-store (S3 / GCS / Minio) snapshot backend,
/// available when the `storage-object` feature is on.
#[cfg(feature = "storage-object")]
pub use partly_proxy_storage_object as object;
pub use wire::{StubFields, WireCommand, WireFilter, WireResponse};
