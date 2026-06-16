//! `partly-proxy-lib` — programmable HTTP/HTTPS proxy for integration testing.
//!
//! See `SPECIFICATION.md` in the workspace root for the full design.

#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

pub mod assertions;
pub mod builder;
pub mod cluster;
pub mod command;
pub mod config;
pub mod context;
mod control_plane;
mod forwarder;
mod listener;
pub mod middleware;
pub mod proxy_io;
pub mod recorder;
pub mod replay;
pub mod stub;
mod tls;
mod upstream;
pub mod wire;

pub use assertions::TrafficFilter;
pub use builder::{ProxyClusterBuilder, ReplayMissHandler};
pub use cluster::ClusterHandle;
pub use command::{Command, CommandResponse, CommandSender};
pub use config::{
    InboundTlsConfig, Mode, ProxyConfig, RecordingConfig, UpstreamTarget, UpstreamTlsConfig,
};
pub use context::{RequestContext, ResponseSource};
pub use middleware::{Next, ProxyMiddleware, SharedMiddleware, Terminal, TerminalFuture, shared};
/// Re-export of the JSON-Lines snapshot backend, available when the
/// `storage-jsonl` feature is on (which it is by default).
#[cfg(feature = "storage-jsonl")]
pub use partly_proxy_storage_jsonl as jsonl;
/// Re-export of the SQLite snapshot backend, available when the
/// `storage-sqlite` feature is on.
#[cfg(feature = "storage-sqlite")]
pub use partly_proxy_storage_sqlite as sqlite;
pub use partly_proxy_types::{
    ExchangeOutcome, InMemoryStorage, ProxyError, RecordedExchange, RecordedRequest,
    RecordedResponse, Result, SharedStorage, SnapshotStorage,
};
pub use proxy_io::{ProxyRequest, ProxyResponse};
pub use recorder::Recorder;
pub use stub::{RequestMatcher, StubEntry, StubStore, StubbedResponse};
pub use wire::{StubFields, WireCommand, WireFilter, WireResponse};
