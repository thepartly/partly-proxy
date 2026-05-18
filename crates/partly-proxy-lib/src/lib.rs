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
pub mod error;
mod forwarder;
mod listener;
pub mod middleware;
pub mod proxy_io;
pub mod recorded;
pub mod recorder;
pub mod stub;
mod upstream;

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
pub use stub::{RequestMatcher, StubEntry, StubStore, StubbedResponse};
