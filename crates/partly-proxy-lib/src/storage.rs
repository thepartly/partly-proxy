//! Snapshot storage trait — re-exported from `partly-proxy-types`.
//!
//! The actual definitions live in the shared types crate so backend
//! crates can implement [`SnapshotStorage`] without picking up
//! `partly-proxy-lib`'s heavy transitive dependency closure. This file
//! is a shim so internal `use crate::storage::{SnapshotStorage, …}`
//! paths and downstream `partly_proxy_lib::storage::*` imports keep
//! working without change.

pub use partly_proxy_types::storage::*;
