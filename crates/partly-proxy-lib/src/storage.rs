//! Snapshot storage trait — re-exported from `partly-proxy-types`.
//!
//! Lives here as a shim so internal `use crate::storage::{SnapshotStorage, …}`
//! paths and downstream `partly_proxy_lib::storage::*` imports keep working
//! without change. See `.scratch/MULTI_BACKEND_IMPLEMENTATION.md` slice 2.

pub use partly_proxy_types::storage::*;
