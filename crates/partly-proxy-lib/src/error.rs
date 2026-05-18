//! Error model — re-exported from `partly-proxy-types`.
//!
//! The actual definitions live in the shared types crate so storage
//! backends can depend on `ProxyError` / `Result` without picking up
//! `partly-proxy-lib`'s heavy transitive dependency closure. This file
//! is a shim so internal `use crate::error::{ProxyError, Result}` paths
//! and downstream `partly_proxy_lib::error::ProxyError` imports keep
//! working without change.

pub use partly_proxy_types::error::*;
