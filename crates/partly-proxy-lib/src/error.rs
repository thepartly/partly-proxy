//! Error model — re-exported from `partly-proxy-types`.
//!
//! Lives here as a shim so internal `use crate::error::{ProxyError, Result}`
//! paths and downstream `partly_proxy_lib::error::ProxyError` imports keep
//! working without change. See `.scratch/MULTI_BACKEND_IMPLEMENTATION.md`
//! slice 2 for why the actual definitions moved out.

pub use partly_proxy_types::error::*;
