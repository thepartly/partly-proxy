//! Recorded data model — re-exported from `partly-proxy-types`.
//!
//! The actual definitions live in the shared types crate so storage
//! backends can depend on `RecordedExchange` without picking up
//! `partly-proxy-lib`'s heavy transitive dependency closure. This file
//! is a shim so internal `use crate::recorded::{RecordedExchange, …}`
//! paths and downstream `partly_proxy_lib::recorded::*` imports keep
//! working without change.

pub use partly_proxy_types::recorded::*;
