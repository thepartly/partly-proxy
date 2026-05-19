//! `RequestContext` — per-request state passed through the middleware chain.
//!
//! See `SPECIFICATION.md` §6.6. The `extensions` field uses `http::Extensions`,
//! which gives us typed `insert<T>` / `get<T>` for free.

use std::time::Instant;

use http::Extensions;
use uuid::Uuid;

/// Which terminal stage produced the response for this request.
///
/// Stamped by `LiveTerminal` after its routing decision and read back by
/// middleware (post-`next.run`) and by the response-emit path.
///
/// Absent from the context when a middleware short-circuited the chain and
/// the terminal never ran — middleware that need to distinguish their own
/// short-circuit from a terminal outcome should treat `None` as "produced
/// by middleware".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseSource {
    /// A registered stub matched and fired.
    Stub,
    /// A recorded exchange satisfied the replay lookup.
    Snapshot,
    /// `Mode::Replay` with no stub and no snapshot match — synthetic 503.
    ReplayMiss,
    /// `Mode::Record` — the request was forwarded to the real upstream.
    /// Stamped before the forward is awaited, so the marker is present
    /// even if the upstream call ultimately errors.
    Upstream,
}

/// Per-request bag of state passed mutably through each middleware.
#[derive(Debug)]
pub struct RequestContext {
    pub id: Uuid,
    pub started: Instant,
    extensions: Extensions,
}

impl Default for RequestContext {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestContext {
    /// Fresh context — new UUID v4, started=now, empty extensions.
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            started: Instant::now(),
            extensions: Extensions::new(),
        }
    }

    /// Insert a typed value. Returns the previous value of the same type, if
    /// any.
    pub fn insert<T: Clone + Send + Sync + 'static>(&mut self, value: T) -> Option<T> {
        self.extensions.insert(value)
    }

    /// Read a typed value by its concrete type, if present.
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.extensions.get::<T>()
    }

    /// Remove and return a typed value, if present.
    pub fn remove<T: Clone + Send + Sync + 'static>(&mut self) -> Option<T> {
        self.extensions.remove::<T>()
    }

    /// Direct access to the underlying extensions, e.g. to forward an
    /// inbound request's extensions wholesale.
    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    /// Mutable access to the underlying extensions.
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        &mut self.extensions
    }

    /// The terminal stage that produced the response, if the terminal ran.
    /// Returns `None` when a middleware short-circuited the chain.
    pub fn response_source(&self) -> Option<ResponseSource> {
        self.get::<ResponseSource>().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_context_has_unique_ids() {
        let a = RequestContext::new();
        let b = RequestContext::new();
        assert_ne!(a.id, b.id);
    }

    #[test]
    fn response_source_round_trips_each_variant() {
        for source in [
            ResponseSource::Stub,
            ResponseSource::Snapshot,
            ResponseSource::ReplayMiss,
            ResponseSource::Upstream,
        ] {
            let mut ctx = RequestContext::new();
            assert_eq!(ctx.response_source(), None);
            ctx.insert(source);
            assert_eq!(ctx.response_source(), Some(source));
        }
    }

    #[test]
    fn extension_round_trips_by_type() {
        #[derive(Clone, Debug, PartialEq)]
        struct Deadline(std::time::Duration);

        let mut ctx = RequestContext::new();
        assert!(ctx.get::<Deadline>().is_none());

        let prev = ctx.insert(Deadline(std::time::Duration::from_secs(3)));
        assert!(prev.is_none());

        assert_eq!(
            ctx.get::<Deadline>(),
            Some(&Deadline(std::time::Duration::from_secs(3)))
        );

        let removed = ctx.remove::<Deadline>();
        assert_eq!(removed, Some(Deadline(std::time::Duration::from_secs(3))));
        assert!(ctx.get::<Deadline>().is_none());
    }
}
