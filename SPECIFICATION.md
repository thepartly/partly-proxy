# `proxy-lib` — Specification

A programmable HTTP/HTTPS proxy library for integration testing. It can record real upstream traffic, replay it deterministically, inject stubbed responses, intercept and modify requests/responses via reqwest-style middleware, simulate failure conditions, and orchestrate multiple upstreams under shared control — both in-process and over a TCP control plane.

---

## 1. Purpose

`proxy-lib` provides building blocks for tests that need to interact with — or mimic — an HTTP upstream. The same proxy instance can:

- Forward requests to a real upstream and **record** every exchange to memory and/or disk.
- **Replay** recorded exchanges deterministically against a dead or absent upstream.
- Serve **stubbed** responses for ad-hoc mocking, without a recording phase.
- Run **middleware** (reqwest-style, with a `next()` call) that can modify, short-circuit, or recover from any request/response in a single function.
- Operate as a **cluster** of named upstreams under shared control, so a single test can simulate a multi-service environment.
- Be driven either **in-process** (typed Rust API) or **out-of-process** (JSON-over-TCP control plane).
- Make **assertions** over recorded traffic via the control plane, which blocks until the predicate holds or a caller-supplied timeout elapses.

The design centres on a single request lifecycle through which all behaviours — record, replay, stub, middleware, forward — compose as layered stages.

---

## 2. Feature Summary

| Capability |
|------------|
| Record live upstream traffic — in-memory ring buffer, optional NDJSON disk persistence |
| Replay from recorded snapshots — indexed by method+path+body-hash; custom matcher supported |
| Body-aware replay matching — SHA-256 body hash |
| Inject runtime stubs — with optional fire-count limit and artificial delay |
| Pause/resume traffic — globally or per upstream |
| Clear stubs / recordings at runtime |
| Modify request before forwarding — via middleware (mutate before `next.run`) |
| Short-circuit with synthetic response — via middleware (return without calling `next`) |
| Modify response before returning — via middleware (mutate after `next.run`) |
| Recover from upstream errors — via middleware (catch the `Err` from `next.run`) |
| Multiple upstreams under one cluster — each named, independently bound, independently controlled |
| Global middleware across all upstreams |
| Per-upstream middleware — appended after global middleware |
| Upstream-scoped commands — stub, pause, resume, shutdown, status |
| Upstream-scoped assertions |
| Regex path matching for filters |
| Inbound TLS (proxy serves HTTPS) — PEM cert + key per upstream |
| Outbound TLS (HTTPS upstreams) — system roots + custom CA, with `accept_invalid_certs` toggle |
| External control protocol — JSON-Lines over TCP |
| Health / readiness endpoints — provided by the runner binary, queries cluster status |
| Configurable per-upstream timeouts — declared on the config type |

---

## 3. Configuration

### 3.1 `ProxyConfig`

One `ProxyConfig` describes a single proxy listener bound to a single upstream:

| Field | Meaning |
|-------|---------|
| `bind_addr: SocketAddr` | The address the proxy listens on |
| `upstream: UpstreamTarget` | The upstream this listener forwards to |
| `inbound_tls: Option<InboundTlsConfig>` | If set, the listener serves HTTPS |

### 3.2 `UpstreamTarget`

| Field | Meaning |
|-------|---------|
| `base_url: String` | Scheme + host (+ optional port and path prefix) of the upstream |
| `host_header: Option<String>` | If set, overrides the `Host:` header sent to the upstream |
| `connect_timeout: Duration` | Declared; default 10s. |
| `request_timeout: Duration` | Declared; default 30s. |
| `tls: Option<UpstreamTlsConfig>` | Outbound TLS settings when the upstream is HTTPS |

Scheme is auto-detected from `base_url` — HTTP and HTTPS upstreams use the same `UpstreamTarget` type.

### 3.3 `RecordingConfig`

| Field | Default | Meaning |
|-------|---------|---------|
| `enabled: bool` | `true` | Whether exchanges are recorded |
| `max_in_memory: usize` | `10_000` | Cap for the in-memory ring buffer (FIFO eviction) |

`RecordingConfig` controls only the in-memory ring. Durable persistence — NDJSON file, SQLite database, or anything else implementing `SnapshotStorage` — is configured separately by passing a `SharedStorage` to `Recorder::with_storage` or `ProxyClusterBuilder::storage(...)`. Mixing the two concerns into a single `persist_path` field would couple the recording cap to one specific backend; the split keeps both axes independent.

### 3.4 `UpstreamTlsConfig`

| Field | Meaning |
|-------|---------|
| `accept_invalid_certs: bool` | Disable cert verification (e.g. for self-signed test upstreams) |
| `custom_ca_cert: Option<PathBuf>` | PEM file with extra trust anchors, merged with the standard root store |

`accept_invalid_certs = true` short-circuits verification, so `custom_ca_cert` is ignored in that mode.

### 3.5 `InboundTlsConfig`

| Field | Meaning |
|-------|---------|
| `cert_path: PathBuf` | PEM cert chain |
| `key_path: PathBuf` | PEM private key (PKCS#8, PKCS#1, or SEC1) |

One certificate per listener; no SNI multiplexing.

---

## 4. Cluster Construction

Everything is built through `ProxyClusterBuilder`:

```rust
let cluster = ProxyClusterBuilder::new()
    .recording(RecordingConfig { /* … */ })
    .add_middleware(GlobalAuthMiddleware)                  // applies to all upstreams
    .add_upstream("api", api_config)                       // no middleware, no replay
    .add_upstream_with_middleware("billing", b_cfg, b_mw)  // per-upstream middleware
    .add_upstream_with("legacy", l_cfg, l_mw, Some(replay_source))
    .run()
    .await?;
```

`run()` binds every listener, starts a shared recorder and command processor, and returns a `ClusterHandle` exposing:

- `addr(name) -> Option<SocketAddr>` — the bound address for a named upstream.
- `upstream_names() -> Vec<&str>` — all registered upstreams.
- `recorder: Recorder` — shared traffic store.
- `command_sender: CommandSender` — in-process command channel.
- `shutdown().await -> Result<()>` — broadcasts a shutdown command, joins every listener task.

Assertions are not exposed as a Rust API. They are driven exclusively through the control plane (see §14), so the same harness works in-process or out-of-process.

### 4.1 Shared vs. per-upstream state

| Shared across cluster | Per upstream |
|----------------------|--------------|
| Recorder (single buffer + persist file) | Forwarder and connection pool |
| Command channel and processor | Middleware chain (global middleware + that upstream's middleware, in that order) |
| Global middleware | Active stubs |
| | Pause flag and resume signal |
| | Optional replay source |
| | Optional inbound TLS acceptor |

---

## 5. Request Lifecycle

Each incoming request flows through the following ordered stages. Earlier stages can terminate the request; later stages only run if the request gets there.

1. **TLS handshake** — if `inbound_tls` is configured on this upstream.
2. **HTTP negotiation** — HTTP/1.1 or HTTP/2 (auto).
3. **Pause gate** — if the upstream is paused, the request waits until `Resume` is sent.
4. **Body collection** — request body is buffered into bytes for hash-based matching.
5. **Middleware chain** — global middleware then per-upstream middleware, composed via `Next`. Each middleware decides whether to call `next.run(req, ctx).await`. The innermost call falls through to the terminal stages below.
6. **Terminal: Stub scan** — first matching active stub wins. Honours its optional artificial `delay`. Decrements its `times` counter; removes the stub when exhausted.
7. **Terminal: Replay lookup** — if a replay source is configured, the proxy makes a working copy of the request, runs `redact_request_for_snapshot` across the middleware chain on that copy, then looks the copy up by the chosen match strategy. A hit returns the recorded response. The original request is unchanged.
8. **Terminal: Forward to upstream** — if no stub and no replay hit. A failure here surfaces as `Err(ProxyError::Upstream*)` back through the middleware chain, where any middleware can catch and recover. If no middleware catches, the proxy returns `502 Bad Gateway`.
9. **Record** — if recording is enabled, the exchange (request + final response or error) is cloned, run through `redact_request_for_snapshot` / `redact_response_for_snapshot` across the middleware chain, and only then hashed, serialised, and recorded under this upstream's name. Recording sees the response *after* middleware `handle` has finished modifying it; the redaction step is on top of that and only affects the persisted copy, not what the client receives.

**Priority of response sources:** middleware short-circuit > stub > replay > upstream. A middleware that synthesises a response wins over everything below; a stub overrides a matching replay snapshot.

---

## 6. Middleware

Request interception is expressed as **middleware**:

```rust
#[async_trait]
pub trait ProxyMiddleware: Send + Sync + 'static {
    async fn handle(
        &self,
        req: ProxyRequest,
        ctx: &mut RequestContext,
        next: Next<'_>,
    ) -> Result<ProxyResponse, ProxyError>;
}
```

### 6.1 Bodies are fully materialised `Bytes`

Middleware is **not** parameterised over a streaming `Body` type. By the time the chain runs, the request body has already been collected (lifecycle stage 4), and any response — synthetic, stubbed, replayed, or forwarded — is also collected before it re-enters the chain on the way back. The `ProxyRequest` and `ProxyResponse` types middleware see therefore carry concrete, in-memory bodies:

```rust
pub struct ProxyRequest {
    pub method: http::Method,
    pub uri: http::Uri,
    pub headers: http::HeaderMap,
    pub body: bytes::Bytes,
    // ...
}

pub struct ProxyResponse {
    pub status: http::StatusCode,
    pub headers: http::HeaderMap,
    pub body: bytes::Bytes,
    // ...
}
```

Middleware receives `ProxyRequest` by value (so `req.body` can be moved or replaced wholesale) and `ProxyResponse` by value from `next.run(...).await` (so `resp.body` is equally free to mutate). Convenience accessors are provided — `req.body_mut() -> &mut Bytes`, `resp.body_mut() -> &mut Bytes`, plus `take_body()` / `set_body(impl Into<Bytes>)` — but the underlying field is always a plain `Bytes`. There is no `Body`, `Stream<Item = ...>`, `BoxBody`, or `impl AsyncRead` wrapper to peel away. A middleware can hash, rewrite, decompress, JSON-parse, or replace the body in one line, and pass the result onward.

This is a deliberate trade against streaming: the proxy targets integration testing, where full buffering keeps middleware code straightforward, supports body-hash matching and recording without re-reading, and avoids the cancellation/EOF edge cases of pass-through streams. Very large bodies should be tested out-of-band.

### 6.2 Using `Next`

`Next<'_>` is a cursor over the remaining middleware plus the terminal stages (stub → replay → forward). A middleware chooses what to do with it:

- **Pass through.** `let resp = next.run(req, ctx).await?;` advances one step. If this is the last middleware, `next.run` drives stub lookup, replay, and forwarding.
- **Inspect or rewrite the request body.** `req.body` is `Bytes`. Hash it, parse it, rewrite it (`req.body = new_bytes;`), then pass `req` to `next.run`.
- **Inspect or rewrite the response body.** Take the `ProxyResponse` returned by `next.run(...).await` and mutate `resp.body` directly. The next middleware up sees your edits as the new body.
- **Short-circuit.** Build a `ProxyResponse` (with whatever `Bytes` body you want) and return `Ok(response)` without ever calling `next` — stub/replay/forward never run.
- **Recover from errors.** Match on the `Err` from `next.run(...).await` and return `Ok(recovery)` instead. This replaces the old `on_error` hook.
- **Fail.** Return `Err(ProxyError::Middleware(...))` to abort the request.

A body-rewriting example:

```rust
struct RedactSecrets;

#[async_trait]
impl ProxyMiddleware for RedactSecrets {
    async fn handle(
        &self,
        mut req: ProxyRequest,
        ctx: &mut RequestContext,
        next: Next<'_>,
    ) -> Result<ProxyResponse, ProxyError> {
        // mutable Bytes handle on the request body — no Body abstraction in the way
        if let Ok(text) = std::str::from_utf8(&req.body) {
            if text.contains("password") {
                req.body = Bytes::from(text.replace("password", "[REDACTED]"));
            }
        }

        let mut resp = next.run(req, ctx).await?;

        // same story on the response — direct Bytes
        if resp.headers.get("content-type").map(|v| v == "application/json").unwrap_or(false) {
            resp.body = redact_json(&resp.body);
        }
        Ok(resp)
    }
}
```

### 6.3 Chain composition

Middleware are evaluated in registration order: global middleware first (in the order added on the builder), then per-upstream middleware (in the order passed to `add_upstream_with_*`), then the terminal stages. Earlier middleware wrap later middleware and the terminal stages through `next`.

There is no "first non-`Continue` wins" rule — composition is structural. A middleware that doesn't call `next` short-circuits; a middleware that does is transparent to whatever happens further down. Errors propagate through `next.run().await` unless an upstream middleware catches them.

### 6.4 Snapshot-boundary redaction

The same middleware trait carries two **optional** synchronous hooks that fire only at the recording/replay boundary, not in the live request path:

```rust
#[async_trait]
pub trait ProxyMiddleware: Send + Sync + 'static {
    async fn handle(
        &self,
        req: ProxyRequest,
        ctx: &mut RequestContext,
        next: Next<'_>,
    ) -> Result<ProxyResponse, ProxyError>;

    /// Mutate the request immediately before it crosses the snapshot boundary.
    /// Called in TWO places:
    ///   1. Just before an exchange is persisted (recording).
    ///   2. Just before a live request is looked up in a replay source.
    /// Because the same transform runs on both sides, the body hash matches
    /// and `MethodPathAndBodyHash` lookups still succeed.
    /// Default: no-op.
    fn redact_request_for_snapshot(&self, _req: &mut ProxyRequest) {}

    /// Mutate the response immediately before it is persisted to a snapshot.
    /// Replay returns the already-redacted on-disk body untouched, so there
    /// is no symmetric "on read" call for responses.
    /// Default: no-op.
    fn redact_response_for_snapshot(&self, _resp: &mut ProxyResponse) {}
}
```

**There is exactly one middleware chain.** The same registered middleware list provides both the live request behaviour (via `handle`) and the snapshot redaction (via the two sync methods). A middleware that only redacts overrides nothing else and gets a pass-through `handle`:

```rust
struct StripAuth;

#[async_trait]
impl ProxyMiddleware for StripAuth {
    async fn handle(
        &self,
        req: ProxyRequest,
        ctx: &mut RequestContext,
        next: Next<'_>,
    ) -> Result<ProxyResponse, ProxyError> {
        next.run(req, ctx).await           // live path: do nothing
    }

    fn redact_request_for_snapshot(&self, req: &mut ProxyRequest) {
        req.headers.remove("authorization");
        req.headers.remove("cookie");
        if is_json(&req.headers) {
            req.body = redact_json_field(&req.body, "api_key");
        }
    }

    fn redact_response_for_snapshot(&self, resp: &mut ProxyResponse) {
        resp.headers.remove("set-cookie");
    }
}
```

#### When and where it runs

- **On record** (lifecycle stage 9): the proxy clones the request/response into a working `ProxyRequest` / `ProxyResponse`, runs `redact_request_for_snapshot` and `redact_response_for_snapshot` across the chain in registration order, **then** computes the body hash, serialises, and writes to the in-memory ring and `persist_path`. The original request/response returned to the client is untouched — the live caller still sees its `Authorization` header.
- **On replay lookup** (lifecycle stage 7): before the live `ProxyRequest` is used to compute the lookup key, the proxy runs `redact_request_for_snapshot` across the chain on a working copy. The key is computed from the redacted copy. Because the snapshot on disk was written with the same redaction applied, the hashes agree and the lookup hits. The original request continues into the chain unmodified.

Both calls are infallible by design — they are pure rewrites, not policy decisions. If a middleware needs to fail-stop on a missing field, it should do so in `handle`, not here.

#### Invariants the caller must preserve

For `MethodPathAndBodyHash` to keep matching, redaction must be **deterministic**: the same input bytes must produce the same output bytes every time, regardless of clock, randomness, or process. Two practical rules follow:

- Don't introduce non-determinism (random tokens, timestamps, generated IDs) — replace them with a fixed placeholder.
- Apply the redaction transform consistently across versions; changing the transform is equivalent to invalidating all prior snapshots indexed by body hash.

### 6.5 Lifecycle events

Server bring-up and shutdown are not middleware concerns. `ProxyClusterBuilder::run().await` and `ClusterHandle::shutdown().await` return when binding and teardown are complete; any setup or teardown that previously lived in `on_startup` / `on_shutdown` belongs in the caller, wrapped around those awaits.

### 6.6 Request context

Each request carries a `RequestContext` that middleware receive mutably and that flows through `next.run` to downstream middleware. It contains:

- A unique UUID per request.
- A start `Instant`.
- A typed extension map (`insert<T>(key, value)`, `get<T>(key) -> Option<&T>`) so middleware can pass state forward (e.g. an upstream middleware stashes a deadline that a downstream middleware reads) without globals.

---

## 7. Stubs

A stub binds a matcher to a canned response:

```rust
command_sender.send(Command::Stub {
    upstream: Some("api".into()),
    matcher: RequestMatcher::default()
        .method(Method::POST)
        .path(r"^/orders/\d+/refund$")
        .header("x-tenant", "acme")
        .body_contains("\"reason\":\"chargeback\""),
    response: StubbedResponse::new(StatusCode::CREATED)
        .header("content-type", "application/json")
        .body(br#"{"ok":true}"#.to_vec())
        .delay(Duration::from_millis(50)),
    times: Some(3),  // None for unlimited
}).await?;
```

### 7.1 Matcher semantics

A stub matches a request when **all** of the following hold (any unset field is ignored):

- `method` — exact match.
- `path_pattern` — regex match against the URI path if the pattern compiles, otherwise exact-string match.
- `header_contains` — every entry must be present, with the supplied value found as a substring of the header value.
- `body_contains` — UTF-8-lossy substring search against the request body.

### 7.2 Fire-count and delay

- `times: Some(n)` — the stub fires up to `n` times, then auto-removes.
- `times: None` — the stub fires indefinitely.
- `delay: Some(d)` — the proxy waits `d` before returning the stub response (useful for testing timeouts and concurrency behaviour).

### 7.3 Stub management commands

- `Command::Stub { upstream, … }` — register a new stub. In a multi-upstream cluster, `upstream` is required; with exactly one upstream, `None` is implicitly that upstream.
- `Command::ClearStubs { upstream }` — clear stubs for one upstream, or for all if `None`.

---

## 8. Replay

A `ReplaySource` is an immutable snapshot of recorded exchanges with a chosen match strategy:

```rust
let replay = ReplaySource::new(exchanges, MatchStrategy::MethodPathAndBodyHash);
// or
let replay = ReplaySource::from_jsonl(path, MatchStrategy::MethodPathAndBodyHash)?;
```

### 8.1 Match strategies

| Strategy | Key | Notes |
|----------|-----|-------|
| `MethodPathAndBodyHash` (default) | (method, path, SHA-256 hex of body) | Distinguishes identical endpoints called with different payloads |
| `Custom(closure)` | arbitrary | `Fn(&RecordedRequest, &Request<Bytes>) -> bool` — falls back to linear scan |

`MethodPathAndBodyHash` builds an index at construction time for O(1) lookup. `Custom` is the only other supported strategy; coarser keys (method-only, method+path, method+URI) are intentionally not provided — callers who want those semantics express them as a `Custom` closure.

### 8.1.1 Scale target

Replay must remain usable with snapshot files containing **10,000 to 100,000 exchanges** — these are realistic sizes for a recorded end-to-end suite, not a worst case to be discouraged. Concretely:

- `ReplaySource::from_jsonl(...)` parses a 100k-line file in a single pass; it does not hold the whole file in a `String` and must stream line-by-line (e.g. `BufReader::lines`) to keep peak memory bounded by the largest single exchange, not the file size.
- Index construction for `MethodPathAndBodyHash` is O(n) in the number of exchanges; lookup remains O(1) per request regardless of snapshot size. Hash-map capacity should be preallocated from the exchange count to avoid repeated rehashing during load.
- `Custom` matchers fall back to a linear scan, which is O(n) per request. With a 100k-exchange snapshot this is the slow path; use it sparingly or pre-filter via the upstream/path before invoking the custom predicate.
- Memory budget at 100k exchanges with typical JSON payloads (~1–4 KiB body each) is on the order of hundreds of MiB. The proxy keeps decoded `Bytes` bodies in the source verbatim — there is no per-exchange duplication into the recorder unless `Replay + recording` is enabled.

### 8.2 Reusability

Snapshots are **reusable**, not one-shot — a single recorded response can match an unbounded number of incoming requests. There is no consumption tracking; concurrent identical requests all receive the same response.

### 8.2.1 Lookup goes through `redact_request_for_snapshot`

Before the lookup key is computed, the live request is run through every middleware's `redact_request_for_snapshot` (see §6.4). The hash used to index into the snapshot is derived from the redacted body, which matches what was written on the record side. This is how a snapshot recorded with `Authorization: Bearer abc` stripped still matches an incoming request that carries `Authorization: Bearer xyz`.

The redaction is applied to a working copy; the live request handed to stub/forward stages is untouched.

### 8.3 Mode interactions

Replay is always layered with middleware and stubs. There are exactly two supported configurations:

- **Replay + middleware + stubs**: configure a replay source, register stubs over the command plane, and run middleware in front. Stubs take priority over replay, so a test can override specific calls without rebuilding the snapshot; middleware wraps the terminal stages, so a replayed response is observable and rewritable exactly like an upstream response. The upstream itself can be unreachable; unmatched requests (no middleware short-circuit, no stub hit, no replay hit, no upstream) yield 502.
- **Replay + middleware + stubs + recording**: as above, plus a `RecordingConfig`. Every served exchange — whether the response came from a middleware short-circuit, a stub, or replay — is recorded under the upstream name, so a session can be observed and re-snapshotted while it runs.

Replay-only (no middleware, no stubs) and replay+recording-only configurations are not supported as standalone modes; in practice every test wants at least middleware in the chain, and stubs cost nothing when none are registered.

---

## 9. Recording

### 9.1 Recorded data model

- `RecordedRequest`: method, URI, headers (binary values are stringified as `<binary>`), body bytes, and the body's lowercase hex SHA-256.
- `RecordedResponse`: status, headers, body bytes.
- `RecordedExchange`: unique id, optional `upstream` name (set in cluster mode), timestamp, duration, request, **either** a response or an error string, and a string-keyed `labels` map for caller-supplied metadata.

Bodies serialise as base64 in JSON; the NDJSON format is round-trippable into a `ReplaySource`.

A single recording session can produce **10,000 to 100,000 exchanges** in one NDJSON file — long-running end-to-end suites realistically generate this volume — and the format must remain usable at that scale. Concretely:

- The on-disk format is strictly one exchange per line, append-only. Loading a 100k-exchange file is a single streaming pass (no whole-file parse, no JSON-array wrapper).
- `persist_path` writes are append-only and per-exchange — a long suite never rewrites earlier lines, so the file grows linearly and is safe to truncate or `tail -f` mid-run.
- Round-tripping a 100k-line NDJSON file into a `ReplaySource` is supported and exercised; see §8.1.1 for the loader's complexity properties.

### 9.2 Recorder API

The shared `Recorder` is cheaply cloneable and exposes async methods:

- `record(exchange)` — insert (also appends to disk if `persist_path` is set). Before insertion, the exchange is passed through every middleware's `redact_request_for_snapshot` / `redact_response_for_snapshot` (see §6.4), so secrets are stripped before bytes leave the recorder. The body hash stored on the recorded request is computed *after* redaction, which is what makes hash-based replay lookups continue to work.
- `exchanges()` — clone the full buffer.
- `len()`, `clear()`.
- `any_matching(pred)`, `count_matching(pred)`, `find_matching(pred)` — predicate-based scans.

When the in-memory buffer is full, the oldest exchange is evicted.

### 9.3 Provenance

Each exchange is tagged with the upstream that served it. This is what makes per-upstream filtering and scoped assertions possible without restarting the cluster.

---

## 10. Forwarding

When a request reaches the forwarding stage:

- The URI is rewritten as `base_url + path_and_query` from the client request.
- If `host_header` is set on the upstream, the `Host:` header is overwritten.
- All other headers pass through unchanged.
- The outbound client negotiates HTTP/1.1 or HTTP/2 and maintains a per-authority connection pool. Scheme (HTTP vs. HTTPS) is auto-detected.
- Connection failures map to `UpstreamConnect`; body collection failures map to `UpstreamRequest`. Both propagate as `Err` from `next.run(...).await` through the middleware chain, and yield 502 to the client unless a middleware catches the error and returns a recovery response.

---

## 11. TLS

### 11.1 Outbound

- Default: rustls with the standard Mozilla root store.
- `custom_ca_cert` appends additional trust anchors from a PEM file.
- `accept_invalid_certs = true` installs a verifier that accepts every certificate (use for self-signed test upstreams only).
- No mTLS — outbound client certificates are not configurable.

### 11.2 Inbound

- A `TlsAcceptor` is built from the PEM cert chain and private key.
- One certificate per listener (no SNI).
- No mTLS — incoming client certificates are not requested or verified.

---

## 12. Control Plane

The same set of commands can be issued in two ways.

### 12.1 In-process — `CommandSender`

`cluster.command_sender` is an async sender backed by a bounded mpsc channel. Each `send(cmd)` awaits a typed `CommandResponse`. Commands and their responses:

| Command | Scope | Response |
|---------|-------|----------|
| `Stub { upstream, matcher, response, times }` | Per upstream | `Ok` / `Error` |
| `ClearStubs { upstream }` | Per upstream (or all) | `Ok` |
| `Pause { upstream }` | Per upstream (or all) | `Ok` |
| `Resume { upstream }` | Per upstream (or all) | `Ok` |
| `AssertSeen { filter, timeout }` | Recorder-wide (filter narrows) | `AssertionResult { passed, message }` |
| `AssertCount { filter, expected, timeout }` | Recorder-wide | `AssertionResult { passed, message }` |
| `QueryTraffic { filter }` | Recorder-wide | `Exchanges(Vec<RecordedExchange>)` |
| `ClearRecordings` | Global | `Ok` |

When a command supports an upstream scope and exactly one upstream is configured, `upstream: None` is taken to mean "that one upstream". With multiple upstreams, scoped commands (notably `Stub`) require an explicit name.

`AssertSeen` and `AssertCount` are evaluated against the live recorder and **block** until either the predicate holds or the supplied `timeout` elapses. The response is only written once one of those two conditions is reached: a passing result the moment the condition is first satisfied, or a failing result at timeout describing the observed state. A `timeout` of zero collapses to a single immediate evaluation. This lets an external harness say "I have just kicked off work that should result in `POST /orders`; wait up to 5 seconds for it" in one round-trip, without polling.

### 12.2 Out-of-process — JSON-Lines over TCP

A TCP adapter exposes the same command set over newline-delimited JSON, suitable for cross-process or cross-language harnesses. Each connection accepts one command per line and replies with one response line. Example:

```
$ nc 127.0.0.1 4500
{"type":"Stub","upstream":"api","method":"GET","path_pattern":"^/health$","status":200,"body":"ok"}
{"type":"Ok"}
{"type":"AssertCount","upstream":"api","path_pattern":"^/health$","expected":1,"timeout_ms":5000}
{"type":"AssertionResult","passed":true,"message":"…"}
```

Wire commands cover `Stub`, `ClearStubs`, `Pause`, `Resume`, `AssertSeen`, `AssertCount`, `QueryTraffic`, `ClearRecordings`. Wire responses are `Ok`, `Error{message}`, `Exchanges{exchanges}`, `Status{status}`, and `AssertionResult{passed,message}`.

Because `AssertSeen` and `AssertCount` block until the assertion holds or the supplied `timeout_ms` elapses, an external harness can use them as synchronisation primitives — "wait until the system under test has actually made the call I expect" — without polling `QueryTraffic` in a loop. The HTTP/TCP response is the test verdict.

### 12.3 TypeScript client for Playwright

A first-party TypeScript client wraps the JSON-Lines protocol and ships alongside the Rust crate. It is the intended way to drive the proxy from a Playwright suite: tests can register stubs, pause/resume traffic, query recorded exchanges, and `await` blocking assertions with the same `timeout_ms` semantics described in §14, all from the same Node process that runs the browser harness. The client exposes typed wrappers for every wire command and response — no raw socket handling required.

---

## 13. Traffic Filtering

`TrafficFilter` is the common selector used by `AssertSeen`, `AssertCount`, and `QueryTraffic`. All conditions AND together; unset conditions match anything.

| Field | Match |
|-------|-------|
| `upstream` | Exact upstream name |
| `method` | Exact HTTP method string |
| `path_pattern` | Regex against the URI path (falls back to exact string if it does not compile) |
| `status` | Exact response status; only matches successful exchanges (those with a response) |
| `labels` | Every key/value must be present in the exchange's `labels` map |

---

## 14. Assertions

Assertions are not exposed as a typed Rust API. They are issued **only** through the control plane (§12), so an external harness — in any language, in or out of process — declares expectations exactly the same way as an in-process caller would. The harness opens a TCP connection (or uses the in-process `CommandSender`), sends an `AssertSeen` / `AssertCount` command carrying a `TrafficFilter` and a `timeout`, and reads back a single `AssertionResult { passed, message }`.

### 14.1 Wait-for semantics

`AssertSeen` and `AssertCount` are eventual: the proxy evaluates the predicate against the recorder as new exchanges arrive, and only writes its response when one of two things happens.

- **Satisfied first.** The predicate transitions to true (any matching exchange exists for `AssertSeen`; the match count equals `expected` for `AssertCount`). The response is written immediately with `passed: true`, so the harness's `read()` unblocks at the moment the system under test has actually done the thing.
- **Timeout first.** The supplied `timeout` elapses without the predicate becoming true. The response is written with `passed: false` and a `message` describing the observed state (current match count, last matching exchange, etc.). For `AssertCount`, an overshoot — more matches than `expected` — is also terminal: further traffic cannot bring the count back down, so the proxy fails fast rather than waiting out the clock.

A `timeout` of zero collapses to a single immediate evaluation, useful for "must already be true" checks. The proxy enforces no upper bound; the harness picks a duration appropriate to the action it just kicked off.

This shape lets a harness express "kick off action X, then assert that the resulting upstream call arrives within 5s" as a single round-trip rather than a poll loop on `QueryTraffic`.

### 14.2 Scope and filtering

There is no separate scoped-assertions handle. Per-upstream assertions are expressed by setting `upstream` on the `TrafficFilter` carried by the command (see §13). All filter fields AND together; unset fields match anything.

### 14.3 Errors

A failing predicate is reported in-band as `AssertionResult { passed: false, message }` — the command itself succeeded, the assertion did not. Transport-level failures (malformed JSON, connection dropped, proxy shutting down) are surfaced as a wire `Error { message }` response or, in-process, as a `ProxyError::Command(...)`.

---

## 15. Error Model

`ProxyError` covers every failure surface in the library:

| Variant | When it fires |
|---------|---------------|
| `Bind(io::Error)` | Listener cannot bind |
| `UpstreamConnect(err)` | Outbound TCP/TLS handshake or request setup failed |
| `UpstreamRequest(err)` | Upstream connection established but the request/response failed |
| `Middleware(err)` | A middleware returned an error |
| `Command(String)` | Command channel closed, response channel dropped, etc. |
| `Recording(io::Error)` | Persistence failed |
| `Tls(String)` | TLS configuration or PEM loading failed |
| `UnknownUpstream(String)` | Command targets an upstream that is not registered |
| `Shutdown(String)` | Shutdown sequence failed |
| `Other(err)` | Catch-all for converted external errors |

A type alias `Result<T> = std::result::Result<T, ProxyError>` is provided.

---

## 16. Lifecycle and Shutdown

- `ProxyClusterBuilder::run().await` binds all listeners and starts background tasks before returning the `ClusterHandle`. Any setup that previously lived in `on_startup` is the caller's responsibility — run it before or after this await as appropriate.
- The cluster runs until either `ClusterHandle::shutdown().await` is called, a `Command::Shutdown` is sent over either control plane, or the parent process exits.
- `ClusterHandle::shutdown_with_timeout(d)` is the graceful-shutdown entry point. `shutdown()` is a shorthand for `shutdown_with_timeout(Duration::from_secs(5))`. The contract:
  1. **Stop accepting.** Per-upstream accept loops and both control planes exit immediately, so no new connections are admitted.
  2. **Drain.** Each listener asks every in-flight connection to finish via `hyper`'s graceful close — `Connection: close` on HTTP/1, GOAWAY on HTTP/2. The current request completes; the connection is then closed by the peer. Pause-gated requests are unblocked at the start of the drain so they can complete within the budget.
  3. **Hard abort.** Connections still running after `d` have their futures dropped. Exchanges aborted at this point are **not** recorded, since `record(...)` is the last step of the request lifecycle.
  4. **Return.** `shutdown_with_timeout` returns within `d + 1s`. The 1s outer slack only fires if a listener task is wedged; in that case the task is aborted defensively and an error is reported in the joined `Result`.
- Any teardown that previously lived in `on_shutdown` is the caller's responsibility — run it after this await returns.

---

## 17. Concurrency Properties

- Accepted connections are spawned as independent tasks — there is no built-in concurrency cap. Backpressure comes from the upstream and from `hyper`'s HTTP/2 flow control.
- The command channel is bounded; senders await capacity when it is full.
- Recordings are protected by a single async read-write lock; multiple readers are allowed concurrently. The recorder is sized by `max_in_memory`, with FIFO eviction.
- Stubs are stored per upstream behind an async read-write lock — reads dominate, writes happen only on register / clear / fire-count decrement.
- Pause is an atomic boolean with a notification primitive for resume — wakes are not lost.

---

## 18. Hosting

The crate does not ship a hosting binary — wiring `ProxyClusterBuilder` into a `tokio::main` and waiting on `Ctrl+C` is ~30 lines that every deployment customises anyway (config-file parsing, health probes, metrics, structured logging). A worked example lives at `crates/partly-proxy-lib/examples/host.rs` and can be run via `cargo run --example host -p partly-proxy-lib`.

---

## 19. Non-Features and Caveats

- **mTLS.** Neither inbound nor outbound TLS supports client certificates.
- **SNI inbound.** One certificate per listener; SNI-based multiplexing is not supported.
- **Hop-by-hop headers.** Headers are forwarded as-is; `hyper`'s defaults handle `Content-Length` and `Transfer-Encoding` normalisation, but other hop-by-hop headers are not stripped.
- **One-shot replay.** Replay snapshots are reusable — there is no per-snapshot consumption tracking and no "all snapshots consumed" assertion.
- **Connection cap.** No semaphore on accepted connections; very high concurrency is bounded only by the OS and the upstream.
- **Telemetry.** Tracing is supported via `tracing`. OpenTelemetry support
  is feature-gated (`otel_0_*` features, off by default) and provides
  W3C TraceContext extraction, response-side injection, and opt-in
  upstream injection — see §21. The library does **not** install a
  tracer provider, exporter, propagator, or `tracing-subscriber`; the
  host binary owns that.

---

## 20. Typical Usage Patterns

### 20.1 Record once, replay forever

1. Configure one upstream with recording enabled and a `persist_path`.
2. Drive the system under test against the proxy. Real upstream traffic accumulates in NDJSON.
3. In future test runs, load the NDJSON via `ReplaySource::from_jsonl` and add it to the upstream. The real upstream is no longer needed.

### 20.2 Ad-hoc mock for a single test

1. Start a cluster with one upstream pointing at any URL (it will never be reached).
2. Register stubs over the command sender at the start of each test.
3. Run the system under test.
4. Assert by sending `AssertSeen` / `AssertCount` commands with a suitable timeout; clear stubs and recordings between tests.

### 20.3 Multi-service environment in one process

1. Add one upstream per service (`api`, `billing`, `auth`, …) — each binds its own port.
2. Use global middleware for cross-service concerns (e.g. injecting a tenant header).
3. Use per-upstream middleware or stubs for service-specific behaviour.
4. Issue assertion commands with `upstream: "billing"` on the filter to scope them to that service.

### 20.4 Cross-language harness

1. Start the proxy from Rust as usual.
2. Bind the JSON-Lines TCP adapter to a known port.
3. Have a non-Rust test harness open a TCP connection and issue commands as JSON lines. For Playwright suites, use the first-party TypeScript client (see §12.3) instead of hand-rolling the protocol.
4. The harness can stub, pause, query traffic, assert, and shut the proxy down without ever linking the Rust crate.

---

## 21. OpenTelemetry

Feature-gated, off by default. One Cargo feature per `opentelemetry`
minor version (`otel_0_27`, future `otel_0_28`, …) so the crate can
track several minors side-by-side as the OTEL Rust stack evolves. The
features are mutually exclusive — enabling more than one trips a
`compile_error!` — because `opentelemetry::global::*` is non-reentrant
across minor versions.

### 21.1 Responsibility split

The library does **not** install any tracer-side machinery. That is
the host binary's job: it picks the exporter (OTLP/gRPC, OTLP/HTTP, …),
the sampler, the resource attributes, the `TracerProvider`, the
`TextMapPropagator`, and the `tracing-subscriber` composition. The lib
calls `opentelemetry::global::get_text_map_propagator` and
`tracing::Span` methods from `tracing-opentelemetry`; whatever the host
installs is what it gets, including a no-op tracer if the host doesn't
install one.

### 21.2 Propagation contract

When any `otel_0_*` feature is enabled:

- **Inbound extraction (opt-out).** Each inbound request creates a
  server span. If the request carried a `traceparent`/`tracestate`,
  the span is parented to that context. Disable per upstream via
  `ProxyConfig::without_otel_extraction()`; skip individual requests
  via `ProxyConfig::with_otel_filter(|method, uri| -> bool)`.
- **Response injection (always, when a span was created).** The proxy
  emits a `traceparent` on the response so callers can correlate. This
  is the W3C-native equivalent of the older
  `axum-tracing-opentelemetry::OtelInResponseLayer`.
- **Outbound injection (opt-in).** Disabled by default. Enable per
  upstream via `ProxyConfig::with_otel_propagation_to_upstream()`. When
  off, the proxy does not add any tracing headers to forwarded
  requests; client-supplied tracing headers still flow through
  unchanged because the proxy forwards inbound headers as-is.

### 21.3 Span shape

Server span (kind `SERVER`):

- Name: `"{http.request.method} {http.route}"` where `http.route` is
  the upstream's configured name (the closest analogue to a route a
  proxy has and bounds attribute cardinality).
- Attributes: `http.request.method`, `http.response.status_code`,
  `http.route`, `url.path`, `url.query`, `url.scheme`,
  `server.address`/`server.port`, `client.address`/`client.port`,
  `user_agent.original`, `network.protocol.version`, plus
  `partly.proxy.upstream` as a namespaced custom attribute.
- `Status::Error` is set for 5xx responses; other outcomes leave the
  status at `Unset`.

Client span (kind `CLIENT`, child of the server span):

- Name: `"{http.request.method}"` per OTEL HTTP-client semconv.
- Attributes: `http.request.method`, `url.full`, `server.address`/
  `server.port` from the outbound URI, `http.response.status_code`,
  `partly.proxy.upstream`.

### 21.4 Control plane

The TCP JSON-Lines control plane is **not** traced — it isn't
user-facing HTTP traffic and the cardinality wouldn't be useful. Only
the per-upstream listeners are wired to the OTEL helpers.
