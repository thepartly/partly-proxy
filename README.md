# partly-proxy

A programmable HTTP/HTTPS proxy library for integration testing. Record real
upstream traffic, replay it deterministically, inject stubbed responses,
intercept and modify requests/responses via reqwest-style middleware,
simulate failure conditions, and orchestrate multiple upstreams under shared
control — both in-process (typed Rust API) and out-of-process (JSON-Lines
over TCP).

The full design is in [`SPECIFICATION.md`](SPECIFICATION.md).

## Workspace layout

```
api-proxy/
├── crates/
│   ├── partly-proxy-lib/       # the library (crates.io target)
│   │   └── examples/host.rs    # minimal env-var-driven hosting binary
│   └── partly-proxy-echo/      # deterministic test upstream (used by lib tests)
├── ts-client/                  # TypeScript client (@partly/proxy-client, npm target)
├── scripts/
│   ├── test-unit.sh            # fmt + clippy + cargo test
│   └── test-ts.sh              # tsc + vitest
├── .github/workflows/ci.yml    # CI delegates to scripts/*.sh
└── SPECIFICATION.md
```

## Quick start (Rust)

```rust
use partly_proxy_lib::{
    Command, ProxyClusterBuilder, ProxyConfig, RecordingConfig,
    RequestMatcher, StubbedResponse, UpstreamTarget,
};
use bytes::Bytes;
use http::{Method, StatusCode};
use std::time::Duration;

#[tokio::main]
async fn main() -> partly_proxy_lib::Result<()> {
    let cluster = ProxyClusterBuilder::new()
        .recording(RecordingConfig::in_memory(10_000))
        .add_upstream(
            "api",
            ProxyConfig::http(
                "127.0.0.1:8080".parse().unwrap(),
                UpstreamTarget::new("https://api.upstream.example"),
            ),
        )
        .tcp_control_plane("127.0.0.1:4500".parse().unwrap())
        .run()
        .await?;

    // Register a stub at runtime.
    cluster
        .command_sender()
        .send(Command::Stub {
            upstream: Some("api".into()),
            matcher: RequestMatcher::new()
                .method(Method::POST)
                .path(r"^/orders/\d+/refund$"),
            response: StubbedResponse::new(StatusCode::CREATED)
                .header("content-type", "application/json")
                .body(Bytes::from_static(b"{\"ok\":true}")),
            times: Some(3),
        })
        .await?;

    // ... drive your system under test against http://127.0.0.1:8080 ...

    cluster.shutdown().await
}
```

## Quick start (TypeScript / Playwright)

```ts
import { ProxyClient } from "@partly/proxy-client";

const client = await ProxyClient.connect({ host: "127.0.0.1", port: 4500 });

await client.stub({
  upstream: "api",
  method: "POST",
  path_pattern: "^/orders/\\d+/refund$",
  status: 201,
  body: '{"ok":true}',
});

// ... run your browser test ...

const verdict = await client.assertSeen(
  { upstream: "api", path_pattern: "^/orders/\\d+/refund$" },
  5_000,
);
if (!verdict.passed) throw new Error(verdict.message);
await client.close();
```

## Running the tests

Every scenario the spec describes is covered by an integration test against
a real TCP socket / hyper listener — no mocked HTTP clients in the hot
paths.

```bash
# Rust: fmt + clippy (with -D warnings) + build + cargo test
scripts/test-unit.sh

# TypeScript client: tsc + vitest
scripts/test-ts.sh
```

Both scripts are the source of truth for what CI runs. The GitHub Actions
workflow in [`.github/workflows/ci.yml`](.github/workflows/ci.yml) delegates
to them.

## Feature coverage

| Capability                                    | Notes                                              |
| --------------------------------------------- | -------------------------------------------------- |
| Plain HTTP listener + forwarder (hyper 1.x)   | HTTP/1.1 + HTTP/2 auto-negotiation                 |
| Inbound + outbound TLS (rustls)               | Custom CAs and `accept_invalid_certs` for testing  |
| Recorder + pluggable snapshot storage         | NDJSON / SQLite                                    |
| Replay (`MethodPathAndBodyHash` + `Custom`)   | O(1) indexed lookup; goes through redaction hooks  |
| Middleware chain with `Next<'_>`              | Body rewrites, short-circuit, error recovery       |
| Stubs + in-process command plane              | Fire-count, artificial delay, pause/resume         |
| TCP JSON-Lines control plane                  | Same command set, cross-language harnesses         |
| Wait-for `AssertSeen` / `AssertCount`         | Overshoot terminates fast                          |
| Hosting example (`examples/host.rs`)          | Env-var-driven; ~30 lines                          |
| TypeScript client + vitest                    | Mock + real-binary e2e suites                      |
| OpenTelemetry (`otel_0_*` features)           | W3C extraction + opt-in upstream injection         |

## OpenTelemetry

Tracing/instrumentation support is feature-gated and off by default. The
library:

- Extracts the W3C `traceparent`/`tracestate` from inbound requests and
  parents a server span on the incoming context (opt-out per upstream
  via `without_otel_extraction`, or per request via `with_otel_filter`).
- Injects the resulting context onto the response so callers can
  correlate (equivalent of the older `OtelInResponseLayer`).
- **Does not** inject context onto outbound requests unless explicitly
  asked via `with_otel_propagation_to_upstream` on the upstream's
  `ProxyConfig`.
- Records HTTP attributes per the current OTEL semantic conventions
  (`http.request.method`, `http.response.status_code`, `http.route` =
  upstream name, `url.path`, `url.scheme`, etc.) and maps 5xx responses
  to `Status::Error`.

The library does **not** install a tracer provider, exporter,
propagator, or `tracing-subscriber`. That is the host binary's
responsibility — it has full control over which exporter, sampler, and
resource attributes to use. The lib just consults
`opentelemetry::global` for whatever the host has set up.

### Version pinning

The OTEL Rust crates ship breaking changes at every 0.x bump and
ecosystem crates can be stuck on different versions for months. One
Cargo feature per OTEL minor lets the lib track multiple minors as the
ecosystem migrates:

| Feature      | `opentelemetry` minor | Sibling crates                                                                 |
| ------------ | --------------------- | ------------------------------------------------------------------------------ |
| `otel_0_27`  | 0.27.x                | `opentelemetry-http` 0.27, `opentelemetry-semantic-conventions` 0.27, `tracing-opentelemetry` 0.28 |

Only one `otel_0_*` feature may be enabled at a time — `lib.rs` carries
a `compile_error!` guard that trips when more than one is selected
(the host's installed propagator must match the lib's compiled-in OTEL
version to round-trip context). Future versions are additive: a new
`otel_0_28` feature, new dep renames, and a new `v0_28.rs` impl module
all sit alongside the existing ones with no renames.

```toml
[dependencies]
partly-proxy-lib = { version = "0.1", features = ["otel_0_27"] }
```

In the host binary, install a tracer provider, propagator, and
`tracing-subscriber` against the matching `opentelemetry` minor; the
lib will see them via `opentelemetry::global`.
