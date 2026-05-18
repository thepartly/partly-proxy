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
│   ├── partly-proxy-runner/    # minimal hosting binary + /health, /ready
│   └── partly-proxy-echo/      # deterministic test upstream
├── ts-client/                  # TypeScript client (@partly/proxy-client, npm target)
├── docker/                     # Dockerfile + compose.yaml for the smoke stack
├── scripts/
│   ├── test-unit.sh            # fmt + clippy + cargo test
│   ├── test-ts.sh              # tsc + vitest
│   └── test-docker.sh          # compose-based smoke test (fallback)
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

The cargo test suite is the primary test path. Every scenario the spec
describes is covered by an integration test against a real TCP socket /
hyper listener — no mocked HTTP clients in the hot paths.

```bash
# Rust: fmt + clippy (with -D warnings) + build + cargo test
scripts/test-unit.sh

# TypeScript client: tsc + vitest
scripts/test-ts.sh

# Docker compose end-to-end smoke (fallback — slower)
scripts/test-docker.sh
```

All three scripts are the source of truth for what CI runs. The GitHub
Actions workflow in [`.github/workflows/ci.yml`](.github/workflows/ci.yml)
delegates to them.

## Implementation status

Built incrementally as twelve slices, each commit producing a green tree
(`cargo fmt --check`, `cargo clippy -D warnings`, full test suite passing):

| Slice | Description |
| ----- | ----------- |
| 1     | Workspace + config + error model |
| 2     | Plain HTTP listener + forwarder (hyper 1.x) |
| 3     | Recorder (in-memory ring + NDJSON append) |
| 4     | Middleware + `Next` + snapshot redaction hooks |
| 5     | Stubs + in-process command plane |
| 6     | Replay (MethodPathAndBodyHash + Custom closures) |
| 7     | TCP JSON-Lines control plane |
| 8     | Wait-for `AssertSeen` / `AssertCount` |
| 9     | TLS (inbound + outbound, with custom CAs and dangerous-mode) |
| 10    | Runner binary + `/health`, `/ready` endpoints |
| 11    | TypeScript client + vitest |
| 12    | Docker compose smoke test |
