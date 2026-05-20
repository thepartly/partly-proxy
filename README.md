# partly-proxy

[![CI](https://github.com/thepartly/partly-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/thepartly/partly-proxy/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/partly-proxy-lib.svg)](https://crates.io/crates/partly-proxy-lib)
[![docs.rs](https://img.shields.io/docsrs/partly-proxy-lib)](https://docs.rs/partly-proxy-lib)
[![npm](https://img.shields.io/npm/v/@partly/proxy-client.svg)](https://www.npmjs.com/package/@partly/proxy-client)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue)](https://github.com/thepartly/partly-proxy/blob/main/Cargo.toml)
![License](https://img.shields.io/crates/l/partly-proxy-lib)

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
| Replay (`MethodUriAndBodyHash` + `Custom`)    | O(1) indexed lookup; goes through redaction hooks  |
| Middleware chain with `Next<'_>`              | Body rewrites, short-circuit, error recovery       |
| Stubs + in-process command plane              | Fire-count, artificial delay, pause/resume         |
| TCP JSON-Lines control plane                  | Same command set, cross-language harnesses         |
| Wait-for `AssertSeen` / `AssertCount`         | Overshoot terminates fast                          |
| Hosting example (`examples/host.rs`)          | Env-var-driven; ~30 lines                          |
| TypeScript client + vitest                    | Mock + real-binary e2e suites                      |
