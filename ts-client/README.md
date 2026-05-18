# @partly/proxy-client

TypeScript client for the [partly-proxy-lib](../crates/partly-proxy-lib) JSON-Lines TCP control plane.

The proxy ships with a JSON-over-TCP control plane (`SPECIFICATION.md` §12.2)
that exposes the same command set as the in-process Rust API. This package is
the first-party Node.js client for that protocol; the design target is
Playwright suites that drive the proxy from the same process that runs the
browser harness.

## Install

```bash
npm install @partly/proxy-client
```

(Once published — for local development inside this workspace, depend on the
`ts-client/` directory directly.)

## Usage

```ts
import { ProxyClient } from "@partly/proxy-client";

// Connect to the proxy's TCP control port (returned by
// ClusterHandle::tcp_control_addr() on the Rust side).
const client = await ProxyClient.connect({ host: "127.0.0.1", port: 4500 });

// Register a stub.
await client.stub({
  upstream: "api",
  method: "GET",
  path_pattern: "^/health$",
  status: 200,
  body: '{"ok":true}',
  times: 5,
});

// Drive the system under test, then assert it called the proxy.
const result = await client.assertSeen(
  { upstream: "api", path_pattern: "^/orders$" },
  5_000, // wait up to 5s
);
if (!result.passed) throw new Error(result.message);

// Snapshot the recorded traffic.
const exchanges = await client.queryTraffic({ upstream: "api" });
console.log("captured", exchanges.length, "exchanges");

await client.close();
```

## Tests

```bash
npm install
npm test
```

The Vitest suite drives the client against an in-process mock TCP server, so
it doesn't depend on a running Rust proxy. End-to-end coverage of the
JSON-Lines protocol against the real proxy is provided by the Rust
integration suite in `crates/partly-proxy-lib/tests/control_plane_tcp.rs`.
