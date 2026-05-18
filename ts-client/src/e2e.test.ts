/**
 * End-to-end tests against the real Rust proxy.
 *
 * Spawns:
 *   - `partly-proxy-echo`  (in-cluster upstream)
 *   - the `host` example   (proxy listener + TCP control plane)
 *
 * Binary paths are passed via env vars by `scripts/test-ts.sh`. Running
 * `npm test` standalone without those env vars makes the suite skip the
 * spawn and fail in `beforeAll` with an explicit "use scripts/test-ts.sh"
 * message — keeping the cargo dependency out of the everyday TS dev loop.
 *
 * Each test reclaims a clean slate via `clearStubs` + `clearRecordings`
 * so the spec sees them as independent.
 */

import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { ChildProcess, spawn } from "node:child_process";
import * as net from "node:net";
import { setTimeout as delay } from "node:timers/promises";

import { ProxyClient, ProxyControlError } from "./index.js";

const HOST_BIN = process.env.PARTLY_PROXY_HOST_BIN;
const ECHO_BIN = process.env.PARTLY_PROXY_ECHO_BIN;

const skipReason =
  !HOST_BIN || !ECHO_BIN
    ? "PARTLY_PROXY_HOST_BIN / PARTLY_PROXY_ECHO_BIN not set — run via scripts/test-ts.sh"
    : null;

// Picks an OS-assigned free port. There's a small race between close()
// and the binary's bind, but on localhost it's effectively never observed.
async function pickPort(): Promise<number> {
  return await new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.unref();
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address() as net.AddressInfo;
      const port = addr.port;
      srv.close((err) => (err ? reject(err) : resolve(port)));
    });
    srv.on("error", reject);
  });
}

async function waitForTcp(port: number, deadlineMs = 5_000): Promise<void> {
  const deadline = Date.now() + deadlineMs;
  let lastErr: Error | undefined;
  while (Date.now() < deadline) {
    try {
      await new Promise<void>((resolve, reject) => {
        const sock = net.createConnection({ host: "127.0.0.1", port }, () => {
          sock.end();
          resolve();
        });
        sock.once("error", reject);
      });
      return;
    } catch (e) {
      lastErr = e as Error;
      await delay(50);
    }
  }
  throw new Error(`port ${port} never accepted: ${lastErr?.message ?? "unknown"}`);
}

function spawnBin(bin: string, env: Record<string, string>, label: string): ChildProcess {
  const proc = spawn(bin, [], {
    env: { ...process.env, RUST_LOG: "warn", ...env },
    stdio: ["ignore", "pipe", "pipe"],
  });
  // Surface stderr to the vitest console so a binary crash isn't silent.
  proc.stderr?.on("data", (chunk: Buffer) => {
    process.stderr.write(`[${label}] ${chunk}`);
  });
  proc.on("exit", (code, signal) => {
    if (code !== 0 && signal !== "SIGTERM") {
      process.stderr.write(`[${label}] exited code=${code} signal=${signal}\n`);
    }
  });
  return proc;
}

async function killAndWait(proc: ChildProcess): Promise<void> {
  if (proc.exitCode !== null || proc.signalCode !== null) return;
  proc.kill("SIGTERM");
  await new Promise<void>((resolve) => {
    const t = setTimeout(() => {
      try {
        proc.kill("SIGKILL");
      } catch {
        // already dead
      }
      resolve();
    }, 2_000);
    proc.once("exit", () => {
      clearTimeout(t);
      resolve();
    });
  });
}

describe.skipIf(skipReason !== null)("ProxyClient e2e", () => {
  let client: ProxyClient;
  let proxyAddr: string;
  let echoProc: ChildProcess;
  let hostProc: ChildProcess;

  beforeAll(async () => {
    if (skipReason) throw new Error(skipReason);
    const echoPort = await pickPort();
    const proxyPort = await pickPort();
    const controlPort = await pickPort();

    echoProc = spawnBin(ECHO_BIN!, { ECHO_BIND: `127.0.0.1:${echoPort}` }, "echo");
    hostProc = spawnBin(
      HOST_BIN!,
      {
        PARTLY_PROXY_BIND: `127.0.0.1:${proxyPort}`,
        PARTLY_PROXY_UPSTREAM: `http://127.0.0.1:${echoPort}`,
        PARTLY_PROXY_TCP_CONTROL_BIND: `127.0.0.1:${controlPort}`,
      },
      "host",
    );

    await Promise.all([
      waitForTcp(echoPort),
      waitForTcp(proxyPort),
      waitForTcp(controlPort),
    ]);
    client = await ProxyClient.connect({ host: "127.0.0.1", port: controlPort });
    proxyAddr = `127.0.0.1:${proxyPort}`;
  });

  afterAll(async () => {
    if (client) await client.close().catch(() => undefined);
    if (echoProc) await killAndWait(echoProc);
    if (hostProc) await killAndWait(hostProc);
  });

  beforeEach(async () => {
    await client.clearStubs();
    await client.clearRecordings();
  });

  it("Stub fires through the proxy on the registered matcher", async () => {
    await client.stub({
      upstream: "upstream",
      method: "GET",
      path_pattern: "^/teapot$",
      status: 418,
      body: "im-a-teapot",
    });
    const resp = await fetch(`http://${proxyAddr}/teapot`);
    expect(resp.status).toBe(418);
    expect(await resp.text()).toBe("im-a-teapot");
  });

  it("ClearStubs lets upstream win again", async () => {
    await client.stub({
      upstream: "upstream",
      path_pattern: "^/x$",
      status: 418,
      body: "stubbed",
    });
    await client.clearStubs();
    const resp = await fetch(`http://${proxyAddr}/x`);
    expect(resp.status).toBe(200); // echo returns 200 with a JSON body
    const body = await resp.json();
    expect(body.path).toBe("/x");
  });

  it("Pause blocks new requests until Resume", async () => {
    await client.pause("upstream");
    const inflight = fetch(`http://${proxyAddr}/blocked`);
    const settled = await Promise.race([
      inflight.then(() => "settled" as const),
      delay(100).then(() => "pending" as const),
    ]);
    expect(settled).toBe("pending");
    await client.resume("upstream");
    const resp = await inflight;
    expect(resp.status).toBe(200);
  });

  it("AssertSeen blocks until traffic arrives, then passes", async () => {
    const assertion = client.assertSeen({ path_pattern: "^/marker$" }, 3_000);
    // Drive the matching traffic after a short gap.
    await delay(60);
    await fetch(`http://${proxyAddr}/marker`);
    const result = await assertion;
    expect(result.passed).toBe(true);
  });

  it("AssertSeen times out when nothing matches", async () => {
    const result = await client.assertSeen({ path_pattern: "^/never$" }, 200);
    expect(result.passed).toBe(false);
    expect(result.message).toMatch(/timeout/);
  });

  it("AssertCount passes on the exact match", async () => {
    const assertion = client.assertCount({ path_pattern: "^/c$" }, 2, 3_000);
    await delay(30);
    await fetch(`http://${proxyAddr}/c`);
    await fetch(`http://${proxyAddr}/c`);
    const result = await assertion;
    expect(result.passed).toBe(true);
  });

  it("AssertCount fails fast on overshoot", async () => {
    // Pre-populate three matching exchanges before the assertion runs.
    await fetch(`http://${proxyAddr}/over`);
    await fetch(`http://${proxyAddr}/over`);
    await fetch(`http://${proxyAddr}/over`);
    // Wait briefly for the recorder to catch up.
    await delay(100);
    const t0 = Date.now();
    const result = await client.assertCount({ path_pattern: "^/over$" }, 2, 5_000);
    const elapsed = Date.now() - t0;
    expect(result.passed).toBe(false);
    expect(result.message).toMatch(/overshoot/);
    expect(elapsed).toBeLessThan(1_000); // fail-fast, not a full timeout
  });

  it("QueryTraffic returns a RecordedExchange matching the wire shape", async () => {
    await fetch(`http://${proxyAddr}/queried?x=1`);
    // Wait for the recorder.
    await delay(100);
    const exchanges = await client.queryTraffic({ path_pattern: "^/queried$" });
    expect(exchanges).toHaveLength(1);
    const ex = exchanges[0]!;
    expect(ex.upstream).toBe("upstream");
    expect(ex.request.method).toBe("GET");
    expect(ex.request.uri).toBe("/queried?x=1");
    expect(typeof ex.request.body_sha256).toBe("string");
    expect(ex.request.body_sha256).toHaveLength(64);
    expect(ex.outcome.kind).toBe("response");
    if (ex.outcome.kind === "response") {
      expect(ex.outcome.status).toBe(200);
    }
  });

  it("ClearRecordings empties the buffer", async () => {
    await fetch(`http://${proxyAddr}/cleared`);
    await delay(100);
    let exchanges = await client.queryTraffic();
    expect(exchanges.length).toBeGreaterThanOrEqual(1);
    await client.clearRecordings();
    exchanges = await client.queryTraffic();
    expect(exchanges).toHaveLength(0);
  });

  it("Stub against unknown upstream surfaces ProxyControlError", async () => {
    await expect(
      client.stub({
        upstream: "does-not-exist",
        path_pattern: "^/x$",
        status: 200,
        body: "x",
      }),
    ).rejects.toBeInstanceOf(ProxyControlError);
  });
});
