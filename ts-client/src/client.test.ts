import { afterEach, beforeEach, describe, expect, it } from "vitest";
import * as net from "node:net";
import { AddressInfo } from "node:net";
import { ProxyClient, ProxyControlError, WireCommand, WireResponse } from "./index.js";

/**
 * Minimal in-process TCP server that replays a scripted response per
 * incoming line. Lets us exercise the client's wire format without
 * spawning the Rust proxy binary.
 */
class MockProxy {
  private server: net.Server;
  /** Each entry handles one received command line. */
  private handlers: Array<(cmd: WireCommand) => WireResponse> = [];
  private receivedLines: string[] = [];
  port = 0;

  constructor() {
    this.server = net.createServer((socket) => {
      socket.setEncoding("utf8");
      let buffer = "";
      socket.on("data", (chunk: string) => {
        buffer += chunk;
        let idx: number;
        while ((idx = buffer.indexOf("\n")) >= 0) {
          const line = buffer.slice(0, idx);
          buffer = buffer.slice(idx + 1);
          this.receivedLines.push(line);
          const cmd = JSON.parse(line) as WireCommand;
          const handler = this.handlers.shift();
          const response: WireResponse = handler
            ? handler(cmd)
            : { type: "Error", message: "no handler registered" };
          socket.write(JSON.stringify(response) + "\n");
        }
      });
    });
  }

  async start(): Promise<void> {
    return await new Promise<void>((resolve) => {
      this.server.listen(0, "127.0.0.1", () => {
        const addr = this.server.address() as AddressInfo;
        this.port = addr.port;
        resolve();
      });
    });
  }

  async stop(): Promise<void> {
    return await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }

  /** Register the next-N handlers in FIFO order. */
  expect(handler: (cmd: WireCommand) => WireResponse): void {
    this.handlers.push(handler);
  }

  received(): string[] {
    return this.receivedLines.slice();
  }
}

describe("ProxyClient", () => {
  let mock: MockProxy;
  let client: ProxyClient;

  beforeEach(async () => {
    mock = new MockProxy();
    await mock.start();
    client = await ProxyClient.connect({ host: "127.0.0.1", port: mock.port });
  });

  afterEach(async () => {
    await client.close();
    await mock.stop();
  });

  it("serialises Stub with all matcher and response fields", async () => {
    mock.expect((cmd) => {
      expect(cmd.type).toBe("Stub");
      if (cmd.type !== "Stub") throw new Error("type narrow");
      expect(cmd.upstream).toBe("api");
      expect(cmd.method).toBe("GET");
      expect(cmd.path_pattern).toBe("^/health$");
      expect(cmd.status).toBe(200);
      expect(cmd.body).toBe("ok");
      expect(cmd.times).toBe(2);
      return { type: "Ok" };
    });

    await client.stub({
      upstream: "api",
      method: "GET",
      path_pattern: "^/health$",
      status: 200,
      body: "ok",
      times: 2,
    });
  });

  it("returns Exchanges payload from queryTraffic", async () => {
    mock.expect((cmd) => {
      expect(cmd.type).toBe("QueryTraffic");
      return {
        type: "Exchanges",
        exchanges: [
          {
            id: "00000000-0000-0000-0000-000000000001",
            upstream: "api",
            timestamp: "2024-01-01T00:00:00Z",
            duration_ms: 12,
            request: {
              method: "GET",
              uri: "/x",
              headers: [],
              body: "",
              body_sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            },
            outcome: { kind: "response", status: 200, headers: [], body: "" },
          },
        ],
      };
    });

    const exchanges = await client.queryTraffic({ path_pattern: "^/x$" });
    expect(exchanges).toHaveLength(1);
    expect(exchanges[0]?.upstream).toBe("api");
  });

  it("maps Error responses to ProxyControlError", async () => {
    mock.expect(() => ({ type: "Error", message: "unknown upstream: missing" }));
    mock.expect(() => ({ type: "Error", message: "unknown upstream: missing" }));
    await expect(client.pause("missing")).rejects.toBeInstanceOf(ProxyControlError);
    await expect(client.pause("missing")).rejects.toThrow(/unknown upstream/);
  });

  it("serialises assertion results into the typed AssertionResult", async () => {
    mock.expect(() => ({ type: "AssertionResult", passed: true, message: "matched 1" }));
    const r = await client.assertSeen({ path_pattern: "^/x$" }, 100);
    expect(r.passed).toBe(true);
    expect(r.message).toBe("matched 1");
  });

  it("preserves command order under concurrent calls", async () => {
    // Three concurrent commands; the mock returns them in order.
    mock.expect(() => ({ type: "Ok" }));
    mock.expect(() => ({ type: "Ok" }));
    mock.expect(() => ({ type: "Ok" }));

    await Promise.all([
      client.pause("a"),
      client.pause("b"),
      client.pause("c"),
    ]);

    const lines = mock.received();
    expect(lines).toHaveLength(3);
    const parsed = lines.map((l) => JSON.parse(l) as WireCommand);
    const upstreams = parsed.map((p) => (p as { upstream?: string }).upstream);
    expect(upstreams).toEqual(["a", "b", "c"]);
  });

  it("clearStubs omits the upstream key when called without args", async () => {
    mock.expect((cmd) => {
      expect(cmd.type).toBe("ClearStubs");
      // The serialised JSON must not carry "upstream":null — the Rust side
      // treats absent as "clear all".
      const upstream = (cmd as { upstream?: string }).upstream;
      expect(upstream).toBeUndefined();
      return { type: "Ok" };
    });
    await client.clearStubs();
  });
});
