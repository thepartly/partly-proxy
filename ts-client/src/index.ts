import * as net from "node:net";

import {
  AssertionResult,
  ProxyControlError,
  RecordedExchange,
  StubOptions,
  TrafficFilter,
  WireCommand,
  WireResponse,
} from "./types.js";

export * from "./types.js";

export interface ProxyClientOptions {
  /** Host the proxy's TCP control plane is bound on. */
  host: string;
  /** Port from `ClusterHandle::tcp_control_addr()`. */
  port: number;
  /** Default timeout (ms) for assertion commands. Defaults to 5000. */
  defaultAssertionTimeoutMs?: number;
  /** Per-command socket timeout (ms). Defaults to 30000. */
  socketTimeoutMs?: number;
}

/**
 * Strongly-typed wrapper around the JSON-Lines control plane.
 *
 * Each method writes one JSON line and awaits one response line. Commands
 * are serialised per connection — concurrent calls from the same client
 * are processed one at a time, in FIFO order.
 */
export class ProxyClient {
  private readonly socket: net.Socket;
  private readonly defaultAssertionTimeoutMs: number;
  private buffer = "";
  /**
   * Queue of pending replies. Each entry has a resolver and a rejecter.
   * Responses are matched FIFO with sent commands.
   */
  private readonly pending: Array<{
    resolve: (line: string) => void;
    reject: (err: Error) => void;
  }> = [];
  private fatal?: Error;
  /** Chains sends so commands are serialised on the wire. */
  private inflight: Promise<void> = Promise.resolve();

  private constructor(socket: net.Socket, opts: ProxyClientOptions) {
    this.socket = socket;
    this.defaultAssertionTimeoutMs = opts.defaultAssertionTimeoutMs ?? 5_000;

    socket.setEncoding("utf8");
    socket.setTimeout(opts.socketTimeoutMs ?? 30_000);

    socket.on("data", (chunk: string) => this.onData(chunk));
    socket.on("error", (err) => this.fail(err));
    socket.on("close", () => this.fail(new Error("control-plane socket closed")));
    socket.on("timeout", () => this.fail(new Error("control-plane socket timed out")));
  }

  static async connect(opts: ProxyClientOptions): Promise<ProxyClient> {
    return await new Promise<ProxyClient>((resolve, reject) => {
      const socket = net.createConnection({ host: opts.host, port: opts.port }, () => {
        resolve(new ProxyClient(socket, opts));
      });
      socket.once("error", (err) => reject(err));
    });
  }

  /** Register a stub. */
  async stub(stub: StubOptions): Promise<void> {
    const resp = await this.send({ type: "Stub", ...stub });
    expectOk(resp);
  }

  /** Clear stubs for one upstream (or all when omitted). */
  async clearStubs(upstream?: string): Promise<void> {
    const cmd: WireCommand =
      upstream === undefined ? { type: "ClearStubs" } : { type: "ClearStubs", upstream };
    const resp = await this.send(cmd);
    expectOk(resp);
  }

  /** Pause an upstream (or all when omitted). */
  async pause(upstream?: string): Promise<void> {
    const cmd: WireCommand =
      upstream === undefined ? { type: "Pause" } : { type: "Pause", upstream };
    const resp = await this.send(cmd);
    expectOk(resp);
  }

  /** Resume an upstream (or all when omitted). */
  async resume(upstream?: string): Promise<void> {
    const cmd: WireCommand =
      upstream === undefined ? { type: "Resume" } : { type: "Resume", upstream };
    const resp = await this.send(cmd);
    expectOk(resp);
  }

  /** Block until any matching exchange appears or the timeout elapses. */
  async assertSeen(
    filter: TrafficFilter,
    timeoutMs?: number,
  ): Promise<AssertionResult> {
    const timeout = timeoutMs ?? this.defaultAssertionTimeoutMs;
    const cmd: WireCommand = {
      type: "AssertSeen",
      timeout_ms: timeout,
      ...filter,
    };
    const resp = await this.send(cmd, Math.max(timeout + 2_000, 5_000));
    return expectAssertion(resp);
  }

  /** Block until the match count equals `expected`, overshoots, or the timeout elapses. */
  async assertCount(
    filter: TrafficFilter,
    expected: number,
    timeoutMs?: number,
  ): Promise<AssertionResult> {
    const timeout = timeoutMs ?? this.defaultAssertionTimeoutMs;
    const cmd: WireCommand = {
      type: "AssertCount",
      expected,
      timeout_ms: timeout,
      ...filter,
    };
    const resp = await this.send(cmd, Math.max(timeout + 2_000, 5_000));
    return expectAssertion(resp);
  }

  /** Snapshot every recorded exchange that matches `filter` (or all when omitted). */
  async queryTraffic(filter?: TrafficFilter): Promise<RecordedExchange[]> {
    const cmd: WireCommand = { type: "QueryTraffic", ...(filter ?? {}) };
    const resp = await this.send(cmd);
    if (resp.type !== "Exchanges") {
      throw asProxyError(resp);
    }
    return resp.exchanges;
  }

  /** Drop every recorded exchange in memory. */
  async clearRecordings(): Promise<void> {
    const resp = await this.send({ type: "ClearRecordings" });
    expectOk(resp);
  }

  /** Close the underlying socket. */
  async close(): Promise<void> {
    return await new Promise<void>((resolve) => {
      this.socket.end(() => resolve());
    });
  }

  /**
   * Serialise sends on the wire and await a single response line.
   * `commandTimeoutMs` lets long-blocking assertions extend the read window
   * past the default socket timeout.
   */
  private async send(cmd: WireCommand, commandTimeoutMs?: number): Promise<WireResponse> {
    if (this.fatal) {
      throw this.fatal;
    }
    const myTurn = this.inflight.then(() => this.doSend(cmd, commandTimeoutMs));
    this.inflight = myTurn.then(
      () => undefined,
      () => undefined,
    );
    return await myTurn;
  }

  private async doSend(
    cmd: WireCommand,
    commandTimeoutMs?: number,
  ): Promise<WireResponse> {
    const line = JSON.stringify(cmd) + "\n";
    const responsePromise = new Promise<string>((resolve, reject) => {
      this.pending.push({ resolve, reject });
    });

    const writeResult = this.socket.write(line);
    if (!writeResult) {
      await new Promise<void>((resolve) => this.socket.once("drain", () => resolve()));
    }

    let timer: NodeJS.Timeout | undefined;
    const timed = commandTimeoutMs
      ? new Promise<never>((_, reject) => {
          timer = setTimeout(
            () => reject(new Error(`command timed out after ${commandTimeoutMs}ms`)),
            commandTimeoutMs,
          );
        })
      : undefined;

    try {
      const raw = timed
        ? await Promise.race([responsePromise, timed])
        : await responsePromise;
      return JSON.parse(raw) as WireResponse;
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  private onData(chunk: string): void {
    this.buffer += chunk;
    let idx: number;
    while ((idx = this.buffer.indexOf("\n")) >= 0) {
      const line = this.buffer.slice(0, idx);
      this.buffer = this.buffer.slice(idx + 1);
      const waiter = this.pending.shift();
      if (waiter) {
        waiter.resolve(line);
      }
    }
  }

  private fail(err: Error): void {
    if (!this.fatal) {
      this.fatal = err;
    }
    while (this.pending.length > 0) {
      const w = this.pending.shift();
      if (w) w.reject(err);
    }
  }
}

function expectOk(resp: WireResponse): void {
  if (resp.type === "Ok") return;
  throw asProxyError(resp);
}

function expectAssertion(resp: WireResponse): AssertionResult {
  if (resp.type === "AssertionResult") {
    return { passed: resp.passed, message: resp.message };
  }
  throw asProxyError(resp);
}

function asProxyError(resp: WireResponse): Error {
  if (resp.type === "Error") {
    return new ProxyControlError(resp.message);
  }
  return new ProxyControlError(`unexpected response: ${JSON.stringify(resp)}`);
}
