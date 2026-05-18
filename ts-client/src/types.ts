// Wire-format types matching the Rust `partly_proxy_lib::wire` module.
// Keep this file mechanically aligned with `crates/partly-proxy-lib/src/wire.rs`.

/** Filter shared by AssertSeen, AssertCount and QueryTraffic. */
export interface TrafficFilter {
  upstream?: string;
  method?: string;
  path_pattern?: string;
  status?: number;
  labels?: Record<string, string>;
}

/** Stub registration parameters. Bodies are UTF-8 strings on the wire. */
export interface StubOptions {
  upstream?: string;
  // Matcher fields
  method?: string;
  path_pattern?: string;
  header_contains?: Record<string, string>;
  body_contains?: string;
  // Response fields
  status?: number;
  response_headers?: Record<string, string>;
  body?: string;
  delay_ms?: number;
  // Fire-count limit; omit for unlimited.
  times?: number;
}

/** One JSON-Lines command in either direction. */
export type WireCommand =
  | ({ type: "Stub" } & StubOptions)
  | { type: "ClearStubs"; upstream?: string }
  | { type: "Pause"; upstream?: string }
  | { type: "Resume"; upstream?: string }
  | ({ type: "AssertSeen"; timeout_ms: number } & TrafficFilter)
  | ({ type: "AssertCount"; expected: number; timeout_ms: number } & TrafficFilter)
  | ({ type: "QueryTraffic" } & TrafficFilter)
  | { type: "ClearRecordings" };

/** Recorded request as serialised by the Rust recorder. */
export interface RecordedRequest {
  method: string;
  uri: string;
  headers: Array<[string, string]>;
  /** Base64-encoded body bytes. */
  body: string;
  body_sha256: string;
}

/** Recorded response. */
export interface RecordedResponse {
  status: number;
  headers: Array<[string, string]>;
  body: string;
}

/** Outcome — either a response or a stringified error. */
export type ExchangeOutcome =
  | { kind: "response"; status: number; headers: Array<[string, string]>; body: string }
  | { kind: "error"; message: string };

/** One recorded exchange. */
export interface RecordedExchange {
  id: string;
  upstream?: string;
  timestamp: string;
  duration_ms: number;
  request: RecordedRequest;
  outcome: ExchangeOutcome;
  labels?: Record<string, string>;
}

/** Wire response carried back to the client. */
export type WireResponse =
  | { type: "Ok" }
  | { type: "Error"; message: string }
  | { type: "Exchanges"; exchanges: RecordedExchange[] }
  | { type: "AssertionResult"; passed: boolean; message: string };

/** Result of an assertion command. */
export interface AssertionResult {
  passed: boolean;
  message: string;
}

/** Error thrown when the proxy returns a wire-level Error response. */
export class ProxyControlError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProxyControlError";
  }
}
