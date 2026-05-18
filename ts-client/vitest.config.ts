import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // The e2e tests block on real wait-for assertions (`assertSeen`,
    // `assertCount`) with timeouts up to a few seconds. Bump the per-test
    // ceiling so a slow CI runner doesn't trip the default 5s.
    testTimeout: 15_000,
    hookTimeout: 15_000,
  },
});
