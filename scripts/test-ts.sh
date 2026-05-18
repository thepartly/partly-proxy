#!/usr/bin/env bash
# Run the TypeScript client's test suite, including end-to-end coverage
# against the real Rust proxy. Reproducible locally and in CI.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
cd "$ROOT"

echo "==> cargo build (host example + echo)"
cargo build -p partly-proxy-lib --example host
cargo build -p partly-proxy-echo

# Absolute paths into the workspace target dir. The e2e vitest spec reads
# these to spawn the real binaries; absent → e2e tests fail loudly with a
# message pointing back at this script.
export PARTLY_PROXY_HOST_BIN="$ROOT/target/debug/examples/host"
export PARTLY_PROXY_ECHO_BIN="$ROOT/target/debug/partly-proxy-echo"

cd "$ROOT/ts-client"

if [ ! -d node_modules ]; then
  echo "==> npm ci"
  npm ci
fi

echo "==> npm run lint"
npm run lint

echo "==> npm run build"
npm run build

echo "==> npm test"
npm test
