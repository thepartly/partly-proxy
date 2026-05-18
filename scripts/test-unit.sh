#!/usr/bin/env bash
# Run the cargo unit + library test suite. Reproducible locally and in CI.
#
# Exercises every supported feature combo so a regression that only shows
# up under --no-default-features or --all-features still trips CI.
#
# Usage: scripts/test-unit.sh [extra cargo test args]
set -euo pipefail

# Run from the workspace root regardless of CWD.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "==> cargo fmt --all -- --check"
cargo fmt --all -- --check

echo "==> cargo clippy --workspace --all-targets -- -D warnings"
cargo clippy --workspace --all-targets -- -D warnings

echo "==> cargo check --workspace --no-default-features"
# Confirms the lib + every crate compiles with zero storage backends.
# Anything that needs storage-jsonl must be feature-gated.
cargo check --workspace --no-default-features

echo "==> cargo check --workspace --all-features"
# Confirms every backend coexists — catches accidental dep collisions
# between sqlx, the JSONL backend, and any other backend that lands.
cargo check --workspace --all-features

echo "==> cargo test -p partly-proxy-lib --no-default-features --lib"
# Pinpoints the lib in the zero-backend configuration. The integration
# tests live under tests/ and depend on default features (echo upstream,
# reqwest with rustls, etc.), so we narrow this run to --lib.
cargo test -p partly-proxy-lib --no-default-features --lib

echo "==> cargo build --workspace --all-targets"
cargo build --workspace --all-targets

echo "==> cargo test --workspace --all-targets $*"
cargo test --workspace --all-targets "$@"
