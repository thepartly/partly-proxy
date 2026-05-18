#!/usr/bin/env bash
# Run the cargo unit + library test suite. Reproducible locally and in CI.
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

echo "==> cargo build --workspace --all-targets"
cargo build --workspace --all-targets

echo "==> cargo test --workspace --all-targets $*"
cargo test --workspace --all-targets "$@"
