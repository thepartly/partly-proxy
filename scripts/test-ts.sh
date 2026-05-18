#!/usr/bin/env bash
# Run the TypeScript client's test suite. Reproducible locally and in CI.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/../ts-client"

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
