#!/usr/bin/env bash
# Docker-compose smoke test for partly-proxy-runner.
#
# The unit + integration suite (cargo test) is the primary test path; this
# script is the documented fallback for verifying the binary builds and
# runs end-to-end inside containers. It builds the image, brings up a
# proxy + in-cluster echo upstream, drives a few requests through the
# proxy, asserts on the health endpoints, and tears the stack down.
#
# The TCP JSON-Lines control plane is exercised exhaustively by
# `crates/partly-proxy-lib/tests/control_plane_tcp.rs`; this script does
# NOT re-test it from bash to avoid duplicating coverage with shell
# tooling that is harder to reason about than the Rust integration tests.
#
# Usage: scripts/test-docker.sh
#
# Requires: docker (Docker Desktop / OrbStack / compatible) with the
# `docker compose` v2 plugin.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
cd "$ROOT"

PROJECT="partly-proxy-smoke-$$"
COMPOSE=(docker compose --project-name "$PROJECT" -f docker/compose.yaml)

cleanup() {
  # Preserve the original exit code so a failing assertion still propagates
  # past the EXIT trap.
  local ec=$?
  echo "==> tearing down stack"
  "${COMPOSE[@]}" down --remove-orphans --volumes >/dev/null 2>&1 || true
  exit "$ec"
}
trap cleanup EXIT

echo "==> building images"
"${COMPOSE[@]}" build

echo "==> bringing stack up"
"${COMPOSE[@]}" up -d

echo "==> waiting for proxy /ready"
DEADLINE=$((SECONDS + 60))
HEALTH=""
while (( SECONDS < DEADLINE )); do
  HEALTH=$("${COMPOSE[@]}" ps proxy --format json 2>/dev/null \
    | python3 -c 'import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        d = json.loads(line)
    except json.JSONDecodeError:
        continue
    print(d.get("Health", ""))
    break' \
    || echo "")
  if [ "$HEALTH" = "healthy" ]; then
    break
  fi
  sleep 1
done

if [ "${HEALTH:-}" != "healthy" ]; then
  echo "proxy never reached healthy state (last: '${HEALTH:-}'); logs follow:"
  "${COMPOSE[@]}" logs --tail=50
  exit 1
fi
echo "    proxy reports healthy"

# Resolve published ports. `docker compose port` returns host:port pairs.
PROXY_PROXY=$("${COMPOSE[@]}" port proxy 8080 | sed 's|.*:||')
PROXY_HEALTH=$("${COMPOSE[@]}" port proxy 9090 | sed 's|.*:||')
PROXY_CTRL=$("${COMPOSE[@]}" port proxy 4500 | sed 's|.*:||')
echo "    proxy ports: proxy=$PROXY_PROXY health=$PROXY_HEALTH control=$PROXY_CTRL"

echo "==> GET /health returns 200 ok"
BODY=$(curl -fsS "http://127.0.0.1:$PROXY_HEALTH/health")
[ "$BODY" = "ok" ] || { echo "expected 'ok' got '$BODY'"; exit 1; }

echo "==> GET /ready returns 200 with ready:true"
BODY=$(curl -fsS "http://127.0.0.1:$PROXY_HEALTH/ready")
echo "$BODY" | grep -qE '"ready"[[:space:]]*:[[:space:]]*true' \
  || { echo "expected ready:true, got: $BODY"; exit 1; }

echo "==> forwards plain GET to in-cluster echo"
BODY=$(curl -fsS "http://127.0.0.1:$PROXY_PROXY/forwarded?x=1")
echo "$BODY" | grep -q '"path":"/forwarded"' \
  || { echo "echo did not see /forwarded: $BODY"; exit 1; }

echo "==> verify TCP control port is open (full coverage in cargo tests)"
# A simple liveness probe — proves the listener is bound. The actual
# command-and-response semantics live in the Rust integration suite.
exec 3<>"/dev/tcp/127.0.0.1/$PROXY_CTRL" || {
  echo "could not open TCP connection to control port"
  exit 1
}
exec 3>&-

echo "==> docker smoke test passed"
