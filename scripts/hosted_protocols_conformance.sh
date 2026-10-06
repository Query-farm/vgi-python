#!/usr/bin/env bash
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Run vgi-rpc's hosted-protocols conformance group (vgi-rpc-test-hosted)
# against this repo's fixture workers on all three transports it covers:
#
#   stdio  vgi-fixture-worker                       (MetaWorker.serve, pipe)
#   unix   vgi-fixture-worker --unix <sock>         (the DuckDB launcher path)
#   http   vgi-fixture-http --identity              (+ the Identity.v1 groups)
#
# Every worker must host, in order, vgi.v2 then conformance.Secondary.v1.
#
# Usage: scripts/hosted_protocols_conformance.sh [stdio|unix|http ...]
# Default: all three. Runs from the project env (`uv run --no-sync`), which
# must have vgi-rpc with the [http,conformance] extras (the dev group does).
set -euo pipefail

cd "$(dirname "$0")/.."

EXPECT="vgi.v2,conformance.Secondary.v1"
RUN=(uv run --no-sync)
BIN="$(dirname "$("${RUN[@]}" python -c 'import sys; print(sys.executable)')")"
TRANSPORTS=("$@")
[ ${#TRANSPORTS[@]} -eq 0 ] && TRANSPORTS=(stdio unix http)

# Short scratch dir: AF_UNIX paths are capped at ~104 bytes, and CI temp
# dirs can be long.
SCRATCH="$(mktemp -d /tmp/vgihp.XXXXXX)"
PIDS=()
cleanup() {
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  rm -rf "$SCRATCH"
}
trap cleanup EXIT

wait_for() {  # wait_for <description> <test command...>
  local what="$1"; shift
  for _ in $(seq 150); do "$@" && return 0; sleep 0.2; done
  echo "timed out waiting for $what" >&2
  return 1
}

run_stdio() {
  echo "== hosted-protocols: stdio =="
  "${RUN[@]}" vgi-rpc-test-hosted --cmd "$BIN/vgi-fixture-worker" --expect "$EXPECT" -- -rs
}

run_unix() {
  echo "== hosted-protocols: unix =="
  local sock="$SCRATCH/w.sock"
  "$BIN/vgi-fixture-worker" --unix "$sock" --idle-timeout 300 >"$SCRATCH/unix.out" 2>"$SCRATCH/unix.err" &
  PIDS+=($!)
  wait_for "unix socket $sock" test -S "$sock" || { cat "$SCRATCH/unix.err" >&2; return 1; }
  "${RUN[@]}" vgi-rpc-test-hosted --unix "$sock" --expect "$EXPECT" -- -rs
}

run_http() {
  echo "== hosted-protocols: http (--identity) =="
  local port
  port="$("${RUN[@]}" python -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
  "$BIN/vgi-fixture-http" --identity --host 127.0.0.1 --port "$port" >"$SCRATCH/http.out" 2>"$SCRATCH/http.err" &
  PIDS+=($!)
  wait_for "HTTP worker on :$port" curl -sf -o /dev/null "http://127.0.0.1:$port/health" \
    || { cat "$SCRATCH/http.err" >&2; return 1; }
  "${RUN[@]}" vgi-rpc-test-hosted --url "http://127.0.0.1:$port" --expect "$EXPECT" --identity -- -rs
}

for t in "${TRANSPORTS[@]}"; do
  case "$t" in
    stdio) run_stdio ;;
    unix) run_unix ;;
    http) run_http ;;
    *) echo "unknown transport: $t (expected stdio, unix or http)" >&2; exit 2 ;;
  esac
done
