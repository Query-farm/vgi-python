#!/usr/bin/env bash
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Run the canonical Query-farm/vgi integration sqllogictest suite against the
# Python example worker, using a prebuilt standalone `haybarn-unittest` and the
# signed community vgi extension — no C++ build from source. See ci/README.md.
#
# Ported from vgi-go's harness. The Python differences:
#   * The "worker binaries" are console scripts installed into the project venv
#     by `uv sync --all-extras` (the vgi-fixtures workspace member); the C++
#     client spawns them as subprocesses. We use their absolute .venv/bin paths
#     so each spawn skips `uv run`'s resolve overhead.
#   * Every per-catalog worker is the same Python program with a different
#     entrypoint; the base `WorkerClass.main()` understands stdin/stdout,
#     `--http`, and the launcher's `--unix`, so one binary covers every lane.
#   * No worker coverage (unlike vgi-go's GOCOVERDIR) — pass/fail only.
#
# This mirrors the env wiring of the vgi repo's Makefile `test_subprocess` /
# `test_shm` / `test_launcher` / `test_http` targets (the canonical local way to
# run this suite against the Python worker — see vgi-python/CLAUDE.md).
#
# Required environment:
#   VGI_SRC           path to a Query-farm/vgi checkout (contains test/sql/integration)
#   HAYBARN_UNITTEST  path to the haybarn-unittest binary
# Optional:
#   BIN_DIR           dir holding the fixture console scripts (default: $REPO/.venv/bin)
#   TRANSPORT         stdio | shm | launch | http   (default: stdio)
#   VGI_RPC_SHM_SIZE_BYTES  shm side-channel segment size (the shm lane)
#   STAGE             scratch dir for the preprocessed test tree (default: mktemp)
set -euo pipefail

: "${VGI_SRC:?path to a Query-farm/vgi checkout}"
: "${HAYBARN_UNITTEST:?path to the haybarn-unittest binary}"

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
BIN_DIR="${BIN_DIR:-$REPO/.venv/bin}"
STAGE="${STAGE:-$(mktemp -d)}"
TRANSPORT="${TRANSPORT:-stdio}"
INTEGRATION="$VGI_SRC/test/sql/integration"
[ -d "$INTEGRATION" ] || { echo "::error::no test/sql/integration under VGI_SRC=$VGI_SRC"; exit 1; }

# The fixture console scripts (installed by `uv sync --all-extras`).
WORKER="$BIN_DIR/vgi-fixture-worker"
VERSIONED="$BIN_DIR/vgi-fixture-versioned-worker"
VERSIONED_TABLES="$BIN_DIR/vgi-fixture-versioned-tables-worker"
ATTACH_OPTIONS="$BIN_DIR/vgi-fixture-attach-options-worker"
BAD_PROTOCOL="$BIN_DIR/vgi-fixture-bad-protocol-worker"
SIMPLE_WRITABLE="$BIN_DIR/vgi-fixture-simple-writable-worker"
for b in "$WORKER" "$VERSIONED" "$VERSIONED_TABLES" "$ATTACH_OPTIONS" "$BAD_PROTOCOL" "$SIMPLE_WRITABLE"; do
  [ -x "$b" ] || { echo "::error::missing fixture worker $b (run: uv sync --all-extras)"; exit 1; }
done

# ---------------------------------------------------------------------------
# Stage a preprocessed copy of the suite. preprocess-require.awk rewrites each
# `require <ext>` gate into a signed INSTALL+LOAD so the standalone runner
# (which links none of these extensions) can run them. On the http lane it also
# injects `LOAD httpfs` before each worker ATTACH.
# ---------------------------------------------------------------------------
AWK_HTTP=0
EXTRA_SKIP=()
if [ "$TRANSPORT" = "http" ]; then
  AWK_HTTP=1
  # Dropped on the http lane only:
  #   * projection_pushdown_repro.test — chunk=2 fixtures emit one POST per two
  #     rows; transport-agnostic projection-id→wire mapping, fully covered by
  #     stdio (matches the vgi Makefile's test_http).
  #   * dynamic_filter.test — Top-N + dynamic-filter continuation terminates
  #     early over http in the prebuilt binary (a property of that C++ build).
  #   * partitioned_sequence.test — partition-local state is not preserved
  #     across stateless HTTP exchanges (known Python HTTP limitation; see
  #     vgi-python/CLAUDE.md "Known HTTP failures").
  #   * buffer_input/sizes.test — input buffering semantics differ over HTTP
  #     (same known limitation). (scale.test_slow is a .test_slow file and is
  #     never staged below, which only finds *.test.)
  EXTRA_SKIP=(
    -not -name 'projection_pushdown_repro.test'
    -not -name 'dynamic_filter.test'
    -not -name 'partitioned_sequence.test'
    -not -path './table_in_out/buffer_input/sizes.test'
  )
fi

echo "Staging preprocessed tests into $STAGE (transport=$TRANSPORT) ..."
mkdir -p "$STAGE/test/sql/integration"
( cd "$INTEGRATION"
  # Out of scope on every lane:
  #   writable/ — the opt-in *generic* writable catalog (VGI_WORKER_ENABLE_WRITABLE),
  #     not modelled by a cross-language fixture; the vgi Makefile excludes it too.
  #     (simple_writable/ IS staged — see VGI_SIMPLE_WRITABLE_WORKER below.)
  #   nested_type_combinations.test — segfaults the prebuilt standalone runner
  #     (a property of that C++ build, not the worker, which passes it against a
  #     locally-built unittest).
  #   bool_in_union.test — a pre-existing, arch-dependent union-bool bug whose
  #     pinned expected output matches arm64 but not amd64 (CI is amd64).
  find . -name '*.test' \
       -not -path './writable/*' \
       -not -name 'nested_type_combinations.test' \
       -not -name 'bool_in_union.test' \
       "${EXTRA_SKIP[@]}" | while read -r f; do
    mkdir -p "$STAGE/test/sql/integration/$(dirname "$f")"
    awk -v http="$AWK_HTTP" -f "$HERE/preprocess-require.awk" "$f" > "$STAGE/test/sql/integration/$f"
  done )

# Empty VGI_RPC_SHM_SIZE_BYTES must not reach the C++ client (it would try to
# attach a zero-size segment); only a real value enables the shm side channel.
[ -n "${VGI_RPC_SHM_SIZE_BYTES:-}" ] || unset VGI_RPC_SHM_SIZE_BYTES

# Force the C++ extension's init_global RPC to run synchronously so multi-conn
# parallel-init tests observe the worker's real max_workers (mirrors the Makefile).
export VGI_SYNC_INIT_GLOBAL=1

# The schema_reconcile fixture (hosted inside vgi-fixture-worker) is gated on
# this env var; point it at a scratch SQLite DB so those tests run.
SCHEMA_RECONCILE_DIR="$(mktemp -d)"
export VGI_SCHEMA_RECONCILE_DB="$SCHEMA_RECONCILE_DIR/vgi_schema_reconcile.sqlite"

# Presence gate for the crash / pool-recovery tests
# (table_in_out/table_buffering_*). The value is not used as a worker spec —
# the SQL attaches the normal pooled worker; this just un-skips the tests.
export VGI_TEST_DEDICATED_WORKER=1

# Background workers (http servers) are tracked in a file and SIGTERMed on exit.
BG_PIDS_FILE="$(mktemp)"
# shellcheck disable=SC2329  # invoked indirectly via `trap cleanup EXIT` below
cleanup() {
  [ -f "$BG_PIDS_FILE" ] || return 0
  while read -r p; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done < "$BG_PIDS_FILE"
}
trap cleanup EXIT

# boot_http_worker <binary> — start it as an HTTP server on an ephemeral port and
# set BOOTED_PORT to the port it reports (PORT:<n>, the worker's readiness
# contract). Sets a global rather than echoing so the backgrounded worker stays a
# child of this shell (a $(...) subshell would reparent it and break teardown).
BOOTED_PORT=""
boot_http_worker() {
  local exe="$1" log pid port=""
  BOOTED_PORT=""
  log="$(mktemp)"
  "$exe" --http --port 0 >"$log" 2>&1 &
  pid=$!
  echo "$pid" >> "$BG_PIDS_FILE"
  for _ in $(seq 1 60); do
    kill -0 "$pid" 2>/dev/null || { echo "::error::http worker '$exe' exited" >&2; cat "$log" >&2; return 1; }
    port="$(sed -n 's/.*PORT:\([0-9]*\).*/\1/p' "$log" | head -1)"
    [ -n "$port" ] && break
    sleep 0.5
  done
  [ -n "$port" ] || { echo "::error::http worker '$exe' never reported a port" >&2; cat "$log" >&2; return 1; }
  BOOTED_PORT="$port"
}

# version_mismatch.test attaches a worker that advertises an incompatible
# protocol_version; set on the subprocess lanes (skips elsewhere via require-env).
export VGI_BAD_PROTOCOL_WORKER="$BAD_PROTOCOL"

# The simple_writable fixture worker un-skips the cross-language
# simple_writable/*.test write-path tests. Set on every lane; they self-skip
# over http (skip-on-error 'HTTP').
export VGI_SIMPLE_WRITABLE_WORKER="$SIMPLE_WRITABLE"

case "$TRANSPORT" in
  stdio|shm)
    # Subprocess transport (the primary lane). shm is identical plus the POSIX
    # shared-memory side channel via VGI_RPC_SHM_SIZE_BYTES.
    export VGI_TEST_WORKER="$WORKER"
    export VGI_VERSIONED_WORKER="$VERSIONED"
    export VGI_VERSIONED_TABLES_WORKER="$VERSIONED_TABLES"
    export VGI_ATTACH_OPTIONS_WORKER="$ATTACH_OPTIONS"
    # Serve the versioned catalogs over HTTP too: attach/versioned_tables_*_http
    # and versioning_http attach an http:// worker regardless of the main transport.
    boot_http_worker "$VERSIONED_TABLES"; vth_port="$BOOTED_PORT"
    export VGI_VERSIONED_TABLES_HTTP_WORKER="http://localhost:${vth_port}"
    boot_http_worker "$VERSIONED"; vh_port="$BOOTED_PORT"
    export VGI_VERSIONED_HTTP_WORKER="http://localhost:${vh_port}"
    SUITE_GLOB="test/sql/integration/*"
    ;;
  launch)
    # AF_UNIX launcher transport. Only the launcher-only tests opt in here
    # (the rest of the suite runs on the stdio lane); mirrors make test-launcher.
    export VGI_TEST_WORKER="launch:${WORKER}"
    export VGI_REQUIRE_LAUNCHER_TRANSPORT=1
    SUITE_GLOB="test/sql/integration/launcher/*"
    ;;
  http)
    # Whole-suite-over-HTTP (mirrors make test_http). Every ATTACH goes over
    # http://, so staging injected `LOAD httpfs` (AWK_HTTP=1) and dropped the
    # http-incompatible files. VGI_REQUIRE_LAUNCHER_TRANSPORT is NOT set (the
    # launcher-only tests must skip on this lane).
    #
    # Only the main worker is booted as an http server. The versioned /
    # versioned_tables http-worker env vars are deliberately left UNSET, so the
    # attach/versioned_tables_*_http and versioning_http tests skip (require-env)
    # — they are covered over http on the stdio lane, which boots those workers.
    boot_http_worker "$WORKER"; port="$BOOTED_PORT"
    export VGI_TEST_WORKER="http://localhost:${port}"
    SUITE_GLOB="test/sql/integration/*"
    ;;
  *)
    echo "::error::unknown TRANSPORT=$TRANSPORT (expected stdio|shm|launch|http)"; exit 1 ;;
esac

cd "$STAGE"

echo "Warming the extension cache (vgi from community, deps from core) ..."
mkdir -p "$STAGE/test"
# FORCE INSTALL vgi re-downloads the currently-published community build,
# overriding any older cached copy, so the suite runs against what users can
# install today (and so a freshly-published extension is picked up immediately).
cat > "$STAGE/test/_warm.test" <<'EOF'
# name: test/_warm.test
# group: [warm]
statement ok
FORCE INSTALL vgi FROM community;

statement ok
INSTALL httpfs FROM core;

statement ok
INSTALL json FROM core;

statement ok
INSTALL parquet FROM core;

statement ok
INSTALL spatial FROM core;
EOF
"$HAYBARN_UNITTEST" "test/_warm.test" >/dev/null 2>&1 || echo "::warning::extension warm step did not fully succeed"
rm -f "$STAGE/test/_warm.test"

# Run the lane, streaming the native sqllogictest report (a progress line per
# file + the final "All tests passed (.. N assertions ..)" summary). Out-of-scope
# tests were dropped at staging; any failed assertion exits non-zero.
#
# simple_writable runs in its OWN invocation (a fresh DuckDB process / worker
# pool). Its table-in-out write workers otherwise leave warm pooled connections
# that perturb the immediately-following crash-recovery test
# (table_in_out/table_buffering_pool_recovery). A separate process gives the
# crash test a clean pool. The launcher lane runs only launcher/* (no
# simple_writable); the http lane's writes self-skip.
echo "Running suite ($SUITE_GLOB, transport=$TRANSPORT) ..."
suite_rc=0
if [ "$TRANSPORT" = "launch" ]; then
  "$HAYBARN_UNITTEST" "$SUITE_GLOB" || suite_rc=$?
else
  "$HAYBARN_UNITTEST" "$SUITE_GLOB" "~test/sql/integration/simple_writable/*" || suite_rc=$?
  echo "Running simple_writable (isolated process) ..."
  "$HAYBARN_UNITTEST" "test/sql/integration/simple_writable/*" || suite_rc=$?
fi

exit "$suite_rc"
