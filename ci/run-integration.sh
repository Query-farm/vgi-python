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
# `test_launcher` / `test_http` targets (the canonical local way to run this
# suite against the Python worker — see vgi-python/CLAUDE.md). The `shm` lane
# deliberately differs from Makefile `test_shm`: it layers the shm side channel
# on the LAUNCHER rather than on raw subprocess, to avoid re-forking a Python
# worker per connection. See the shm|launch case below.
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
# Nothing is dropped on the launch lane. The vgi Makefile's test_launcher
# excludes two files, and neither applies here:
#   * test/sql/vgi_worker_pool.test — outside test/sql/integration, so it is
#     never staged by the find below.
#   * table/filter_echo_partitioned.test — excluded upstream for asserting >1
#     distinct worker_pid, which AF_UNIX socket pooling can't satisfy. That
#     rationale is stale: the test now counts transport-neutral `conn=` ids
#     instead (see its own comment), and it passes over launch: against the
#     Python fixture worker. Keeping it preserves coverage of exactly what the
#     launcher changes — connection multiplexing.
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
  # cache/revalidate.test now runs on http too: HTTP conditional revalidation is
  # implemented (C++ /init-request validators + vgi-rpc >=0.24.0 surfacing them to
  # the producer's first process()). It needs a community vgi extension carrying
  # that C++ change; if the http lane fails on it, the community extension predates
  # the change and needs republishing.
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
  # (bool_in_union.test is NOT excluded here: it disables itself with `mode skip`
  # upstream — a Haybarn/DuckDB Arrow serialization bug, arch-dependent — so the
  # flag in the test is the single source of truth.)
  find . -name '*.test' \
       -not -path './writable/*' \
       -not -name 'nested_type_combinations.test' \
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

# Background workers (http servers) are tracked in a file and SIGTERMed on exit.
# Each line is "<pid>\t<logfile>".
BG_PIDS_FILE="$(mktemp)"
# shellcheck disable=SC2329  # invoked indirectly via `trap cleanup EXIT` below
cleanup() {
  [ -f "$BG_PIDS_FILE" ] || return 0
  while IFS="$(printf '\t')" read -r p _; do
    [ -n "$p" ] && kill "$p" 2>/dev/null || true
  done < "$BG_PIDS_FILE"
}
trap cleanup EXIT

# assert_bg_workers_alive — fail if any background http worker died during a
# run, dumping its log. A dead shared worker is otherwise near-invisible: the
# runner's default error handling turns the resulting cascade of failed ATTACHes
# into skips, so the lane still prints "All tests passed" having tested nothing
# past the point of death (the failure mode that hid 136 voided files in run
# 29979884253). Cheap insurance against a green-but-empty lane.
assert_bg_workers_alive() {
  local rc=0 p log
  [ -s "$BG_PIDS_FILE" ] || return 0
  while IFS="$(printf '\t')" read -r p log; do
    [ -n "$p" ] || continue
    kill -0 "$p" 2>/dev/null && continue
    echo "::error::background http worker (pid $p) died during the run — everything" \
         "attached to it after that point was not really tested. Its log follows."
    [ -n "$log" ] && [ -f "$log" ] && sed -n '1,200p' "$log"
    rc=1
  done < "$BG_PIDS_FILE"
  return "$rc"
}

# boot_http_worker <binary> — start it as an HTTP server on an ephemeral port and
# set BOOTED_PORT to the port it reports (PORT:<n>, the worker's readiness
# contract). Sets a global rather than echoing so the backgrounded worker stays a
# child of this shell (a $(...) subshell would reparent it and break teardown).
#
# The worker is spawned with cwd=$STAGE — the same directory the unittest binary
# runs from below (`cd "$STAGE"`). copy_from/copy_to tests have DuckDB write a
# source file under a relative `__TEST_DIR__` (duckdb_unittest_tempdir/...) that
# the worker then opens by the same relative path; the worker only resolves it
# if it shares DuckDB's cwd. On the stdio lane the C++ extension spawns the
# worker as a subprocess that inherits DuckDB's cwd, so it matches for free; the
# `( cd … ; exec … )` subshell gives the http worker the same footing (and
# mirrors the vgi repo's run_http_integration.sh, which boots server and DuckDB
# from one cwd). exec keeps the worker on the subshell's pid so teardown via the
# captured `$!` still works.
BOOTED_PORT=""
boot_http_worker() {
  local exe="$1" log pid port=""
  BOOTED_PORT=""
  log="$(mktemp)"
  ( cd "$STAGE" && exec "$exe" --http --port 0 ) >"$log" 2>&1 &
  pid=$!
  printf '%s\t%s\n' "$pid" "$log" >> "$BG_PIDS_FILE"
  for _ in $(seq 1 60); do
    kill -0 "$pid" 2>/dev/null || { echo "::error::http worker '$exe' exited" >&2; cat "$log" >&2; return 1; }
    port="$(sed -n 's/.*PORT:\([0-9]*\).*/\1/p' "$log" | head -1)"
    [ -n "$port" ] && break
    sleep 0.5
  done
  [ -n "$port" ] || { echo "::error::http worker '$exe' never reported a port" >&2; cat "$log" >&2; return 1; }
  BOOTED_PORT="$port"
}

# On the launcher-family lanes EVERY worker is fronted by `launch:` so the C++
# launcher serves it (ResolveLauncherSocketPath -> AF_UNIX -> UnixSocketWorker),
# as the vgi Makefile's test_launcher does. A worker left unprefixed would
# silently run over stdio inside a launcher lane. Empty on stdio and http.
LAUNCH_PREFIX=""
case "$TRANSPORT" in shm|launch) LAUNCH_PREFIX="launch:" ;; esac

# version_mismatch.test attaches a worker that advertises an incompatible
# protocol_version; set on the subprocess lanes (skips elsewhere via require-env).
export VGI_BAD_PROTOCOL_WORKER="${LAUNCH_PREFIX}${BAD_PROTOCOL}"

# The simple_writable fixture worker un-skips the cross-language
# simple_writable/*.test write-path tests. Set on every lane — on the http lane
# it stays a binary path, so those tests run over subprocess there (they do NOT
# self-skip: the http lane passes all 5 files).
export VGI_SIMPLE_WRITABLE_WORKER="${LAUNCH_PREFIX}${SIMPLE_WRITABLE}"

case "$TRANSPORT" in
  stdio)
    # Subprocess transport (the primary lane) — the only lane that spawns a
    # fresh worker process per DuckDB connection, and so the only one that can
    # host the crash / pool-recovery tests below.
    export VGI_TEST_WORKER="$WORKER"
    # Presence gate for the crash / pool-recovery tests
    # (table_in_out/table_buffering_{worker_crash,pool_recovery}). The value is
    # not a worker spec — the SQL attaches the normal pooled worker; this just
    # un-skips the tests.
    #
    # DEDICATED (subprocess) LANE ONLY. Those tests call the `crash_on_process`
    # fixture, which does os.kill(getpid(), SIGKILL). Under subprocess transport
    # that kills the per-DuckDB-process worker child and the pool recovers —
    # exactly what the tests assert. Under a SHARED-worker transport (http://,
    # unix://, launch:) it kills the single process serving the whole suite, and
    # every later ATTACH fails. Exporting it unconditionally silently voided 136
    # of 286 files on the http lane while still printing "All tests passed"
    # (run 29979884253) — the crash landed at file 132 and the runner's default
    # error handling swallowed the cascade. Since shm now runs over the launcher
    # (a shared worker), stdio is the ONLY lane that may set this.
    export VGI_TEST_DEDICATED_WORKER=1
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
  shm|launch)
    # AF_UNIX launcher transport — the WHOLE suite with every worker fronted by
    # `launch:`, mirroring the vgi Makefile's test_launcher. Validates that the
    # launcher path produces identical query results to the subprocess path;
    # running only launcher/* here would leave the transport almost untested.
    # VGI_REQUIRE_LAUNCHER_TRANSPORT additionally un-skips launcher/*, whose
    # options only apply to the launch: dispatch path (the other lanes skip them).
    #
    # `shm` is this same lane plus the POSIX shared-memory side channel
    # (VGI_RPC_SHM_SIZE_BYTES, exported by the workflow). It rides the launcher
    # rather than raw subprocess because stdio spends most of its wall-clock
    # fork+exec'ing a fresh Python worker per connection; the launcher keeps one
    # warm worker per argv and reuses it, so the lane exercises the same shm
    # paths far faster. The side channel is transport-independent — it is
    # negotiated via the __transport_options__ handshake and carried in POSIX
    # shm, not in the pipe/socket — and engages identically here (verified: the
    # same 18 [shm] transfers under VGI_RPC_SHM_DEBUG on stdio and on launch).
    #
    # The versioned / versioned_tables *http* worker env vars are deliberately
    # left UNSET (as in test_launcher), so attach/versioned_tables_*_http and
    # versioning_http skip — they're covered over http on the stdio lane.
    export VGI_TEST_WORKER="launch:${WORKER}"
    export VGI_VERSIONED_WORKER="launch:${VERSIONED}"
    export VGI_VERSIONED_TABLES_WORKER="launch:${VERSIONED_TABLES}"
    export VGI_ATTACH_OPTIONS_WORKER="launch:${ATTACH_OPTIONS}"
    export VGI_REQUIRE_LAUNCHER_TRANSPORT=1
    SUITE_GLOB="test/sql/integration/*"
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
# crash test a clean pool. Every lane does the split: simple_writable always
# attaches a spawned binary (VGI_SIMPLE_WRITABLE_WORKER), even on the http lane.
echo "Running suite ($SUITE_GLOB, transport=$TRANSPORT) ..."
suite_rc=0

# run_unittest — invoke haybarn-unittest, streaming its output, and additionally
# fail on a fatal-signal report that the process's own exit code cannot express.
#
# Catch2 arms handlers for SIGTERM/SIGINT/SIGSEGV/... for the duration of a test
# case. Those handlers are inherited by any process the extension fork()s, and
# run in the child if a signal lands before it execs. The child then prints a
# full "FAILED: ... due to a fatal error condition: SIGTERM" block plus a run
# summary — the *parent's* accumulated counters, since it's an address-space
# copy — and dies. The parent never sees it, records no failure, and exits 0.
# The only trace is on stdout, so that is what we scan. Seen on the shm lane in
# https://github.com/Query-farm/vgi-python/actions/runs/29051359074.
run_unittest() {
  local log rc=0
  log="$(mktemp)"
  # `set +e` rather than `|| true`: the latter runs before PIPESTATUS is read and
  # overwrites it with true's 0, silently swallowing every real test failure.
  set +e
  "$HAYBARN_UNITTEST" "$@" 2>&1 | tee "$log"
  rc="${PIPESTATUS[0]}"
  set -e
  if grep -q 'due to a fatal error condition' "$log"; then
    echo "::error::a forked child ran the test harness's signal handler (see the" \
         "'fatal error condition' block above). The parent exited $rc and would" \
         "otherwise have passed. This is invisible to the exit code by construction."
    rc=1
  fi
  rm -f "$log"
  return "$rc"
}

run_unittest "$SUITE_GLOB" "~test/sql/integration/simple_writable/*" || suite_rc=$?
assert_bg_workers_alive || suite_rc=1

echo "Running simple_writable (isolated process) ..."
run_unittest "test/sql/integration/simple_writable/*" || suite_rc=$?
assert_bg_workers_alive || suite_rc=1

exit "$suite_rc"
