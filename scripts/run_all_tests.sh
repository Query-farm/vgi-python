#!/usr/bin/env bash
# Run pytest + the C++ integration suite IN PARALLEL, capture full output to
# files under /tmp/vgi-test-cache/, and emit a one-line summary per suite.
#
# Subsequent reads use `cat /tmp/vgi-test-cache/<file>` instead of re-running.
#
# Flags:
#   --pytest-only        skip the C++ integration tests
#   --integration-only   skip pytest
#   --quiet              don't print the summary, just exit
set -uo pipefail

CACHE=/tmp/vgi-test-cache
mkdir -p "$CACHE"

PY_LOG="$CACHE/pytest.log"
PY_SUMMARY="$CACHE/pytest.summary"
INT_LOG="$CACHE/integration.log"
INT_SUMMARY="$CACHE/integration.summary"

run_pytest=1
run_integration=1
quiet=0
for arg in "$@"; do
  case "$arg" in
    --pytest-only) run_integration=0 ;;
    --integration-only) run_pytest=0 ;;
    --quiet) quiet=1 ;;
    *) echo "unknown flag: $arg" >&2; exit 2 ;;
  esac
done

run_pytest_job() {
  cd /Users/rusty/Development/vgi-python || exit 99
  uv run pytest --no-cov -n auto >"$PY_LOG" 2>&1
  local rc=$?
  {
    grep -E '^(FAILED|ERROR )|passed|failed|error' "$PY_LOG" | tail -200
    echo "exit=$rc"
  } >"$PY_SUMMARY"
}

run_integration_job() {
  cd /Users/rusty/Development/vgi || exit 99
  VGI_TEST_WORKER="uv run --project /Users/rusty/Development/vgi-python vgi-fixture-worker" \
    timeout 600 ./build/release/test/unittest "test/sql/integration/*" >"$INT_LOG" 2>&1
  local rc=$?
  {
    grep -E 'FAILED|fail|test cases' "$INT_LOG" | tail -200
    echo "exit=$rc"
  } >"$INT_SUMMARY"
}

pids=()
if [[ "$run_pytest" == 1 ]]; then
  run_pytest_job &
  pids+=($!)
fi
if [[ "$run_integration" == 1 ]]; then
  run_integration_job &
  pids+=($!)
fi

for pid in "${pids[@]}"; do
  wait "$pid"
done

if [[ "$quiet" == 0 ]]; then
  if [[ "$run_pytest" == 1 ]]; then
    echo "=== pytest ==="
    tail -1 "$PY_LOG"
    echo "  log:     $PY_LOG"
    echo "  summary: $PY_SUMMARY"
  fi
  if [[ "$run_integration" == 1 ]]; then
    echo "=== integration ==="
    grep "test cases" "$INT_LOG" | tail -1
    echo "  log:     $INT_LOG"
    echo "  summary: $INT_SUMMARY"
  fi
fi
