#!/usr/bin/env bash
# Run pytest + the C++ integration suite IN PARALLEL, capture full output to
# files under /tmp/vgi-test-cache/, and emit rich, ready-to-read summaries.
#
# THIS SCRIPT EXISTS SO YOU NEVER NEED TO RE-RUN A 5-MINUTE TEST SUITE JUST
# TO GREP THE OUTPUT. After one run, the full log + extracted failure context
# stay on disk; subsequent inspection is a `cat` away.
#
# Outputs (under /tmp/vgi-test-cache/):
#   pytest.log          full pytest output
#   pytest.summary      pass/fail totals + FAILED/ERROR lines + tracebacks
#   pytest.failures     just the FAILED test paths (one per line)
#   integration.log     full DuckDB unittest output
#   integration.summary pass/fail totals + per-test failure blocks
#   integration.failures just the failing .test file paths (one per line)
#
# Flags:
#   --pytest-only        skip the C++ integration tests
#   --integration-only   skip pytest
#   --quiet              don't print summaries to stdout
#   --show               print cached summaries WITHOUT re-running
set -uo pipefail

CACHE=/tmp/vgi-test-cache
mkdir -p "$CACHE"

PY_LOG="$CACHE/pytest.log"
PY_SUMMARY="$CACHE/pytest.summary"
PY_FAILURES="$CACHE/pytest.failures"
INT_LOG="$CACHE/integration.log"
INT_SUMMARY="$CACHE/integration.summary"
INT_FAILURES="$CACHE/integration.failures"

run_pytest=1
run_integration=1
quiet=0
show_only=0
for arg in "$@"; do
  case "$arg" in
    --pytest-only) run_integration=0 ;;
    --integration-only) run_pytest=0 ;;
    --quiet) quiet=1 ;;
    --show) show_only=1 ;;
    -h|--help) sed -n '2,22p' "$0"; exit 0 ;;
    *) echo "unknown flag: $arg" >&2; exit 2 ;;
  esac
done

# Extract failure context from pytest output. Captures FAILED/ERROR test ids
# plus traceback blocks (between "_ test_foo _" separators and the final
# "==== short test summary").
summarize_pytest() {
  local log="$1" summary="$2" failures="$3"
  {
    echo "### TOTALS ###"
    grep -E '^=+.*(passed|failed|error)' "$log" | tail -3
    echo
    echo "### FAILED / ERROR LINES ###"
    grep -E '^(FAILED|ERROR) ' "$log" | head -200
    echo
    echo "### FAILURE BLOCKS ###"
    awk '
      /^=+ FAILURES =+/  { in_fail=1; next }
      /^=+ ERRORS =+/    { in_fail=1; next }
      /^=+ short test summary info =+/ { in_fail=0 }
      in_fail { print }
    ' "$log" | head -800
  } >"$summary"

  grep -E '^(FAILED|ERROR) ' "$log" | awk '{print $2}' >"$failures" || true
}

# Extract failure context from DuckDB unittest output (Catch v2 console reporter +
# sqllogictest custom messages). Failures look like:
#
#   .../path/to/test.test:LINE: FAILED:
#   ...
#   with message:
#   ...
#
# or sqllogictest-specific:
#
#   ===== Wrong result in query! =====
#   QUERY: SELECT ...
#   Expected ... Got ...
#
# Strategy: sweep the log once and emit, for every line containing "FAILED"
# or "Wrong result", a 25-line context window. Also list unique .test paths
# that appear in those windows.
summarize_integration() {
  local log="$1" summary="$2" failures="$3"
  {
    echo "### TOTALS ###"
    grep -E 'test cases:|assertions:' "$log" | tail -2
    echo
    echo "### FAILED TESTS (Catch summary) ###"
    grep -E 'failed (assertion|in test)' "$log" | head -200
    echo
    echo "### FAILURE BLOCKS (25-line context windows) ###"
    awk '
      BEGIN { ctx=25 }
      {
        line[NR]=$0
        if ($0 ~ /FAILED:|Wrong result|RESULT MISMATCH|FAILURE in|failed in test|\[FAIL\]/) {
          marks[NR]=1
        }
      }
      END {
        for (i=1; i<=NR; i++) {
          if (marks[i]) {
            lo=i-2; if (lo<1) lo=1
            hi=i+ctx
            if (lo <= last_hi) lo = last_hi+1
            if (lo > 1 && lo > last_hi+1) print "---"
            for (j=lo; j<=hi && j<=NR; j++) print line[j]
            last_hi=hi
          }
        }
      }
    ' "$log" | head -1500
  } >"$summary"

  # Extract unique .test paths that appear in failure windows.
  awk '
    BEGIN { ctx=25 }
    {
      line[NR]=$0
      if ($0 ~ /FAILED:|Wrong result|RESULT MISMATCH|FAILURE in|failed in test|\[FAIL\]/) {
        marks[NR]=1
      }
    }
    END {
      for (i=1; i<=NR; i++) {
        if (marks[i]) {
          lo=i-2; if (lo<1) lo=1; hi=i+ctx
          for (j=lo; j<=hi && j<=NR; j++) {
            if (match(line[j], /test\/sql\/[A-Za-z0-9_\/\.\-]+\.test/)) {
              path=substr(line[j], RSTART, RLENGTH)
              if (!seen[path]++) print path
            }
          }
        }
      }
    }
  ' "$log" >"$failures" || true
}

print_summary() {
  if [[ "$run_pytest" == 1 ]]; then
    echo "=== pytest ==="
    head -3 "$PY_SUMMARY" | grep -v '^### '
    local nfail
    nfail=$(wc -l <"$PY_FAILURES" | tr -d ' ')
    echo "  failures: ${nfail:-0}  (see $PY_FAILURES)"
    echo "  log:      $PY_LOG"
    echo "  summary:  $PY_SUMMARY"
  fi
  if [[ "$run_integration" == 1 ]]; then
    echo "=== integration ==="
    grep -E 'test cases:|assertions:' "$INT_LOG" | tail -2
    local nfail
    nfail=$(wc -l <"$INT_FAILURES" | tr -d ' ')
    echo "  failures: ${nfail:-0}  (see $INT_FAILURES)"
    echo "  log:      $INT_LOG"
    echo "  summary:  $INT_SUMMARY  (failure blocks)"
  fi
}

if [[ "$show_only" == 1 ]]; then
  print_summary
  exit 0
fi

run_pytest_job() {
  cd /Users/rusty/Development/vgi-python || exit 99
  uv run pytest --no-cov -n auto >"$PY_LOG" 2>&1
  local rc=$?
  summarize_pytest "$PY_LOG" "$PY_SUMMARY" "$PY_FAILURES"
  echo "exit=$rc" >>"$PY_SUMMARY"
}

run_integration_job() {
  cd /Users/rusty/Development/vgi || exit 99
  VGI_TEST_WORKER="uv run --project /Users/rusty/Development/vgi-python vgi-fixture-worker" \
    timeout 600 ./build/release/test/unittest "test/sql/integration/*" >"$INT_LOG" 2>&1
  local rc=$?
  summarize_integration "$INT_LOG" "$INT_SUMMARY" "$INT_FAILURES"
  echo "exit=$rc" >>"$INT_SUMMARY"
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
  print_summary
fi
