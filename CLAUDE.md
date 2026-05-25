# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

```bash
uv sync --all-extras      # Install dependencies
uv run pytest -n auto     # Run tests using pytest-xdist in parallel.
uv run ruff check .       # Lint
uv run ruff format .      # Format
uv run mypy vgi/          # Type check

# Run tests with coverage (includes subprocess/worker coverage)
uv run coverage run -m pytest --no-cov -n auto
uv run coverage combine   # Merge subprocess coverage data
uv run coverage report    # Show coverage report
uv run coverage html      # Generate HTML report in htmlcov/

# Test documentation examples (validates Python code blocks in markdown)
uv run pytest tests/test_documentation_examples.py -v
```

When you run pytest I prefer that you include "-n auto" to run tests in parallel. This allows the tests to complete faster.

**Before committing**, always run lint and format checks:
```bash
uv run ruff check --fix . && uv run ruff format . && uv run mypy vgi/
```

Before running `pytest`, you must run ruff's check and fix commands, otherwise fixing problems
takes longer:

```bash
uv run ruff check --fix . && uv run ruff format .
```

When making changes, we don't need to worry about backward compatibility, make the changes and change the import references.

## Conformance Tests

Python conformance tests live in `tests/conformance/` and mirror the C++ integration tree
(`~/Development/vgi/test/sql/integration/`). They drive the Python `Client` against the
example worker across every feature area exercised by the C++ suite — drift guard so a
new C++ capability cannot land without a Python counterpart.

These run automatically as part of `uv run pytest -n auto` (picked up via
`testpaths = ["tests"]`). The `test_directory_parity.py` test enforces that every
subdirectory under `vgi/test/sql/integration/` has a matching `test_<area>.py` here;
exemptions go in `_EXEMPTIONS` with a reason. The check skips when the C++ repo isn't
present (acceptable for Python-only CI workers).

## Integration Testing

Integration tests live in the `vgi` C++ repo (sibling directory) at `test/sql/integration/`.
They run DuckDB's sqllogictest framework against a real VGI worker. Run from the `vgi` directory.

**Important:** When adding or modifying integration tests, or adding/changing functions registered
in the example worker, you **must** run the DuckDB integration tests (subprocess transport) before
considering the work complete. Always run the **full** test suite (`test/sql/integration/*`) to
ensure all tests pass — not just the new/changed test file. Adding a function can break
`function_registration.test` counts, for example.

**Run-once, inspect-many: use `scripts/run_all_tests.sh`.** The full subprocess
integration suite takes minutes. Re-running it just to grep its output is
wasteful — instead, run it through this wrapper, which captures the full log
and pre-extracts failure context to disk:

```bash
scripts/run_all_tests.sh                    # pytest + integration in parallel
scripts/run_all_tests.sh --integration-only # just DuckDB integration
scripts/run_all_tests.sh --pytest-only      # just pytest
scripts/run_all_tests.sh --show             # print summaries from cache (no run)
```

Outputs land in `/tmp/vgi-test-cache/`:

| File | What it contains |
|---|---|
| `integration.log` | full unittest stdout/stderr |
| `integration.summary` | pass/fail totals + 25-line context windows around every failure |
| `integration.failures` | unique failing `.test` paths, one per line |
| `pytest.log` | full pytest stdout/stderr |
| `pytest.summary` | totals, FAILED/ERROR lines, full traceback blocks |
| `pytest.failures` | failing pytest node ids |

**Investigate failures by reading the cache, not by re-running.** `cat
/tmp/vgi-test-cache/integration.summary` to see every failure with context;
`cat /tmp/vgi-test-cache/integration.failures` to get just the file list. Only
re-run after you've made a code change you want to verify. The integration
log is fully reproducible across runs — re-grepping with different flags is
free; re-running is not.

**Direct invocation** (use only when you genuinely need to bypass the cache,
e.g. to pass an unusual filter to `unittest`):
```bash
cd ~/Development/vgi
VGI_TEST_WORKER="uv run --project ~/Development/vgi-python vgi-fixture-worker" \
    ./build/release/test/unittest "test/sql/integration/*"
```

**HTTP transport** (worker as an HTTP server):
```bash
cd ~/Development/vgi
./test/run_http_integration.sh "test/sql/integration/*"
```

The HTTP script (`test/run_http_integration.sh`) starts the HTTP server, waits for it to be
ready, runs the tests, and cleans up. Always use this script instead of manually managing
the server. Server logs are at `/tmp/vgi-http-test-server.log`.

**HTTP transport with demo blob storage** (forces all batches through external storage):
```bash
cd ~/Development/vgi
VGI_DEMO_STORAGE=1 ./test/run_http_integration.sh "test/sql/integration/*"
```

This starts the server with `--demo-storage` and `--externalize-threshold-bytes 1`,
forcing every batch through the in-process blob store. Control threshold and
compression via `VGI_EXTERNALIZE_THRESHOLD_BYTES` and `VGI_EXTERNALIZE_COMPRESSION`
env vars.

**Note:** These tests require the C++ VGI extension to support resolving external
location pointer batches. Until that support lands, all tests will fail with
`Empty response` errors because the C++ client receives pointer batches instead
of actual data.

**Filter by test subset:**
```bash
./test/run_http_integration.sh "test/sql/integration/secret/*"
./test/run_http_integration.sh "test/sql/integration/table/countdown*"
```

**Profiling integration test timing:**

The DuckDB unittest binary has per-query timing instrumentation in the
sqllogictest runner (`duckdb/test/sqlite/sqllogic_command.cpp`). When enabled,
each statement and query emits `[stmt ...]` or `[query ...]` lines to stderr
with elapsed milliseconds. This helps identify slow queries and bottlenecks.

```bash
# Timing output goes to stderr, grep for the bracket-prefixed lines:
cd ~/Development/vgi
VGI_TEST_WORKER="uv run --project ~/Development/vgi-python vgi-fixture-worker" \
    ./build/release/test/unittest "test/sql/integration/table/writable_table*" \
    2>&1 | grep "^\[stmt\|^\[query" | sort -t']' -k2 -rn
```

Note: each VGI query has ~270ms connection setup overhead (worker spawn + bind +
init). A test file with 100 statements takes ~27s just in overhead. This is
normal for subprocess transport.

**Combined coverage** (pytest + subprocess + HTTP integration):

The integration tests exercise real protocol paths that unit tests don't cover:
subprocess tests hit filter pushdown and parallel workers; HTTP tests additionally
hit state serialization (`rehydrate`), `_resolve_state_type`, and `_to_row_dict`.

```bash
# 1. Clean old data
find . -name '.coverage*' -not -path './.venv/*' -delete

# 2. Run pytest with coverage
uv run coverage run -m pytest --no-cov -n auto

# 3. Run DuckDB subprocess integration tests with coverage
cd ~/Development/vgi
VGI_TEST_WORKER="/tmp/vgi-coverage-worker.sh" \
    ./build/release/test/unittest "test/sql/integration/*"

# 4. Run DuckDB HTTP integration tests with coverage
cd ~/Development/vgi
VGI_PYTHON_DIR=/Users/rusty/Development/vgi-python \
    ./test/run_http_integration_coverage.sh "test/sql/integration/*"

# 5. Combine and report
cd ~/Development/vgi-python
uv run coverage combine
uv run coverage report --show-missing --skip-covered
```

**Wrapper scripts** — needed because the C++ test binary spawns the Python worker
as a subprocess, so `coverage`'s `patch = ["subprocess"]` (which patches Python's
`subprocess.Popen`) has no effect. The wrappers `cd` to the project so `pyproject.toml`
coverage settings are found.

Subprocess worker wrapper (`/tmp/vgi-coverage-worker.sh`):
```bash
#!/bin/bash
cd /Users/rusty/Development/vgi-python
exec uv run coverage run --parallel-mode -m vgi._test_fixtures.worker "$@"
```

HTTP server wrapper (`~/Development/vgi/test/run_http_integration_coverage.sh`):
Same as `run_http_integration.sh` but replaces the `uv run ... vgi-serve` line with
`uv run ... coverage run --parallel-mode -m vgi.serve` so the HTTP server process
writes coverage data. See the script for details.

**Running ad-hoc SQL against DuckDB CLI:**
Use `-f` to supply SQL files to DuckDB, not stdin redirection. Do not redirect stderr to stdout.
```bash
# Correct:
~/Development/vgi/build/debug/duckdb -f /tmp/my_test.sql

# Wrong:
~/Development/vgi/build/debug/duckdb < /tmp/my_test.sql 2>&1
```

**Known HTTP failures** (3 tests fail, not regressions):
- `table/partitioned_sequence.test` — partition-local state not preserved across HTTP exchanges
- `table_in_out/buffer_input/sizes.test` — input buffering semantics differ over HTTP
- `table_in_out/buffer_input/scale.test_slow` — input buffering semantics differ over HTTP

Two assertions are also skipped (via `skip on error_message matching 'HTTP'`).

### Demo Blob Storage (External Batch Offloading)

The example HTTP server supports in-process blob storage for demonstrating and testing
external record batch offloading without S3 or any cloud infrastructure.

**Running the example server with demo storage:**
```bash
vgi-fixture-http --demo-storage
vgi-fixture-http --demo-storage --externalize-threshold-bytes 4096 --externalize-compression zstd
```

When `--demo-storage` is enabled:
- Batches larger than `--externalize-threshold-bytes` are stored in-memory and replaced
  with pointer batches containing `http://` URLs to `/__blobs__/{id}` endpoints
- Upload URLs are supported via the `__upload_url__` endpoint for client-side uploads
- `VGI-Max-Request-Bytes` is advertised and enforced (413 for oversized requests)
- `--demo-storage` and `--s3-bucket` are mutually exclusive

**Running demo storage pytest tests:**
```bash
uv run pytest tests/test_http_demo_storage.py -n auto -v
```

These tests require `vgi-rpc[external]` (aiohttp, tenacity) for external location resolution.

## Project Overview

VGI (Vector Gateway Interface) provides an Apache Arrow-based protocol for connecting DuckDB to external programs. It enables user-defined functions to run in separate processes, communicating via stdin/stdout using Arrow IPC streaming.

## Architecture

FIXME: complete this,.

### Key Components

- **Worker** (`vgi/worker.py`): Subprocess that hosts functions
- **Client** (`vgi/client/client.py`): Spawns workers, streams data
- **ScalarFunction** (`vgi/scalar_function.py`): Base for scalar functions
- **TableFunctionGenerator** (`vgi/table_function.py`): Base for table functions
- **TableInOutFunction** (`vgi/table_in_out_function.py`): Base for table-in-out functions

## Project Structure

FIXME: complete this.

## CLI Commands

```bash
vgi-fixture-worker                                                    # Run example worker
vgi-client --input data.parquet --function echo --worker vgi-fixture-worker
vgi-client --input data.parquet --function sum_all_columns --worker vgi-fixture-worker
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `VGI_WORKER_DEBUG=1` | Enable DEBUG logging on worker and stderr passthrough on client (see below) |
| `VGI_FILTER_DEBUG=1` | Enable filter pushdown debug logging (see below) |
| `VGI_QUIET=1` | Suppress worker startup logging |
| `VGI_BEARER_TOKENS` | Comma-separated `token=principal` pairs for static bearer auth (HTTP only) |
| `VGI_JWT_ISSUER` | JWT issuer URL (requires `[oauth]` extra) |
| `VGI_JWT_AUDIENCE` | JWT audience string (comma-separated for multiple audiences) |
| `VGI_JWT_JWKS_URI` | JWKS endpoint URL (auto-discovered if omitted) |
| `VGI_OAUTH_RESOURCE` | OAuth resource URL for RFC 9728 metadata |
| `VGI_OAUTH_AUTH_SERVERS` | Comma-separated authorization server URLs |
| `VGI_OAUTH_CLIENT_ID` | Client ID for MCP compatibility (optional, URL-safe chars only) |
| `VGI_OAUTH_CLIENT_SECRET` | Client secret for OAuth (optional, URL-safe chars only; for public/PKCE clients) |
| `VGI_OAUTH_DEVICE_CODE_CLIENT_ID` | Client ID for device-code flow (optional, URL-safe chars only) |
| `VGI_OAUTH_DEVICE_CODE_CLIENT_SECRET` | Client secret for device-code flow (optional, URL-safe chars only) |
| `VGI_OAUTH_USE_ID_TOKEN` | When `1`/`true`/`yes`, clients use OIDC `id_token` as Bearer instead of `access_token` |
| `VGI_OTEL_ENABLED` | Enable OpenTelemetry instrumentation (`1`/`true`/`yes`) |
| `VGI_OTEL_CUSTOM_ATTRIBUTES` | Comma-separated `key=value` pairs for custom span/metric attributes |
| `VGI_OTEL_CLAIM_ATTRIBUTES` | Comma-separated `claim_key=span_attr_name` pairs for claim extraction |
| `VGI_OTEL_DISABLE_TRACING` | Disable tracing only (`1`/`true`/`yes`) |
| `VGI_OTEL_DISABLE_METRICS` | Disable metrics only (`1`/`true`/`yes`) |
| `SENTRY_DSN` | Enable Sentry error reporting (requires `[sentry]` extra). When set, `vgi-serve` calls `sentry_sdk.init()` before constructing the worker so vgi-rpc's auto-attach picks up RPC dispatch errors and VGI enriches with `vgi.function.name`, `vgi.attach_id`, `vgi.transaction_id`, etc. |
| `SENTRY_ENVIRONMENT` | Environment tag passed to `sentry_sdk.init()` (e.g. `production`, `staging`) |
| `SENTRY_RELEASE` | Release identifier passed to `sentry_sdk.init()` (e.g. git SHA). When unset, falls back to the installed `vgi` package version so every run is associated with a release; deploys should set this to a git SHA for commit tracking. |
| `SENTRY_TRACES_SAMPLE_RATE` | Float in `[0, 1]` for performance sampling (Sentry's standard knob) |
| `VGI_WORKER_SHARED_STORAGE` | Storage backend: `sqlite` (default), `azure-sql` (requires `[azure]` extra), or `cloudflare-do` |
| `VGI_AZURE_SQL_SERVER` | Azure SQL server hostname (required when `azure-sql`) |
| `VGI_AZURE_SQL_DATABASE` | Azure SQL database name (required when `azure-sql`) |
| `VGI_AZURE_SQL_USER` | SQL auth username (omit for managed identity) |
| `VGI_AZURE_SQL_PASSWORD` | SQL auth password (omit for managed identity) |
| `VGI_AZURE_SQL_DEBUG_LOG` | File path for Azure SQL storage debug/timing logs |
| `VGI_CF_DO_URL` | Cloudflare Worker URL (required when `cloudflare-do`) |
| `VGI_CF_DO_TOKEN` | Bearer token for Cloudflare Worker auth (optional) |
| `VGI_CF_DO_DEBUG_LOG` | File path for Cloudflare DO storage debug/timing logs |

### Worker Debug Mode

Set `VGI_WORKER_DEBUG=1` to enable comprehensive debugging for worker failures. This single env var has two effects:

1. **Worker side**: Enables DEBUG-level logging on all `vgi` and `vgi_rpc` loggers (equivalent to `--debug` CLI flag)
2. **Client side**: Forces `passthrough_stderr=True`, streaming worker logs to the terminal in real-time

```bash
VGI_WORKER_DEBUG=1 vgi-fixture-worker
```

When used from the Python client without this env var, errors from worker failures automatically include captured stderr (last 50 lines) in the `ClientError` message. This means integrators using C++ or other clients get the Python traceback in the error message instead of just a generic exit code.

```python test="skip"
# Without VGI_WORKER_DEBUG: stderr is captured and included in errors
with Client("./my_worker.py", pool=None) as client:
    try:
        list(client.table_function(function_name="broken"))
    except ClientError as e:
        # e.g.: "Worker Exception: ...\n\nWorker stderr:\nTraceback (most recent call last):..."
        print(e)

# With VGI_WORKER_DEBUG=1: stderr streams to terminal in real-time
# (error messages won't duplicate stderr since it went to terminal)
```

Accepts `1`, `true`, or `yes` (case-insensitive). Zero overhead when not set.

### Filter Pushdown Debug Logging

Enable `VGI_FILTER_DEBUG=1` to trace filter pushdown deserialization, parsing, and evaluation. Useful for debugging filter pushdown issues and understanding how filters are applied.

```bash
VGI_FILTER_DEBUG=1 vgi-fixture-worker
```

**Key events logged:**
- `deserialize_start` - When filter bytes are received (with byte size)
- `deserialize_specs` - Parsed filter specifications from JSON
- `parse_filter_*` - Individual filter parsing (constant, in, is_null, and, or, struct)
- `pushdown_filters_ready` - Deserialized filters summary
- `evaluate_start` - Beginning filter evaluation against a batch
- `evaluate_filter` - Each filter's result (rows passing)
- `evaluate_complete` - Final evaluation result (input rows, rows passing, rows filtered)
- `auto_apply_start/complete` - When auto_apply_filters triggers

**Example output:**
```
deserialize_start             ipc_bytes_size=600
deserialize_specs             num_filters=1 specs=[{'column_name': 'n', 'type': 'constant', 'op': 'ge', 'value_ref': 0}]
parse_filter_constant         column=n op=ge value=5 value_type=int64
pushdown_filters_ready        function=SequenceFunction num_filters=1 filter_summary=['ConstantFilter(n >= 5)']
evaluate_start                columns=['n'] input_rows=100 num_filters=1
evaluate_filter               filter_index=0 filter_repr='ConstantFilter(n >= 5)' rows_passing=50
evaluate_complete             input_rows=100 rows_passing=50 rows_filtered=50
auto_apply_complete           function=SequenceFunction input_rows=100 output_rows=50 rows_removed=50
```

**Performance:** Zero overhead when disabled (just a boolean check).

### Sentry Error Reporting

Set `SENTRY_DSN` (and install `vgi[sentry]`) to forward unhandled exceptions to Sentry. `vgi-serve` calls `sentry_sdk.init()` automatically before building the worker, which lets vgi-rpc's auto-attach hook (added in `vgi-rpc 0.12.0`) wire RPC-level instrumentation onto every `RpcServer`. VGI then layers on:

- **Dispatch-scoped scope tags** — `vgi.function.name`, `vgi.function.type`, `vgi.principal`, `vgi.auth_domain`, `vgi.authenticated`, plus init-time fields (`vgi.init.execution_id`, `vgi.init.phase`). `vgi.attach_id` and `vgi.transaction_id` are set by **vgi-rpc's** dispatch hook (since 0.14.0) on every method that carries them — top-level kwargs, request dataclasses, and `InitRequest.bind_call` are all unwrapped automatically. Tag values are 12-char SHA-256 prefixes, so the tag-value distribution UI stays bounded; the full hex remains in catalog breadcrumbs for direct lookup.
- **Per-batch breadcrumbs** under category `vgi.execute` carrying `function_name`, `function_type`, `duration_ms`, `input_rows`, `output_rows`, `input_bytes`, `output_bytes`, and `execution_id`. If a stream crashes mid-flight, the event timeline shows the size and shape of every preceding batch.
- **Catalog-lifecycle breadcrumbs** under categories `catalog.attach`, `catalog.detach`, `catalog.create`, `catalog.transaction.begin`, `catalog.transaction.commit`, `catalog.transaction.rollback`. These provide the mapping from short-hashed `vgi.attach_id` / `vgi.transaction_id` tags back to full hex IDs (and to catalog names) — without them, an event tagged `vgi.attach_id=4f3c2a1b9d8e` is unreadable to a developer.
- **Catalog-attach scope tags** — `vgi.catalog.name`, `vgi.data_version_spec`, `vgi.implementation_version` are set on the `catalog_attach` transaction so events fired during attach are filterable by catalog identity. Note: these tags only apply to the attach transaction itself, not subsequent operations on the attached catalog (one Sentry transaction per RPC method).
- **Standards-aligned user mapping** — JWT `sub` → `user.id`; `preferred_username` → `user.username`; `email` → `user.email`; `name` → `user.name`. Override per-IdP via `SentryConfig.user_claim_map` (e.g. Auth0 namespaced claims). Static bearer tokens populate only `user.id`.

**Attach options redaction:** by default *no* options are logged in the `catalog.attach` / `catalog.create` breadcrumbs because options routinely carry credentials. Implementers opt in via `CatalogInterface.loggable_attach_options(options) -> Mapping`, which returns a redacted, safe-to-log subset (host, region, bucket — never password/token/secret). When the override returns an empty mapping (the default), the `options` field is omitted from the breadcrumb entirely. See `docs/catalog-interface.md` for details.

**Releases.** `vgi-serve` populates `release` from `SENTRY_RELEASE` if set, otherwise from `importlib.metadata.version("vgi")`. Production deploys should set `SENTRY_RELEASE=$(git rev-parse HEAD)` (or a tagged release identifier) so Sentry's commit-tracking and regression-detection features can correlate events to specific commits. Publishing GitHub releases makes the release-comparison UI more useful.

The same enrichment applies to OTel spans when `VGI_OTEL_ENABLED=1` — both backends read from the same `VgiTracer.set_current_span_attributes()` call sites in `vgi/otel.py`. Either, neither, or both can be active in a process.

### Key Constraints for Scalar Functions:
- **1:1 row mapping**: Output must have exactly the same number of rows as input
- **Single column output**: Output schema has exactly one column named "result"
- **Type validation**: Input/output types are validated at runtime (TypeMismatchError on mismatch)

## Specialized Pattern Classes

For common use cases, VGI provides specialized base classes that handle boilerplate:

## Stream cancellation (`on_cancel`)

A streaming function (`TableFunctionGenerator` or
`TableInOutGenerator`) may override `on_cancel(cls, params, state)` to
release resources when the C++ extension tears down a scan early —
e.g., DuckDB `LIMIT` clauses, user `break`, Ctrl-C, or exception
unwind. The override receives the same `ProcessParams` that `process()`
sees, plus the current user state (possibly deserialized from an HTTP
state-token on a different worker than the one that originally built
it). Typical bodies close a DB cursor, cancel an upstream HTTP
request, or release a GPU buffer.

```python
class SlowCancellableFunction(TableFunctionGenerator[Args, State]):
    @classmethod
    def on_cancel(cls, params: ProcessParams[Args], state: State) -> None:
        if state.cursor is not None:
            state.cursor.close()
```

**Best-effort hook — do not rely on it for correctness.** Several
classes of cancellation skip `on_cancel`:

- Worker process kill (OOM, SIGKILL, crash), network partition, and
  some error-on-error unwinds do not run the hook at all.
- Mid-batch Ctrl-C: under VGI's lockstep RPC, a `ReadDataBatch` blocks
  until the worker produces. The extension only enqueues the cancel
  after the current batch returns, so a long `process()` (e.g. an LLM
  streaming call) finishes before `on_cancel` sees the signal. Expect
  up to "one batch of latency" after Ctrl-C.
- **HTTP with `max_workers > 1`:** the cancel POST routes to any
  worker in the pool, not necessarily the one that originally handled
  the stream. `on_cancel` runs on the receiving worker after
  deserializing the state, so it cannot reach process-local resources
  (file handles, in-memory buffers) that live on the original worker.
  Users who need guaranteed release should either set `max_workers=1`,
  use subprocess transport, or keep resources in shared infrastructure
  (Redis, DB pool) whose handle is derivable from the serialized state.

Commit correctness-critical cleanup elsewhere (transactions, explicit
`with`-statement finalization, idempotent end-of-stream processing).

**Globally disabling:** `SET vgi_cancel_enabled=false;` skips the
cancel dispatcher entirely on both subprocess and HTTP. When disabled,
the dispatcher thread doesn't even start if the setting was false for
the life of the process. `on_cancel` is never invoked; workers learn a
stream is gone only via normal stream-close / HTTP state-token TTL.

## Using DuckDB Settings

Functions can declare required settings via `Meta.required_settings` and
access them via `self.settings` or `self.get_setting()`. Settings are available
during the bind phase, allowing output schema to depend on setting values.

Client passes settings when invoking a method:

```python
with Client("vgi-fixture-worker") as client:
    for batch in client.table_function(
        function_name="settings_aware",
        arguments=Arguments(positional=(pa.scalar(10),)),
        settings={"vgi_verbose_mode": "true"},
    ):
        process(batch)
```

## Quick Reference

### Imports

### Parallel Execution and Worker State

FIXME: complete this.

### Method Override Summary

FIXME: complete this.

### Pattern Decision Tree

```
How will your function be used in SQL?

1. SELECT my_func(col1, col2) FROM table
   → SCALAR FUNCTION: Returns one value per input row
   → Use ScalarFunction with Param/ConstParam/Returns on compute()
   → Example: upper(), abs(), concat()

2. SELECT * FROM my_func(args)
   → TABLE FUNCTION: Generates rows from arguments (no input table)
   → Use TableFunctionGenerator, override process()
   → Example: range(), read_csv(), glob()

3. SELECT * FROM my_func(args, (SELECT * FROM input_table))
   → TABLE-IN-OUT FUNCTION: Transforms input rows to output rows
   → Use TableInOutFunction, override transform() and optionally finish()
   → Example: filtering, enrichment, aggregation
```

## Declarative Catalogs

VGI supports declarative catalog definitions using `Catalog`, `Schema`, `Table`, and `View` classes.
This allows workers to expose structured metadata (tables, views, functions) to DuckDB via `ATTACH`.

### Function-Backed Tables (Recommended)

The recommended pattern is to back tables with `TableFunctionGenerator` functions.
The table schema is automatically derived from the function's `output_schema`:

## Additional Documentation

- **Access log**: See `vgi-rpc` docs site
- **Aggregate functions**: `docs/aggregate-functions.md`
- **Function metadata**: `docs/metadata.md`
- **Function lifecycle**: `docs/lifecycle.md`
- **Generator API (advanced)**: `docs/generator-api.md`
- **Catalog interface**: `docs/catalog-interface.md`
- **Shared storage backends**: `docs/shared-storage.md`