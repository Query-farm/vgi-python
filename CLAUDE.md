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

## Integration Testing

Integration tests live in the `vgi` C++ repo (sibling directory) at `test/sql/integration/`.
They run DuckDB's sqllogictest framework against a real VGI worker. Run from the `vgi` directory.

**Important:** When adding or modifying integration tests, or adding/changing functions registered
in the example worker, you **must** run the DuckDB integration tests (subprocess transport) before
considering the work complete. Always run the **full** test suite (`test/sql/integration/*`) to
ensure all tests pass — not just the new/changed test file. Adding a function can break
`function_registration.test` counts, for example.

**Subprocess transport** (worker spawned as a child process):
```bash
cd ~/Development/vgi
VGI_TEST_WORKER="uv run --project ~/Development/vgi-python vgi-example-worker" \
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
VGI_TEST_WORKER="uv run --project ~/Development/vgi-python vgi-example-worker" \
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
exec uv run coverage run --parallel-mode -m vgi.examples.worker "$@"
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
vgi-example-http --demo-storage
vgi-example-http --demo-storage --externalize-threshold-bytes 4096 --externalize-compression zstd
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
vgi-example-worker                                                    # Run example worker
vgi-client --input data.parquet --function echo --worker vgi-example-worker
vgi-client --input data.parquet --function sum_all_columns --worker vgi-example-worker
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
VGI_WORKER_DEBUG=1 vgi-example-worker
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
VGI_FILTER_DEBUG=1 vgi-example-worker
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

### Type Inference Rules

| Annotation | Inferred Type | Notes |
|------------|---------------|-------|
| `pa.Int64Array` | `pa.int64()` | All integer array types supported |
| `pa.StringArray` | `pa.string()` | Also `pa.LargeStringArray` → `pa.large_string()` |
| `pa.DoubleArray` | `pa.float64()` | `pa.FloatArray` → `pa.float32()` |
| `pa.BooleanArray` | `pa.bool_()` | |
| `pa.Date32Array` | `pa.date32()` | `pa.Date64Array` → `pa.date64()` |
| `pa.BinaryArray` | `pa.binary()` | |
| `pa.Array` | AnyArrow | Dynamic type, requires `bind()` |
| `pa.StructArray` | Error | Must specify `arrow_type=...` |
| `pa.ListArray` | Error | Must specify `arrow_type=...` |
| `pa.TimestampArray` | Error | Must specify `arrow_type=...` (needs unit) |

**Explicit types always override inference:**
```python test="skip"
# Override inference with explicit arrow_type
column: Annotated[pa.Int64Array, Param(arrow_type=pa.int32(), doc="...")]
```

### Key Constraints for Scalar Functions:
- **1:1 row mapping**: Output must have exactly the same number of rows as input
- **Single column output**: Output schema has exactly one column named "result"
- **Type validation**: Input/output types are validated at runtime (TypeMismatchError on mismatch)

## Specialized Pattern Classes

For common use cases, VGI provides specialized base classes that handle boilerplate:

## Using DuckDB Settings

Functions can declare required settings via `Meta.required_settings` and
access them via `self.settings` or `self.get_setting()`. Settings are available
during the bind phase, allowing output schema to depend on setting values.

Client passes settings when invoking a method:

```python
with Client("vgi-example-worker") as client:
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