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

- **Function metadata**: `docs/metadata.md`
- **Function lifecycle**: `docs/lifecycle.md`
- **Generator API (advanced)**: `docs/generator-api.md`
- **Catalog interface**: `docs/catalog-interface.md`