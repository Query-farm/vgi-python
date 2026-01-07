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

```
┌─────────────────────────────────────────────────────────────────────┐
│                           DuckDB / Client                           │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │ Client spawns worker subprocess, sends Invocation,            │  │
│  │ streams input batches (if any), receives output via Arrow IPC │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │ stdin/stdout                         │
│                              ▼                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                      Worker Process                           │  │
│  │  SCALAR FUNCTION (ScalarFunction)                             │  │
│  │  - compute(batch): Transform each row to single output column │  │
│  │                           OR                                  │  │
│  │  TABLE FUNCTION (TableFunctionGenerator)                      │  │
│  │  - process(): Generator yielding output batches (no input)    │  │
│  │                           OR                                  │  │
│  │  TABLE-IN-OUT FUNCTION (TableInOutFunction)                   │  │
│  │  - transform(batch): Process each input batch                 │  │
│  │  - finish(): Emit final results after all input               │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

| Type | Base Class | Input | Use Case |
|------|------------|-------|----------|
| **Scalar Function** | `ScalarFunction` | Batches | Per-row transforms (1:1 row mapping, single output column) |
| **Table Function** | `TableFunctionGenerator` | None | Generate data (sequences, ranges) |
| **Table-In-Out Function** | `TableInOutFunction` | Batches | Transform, filter, aggregate |

### Key Components

- **Worker** (`vgi/worker.py`): Subprocess that hosts functions
- **Client** (`vgi/client/client.py`): Spawns workers, streams data
- **ScalarFunction** (`vgi/scalar_function.py`): Base for scalar functions
- **TableFunctionGenerator** (`vgi/table_function.py`): Base for table functions
- **TableInOutFunction** (`vgi/table_in_out_function.py`): Base for table-in-out functions

## Project Structure

```
vgi/
  __init__.py                      # Package exports
  function.py                      # Function base class, OutputSpec, FunctionInitInput
  scalar_function.py               # ScalarFunction, ScalarFunctionGenerator
  table_function.py                # TableFunctionGenerator, TableCardinality, Output
  table_in_out_function.py         # TableInOutFunction, TableInOutGenerator
  table_in_out_function_patterns.py # AggregationFunction, FilterFunction, MapFunction
  metadata.py                      # Function metadata for introspection
  schema_utils.py                  # Schema builder helpers (schema, schema_like)
  arguments.py                     # Arg descriptor, Arguments, AnyArrow, TableInput
  invocation.py                    # Invocation structure
  worker.py                        # Worker base class
  testing.py                       # TableInOutFunctionTestClient for in-process testing
  client/
    client.py                      # Client class
    cli.py                         # CLI command-line interface
  catalog/
    catalog_interface.py           # CatalogInterface for DuckDB integration
    storage.py                     # Catalog storage implementation
  examples/
    scalar.py                      # Example scalar functions
    table.py                       # Example table functions
    table_in_out.py                # Example table-in-out functions
    worker.py                      # ExampleWorker with registry
```

## CLI Commands

```bash
vgi-example-worker                                                    # Run example worker
vgi-client --input data.parquet --function echo --server vgi-example-worker
vgi-client --input data.parquet --function sum_all_columns --server vgi-example-worker
```

## Creating a Scalar Function (Per-Row Transform)

```python
import pyarrow as pa
import pyarrow.compute as pc
from vgi import ScalarFunction, Arg
from vgi.arguments import AnyArrow

class AddColumns(ScalarFunction):
    """Add two integer columns together."""

    class Meta:
        output_type = pa.int64()

    left = Arg[AnyArrow](0, type_bound=pa.types.is_integer, doc="First column")
    right = Arg[AnyArrow](1, type_bound=pa.types.is_integer, doc="Second column")

    def compute(self, batch: pa.RecordBatch) -> pa.Array:
        return pc.add(batch.column(self.left.value), batch.column(self.right.value))
```

### Key Constraints for Scalar Functions:
- **1:1 row mapping**: Output must have exactly the same number of rows as input
- **Single column output**: Output schema has exactly one column named "result"
- **No finalize phase**: All processing happens in compute()

## Creating a Table-In-Out Function (Recommended)

```python
import pyarrow as pa
from vgi import TableInOutFunction, Arg

class MyFunction(TableInOutFunction):
    """Transform each batch by doubling numeric values."""

    # Declare arguments as class attributes
    multiplier = Arg[int](0, default=2)  # positional with default

    @property
    def output_schema(self) -> pa.Schema:
        return self.input_schema  # Or custom schema

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        # Called for each input batch
        return batch

    def finish(self) -> list[pa.RecordBatch]:
        # Called after all input (optional)
        return []
```

### Aggregation Example

```python
class SumFunction(TableInOutFunction):
    """Sum all values, emit single result."""

    class Meta:
        max_workers = 1  # Aggregations must be single-process

    def bind(self) -> None:
        """Initialize accumulator after schema is available."""
        self.total = 0

    @property
    def output_schema(self) -> pa.Schema:
        return pa.schema([("sum", pa.int64())])

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        self.total += pc.sum(batch.column(0)).as_py()
        return self.empty_output_batch

    def finish(self) -> list[pa.RecordBatch]:
        return [pa.RecordBatch.from_pydict({"sum": [self.total]}, schema=self.output_schema)]
```

## Specialized Pattern Classes

For common use cases, VGI provides specialized base classes that handle boilerplate:

### AggregationFunction (Reduce Pattern)

Use when reducing all input to a summary (sum, count, mean):

```python
import pyarrow as pa
import pyarrow.compute as pc
from vgi import AggregationFunction

class SumColumns(AggregationFunction):
    """Sum all numeric columns."""

    @property
    def output_schema(self) -> pa.Schema:
        return pa.schema([("total", pa.int64())])

    @property
    def state_schema(self) -> pa.Schema:
        return self.output_schema  # Same for this simple case

    def accumulate(self, batch: pa.RecordBatch) -> None:
        self._total = getattr(self, '_total', 0) + pc.sum(batch.column(0)).as_py()

    def get_accumulated_state(self) -> pa.RecordBatch:
        return pa.RecordBatch.from_pydict({"total": [self._total]}, schema=self.state_schema)

    def merge_accumulated_states(self, states: pa.Table) -> None:
        self._total = pc.sum(states.column("total")).as_py()

    def compute_result(self) -> pa.RecordBatch:
        return pa.RecordBatch.from_pydict({"total": [self._total]}, schema=self.output_schema)
```

### FilterFunction (Row Filtering)

Use when filtering rows by a boolean predicate:

```python
import pyarrow as pa
import pyarrow.compute as pc
from vgi import FilterFunction, Arg

class PositiveFilter(FilterFunction):
    """Keep only rows where value is positive."""

    column = Arg[str](0, doc="Column to filter on")

    def predicate(self, batch: pa.RecordBatch) -> pa.Array:
        return pc.greater(batch.column(self.column), 0)
```

### MapFunction (Column Transform)

Use when transforming columns independently per row:

```python
import pyarrow as pa
import pyarrow.compute as pc
from vgi import MapFunction

class DoubleValues(MapFunction):
    """Double all values in a column."""

    def map_columns(self, batch: pa.RecordBatch) -> dict[str, pa.Array]:
        return {"value": pc.multiply(batch.column("value"), 2)}
```

## Creating a Table Function (No Input)

```python
import pyarrow as pa
from vgi import TableFunctionGenerator, Output, Arg

class SequenceFunction(TableFunctionGenerator):
    """Generate a sequence of integers."""

    class Meta:
        max_workers = 1

    count = Arg[int](0, doc="Number of integers")

    @property
    def output_schema(self) -> pa.Schema:
        return pa.schema([("n", pa.int64())])

    def process(self):
        for start in range(0, self.count, 1000):
            end = min(start + 1000, self.count)
            yield Output(pa.RecordBatch.from_pydict(
                {"n": list(range(start, end))}, schema=self.output_schema
            ))
```

## Using DuckDB Settings

Functions can declare required settings via `Meta.required_settings` and
access them via `self.settings` or `self.get_setting()`. Settings are available
during the bind phase, allowing output schema to depend on setting values.

```python
class SettingsAwareFunction(TableFunctionGenerator):
    """Function that uses settings to determine output."""

    class Meta:
        required_settings = ["vgi_verbose_mode"]  # Declare required settings
        max_workers = 1

    count = Arg[int](0, doc="Number of rows")

    @property
    def output_schema(self) -> pa.Schema:
        # Settings available during bind - can influence output schema
        fields = [pa.field("id", pa.int64())]

        if self.get_setting("vgi_verbose_mode") == "true":
            fields.append(pa.field("details", pa.string()))

        return pa.schema(fields)

    def process(self):
        verbose = self.get_setting("vgi_verbose_mode") == "true"
        for i in range(self.count):
            data = {"id": [i]}
            if verbose:
                data["details"] = [f"row_{i}"]
            yield Output(pa.RecordBatch.from_pydict(data, schema=self.output_schema))
```

Client passes settings when invoking:

```python
with Client("vgi-example-worker") as client:
    for batch in client.table_function(
        function_name="settings_aware",
        arguments=Arguments(positional=(pa.scalar(10),)),
        settings={"vgi_verbose_mode": "true"},
    ):
        process(batch)
```

## Creating a Worker

```python
from vgi.worker import Worker

class MyWorker(Worker):
    functions = [MyFunction, AnotherFunction]

if __name__ == "__main__":
    MyWorker().run()
```

## Quick Reference

### Imports

```python
# Scalar Functions (per-row transform)
from vgi import ScalarFunction, Arg, Worker

# Table Functions (no input)
from vgi import TableFunctionGenerator, Output, Arg, Worker

# Table-In-Out Functions (transform input)
from vgi import TableInOutFunction, Arg, Worker

# Specialized Pattern Classes
from vgi import AggregationFunction, FilterFunction, MapFunction

# Schema helpers
from vgi import schema, schema_like

# Testing
from vgi import TableInOutFunctionTestClient

# Logging
from vgi.log import Level
```

### Argument Declaration

```python
class MyFunction(TableInOutFunction):
    count = Arg[int](0)                        # Required positional
    multiplier = Arg[int](1, default=1)        # Optional positional
    target = Arg[str]("target")                # Required named
    format = Arg[str]("format", default="json") # Optional named
```

### Schema Helpers

```python
from vgi import schema, schema_like

# Build from scratch
output_schema = schema(sum=pa.int64(), count=pa.int64())

# Derive from input
output_schema = schema_like(self.input_schema, add={"total": pa.int64()})
output_schema = schema_like(self.input_schema, remove=["temp"])
output_schema = schema_like(self.input_schema, rename={"old": "new"})
```

### Using bind() for Schema Processing

The `bind()` method is called automatically after initialization. Use it to:
- Compute dynamic output types from input schema
- Initialize instance state that depends on schema
- Perform additional validation

At bind time:
- `self.input_schema` is available (for functions that receive input)
- All `Arg` values are resolved and accessible
- Type bounds have been validated

```python
class AddColumns(ScalarFunction):
    """Add two numeric columns with dynamic output type."""

    class Meta:
        output_type = AnyArrow  # Output type depends on input columns

    left = Arg[AnyArrow](0, type_bound=[pa.types.is_integer, pa.types.is_floating])
    right = Arg[AnyArrow](1, type_bound=[pa.types.is_integer, pa.types.is_floating])

    def bind(self) -> None:
        """Compute output type from input columns."""
        self._output_type = self.input_schema.field(self.left.value).type

    @property
    def output_type(self) -> pa.DataType:
        return self._output_type

    def compute(self, batch: pa.RecordBatch) -> pa.Array:
        return pc.add(batch.column(self.left.value), batch.column(self.right.value))
```

### Parallel Execution and bind() State

When `max_workers > 1`, each worker runs in a **separate process**. The `bind()` method is called independently on each worker, so **state set in bind() is NOT shared** across workers.

| State Type | Example | Safe with max_workers > 1? |
|------------|---------|---------------------------|
| Computed from schema | `self._output_type = ...` | Yes (deterministic, same on all workers) |
| Accumulators | `self.total = 0` | No - use `max_workers = 1` |
| Mutable collections | `self.buffer = []` | No - use `max_workers = 1` |

For aggregations that need to accumulate state across batches, **always set `max_workers = 1`** in Meta:

```python
class SumFunction(TableInOutFunction):
    class Meta:
        max_workers = 1  # Required for aggregations

    def bind(self) -> None:
        self.total = 0  # Safe because max_workers = 1
```

For advanced distributed aggregations with `max_workers > 1`, use `store_state()`/`collect_states()` to coordinate state across workers (see `docs/generator-api.md`).

### Method Override Summary

**ScalarFunction:**

| Method/Attribute | When to Override | Default |
|------------------|------------------|---------|
| `Meta.output_type` | Always required (pa.DataType or AnyArrow) | Required |
| `bind()` | Process input schema, compute dynamic output type | No-op |
| `output_type` | Override if Meta.output_type is AnyArrow | Uses Meta.output_type |
| `compute(batch)` | Transform batch to single array | Required |
| `setup()` | Acquire resources | No-op |
| `teardown()` | Release resources | No-op |

**TableInOutFunction:**

| Method | When to Override | Default |
|--------|------------------|---------|
| `bind()` | Process input schema, initialize state | No-op |
| `output_schema` | Change output columns | Returns input_schema |
| `transform(batch)` | Per-batch processing | Returns batch unchanged |
| `finish()` | Final output after all input | Returns empty list |
| `setup()` | Acquire resources | No-op |
| `teardown()` | Release resources | No-op |

**All Functions (Common):**

| Property/Method | Description |
|-----------------|-------------|
| `bind()` | Called after init; use for schema processing |
| `settings` | Dict of settings passed to function |
| `get_setting(name, default)` | Get specific setting value |

### Pattern Decision Tree

```
How will your function be used in SQL?

1. SELECT my_func(col1, col2) FROM table
   → SCALAR FUNCTION: Returns one value per input row
   → Use ScalarFunction, define Meta.output_type and compute()
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

## Additional Documentation

- **Protocol details**: `docs/protocol.md`
- **Function metadata**: `docs/metadata.md`
- **Function lifecycle**: `docs/lifecycle.md`
- **Generator API (advanced)**: `docs/generator-api.md`
