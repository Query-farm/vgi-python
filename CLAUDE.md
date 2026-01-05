# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

```bash
uv sync --all-extras      # Install dependencies
uv run pytest             # Run tests
uv run ruff check .       # Lint
uv run ruff format .      # Format
uv run mypy vgi/          # Type check

# Run tests with coverage (includes subprocess/worker coverage)
uv run coverage run -m pytest --no-cov
uv run coverage combine   # Merge subprocess coverage data
uv run coverage report    # Show coverage report
uv run coverage html      # Generate HTML report in htmlcov/
```

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
  __init__.py              # Package exports
  function.py              # Invocation, OutputSpec, Arguments, FunctionType
  scalar_function.py       # ScalarFunction, ScalarFunctionGenerator
  table_function.py        # TableFunctionGenerator, TableCardinality, Output
  table_in_out_function.py # TableInOutFunction, TableInOutGeneratorFunction
  metadata.py              # Function metadata for introspection
  schema_utils.py          # Schema builder helpers (schema, schema_like)
  worker.py                # Worker base class
  client/
    client.py              # Client class
  examples/
    scalar.py              # Example scalar functions
    table.py               # Example table functions
    table_in_out.py        # Example table-in-out functions
    worker.py              # ExampleWorker with registry
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

class DoubleColumn(ScalarFunction):
    """Double the value in a specified column."""

    column = Arg[str](0, doc="Column to double")

    @property
    def output_type(self) -> pa.DataType:
        # Output type matches input column type
        return self.input_schema.field(self.column).type

    def compute(self, batch: pa.RecordBatch) -> pa.Array:
        return pc.multiply(batch.column(self.column), 2)
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

    def __init__(self, invocation, logger):
        super().__init__(invocation, logger)
        self.total = 0

    class Meta:
        max_workers = 1  # Aggregations must be single-process

    @property
    def output_schema(self) -> pa.Schema:
        return pa.schema([("sum", pa.int64())])

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        self.total += pc.sum(batch.column(0)).as_py()
        return self.empty_output_batch

    def finish(self) -> list[pa.RecordBatch]:
        return [pa.RecordBatch.from_pydict({"sum": [self.total]}, schema=self.output_schema)]
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

# Schema helpers
from vgi import schema, schema_like

# Logging
from vgi.log import Level
```

### Argument Declaration

```python
class MyFunction(TableInOutFunction):
    count = Arg[int](0)                        # Required positional
    multiplier = Arg[int](1, default=1)        # Optional positional
    column = Arg[str]("column")                # Required named
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

### Method Override Summary

**ScalarFunction:**

| Method | When to Override | Default |
|--------|------------------|---------|
| `output_type` | Define output column type | Required |
| `compute(batch)` | Transform batch to single array | Required |
| `setup()` | Acquire resources | No-op |
| `teardown()` | Release resources | No-op |

**TableInOutFunction:**

| Method | When to Override | Default |
|--------|------------------|---------|
| `output_schema` | Change output columns | Returns input_schema |
| `transform(batch)` | Per-batch processing | Returns batch unchanged |
| `finish()` | Final output after all input | Returns empty list |
| `setup()` | Acquire resources | No-op |
| `teardown()` | Release resources | No-op |

**All Functions (Common):**

| Property/Method | Description |
|-----------------|-------------|
| `settings` | Dict of settings passed to function |
| `get_setting(name, default)` | Get specific setting value |

### Pattern Decision Tree

```
How will your function be used in SQL?

1. SELECT my_func(col1, col2) FROM table
   → SCALAR FUNCTION: Returns one value per input row
   → Use ScalarFunction, override output_type and compute()
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
