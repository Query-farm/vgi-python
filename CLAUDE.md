# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

```bash
uv sync --all-extras      # Install dependencies
uv run pytest             # Run tests
uv run ruff check .       # Lint
uv run ruff format .      # Format
uv run mypy vgi/          # Type check
```

**Before committing**, always run lint and format checks:
```bash
uv run ruff check --fix . && uv run ruff format . && uv run mypy vgi/
```

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
| **Table Function** | `TableFunctionGenerator` | None | Generate data (sequences, ranges) |
| **Table-In-Out Function** | `TableInOutFunction` | Batches | Transform, filter, aggregate |

### Key Components

- **Worker** (`vgi/worker.py`): Subprocess that hosts functions
- **Client** (`vgi/client/client.py`): Spawns workers, streams data
- **TableFunctionGenerator** (`vgi/table_function.py`): Base for table functions
- **TableInOutFunction** (`vgi/table_in_out_function.py`): Base for table-in-out functions

## Project Structure

```
vgi/
  __init__.py              # Package exports
  function.py              # Invocation, OutputSpec, Arguments, GlobalInitResult
  table_function.py        # TableFunctionGenerator, CardinalityInfo, Output
  table_in_out_function.py # TableInOutFunction, TableInOutGeneratorFunction
  metadata.py              # Function metadata for introspection
  schema_utils.py          # Schema builder helpers (schema, schema_like)
  worker.py                # Worker base class
  client/
    client.py              # Client class
  examples/
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

| Method | When to Override | Default |
|--------|------------------|---------|
| `output_schema` | Change output columns | Returns input_schema |
| `transform(batch)` | Per-batch processing | Returns batch unchanged |
| `finish()` | Final output after all input | Returns empty list |
| `setup()` | Acquire resources | No-op |
| `teardown()` | Release resources | No-op |

### Pattern Decision Tree

```
Need to implement a VGI function?
│
├─ Does the function receive input data?
│  │
│  ├─ NO → Use TableFunctionGenerator
│  │       Override process() to yield Output batches
│  │
│  └─ YES → Use TableInOutFunction
│           ├─ Transform each batch? → Override transform()
│           ├─ Aggregate results? → Accumulate in transform(), emit in finish()
│           └─ Need generator control? → See docs/generator-api.md
```

## Additional Documentation

- **Protocol details**: `docs/protocol.md`
- **Function metadata**: `docs/metadata.md`
- **Function lifecycle**: `docs/lifecycle.md`
- **Generator API (advanced)**: `docs/generator-api.md`
