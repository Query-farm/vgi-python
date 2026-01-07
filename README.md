# VGI (Vector Gateway Interface)

**Apache Arrow-based protocol for connecting DuckDB to external programs**

VGI enables user-defined functions to run in separate processes, communicating via stdin/stdout using Arrow IPC streaming. DuckDB attaches to VGI workers as external catalogs, letting you call Python functions directly from SQL.

## Why VGI?

The goal of VGI is to create a generic Arrow IPC-based interface so that extensions for DuckDB—and other Arrow-compatible databases—can be built in a way that scales, is easy to distribute, and can be easily created by LLMs.

**How VGI achieves this:**

- **Generic Arrow IPC protocol**: Functions communicate via Arrow IPC over stdin/stdout. Any language that can read/write Arrow can implement workers, and any database that speaks Arrow can be a client.

- **Scalable by design**: Workers run as separate processes with configurable parallelism (`max_workers`). Data streams batch-by-batch without loading entire datasets into memory.

- **Easy to distribute**: Workers are standalone executables. No compilation, no database-specific plugin APIs, no version coupling. Ship a Python script or a binary—if it speaks the protocol, it works.

- **LLM-friendly**: Functions are simple Python classes with declarative arguments (`Arg[int](0)`), typed schemas, and minimal boilerplate. The patterns are consistent and well-documented, making them straightforward for LLMs to generate.

## Features

- **DuckDB integration**: Attach workers as catalogs and call functions from SQL
- **Process isolation**: Functions run in separate worker processes for safety and resource management
- **Streaming data**: Process large datasets batch-by-batch without loading everything into memory
- **Parallel execution**: Distribute work across multiple workers for CPU-bound tasks
- **Type-safe**: Full Arrow type system with schema validation
- **Three function types**: Scalar (1:1 row transforms), Table (generators), and Table-In-Out (transformations)

## Installation

```bash
pip install vgi
```

Or with [uv](https://github.com/astral-sh/uv):

```bash
uv add vgi
```

## How It Works

VGI has two components:

- **Worker**: A Python process that hosts your functions. You define functions by subclassing `ScalarFunction`, `TableFunctionGenerator`, or `TableInOutFunction`, then register them in a `Worker`.

- **Client**: Spawns worker processes and invokes functions. DuckDB is the primary client (via `ATTACH`), but VGI also provides a Python client and CLI client for testing and development.

The wire protocol varies by function type—scalar functions stream input batches and return single-column results, table functions generate output without input, and table-in-out functions support both streaming transforms and finalization for aggregations.

Beyond functions, VGI includes a **Catalog Interface** that exposes database-like metadata (schemas, tables, views, functions) to DuckDB. This enables `ATTACH` to discover available functions and, for more advanced use cases, expose virtual tables backed by external data sources. See [Catalog Interface](docs/catalog-interface.md) for details.

## Quick Start

### 1. Create a Worker with Functions

```python
#!/usr/bin/env python
# my_worker.py
import pyarrow as pa
import pyarrow.compute as pc
from vgi import ScalarFunction, Arg, Worker


class DoubleColumn(ScalarFunction):
    """Double values in a numeric column."""

    class Meta:
        output_type = pa.int64()

    column = Arg[str](0, doc="Column to double")

    def compute(self, batch: pa.RecordBatch) -> pa.Array:
        return pc.multiply(batch.column(self.column), 2)


class UpperCase(ScalarFunction):
    """Convert string column to uppercase."""

    class Meta:
        output_type = pa.string()

    column = Arg[str](0, doc="Column to uppercase")

    def compute(self, batch: pa.RecordBatch) -> pa.Array:
        return pc.utf8_upper(batch.column(self.column))


class MyWorker(Worker):
    catalog_name = "my_funcs"
    functions = [DoubleColumn, UpperCase]


if __name__ == "__main__":
    MyWorker().run()
```

### 2. Use from DuckDB

```sql
-- Attach the VGI worker as a catalog
ATTACH 'my_funcs' (TYPE 'vgi', LOCATION './my_worker.py');

-- Call scalar functions from SQL
SELECT double_column(price) FROM products;
SELECT upper_case(name) FROM users;

-- Use in complex queries
SELECT
    id,
    double_column(quantity) as doubled_qty,
    upper_case(status) as status_upper
FROM orders
WHERE quantity > 10;
```

### Alternative: Python Client

For testing or Python-only workflows:

```python
from vgi.client import Client
from vgi import Arguments
import pyarrow as pa

batch = pa.RecordBatch.from_pydict({"value": [1, 2, 3, 4, 5]})

with Client("./my_worker.py") as client:
    for output in client.scalar_function(
        function_name="double_column",
        input=iter([batch]),
        arguments=Arguments(positional=[pa.scalar("value")]),
    ):
        print(output.to_pydict())
# Output: {'result': [2, 4, 6, 8, 10]}
```

## Function Types

| Type | Base Class | SQL Pattern | Use Case |
|------|------------|-------------|----------|
| **Scalar** | `ScalarFunction` | `SELECT func(col) FROM t` | Per-row transforms (1:1) |
| **Table** | `TableFunctionGenerator` | `SELECT * FROM func(args)` | Data generation |
| **Table-In-Out** | `TableInOutFunction` | `SELECT * FROM func((SELECT ...))` | Aggregation, filtering |

### Scalar Function

Per-row transformations returning a single column:

```python
class UpperCase(ScalarFunction):
    class Meta:
        output_type = pa.string()

    column = Arg[str](0)

    def compute(self, batch: pa.RecordBatch) -> pa.Array:
        return pc.utf8_upper(batch.column(self.column))
```

```sql
SELECT upper_case(name) FROM users;
```

### Table Function

Generate data without input:

```python
class Sequence(TableFunctionGenerator):
    class Meta:
        max_workers = 1

    count = Arg[int](0)

    @property
    def output_schema(self) -> pa.Schema:
        return pa.schema([("n", pa.int64())])

    def process(self):
        for i in range(0, self.count, 1000):
            yield Output(pa.RecordBatch.from_pydict(
                {"n": list(range(i, min(i + 1000, self.count)))},
                schema=self.output_schema
            ))
```

```sql
SELECT * FROM sequence(1000);
```

### Table-In-Out Function

Transform or aggregate input data:

```python
class SumColumn(TableInOutFunction):
    class Meta:
        max_workers = 1  # Aggregations need single worker

    column = Arg[str](0)

    def bind(self) -> None:
        self.total = 0

    @property
    def output_schema(self) -> pa.Schema:
        return pa.schema([("sum", pa.int64())])

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        self.total += pc.sum(batch.column(self.column)).as_py()
        return self.empty_output_batch

    def finish(self) -> list[pa.RecordBatch]:
        return [pa.RecordBatch.from_pydict(
            {"sum": [self.total]}, schema=self.output_schema
        )]
```

```sql
SELECT * FROM sum_column('amount', (SELECT * FROM orders));
```

## Declaring Arguments

Use `Arg` descriptors to declare function parameters:

```python
class MyFunction(TableInOutFunction):
    # Positional arguments (by index)
    count = Arg[int](0)                       # Required, position 0
    multiplier = Arg[int](1, default=2)       # Optional with default

    # Named arguments (by name)
    column = Arg[str]("column")               # Required named
    format = Arg[str]("format", default="json")  # Optional named

    # With validation
    limit = Arg[int](2, ge=0, le=1000)        # Range validation

    # Table input (for table-in-out functions)
    data = Arg[TableInput](3, doc="Input data")
```

## Function Metadata

Configure function behavior with `Meta`:

```python
class MyFunction(TableInOutFunction):
    class Meta:
        name = "my_func"           # Override function name
        description = "Does stuff" # Documentation
        max_workers = 4            # Parallel worker limit
        categories = ["transform"] # Categorization
        required_settings = ["TimeZone"]  # Required DuckDB settings
```

## Parallel Execution

Functions can run across multiple workers when `max_workers > 1`:

```python
class ParallelTransform(TableInOutFunction):
    class Meta:
        max_workers = 8  # Up to 8 parallel workers

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        # Each worker processes different batches
        return batch
```

For aggregations, use `max_workers = 1` or implement state sharing (see [Generator API](docs/generator-api.md)).

## CLI

```bash
# Invoke a function via CLI client (starts worker automatically)
vgi-client --function echo --server vgi-example-worker --input data.parquet

# Use your own worker
vgi-client --function double_column --server ./my_worker.py --input data.parquet
```

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

## Protocol Overview

VGI uses Apache Arrow IPC streaming over stdin/stdout:

```
Client                              Worker
  │                                   │
  │──── Invocation ────────────────▶  │  Function name, arguments, input schema
  │◀─── OutputSpec ─────────────────  │  Output schema, max_workers
  │                                   │
  │──── Input Batch 1 ──────────────▶ │
  │◀─── Output Batch 1 ──────────────  │  transform(batch)
  │                                   │
  │──── Input Batch N ──────────────▶ │
  │◀─── Output Batch N ──────────────  │
  │                                   │
  │──── FINALIZE ───────────────────▶ │  Signal end of input
  │◀─── Final Output ────────────────  │  finish() results
  └───────────────────────────────────┘
```

## Documentation

- [Protocol Specification](docs/protocol.md) - Wire format details
- [Function Lifecycle](docs/lifecycle.md) - Setup, bind, process, teardown
- [Metadata API](docs/metadata.md) - Function introspection
- [Generator API](docs/generator-api.md) - Advanced streaming patterns
- [Catalog Interface](docs/catalog-interface.md) - DuckDB ATTACH integration

## Development

```bash
# Clone the repository
git clone https://github.com/your-org/vgi-python
cd vgi-python

# Install dev dependencies
uv sync --all-extras

# Run tests
uv run pytest -n auto

# Lint and format
uv run ruff check --fix . && uv run ruff format .

# Type check
uv run mypy vgi/

# Run with coverage
uv run coverage run -m pytest --no-cov -n auto
uv run coverage combine && uv run coverage report
```

## Requirements

- Python >= 3.12.4
- pyarrow
- click
- structlog
- platformdirs

## License

This code is currently restrictively licensed right now, you shouldn't use it.

Copyright 2025-2026 Query.Farm LLC.  All Rights Reserved.