# VGI (Vector Gateway Interface)

**Apache Arrow-based protocol for connecting DuckDB to external programs**

VGI enables user-defined functions to run in separate processes, communicating via stdin/stdout using Arrow IPC streaming. Functions receive batches of data, process them, and return results - all with zero-copy efficiency through Apache Arrow.

## Features

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

### Development Installation

```bash
git clone https://github.com/your-org/vgi-python
cd vgi-python
uv sync --all-extras
```

## Quick Start

### 1. Create a Function

```python
# my_functions.py
import pyarrow as pa
import pyarrow.compute as pc
from vgi import ScalarFunction, Arg

class DoubleColumn(ScalarFunction):
    """Double values in a numeric column."""

    class Meta:
        output_type = pa.int64()  # Output type

    column = Arg[str](0, doc="Column to double")

    def compute(self, batch: pa.RecordBatch) -> pa.Array:
        return pc.multiply(batch.column(self.column), 2)
```

### 2. Create a Worker

```python
# worker.py
from vgi import Worker
from my_functions import DoubleColumn

class MyWorker(Worker):
    functions = [DoubleColumn]

if __name__ == "__main__":
    MyWorker().run()
```

### 3. Use the Client

```python
from vgi.client import Client
from vgi import Arguments
import pyarrow as pa

# Create input data
batch = pa.RecordBatch.from_pydict({"value": [1, 2, 3, 4, 5]})

with Client("python worker.py") as client:
    for output in client.scalar_function(
        function_name="double_column",
        input=iter([batch]),
        arguments=Arguments(positional=[pa.scalar("value")]),
    ):
        print(output.to_pydict())
# Output: {'result': [2, 4, 6, 8, 10]}
```

## Function Types

VGI provides three function types for different use cases:

| Type | Base Class | Input | Output | Use Case |
|------|------------|-------|--------|----------|
| **Scalar** | `ScalarFunction` | Batches | Single column (1:1 rows) | `upper()`, `abs()`, per-row transforms |
| **Table** | `TableFunctionGenerator` | None | Multi-column batches | `range()`, `read_csv()`, data generation |
| **Table-In-Out** | `TableInOutFunction` | Batches | Multi-column batches | Filtering, aggregation, enrichment |

### Scalar Function Example

Per-row transformations that return a single column:

```python
from vgi import ScalarFunction, Arg
import pyarrow as pa
import pyarrow.compute as pc

class UpperCase(ScalarFunction):
    """Convert string column to uppercase."""

    class Meta:
        output_type = pa.string()

    column = Arg[str](0, doc="Column to uppercase")

    def compute(self, batch: pa.RecordBatch) -> pa.Array:
        return pc.utf8_upper(batch.column(self.column))
```

### Table Function Example

Generate data without input:

```python
from vgi import TableFunctionGenerator, Output, Arg
import pyarrow as pa

class Sequence(TableFunctionGenerator):
    """Generate a sequence of integers."""

    class Meta:
        max_workers = 1

    count = Arg[int](0, doc="Number of integers")

    @property
    def output_schema(self) -> pa.Schema:
        return pa.schema([("n", pa.int64())])

    def process(self):
        for i in range(0, self.count, 1000):
            batch_end = min(i + 1000, self.count)
            yield Output(pa.RecordBatch.from_pydict(
                {"n": list(range(i, batch_end))},
                schema=self.output_schema
            ))
```

### Table-In-Out Function Example

Transform input data with optional aggregation:

```python
from vgi import TableInOutFunction, Arg, TableInput
import pyarrow as pa
import pyarrow.compute as pc

class SumColumn(TableInOutFunction):
    """Sum values in a column."""

    class Meta:
        max_workers = 1  # Aggregations need single worker

    data = Arg[TableInput](0, doc="Input table")
    column = Arg[str](1, doc="Column to sum")

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
            {"sum": [self.total]},
            schema=self.output_schema
        )]
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

## Schema Helpers

Build output schemas easily:

```python
from vgi import schema, schema_like
import pyarrow as pa

# Build from scratch
output = schema(id=pa.int64(), name=pa.string(), value=pa.float64())

# Derive from input schema
output = schema_like(self.input_schema, add={"total": pa.int64()})
output = schema_like(self.input_schema, remove=["temp_col"])
output = schema_like(self.input_schema, rename={"old_name": "new_name"})
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

For aggregations that need to combine results:

```python
from vgi.ipc_utils import RecordBatchState

class DistributedSum(TableInOutFunction):
    """Parallel aggregation with state sharing."""

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        self.partial_sum += pc.sum(batch.column(0)).as_py()
        return self.empty_output_batch

    def save_state(self) -> RecordBatchState:
        """Save partial state for collection."""
        return RecordBatchState(batch=pa.RecordBatch.from_pydict(
            {"sum": [self.partial_sum]},
            schema=self.output_schema
        ))

    def load_states(self, states: list[RecordBatchState]) -> None:
        """Merge states from all workers."""
        self.total = sum(s.batch.column(0)[0].as_py() for s in states)
```

## CLI Tools

```bash
# Run the example worker
vgi-example-worker

# Invoke a function via CLI
vgi-client --function echo --server vgi-example-worker --input data.parquet
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

## Development

```bash
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
