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
│  │  - compute(**cols): Transform each row to single output column│  │
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
vgi-client --input data.parquet --function echo --worker vgi-example-worker
vgi-client --input data.parquet --function sum_all_columns --worker vgi-example-worker
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `VGI_IPC_DEBUG=1` | Enable Arrow IPC debug logging (see below) |
| `VGI_IPC_STATS=1` | Enable IPC stream statistics logging (see below) |
| `VGI_QUIET=1` | Suppress worker startup logging |

### IPC Stream Statistics

Enable `VGI_IPC_STATS=1` to log aggregate IPC stream statistics at the end of each worker invocation. This logs the total number of batches, rows, and PyArrow IPC message counts for both reader and writer streams.

```bash
VGI_IPC_STATS=1 vgi-client --input data.parquet --function echo --worker vgi-example-worker --worker-stderr
```

**Output format:**
```
ipc_stream_stats batches=2 input_rows=100 output_rows=100 reader_messages=3 reader_batches=2 writer_messages=3 writer_batches=2
```

**Fields logged:**
- `batches` - Number of data batches processed
- `input_rows` - Total input rows processed
- `output_rows` - Total output rows produced
- `reader_messages` - IPC messages read (from PyArrow stats)
- `reader_batches` - Record batches read (from PyArrow stats)
- `writer_messages` - IPC messages written (from PyArrow stats)
- `writer_batches` - Record batches written (from PyArrow stats)

### IPC Debug Logging

Enable `VGI_IPC_DEBUG=1` to trace Arrow record batches read and written during client-worker communication. Useful for debugging protocol issues and client integration.

```bash
VGI_IPC_DEBUG=1 vgi-example-worker
VGI_IPC_DEBUG=1 vgi-client --input data.parquet --function echo --worker vgi-example-worker
```

**Output format:**
```
ipc_write  num_rows=100 schema={'id': 'int64', 'value': 'string'} metadata=None nbytes=4096
ipc_read   context=bind_result num_rows=1 schema={'output_schema': 'binary', ...} metadata=None
ipc_write  num_rows=0 schema={'id': 'int64'} metadata={'type': 'FINALIZE'} nbytes=512
ipc_read   num_rows=1 schema={'sum': 'int64'} metadata={'vgi.status': 'FINISHED'} nbytes=256
```

**Fields logged:**
- `context` - Protocol phase (invocation, bind_result, init_result, data)
- `num_rows` - Row count in the batch
- `schema` - Column names and types as `{name: type}` dict
- `metadata` - Custom metadata dict (shows protocol state like `vgi.status`)
- `nbytes` - Serialized byte size

**Performance:** Zero overhead when disabled (just a boolean check).

## Creating a Scalar Function (Per-Row Transform)

Use `Annotated[T, Param(...)]`, `Annotated[T, ConstParam(...)]`, and `Annotated[T, Returns(...)]` on the `compute()` method. **Arrow types are inferred from array classes** for concise declarations:

```python
from typing import Annotated

import pyarrow as pa
import pyarrow.compute as pc
from vgi import ConstParam, Param, Returns, ScalarFunction

class AddColumns(ScalarFunction):
    """Add two integer columns together."""

    def compute(
        self,
        # Type inferred from pa.Int64Array -> pa.int64()
        left: Annotated[pa.Int64Array, Param(doc="First column")],
        right: Annotated[pa.Int64Array, Param(doc="Second column")],
    ) -> Annotated[pa.Int64Array, Returns()]:  # Output type also inferred
        return pc.add(left, right)
```

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

### With Constant Argument (ConstParam)

Use `ConstParam` for values known at planning time (not per-row arrays):

```python
class MultiplyByFactor(ScalarFunction):
    """Multiply column by constant factor."""

    def compute(
        self,
        column: Annotated[pa.Int64Array, Param(doc="Column to multiply")],
        factor: Annotated[int, ConstParam("Multiplication factor")],
    ) -> Annotated[pa.Int64Array, Returns()]:
        # factor is Python int (scalar), not pa.Array
        return pc.multiply(column, factor)
```

### With Dynamic Output Type (AnyArrow)

Use `pa.Array` (generic) with no `arrow_type` when output type depends on input schema:

```python
class Double(ScalarFunction):
    """Double values, preserving input type."""

    _output_type: pa.DataType

    def bind(self) -> None:
        self._output_type = self.input_schema.field(0).type

    @property
    def output_type(self) -> pa.DataType:
        return self._output_type

    def compute(
        self,
        column: Annotated[pa.Array, Param(doc="Numeric value")],  # AnyArrow
    ) -> Annotated[pa.Array, Returns()]:  # Dynamic output
        return pc.multiply(column, 2)
```

### With Complex Types (Explicit arrow_type Required)

Complex/parameterized types like `StructArray`, `ListArray`, `TimestampArray` require explicit `arrow_type`:

```python
class ExtractX(ScalarFunction):
    """Extract x field from point struct."""

    def compute(
        self,
        point: Annotated[
            pa.StructArray,
            Param(arrow_type=pa.struct([("x", pa.int64()), ("y", pa.int64())]), doc="Point")
        ],
    ) -> Annotated[pa.Int64Array, Returns()]:
        return pc.struct_field(point, "x")
```

### Key Constraints for Scalar Functions:
- **1:1 row mapping**: Output must have exactly the same number of rows as input
- **Single column output**: Output schema has exactly one column named "result"
- **Type validation**: Input/output types are validated at runtime (TypeMismatchError on mismatch)

## Creating a Polars Scalar Function

For scalar functions that use Polars, use `PolarsScalarFunction` which handles
zero-copy Arrow <-> Polars conversion automatically. The API uses an expression-based
approach where `compute_polars()` returns a `pl.Expr` and columns are referenced
by their declared parameter names.

```python
from typing import Annotated
import polars as pl
from vgi import PolarsScalarFunction, Param

class UpperCase(PolarsScalarFunction):
    """Convert string column to uppercase using Polars."""

    # Declare parameter with position and Polars type
    text: Annotated[pl.Utf8, Param(position=0, doc="String value to uppercase")]

    class Meta:
        output_type = pl.Utf8  # Polars type, not Arrow

    def compute_polars(self) -> pl.Expr:
        # Reference column by param name
        return pl.col("text").str.to_uppercase()
```

### Multiple Parameters

```python
class AddValues(PolarsScalarFunction):
    """Add two numeric values together."""

    left: Annotated[pl.Float64, Param(position=0, doc="First value")]
    right: Annotated[pl.Float64, Param(position=1, doc="Second value")]

    class Meta:
        output_type = pl.Float64

    def compute_polars(self) -> pl.Expr:
        return pl.col("left") + pl.col("right")
```

### With Constant Argument

Access constant arguments via `self.invocation.arguments.positional`:

```python
class Multiply(PolarsScalarFunction):
    """Multiply a column by a constant factor."""

    value: Annotated[pl.Float64, Param(position=0, doc="Value to multiply")]

    class Meta:
        output_type = pl.Float64

    @property
    def factor(self) -> float:
        # Constant argument at position 0 in Arguments
        return self.invocation.arguments.positional[0].as_py()

    def compute_polars(self) -> pl.Expr:
        return pl.col("value") * self.factor
```

### Dynamic Output Type with AnyPolars

Use `AnyPolars` when output type depends on input. Use `type_bound` to constrain
acceptable input types:

```python
from typing import Any, Annotated
import pyarrow.types as pat
from vgi import PolarsScalarFunction, Param, AnyPolars
import polars as pl

class Double(PolarsScalarFunction):
    """Double values, preserving input type."""

    # Any type with constraint: must be integer or floating point
    value: Annotated[
        Any,
        Param(
            position=0,
            doc="Numeric value to double",
            type_bound=[pat.is_integer, pat.is_floating],
        ),
    ]

    class Meta:
        output_type = AnyPolars

    @property
    def output_polars_type(self) -> pl.DataType:
        # Return input type to preserve it
        return self.polars_schema[self.input_schema.field(0).name]

    def compute_polars(self) -> pl.Expr:
        return pl.col("value") * 2
```

### Varargs (Variable Number of Arguments)

Use `varargs=True` to accept multiple columns. Columns are renamed to
`{name}_0`, `{name}_1`, etc. and can be matched with regex:

```python
class SumValues(PolarsScalarFunction):
    """Sum multiple numeric values."""

    values: Annotated[pl.Float64, Param(position=0, doc="Values to sum", varargs=True)]

    class Meta:
        output_type = pl.Float64

    def compute_polars(self) -> pl.Expr:
        # Use regex to match all vararg columns
        return pl.sum_horizontal(pl.col("^values_.*$"))
```

### Key Features of PolarsScalarFunction:
- **Expression-based**: `compute_polars()` returns `pl.Expr`, not `pl.Series`
- **Named column access**: Reference columns by param name with `pl.col("param_name")`
- **Position-based params**: Use `Param(position=N, ...)` to declare column positions
- **Type bounds**: Use `type_bound` to constrain dynamic types with pyarrow type predicates
- **Zero-copy**: Automatic Arrow <-> Polars conversion without data copying
- **Meta.output_type**: Use Polars types (`pl.Utf8`, `pl.Int64`) or `AnyPolars` for dynamic

## Creating a Table-In-Out Function (Recommended)

```python
from typing import Annotated
import pyarrow as pa
from vgi import TableInOutFunction, Arg

class MyFunction(TableInOutFunction):
    """Transform each batch by doubling numeric values."""

    # Declare arguments as class attributes
    multiplier: Annotated[int, Arg(0, default=2)]  # positional with default

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
from typing import Annotated
import pyarrow as pa
import pyarrow.compute as pc
from vgi import FilterFunction, Arg

class PositiveFilter(FilterFunction):
    """Keep only rows where value is positive."""

    column: Annotated[str, Arg(0, doc="Column to filter on")]

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
from typing import Annotated
import pyarrow as pa
from vgi import TableFunctionGenerator, Output, Arg

class SequenceFunction(TableFunctionGenerator):
    """Generate a sequence of integers."""

    class Meta:
        max_workers = 1

    count: Annotated[int, Arg(0, doc="Number of integers")]
    batch_size: Annotated[int, Arg(1, default=1000, doc="Batch size for output")]

    @property
    def output_schema(self) -> pa.Schema:
        return pa.schema([("n", pa.int64())])

    def process(self):
        for start in range(0, self.count, self.batch_size):
            end = min(start + self.batch_size, self.count)
            yield Output(pa.RecordBatch.from_pydict(
                {"n": list(range(start, end))}, schema=self.output_schema
            ))
```

## Using DuckDB Settings

Functions can declare required settings via `Meta.required_settings` and
access them via `self.settings` or `self.get_setting()`. Settings are available
during the bind phase, allowing output schema to depend on setting values.

```python
from typing import Annotated

class SettingsAwareFunction(TableFunctionGenerator):
    """Function that uses settings to determine output."""

    class Meta:
        required_settings = ["vgi_verbose_mode"]  # Declare required settings
        max_workers = 1

    count: Annotated[int, Arg(0, doc="Number of rows")]

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
# Scalar Functions (per-row transform) - Annotated[T, Param/Returns] API
from typing import Annotated
from vgi import ScalarFunction, Param, ConstParam, Returns, Worker

# Scalar Functions - legacy Arg API (still supported)
from vgi import ScalarFunction, Arg, AnyArrowValue, Worker

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

**For ScalarFunction (Recommended): Annotated[T, Param/ConstParam/Returns] on compute()**

```python
from typing import Annotated
from vgi import ScalarFunction, Param, ConstParam, Returns
import pyarrow as pa
import pyarrow.compute as pc

class MyScalar(ScalarFunction):
    def compute(
        self,
        # Concise: type inferred from pa.Int64Array
        col: Annotated[pa.Int64Array, Param(doc="Column input")],
        factor: Annotated[int, ConstParam("Constant factor")],
    ) -> Annotated[pa.Int64Array, Returns()]:
        return pc.multiply(col, factor)

# Alternative: explicit arrow_type (for complex types or override)
class MyExplicitScalar(ScalarFunction):
    def compute(
        self,
        col: Annotated[pa.Array, Param(pa.int64(), "Column input")],
        factor: Annotated[int, ConstParam("Constant factor")],
    ) -> Annotated[pa.Array, Returns(pa.int64())]:
        return pc.multiply(col, factor)
```

**For TableInOutFunction/TableFunctionGenerator: Annotated[T, Arg(...)]**

```python
from typing import Annotated
from vgi import Arg, AnyArrowValue, TableInOutFunction

class MyFunction(TableInOutFunction):
    count: Annotated[int, Arg(0)]                        # Required positional
    multiplier: Annotated[int, Arg(1, default=1)]        # Optional positional
    target: Annotated[str, Arg("target")]                # Required named
    format: Annotated[str, Arg("format", default="json")] # Optional named
    column: Annotated[AnyArrowValue, Arg(0, type_bound=pa.types.is_integer)]  # AnyArrow
```

**Alternative: Legacy Arg[T] pattern** (requires `# type: ignore`):

```python
class MyFunction(TableInOutFunction):
    count = Arg[int](0)                        # type: ignore[assignment]
    multiplier = Arg[int](1, default=1)        # type: ignore[assignment]
```

**Important**: When using `Annotated` inside functions/methods with `from __future__ import annotations`, ensure `Annotated` and `AnyArrowValue` are imported at module level (not locally inside the function).

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
from typing import Annotated
from vgi import ScalarFunction, Param, Returns
import pyarrow as pa
import pyarrow.compute as pc

class AddColumns(ScalarFunction):
    """Add two numeric columns with dynamic output type."""

    _output_type: pa.DataType

    def bind(self) -> None:
        """Compute output type from input columns."""
        self._output_type = self.input_schema.field(0).type

    @property
    def output_type(self) -> pa.DataType:
        return self._output_type

    def compute(
        self,
        left: Annotated[pa.Array, Param(doc="First column")],    # AnyArrow (pa.Array)
        right: Annotated[pa.Array, Param(doc="Second column")],  # AnyArrow (pa.Array)
    ) -> Annotated[pa.Array, Returns()]:  # Dynamic output type
        return pc.add(left, right)
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
| `compute(self, ...)` | Always - use Param/ConstParam annotations, Returns() for output | Required |
| `bind()` | Compute dynamic output type when using AnyArrow | No-op |
| `output_type` | Override when using Returns(AnyArrow) | Uses Returns() type |
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

## Additional Documentation

- **Protocol details**: `docs/protocol.md`
- **Function metadata**: `docs/metadata.md`
- **Function lifecycle**: `docs/lifecycle.md`
- **Generator API (advanced)**: `docs/generator-api.md`
