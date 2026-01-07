# VGI (Vector Gateway Interface)

<p align="center">
  <img src="docs/vgi-logo.png" alt="VGI Logo" width="200">
</p>

<p align="center">
  <strong>Apache Arrow-based protocol for connecting DuckDB to external programs</strong>
</p>

<p align="center">
  Created by <a href="https://query.farm">Query.Farm</a>
</p>

---

## See It in Action

```python
# my_worker.py
from vgi import ScalarFunction, Arg, Worker
import pyarrow as pa
import pyarrow.compute as pc

class Greeting(ScalarFunction):
    """Generate a greeting for each name."""

    class Meta:
        output_type = pa.string()

    col_name = Arg[str](0, doc="Column containing names")

    def compute(self, batch: pa.RecordBatch) -> pa.Array:
        names = batch.column(self.col_name)
        return pc.binary_join_element_wise("Hello, ", names, "!")

class MyWorker(Worker):
    functions = [Greeting]

if __name__ == "__main__":
    MyWorker().run()
```

```sql
-- First time only.
INSTALL vgi FROM COMMUNITY;
LOAD vgi;
ATTACH 'my_worker' (TYPE 'vgi', LOCATION './my_worker.py');

SELECT greeting(name) FROM users;
-- "Hello, Alice!"
-- "Hello, Bob!"
```

Or you can launch the DuckDB CLI with

`duckdb vgi:my_worker.py` to start a new session with the functions you just added.

That's it. No C++ compilation, no extension versioning, no complex build process. Just a Python script that DuckDB can call.

---

## Installation

```bash
pip install vgi
```

Or with [uv](https://github.com/astral-sh/uv):

```bash
uv add vgi
```

---

## Why VGI?

VGI lets you extend DuckDB with Python functions that run in separate processes, communicating via Apache Arrow IPC. This means:

| Traditional Extensions | VGI Workers |
|----------------------|-------------|
| C/C++ compilation required | Any language but first Python and Typescript and Go |
| Tied to DuckDB version | Version independent |
| Complex build/release cycle | Ship a script or executable |
| Runs in-process | Process isolation |
| Single-threaded | Parallel workers |

**Use cases:**
- Call REST APIs or external services from SQL
- Run ML inference (PyTorch, scikit-learn, etc.)
- Process data with Python libraries (pandas, numpy)
- Build custom ETL transforms
- Create domain-specific functions for your team
- Expose external data sources as queryable tables and views

---

## Quick Start

### Step 1: Create a Worker

A worker is a Python script that defines one or more functions:

```python
#!/usr/bin/env python
# my_worker.py
import pyarrow as pa
import pyarrow.compute as pc
from vgi import ScalarFunction, Arg, Worker


class UpperCase(ScalarFunction):
    """Convert a string column to uppercase."""

    class Meta:
        output_type = pa.string()

    col_name = Arg[str](0, doc="Column to uppercase")

    def compute(self, batch: pa.RecordBatch) -> pa.Array:
        return pc.utf8_upper(batch.column(self.col_name))


class MyWorker(Worker):
    catalog_name = "my_funcs"
    functions = [UpperCase]


if __name__ == "__main__":
    MyWorker().run()
```

### Step 2: Use from DuckDB

```sql
-- Attach the worker as a catalog
ATTACH 'my_funcs' (TYPE 'vgi', LOCATION './my_worker.py');

-- Call your function
SELECT upper_case(name) FROM users;

-- Use in complex queries
SELECT id, upper_case(status) as status
FROM orders
WHERE created_at > '2024-01-01';
```

### Step 3: There is no step 3

Your function is now available in DuckDB. Ship the Python script to your team, and they can use it immediately.

---

## Going Further: Type-Safe Arguments

For production use, you'll want type validation. Use `Arg[AnyArrow]` with `type_bound` to ensure columns have the correct type:

```python
from vgi import ScalarFunction, Arg, Worker
from vgi.arguments import AnyArrow
import pyarrow as pa
import pyarrow.compute as pc


class AddColumns(ScalarFunction):
    """Add two integer columns together."""

    class Meta:
        output_type = pa.int64()

    left = Arg[AnyArrow](0, type_bound=pa.types.is_integer, doc="First column")
    right = Arg[AnyArrow](1, type_bound=pa.types.is_integer, doc="Second column")

    def compute(self, batch: pa.RecordBatch) -> pa.Array:
        return pc.add(
            batch.column(self.left.value),
            batch.column(self.right.value)
        )
```

```sql
SELECT add_columns(price, tax) as total FROM orders;

-- This would fail at bind time with a clear error:
-- SELECT add_columns(name, price) FROM orders;
-- Error: Column 'name' has type string, expected integer
```

Key differences from `Arg[str]`:
- `Arg[AnyArrow]` validates the column's Arrow type at bind time
- `type_bound` specifies which types are allowed
- Access the column name via `.value` (e.g., `self.left.value`)

---

## Function Types

VGI supports three function types:

| Type | Base Class | SQL Pattern | Use Case |
|------|------------|-------------|----------|
| **Scalar** | `ScalarFunction` | `SELECT func(col) FROM t` | Per-row transforms (1:1) |
| **Table** | `TableFunctionGenerator` | `SELECT * FROM func(args)` | Generate data |
| **Table-In-Out** | `TableInOutFunction` | `SELECT * FROM func((SELECT ...))` | Aggregation, filtering |

### Scalar Functions

Transform each row independently. Output has the same number of rows as input.

```python
class Double(ScalarFunction):
    class Meta:
        output_type = pa.int64()

    col_name = Arg[str](0)

    def compute(self, batch: pa.RecordBatch) -> pa.Array:
        return pc.multiply(batch.column(self.col_name), 2)
```

### Table Functions

Generate data without input. Useful for sequences, reading external sources, etc.

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
            batch = pa.RecordBatch.from_pydict(
                {"n": list(range(i, min(i + 1000, self.count)))},
                schema=self.output_schema
            )
            yield Output(batch)
```

```sql
SELECT * FROM sequence(10000);
```

### Table-In-Out Functions

Transform or aggregate input data. Supports streaming transforms and final aggregation.

```python
class Sum(TableInOutFunction):
    class Meta:
        max_workers = 1  # Aggregations need single worker

    col_name = Arg[str](0)

    def bind(self) -> None:
        self.total = 0

    @property
    def output_schema(self) -> pa.Schema:
        return pa.schema([("sum", pa.int64())])

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        self.total += pc.sum(batch.column(self.col_name)).as_py()
        return self.empty_output_batch

    def finish(self) -> list[pa.RecordBatch]:
        return [pa.RecordBatch.from_pydict(
            {"sum": [self.total]}, schema=self.output_schema
        )]
```

```sql
SELECT * FROM sum('amount', (SELECT * FROM orders));
```

---

## Beyond Functions: Full Catalog Support

VGI workers can expose more than just functions. A worker can provide a complete database catalog with:

- **Schemas** - Organize objects into namespaces
- **Tables** - Expose external data as queryable tables
- **Views** - Define SQL views over your data
- **Functions** - Scalar, table, and table-in-out functions

```sql
ATTACH 'external_db' (TYPE 'vgi', LOCATION './my_catalog_worker.py');

-- Query tables from the attached catalog
SELECT * FROM external_db.main.users;

-- Use views
SELECT * FROM external_db.analytics.daily_summary;

-- Call functions
SELECT external_db.main.transform(col) FROM my_table;
```

This enables VGI workers to act as bridges to external systems—databases, APIs, file systems—presenting them as native DuckDB catalogs.

See [Catalog Interface](docs/catalog-interface.md) for implementation details.

---

## Parallel Execution

Functions can run across multiple worker processes:

```python
class ParallelTransform(TableInOutFunction):
    class Meta:
        max_workers = 8  # Up to 8 parallel workers

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        # Each worker processes different batches
        return expensive_computation(batch)
```

For aggregations that accumulate state, use `max_workers = 1`.

See [Generator API](docs/generator-api.md) for advanced patterns like distributed aggregation.

---

## Error Handling

Errors in your functions propagate to DuckDB with clear messages:

```python
def compute(self, batch: pa.RecordBatch) -> pa.Array:
    if batch.num_rows == 0:
        raise ValueError("Empty batch not supported")
    # ...
```

```sql
SELECT my_func(col) FROM empty_table;
-- Error: Empty batch not supported
```

Type bound violations are caught at bind time (before processing starts):

```sql
SELECT add_columns(name, price) FROM orders;
-- Error: Argument 'left': Column 'name' has type string,
--        but type bound requires: is_integer
```

---

## Testing Your Functions

Test functions directly in Python without DuckDB:

```python
import pyarrow as pa
from my_worker import UpperCase

# Create test input
batch = pa.RecordBatch.from_pydict({"name": ["alice", "bob"]})

# Create function instance (normally done by the worker)
from vgi import Invocation, Arguments
invocation = Invocation(
    function_name="upper_case",
    arguments=Arguments(positional=[pa.scalar("name")]),
    input_schema=batch.schema,
)

import structlog
func = UpperCase(invocation=invocation, logger=structlog.get_logger())

# Call compute
result = func.compute(batch)
assert result.to_pylist() == ["ALICE", "BOB"]
```

Or use the VGI client for integration tests:

```python
from vgi.client import Client
from vgi import Arguments
import pyarrow as pa

batch = pa.RecordBatch.from_pydict({"name": ["alice", "bob"]})

with Client("./my_worker.py") as client:
    results = list(client.scalar_function(
        function_name="upper_case",
        input=iter([batch]),
        arguments=Arguments(positional=[pa.scalar("name")]),
    ))

assert results[0]["result"].to_pylist() == ["ALICE", "BOB"]
```

---

## Protocol Overview

VGI uses Apache Arrow IPC streaming over stdin/stdout:

```
Client                              Worker
  │                                   │
  │──── Invocation ────────────────▶  │  Function name, args, input schema
  │◀─── OutputSpec ─────────────────  │  Output schema, max_workers
  │                                   │
  │──── Input Batch 1 ──────────────▶ │
  │◀─── Output Batch 1 ──────────────  │  transform(batch)
  │         ...                       │
  │──── FINALIZE ───────────────────▶ │  Signal end of input
  │◀─── Final Output ────────────────  │  finish() results
  └───────────────────────────────────┘
```

See [Protocol Specification](docs/protocol.md) for details.

---

## Documentation

- [Protocol Specification](docs/protocol.md) - Wire format details
- [Function Lifecycle](docs/lifecycle.md) - Setup, bind, process, teardown
- [Metadata API](docs/metadata.md) - Function introspection
- [Generator API](docs/generator-api.md) - Advanced streaming patterns
- [Catalog Interface](docs/catalog-interface.md) - DuckDB ATTACH integration

---

## Development

```bash
git clone https://github.com/query-farm/vgi-python
cd vgi-python

uv sync --all-extras        # Install dependencies
uv run pytest -n auto       # Run tests
uv run ruff check --fix .   # Lint
uv run ruff format .        # Format
uv run mypy vgi/            # Type check
```

## Requirements

- Python >= 3.12.4
- pyarrow
- DuckDB (for SQL integration)

---

## License

Copyright 2025-2026 Query.Farm LLC. All Rights Reserved.

This code is currently restrictively licensed. Contact [Query.Farm](https://query.farm) for licensing information.
