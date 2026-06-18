# VGI (Vector Gateway Interface)

<p align="center">
  <img src="https://raw.githubusercontent.com/Query-farm/vgi-python/main/docs/vgi-logo.png" alt="VGI Logo" width="480">
</p>

<p align="center">
  <strong>Apache Arrow-based protocol for extending DuckDB using any language.</strong>
</p>

<p align="center">
  Created by <a href="https://query.farm">Query.Farm</a>
</p>

---

## See It in Action

```python
# my_worker.py
# /// script
# requires-python = ">=3.13"
# dependencies = ["vgi-python"]
# ///
from typing import Annotated
from vgi import ScalarFunction, Param, Returns, Worker
from vgi.catalog import Catalog, Schema
import pyarrow as pa
import pyarrow.compute as pc

class Multiply(ScalarFunction):
    """Multiply two columns element-wise."""

    @classmethod
    def compute(
        cls,
        a: Annotated[pa.Int64Array, Param(doc="First operand")],
        b: Annotated[pa.Int64Array, Param(doc="Second operand")],
    ) -> Annotated[pa.Int64Array, Returns()]:
        return pc.multiply(a, b)

class MyWorker(Worker):
    catalog = Catalog(
        name="my_worker",
        schemas=[Schema(name="main", functions=[Multiply])],
    )

if __name__ == "__main__":
    MyWorker().run()
```

The `# /// script` block is [inline script metadata](https://packaging.python.org/en/latest/specifications/inline-script-metadata/):
`uv run my_worker.py` reads it, provisions an isolated environment with
`vgi-python`, and runs the worker — no virtualenv to create or activate.

```sql
-- First time only.
INSTALL vgi FROM community;
LOAD vgi;
-- LOCATION is the command that launches the worker. `uv run` resolves the
-- script's inline dependencies, so nothing needs to be installed first.
ATTACH 'my_worker' (TYPE vgi, LOCATION 'uv run my_worker.py');

SELECT my_worker.multiply(6, 7);
-- 42
```

Or you can launch the [Haybarn](https://github.com/Query-farm-haybarn/haybarn)
CLI and attach the worker in one step:

```bash
uvx haybarn-cli "vgi:my_worker?location=uv run my_worker.py"
```

This drops you into a session with the functions you just added, available as
`my_worker.multiply(...)`.

That's it. No C++ compilation, no extension versioning, no complex build process. Just a Python script that Haybarn (or DuckDB) can call.

---

## Installation

The Python package is published on PyPI as `vgi-python` (the `vgi` name was
taken), but you still `import vgi` in code. The examples above don't install it
explicitly — the worker script's inline `# /// script` metadata lets `uv run`
provision it on demand. To add it to a project or environment directly:

```bash
pip install vgi-python      # or: uv add vgi-python
```

You also need a DuckDB-compatible SQL engine to load the `vgi` extension and
call your functions. These examples use [Haybarn](https://github.com/Query-farm-haybarn/haybarn),
Query Farm's DuckDB distribution, which ships the `vgi` extension signed for its
own catalog and runs with no install via `uvx`:

```bash
uvx haybarn-cli              # start an interactive SQL session
```

Stock `duckdb` works too — `INSTALL vgi FROM community; LOAD vgi;` resolves the
extension from the DuckDB community repository instead.

---

## Why VGI?

VGI lets you extend DuckDB with Python functions that run in separate processes, communicating via Apache Arrow IPC. This means:

| Traditional Extensions | VGI Workers |
|----------------------|-------------|
| C/C++ compilation required | Any language with an Apache Arrow library |
| Tied to DuckDB version | Version independent |
| Complex build/release cycle | Ship a script or executable |
| Runs in-process | Process isolation |

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
# my_worker.py
# /// script
# requires-python = ">=3.13"
# dependencies = ["vgi-python"]
# ///
from typing import Annotated
import pyarrow as pa
import pyarrow.compute as pc
from vgi import ScalarFunction, Param, Returns, Worker
from vgi.catalog import Catalog, Schema


class UpperCase(ScalarFunction):
    """Convert string values to uppercase."""

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.StringArray, Param(doc="String value to uppercase")],
    ) -> Annotated[pa.StringArray, Returns()]:
        return pc.utf8_upper(value)


class MyWorker(Worker):
    catalog = Catalog(
        name="my_funcs",
        schemas=[Schema(name="main", functions=[UpperCase])],
    )


if __name__ == "__main__":
    MyWorker().run()
```

### Step 2: Use from SQL

```sql
-- Attach the worker as a catalog (its catalog name is "my_funcs")
ATTACH 'my_funcs' (TYPE vgi, LOCATION 'uv run my_worker.py');

-- Call your function (qualify with the catalog name, or run `USE my_funcs;` first)
SELECT my_funcs.upper_case(name) FROM users;

-- Use in complex queries
SELECT id, my_funcs.upper_case(status) as status
FROM orders
WHERE created_at > '2024-01-01';
```

### Step 3: There is no step 3

Your function is now available in any DuckDB-compatible engine. Ship the Python script to your team, and they can use it immediately.

---

## Function Types

VGI supports five function types:

| Type | Base Class | SQL Pattern | Use Case |
|------|------------|-------------|----------|
| **Scalar** | `ScalarFunction` | `SELECT func(col) FROM t` | Per-row transforms (1 row → 1 value) |
| **Table** | `TableFunctionGenerator` | `SELECT * FROM func(args)` | Generate rows from arguments (args → N rows) |
| **Table-In-Out** | `TableInOutFunction` | `SELECT * FROM func((SELECT ...))` | Reshape or filter a streamed table (N rows → M rows) |
| **Aggregate** | `AggregateFunction` | `SELECT func(col) FROM t GROUP BY k` | Accumulate per `GROUP BY` group (N rows → 1 value) |
| **Buffering** | `TableBufferingFunction` | `SELECT * FROM func((SELECT ...))` | See every row first — sort, top-k, full reduction (stream → state → stream) |

Each type overrides a small, predictable surface (see the `Multiply` example
above for a complete scalar worker):

- **Scalar** — `compute()` maps input arrays to one output array of the same length.
- **Table** — `process()` streams batches via `out.emit()` until `out.finish()`.
- **Table-In-Out** — `transform()` reshapes each input batch; `finish()` emits any trailing rows.
- **Aggregate** — `initialize` / `update` / `combine` / `finalize` accumulate one value per `GROUP BY` group, merging partial states across parallel workers.
- **Buffering** — like aggregate, but `finalize` streams a whole relation out once every input row has been seen (sort, top-k, full-stream reductions).

---

## Beyond Functions: Full Catalog Support

VGI workers can expose more than just functions. A worker can provide a complete database catalog with:

- **Schemas** - Organize objects into namespaces
- **Tables** - Expose external data as queryable tables
- **Views** - Define SQL views over your data
- **Functions** - Scalar, table, and table-in-out functions

```sql
ATTACH 'external_db' (TYPE vgi, LOCATION 'uv run my_catalog_worker.py');

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

Functions can run across multiple worker processes. The client automatically
distributes input batches round-robin across workers and collects results.

See [Function API Reference](docs/generator-api.md) for advanced patterns like distributed aggregation.

---

## Error Handling

Errors in your functions propagate to DuckDB with clear messages:

```python test="skip"
@classmethod
def compute(cls, value: Annotated[pa.Int64Array, Param()]) -> Annotated[pa.Int64Array, Returns()]:
    raise ValueError("Something went wrong")
```

```sql
SELECT my_func(col) FROM my_table;
-- Error: Something went wrong
```

Type bound violations are caught at bind time (before processing starts):

```sql
SELECT add_values(name, price) FROM orders;
-- Error: Argument 'left': Column 'name' has type string,
--        but type bound requires: is_integer
```

### Debugging Worker Failures

When a worker fails, the Python traceback is written to stderr. By default, the client captures this stderr and includes it in the error message (last 50 lines), so you get the full context:

```
ClientError: Worker Exception: function 'my_func' raised ValueError

Worker stderr:
Traceback (most recent call last):
  File "my_worker.py", line 42, in compute
    ...
ValueError: Something went wrong
```

For real-time debugging, set `VGI_WORKER_DEBUG=1` to stream worker logs directly to your terminal and enable DEBUG-level logging:

```bash
VGI_WORKER_DEBUG=1 python my_script.py
```

This is especially useful when integrating from C++ or other clients where stderr might otherwise be lost.

---

## Testing Your Functions

Use the VGI client for integration tests:

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

VGI uses `vgi_rpc`, an Apache Arrow IPC-based RPC framework, for all
client-worker communication over stdin/stdout pipes:

```
Client                              Worker
  │                                   │
  │──── bind(request) ──────────────▶ │  Function name, args, input schema
  │◀─── BindResponse ────────────────  │  Output schema, opaque data
  │                                   │
  │──── init(request) ──────────────▶ │  Start processing stream
  │◀─── Stream header ───────────────  │  execution_id, max_workers
  │                                   │
  │──── exchange(batch1) ───────────▶ │
  │◀─── output batch 1 ──────────────  │  transform(batch)
  │         ...                       │
  │──── [stream close] ─────────────▶ │  Signal end of input
  │                                   │
  │──── init(phase=FINALIZE) ───────▶ │  Start finalize stream
  │◀─── final output batches ────────  │  finish() results
  └───────────────────────────────────┘
```

---

## External Batch Offloading (Demo Storage)

When record batches are too large for HTTP request/response bodies, VGI supports
externalizing them to blob storage. The server replaces oversized batches with
pointer batches containing a URL, and the client transparently fetches the data.

The example HTTP server includes a built-in demo blob store for testing this
without S3 or any cloud infrastructure:

```bash
# Start with demo storage (4 KiB threshold for testing)
vgi-fixture-http --demo-storage --externalize-threshold-bytes 4096

# With zstd compression
vgi-fixture-http --demo-storage --externalize-threshold-bytes 4096 --externalize-compression zstd
```

When `--demo-storage` is enabled:
- Batches exceeding `--externalize-threshold-bytes` are stored in-memory and
  served from `/__blobs__/{id}` endpoints on the same server
- Clients can request upload URLs for large inputs via the `__upload_url__` endpoint
- The server advertises `VGI-Max-Request-Bytes` and rejects oversized requests with 413

For production use, implement the `ExternalStorage` protocol from `vgi_rpc` against
your cloud storage (S3, GCS, etc.). The example server also supports S3 via `--s3-bucket`.

---

## Documentation

- [Function Lifecycle](docs/lifecycle.md) - Bind, init, process, finalize
- [Metadata API](docs/metadata.md) - Function introspection
- [Function API Reference](docs/generator-api.md) - Advanced function patterns
- [Catalog Interface](docs/catalog-interface.md) - DuckDB ATTACH integration

---

## Logging

Workers support `--debug`, `--log-level`, `--log-format`, and `--log-logger` options:

```bash
# Enable debug logging
vgi-fixture-worker --debug

# JSON-formatted logs for structured pipelines
vgi-fixture-worker --log-format json

# Target a specific logger
vgi-fixture-worker --log-level DEBUG --log-logger vgi.worker
```

You can also use the `VGI_WORKER_DEBUG=1` environment variable, which enables `--debug` on the worker and stderr passthrough on the client without changing any code or CLI flags:

```bash
VGI_WORKER_DEBUG=1 python my_script.py
```

See [CLI Reference](docs/cli.md#worker-logging) for the full list of loggers and options.

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

- Python >= 3.13
- pyarrow
- A DuckDB-compatible engine for SQL integration — [Haybarn](https://github.com/Query-farm-haybarn/haybarn) (`uvx haybarn-cli`) or stock DuckDB

---

## License

Copyright (c) 2025, 2026 Query Farm LLC.

Licensed under the **Query Farm Source-Available License, Version 1.0** — see
[LICENSE](LICENSE) for the binding terms.

For a commercial license or any licensing questions, contact
[hello@query.farm](mailto:hello@query.farm).
