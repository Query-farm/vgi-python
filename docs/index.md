---
description: "vgi-python: add scalar, table, and aggregate functions to DuckDB in pure Python over Apache Arrow — no C++ to compile, no extension to version, no build step."
---

# vgi-python

**Extend DuckDB in pure Python.** Add scalar, table, and aggregate functions that run in your own
process and stream data to DuckDB over Apache Arrow — no C++ to compile, no extension to version,
no build step.

Write a function, `uv run` the script, query it from SQL. Other languages work too.

<p align="center">
  <img src="assets/logo.png" alt="VGI logo" width="360">
</p>

Built by [🚜 Query.Farm](https://query.farm).

## See it in action

A **scalar** function — one row in, one row out:

```python
--8<-- "examples/calc_worker.py:scalar"
```

A **table** function — generate rows from an argument:

```python
--8<-- "examples/calc_worker.py:table"
```

Drop both into a `Worker`, add an [inline-script-metadata](https://packaging.python.org/en/latest/specifications/inline-script-metadata/)
header, and `uv run` it — no virtualenv, nothing to `pip install`. (The
[tutorial](tutorial/index.md) builds the complete file step by step.)

```sql
INSTALL vgi FROM community;
LOAD vgi;
-- LOCATION is the command that launches the worker.
ATTACH 'calc' (TYPE vgi, LOCATION 'uv run calc_worker.py');

SELECT calc.double(21);          -- 42
SELECT * FROM calc.series(3);     -- 0, 1, 2
```

That's it. No compilation, no extension versioning, no build process.

[Build this worker step by step in the tutorial →](tutorial/index.md){ .md-button }

## Installation

The package is published on PyPI as `vgi-python` (the `vgi` name was taken), but you `import vgi`
in code:

```bash
pip install vgi-python      # or: uv add vgi-python
```

You also need a DuckDB-compatible engine. [Haybarn](https://github.com/Query-farm-haybarn/haybarn),
Query.Farm's DuckDB distribution, ships the `vgi` extension and runs with no install:

```bash
uvx haybarn-cli            # interactive SQL session
```

Stock `duckdb` works too — `INSTALL vgi FROM community; LOAD vgi;`.

## Why VGI?

| Traditional extensions | VGI workers |
|---|---|
| C/C++ compilation required | Any language with an Apache Arrow library |
| Tied to a DuckDB version | Version independent |
| Complex build/release cycle | Ship a script or executable |
| Runs in DuckDB's process | Isolated worker process — a crash or a heavy dependency can't take DuckDB down |
| Native code in DuckDB's threads | Your code, optionally fanned out across worker processes |

**The tradeoff:** data crosses a process boundary as Apache Arrow IPC. That's fast and columnar,
but not free — co-locate workers (subprocess transport) for latency-sensitive paths, and reach for
VGI when the productivity and isolation win outweighs the hop.

**Use cases:** call REST APIs from SQL, run ML inference, process data with pandas/numpy, build
custom ETL transforms, expose external data sources as queryable tables and views.

## Function patterns

| Type | Base class | SQL pattern | Use case |
|---|---|---|---|
| **Scalar** | `ScalarFunction` | `SELECT func(col) FROM t` | Per-row transforms (1:1) |
| **Table** | `TableFunctionGenerator` | `SELECT * FROM func(args)` | Generate data |
| **Table-in-out** | `TableInOutFunction` | `SELECT * FROM func((SELECT ...))` | Streaming transforms, filtering |
| **Aggregate** | `AggregateFunction` | `SELECT func(col) ... GROUP BY` | Grouped accumulation |
| **Buffering** | `TableBufferingFunction` | `SELECT * FROM func((SELECT ...))` | Sees every row first (sort, top-k) |

See the [API Reference](api/index.md) for the full surface, or jump into the guides below.

## Documentation

- **[Tutorial](tutorial/index.md)** — build your first worker (scalar + table function callable
  from DuckDB) in about 20 minutes. **Start here.**
- **[How-to guides](how-to/index.md)** — task-oriented recipes: function patterns, catalogs,
  state, auth/HTTP, and optimizer integration.
- **[Concepts](concepts/index.md)** — how it works: the worker lifecycle, transports, and the
  Arrow data model.
- **[API Reference](api/index.md)** — auto-generated from the source, organized by module.

## Project links

- Source: [github.com/Query-farm/vgi-python](https://github.com/Query-farm/vgi-python)
- PyPI: [vgi-python](https://pypi.org/project/vgi-python/)
- Built on [vgi-rpc](https://vgi-rpc-python.query.farm/) — the transport-agnostic RPC layer.
