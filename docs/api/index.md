---
description: "vgi-python API reference — functions, arguments, worker, client, catalogs, state storage, filter pushdown, auth, and observability."
---

# API Reference

## Where to Start

New to VGI? Follow this path:

1. **Pick a function pattern** — `ScalarFunction`, `TableFunctionGenerator`,
   `TableInOutFunction`, or `AggregateFunction` ([Functions](functions.md))
2. **Declare arguments** — `Param`, `ConstParam`, `Returns`, `TableInput` ([Arguments & Schema](arguments.md))
3. **Host them in a worker** — subclass `Worker`, then `vgi-serve` it over stdio or HTTP ([Worker & Serving](worker.md))
4. **Connect from DuckDB or Python** — the DuckDB extension or the Python `Client` ([Client](client.md))

Everything else — catalogs, state storage, filter pushdown, auth, observability — is optional and
added incrementally.

## Modules

| Module | Description | Required? |
|---|---|---|
| [Functions](functions.md) | `ScalarFunction`, `TableFunctionGenerator`, `TableInOutFunction`, `AggregateFunction`, `Function` | Yes |
| [Arguments & Schema](arguments.md) | `Param`, `ConstParam`, `Returns`, `TableInput`, `ArgumentSpec`, `schema` | Yes |
| [Worker & Serving](worker.md) | `Worker`, the `vgi-serve` entry point | Yes |
| [Client](client.md) | `Client`, `ClientError`, catalog client helpers | If calling workers from Python |
| [Catalogs](catalogs.md) | `Catalog`, `Schema`, `Table`, `View`, `CatalogStorage` | If exposing a catalog |
| [State Storage](storage.md) | `FunctionStorage`, SQLite / Azure SQL / Cloudflare DO backends | If functions keep state |
| [Metadata & Protocol](metadata.md) | `ResolvedMetadata`, `FunctionExample`, `FunctionStability`, protocol types | For introspection |
| [Filter Pushdown](filters.md) | `PushdownFilters`, filter node types, `deserialize_filters` | If accepting pushed-down filters |
| [Auth & Secrets](auth.md) | `AuthContext`, `CallContext`, bearer/JWT authenticators, secret protocol | HTTP: `[http]`; JWT: `[oauth]` |
| [Observability](observability.md) | OpenTelemetry tracing, worker logging configuration | `[otel]` for tracing |
| [HTTP](http.md) | Worker page, blob storage, request-size middleware | `pip install vgi-python[http]` |
| [Transactor](transactor.md) | `TransactorClient`, `TransactorProtocol` | `pip install vgi-python[transactor]` |
| [Exceptions](exceptions.md) | VGI exception types | — |

## Import Convention

The most common symbols are re-exported from the top-level `vgi` package:

```python
from vgi import ScalarFunction, TableInOutFunction, AggregateFunction, Param, Returns, Worker
```

Subpackages (`vgi.catalog`, `vgi.client`, `vgi.http`, `vgi.transactor`) are imported explicitly.
Optional modules require their corresponding extras to be installed.
