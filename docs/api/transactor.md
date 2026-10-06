# Transactor

!!! note "Advanced — reference only"
    The transactor is an advanced feature without a dedicated how-to guide yet. This page is the
    API reference; start from the [tutorial](../tutorial/index.md) and
    [function patterns](../how-to/function-patterns.md) if you're new to VGI.

The transactor is a long-lived subprocess that gives worker functions transactional access to a
database, mediated by `TransactorClient` over the `TransactorProtocol`.

## Runtime requirements

The bundled transactor requires both:

- `pip install 'vgi-python[transactor]'` for its Python dependencies.
- A VGI fork build of `duckdb-python` providing `DuckDBPyConnection.subcursor()`,
  which lets reads share an open write transaction. This extension has not yet
  been merged into Haybarn or upstream DuckDB; installing the extra alone is
  insufficient.

Engine resolution prefers `haybarn` when installed, then falls back to `duckdb`.
Use an environment where the selected engine provides `subcursor()`; installing
a compatible `duckdb` build alongside an incompatible `haybarn` installation
does not override that preference.

These requirements apply to the bundled transactor. Workers can implement
[table writes](../catalog-interface.md#table-writes-and-indexes) against their own
backing data sources without using it.

## API

::: vgi.transactor
