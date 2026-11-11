# Changelog

## Unreleased

### Protocol 3.3: write-path wire changes

Part of `vgi.v3` / `3.3.0` (the version string does not change again).

- **Removed `catalog_table_scan_function_get`.** `catalog_table_scan_branches_get` is the only scan-discovery RPC
  and every worker must answer it; the extension no longer falls back. Author code is unaffected:
  `CatalogInterface.table_scan_function_get` still works, wrapped as a one-branch result by the default
  `table_scan_branches_get`. `Client.table_scan_function_get` now reads the single branch of
  `catalog_table_scan_branches_get` (and raises `CatalogClientError` for a multi-branch table); the client's
  `MethodNotImplemented` fallback is gone.
- **`catalog_table_create` returns `TableCreateResponse(created: bool not null)`** (new in `vgi.protocol`).
  `CatalogInterface.table_create` may return `False` when `on_conflict` is `IGNORE` and the table already
  existed; `True` or `None` (the old return) means it was created. The extension uses it so
  `CREATE TABLE IF NOT EXISTS ... AS SELECT` no longer appends into an existing table.
  `Client.table_create` returns the flag.
- **The write-options batch carries only `result_mode`.** DuckDB rewrites every `INSERT ... ON CONFLICT` /
  `INSERT OR REPLACE` into a MERGE before the catalog sees it, so `on_conflict` / `on_conflict_columns` were only
  ever `"throw"` / `[]`. `WriteOptions.on_conflict` and `WriteOptions.on_conflict_columns` remain as deprecated
  read-only properties with those values; they are no longer constructor arguments.
- Codegen: every SDK's generated registry and schemas drop the removed method and gain the
  `catalog_table_create` result; vgi-java gains the generated `TableCreateResponse` record.

### Protocol 3.3: table scans and write functions are table-only

- **Requires the matching DuckDB extension.** The protocol version is now `vgi.v3` / `3.3.0`; peers match on
  major and minor, so 3.3 workers and 3.2 clients reject each other.
- `FunctionInfo` gains `visibility` (`"public"`, the default, or `"table_only"`), with the constants
  `FUNCTION_VISIBILITY_PUBLIC`, `FUNCTION_VISIBILITY_TABLE_ONLY` and `FUNCTION_VISIBILITIES` in `vgi.catalog`.
  See `docs/protocol/function-visibility.md`.
- `ReadOnlyCatalogInterface` now lists every function a `Table` names (scan, insert, update, delete) even when it is
  not in the schema's `functions`, so DuckDB always has its capabilities (projection/filter pushdown, row ids,
  `has_finalize`, required secrets). Scans that were not listed used to run with every capability off.
- **User-visible:** write functions are always table-only, and a table's scan is table-only unless the table sets
  the new `Table(expose_scan=True)` or the scan class is also registered in a schema's `functions`. Table-only
  functions disappear from `duckdb_functions()` and cannot be called from SQL.
- The requirement to repeat an UPDATE/DELETE table's scan in the schema's `functions` is gone (it is listed for you;
  a scan repeated there stays public).
- **Hand-written catalogs** (custom `schema_contents` / `table_scan_function_get`) must list every function their
  tables name in the function listing, or the extension refuses the scan with a Binder Error naming the function. Set
  `visibility=FUNCTION_VISIBILITY_TABLE_ONLY` on the ones that should not be SQL functions.
- `Schema.to_schema_info` counts the tables' functions in `estimated_object_count`.
