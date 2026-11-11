# Changelog

## Unreleased

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
- `Schema.to_schema_info` counts the tables' functions in `estimated_object_count`.
