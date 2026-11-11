# Function visibility and table functions (protocol 3.3)

Status: **normative**. Introduced in `vgi.v3` / `3.3.0`.

## Problem

A table's descriptor names its scan function and its write functions (insert, update, delete). The
client binds them by name over RPC, but it takes their capabilities only from the function's entry in
the schema's function listing (`catalog_schema_contents_functions`, or the batched
`catalog_contents`): projection, filter and sampling pushdown, late materialization, required
secrets, partitioning, ordering, filter profiles, `has_finalize`, and the write schema. A worker that
did not list a table's scan therefore had every capability silently off (a row-id table failed with
"Virtual columns require projection pushdown"), and a worker that did list them made them ordinary
SQL functions, cluttering `duckdb_functions()` and letting users call write functions directly.

## `FunctionInfo.visibility`

`FunctionInfo` gains one optional field:

| Field | Arrow type | Values |
|---|---|---|
| `visibility` | nullable `utf8` (a plain string, not a dictionary or enum type) | `"public"`, `"table_only"`; null or absent means `"public"` |

- **`public`**: an ordinary catalog function. The client exposes it to SQL: it appears in
  `duckdb_functions()` and autocomplete and can be called, qualified or not.
- **`table_only`**: the client keeps the function's full description but does not expose it to SQL.
  It is absent from `duckdb_functions()` and autocomplete and cannot be called, not even fully
  qualified. The client uses it only to resolve the functions tables name.

A client MUST reject any other value before any function from that listing is used, with an error
that names the function. (The DuckDB extension decodes function listings lazily, so the rejection
happens when the listing is first read, not necessarily at ATTACH.) A worker MUST NOT send another
value.

## Listing rule

A worker MUST list, in the function listing, every function a table descriptor names (its scan
function and each of its insert, update and delete functions), so the client always has their
capabilities. Then:

1. Write functions (insert, update, delete) are always `table_only`.
2. A table's scan function is `table_only` by default. The author opts in to a SQL-callable scan
   with one flag on the table descriptor (`expose_scan` in Python and Rust, `exposeScan` in
   TypeScript and Java, `ExposeScan` in Go and C#; default false). A scan the author also registers
   as an ordinary function is `public`.
3. Functions not named by any table are unchanged: `public`.

The SDKs enforce this rule automatically; authors do not set `visibility` by hand unless they build
their own listing.

## Client resolution

To resolve a function a table names, the client looks it up among the visible functions (the
table's schema, the schema the worker's redirect claims, then the default schema), then among the
`table_only` functions, then in the engine's own catalog (so delegation to built-in scans such as
`read_parquet` keeps working). If nothing resolves, binding fails with an error naming the worker,
the table and the function, saying the worker must list it. There is no silent fallback to a scan
with every capability off.

## Compatibility

Protocol 3.3.0 is a minor bump of `vgi.v3`. Clients and workers match on exact major and minor, so 3.3
peers reject 3.2 peers. User-visible change: table scans and write functions disappear from
`duckdb_functions()` unless the table sets `expose_scan`, or the scan is also registered as an ordinary
function.

## Python surface

- `FunctionInfo.visibility`, with the constants `FUNCTION_VISIBILITY_PUBLIC`,
  `FUNCTION_VISIBILITY_TABLE_ONLY` and `FUNCTION_VISIBILITIES` in `vgi.catalog`.
- `Table(expose_scan=...)`.
- `ReadOnlyCatalogInterface` (declarative `Catalog`) lists every table-named function in the
  table's schema when the author did not register it anywhere, and sets `visibility` per the rule.
  `Schema.to_schema_info` counts those functions, so the client never skips the listing on a zero
  `estimated_object_count`.
- The former requirement to repeat an UPDATE/DELETE table's scan in the schema's `functions` is
  gone; a scan repeated there is public, as before.
