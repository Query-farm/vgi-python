# Write path changes in protocol 3.3

**Status: normative.** Part of `vgi.v3` / `3.3.0`, together with
[function visibility](function-visibility.md). The protocol version string does not change again:
a 3.3 peer implements both pages.

Protocol 3.3 makes three wire changes to the table write path and to scan discovery. Each one
removes a compatibility path or a field that could never carry information, or adds one bit the
client needs to get `CREATE TABLE ... AS SELECT` right.

## 1. `catalog_table_scan_function_get` is removed

`catalog_table_scan_branches_get` is the only scan-discovery method. It is **required**: every
worker that serves tables answers it, and a single-source table answers with one branch.

- The method `catalog_table_scan_function_get` is no longer part of `vgi.v3`. Its params schema,
  its registry entry and its place in the method-inventory hash are gone from every SDK.
- A client MUST NOT fall back to another method when a worker answers `catalog_table_scan_branches_get`
  with `MethodNotImplemented`. That answer is a protocol error naming the worker and the table.
  (Before 3.3 the DuckDB extension retried the single-function method and wrapped its answer as
  one branch; every worker that needed that fallback predates 3.1 and fails the 3.3 version check
  anyway.)
- The inlined `TableInfo.scan_function` is unchanged.

SDK authoring APIs are not part of the wire. In vgi-python a catalog still implements
`CatalogInterface.table_scan_function_get`; the default `table_scan_branches_get` wraps its result as
a one-branch `ScanBranchesResult`. `Client.table_scan_function_get` reads the single function branch
of `catalog_table_scan_branches_get` and raises `CatalogClientError` for a table with any other number
of branches.

## 2. `catalog_table_create` returns `created`

The result of `catalog_table_create` (until 3.2: no result) is one record:

| Field | Arrow type | Meaning |
|---|---|---|
| `created` | `bool`, not null | `true` when this call created the table; `false` when the request's `on_conflict` was `IGNORE` (`CREATE TABLE IF NOT EXISTS`) and a table of that name already existed |

- A worker MUST return `false` only for `IGNORE` on an existing table, and MUST then leave that
  table exactly as it was (columns, constraints and rows).
- `REPLACE` that replaces an existing table returns `true`: the call created the new table.
- `ERROR` on an existing name fails the call; there is no result.
- The record carries no `TableInfo`. A client that needs the table's description reads it with
  `catalog_table_get` as before.

The client uses `created` for `CREATE TABLE [IF NOT EXISTS] ... AS SELECT`: when it is `false`,
the statement inserts nothing (DuckDB's semantics for native tables); before 3.3 the DuckDB
extension appended the query's rows to the existing table. When a CTAS fails after `created = true`
on a non-transactional attach, the extension drops the table again (best effort). `CREATE OR REPLACE
TABLE ... AS` cannot restore the replaced table if the statement then fails.

In vgi-python, `CatalogInterface.table_create` returns `False` for "already existed" and `True`
or `None` (the historical return) for "created"; the worker sends `TableCreateResponse`.
`Client.table_create` returns the flag.

## 3. The write-options batch keeps only `result_mode`

Every write function call (insert, update, delete) receives the `write_options` named argument: a
serialized one-row Arrow batch. In 3.3 its schema is exactly:

| Column | Arrow type | Values |
|---|---|---|
| `result_mode` | `utf8`, not null | `count`, `rows` or `changes` (see `write_result_modes`) |

The former `on_conflict: utf8` and `on_conflict_columns: list<utf8>` columns are removed. DuckDB's
binder rewrites every `INSERT ... ON CONFLICT` (`DO NOTHING`, `DO UPDATE`) and `INSERT OR REPLACE`
into a `MERGE` before a catalog sees the statement, so those columns could only ever say `throw` and
`[]`. A worker MUST ignore any other column it finds in the batch, so a stray column from an older
client is harmless.

`MERGE` and `INSERT ... ON CONFLICT` on a VGI table are emulated by the client with the table's
ordinary write functions: one write call per `INSERT` / `UPDATE` / `DELETE` action of the MERGE, fed
from a scan of the target joined with the source. Workers see only ordinary write calls. The calls
of one MERGE are atomic together only on a transactional attach, and MERGE requires the table to
have a row id.

In vgi-python, `WriteOptions` has only `result_mode`; its `on_conflict` and `on_conflict_columns`
properties remain, deprecated, and always return `"throw"` and `[]`.
