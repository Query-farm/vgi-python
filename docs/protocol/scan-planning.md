# Filtered scan planning

Protocol 3.2 lets a catalog table that redirects its scan to another function
(`read_parquet`, `iceberg_scan`, a VGI table function, an attached catalog
table) see the query's filters before it chooses what to scan, and lets the
client prune the branches it returns.

Protocol 3.1 resolved scan redirects at bind time. `catalog_table_scan_branches_get`
ran while the client was still binding `FROM`, before `WHERE` existed, so a
worker that publishes many files had to return, and sign, every file on every
scan. Pruning then depended entirely on the redirected function (Hive paths or
per-file footers for `read_parquet`). Table functions already received
`pushdown_filters`; redirected tables did not.

Protocol 3.2 adds three independent mechanisms:

1. **Filtered planning.** A table declares `TableInfo.scan_planning = "filtered"`.
   The client then defers `catalog_table_scan_branches_get` until it has
   collected the query's predicates on that table and sends them as
   `pushdown_filters`. The worker may use them to return fewer branches or fewer
   files per branch.
2. **Branch statistics.** A `ScanBranch` may carry `row_count` and
   `column_statistics`. A client may drop a branch whose statistics prove that
   no row can satisfy the query's predicates.
3. **Branch-filter contradiction.** A client may drop a branch whose
   `branch_filter` contradicts the query's predicates, before binding it. This
   needs no new wire fields.

Every mechanism is pruning only. The client still applies every query predicate
to the rows the remaining branches produce, so a worker that ignores the
filters, or returns more than it needs to, stays correct.

## `TableInfo.scan_planning`

`scan_planning` is a string, `"bind"` (the default) or `"filtered"`.

- `"bind"`: unchanged 3.1 behaviour. The client may call
  `catalog_table_scan_branches_get` at bind time with `pushdown_filters = null`,
  or use the inlined `scan_function`.
- `"filtered"`: the client calls `catalog_table_scan_branches_get` once per scan
  of the table in a query, after it has collected that scan's predicates.
  `scan_function` MUST be null; a client rejects a table that sets both. A
  client that cannot defer the call (an engine without a planning hook) calls it
  at bind time with `pushdown_filters = null`, which is always permitted.

Unknown values are a protocol error.

## `catalog_table_scan_branches_get(pushdown_filters)`

The method gains one nullable parameter, `pushdown_filters` (`binary`): an Arrow
IPC stream holding one VGI Filter Encoding v2 snapshot batch, exactly the shape
table functions receive in `InitRequest.pushdown_filters`
([filter encoding v2](vgi-filter-encoding-v2-spec.md)).

- Column references index the table's declared columns (`TableInfo.columns`),
  in declaration order.
- Every predicate is **advisory**. The worker may ignore any of them.
- The client sends only predicates that are constant at planning time: a
  predicate containing a prepared-statement parameter, a non-foldable function,
  or a reference to another table is omitted, not approximated.
- `null` means no predicates were collected (bind-time planning, or a query
  with no usable predicates).

The worker returns the branches needed to produce every row that can satisfy
the predicates. Returning extra branches or files is permitted; omitting a
branch that holds a matching row is a correctness bug in the worker.

`at_unit` / `at_value` are sent unchanged. A client rejects `AT (...)` when the
result has more than one branch, as in 3.1.

### Zero branches

For a `"filtered"` table, a `ScanBranchesResult` with zero branches is legal
and means the scan produces no rows. The client plans an empty scan with the
table's declared columns. For a `"bind"` table zero branches remain a protocol
error.

## `ScanBranch` statistics

Two nullable fields are added to `ScanBranch`:

- `row_count` (`int64`): an estimate of the rows the branch produces before any
  predicate. Used for cardinality estimation only; it is never trusted as exact.
- `column_statistics` (`binary`): the same payload as
  `TableInfo.column_statistics` / `ScanSplit.column_statistics`, produced by
  `serialize_column_statistics`, keyed by the table's declared column names.
  Columns may be omitted.

Statistics are **conservative claims** about every row the branch produces:

- every non-null value of a column lies within `[min, max]`;
- `has_null = false` means the column has no NULLs;
- `has_not_null = false` means the column is entirely NULL.

A client prunes a branch only when a collected predicate is provably false
against its statistics. Wrong statistics silently drop rows, so a worker must
leave a column out rather than guess.

Statistics are honoured for both `"bind"` and `"filtered"` tables.

## `branch_filter` contradiction

A client may evaluate a branch's `branch_filter` against the collected
predicates and drop the branch when their conjunction is unsatisfiable (for
example `branch_filter = "event_date = DATE '2026-10-15'"` with
`WHERE event_date >= DATE '2026-11-01'`). `branch_filter` semantics are
otherwise unchanged: it is still AND-ed into every scan of the branch.

`branch_filter` is exactly one SQL expression. A value that parses as more than
one expression (for example containing a top-level comma) is a protocol error;
3.1 clients silently kept only the first expression.

## Client behaviour (DuckDB extension)

The reference client resolves `"filtered"` tables in its pre-optimizer rewrite:

1. At bind time a `"filtered"` table returns a placeholder scan with the table's
   declared columns and makes no RPC.
2. The rewrite collects the conjuncts of the filters above the placeholder that
   reference only its columns and are constant, serializes them as a filter v2
   snapshot, and calls `catalog_table_scan_branches_get` once for that scan.
3. It drops branches by `column_statistics` and `branch_filter` contradiction.
4. It emits one arm per remaining branch (a `UNION ALL` when there is more than
   one), or an empty result when none remain. DuckDB's optimizer then runs on
   the arms, so the redirected function's own pruning (for example
   `read_parquet` Hive partition and row-group pruning) still applies.

Because collection runs before DuckDB pushes filters through joins, the
reference client sees `WHERE` predicates on the table but not predicates DuckDB
would later derive from join conditions. That limitation is a property of the
client, not of the protocol.

## SDK surface

Every SDK exposes:

- `scan_planning` on table descriptors (`"bind"` default);
- a planning hook (`table_scan_branches_plan` in Python) that receives the
  decoded `pushdown_filters` together with the raw bytes. Its default ignores
  the predicates and returns the existing `table_scan_branches_get` result, so
  existing overrides keep their 3.1 signatures and behaviour;
- a helper that tests whether the predicates can match a candidate described by
  min/max statistics (`ScanPlanningFilters.may_match` in Python), for pruning a
  manifest before returning branches;
- `row_count` and `column_statistics` on its `ScanBranch` builder;
- validation that a `"filtered"` table does not inline `scan_function`.

## Compatibility

Protocol 3.2.0 is a minor bump of `vgi.v3`. Clients and workers match on exact
major and minor, so 3.2 peers reject 3.1 peers. A worker upgraded to a 3.2 SDK
needs no code changes unless it opts in to `"filtered"` planning or branch
statistics.
