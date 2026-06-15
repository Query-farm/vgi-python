---
description: "How to integrate VGI table functions with DuckDB's optimizer: accept pushed-down filters and report column statistics."
---

# Integrate with the optimizer

**What this is:** how to make table functions cooperate with DuckDB's query optimizer — receiving
pushed-down `WHERE` predicates and reporting column statistics so the planner can skip work.
**Who it's for:** developers whose table functions back real data sources and want less data moved.

## Prerequisites

- A table or table-in-out function (see [Function patterns](function-patterns.md)).
- Familiarity with your data's shape (which columns are filterable, their ranges).

## Filter pushdown

A table function can receive the `WHERE` predicates DuckDB would otherwise apply *after* the scan,
and apply them at the source. Opt in with `filter_pushdown = True` in the function's `Meta`, then
read the decoded filters in `process` via `params`:

```python test="skip"
class Events(TableFunctionGenerator[EventsArgs]):
    class Meta:
        filter_pushdown = True      # opt in to receiving WHERE predicates

    @classmethod
    def process(cls, params, state, out):
        filters = params.pushdown_filters   # decoded predicate tree (or None)
        # apply `filters` while generating rows, then out.emit(...) / out.finish()
```

Filters arrive as a hybrid JSON + Arrow structure; decode and evaluate them with
`deserialize_filters` (see [API: Filter Pushdown](../api/filters.md)). The wire format and a worked
example are in the [Filter Pushdown reference](../filter-pushdown.md).

## Column statistics

When a table reports per-column min/max, null, and distinct-count statistics, DuckDB's optimizer
can eliminate scans and order joins better. The declarative path is a `statistics` entry on the
`Table` descriptor:

```python test="skip"
from vgi.catalog import Table
from vgi.catalog.descriptors import ColumnStatisticsInput

Table(
    name="departments",
    columns=schema,
    statistics={"id": ColumnStatisticsInput(min=1, max=10, has_null=False)},
)
```

```sql
-- With statistics the optimizer can prune an impossible predicate entirely:
EXPLAIN SELECT * FROM mydb.data.departments WHERE id > 100;   -- Physical Plan: EMPTY_RESULT
```

Full details — RPC-based dynamic statistics, TTLs, spatial bounds — are in the
[Column Statistics reference](../column-statistics.md).

## Next steps

- **Filter format & evaluation** → [Filter Pushdown reference](../filter-pushdown.md) ·
  [API: Filter Pushdown](../api/filters.md).
- **Statistics options** → [Column Statistics reference](../column-statistics.md).
