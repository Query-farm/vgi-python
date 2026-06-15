---
description: "How to integrate VGI table functions with DuckDB's optimizer: accept pushed-down filters and report column statistics."
---

# Integrate with the optimizer

**What this is:** how to make table functions cooperate with DuckDB's query optimizer — receiving
pushed-down `WHERE` predicates and reporting column statistics so the planner can skip work.<br>
**Who it's for:** developers whose table functions back real data sources and want less data moved.

## Prerequisites

- A table or table-in-out function (see [Function patterns](function-patterns.md)).
- Familiarity with your data's shape (which columns are filterable, their ranges).

## Filter pushdown

A table function can receive the `WHERE` predicates DuckDB would otherwise apply *after* the scan,
and apply them at the source. Opt in with `filter_pushdown = True` in the function's `Meta`. The
framework deserializes the predicates for you and exposes them on `params.current_pushdown_filters`
as a `PushdownFilters` tree (or `None` when no filter applies), refreshed before each `process`
call:

```python
# illustrative — sketch using your own types
class Events(TableFunctionGenerator[EventsArgs]):
    class Meta:
        filter_pushdown = True      # opt in to receiving WHERE predicates

    @classmethod
    def process(cls, params, state, out):
        filters = params.current_pushdown_filters   # PushdownFilters tree, or None
        # apply the filters while generating rows, then out.emit(...) / out.finish()
```

`PushdownFilters` is already decoded — you don't call `deserialize_filters` yourself (that helper is
for the raw wire bytes). To have the framework apply the filters to your output automatically,
set `auto_apply_filters = True` in `Meta`. The node types and a worked example are in the
[Filter Pushdown reference](../filter-pushdown.md) and [API: Filter Pushdown](../api/filters.md).

## Column statistics

When a table reports per-column min/max, null, and distinct-count statistics, DuckDB's optimizer
can eliminate scans and order joins better. The declarative path is a `statistics` entry on the
`Table` descriptor:

```python
# illustrative — `schema` and the table stand in for your own catalog
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

(The snippets above are illustrative — `schema` and the `departments` table stand in for your own
catalog.) Full details — RPC-based dynamic statistics, TTLs, spatial bounds — are in the
[Column Statistics reference](../column-statistics.md).

## Next steps

- **Filter format & evaluation** → [Filter Pushdown reference](../filter-pushdown.md) ·
  [API: Filter Pushdown](../api/filters.md).
- **Statistics options** → [Column Statistics reference](../column-statistics.md).
