# Filter Pushdown

Table functions can receive SQL `WHERE` predicates pushed down from DuckDB, letting the worker
prune data at the source. Filters arrive as a `PushdownFilters` tree of typed nodes; use
`deserialize_filters` to decode them. See the [Filter Pushdown](../filter-pushdown.md) guide for
the protocol and a worked example.

::: vgi.table_filter_pushdown
