# Filter pushdown

Filter pushdown lets a VGI table function receive predicates from the calling engine and use them to avoid generating,
reading, or transferring rows that cannot contribute to the result.

This page is an informative Python user guide. The proposed wire contract is
[VGI Filter Encoding v2](protocol/vgi-filter-encoding-v2-spec.md). Its required/advisory correctness rules, expression
semantics, validation limits, and Arrow container layout are normative and take precedence over this guide. The
[DuckDB adapter guide](protocol/vgi-duckdb-filter-adapter.md) explains how DuckDB 1.5 and 2.0 expressions map to that
engine-neutral contract.

!!! note "Implementation status"

    Filter Encoding v2 is currently a proposed specification. The Python SDK implements its strict typed decoder,
    snapshot/delta state machine, and DuckDB reference evaluator. The longstanding convenience filter classes remain
    available as read-only views over common v2 expression shapes; the typed classes in `vgi.filter_v2` are the wire
    model.

## Opt in

A table function advertises pushdown support in its metadata:

```python
class Meta:
    filter_pushdown = True
```

Advertising support is a correctness promise. A required predicate must be applied completely and exactly or the
request must fail. An advisory predicate may be ignored, because the calling engine retains its exact local residual,
but any pruning performed from it must be conservative.

For functions that produce Arrow batches in Python, the framework can apply supported filters automatically:

```python
class Meta:
    filter_pushdown = True
    auto_apply_filters = True
```

Automatic filtering is the simplest safe choice when the function first materializes complete batches locally. It is
less useful when the worker can translate the predicate into a database query, file scan, API request, or partition
selection and avoid reading the rows in the first place.

## Handle filters manually

Custom implementations can inspect `params.current_pushdown_filters` during processing. This value reflects the
initial predicate and any accepted runtime update delivered before the current output batch. Use it to:

- translate a supported predicate into a deeper data source;
- derive bounds or exact values for partition pruning;
- choose an index or lookup strategy; or
- apply the complete predicate to a batch before emitting it.

Treat translation as an optimization boundary. Never discard unsupported children from `OR`, `NOT`, or another
indivisible subtree, and never turn an advisory approximation into an exact claim. If a required expression cannot be
represented or evaluated exactly, fail before emitting rows.

## Projection and column identity

Filters identify columns against the unprojected bind output schema. A filtered column can therefore be required for
evaluation even when the user's final projection omits it. Apply required filtering before dropping helper columns or
projecting the emitted batch.

Column names validate the mapping; indexes are authoritative. A dot inside a name is part of that identifier and is
not a nesting separator.

## Runtime filters

Dynamic join or Top-N pruning is advisory and versioned. The optional
[runtime-filter artifact specification](protocol/vgi-runtime-filter-artifacts.md) defines capability-gated Bloom and
prefix-range transport. Those algorithms are not part of base filter-v2 conformance, and an implementation must not
advertise an algorithm until it implements and validates that algorithm's complete immutable artifact contract.

## Further reading

- [Filter Encoding v2 specification](protocol/vgi-filter-encoding-v2-spec.md) — normative wire contract
- [Runtime-filter artifacts](protocol/vgi-runtime-filter-artifacts.md) — proposed optional normative extension
- [DuckDB filter adapter](protocol/vgi-duckdb-filter-adapter.md) — informative engine mapping
- [Pushdown and statistics](how-to/pushdown-and-statistics.md) — optimizer integration patterns
- [Filter API reference](api/filters.md) — current Python API
