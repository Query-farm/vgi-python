# VGI DuckDB Filter Adapter

**Status:** Proposed implementation guide
**Target protocol:** VGI 2.0.0
**DuckDB adapters:** 1.5.x and 2.0
**Initial expression semantics:** `vgi.duckdb.standard.v1` (upstream DuckDB v1.5.5)
**Normative wire specification:** [VGI Filter Encoding v2](vgi-filter-encoding-v2-spec.md)

## 1. Scope

This document maps DuckDB scan-filter APIs to VGI Filter Encoding v2. DuckDB class names, enum values, bound-expression layouts, and private scalar functions are adapter concerns; they are not VGI wire types.

The adapter reads bound expressions. It never parses rendered SQL and never forwards raw parser spellings or DuckDB's private serialized AST.

## 2. Shared rules

Both DuckDB adapters:

- emit only `vgi.filters.v2`;
- set each filter document's `semantics` to `vgi.duckdb.standard.v1` only after the worker advertises that profile and
  the adapter proves that the bound expression matches upstream DuckDB v1.5.5 behavior;
- use `vgi.none.v1` for a scan whose filters are all independent of DuckDB session context;
- use `vgi.duckdb.session.v1` only after verifying that DuckDB's `icu` extension supplied the bound `TimeZone` and
  `Calendar` settings, capturing `default_collation`, `ieee_floating_point_ops`, and `integer_division`, and confirming
  that the worker advertised the profile;
- request optional strict provider matching only when the local and advertised opaque fingerprints are identical;
- map every column to the unprojected `BindResponse.output_schema`;
- include both authoritative column index and validating exact name;
- serialize a required predicate atomically or decline its pushdown;
- retain DuckDB's exact local predicate for every advisory predicate;
- normalize recognized operations into VGI core nodes or registered function identities; and
- never drop an unsupported child from `OR`, `NOT`, or another indivisible subtree.

Typed constants remain Arrow values. Small exact `IN` sets use a typed Arrow list scalar; larger exact sets use `InitRequest.join_keys`. Sets are never truncated.

## 3. DuckDB 1.5 adapter

DuckDB 1.5 exposes concrete `TableFilter` subclasses and single-column table-function filters. The adapter converts them into the unified VGI AST:

| DuckDB 1.5 filter | VGI v2 representation |
|---|---|
| `ConstantFilter` | `comparison` plus typed `literal` |
| `IsNullFilter` / `IsNotNullFilter` | `is_null` |
| `ConjunctionAndFilter` / `ConjunctionOrFilter` | `and` / `or` |
| `StructFilter` | repeated `field_ref` |
| `InFilter` | `in` with inline or external exact set |
| `ExpressionFilter` | recursively translated expression AST |
| `OptionalFilter` / `SelectivityOptionalFilter` | separate advisory predicate |
| initialized `DynamicFilter` | advisory predicate update with stable ID and revision |
| `BFTableFilter` | omit until a negotiated runtime-artifact exporter exists |

The DuckDB 1.5 API offers one root scan column, so this adapter emits only single-column expressions even though the VGI wire permits multiple references. This is an engine-adapter limitation, not a protocol restriction.

The v1 semantic oracle is upstream DuckDB v1.5.5 commit
`d8cdaa33fda8df955cc76ef58a280f68f4cd43fa`. Haybarn v1.5.5-rc1 commit
`105edd31b57fe7e6d32ee648efad562e45a9f908` is a reference distribution expected to pass the same corpus; upstream
behavior wins if an unlisted Haybarn patch differs.

## 4. DuckDB 2.0 filter model

DuckDB 2.0 converges active scan filters on `ExpressionFilter`. Old `TableFilterType` values are named `LEGACY_*` and remain for compatibility/deserialization. They are not emitted as VGI discriminators.

`ExpressionFilter` contains:

- one bound expression tree; and
- `column_indexes`, which maps each `BoundReferenceExpression` index to a scan projection index.

For every bound reference, the adapter performs both mappings:

```text
BoundReferenceExpression index
    -> ExpressionFilter.column_indexes
    -> scan column-index mapping
    -> unprojected VGI bind-schema index
```

It validates the resulting field name before serialization. Projected output positions never appear on the wire.

Multi-column predicates offered through DuckDB's partial table-function pushdown contract are advisory because DuckDB retains their exact residual. A single-column predicate may be required only when DuckDB's table-function contract removes the local filter and the complete VGI expression is supported exactly.

Until stable DuckDB 2.0 semantics are published, the 2.0 adapter emits `vgi.duckdb.standard.v1` only for expressions it
can map exactly to the v1.5.5 oracle. After DuckDB 2.0 is released, its distinct behavior will be advertised as
`vgi.duckdb.standard.v2`; it does not revise v1. Workers may advertise both, and the filter document selects one.

## 5. Bound-expression mapping

The DuckDB 2.0 adapter translates semantic identity rather than relying on legacy expression classes:

| DuckDB expression | VGI node |
|---|---|
| `BoundReferenceExpression` | `column_ref` after index mapping |
| bound constant | typed `literal` |
| `COMPARE_IN` | `in` |
| `OPERATOR_IS_NULL` / `OPERATOR_IS_NOT_NULL` | `is_null` |
| `OPERATOR_NOT` | `not` |
| conjunction | `and` / `or` |
| bound comparison scalar function | `comparison` |
| bound cast scalar function | `cast` |
| recognized arithmetic function | `arithmetic` or `negate` |
| registered standard function | symbolic VGI `call` |
| negotiated extension function | namespaced VGI `call` |

DuckDB 2.0 represents comparisons and casts as `BoundFunctionExpression`. The adapter identifies the resolved DuckDB function and types, then emits the corresponding core VGI node. It must not emit generic calls for equality, ordering, or casts merely because their C++ representation is now a scalar function.

Unknown deterministic functions may be emitted only under an advertised VGI semantic contract. Matching a displayed function name is insufficient.

The initial DuckDB 1.5 adapter does not emit a standard or extension `call` from a generic
`BoundFunctionExpression`, because that API exposes no stable semantic identity with which to prove an overload from
its displayed name. It may still serialize a DuckDB-generated `TableFilter` over `VARCHAR` with `vgi.none.v1` when
the optimizer's documented generation invariant proves that the original bound operation was uncollated and binary.
If a collation wrapper, non-binary default, or other function remains in the bound operation, generic admission
rejects it. The 1.5 integration suite verifies this invariant under both binary and non-binary default collations.

The adapter captures evaluation context from the `ClientContext` that bound the expression. It MUST NOT substitute the
`GetClientProperties` UTC fallback when DuckDB's `TimeZone` setting is absent. A missing `TimeZone` or `Calendar`
setting means the `icu` extension is unavailable for purposes of `vgi.duckdb.session.v1`; context-independent filters
may still use `vgi.none.v1`.

The adapter captures `ieee_floating_point_ops` and `integer_division` from that same context, including their effective
local or global values. It classifies division and any other affected arithmetic only after binding, so both resolved
value semantics and result type must match the serialized expression. It does not encode the deprecated
`null_on_division_by_zero` setting: when that setting is enabled, the adapter declines every division, modulo, or
function expression whose behavior it can affect. The worker evaluator verifies the deprecated setting is `false`
before binding such an expression.

## 6. Required-filter admission is a correctness boundary

`pushdown_expression` is not the only route by which DuckDB can install a required table filter. For every table
function with `filter_pushdown = true`, `GenerateTableScanFilters` first performs DuckDB-owned rewrites without
consulting that callback. `TryPushdownPrefixFilter`, `TryPushdownLikeFilter`, and `TryPushdownInFilter` all have
`PUSHED_DOWN_FULLY` paths. `TryPushdownConstantFilter` also generates fully pushed filters from the equivalence map.
When a rewrite reports `PUSHED_DOWN_FULLY`, `GenerateTableScanFilters` removes the original residual. Setting
`pushdown_expression` to a conservative callback, or having it return `false`, does not protect these paths.

Required filters can therefore enter the scan through two distinct routes:

```text
original BoundColumnRefExpression predicate
    |-- DuckDB-generated table-filter rewrite
    |      prefix / LIKE / IN / equivalence-map comparison
    |      -> may report PUSHED_DOWN_FULLY and remove the residual
    |      -> does not call pushdown_expression
    |
    `-- generic-expression pushdown
           -> calls pushdown_expression
           -> single column: PUSHED_DOWN_FULLY and remove the residual
           -> multiple columns: conversion may decline; if installed,
                                PUSHED_DOWN_PARTIALLY and retain the residual
```

The correctness boundary is admission of any filter for which DuckDB will remove, weaken, or cease to rely on the
local residual. The callback is only the admission point for the generic-expression route. A false positive at either
route can silently return wrong rows.

The VGI adapter MUST implement one pure required-filter eligibility facility as the single source of truth for
admission and serialization. It MUST cover both original expressions and DuckDB-generated `TableFilter` candidates.
The generic callback and any generated-filter admission hook MUST delegate to this facility. In particular:

- the admission and serialization paths MUST NOT maintain independent node, filter, function, overload, type,
  collation, or session-context allowlists;
- eligibility MUST recurse through the complete expression or generated filter group atomically and reject any
  unsupported descendant;
- generic-expression logic MUST account for both representations: optimize-time `BoundColumnRefExpression` bindings
  and serialization-time `BoundReferenceExpression` indexes plus `ExpressionFilter.column_indexes`;
- eligibility MAY validate optimize-time bindings against `LogicalGet`, but MUST NOT assume that final dense reference
  indexes or `column_indexes` already exist;
- generated rewrites that produce multiple filters, including prefix ranges and dense `IN` ranges, MUST be staged and
  admitted as one unit before the original residual is removed; and
- successful serialization MUST reproduce the same column identities, resolved functions, explicit casts, types,
  collation, session-dependent semantics, and conjunction of generated filters that admission approved.

The callback and generated-filter gate MUST be side-effect-free. They MUST NOT allocate predicate IDs or wire value
references, emit filter payloads, mutate bind data or capability state, or record pending filters for later
transmission. DuckDB's multi-column generic path invokes the callback before
`TryCreateMultiColumnExpressionFilter`, which may subsequently return `nullptr`. A generated rewrite may likewise be
declined before installation. Serialization begins only from the `TableFilterSet` that DuckDB actually installs.

Stock DuckDB 2.0 currently provides no table-function admission callback between its specialized rewrites and their
`PUSHED_DOWN_FULLY` residual pruning. Until that API has a generated-filter gate, a VGI adapter MUST set
`filter_pushdown = true` only when it can guarantee that every non-optional filter this DuckDB build can generate is
serializable and supported by the bound worker for all admitted input types, collations, and evaluation contexts. If
it cannot make that guarantee, it MUST disable `filter_pushdown` for the scan or use a DuckDB build with such a gate.
The gate SHOULD stage the complete generated filter group and commit it only after acceptance; rejection MUST leave
the original residual intact.

DuckDB 1.5 has the same callback-bypassing admission constraint and no generated-filter gate. Its VGI adapter therefore
enables required filter pushdown only for a worker and scan configuration for which it implements every specialized
`TableFilter` that DuckDB 1.5 can install. A resource limit discovered only while materializing an already-admitted
required filter, such as an exact `IN` payload exceeding its configured byte limit, MUST fail the query before the
remote scan begins. It MUST NOT omit the filter or claim that DuckDB will apply a residual that may already have been
removed. This fail-closed resource error is distinct from declining an original generic expression in
`pushdown_expression`, which still leaves that expression local.

Any required filter admitted by either route but rejected by the later serializer is an adapter invariant violation.
The adapter MUST fail the query closed before starting the remote scan; it MUST NOT omit that filter and continue. A
generic multi-column conversion failure or a generated rewrite rejected before installation is an ordinary decline
and produces no VGI side effect.

## 7. Internal table-filter wrappers

DuckDB 2.0 represents optimizer-specific filters with private scalar wrappers. The adapter intercepts them before ordinary function translation:

| Wrapper | Adapter behavior |
|---|---|
| `__internal_tablefilter_optional` | Unwrap its child and emit a separate advisory predicate |
| `__internal_tablefilter_selectivity_optional` | Unwrap its child as advisory; do not serialize local pause/backoff state |
| `__internal_tablefilter_dynamic` | When initialized, emit its semantic comparison as an advisory upsert with stable ID and increasing revision |
| `__internal_tablefilter_bloom_filter` | Emit a root runtime artifact only after `duckdb.runtime_filter/bloom@1` capability matching; otherwise omit |
| `__internal_tablefilter_prefix_range` | Emit a root runtime artifact only after `duckdb.runtime_filter/prefix_range@1` capability matching; otherwise omit |

Names beginning `__internal_tablefilter_` never become VGI `call` names. An unrecognized internal wrapper causes required pushdown to be declined atomically. For advisory input, the adapter omits the affected predicate unless it can extract a separately safe top-level conjunct.

Optional and selectivity-optional status belongs to the predicate envelope, not the expression semantics. Dynamic state changes use the VGI delta envelope; mutable DuckDB state pointers are never serialized.

## 8. Runtime filters

Runtime artifact transport is defined separately in [VGI Runtime-Filter Artifacts](vgi-runtime-filter-artifacts.md).

The adapter recognizes Bloom and prefix-range wrappers now, but the current native DuckDB objects expose no stable immutable export/import API. Initial VGI 2.0 implementations therefore omit them and retain the exact join.

Before those exporters exist, the adapter's join-runtime output is limited to DuckDB's exposed min/max narrowing and
the exact `IN` filter DuckDB generates when an equality-join build has more than one key and no more than the effective
`dynamic_or_filter_threshold` value (default 50). It does not enumerate a larger hash table into tick metadata, and
`InitRequest.join_keys` cannot be added to after initialization. Larger build sets therefore produce no VGI exact-set
update.

When a conforming exporter becomes available, the adapter may emit a root advisory `runtime_filter` only after the worker advertises the exact algorithm version. It includes the exact pre-lookup cast chain and correct NULL pass/reject behavior. It never copies object pointers or undocumented native buffers.

## 9. Required and advisory decisions

The adapter decides mode from DuckDB's actual planning contract:

- delegated exact single-column filter: `required`, only if fully encodable and supported;
- non-optional filters installed by a DuckDB rewrite whose local residual was removed: `required`, with every filter
  produced by one rewrite admitted and serialized atomically;
- partial multi-column pushdown: `advisory`;
- optional/selectivity filter: `advisory`;
- join- or Top-N-derived dynamic filter: `advisory`; and
- Bloom or prefix-range artifact: always `advisory`.

Unsupported required translation is declined before DuckDB delegates the predicate, so DuckDB evaluates the original
predicate. It does not send a weakened expression as required. On a DuckDB build without a generated-filter admission
gate, a failure discovered after a specialized rewrite has already delegated the predicate instead fails the query
closed as described in section 6.

A top-level `AND` may yield separate advisory conjuncts only when each emitted conjunct is independently necessary and
non-throwing. The adapter MUST consult DuckDB's bound `Expression::CanThrow()` classification, or a conservative
equivalent, and MUST NOT emit a throwing conjunct separately when the original `AND` contains more than one filter.
For `x <> 0 AND 10 / x > 1`, it may emit `x <> 0` but not `10 / x > 1`. The latter remains local unless the complete
unsplit conjunction can be delegated with DuckDB-equivalent evaluation order and error behavior. Partial extraction
from `OR`, `NOT`, comparison, call, or runtime-filter input is forbidden.

## 10. Projection behavior

All referenced columns must be available while applying the predicate, even when absent from the user's final projection. The adapter coordinates filter columns with DuckDB's scan projection mapping and the worker's `projection_ids`.

Evaluation occurs before final projection. Projection reorder, duplicate-looking names, row identifiers, and hidden filter-only columns require explicit tests.

## 11. Framework and generated-code boundaries

The protocol schemas generate the AST, predicate-envelope, capability, and runtime-artifact reference models. DuckDB-specific visitors and wrapper recognition remain handwritten adapter logic because they depend on DuckDB headers and version-specific bound representations.

Generated VGI models should be used for request, response, and AST construction. The adapter should not maintain a parallel handwritten wire hierarchy for DuckDB filter classes.

## 12. Test plan

The DuckDB 1.5 integration suite covers:

- differential behavior against upstream DuckDB v1.5.5 and Haybarn v1.5.5-rc1 for the complete v1 conformance corpus;
- every concrete supported filter class;
- required versus optional behavior;
- nested struct fields;
- inline and external `IN` sets, including NULLs and empty sets;
- an already-admitted required `IN` set exceeding the configured transport limit, which must fail closed before any
  remote scan;
- projection reorder and omitted filter columns;
- buffered table-in-out scans with reordered projections and filter-only columns, proving that original bind-schema
  indexes are not confused with projected positions;
- unsupported expression fallback;
- throwing-conjunct preservation, including division-by-zero and failing-cast guards; and
- Bloom omission with correct join results.

The DuckDB 2.0 suite additionally covers:

- two- and three-column expressions;
- every `column_indexes` mapping path;
- `filter_pushdown = true` with `pushdown_expression` forced to return `false`, proving that fully pushed prefix,
  exact-LIKE, singleton-`IN`, dense-range-`IN`, and equivalence-map filters still reach the table scan;
- acceptance and exact serialization of every required filter produced by those callback-bypassing rewrites, across
  all admitted types, collations, and evaluation contexts;
- atomic admission of two-bound prefix and dense-`IN` rewrites, with rejection of either bound retaining the original
  residual and installing neither bound when a generated-filter gate is available;
- disabling `filter_pushdown` when stock DuckDB is used and total coverage of its required generated filters cannot be
  guaranteed;
- `vgi.none.v1` emission with no DuckDB session fields when `icu` is not loaded;
- rejection of a context-dependent predicate rather than fabrication of UTC or Gregorian defaults when `icu` is not
  loaded;
- `vgi.duckdb.session.v1` capture from the parsing and binding `ClientContext`, worker-side `icu` loading, and
  application of all five settings before parsing and binding the remote evaluator;
- both values of `ieee_floating_point_ops`, covering floating division and modulo by zero and math-domain behavior;
- both values of `integer_division`, checking both the value and result type of integer `/`;
- enabled `null_on_division_by_zero`, proving that every affected filter is retained locally rather than encoded;
- dynamic equality-join builds at 1, 2, the effective `dynamic_or_filter_threshold`, and threshold plus 1 keys,
  including a nondefault threshold and zero;
- no inline set, external-set reference, or artifact for a build above the threshold, while any exposed min/max
  narrowing remains advisory;
- default semantic-profile operation across different provider fingerprints, plus opt-in strict success and mismatch
  fallback;
- equivalence between the optimize-time `BoundColumnRefExpression` tree and the installed
  `BoundReferenceExpression` plus `column_indexes` representation;
- identical accept/reject results from the callback and serializer eligibility logic for every supported and
  unsupported node, function, overload, type, collation, and evaluation context;
- repeated callback invocations with no mutation, emitted payload, predicate-ID allocation, or other observable
  planning state;
- callback acceptance followed by a multi-column conversion decline, with no filter transmitted;
- injected serialization failure after either generic or generated required-filter admission, which must fail the
  query before the remote scan rather than run unfiltered;
- `COMPARE_IN` and null-check operators;
- function-backed comparisons and casts;
- all five known internal table-filter wrappers;
- dynamic initialization, replacement, duplicate and stale revisions;
- prefix-range and Bloom omission without exporters;
- capability-gated artifact export when implementations become available;
- no false negatives for every advertised runtime algorithm; and
- no private `__internal_tablefilter_*` name in serialized output.

Both suites include negative tests for unsupported required descendants, partial `OR`, malformed bind-schema mappings,
collations, cast differences, and version/capability mismatch. For every expression accepted as required, a contract
test MUST construct the installed `ExpressionFilter` and prove that full serialization succeeds.

## 13. Source audit basis

The adapter design was checked against upstream DuckDB `v1.5.5`
(`d8cdaa33fda8df955cc76ef58a280f68f4cd43fa`), Haybarn `v1.5.5-rc1`
(`105edd31b57fe7e6d32ee648efad562e45a9f908`), and the local 2.0 checkout through
`03a1639e3bba0e50bab4c341ebc89137c86b793a`.

The relevant current control flow is split across `pushdown_get.cpp`, which calls `GenerateTableScanFilters` before
generic-expression pushdown and removes generic `PUSHED_DOWN_FULLY` residuals, and `filter_combiner.cpp`, which removes
specialized-rewrite residuals inside `GenerateTableScanFilters` without consulting the table function's
`pushdown_expression` callback.

The local 2.0 settings definitions make `ieee_floating_point_ops` and `integer_division` Boolean session settings with
defaults `true` and `false`, respectively. DuckDB binds floating division/modulo callbacks from the former and changes
the parser's `/` interpretation from the latter. `null_on_division_by_zero` affects division and modulo too, but its
setter emits DuckDB's deprecation warning, so the adapter declines affected filters when it is enabled.

Both upstream v1.5.5 and the local 2.0 checkout define `dynamic_or_filter_threshold` with default 50. In both,
`JoinFilterPushdownInfo::CanUseInFilter` requires an equality comparison and a hash-table cardinality greater than one
and no greater than the effective threshold.

Relevant DuckDB changes include:

- `8726f24076` — use `ExpressionFilter` and scalar functions for extensible filters;
- `3d94704c6d` — rename old table-filter enum values to `LEGACY_*`;
- `270a73ecc9` — implement internal table-filter scalar functions;
- `c07dd75a17` — add prefix-range filtering;
- `3b0d794f1c` — represent comparisons as bound scalar functions;
- `026e44e376` — represent casts as bound scalar functions; and
- `ef1a7ca225` — support multiple columns in expression filters.

The enum comparison found one new named kind relative to 1.5: prefix range. Bloom, dynamic, optional, selectivity-optional, `IN`, null, conjunction, struct, constant-comparison, and generic expression filtering existed in 1.5 but changed active representation in 2.0.
