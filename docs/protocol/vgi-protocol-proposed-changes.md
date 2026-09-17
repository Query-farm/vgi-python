# Proposed VGI protocol changes for DuckDB 2.0

**Status:** design/audit report; the argument-monotonicity subsection records an accepted VGI 2.0 protocol addition
**Audit date:** 2026-09-09
**Filter-v2 decision:** 2026-09-09 — mandatory in VGI 2.0; no v1 compatibility path
**DuckDB range examined:** `v1.5.5..03a1639e3bba0e50bab4c341ebc89137c86b793a` (`v2.0-cyanoptera` plus the multi-column table-function pushdown branch)
**VGI examined:** `588bb2fd99de6a58461b3dfbbf26cc604da4c94e`
**vgi-python examined:** `97b68f0f42d560bbc00947eddf39370ca167aa2d`

**Function-signature decision:** 2026-09-10 — named fixed parameters,
authoritative typed defaults, and bind-time resolved names are part of VGI 2.0;
there is no protocol overload identifier.

## Executive answer

Nested schemas are **not** the only possible VGI protocol change.

The audit found three protocol areas that deserve action, plus several optional 2.0 capabilities:

| Area | Recommendation | Required for correctness? | Compatibility impact |
|---|---|---:|---|
| Qualified schema paths | Replace every scalar `schema_name` that identifies an object owner with a component list | Yes, if VGI is to support nested schemas | Breaking; VGI protocol 2.0 |
| Filter expression encoding | Replace v1 with the mandatory `vgi.filters.v2` expression AST | Yes for general DuckDB 2.0 multi-column pushdown | Breaking; VGI protocol 2.0 only |
| Runtime pruning artifacts | Add a capability-gated, root-only advisory `runtime_filter` node for versioned Bloom and DuckDB 2.0 prefix-range artifacts | No for correctness; required to transport these optimizer hints | Additive within mandatory filter v2 |
| Logical-type identity | Define a VGI annotation for DuckDB types that plain Arrow cannot distinguish, initially `TUPLE` and potentially aggregate-state extension types | Required only if those types cross the VGI boundary | Additive if negotiated; breaking if made universal |
| Optimizer metadata | Optionally expose repeatability, projection-expression pushdown, partition selection, and typed metrics | No; leaving callbacks unset is conservative/correct | Additive capabilities |
| Triggers and new catalog kinds | Explicitly reject or advertise unsupported; design a separate protocol only if VGI intends to virtualize them | No | Separate feature proposal |

My confidence is **high that the audit has found the DuckDB 2.0 changes that alter VGI's current wire semantics**, but not absolute: the examined DuckDB branch is still moving. The audit should be rerun against the 2.0 release candidate or final tag. Most of the large C++ migration is API adaptation inside the extension and does not require a protocol change.

VGI protocol 2.0 will not carry a v1 compatibility path. A 1.x peer is rejected during protocol negotiation. The DuckDB 1.5 extension can still implement the v2 wire format; its engine adapter simply produces the subset of the v2 AST that DuckDB 1.5 can offer.

## Accepted function-signature contract

DuckDB 2.0 can bind named arguments and defaults for scalar, aggregate, and
window functions. VGI models the portable semantics rather than exposing a
DuckDB overload handle:

1. For scalar and aggregate functions, fields in `FunctionInfo.arguments` are
   ordered fixed signature slots and each field name is the SQL/VGI parameter
   name. `vgi_arg=named` remains reserved for table-function-style named-only
   options. `vgi_const` is orthogonal to naming.
2. `FunctionInfo.parameter_default_values` is a nullable IPC-serialized
   `RecordBatch`. When present it has exactly one row and contains only
   defaulted parameters, in signature order. Column absence means required/no
   default; a present null scalar means an explicit `NULL` default. Each column
   type exactly matches its argument field. Scalar and aggregate defaults form
   a trailing sequence of fixed parameters, and varargs cannot have defaults.
3. `BindRequest.argument_names` and `AggregateBindRequest.argument_names` are
   nullable `list<utf8>` values aligned with the complete logical argument
   order before constant arguments are separated for execution. Fixed slots
   carry their declared names, unnamed varargs carry null elements, and named
   varargs retain caller-provided names. A null list means the engine cannot
   provide resolved names.

The legacy `vgi_default` field metadata is discovery-only and may be removed in
a later protocol. It is never authoritative when the typed batch is present.
Implementations select behavior by inspecting the bound arguments, types, and
resolved names; VGI does not add `overload_id`.

The DuckDB 1.5 adapter implements this VGI 2.0 wire shape now. It advertises and
validates typed defaults and synthesizes fixed names plus null unnamed-vararg
entries. Because DuckDB 1.5 cannot register scalar/aggregate named calls or
engine-applied defaults, SQL callers must still provide those values
positionally. A DuckDB 2.0 adapter can map the same metadata to
`FunctionSignature` without changing the worker protocol. DataFusion, Spark,
and SQLite adapters should normalize their native call forms to the same bind
representation.

## 1. Protocol version and rollout

At the start of the audit, the application protocol declared `1.5.0`. It now declares `2.0.0` in the canonical `vgi-python/vgi/protocol_version.txt`. The documented version rules treat a parameter or return-type change as a major change, the framework enforces an exact major/minor match, and the C++ consumer rejects response column-count drift.

Therefore, if schema identifiers change from `utf8` to `list<utf8>`, the clean protocol version is:

```text
VGI application protocol: 2.0.0
Filter expression encoding: vgi.filters.v2
```

The filter label remains embedded in the payload as a validation and diagnostic marker, but it is not independently negotiated in VGI 2.0. A VGI 2.0 implementation accepts only `vgi.filters.v2`.

Recommended deployment strategy:

1. Require an exact VGI 2.0 application-protocol match at attach/dispatch.
2. Remove the v1 filter encoder and decoder from every 2.0 SDK rather than carrying a dual representation.
3. Make `vgi.filters.v2` the fixed encoding of every non-null `InitInput.filters` payload and every dynamic filter update.
4. Remove `ClientCapabilities.filter_encodings`; support for the v2 encoding follows from support for VGI protocol 2.0.
5. Keep function-level filter capabilities so a function can decline filter pushdown or restrict optional function-call nodes.

## 2. Required change: qualified schema paths

### Why the current model is insufficient

DuckDB 2.0 represents a qualified object as components:

```text
[catalog, schema, nested_schema, ..., object]
```

Relevant changes include `QualifiedName`, full-name `EntryLookupInfo`, qualified `CreateInfo`/`DropInfo`/`AlterInfo`, `SchemaCatalogEntry::GetParentSchema()`, `GetSchemaPath()`, and `GetQualifiedName()`. The implementation and parser now support deeply nested schemas rather than treating a schema as a single string.

VGI currently models the same information as one scalar string in many places:

- `BindRequest.schema_name`
- `CatalogSchemaObject.schema_name`
- `SchemaInfo.name`, which has no parent or path
- roughly 35 catalog interface methods taking `schema_name`
- `ScanFunctionResult`/`WriteFunctionResult.schema_name`
- function branches in `ScanBranch.schema_name`
- `ScanBranch.source_schema` for a cross-catalog table branch
- `ForeignKeyDef.referenced_schema`
- aggregate, buffering, window, and streaming dispatch requests that repeat the function's schema
- C++ catalog RPC builders, metadata objects, cache keys, and registries derived from those fields

This is broad rather than cosmetic. The audit counted 66 `schema_name` occurrences in `vgi-python/vgi/protocol.py`, 109 in `catalog_interface.py`, 44 generated Arrow fields named `schema_name`, and 33 occurrences in the C++ catalog RPC declaration header.

### Proposed wire model

Use raw identifier components, not a dotted SQL string:

```python
SchemaPath = list[str]

@dataclass(frozen=True)
class QualifiedObjectRef:
    schema_path: SchemaPath
    name: str
```

The corresponding Arrow field is `list<utf8>` (non-null where an owning schema is required). The attached DuckDB catalog is implicit in normal catalog RPCs. Cross-catalog scan branches retain `source_catalog` and replace `source_schema` with `source_schema_path`.

Normative identifier rules should say:

- Each array element is exactly one unquoted identifier component.
- Preserve the component's original spelling.
- Comparisons follow DuckDB identifier semantics, not ordinary case-sensitive string equality.
- A component may contain `.`; the dot has no separator meaning on the wire.
- Empty paths are allowed only where DuckDB genuinely permits an object without an owning schema.
- Do not carry both `schema_name` and `schema_path` as simultaneous sources of truth in the final v2 schema.

Using strings for individual components is adequate. DuckDB's new `Identifier` C++ type does not by itself require a structured wire type; the important part is retaining boundaries between components.

### Schema enumeration and mutation

`SchemaInfo` should return a canonical `path`, not only a leaf `name`:

```python
@dataclass(frozen=True)
class SchemaInfo:
    path: list[str]
    comment: str | None = None
    # existing metadata follows
```

For the existing eager `catalog_schema_list` method, return parents before children. That makes the response deterministic and lets clients build the hierarchy without additional round trips. A later lazy child-list method is an optimization, not a requirement.

Every schema operation must accept a path:

- create schema: parent path plus new leaf, or the complete new path
- get/exists/list children
- alter/rename/comment
- drop

The specification must define rename semantics for descendants and cache invalidation. The simplest rule is that a schema rename changes the prefix of every descendant object's qualified path atomically within the catalog transaction.

### Object and function operations affected

The path conversion applies consistently to:

- tables and views
- scalar, aggregate, table, table-in/out, and buffering functions
- macros and table macros
- indexes
- COPY functions/formats registered in a schema
- table insert/update/delete function lookup
- statistics and cardinality lookup
- comments, create, alter, and drop operations
- function bind/init and all secondary aggregate/function callbacks
- scan branch descriptions and foreign-key targets

The worker registry and C++ caches should use a structural key such as `(tuple(schema_path), object_name)`, never a joined display name.

### Default schema does not need to become a path yet

`CatalogAttachResult.default_schema` can remain one identifier for now. DuckDB's current nested-schema implementation does not accept a nested schema as the ordinary default/search-path entry. The C++ return type becoming `optional<Identifier>` does require local adaptation, but it does not force a VGI wire change unless VGI wants to represent “no default schema”; the current VGI default of `main` remains valid.

## 3. Required change: filter encoding v2

### Why `vgi.filters.v1` cannot represent the new semantics

VGI currently advertises only `vgi.filters.v1`. Its top-level filter representation identifies one root column. `ColumnRefNode.index` is documented as reserved for future use, while evaluation resolves all column references through that root column.

DuckDB 2.0 has converged scan filters on `ExpressionFilter` and now supports arbitrary expressions referring to multiple columns. Relevant commits include:

- `88b5dbcea4` — unify remaining table filters with `ExpressionFilter`
- `30370c11f7` — add the row-group expression filter structure
- `ef1a7ca225` — support multiple columns in expression filters
- `c85c253088` — prune row groups with cross-column comparisons
- `728402e61b` — support string types for multi-column pushdown

The direct `v1.5.5` comparison also shows that DuckDB 2.0 has one newly added named table-filter kind: `PREFIX_RANGE_FILTER`, retained in the current enum as `LEGACY_PREFIX_RANGE_FILTER = 12` after active filters were unified. The broader 2.0 work is a representation change rather than a proliferation of enum kinds:

- `8726f24076` moves extensible filters to `ExpressionFilter` plus scalar functions;
- `3d94704c6d` renames the old enum entries to `LEGACY_*`;
- `270a73ecc9` implements the internal table-filter scalar wrappers;
- `c07dd75a17` adds the prefix-range runtime filter;
- `3b0d794f1c` converts bound comparisons to bound scalar functions; and
- `026e44e376` converts bound casts to bound scalar functions.

Bloom, dynamic, optional, selectivity-optional, `IN`, null, conjunction, struct, constant-comparison, and generic expression filters already existed in 1.5. Their 2.0 forms still require adapter changes because active execution now presents semantic expression trees and private internal wrappers instead of the former concrete filter classes.

Reusing the v1 `index` field with a new meaning would be a silent breaking change. An old worker could evaluate a multi-column predicate against the wrong column, which is worse than losing pushdown. Because VGI 2.0 already has a breaking application-protocol boundary, v1 should be removed instead of negotiated.

### Proposed `vgi.filters.v2`

Use one canonical expression AST rather than v1's split between column-scoped filter objects and an expression subtree. The payload contains a list of predicate records; the list is an implicit `AND`. Every predicate has a stable ID, an application mode, and one expression root:

```json
{
  "predicates": [
    {
      "id": "static:0",
      "mode": "required",
      "revision": 0,
      "expr": {
        "type": "comparison",
        "op": "lt",
        "left": {"type": "column_ref", "column_index": 1, "column_name": "start"},
        "right": {"type": "column_ref", "column_index": 2, "column_name": "end"}
      }
    }
  ]
}
```

Every column-reference node contains:

```text
column_index: int32
column_name: utf8
```

Define `column_index` as an index into the **unprojected `BindResponse.output_schema`**, before `InitRequest.projection_ids` is applied. This identity stays stable when DuckDB reorders or adds scan projections. The engine adapter must map DuckDB's `ProjectionIndex` through the scan's column-index list before serialization. The worker maps the schema index back through `InitRequest.projection_ids` when applying a predicate to a projected output batch.

`column_name` is redundant by design. The receiver must validate that index and name agree with the bound schema before evaluating the predicate. The index is authoritative; names are never searched as a substitute. This also handles duplicate-looking or case-varied names deterministically.

### Mandatory core nodes

All VGI 2.0 SDKs must decode and evaluate the same portable core:

- `column_ref(column_index, column_name)`
- `literal(value_ref)`, where the referenced Arrow column preserves the exact value and type
- `comparison(eq, ne, lt, le, gt, ge, distinct_from, not_distinct_from)`
- `and`, `or`, and `not`, using SQL three-valued Boolean semantics
- `is_null` and `is_not_null`
- `in` and `not_in`, including specified NULL behavior
- `field_ref(parent, child_index, child_name)` for nested struct/tuple access
- `cast(child, type_ref)` where `type_ref` names a canonical VGI logical-type descriptor
- `call(function, arguments)` using a readable standard name or an explicitly advertised namespaced extension identity

V2 additionally defines a capability-gated `runtime_filter` node for one-sided pruning artifacts. It is not part of the mandatory exact core: it may appear only as the root of a separate advisory predicate, may never be negated, and always retains the exact local residual. The node identifies a versioned algorithm, an input expression, an `artifact_N` Arrow payload, and explicit NULL pass/reject behavior. Unsupported known algorithms cause the entire advisory predicate to be ignored.

The existing Python `ConstantFilter`, `InFilter`, `StructFilter`, and similar classes may remain as convenience APIs, but they become views/builders over this AST. They are no longer distinct wire node families. `ColumnRefNode.to_sql(root_column_name)` must disappear: every column reference resolves its own schema index, and SQL rendering—when used—must bind generated aliases rather than interpolate a caller-provided root name.

Raw parser spellings and aliases are not stable function identities. V2 normalizes supported DuckDB expressions into
readable standard names such as `starts_with`, defined by the selected `vgi.duckdb.standard.v1` semantic profile.
Every snapshot and delta carries `semantics: "vgi.duckdb.standard.v1"`, and workers advertise supported profiles in
`filter_semantic_profiles`. Nonstandard calls use a structured namespace, name, and semantic version. Remove
`supported_expression_filters` in favor of `filter_semantic_profiles` plus `additional_filter_functions`; the producer
emits a call only when the worker advertised its exact semantic contract.

The filter proposal is split by responsibility:

- [`vgi-filter-encoding-v2-spec.md`](vgi-filter-encoding-v2-spec.md) is the compact engine-neutral normative wire contract;
- [`vgi-runtime-filter-artifacts.md`](vgi-runtime-filter-artifacts.md) defines the optional advisory artifact extension and the DuckDB Bloom/prefix-range candidates; and
- [`vgi-duckdb-filter-adapter.md`](vgi-duckdb-filter-adapter.md) records the version-specific DuckDB 1.5/2.0 mapping and test plan.

The core specification uses embedded DuckDB as the portable reference evaluator. `vgi.duckdb.standard.v1` pins core
nodes and standard functions to upstream DuckDB v1.5.5 commit
`d8cdaa33fda8df955cc76ef58a280f68f4cd43fa`; Haybarn v1.5.5-rc1 commit
`105edd31b57fe7e6d32ee648efad562e45a9f908` is a conforming reference distribution. Stable DuckDB 2.0 behavior will
be introduced as `vgi.duckdb.standard.v2` rather than changing v1. Standard calls use readable generated names such as
`starts_with` without repeating signatures; nonstandard calls use a structured namespace, name, and semantic version;
DataFusion may translate supported subsets for deeper pushdown while retaining an exact residual; Polars is primarily
a client and retains its native filter. Substrait is not a protocol dependency.

### Required versus advisory predicates

The wire contract must reflect what the engine guarantees:

- `mode: "required"` means the engine has delegated exact evaluation to the worker. A worker must apply the complete expression with SQL WHERE semantics or fail the query. It must never silently skip an unknown node or child.
- `mode: "advisory"` means the engine retains an exact local operator. The worker may use the expression for pruning and may ignore it safely. DuckDB 2.0 multi-column filters use this mode because the upstream pushdown path returns `PUSHED_DOWN_PARTIALLY`.
- Dynamic Top-N and join-derived pruning updates are advisory unless the engine explicitly preserves an exact residual by another mechanism.

This replaces the earlier proposed `filters_exactly_applied` response. A streaming worker cannot safely announce non-application after DuckDB has already planned away a required filter. Exactness is therefore a function-registration and per-predicate input contract, not a late response flag.

The current v1 serializer needs an explicit audit during replacement: skipping an unsupported child from `AND` weakens the predicate, while skipping one from `OR` strengthens it. Neither transformation is an exact encoding. V2 serialization is atomic per required predicate: serialize the complete tree or do not let DuckDB delegate that predicate.

### Transport and dynamic updates

Keep the efficient hybrid transport: JSON carries AST structure and Arrow columns carry typed literals or large value sets. Move the marker to schema metadata as `vgi_filter_encoding=vgi.filters.v2`; the decoder accepts no other value.

Use the same envelope for initial filters and tick-time updates. `id` identifies the logical predicate and `revision` is monotonically increasing. A worker replaces an older advisory predicate only when a newer revision for the same ID arrives, ignores duplicate/stale revisions, and must not identify updates by array position. Result-cache keys include the canonical required predicate encoding; a scan with changing advisory predicates remains ineligible unless the existing cache policy proves independence from those hints.

The v2 marker belongs to the filter `RecordBatch` schema metadata (`batch.schema.metadata`), not to metadata on the `filter_spec` field. Filter v1 attached its version to field zero; the VGI 2.0 break is the appropriate time to put document-level metadata at document scope.

V2 retains VGI's special transport for `IN` element sets. Small sets live as one typed Arrow list scalar referenced from the AST; large exact sets remain separate typed Arrow `join_keys` batches referenced by batch and column index plus a validating name. Size thresholds choose the transport but never change semantics, and a set is declined rather than truncated. Correlated composite keys must not be represented as independent required `IN` filters.

Runtime artifacts use a dedicated capability list rather than pretending to be ordinary functions:

```text
runtime_filter_algorithms: list<RuntimeFilterAlgorithmCapability>
```

Candidate algorithm identities are `duckdb.runtime_filter/bloom@1` and `duckdb.runtime_filter/prefix_range@1`. Their contracts must specify the immutable Arrow/binary layout, endianness, checksum and bounds validation, supported types, hashing or prefix conversion, construction parameters, exact pre-lookup casts, NULL policy, and false-negative prohibition.

DuckDB's current native Bloom and prefix-range objects are not yet transportable. Both own private process-local state and expose no stable immutable export/import contract. The VGI 2.0 envelope now supports them without another AST change, but an implementation MUST NOT advertise either candidate identity until its artifact contract and exporter/evaluator exist. Exact inline/external key sets and exposed min/max expressions remain the portable initial behavior.

For DuckDB join-runtime pushdown specifically, the initial exact-set contribution is capped by the effective
`dynamic_or_filter_threshold` setting (default 50): DuckDB generates an `IN` filter only for an equality-join build
with more than one and no more than that many keys. Above the threshold, VGI receives only independently exposed
min/max narrowing or no runtime predicate. `InitRequest.join_keys` is fixed at initialization and is not a large
dynamic-set update channel.

Other requirements:

- Canonicalize commutative nodes and predicate ordering before hashing/cache-key construction, without changing evaluation order where errors or volatile calls could be observable.
- Reject malformed indexes, index/name disagreement, missing literal/type references, unknown required nodes, and unsupported required functions with a protocol error.
- Preserve SQL NULL behavior; the auto-apply path keeps only rows for which the final predicate is `TRUE`.
- Bound recursion depth, child count, JSON size, literal bytes, and join-key bytes before allocation.
- Never serialize volatile, side-effecting, unencoded session-dependent, or unversioned engine-specific functions.

### DuckDB 1.5 and 2.0 adapters

The current DuckDB 1.5 extension should switch directly to v2. It emits required single-column predicates and v2 expression nodes for the subset its APIs expose. It must still use unprojected bind-schema indexes on the wire, not current projected batch positions.

The later DuckDB 2.0 port treats active scan filters as `ExpressionFilter`, walks `TableFilterSet::GetMultiColumnFilters()`, maps every `ExpressionFilter.column_indexes` entry to the bind schema, and emits partial multi-column predicates as advisory. The upstream enabling change is [DuckDB PR #25546](https://github.com/duckdb/duckdb/pull/25546).

It must translate semantics rather than old C++ class tags: `COMPARE_IN` becomes VGI `in`; null checks and conjunctions become their core nodes; and comparisons and casts are recognized from their bound scalar-function identities. The adapter must intercept `__internal_tablefilter_optional`, `__internal_tablefilter_selectivity_optional`, `__internal_tablefilter_dynamic`, `__internal_tablefilter_bloom_filter`, and `__internal_tablefilter_prefix_range`. Optional wrappers become advisory predicates, initialized dynamic filters become stable-ID/revision updates, and Bloom/prefix-range wrappers become root runtime artifacts only after exact algorithm capability matching. No `__internal_tablefilter_*` name is ever forwarded as a VGI function call.

## 4. Conditional change: preserve DuckDB logical-type identity

DuckDB 2.0 adds or reworks logical types relevant to an Arrow-based boundary:

- `TIMESTAMP_TZ_NS`
- `TUPLE`, an unnamed struct with positional members
- aggregate states, now represented using extension type information rather than the old special `AGGREGATE_STATE` model (`LEGACY_AGGREGATE_STATE` remains as the old ID)

### What plain Arrow preserves

`TIMESTAMP_TZ_NS` needs tests but no new protocol shape. DuckDB exports it as Arrow nanosecond timestamp with a timezone (`tsn:<timezone>`) and imports that format back as `TIMESTAMP_TZ_NS`.

### What plain Arrow loses

DuckDB currently exports both `STRUCT` and `TUPLE` with Arrow's `+s` struct format. For a tuple it synthesizes child names such as `element1`. The Arrow importer maps `+s` back to `LogicalType::STRUCT`. Consequently, a DuckDB 2.0 `TUPLE` crossing the current VGI Arrow IPC boundary returns as a named `STRUCT`; its logical identity and SQL/JSON behavior are lost.

That is a protocol issue if VGI promises round-trip support for every DuckDB logical type. It is not a blocker if VGI explicitly rejects `TUPLE` at bind/catalog boundaries.

### Proposed type annotation

Add a versioned, recursive logical-type annotation for cases where Arrow storage type is ambiguous. Two viable designs are:

1. An Arrow extension annotation on the affected nested field, for example `vgi.duckdb.tuple`, whose storage type is `struct`.
2. A parallel serialized VGI logical-type descriptor for each schema field and nested child.

Prefer the Arrow extension form if a round-trip spike confirms that Python, C++, Rust, and Go preserve the metadata on nested fields without requiring a registered PyArrow extension class. The descriptor must be recursive; top-level schema metadata alone is insufficient for a tuple nested inside a list or struct.

Do not use a DuckDB SQL type string as the sole interchange format unless its quoting, aliases, collations, extension parameters, and version stability are made normative.

Aggregate-state types need their own decision. A remote worker cannot safely manufacture or consume a DuckDB aggregate state merely because its Arrow storage type is known. Until a portable state contract exists, advertise them as unsupported across VGI. If support is desired, the descriptor must include the aggregate/function identity and parameter types and the attach handshake must confirm that both peers implement the same state serialization.

Recommended type capability fields:

```text
logical_type_encodings: ["arrow", "vgi.duckdb.logical-types.v1"]
supported_logical_extensions: ["tuple"]
```

Unsupported/ambiguous types should fail during bind or catalog materialization, not after rows have started streaming.

## 5. New DuckDB 2.0 contracts that do not require a wire change

These changes require C++ porting or offer optimization opportunities, but the current VGI protocol can remain correct without exposing them.

### Projection-expression pushdown

DuckDB replaces the older type-pushdown hook with `projection_expression_pushdown`, which can offer expressions such as casts or `length(column)` to a scan. VGI currently sends only `projection_ids`.

Leave the callback unset for the initial port. If implemented later, reuse a safe subset of the v2 expression AST and add `projection_expressions` as a negotiated optional field. The worker must return the declared expression result type and DuckDB must retain local evaluation when the worker declines it.

### Repeatability

`TableFunction::is_repeatable` tells the optimizer whether executions with identical bind data are stable within one query. If the callback is absent, DuckDB treats the function conservatively as not repeatable. That is correct.

A future `repeatability` metadata value could improve plans, but it must be defined separately from scalar-function stability. A remote table may change between calls even when its schema and arguments do not. Suggested values are `unknown` and `stable_within_query`; do not infer repeatability merely from a read-only-looking function.

### Partition selection

DuckDB adds `set_partitions_to_scan`, complementing partition statistics. VGI already carries split tokens and partition metadata. Initially, apply selected partition indices locally when building/redeeming the existing split list; no new RPC is inherently necessary. Add a selected-partition field only if selection happens after an immutable remote plan has already been issued.

### Parallelism classification

DuckDB now distinguishes self-managed parallelism, externally parallelizable sequential sources, and forced single-threaded sources. VGI already has `max_workers`, ordering, batch-index, splits, and sink/source ordering metadata. Map these conservatively in C++. A new explicit `parallelism` field is optional and should be introduced only if the current fields cannot express a real worker mode.

### Metrics

DuckDB replaces the old dynamic-to-string hook with `get_metrics` and `OperatorMetrics`. VGI can keep its existing worker telemetry/dynamic-description RPC and add the returned information to DuckDB's extra-info metrics locally. A future typed metric schema is useful but not required for execution correctness.

### Function metadata

DuckDB 2.0 adds optimizer-facing per-argument monotonicity and strengthens
fallibility handling. VGI 2.0 adds the following nullable field immediately
after `FunctionInfo.null_handling`:

```text
argument_monotonicity: list<utf8>?
```

The allowed strings are `UNKNOWN`, `CONSTANT`, `NON_DECREASING`,
`STRICTLY_INCREASING`, `NON_INCREASING`, and `STRICTLY_DECREASING`. This field
is valid only for scalar functions. Null means that the function makes no
claims. A present list contains exactly one non-null entry for every ordered
field in `FunctionInfo.arguments`. Fixed, defaulted, and constant parameters
each occupy a slot; a vararg declaration occupies one slot and its property
applies independently to every expansion. The order is declaration order and
is unaffected by `BindRequest.argument_names` or named call syntax.

The DuckDB 1.5 extension validates and preserves this metadata using VGI-owned
types, but does not apply it to the optimizer. A DuckDB 2.0 adapter can later
map the same values to `ArgProperties` without changing the wire format.

`error_mode` remains out of scope. In particular, VGI must not claim that a
remote function cannot throw unless transport, worker code, and all
data-dependent failures are covered by that guarantee.

## 6. Catalog and DML features to reject explicitly unless separately designed

DuckDB 2.0 adds catalog entry kinds for triggers and standalone window functions. It also expands trigger execution with OLD/NEW row images and transition tables. Physical update/delete operators consequently carry fields such as selected RETURNING columns, captured old rows, and duplicate row-id handling.

The current VGI write design already supports INSERT/UPDATE/DELETE and RETURNING through write table functions. Most corresponding 2.0 changes are local physical-planner integration work, not new worker semantics:

- RETURNING projection can be handled by the VGI physical operator and its existing output schema.
- Duplicate target row IDs from `UPDATE ... FROM` should be deduplicated or rejected on the DuckDB side before remote side effects are issued.
- The C++ `TableIndex`, `DuckTableEntry`, and operator constructor changes are local API migrations.

Triggers are different. Supporting a trigger in a virtual catalog would require a substantial separate contract for timing, event, row/statement granularity, SQL body, dependencies, OLD/NEW rows, transition tables, transactional ordering, and failure semantics. Do not smuggle those fields into the normal update RPC. For the 2.0 port:

- report `TRIGGER_ENTRY` unsupported for a VGI catalog;
- reject `CREATE TRIGGER` against VGI objects with a clear capability error;
- ensure an attached catalog never claims trigger support merely because the DuckDB base interface gained the enum;
- make standalone `WINDOW_FUNCTION_ENTRY` unsupported unless VGI intentionally adds native window-function registration. Existing aggregate window callbacks are a separate feature.

The same principle applies to secure views, composable COPY, remote-plan pushdown, coordinate systems, and any catalog object VGI does not currently virtualize: they are not protocol requirements simply because DuckDB implements them. They should be gated through explicit catalog capabilities and fail early.

The separate [catalog query pushdown design](vgi-catalog-query-pushdown-design.md) now proposes an
optional read-query preparation/execution contract and implementation sequence. It distinguishes
DuckDB 1.5 explicit query execution from automatic pushdown in the inspected 2.0 development API;
the feature is not yet implemented.

### 6.1 Writable result modes and OLD/NEW images

VGI 2.0 replaces the four `TableInfo` booleans `supports_insert`,
`supports_update`, `supports_delete`, and `supports_returning` with one required,
non-null Arrow map:

```text
write_result_modes: map<utf8, utf8>
```

The only operation keys are `insert`, `update`, and `delete`; absence means the
operation is unsupported. Each value is the maximum supported mode in the
ordered lattice `count < rows < changes`, and therefore promises every lower
mode. Duplicate keys and unknown operations or modes are protocol errors.

The serialized `write_options` batch replaces `return_chunks: bool` with the
required `result_mode: utf8`. `count` returns `(count int64 not null)`. `rows`
returns NEW rows for INSERT/UPDATE and OLD rows for DELETE. `changes` returns
two nullable structs, `old` and `new`, each shaped exactly like the table:
INSERT populates only `new`, DELETE only `old`, and UPDATE both. The operation
is known from the invoked write function and is not repeated in each result row.

The DuckDB 1.5 adapter requests `count` for ordinary DML and `rows` for DML with
RETURNING. It validates and preserves `changes` capability metadata but never
requests that mode. A DuckDB 2.0 adapter can request `changes` when trigger
planning needs OLD/NEW images without another protocol revision.

### 6.2 New catalog kinds in DuckDB 2.0

The `CatalogType` enum has exactly two additions relative to DuckDB 1.5.5:

| Kind | Value | Meaning for VGI |
|---|---:|---|
| `TRIGGER_ENTRY` | 11 | A trigger definition owned by a particular base table. Supporting it is valuable and feasible, but requires catalog RPCs and exact transactional DML semantics. |
| `WINDOW_FUNCTION_ENTRY` | 32 | A native `WindowFunctionSet`, registered and bound separately from aggregate functions. VGI's existing aggregate `supports_window` path is not the same abstraction. |

Nested schemas are recursively contained `SCHEMA_ENTRY` objects, not another kind. Secure views remain `VIEW_ENTRY`; changes to aggregate-state representation belong to the logical type system rather than the catalog enum.

### 6.3 How native DuckDB stores triggers

Triggers are first-class `TriggerCatalogEntry` objects, but they do not live in the schema's ordinary catalog set. Each `DuckTableEntry` owns a separate transactional `CatalogSet`:

```text
catalog
└── schema path
    └── base table
        └── trigger CatalogSet
            ├── trigger A
            └── trigger B
```

This makes the effective identity `(catalog, schema_path, table_name, trigger_name)`. Trigger names are unique only within a table, and the same trigger name may occur on two tables. A trigger inherits its base table's catalog, complete possibly-nested schema path, and temporary status.

`TriggerCatalogEntry` stores:

- the base-table reference;
- `BEFORE`/`AFTER`/nominal `INSTEAD OF` timing;
- `INSERT`, `UPDATE`, or `DELETE` event;
- an optional `UPDATE OF` column list;
- `FOR EACH STATEMENT` or `FOR EACH ROW` granularity;
- optional NEW/OLD transition-table aliases;
- the action as a parsed `QueryNode`, not a bound physical plan;
- dependencies, comment, tags, and temporary status.

Native checkpoints write tables before scanning and writing each table's private trigger set. A trigger is serialized as `CreateTriggerInfo`; restore locates the already-restored base table and recreates the trigger in that table's set. The WAL writes the full create definition for `CREATE TRIGGER`, while `DROP TRIGGER` records the trigger's qualified name plus the owning table. The shared trigger set is inherited when an `ALTER TABLE` rebuild produces a replacement `DuckTableEntry`.

The dependency manager has special trigger identity/lookup handling because schema path plus trigger name is insufficient. The base table is an ownership dependency: dropping that table removes the trigger without requiring `CASCADE`. Objects referenced by the trigger action are blocking/alter-blocking dependencies. Dropping such an object requires `CASCADE`; renaming the owning table is currently blocked; and renaming a column in an `UPDATE OF` list rewrites the stored trigger transactionally.

At bind time DuckDB scans the table's triggers and expands them into the DML plan. Statement triggers form a materialized CTE chain (BEFORE actions, base DML, AFTER actions), including materialized OLD/NEW transition tables when requested. Supported row triggers use `LogicalTrigger` and correlated `NEW.col`/`OLD.col` bindings. The current catalog set yields triggers in case-insensitive alphabetical name order.

### 6.4 DuckDB changes needed by custom table implementations

DuckDB 2.0 already exposes virtual `TableCatalogEntry::CreateTrigger`, `ScanTriggers`, and `GetTrigger` hooks, and its trigger binder consumes the generic scan hook. Two remaining native-table assumptions should be fixed upstream:

1. Add virtual `TableCatalogEntry::DropTrigger`, with the default throwing “triggers are not supported for this table type”; override it in `DuckTableEntry`; and make `PhysicalDrop` call the generic table method using `table.ParentCatalog().GetCatalogTransaction(context)`.
2. Make `duckdb_triggers()` collect all `TableCatalogEntry` objects and call their virtual `ScanTriggers` hooks, rather than filtering/casting to `DuckTableEntry`.

These belong in one small upstream PR because together they complete an interface DuckDB has already started to generalize. The default drop implementation must throw rather than return `false`, so `DROP TRIGGER IF EXISTS` cannot disguise an unsupported table kind as an absent trigger.

Upstream PR: [duckdb/duckdb#25539 — Support triggers on custom table catalog entries](https://github.com/duckdb/duckdb/pull/25539). The PR is based on `v2.0-cyanoptera` and implements both changes above.

The `ScanTriggers` contract also implies that returned catalog entries remain alive beyond the callback: both trigger selection and introspection retain references. VGI should materialize trigger entries in a table-owned metadata cache, invalidated through the existing catalog-version mechanism, rather than returning callback-local objects.

### 6.5 Proposed VGI trigger protocol

The remote VGI catalog should be authoritative for persistent trigger definitions. Keeping definitions only in a local extension cache would lose them on detach and would give different clients inconsistent catalogs.

Suggested identity and definition:

```text
TriggerRef
    schema_path: list<string>
    table_name: string
    trigger_name: string

TriggerInfo
    ref: TriggerRef
    timing: before | after
    event: insert | update | delete
    update_columns: list<string>
    for_each: statement | row
    referencing_new_table: optional<string>
    referencing_old_table: optional<string>
    definitions: map<string, string>
    dependencies: list<TriggerDependency>
    comment: optional<string>
    tags: map<string, string>
```

Use structured fields plus engine-specific textual definitions keyed by stable
lowercase engine identifiers such as `duckdb`, `datafusion`, `spark`, and
`sqlite`. An engine executes only its matching definition; a missing entry means
that trigger is unavailable on that engine, with no implicit SQL-dialect
fallback. Do not put DuckDB's binary `QueryNode` serialization on the VGI wire:
it is an internal, version-coupled representation. The DuckDB extension parses
the `duckdb` definition and creates cached `TriggerCatalogEntry` objects. Each
resolved dependency should carry its qualified object reference plus ownership,
drop-blocking, and alter-blocking flags so the remote catalog can enforce DDL
integrity across clients.

Suggested methods:

- `catalog_table_trigger_list`
- `catalog_table_trigger_get`
- `catalog_table_trigger_create`
- `catalog_table_trigger_drop`
- optional trigger comment/tag alteration

Add explicit catalog/table capability metadata such as `supports_triggers`. A
DuckDB-managed trigger-capable table must advertise `changes` for every event it
uses, because DuckDB's statement-trigger expansion needs exact OLD/NEW images
even when the user did not write a RETURNING clause.

All base and trigger-body writes must use the same VGI transaction token and roll back atomically. Stable statement/sub-operation identifiers are recommended so transport retries cannot apply a trigger side effect twice. Trigger support should be rejected when the VGI catalog cannot provide transactional atomicity.

Finally, distinguish two mutually exclusive execution modes:

- **DuckDB-managed:** VGI stores the definition, DuckDB expands it, and ordinary VGI write RPCs execute the base and trigger actions.
- **Backend-managed:** the remote database fires its own native trigger; DuckDB must not also materialize that definition as an executable `TriggerCatalogEntry`.

Without this distinction, one logical trigger can fire twice.

Recommended delivery phases are: transactional statement triggers without transition tables; statement transition tables after exact OLD/NEW row support; then the currently supported AFTER INSERT/DELETE row-trigger subset.

## 7. Proposed application protocol 2.0 checklist

### Must be in the 2.0 design if nested schemas are enabled

- [ ] Replace object-owner `schema_name` fields with `schema_path: list<utf8>`.
- [ ] Replace cross-catalog `source_schema` with `source_schema_path`.
- [ ] Replace foreign-key `referenced_schema` with `referenced_schema_path`.
- [ ] Give `SchemaInfo` a complete path and define enumeration order.
- [ ] Convert every catalog method, write-function lookup, scan result, and function callback family consistently.
- [ ] Use structural path keys in Python and C++ registries/caches.
- [ ] Define component preservation, comparison, dots, and empty-path rules.
- [ ] Reject all VGI 1.x peers at the VGI 2.0 protocol boundary.
- [ ] Add nested create/get/list/rename/drop tests, including names containing dots and mixed case.

### Must be done for safe DuckDB 2.0 filter integration

- [ ] Remove the v1 encoder/decoder and `ClientCapabilities.filter_encodings` from every 2.0 SDK.
- [ ] Make `vgi.filters.v2` the only accepted filter payload marker.
- [ ] Replace the column-scoped wire hierarchy with one expression AST.
- [ ] Put an unprojected schema index and validating name on every column-reference node.
- [ ] Define the index space as the unprojected bind output schema.
- [ ] Map projected engine indexes to bind-schema indexes in every adapter.
- [ ] Distinguish required predicates from advisory predicates.
- [ ] Serialize required predicates atomically; never drop an unsupported child.
- [ ] Retain DuckDB's local predicate for every advisory multi-column filter.
- [ ] Replace positional dynamic updates with stable IDs and monotonic revisions.
- [ ] Replace raw parser spellings with standard symbolic names or advertised namespaced extension functions.
- [ ] Translate DuckDB 2.0 `ExpressionFilter` trees semantically, including function-backed comparisons/casts and `COMPARE_IN`.
- [ ] Intercept every known `__internal_tablefilter_*` wrapper; never forward its private name as a VGI call.
- [ ] Add `runtime_filter_algorithms`, root advisory `runtime_filter`, and immutable `artifact_N` payload slots.
- [ ] Reserve Bloom and prefix-range candidate identities but do not advertise them until versioned artifact exporters/evaluators exist.
- [ ] Test cross-column comparisons, casts, NULLs, rowid, dictionary columns, dynamic filters, projection reorder, malformed references, and ignored unsupported runtime artifacts.

### Must be decided, even if the decision is “unsupported”

- [ ] `TUPLE` round-trip behavior.
- [ ] aggregate-state types across the worker boundary.
- [ ] trigger and standalone window-function catalog capabilities.
- [ ] behavior for an absent default schema.

### Optional follow-up capabilities

- [ ] projection-expression pushdown
- [ ] table-function repeatability
- [ ] explicit partition selection after planning
- [ ] explicit parallelism classification
- [ ] typed operator metrics
- [ ] scalar fallibility and argument monotonicity
- [ ] DuckDB Bloom artifact export/import and `duckdb.runtime_filter/bloom@1`
- [ ] DuckDB prefix-range artifact export/import and `duckdb.runtime_filter/prefix_range@1`

## 8. Validation matrix

Before declaring VGI protocol 2.0 complete, run version-rejection and round-trip tests rather than only compilation tests.

| Client | Worker | Expected result |
|---|---|---|
| 1.x | 1.x | Outside the VGI 2.0 implementation and test matrix |
| 1.x | 2.0 | Clean version rejection before catalog or function dispatch |
| 2.0 | 1.x | Clean version rejection before catalog or function dispatch |
| 2.0 | 2.0 | Nested paths, mandatory filter v2, and negotiated optional capabilities |

Minimum semantic tests:

1. Two objects with the same leaf name under different nested paths never collide in caches or dispatch.
2. A component containing a literal dot is distinct from two components.
3. Create/rename/drop of a parent schema invalidates descendants correctly in one transaction.
4. Function callbacks after bind retain the exact function schema path.
5. Foreign keys resolve a nested target without string parsing.
6. A filter `left_col < right_col` reaches a v2 worker with two independently validated column references and is retained locally by DuckDB.
7. Projection reorder does not change the meaning of v2 filter column indices.
8. An unsupported child makes a required predicate fail atomically; neither `AND` nor `OR` is partially applied.
9. Nanosecond zoned timestamps preserve unit and timezone.
10. A tuple nested inside a list either round-trips as `TUPLE` or is rejected before execution.
11. VGI DML preserves DuckDB 2.0 duplicate-row semantics without applying the same remote update twice.
12. Unsupported triggers/window-function catalog entries fail explicitly.
13. DuckDB 2.0 `COMPARE_IN`, function-backed comparisons/casts, and optional/dynamic internal wrappers produce the corresponding semantic VGI nodes without leaking private function names.
14. A supported Bloom or prefix-range artifact produces no false negatives; a known but unsupported algorithm is ignored as one complete advisory predicate.
15. Required, nested, negated, malformed, stale, or wrongly versioned runtime artifacts are rejected or ignored exactly as Filter Encoding v2 specifies.

## 9. Evidence and confidence boundary

The strongest source-level evidence is:

- DuckDB nested schema implementation: `src/include/duckdb/common/qualified_name.hpp`, `src/include/duckdb/catalog/catalog_entry/schema_catalog_entry.hpp`, and the qualified create/drop/alter APIs.
- DuckDB filter implementation: `src/include/duckdb/planner/table_filter.hpp`, `src/include/duckdb/planner/filter/expression_filter.hpp`, `src/include/duckdb/planner/filter/table_filter_functions.hpp`, and the filter commits listed above.
- DuckDB type behavior: `src/common/arrow/arrow_converter.cpp` exports `STRUCT` and `TUPLE` through the same struct path; `src/function/table/arrow/arrow_duck_schema.cpp` imports `+s` as `STRUCT`.
- DuckDB table-function surface: `src/include/duckdb/function/table_function.hpp`.
- Current VGI filter serialization: `vgi/src/vgi_table_function_impl.cpp` and `vgi/src/vgi_rpc_types.cpp`.
- Current worker filter semantics: `vgi-python/vgi/table_filter_pushdown.py`.
- Current catalog/application schemas: `vgi-python/vgi/catalog/catalog_interface.py`, `vgi-python/vgi/catalog/descriptors.py`, and `vgi-python/vgi/protocol.py`.

Relevant nested-schema commits include `99079b3c99`, `1fb649f90a`, `f1088dcf7d`, `66ebb8023a`, `43ca82810e`, `f1c54da78b`, and `95697fa642`. Relevant type commits include `cbcc99333f` (`TIMESTAMP_TZ_NS`), `ed68815148` (`TUPLE`), and `582280ebcc`/`f41a2338ae` (aggregate-state rework). Relevant runtime-filter commits include `8726f24076`, `270a73ecc9`, `c07dd75a17`, `3d94704c6d`, `3b0d794f1c`, and `026e44e376`.

This was a static source, history, and protocol-schema audit. It is enough to reject the claim that nested schemas are the only protocol concern, and it gives a bounded proposal. It is not a substitute for compiling the adapted extension and executing the validation matrix. Because `v2.0-cyanoptera` is not yet a frozen release in this checkout, repeat the diff/API audit at the RC/final commit and record that exact hash in the implementation PR.

## 10. Python protocol 2.0 implementation status

**Implemented:** 2026-09-09 in `vgi-python` commit `665a38d` on the pushed branch `protocol-v2-schema-paths`.

The first protocol-breaking phase is now implemented in Python:

- `VgiProtocol.protocol_version` and the canonical `vgi/protocol_version.txt` are `2.0.0`.
- `SchemaPath` is an ordered `list[str]` of raw identifier components and is exported by `vgi`.
- All object-owner, function-dispatch, schema-operation, scan-branch, and foreign-key schema identifiers use paths. An audit of the complete Python code-generation inventory finds 64 schema-location fields, all encoded as Arrow `list<utf8>`, with no legacy `schema_name`, `source_schema`, or `referenced_schema` wire fields.
- `SchemaInfo.name` is replaced by `SchemaInfo.path`; schema-level RPCs take `path`, while object-level RPCs take `schema_path` plus the object's leaf `name`.
- Declarative catalogs and worker registries use structural tuple keys. Component boundaries are never flattened, so `["a.b"]` and `["a", "b"]` remain distinct; matching is case-insensitive per component.
- Nested declarative schemas must include every parent prefix, and wire enumeration places parents before children while preserving peer order.
- SQL qualification quotes every schema component independently.
- CLI schema arguments accept a bare root component or a JSON array for a nested path. Dots are never parsed as separators.
- The Python SDK generator inventory now includes every opaque request dataclass, closing a pre-existing gap that would otherwise hide aggregate streaming/window and table/macro/index creation schemas from sibling SDK generation.
- The bundled DuckDB 1.5 Python transactor explicitly rejects paths deeper than one component. The DuckDB 1.5 C++ VGI extension must implement the same rule: accept `[schema]`, reject deeper paths, and never join components with dots.

`CatalogAttachResult.default_schema` intentionally remains a scalar root-schema identifier for the reason described in section 2.

The writable-result phase replaces the legacy write booleans and
`return_chunks` option with the `write_result_modes`/`result_mode` contract from
section 6.1. Python implements and directly tests all three modes. The DuckDB
1.5 extension consumes only `count` and `rows`; `changes` is reserved for the
future DuckDB 2.0 trigger adapter. Trigger catalog metadata and RPCs remain
unimplemented.

This phase does **not** implement the other proposed protocol areas: logical-type identity annotations, trigger RPCs, or standalone window-function catalog entries. The sibling C++, C#, Go, Java, Rust, and TypeScript schema migrations are maintained in their corresponding repositories. The vendored browser `vgi-client.js` remains generated from the TypeScript SDK and must never be edited manually in the Python repository.
