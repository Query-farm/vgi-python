# VGI Filter Encoding v2

**Status:** Proposed specification
**Target protocol:** VGI 2.0.0
**Encoding identifier:** `vgi.filters.v2`
**Filter version marker:** `2`
**Expression semantics:** `vgi.duckdb.standard.v1`
**Normative engine oracle:** DuckDB `v1.5.5` (`d8cdaa33fda8df955cc76ef58a280f68f4cd43fa`)

## 1. Purpose

VGI Filter Encoding v2 is the only filter encoding in VGI protocol 2.0. It replaces the version 1 collection of column-scoped filter objects and its single-column expression fallback with one recursive, typed expression model.

This document is the engine-neutral normative wire specification. Optional runtime-artifact contracts live in [VGI Runtime-Filter Artifacts](vgi-runtime-filter-artifacts.md), and DuckDB version-specific mappings live in [VGI DuckDB Filter Adapter](vgi-duckdb-filter-adapter.md).

V2 is designed to support:

- filters that reference any number of columns;
- arbitrarily nested struct-field references;
- typed literals without converting values to JSON or SQL text;
- immutable evaluation context for time-zone-, calendar-, default-collation-, and arithmetic-policy-dependent
  semantics;
- static query predicates and changing runtime pruning predicates;
- capability-gated probabilistic runtime-filter artifacts;
- an embedded DuckDB evaluator as the common implementation path;
- optional deeper translation into engines such as DataFusion;
- readable, stable function identities without transmitting signatures on every call; and
- exact failure behavior when a calling engine delegates filtering to a worker.

VGI 2.0 implementations MUST NOT emit or accept filter encoding v1. A VGI 1.x peer is rejected during protocol negotiation rather than supported through a dual decoder.

## 2. Normative language

The terms **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** describe conformance requirements.

The following roles are used throughout this specification:

- **producer**: the VGI framework adapting a calling engine expression into the wire format;
- **consumer**: the worker SDK or worker function receiving the filter document;
- **calling engine**: the engine executing the user's outer query, such as DuckDB, DataFusion, or Polars;
- **evaluation engine**: the engine that actually applies a filter to worker output, normally embedded DuckDB unless a worker provides another exact implementation; and
- **deeper source**: a data source or engine below the VGI worker, such as a DataFusion `TableProvider`, Parquet scan, database, or remote API.

## 3. Design principles

### 3.1 The wire carries semantics, not an engine AST

VGI does not serialize DuckDB, DataFusion, or Polars internal expression objects. Those objects are version-specific and often contain engine-owned pointers, bind state, catalogs, or optimizer details.

The wire carries a VGI expression AST. The document's `semantics` value selects the versioned behavior of core nodes
and standard functions. DuckDB supplies the reference behavior, but DuckDB's private serialization format is not part
of the protocol.

### 3.2 DuckDB is the portable evaluation baseline

A VGI framework or worker MAY use embedded DuckDB to evaluate a v2 expression over Arrow batches. This is the preferred baseline when the worker's native engine cannot reproduce the required semantics exactly.

A framework MAY translate recognized expressions into a native engine such as DataFusion for deeper pruning or filtering. Native translation is an optimization. It does not relax the obligation to preserve the VGI expression's result.

### 3.3 Functions remain readable

Standard function calls carry canonical symbolic names such as `starts_with`, not numeric identifiers and not repeated signatures. Types are obtained from the bound schema, typed literals, field references, and explicit casts.

Nonstandard functions carry a structured namespace, name, and semantic version. They are sent only after explicit capability matching.

### 3.4 No Substrait dependency

VGI borrows the general ideas of stable registries and namespaced extensions, but Substrait URNs, protobuf messages, plans, and function anchors are not part of Filter Encoding v2. An implementation MAY provide an external VGI-to-Substrait adapter, but lack of a Substrait mapping never prevents a DuckDB expression from using a VGI-defined function.

### 3.5 Correctness is decided before execution

A producer never sends a partially serialized predicate as though it were exact. A required predicate is encoded atomically or is not delegated. An advisory predicate can be ignored, but any pruning derived from it must retain every row that could satisfy the original predicate.

## 4. Transport container

### 4.1 Existing protocol fields

The following existing fields continue to carry filter data:

- `InitRequest.pushdown_filters`;
- `SplitPlanRequest.pushdown_filters`;
- `SplitPlanRequest.refined_filters`; and
- dynamic tick metadata under `vgi_pushdown_filters`.

The field value is an Arrow IPC-serialized `RecordBatch`. Initial and split-planning payloads use a `snapshot` document. Runtime updates use a `delta` document.

`InitRequest.join_keys` remains a list of Arrow `RecordBatch` values and can be referenced by an expression as described in section 9.

### 4.2 Filter RecordBatch layout

A filter `RecordBatch` MUST contain exactly one row.

Its first field MUST be:

```text
filter_spec: utf8 not null
```

Every filter `RecordBatch` schema MUST have this schema-level metadata:

```text
vgi_filter_encoding          = "vgi.filters.v2"
vgi_filter_version           = "2"
vgi_evaluation_context       = <selected context profile>
```

VGI 2.0 defines two context profiles. `vgi.none.v1` states that no expression in the batch depends on session context.
It requires no additional context metadata and is the normal profile for context-independent DuckDB, DataFusion, and
Polars predicates.

`vgi.duckdb.session.v1` adopts DuckDB session semantics and additionally requires:

```text
vgi_time_zone                = <configured DuckDB TimeZone>
vgi_calendar                 = <configured DuckDB Calendar>
vgi_default_collation        = <configured DuckDB default_collation>
vgi_ieee_floating_point_ops  = "true" | "false"
vgi_integer_division         = "true" | "false"
vgi_context_provider_fingerprint = <optional opaque provider identity>
```

The fingerprint entry is optional. Its absence requests ordinary semantic-profile compatibility. Its presence
requests exact provider matching as defined in section 11.3. The other five entries are mandatory under
`vgi.duckdb.session.v1`. All six profile-specific entries MUST NOT appear under `vgi.none.v1`. Boolean metadata uses
the canonical lowercase UTF-8 values `true` and `false`.

Context values are UTF-8 strings captured from the producer context that governed parsing and binding of the filter expression.
An empty DuckDB `default_collation` setting is encoded canonically as `binary`. The producer MUST NOT obtain these
values from an unrelated connection or reread mutable settings after the expression has been bound. If it cannot prove
which context governed a context-dependent expression, it declines that predicate.

This metadata belongs to `RecordBatch.schema.metadata`, not to `RecordBatch.schema.field("filter_spec").metadata`. In PyArrow, a conforming schema is constructed as follows:

```python
pa.schema(
    [
        pa.field("filter_spec", pa.string(), nullable=False),
        # value_N and type_N fields follow
    ],
    metadata={
        b"vgi_filter_encoding": b"vgi.filters.v2",
        b"vgi_filter_version": b"2",
        b"vgi_evaluation_context": b"vgi.none.v1",
    },
)
```

For example, a DuckDB session-profile batch replaces the last entry above with the following entries:

```python
        b"vgi_evaluation_context": b"vgi.duckdb.session.v1",
        b"vgi_time_zone": b"America/New_York",
        b"vgi_calendar": b"gregorian",
        b"vgi_default_collation": b"binary",
        b"vgi_ieee_floating_point_ops": b"true",
        b"vgi_integer_division": b"false",
        # Optional strict mode:
        b"vgi_context_provider_fingerprint": b"duckdb-icu:opaque-provider-id",
```

Evaluation context is immutable for one VGI scan. Every snapshot, refinement, and delta for that scan MUST repeat the
selected profile and its complete set of present context entries byte for byte, including the fingerprint when used. A
consumer MUST NOT substitute its process, machine, or connection defaults for missing context. Section 11.3 defines
how each profile is applied and capability-gated.

The single `filter_spec` value is the UTF-8 JSON document described below.

Remaining fields are typed payload slots:

```text
value_0: <literal Arrow type>
value_1: <literal Arrow type>
...
type_0:  <cast target Arrow type>
type_1:  <cast target Arrow type>
...
artifact_0: <algorithm-defined Arrow type>
artifact_1: <algorithm-defined Arrow type>
...
```

Every payload field MUST contain exactly one value because the batch contains one row.

- A `value_N` field contains the scalar referenced by `value_ref: N`. A typed NULL is represented by a NULL cell whose field still carries the intended Arrow type and metadata.
- A literal candidate set for `IN` is stored as one list-typed value in a `value_N` field.
- A `type_N` field MUST contain NULL. Its Arrow field type and extension metadata define the target referenced by `type_ref: N`.
- An `artifact_N` field contains one immutable runtime-filter artifact. Its Arrow type, extension metadata, and byte-level validation rules are defined by the negotiated runtime-filter algorithm version.

References are resolved by exact field name, not physical position. Missing fields, duplicate field names, noncanonical names, or an out-of-range reference make the document malformed.

JSON MUST NOT contain user literal values except protocol enums, indexes, identifiers, and other structural data. In particular, timestamps, decimals, UUIDs, intervals, blobs, geometry values, lists, and structs remain typed Arrow values.

### 4.3 Snapshot document

An initial document has this shape:

```json
{
  "encoding": "vgi.filters.v2",
  "semantics": "vgi.duckdb.standard.v1",
  "kind": "snapshot",
  "predicates": [
    {
      "id": "query:0",
      "revision": 0,
      "mode": "required",
      "source": "query",
      "expression": {"node": "is_null", "expression": {"node": "column_ref", "column_index": 0, "column_name": "a"}, "negated": false}
    }
  ]
}
```

The `predicates` array is the complete initial predicate state. Its entries are implicitly combined with `AND`. An
empty `predicates` array is legal and means that the snapshot contains no filters.

`semantics` is required and selects the contract for every core expression node and string-named standard function in
the document. The producer MUST select a profile advertised by the worker. VGI 2.0 initially defines
`vgi.duckdb.standard.v1`; section 6 fixes its reference behavior.

Each predicate contains:

- `id`: a nonempty UTF-8 identifier unique within one VGI scan;
- `revision`: an unsigned 64-bit integer, initially zero;
- `mode`: `required` or `advisory`;
- `source`: `query`, `join`, `top_n`, `split_refinement`, or `other`; and
- `expression`: a Boolean expression node.

`source` is diagnostic and does not alter expression semantics.

### 4.4 Delta document

A runtime update has this shape:

```json
{
  "encoding": "vgi.filters.v2",
  "semantics": "vgi.duckdb.standard.v1",
  "kind": "delta",
  "updates": [
    {
      "operation": "upsert",
      "id": "join:build-side-3",
      "revision": 4,
      "mode": "advisory",
      "source": "join",
      "expression": {"node": "in", "expression": {"node": "column_ref", "column_index": 1, "column_name": "customer_id"}, "set": {"kind": "literal", "value_ref": 0}, "negated": false}
    }
  ]
}
```

An update operation is either:

- `upsert`, which includes `mode`, `source`, and `expression`; or
- `remove`, which includes only `operation`, `id`, and `revision`.

Update IDs MUST be unique within one delta. An empty `updates` array is a legal no-op but SHOULD NOT be transmitted.
Every delta MUST repeat the initial snapshot's `semantics` value exactly. The expression-semantics profile cannot
change during a scan.

For a given `id`, a consumer applies an update only when its revision is greater than the last applied revision.
Duplicate and stale revisions are no-ops. After validating the container and update syntax, the consumer determines the
applicable updates, validates every applicable update and referenced payload, and commits all of them atomically. If
any applicable update is invalid, it rejects the complete delta and changes no predicate or remembered revision.

Runtime delta entries MUST be advisory. A required query predicate cannot be added, replaced, or removed after
execution begins. The consumer MUST retain the IDs introduced as required by the initial snapshot and reject an entire
delta that targets any such ID, including an advisory upsert or remove operation.

After a removal, the consumer retains the ID and last revision as a tombstone until the scan ends so that an older
upsert cannot resurrect the predicate. Live IDs and tombstones count toward the per-scan predicate-ID limit in section
12.

An update applies before the next output batch produced after the consumer receives it. It is not retroactive and does not invalidate rows or split tokens already emitted.

## 5. Expression model

Every expression object has a `node` discriminator. Node and property names are case-sensitive ASCII strings.

V2 defines these nodes:

| Node | Result | Purpose |
|---|---|---|
| `column_ref` | column type | Top-level bound output column |
| `field_ref` | field type | A field of a struct expression |
| `literal` | literal type | Typed Arrow scalar |
| `comparison` | Boolean | Binary comparison |
| `and` | Boolean | SQL/Kleene conjunction |
| `or` | Boolean | SQL/Kleene disjunction |
| `not` | Boolean | SQL/Kleene negation |
| `is_null` | Boolean | NULL test |
| `in` | Boolean | Membership test |
| `cast` | target type | Explicit DuckDB-compatible cast |
| `arithmetic` | resolved numeric type | Arithmetic expression |
| `negate` | resolved numeric type | Unary numeric negation |
| `call` | function result type | Standard or extension scalar function |
| `runtime_filter` | Boolean pruning decision | Capability-gated advisory runtime-filter artifact |

The root of every predicate MUST resolve to `BOOLEAN`. During filtering, only `TRUE` retains a row; `FALSE` and `NULL` discard it.

### 5.1 Column reference

```json
{
  "node": "column_ref",
  "column_index": 3,
  "column_name": "total"
}
```

`column_index` indexes the unprojected `BindResponse.output_schema`. It does not index the projected output batch. The index is authoritative and `column_name` is a required validation value. The consumer MUST reject the reference if the indexed field's exact name does not equal `column_name`.

Producers MUST map engine-local projected indexes back to bind-schema indexes before serialization. Consumers MUST arrange for every referenced column to be available during evaluation even when that column is absent from `projection_ids`. Projection applies after required filter evaluation.

### 5.2 Nested field reference

```json
{
  "node": "field_ref",
  "expression": {
    "node": "field_ref",
    "expression": {"node": "column_ref", "column_index": 1, "column_name": "address"},
    "field_index": 2,
    "field_name": "location"
  },
  "field_index": 0,
  "field_name": "latitude"
}
```

The input expression MUST resolve to an Arrow struct-compatible type. `field_index` is authoritative and `field_name` is validating. Repeated `field_ref` nodes provide arbitrary nesting depth, subject to implementation safety limits.

`field_ref` is null-propagating. If its input struct evaluates to NULL, the result is a typed NULL of the selected
field's declared type; the consumer MUST NOT read or interpret the child slot for that row. If the input struct is
non-NULL but the selected child is NULL, the result is likewise a typed NULL. Therefore, a NULL at any struct level
propagates through every enclosing `field_ref`, and `is_null` over that result evaluates to `TRUE`. Child-array values
or validity bits beneath a NULL parent do not override the parent struct's validity.

V2 does not define list indexing, map lookup, or variant traversal as core nodes. Those operations require a registered function or a future encoding revision.

### 5.3 Literal

```json
{"node": "literal", "value_ref": 0}
```

`value_ref: 0` resolves field `value_0`. The Arrow field and scalar jointly define the logical value. A consumer MUST preserve Arrow extension metadata when converting the value into its evaluation engine.

### 5.4 Comparison

```json
{
  "node": "comparison",
  "op": "lt",
  "left": {"node": "column_ref", "column_index": 0, "column_name": "cost"},
  "right": {"node": "column_ref", "column_index": 1, "column_name": "budget"}
}
```

`op` is one of:

```text
eq ne lt le gt ge distinct_from not_distinct_from
```

The first six operations use SQL NULL behavior: if either operand is NULL, the result is NULL. `distinct_from` and `not_distinct_from` treat NULL as a comparable value according to DuckDB's `IS DISTINCT FROM` and `IS NOT DISTINCT FROM` semantics.

Operands MUST be bind-compatible under the reference cast rules. A producer inserts an explicit `cast` whenever the intended expression depends on a cast that is not represented by the operands' resolved types.

For `VARCHAR`, an uncollated binary comparison is a context-independent core operation and MAY use `vgi.none.v1`.
The producer MUST prove from the bound expression, or from a documented generated-filter invariant of its engine
adapter, that binary comparison semantics were selected; merely observing a locally configured default is
insufficient. A comparison using a non-binary default collation requires
`vgi.duckdb.session.v1`. An explicit or type-level collation remains ineligible unless a registered extension contract
encodes it, as specified in section 11.4.

### 5.5 Boolean nodes

```json
{"node": "and", "children": [/* two or more Boolean expressions */]}
```

```json
{"node": "or", "children": [/* two or more Boolean expressions */]}
```

```json
{"node": "not", "expression": {/* Boolean expression */}}
```

`and`, `or`, and `not` use SQL three-valued/Kleene logic. `and` and `or` MUST have at least two children. Producers SHOULD flatten nested nodes of the same kind while retaining child order.

Boolean constants use typed Arrow literals. Producers do not encode zero-child conjunctions.

### 5.6 NULL test

```json
{
  "node": "is_null",
  "expression": {"node": "column_ref", "column_index": 0, "column_name": "deleted_at"},
  "negated": false
}
```

`negated: false` means `IS NULL`; `negated: true` means `IS NOT NULL`. The result is never NULL.

### 5.7 Membership

Literal set:

```json
{
  "node": "in",
  "expression": {"node": "column_ref", "column_index": 2, "column_name": "status"},
  "set": {"kind": "literal", "value_ref": 1},
  "negated": false
}
```

External join-key set:

```json
{
  "node": "in",
  "expression": {"node": "column_ref", "column_index": 2, "column_name": "customer_id"},
  "set": {
    "kind": "external",
    "batch_index": 0,
    "column_index": 0,
    "column_name": "customer_id"
  },
  "negated": false
}
```

`negated: false` is SQL `IN`; `negated: true` is SQL `NOT IN`.

For a literal set, the referenced `value_N` MUST contain one Arrow list scalar whose child type is compatible with the tested expression. An external set references a column in `InitRequest.join_keys`; both indexes are authoritative and the name is validating.

Membership has SQL semantics:

- a matching non-NULL value produces `TRUE` for `IN`;
- no match with a NULL candidate present produces `NULL`;
- no match with no NULL candidate produces `FALSE`;
- a NULL tested value produces `NULL` for a nonempty set;
- `IN` against an empty set is `FALSE`, including for a NULL tested value;
- `NOT IN` is the SQL three-valued negation of `IN`.

String membership follows the same collation rule as comparison in section 5.4. A producer-proven binary,
uncollated membership operation may use `vgi.none.v1`; non-binary default-collation semantics require the DuckDB
session profile.

External sets used for dynamic pruning are advisory. A producer MUST NOT reference an external batch that is unavailable in the initialized execution state. A missing external batch or column is malformed input and causes a protocol error regardless of predicate mode.

#### 5.7.1 Small element lists and large exact key sets

V2 explicitly preserves the special handling that VGI already provides for DuckDB `IN` filters. There are two wire representations for the same exact set-membership semantics:

1. **Inline typed element list.** `set.kind` is `literal`, and `value_ref` points to one Arrow `LIST<T>` scalar in the filter batch. This is appropriate for a SQL literal list, a small optimizer-produced `IN` set, or a dynamic set small enough to fit comfortably in tick metadata.
2. **External exact key batch.** `set.kind` is `external`, and the indexes identify a typed Arrow column in `InitRequest.join_keys`. This is appropriate for a larger optimizer-produced set or join build-side keys. The key values are not converted to JSON and are not duplicated into `filter_spec`.

For example, this SQL predicate:

```sql
WHERE status IN ('new', 'ready', 'held')
```

may use:

```json
{
  "node": "in",
  "expression": {"node": "column_ref", "column_index": 2, "column_name": "status"},
  "set": {"kind": "literal", "value_ref": 0},
  "negated": false
}
```

with `value_0` containing the single Arrow list value `['new', 'ready', 'held']` and type `list<utf8>`.

The choice between inline and external storage does not change the expression or its NULL behavior. It is a transport decision. Producers SHOULD use a configurable encoded-byte threshold and SHOULD prefer the external form when copying the list into tick metadata would be expensive. If an exact key set exceeds all configured transport limits, the producer declines that `IN` pushdown or substitutes a separately supported advisory pruning artifact; it never truncates the set.

Each external set reference identifies one column. Multiple independently pushed key columns may use separate batches. A correlated composite-key membership relation is not represented as independent required `IN` predicates because doing so loses tuple correlation. Until VGI defines a row-set membership node, composite-key sets may be used only as advisory per-column pruning or evaluated through another exact mechanism.

### 5.8 Cast

```json
{
  "node": "cast",
  "expression": {"node": "column_ref", "column_index": 0, "column_name": "amount"},
  "type_ref": 0
}
```

`type_ref: 0` resolves field `type_0`. The field contains one NULL value; its Arrow type and field metadata define the target type.

The cast is an ordinary throwing cast with the behavior fixed by `vgi.duckdb.standard.v1` (section 6). `TRY_CAST` is not part
of the core v2 AST. A producer MUST NOT translate an engine cast when its overflow, parsing, time-zone, collation, or
error behavior differs from `vgi.duckdb.standard.v1` (section 6).

A cast that can consult session context is eligible only when the worker advertises the matching evaluation-context
profile and the filter batch carries that profile's complete context. This includes DuckDB casts from `VARCHAR` to
`TIMESTAMPTZ` or `TIMETZ` when an input might omit an explicit offset, casts between local `DATE`/`TIMESTAMP` values
and `TIMESTAMPTZ`, and casts from `TIMESTAMPTZ` to local civil date or time types. A producer cannot assume that an
embedded worker's machine time zone matches the calling engine's bound session.

### 5.9 Arithmetic and negation

```json
{
  "node": "arithmetic",
  "op": "add",
  "left": {"node": "column_ref", "column_index": 0, "column_name": "price"},
  "right": {"node": "literal", "value_ref": 0}
}
```

`op` is one of:

```text
add subtract multiply divide modulo
```

Unary negation is:

```json
{"node": "negate", "expression": {"node": "column_ref", "column_index": 0, "column_name": "delta"}}
```

Input coercion, result type, overflow, division by zero, and error behavior follow `vgi.duckdb.standard.v1` (section 6). A native adapter MUST decline exact translation where its behavior differs.

DuckDB division semantics are session-dependent. `integer_division` can change both the value and bound result type of
the `/` operator, while `ieee_floating_point_ops` changes floating-point exceptional results such as infinity and NaN
into errors when disabled. A producer emits an arithmetic expression affected by either setting only under
`vgi.duckdb.session.v1`, with both settings captured as described in section 11.3. It MUST NOT infer the intended
behavior from DuckDB's compiled defaults or from the worker's current connection.

### 5.10 Standard function call

```json
{
  "node": "call",
  "function": "starts_with",
  "arguments": [
    {"node": "column_ref", "column_index": 0, "column_name": "path"},
    {"node": "literal", "value_ref": 0}
  ]
}
```

A string `function` identifies a member of the document's selected `semantics` profile. Under
`vgi.duckdb.standard.v1`, the name is lowercase ASCII snake case and selects the standard function of that name. It is
serialized as a human-readable string and generated as an enum in every VGI SDK.

The call does not carry argument or result signatures. The consumer resolves the overload from the recursively resolved
argument types and the function registry. Before encoding the call, the producer MUST insert explicit `cast` nodes as
needed so that exactly one registered overload matches. No matching overload, multiple matching overloads, or a result
other than the registry-defined result is a protocol evaluation error.

### 5.11 Extension function call

```json
{
  "node": "call",
  "function": {
    "namespace": "duckdb.spatial",
    "name": "intersects_extent",
    "version": 1
  },
  "arguments": [
    {"node": "column_ref", "column_index": 0, "column_name": "geom"},
    {"node": "literal", "value_ref": 0}
  ]
}
```

An extension function identity contains:

- `namespace`: a stable dotted lowercase namespace controlled by its owner;
- `name`: a lowercase ASCII snake-case semantic name; and
- `version`: a positive integer semantic-contract version.

An optional `options` object MAY accompany the call when the extension definition specifies its keys, value types, defaults, and behavior. Unknown options are errors; options never silently fall back to an engine default.

Extension identity describes semantics, not merely a local catalog spelling. DuckDB operator `&&` and function aliases could both normalize to `duckdb.spatial/intersects_extent@1`; the punctuation `&&` is not transmitted as executable SQL.

Only deterministic, side-effect-free functions are eligible. Volatile functions and functions that inspect secrets or
arbitrary settings MUST NOT be pushed. A deterministic function whose behavior depends only on the evaluation-context
settings defined in section 11.3 is eligible when its semantic contract declares that dependency, the complete context
is encoded, and the worker advertised the matching context profile. Functions that depend on any unencoded or
unsupported session state, collation, calendar, or time zone remain ineligible.

### 5.12 Runtime-filter artifact

Runtime filters produced during execution use a distinct capability-gated node:

```json
{
  "node": "runtime_filter",
  "algorithm": {
    "namespace": "duckdb.runtime_filter",
    "name": "prefix_range",
    "version": 1
  },
  "input": {
    "node": "column_ref",
    "column_index": 2,
    "column_name": "customer_id"
  },
  "artifact_ref": 0,
  "null_handling": "pass"
}
```

`artifact_ref: N` resolves to `artifact_N`. `algorithm` selects a separately registered immutable artifact contract. `null_handling` is `pass` or `reject`.

This node MUST be the root of its own advisory predicate, MUST be used only positively, and MUST retain the exact local residual. It never appears beneath another expression node and can never be required. A known but unsupported algorithm causes the complete advisory predicate to be ignored. False positives are allowed; false negatives are forbidden.

Artifact formats, capability rules, algorithm registration, DuckDB Bloom and prefix-range candidates, validation, and conformance tests are specified in [VGI Runtime-Filter Artifacts](vgi-runtime-filter-artifacts.md).

## 6. Standard semantics and function set

`vgi.duckdb.standard.v1` defines the semantics of both core expression nodes and the deliberately small standard
function set below. Its normative oracle is the observable SQL behavior of upstream DuckDB `v1.5.5`, tag commit
`d8cdaa33fda8df955cc76ef58a280f68f4cd43fa`, after applying the evaluation-context settings required by section 11.3.
It is not a reference to the moving `1.5.x` release family.

Haybarn `v1.5.5-rc1`, tag commit `105edd31b57fe7e6d32ee648efad562e45a9f908`, is a reference distribution built
on that upstream release and is expected to conform to `vgi.duckdb.standard.v1`. If an unlisted Haybarn modification
and upstream DuckDB v1.5.5 produce different observable expression behavior, upstream DuckDB v1.5.5 is the tie-breaking
oracle unless this specification explicitly states otherwise.

The profile covers values, logical result types, NULL behavior, errors by category, overload selection, coercion, and
the effects of encoded session settings. Exact diagnostic wording, private expression classes, optimizer choices, and
physical execution algorithms are not semantic results. The normative sources have this precedence: explicit rules in
this specification; observable upstream DuckDB v1.5.5 behavior for cases those rules do not fix; and conformance-corpus
vectors as executable examples. A corpus vector that conflicts with either higher-precedence source is a corpus defect
and MUST NOT redefine the profile.

This behavior does not change merely because an installed DuckDB version changes. When stable DuckDB 2.0 semantics are
adopted, they will be published as `vgi.duckdb.standard.v2`; they will not silently redefine v1. A producer or consumer
may support both profiles, and the document-level `semantics` value selects one unambiguously.

### 6.1 `starts_with`

```text
starts_with(input: VARCHAR, prefix: VARCHAR) -> BOOLEAN
```

- Returns NULL when either argument is NULL.
- Performs case-sensitive literal matching.
- Does not normalize Unicode.
- An empty prefix matches every non-NULL input.
- Inputs with a non-default collation are not eligible for this standard function.

DuckDB `starts_with`, DuckDB `prefix`, and DuckDB `^@` normalize to this name only when their bound semantics match this contract.

### 6.2 `ends_with`

```text
ends_with(input: VARCHAR, suffix: VARCHAR) -> BOOLEAN
```

- Returns NULL when either argument is NULL.
- Performs case-sensitive literal matching.
- Does not normalize Unicode.
- An empty suffix matches every non-NULL input.
- Inputs with a non-default collation are not eligible.

### 6.3 `contains`

```text
contains(input: VARCHAR, substring: VARCHAR) -> BOOLEAN
```

- Returns NULL when either argument is NULL.
- Performs case-sensitive literal substring matching, never regular-expression matching.
- Does not normalize Unicode.
- An empty substring matches every non-NULL input.
- Inputs with a non-default collation are not eligible.

An adapter may translate Polars `str.contains(..., literal=True)` into this function. It MUST NOT translate Polars' default regex form into this function.

### 6.4 `list_contains`

```text
list_contains(input: LIST<T>, needle: T) -> BOOLEAN
```

- Returns NULL when the input list is NULL.
- Returns NULL when the needle is NULL.
- NULL elements do not match a non-NULL needle and do not by themselves make a failed search NULL.
- Returns `FALSE` for an empty non-NULL list and non-NULL needle.
- Equality uses the registered equality semantics for `T`.
- Collated child types are not eligible unless a future function version encodes the collation.

DuckDB `array_contains` and the list overload of DuckDB `contains` normalize to `list_contains` when their bound semantics match.

### 6.5 Adding standard functions

Additional standard names may be added compatibly within VGI protocol 2 only through capability advertisement. A peer never assumes support merely because a newer SDK knows the name.

A standard function definition must specify:

- accepted argument type families;
- result type and nullability;
- NULL behavior;
- error behavior;
- collation, Unicode, time-zone, NaN, overflow, and rounding behavior where relevant;
- determinism requirements; and
- conformance vectors.

Regex functions are intentionally absent from the initial set because DuckDB, DataFusion, and Polars can use different regex engines and options. They require a separate semantic contract rather than a name mapping.

## 7. Filter capabilities

### 7.1 Capability metadata

VGI 2.0 removes `supported_expression_filters`. A table function uses:

```text
filter_pushdown: bool
filter_semantic_profiles: list<utf8>
additional_filter_functions: list<FilterFunctionCapability>
runtime_filter_algorithms: list<RuntimeFilterAlgorithmCapability>
filter_evaluation_contexts: list<EvaluationContextCapability>
```

`filter_pushdown` controls ordinary expression predicates independently of runtime-filter artifacts.

- `filter_pushdown: false` means the producer sends no core or function-call expression predicates.
- A nonempty `runtime_filter_algorithms` list permits artifact-only pruning even when `filter_pushdown` is false. In
  that case every transmitted predicate MUST be advisory and have `runtime_filter` as its root.
- If `filter_pushdown` is false and `runtime_filter_algorithms` is empty, the producer sends no predicates.
- Semantic-profile, additional-function, and evaluation-context capabilities have no effect on ordinary expression
  pushdown while `filter_pushdown` is false.

`filter_pushdown: true` permits ordinary filters but does not select their semantics. The worker MUST advertise every
core and standard-function contract it supports in `filter_semantic_profiles`; the filter document selects exactly one
with `semantics`. For each advertised profile, `filter_pushdown: true` claims support for all of its context-independent
core nodes and string-named standard functions. Structured extension calls require a matching extension capability.
Runtime-filter artifacts require a matching algorithm capability. A core node or function whose behavior depends on
an evaluation context additionally requires a matching `filter_evaluation_contexts` entry.

An artifact-only worker with `filter_pushdown: false` still advertises the semantic profiles it can use to interpret
permitted `runtime_filter` input expressions. That advertisement does not permit an ordinary expression predicate;
the root restrictions above remain controlled by `filter_pushdown` and `runtime_filter_algorithms` together.

An embedded-DuckDB-capable worker normally advertises:

```json
{
  "filter_pushdown": true,
  "filter_semantic_profiles": ["vgi.duckdb.standard.v1"],
  "additional_filter_functions": [],
  "runtime_filter_algorithms": [],
  "filter_evaluation_contexts": [{"profile": "vgi.duckdb.session.v1"}]
}
```

Advertising `vgi.duckdb.session.v1` means the consumer can apply DuckDB-compatible `TimeZone`, `Calendar`,
`default_collation`, `ieee_floating_point_ops`, and `integer_division` settings in an isolated evaluation context before
parsing, binding, and evaluating a predicate. It also means its temporal, collation, and arithmetic implementations
have the semantics required by that profile. An embedded-DuckDB consumer MUST load DuckDB's `icu` extension before
advertising the profile or applying its `TimeZone` and `Calendar` values. The extension name is retained even by
DuckDB versions whose implementation embeds generated Unicode data and does not link to the external ICU library. A
native engine adapter such as DataFusion MUST NOT advertise the profile merely because it accepts similarly named
settings.

`EvaluationContextCapability` has this logical shape:

```json
{
  "profile": "vgi.duckdb.session.v1",
  "provider_fingerprint": "duckdb-icu:opaque-provider-id"
}
```

`profile` is required. `provider_fingerprint` is optional, nonempty UTF-8 with a maximum encoded length of 256 bytes,
and opaque to the protocol. Advertising the profile is always a claim of ordinary semantic compatibility, regardless
of whether a fingerprint is present. Advertising a fingerprint additionally permits a producer to request exact
provider matching. `vgi.none.v1` is part of base Filter Encoding v2 and need not be advertised.

`FilterFunctionCapability` has this logical shape:

```json
{
  "namespace": "duckdb.spatial",
  "name": "intersects_extent",
  "version": 1
}
```

Advertising an extension function means the worker implements every overload and option defined by that exact semantic-contract version. A partial implementation uses a different function name or contract version; signatures are not negotiated on every invocation.

`RuntimeFilterAlgorithmCapability` has the same identity fields:

```json
{
  "namespace": "duckdb.runtime_filter",
  "name": "prefix_range",
  "version": 1
}
```

Advertising a runtime-filter algorithm means the consumer implements the complete artifact contract for that exact version, including Arrow representation, byte layout, type normalization, construction and lookup behavior, NULL behavior, size validation, and the false-negative prohibition. Algorithm parameters are part of the versioned artifact rather than separately negotiated at each node.

Runtime-filter capability and artifact details are defined by [VGI Runtime-Filter Artifacts](vgi-runtime-filter-artifacts.md).

`auto_apply_filters` remains an SDK-local convenience setting rather than a wire capability. Whether the worker SDK applies the expression automatically or user code applies it manually does not change the producer's contract.

### 7.2 Producer capability intersection

A producer emits an ordinary expression only when the worker advertised the document's selected semantic profile. It
emits a `call` only when all of these additional conditions hold:

1. its calling-engine adapter recognizes the resolved native expression;
2. it can prove that the mapping preserves the registered semantics;
3. a string-named function belongs to the selected semantic profile, or the worker advertised the matching structured
   extension identity; and
4. every child expression is independently encodable.

Matching a displayed function name is insufficient. The adapter considers the bound function identity, resolved argument types, options, collation, and relevant session state.

Context-independent core nodes require no per-node capability list and use `vgi.none.v1` unless another expression in
the scan requires a negotiated context. A producer emits a context-dependent core node only when the worker advertised
the batch's evaluation-context profile. Under ordinary compatibility mode, matching the profile is sufficient. Under
optional strict mode, the producer additionally requires an identical advertised provider fingerprint and includes it
in every filter batch for the scan. A producer that cannot encode a core native expression simply declines that
predicate.

A producer emits `runtime_filter` only after exact algorithm capability matching. This permission is independent of
`filter_pushdown`. Otherwise it omits that advisory artifact and retains the exact local residual.

## 8. Required and advisory predicates

### 8.1 Required

`mode: "required"` means the calling engine has delegated exact row filtering to the VGI worker and may not retain a local filter.

For a required predicate, the consumer MUST:

- validate the entire expression;
- apply the complete expression with the semantics in this specification;
- perform evaluation before final projection;
- retain only rows for which the expression is TRUE; and
- fail the query if validation, binding, or evaluation cannot be completed exactly.

The consumer MUST NOT skip an unknown node, unsupported function, missing set, or invalid child. Required `AND` and `OR` trees are atomic.

### 8.2 Advisory

`mode: "advisory"` means the calling engine retains an exact residual predicate. The worker may ignore the predicate.

If a worker uses an advisory predicate to eliminate data, its derived pruning condition `H` MUST be a logical
weakening of the original predicate `P`. Under SQL three-valued logic, the requirement is:

```text
For every row: if P evaluates to TRUE, H MUST evaluate to TRUE.
```

Rows for which `P` is FALSE or UNKNOWN impose no condition on `H`. This ensures that every row satisfying `P` remains
available to the calling engine. For example, using `A` to prune for `A AND B` is safe; using `A` to prune for `A OR B`
is not safe.

A worker may instead evaluate the complete advisory expression exactly. Unknown or unsupported advisory content causes the whole predicate to be ignored unless the worker can prove a safe weakening.

### 8.3 Producer rules

A producer MUST decide mode from the calling engine's actual plan contract:

- If the calling engine removes its local predicate, send `required` only after the complete expression and worker capabilities have been validated.
- If the calling engine retains an exact local predicate, send `advisory`.
- A partial translation of a top-level conjunction may send independently non-throwing conjuncts as separate advisory
  predicates because each is a safe necessary condition.
- When the original top-level `AND` has more than one conjunct, the producer MUST NOT extract and independently send a
  conjunct that can throw. Independent evaluation can expose an error that the calling engine's evaluation of the
  original conjunction would not reach. For example, from `x <> 0 AND 10 / x > 1`, the producer may send `x <> 0`
  separately but MUST NOT send `10 / x > 1` separately. The producer retains the throwing conjunct locally, or sends
  the complete unsplit conjunction only when exact evaluation order and error behavior are guaranteed.
- A partial translation of an `OR`, `NOT`, function call, comparison, or other indivisible subtree MUST NOT be sent.

## 9. Static, dynamic, and join-derived filters

### 9.1 Static filters

Static query predicates appear in the initial snapshot with source `query`. They retain the same ID for the scan lifetime. Required static predicates have revision zero and are immutable.

### 9.2 Dynamic filters

Join-derived and Top-N-derived filters are advisory and use delta updates. IDs identify logical predicates, not positions in an array. Revisions are monotonically increasing per ID.

An empty build-side key set should be represented as an `in` expression referencing an empty list scalar, or as a typed Boolean `FALSE` literal. It is not represented by removing the predicate.

Dynamic filter updates carried only in tick metadata embed an exact candidate set in a `value_N` list scalar only when
the calling-engine adapter already exposes that set within its configured limit. The initial DuckDB adapter emits such
a join-derived set only when DuckDB generates it within `dynamic_or_filter_threshold`, whose default is 50. It does not
copy a larger build set into tick metadata.

An `external` set reference is valid only when the referenced `join_keys` batch is available in the same initialized
execution state. `InitRequest.join_keys` is fixed at initialization and is not a transport for a key set discovered
later during join execution. Until a negotiated runtime-artifact exporter exists, a larger DuckDB join build can
contribute exposed min/max narrowing or no VGI runtime predicate; VGI 2.0's initial DuckDB profile has no large dynamic
exact-set path. The precise initial scope is recorded in [VGI Runtime-Filter Artifacts](vgi-runtime-filter-artifacts.md#11-initial-implementation-decision).

### 9.3 Split refinement

`SplitPlanRequest.refined_filters` uses advisory predicates with source `split_refinement`. A refinement narrows only splits not yet emitted. It never changes the meaning or validity of already issued split tokens.

### 9.4 Chained pushdown

A VGI worker may push a received expression into a deeper engine:

```text
calling engine residual
    -> VGI worker
       -> optional native translation
          -> deeper source
```

The worker remains responsible for its upstream contract:

- For an upstream required predicate, the worker may send a translated advisory predicate deeper, but must evaluate the exact VGI residual before returning rows.
- It may delegate the predicate as required to the deeper source only when that source provides an exact guarantee.
- For an upstream advisory predicate, each layer may use a safe weakening and retain or ignore the residual.

This permits DataFusion inside a worker to push recognized conditions into a `TableProvider` while embedded DuckDB remains the correctness backstop.

### 9.5 Runtime-filter transport

Filter Encoding v2 reserves capability-gated advisory runtime artifacts. Their algorithm contracts and the DuckDB Bloom and prefix-range candidates are defined in [VGI Runtime-Filter Artifacts](vgi-runtime-filter-artifacts.md). Runtime artifacts are optional and are not required for base Filter Encoding v2 conformance.

## 10. Framework behavior

### 10.1 DuckDB client adapter

DuckDB adapters map bound semantics rather than parsing SQL or forwarding private expression serialization. DuckDB 1.5 emits its supported single-column subset; DuckDB 2.0 additionally maps each multi-column `ExpressionFilter` reference to the unprojected bind schema and retains advisory residuals.

The version-specific class mapping, internal wrapper handling, projection rules, and test plan are defined in [VGI DuckDB Filter Adapter](vgi-duckdb-filter-adapter.md).

### 10.2 Embedded DuckDB worker evaluator

The standard worker path binds a generated expression against the unprojected bind schema and generated column aliases.
Implementations SHOULD bind or compile each accepted predicate revision once and reuse the prepared evaluator across
input batches. Registering a batch, parsing SQL, and rebinding the unchanged expression for every batch is not the
recommended baseline. When a delta changes a predicate, the worker validates and binds the replacement evaluator once
before atomically installing that revision.

Implementations MUST NOT interpolate user identifiers or literal text into SQL.

Typed literals SHOULD be supplied as registered Arrow values or bound parameters. Extension functions are available only when the required DuckDB extension is loaded and its advertised semantic version passes conformance tests.

An embedded DuckDB version that no longer matches `vgi.duckdb.standard.v1` must compensate in the adapter or stop
advertising that semantic profile.

### 10.3 DataFusion

DataFusion may appear on either side of VGI.

As a client, it translates recognized `Expr` trees into v2. It should report VGI scan pushdown as inexact unless the worker function has explicitly guaranteed exact application, thereby retaining a DataFusion residual.

A DataFusion client normally uses `vgi.none.v1`. It adopts `vgi.duckdb.session.v1` only for a translation that
deliberately selects the transmitted DuckDB session values and proves DuckDB-profile equivalence.

Inside a worker, DataFusion may translate core and standard functions into native `Expr` values for deeper pushdown. A native translation registry maps a VGI semantic node to a DataFusion expression only when types, NULL behavior, options, and errors match. Otherwise the worker obtains the broader input and applies the VGI expression through embedded DuckDB before returning rows.

### 10.4 Polars

Polars is primarily a VGI client. Its adapter translates recognized Polars expressions into v2 and retains the complete Polars filter locally. Therefore its predicates are normally advisory.

The adapter maps semantic operations, not method names. In particular, Polars literal substring matching can map to `contains`; its default regular-expression matching cannot.

A Polars client normally uses `vgi.none.v1` and does not supply time-zone or calendar placeholders. It may adopt
`vgi.duckdb.session.v1` only when its adapter deliberately chooses the transmitted DuckDB session values and proves the
translated expression has DuckDB-profile semantics. Otherwise it omits the context-dependent advisory predicate and
retains its native filter.

### 10.5 Acero

Acero-specific native translation is outside the initial VGI 2.0 implementation scope. Acero can continue to rely on a local residual or an embedded DuckDB evaluator without affecting this protocol.

## 11. Type, NULL, evaluation-context, collation, and error rules

### 11.1 Type resolution

The consumer resolves expression types bottom-up from:

- `BindResponse.output_schema` for `column_ref`;
- struct field metadata for `field_ref`;
- Arrow payload fields for `literal` and `cast`;
- core operator rules; and
- the registered standard or extension function definition.

Function signatures are not repeated in call nodes. An ambiguous or invalid overload is an error rather than an opportunity to guess or use an engine's implicit default.

### 11.2 NULL and filtering

Expression evaluation uses SQL three-valued logic. The final filter mask retains TRUE only. This rule applies equally to automatic SDK filtering, embedded DuckDB evaluation, native translation, and user-managed filtering.

### 11.3 Evaluation context

The filter batch carries an immutable, versioned evaluation-context profile in its schema metadata.

`vgi.none.v1` means that every expression in the batch is independent of mutable producer session settings and
evaluation-context provider state. A producer MUST NOT use this profile for a cast, function, or collation operation
whose result can depend on a time zone, calendar, non-binary default collation, floating-point policy,
integer-division policy, or another unencoded setting. The consumer receives and applies no producer session values
for this profile, but it MUST isolate evaluation from its own mutable session state and use the fixed
`vgi.duckdb.standard.v1` baseline where the evaluator requires a setting. For embedded DuckDB this includes binary
string comparison for producer-proven uncollated binary comparison and membership nodes. A DuckDB producer without
its `icu` extension loaded can still use `vgi.none.v1` for context-independent predicates; it MUST NOT invent `UTC`
or `gregorian` values and claim the DuckDB session profile.

Under `vgi.duckdb.session.v1`, the context consists of `vgi_time_zone`, `vgi_calendar`, and
`vgi_default_collation`, plus the Boolean `vgi_ieee_floating_point_ops` and `vgi_integer_division` values. `TimeZone`
and `Calendar` are options registered by DuckDB's `icu` extension rather than core DuckDB settings;
`ieee_floating_point_ops` and `integer_division` are core DuckDB session settings. A DuckDB producer MUST have the
`icu` extension loaded, verify that all five settings exist, and capture their values from the session that parsed and
bound the expression. A non-DuckDB producer MAY adopt this profile only when it deliberately chooses all five supplied
DuckDB setting values and proves that its expression translation has the profile's semantics; it MUST NOT fill the
fields with unrelated engine defaults merely to enable pushdown.

Before binding or evaluating a DuckDB-session predicate, an embedded-DuckDB consumer MUST load the `icu` extension and
apply all five values to an isolated evaluation session. `vgi_integer_division` MUST be applied before parsing any
generated expression because it changes `/` during parsing as well as the resulting value and type. The consumer MUST
NOT mutate a connection concurrently used by unrelated work. A consumer that evaluates the VGI AST directly rather
than through SQL MUST give its cast, temporal, comparison, and arithmetic implementations the same context explicitly
and satisfy the same profile conformance tests.

The deprecated DuckDB setting `null_on_division_by_zero` is not encoded. The profile fixes it to `false`. A DuckDB
producer with that setting enabled MUST decline every division, modulo, or function expression whose behavior it can
affect, rather than silently translating it under the profile. An embedded-DuckDB consumer MUST verify that the setting
is `false` before binding affected expressions and MUST NOT inherit a connection-local `true` value. The setting does
not exist in the v1.5.5 oracle; absence there is the fixed-false reference behavior rather than an error.

The producer sends a context-dependent predicate only after matching the profile against
`filter_evaluation_contexts`. If the profile is unsupported, a required predicate is not delegated and an advisory
predicate is omitted. If a consumer nevertheless receives a required context-dependent predicate it cannot reproduce
exactly, it fails the request. It may ignore the complete advisory predicate, but MUST NOT evaluate it using local
defaults because that could introduce false-negative pruning.

Advertising `vgi.duckdb.session.v1` without exact matching is a normative claim that the consumer's provider is
semantically compatible with the profile. This is the default interoperability policy and permits independently
updated DuckDB producers and workers. Implementations rely on the profile conformance corpus rather than requiring
identical provider builds.

Strict provider matching is optional. A producer requests it by including `vgi_context_provider_fingerprint`. Before
doing so, the producer MUST derive its local provider fingerprint and have received the identical nonempty fingerprint
in the worker's `EvaluationContextCapability`. The consumer MUST compare the value byte for byte with its advertised
fingerprint before binding or evaluating any predicate. A missing or unequal fingerprint does not satisfy strict
matching. The producer then retains a required predicate locally or omits an advisory predicate; if a mismatched
required batch is nevertheless received, the consumer fails the request, while a consumer MAY ignore all affected
advisory predicates.

The fingerprint is an opaque semantic-provider identifier, not a security attestation. An implementation that supplies
one MUST change it whenever an implementation or data change can affect the profile's observable results. DuckDB
integrations SHOULD derive it from a canonical build manifest covering the DuckDB `icu` extension implementation and
its IANA time-zone, CLDR, and Unicode/UCD data versions. Exact matching compares only the resulting identifier;
protocol implementations do not parse its components. Conservative false mismatches are permitted because strict mode
is opt-in.

### 11.4 Collations

`vgi_default_collation` supplies the default used when an expression has no explicit collation. Plain Arrow types do
not generally preserve DuckDB explicit or column-level collation expressions, and changing the batch default does not
encode such a collation. A producer MUST NOT push an operation whose result depends on an explicit or type-level
collation unless the chosen extension function contract carries that collation and the worker advertises it.

### 11.5 Errors

Errors observable under `vgi.duckdb.standard.v1` (section 6) are part of exact behavior. A native adapter cannot claim exact equivalence if it converts an error to NULL, uses different overflow behavior, accepts invalid input the profile rejects, or rejects input the profile accepts.

Advisory pruning SHOULD avoid evaluating expressions that can throw unless the pruning implementation proves that doing so cannot introduce an error absent from the calling engine's retained evaluation.
This consumer-side obligation does not relax the producer-side conjunct-splitting prohibition in section 8.3.

## 12. Validation and security

A consumer validates the complete Arrow container and JSON document before applying a snapshot or delta. Delta
validation and state changes follow the atomicity rules in section 4.4.

It MUST reject:

- the wrong filter encoding or version;
- a missing, unknown, unsupported, or changed expression-semantics profile;
- filter metadata placed only on `filter_spec` rather than on the batch schema;
- missing, unknown, malformed, or incomplete evaluation-context metadata;
- profile-specific DuckDB session entries under `vgi.none.v1`, or a context-dependent expression labeled
  `vgi.none.v1`;
- `vgi.duckdb.session.v1` without all five required setting values or without an available compatible semantic
  provider;
- an empty or longer-than-256-byte provider fingerprint;
- a required predicate whose requested provider fingerprint was not advertised identically by the consumer;
- evaluation-context metadata that changes within one scan;
- a batch with other than one row;
- a missing, NULL, or non-UTF-8 `filter_spec`;
- malformed JSON or duplicate JSON object keys;
- unknown node, operator, mode, source, document kind, or update operation;
- duplicate predicate IDs in a snapshot;
- duplicate update IDs in a delta;
- a delta upsert or removal targeting an ID introduced as required by the initial snapshot;
- an invalid or stale column/field name-to-index pairing;
- a missing or wrongly typed literal, set, or type reference;
- an expression whose root is not Boolean;
- a required call not allowed by the advertised capability contract;
- a required, nested, negated, or otherwise non-root `runtime_filter` node;
- a runtime artifact whose Arrow representation does not match its registered contract or whose bytes fail complete validation after its algorithm capability is selected;
- an invalid extension namespace, name, version, or option;
- an unknown Arrow extension type used where its semantics affect decoding, binding, or evaluation;
- cycles or references outside the current filter document; and
- a required predicate that cannot be evaluated exactly.

A syntactically valid advisory call or runtime-filter algorithm whose semantic identity is known by the protocol but unsupported by this consumer MAY cause that entire advisory predicate to be ignored. An unknown identity that cannot be validated against any known standard, extension, or runtime-filter definition is malformed rather than merely unsupported. A producer that sends an artifact the consumer did not advertise is nonconforming even though ignoring that advisory predicate preserves query correctness.

Implementations MUST enforce finite configurable limits before large allocation or recursion. Recommended defaults are:

```text
JSON bytes                 1 MiB
expression depth           64
expression nodes           10,000
predicates per snapshot    1,024
predicate IDs per scan     4,096 (live plus tombstoned)
arguments per call         256
predicate ID bytes         128
literal payload bytes      16 MiB
runtime artifact bytes     16 MiB
```

Implementations may choose lower limits and must return a clear resource-limit error. Join-key payload limits continue
to apply independently. A delta that would exceed the predicate-ID limit is rejected atomically and does not advance
any revision.

## 13. Canonicalization and caching

For diagnostics, implementations SHOULD render standard functions by symbolic name and extension functions as:

```text
namespace/name@version
```

For hashing, JSON is canonicalized with UTF-8 encoding, lexicographically ordered object keys, no insignificant whitespace, and original array order. Producers SHOULD flatten nested `and` and `or` nodes but MUST NOT reorder children merely to improve cache hits; evaluation errors can make order observable.

A semantic cache identity includes:

- every initial expression and mode;
- the selected expression-semantics profile;
- the complete evaluation-context profile, its setting values, and the provider fingerprint when present;
- the typed Arrow representation and field metadata of every referenced literal and cast type;
- every referenced external set identity and content;
- every runtime-filter algorithm identity and immutable artifact content; and
- the applicable extension semantic versions.

Cache implementations MAY represent external-set or artifact content by an algorithm-tagged collision-resistant
digest of its canonical typed Arrow content. The digest substitutes for the content in the cache key; it does not make
schema, field metadata, logical type, NULL placement, or algorithm identity optional.

Diagnostic predicate IDs, revisions, and sources are excluded from the semantic identity.

A scan receiving any runtime delta is not result-cache eligible unless the cache implementation proves independence from dynamic predicates. A static advisory snapshot may be cached only when the complete advisory expression and the worker's application policy are included in the key; conservative implementations should disable result caching for advisory scans.

## 14. Examples

### 14.1 Required single-column predicate

SQL:

```sql
WHERE amount >= 100::DECIMAL(18,2)
```

JSON:

```json
{
  "encoding": "vgi.filters.v2",
  "semantics": "vgi.duckdb.standard.v1",
  "kind": "snapshot",
  "predicates": [{
    "id": "query:0",
    "revision": 0,
    "mode": "required",
    "source": "query",
    "expression": {
      "node": "comparison",
      "op": "ge",
      "left": {"node": "column_ref", "column_index": 4, "column_name": "amount"},
      "right": {"node": "literal", "value_ref": 0}
    }
  }]
}
```

`value_0` is a one-row Arrow `decimal128(18,2)` field.

### 14.2 DuckDB 2.0 multi-column advisory predicate

SQL:

```sql
WHERE start_time < end_time AND tenant_id = 7
```

The expression contains two independently validated column references. The DuckDB 2.0 adapter sends it as advisory when using the partial multi-column hook, and DuckDB retains the exact local filter.

### 14.3 Standard function from Polars

Polars:

```python
pl.col("path").str.starts_with("s3://warehouse/")
```

VGI expression:

```json
{
  "node": "call",
  "function": "starts_with",
  "arguments": [
    {"node": "column_ref", "column_index": 0, "column_name": "path"},
    {"node": "literal", "value_ref": 0}
  ]
}
```

The Polars adapter retains the full native predicate and marks the VGI predicate advisory.

### 14.4 Worker with DataFusion beneath it

A worker receives required expression `starts_with(path, 's3://warehouse/')`.

1. Its DataFusion adapter translates the expression and offers it to the underlying provider as inexact/advisory pruning.
2. DataFusion or the provider may skip files and row groups.
3. The worker applies the original VGI expression exactly with embedded DuckDB before returning rows.

If the provider later supplies a tested exact guarantee, step 3 can be removed for that mapping without changing the wire protocol.

### 14.5 Spatial extension

```json
{
  "node": "call",
  "function": {
    "namespace": "duckdb.spatial",
    "name": "intersects_extent",
    "version": 1
  },
  "arguments": [
    {"node": "column_ref", "column_index": 5, "column_name": "geom"},
    {"node": "literal", "value_ref": 0}
  ]
}
```

The worker must advertise that exact extension identity. `value_0` preserves its GeoArrow extension metadata. A worker without the capability never receives this as required.

## 15. Conformance suite

VGI 2.0 ships one language-neutral filter corpus. Each case contains:

- the unprojected Arrow input schema and record batches;
- the filter `RecordBatch`;
- optional external join-key batches;
- the expected three-valued Boolean result;
- the expected filtered rows; or
- the expected protocol/binding/evaluation error.

Every SDK decoder and evaluator must pass the applicable corpus. Engine adapters additionally run differential tests against the reference embedded-DuckDB evaluator.

Required positive cases include:

1. every comparison operation with NULLs;
2. `IS NULL`, `IS NOT NULL`, `AND`, `OR`, and `NOT` truth tables;
3. cross-column comparison;
4. projected output that omits a filtered column;
5. repeated nested field references beyond one level, including a NULL top-level struct, a NULL intermediate struct,
   and a NULL leaf field whose child storage contains otherwise readable values;
6. duplicate-looking and case-varied column names resolved by index plus name;
7. typed NULL, decimal, timestamp, interval, UUID, blob, list, struct, dictionary, and extension literals;
8. `IN` and `NOT IN` with empty sets, NULL needles, and NULL candidates;
9. inline `IN` element lists and external exact key batches producing identical results;
10. every standard function, including NULL and empty-string cases;
11. static required and advisory predicates;
12. dynamic upsert, replacement, removal, duplicate revision, stale revision, and out-of-order delivery;
13. external join-key references;
14. a DataFusion deeper-pushdown path with an exact VGI residual;
15. an unsupported known runtime-filter algorithm causing its complete advisory predicate to be ignored;
16. session-context-sensitive casts evaluated under at least two time zones and across a daylight-saving transition;
17. `TIMESTAMPTZ` temporal binning under at least two calendars;
18. an uncollated string comparison under both binary and a supported non-binary default collation;
19. NaN and negative-zero comparison and membership behavior;
20. an empty snapshot and an empty no-op delta;
21. atomic application of a valid multi-update delta;
22. artifact-only pruning when `filter_pushdown` is false and the exact runtime algorithm is advertised;
23. `vgi.none.v1` used without profile-specific metadata for context-independent DuckDB, DataFusion, and Polars
    predicates;
24. `vgi.duckdb.session.v1` evaluated with DuckDB's `icu` extension loaded and all five settings applied;
25. ordinary DuckDB-session compatibility across two consumers advertising the same profile without requesting a
    fingerprint;
26. successful strict matching when producer, advertised capability, and filter batch carry the identical provider
    fingerprint; and
27. core-node values, result types, NULLs, and error categories matching upstream DuckDB v1.5.5 under
    `vgi.duckdb.standard.v1`, with Haybarn v1.5.5-rc1 passing the same corpus.

Exact-evaluation parity cases include arithmetic overflow, integer and floating-point division by zero, and failing
casts. Each case verifies whether evaluation returns a value or raises an error, along with the error category required
by `vgi.duckdb.standard.v1` (section 6). These cases cover required evaluation and advisory evaluation when the worker
chooses to execute the complete expression exactly.

Arithmetic session-context cases additionally bind and evaluate the same expressions under both values of
`vgi_ieee_floating_point_ops` and `vgi_integer_division`. They verify infinity/NaN versus DuckDB error behavior for
floating-point division and modulo, and both the result value and logical result type of integer `/`. The worker cases
begin with conflicting local settings to prove that the transmitted values are applied before parsing and binding.
Producer cases verify that an enabled deprecated `null_on_division_by_zero` causes affected expressions to be declined.

Required negative cases include:

1. unknown nodes and functions;
2. unsupported required extension functions;
3. malformed or mismatched indexes and names;
4. missing literal, type, or external-set references;
5. an invalid function overload;
6. a non-Boolean root;
7. an unsupported collation-dependent expression;
8. missing, incomplete, unknown, or changing evaluation context;
9. a context-dependent cast sent without a negotiated evaluation-context profile;
10. a context-dependent expression labeled `vgi.none.v1` or DuckDB session fields supplied under that profile;
11. `vgi.duckdb.session.v1` with missing settings or unavailable DuckDB ICU semantics;
12. strict matching requested without an identically advertised fingerprint, or with an empty or oversized
    fingerprint;
13. a DuckDB session profile with a noncanonical Boolean setting value or changed arithmetic setting during a scan;
14. an affected division or modulo expression sent after the producer observed `null_on_division_by_zero=true`;
15. over-depth and over-size inputs;
16. attempted partial serialization under `OR`;
17. a required expression with any unsupported descendant;
18. an `IN` list truncated because of a transport limit;
19. a runtime artifact used as required, nested, negated, or emitted without capability matching;
20. an artifact-only worker receiving an ordinary core or function-call predicate;
21. a delta targeting an initial required predicate ID;
22. a malformed applicable update causing the complete delta to roll back without advancing any revision;
23. duplicate update IDs or exhaustion of the per-scan predicate-ID limit;
24. an unknown Arrow extension type or extension metadata silently degraded to its storage type;
25. extracting a throwing conjunct such as `10 / x > 1` from a multi-conjunct expression and transmitting it as an
    independently evaluated advisory predicate; and
26. a missing, unsupported, or scan-changing `semantics` value, including a v2-only behavior mislabeled as
    `vgi.duckdb.standard.v1`.

Algorithm-specific runtime-filter tests are defined by [VGI Runtime-Filter Artifacts](vgi-runtime-filter-artifacts.md).

## 16. Migration from filter encoding v1

VGI 2.0 performs a direct replacement:

1. Change the filter metadata marker from `1` to `2` and require `vgi.filters.v2`.
2. Replace the top-level list of column-scoped filter objects with snapshot/delta predicate entries.
3. Replace `ConstantFilter`, `InFilter`, `StructFilter`, and `ExpressionFilter` wire families with builders or views over the unified AST.
4. Make every `column_ref` independently identify a bind-schema column.
5. Replace raw `function_name` with a standard symbolic enum or structured extension identity.
6. Remove `supported_expression_filters` and add expression-semantics, extension-function, runtime-filter algorithm,
   and structured evaluation-context capabilities.
7. Preserve typed literals in Arrow payload fields and add typed cast fields.
8. Preserve the large exact-`IN` path through typed external join-key batches; never truncate an element list.
9. Reserve capability-gated root advisory `runtime_filter` predicates and `artifact_N` payload slots for separately specified runtime-filter algorithms.
10. Implement optional artifacts only under the companion runtime-filter specification.
11. Replace dynamic array-position behavior with predicate IDs and revisions.
12. Make required decoding and evaluation atomic; remove all silent child dropping.
13. Mark context-independent batches with `vgi.none.v1`; for negotiated DuckDB-session batches, carry the bound time
    zone, calendar, default collation, IEEE floating-point policy, and integer-division policy plus an optional strict
    provider fingerprint as immutable schema metadata.
14. Select `vgi.duckdb.standard.v1` in every initial filter document and pin it to upstream DuckDB v1.5.5 semantics.
15. Reject every VGI 1.x peer before bind or execution.

The Python SDK should implement the schema, generated model, validation, reference evaluator, and corpus first. Other language SDKs consume the generated schema and implement decoding/evaluation without designing independent wire shapes. DuckDB-specific work follows [VGI DuckDB Filter Adapter](vgi-duckdb-filter-adapter.md).

## 17. Decision summary

This proposal makes the following choices:

- VGI 2.0 has one filter encoding and no v1 compatibility mode.
- The wire is a unified expression AST inside the existing Arrow IPC container.
- `vgi.duckdb.standard.v1` pins core and standard-function behavior to upstream DuckDB v1.5.5; Haybarn
  v1.5.5-rc1 is a conforming reference distribution.
- Stable DuckDB 2.0 behavior will be introduced as `vgi.duckdb.standard.v2`, without changing v1.
- Embedded DuckDB is the recommended portable evaluator.
- DataFusion native pushdown is an optional deeper optimization with a residual correctness backstop.
- Polars is treated primarily as a client and retains its native residual.
- Acero-native translation is not an initial requirement.
- Standard function names are readable strings, not numeric IDs.
- Signatures are inferred and validated rather than repeated in calls.
- Extension functions use structured namespace, name, and version identities.
- Substrait is not a protocol dependency.
- Required predicates are atomic and exact; advisory predicates may be ignored or safely weakened.
- Every filter batch selects an immutable context profile; `vgi.none.v1` carries no settings, while negotiated
  `vgi.duckdb.session.v1` carries DuckDB time-zone, calendar, default-collation, IEEE floating-point, and
  integer-division values.
- DuckDB-session profiles use semantic compatibility by default and optionally enforce exact opaque provider
  fingerprint matching.
- Dynamic predicates have stable IDs and monotonically increasing revisions; each delta commits atomically.
- Small `IN` element lists are inline typed Arrow list scalars; large exact sets use external Arrow key batches.
- Runtime-filter capability is independent of ordinary expression-filter pushdown, permitting artifact-only workers.
- Runtime pruning uses a distinct capability-gated, root-only advisory `runtime_filter` node with immutable `artifact_N` payloads.
- Runtime algorithm contracts and engine-specific adapter mappings are separate companion specifications.
