# VGI Runtime-Filter Artifacts

**Status:** Proposed optional extension specification
**Target protocol:** VGI 2.0.0
**Depends on:** [VGI Filter Encoding v2](vgi-filter-encoding-v2-spec.md)

## 1. Scope

This specification defines optional transport for probabilistic or approximate runtime pruning artifacts, such as Bloom and prefix-range filters. It does not change query semantics and is not required for VGI 2.0 conformance.

Every runtime artifact is advisory. The calling engine retains the exact join, Top-N, or query predicate. A worker may ignore an artifact and still produce correct results.

## 2. Wire node

Filter Encoding v2 reserves a `runtime_filter` expression node:

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

`artifact_ref: N` resolves to the one-row `artifact_N` field in the filter `RecordBatch`. Its Arrow type, extension metadata, and byte layout are defined by the selected algorithm version.

`input` is the value tested by the artifact. It may contain deterministic casts required to reproduce the producer's lookup input.

`null_handling` is:

- `pass`: retain a NULL input; or
- `reject`: prune a NULL input, permitted only when the retained exact predicate also rejects it.

## 3. Correctness contract

A runtime-filter lookup is one-sided:

- `false` asserts that the row cannot satisfy the retained exact predicate;
- `true` means only that the row might satisfy it;
- false positives are permitted; and
- false negatives are forbidden.

The following rules are normative:

- the enclosing predicate uses `mode: "advisory"`;
- `runtime_filter` is the root of its own predicate entry;
- it does not occur under `not`, `or`, `and`, comparisons, casts, or calls;
- it is used only positively and never discharges an exact residual;
- a newer predicate revision replaces the complete immutable artifact; and
- a known but unsupported algorithm causes the entire advisory predicate to be ignored.

Separate predicate entries are implicitly combined with `AND` by the Filter Encoding v2 snapshot/delta envelope.

## 4. Payload and capability negotiation

Filter `RecordBatch` payload fields use:

```text
artifact_0: <algorithm-defined Arrow type>
artifact_1: <algorithm-defined Arrow type>
...
```

Each field contains exactly one immutable artifact. References resolve by exact field name.

Table functions advertise algorithms separately from scalar functions:

```text
runtime_filter_algorithms: list<RuntimeFilterAlgorithmCapability>
```

This capability is independent of ordinary expression pushdown. A table function may advertise one or more runtime
algorithms while setting `filter_pushdown: false`; it then accepts only advisory predicates whose root is
`runtime_filter`, including only the input expression forms permitted by the selected algorithm contract. If both
`filter_pushdown` is false and this list is empty, the producer sends no predicates.

The worker also advertises the filter document's selected expression profile in `filter_semantic_profiles`, even when
ordinary pushdown is false. This permits interpretation of the runtime filter's input expression but does not enable
ordinary predicate roots.

A capability has this shape:

```json
{
  "namespace": "duckdb.runtime_filter",
  "name": "prefix_range",
  "version": 1
}
```

Advertising an algorithm means the consumer implements its complete artifact contract. A producer emits it only when it can export or construct that exact version and the consumer advertised it. Algorithm parameters are encoded in the artifact, not negotiated at each node.

An engine-owned pointer, mutable buffer address, or undocumented private layout is never a valid VGI artifact.

## 5. Algorithm registration requirements

Every registered algorithm version defines and tests:

- immutable Arrow representation and field metadata;
- byte layout, endianness, checksums, and bounds validation;
- construction parameters and lookup algorithm;
- supported logical and physical input types;
- normalization for decimals, timestamps, strings, floating-point zero/NaN, and extension types;
- exact pre-lookup casts;
- NULL behavior;
- maximum encoded and decoded sizes;
- absence of false negatives; and
- import/evaluation behavior for every advertising implementation.

An incompatible change receives a new algorithm version.

## 6. DuckDB Bloom candidate

The candidate identity is:

```text
duckdb.runtime_filter/bloom@1
```

DuckDB 1.5 and 2.0 can build Bloom filters for hash-join probe pruning. The current native `BloomFilter` owns a private sector buffer and exposes no stable immutable export/import contract. Its bind data carries a process-local object pointer.

Consequently, an implementation must not advertise `duckdb.runtime_filter/bloom@1` merely because its DuckDB version contains `BloomFilter`. Support requires either:

- a stable DuckDB snapshot exporter and matching importer/evaluator; or
- access to the exact build keys so the adapter can construct a separately specified artifact.

Until then, VGI uses inline exact `IN` lists, external Arrow key batches, and exposed min/max predicates. A Bloom artifact never replaces exact `IN` semantics.

The Bloom v1 contract must additionally fix sector and bit counts, hash algorithm and version for every supported type, and all construction seeds or constants.

## 7. DuckDB prefix-range candidate

The candidate identity is:

```text
duckdb.runtime_filter/prefix_range@1
```

DuckDB 2.0 adds a probabilistic prefix-range bitmap for eligible equality-join keys. Active filters appear through the `__internal_tablefilter_prefix_range` wrapper inside `ExpressionFilter`; `LEGACY_PREFIX_RANGE_FILTER = 12` remains for compatibility.

The current native object is process-local and has no stable export/import contract. An adapter therefore recognizes but omits it until a conforming exporter and evaluator exist.

The prefix-range v1 contract must additionally define:

- numeric minimum and span encoding;
- bitmap length and bucket shift;
- string prefix-to-comparable conversion;
- range lookup behavior;
- supported integral and string representations; and
- behavior when the build range cannot be represented.

The private DuckDB bitmap layout must not be copied into `artifact_N` unless DuckDB makes that exact version a supported immutable interchange format.

## 8. Runtime update example

After an equality-join build completes, a capable producer may send:

```json
{
  "encoding": "vgi.filters.v2",
  "semantics": "vgi.duckdb.standard.v1",
  "kind": "delta",
  "updates": [{
    "operation": "upsert",
    "id": "join:3:prefix-range",
    "revision": 1,
    "mode": "advisory",
    "source": "join",
    "expression": {
      "node": "runtime_filter",
      "algorithm": {
        "namespace": "duckdb.runtime_filter",
        "name": "prefix_range",
        "version": 1
      },
      "input": {"node": "column_ref", "column_index": 2, "column_name": "customer_id"},
      "artifact_ref": 0,
      "null_handling": "reject"
    }
  }]
}
```

This is legal only after capability matching and only when `artifact_0` conforms to the registered algorithm. Without either condition, the producer sends no runtime-filter update.

## 9. Validation and limits

A consumer validates the complete artifact before lookup. It rejects malformed types, metadata, lengths, checksums, offsets, algorithm parameters, or NULL policy. Validation occurs before allocating from artifact-controlled sizes.

Recommended default maximum artifact payload is 16 MiB per predicate. Implementations may choose a lower advertised operational limit.

Unknown algorithm identities are malformed. Known but unsupported identities may be ignored because the predicate is advisory. Sending an algorithm the consumer did not advertise is a producer conformance failure even though ignoring it preserves correctness.

## 10. Conformance tests

Every registered algorithm includes:

- positive lookup vectors for every supported input type;
- absent-key, boundary, empty-build, NULL, NaN, and cast cases;
- differential tests against the producing engine;
- randomized tests demonstrating no false negatives;
- artifact replacement, duplicate revision, and stale revision tests;
- ignored known-but-unsupported capability tests; and
- malformed type, format, checksum, bounds, size, and version tests.

The initial DuckDB adapter suite also tests equality-join build cardinalities of 1, 2, the effective
`dynamic_or_filter_threshold`, and threshold plus 1, including nondefault and zero thresholds. It verifies that no
inline set, external-set reference, or runtime artifact is emitted above the threshold and that exposed min/max
narrowing remains advisory.

Negative protocol cases include an artifact used as required, nested, negated, placed under `OR`, decoded under the wrong algorithm version, or used without retaining the exact residual.

## 11. Initial implementation decision

VGI 2.0 reserves the runtime-filter node, artifact slots, capability field, and the two candidate DuckDB identities. Neither DuckDB candidate is advertised initially. Native Bloom and prefix-range objects stay local until their v1 artifact contracts and stable exporters/evaluators or independent constructors are complete.

Until such an exporter exists, a DuckDB producer's join-runtime contribution is limited to exposed min/max narrowing
and exact `IN` sets that DuckDB itself generates within the effective `dynamic_or_filter_threshold` setting. That
setting defaults to 50; DuckDB generates the dynamic `IN` form only for an equality join with more than one and no more
than the configured number of build keys. The producer MUST NOT manufacture a larger inline set, treat
`InitRequest.join_keys` as a tick-time update channel, or serialize DuckDB's private Bloom state. A larger build-key set
therefore contributes only any independently exposed min/max narrowing, or no VGI runtime predicate at all. This is an
initial implementation limit, not a claim that Filter Encoding v2 transports arbitrary large dynamic sets.
