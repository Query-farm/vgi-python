# Optional scalar projection expressions

This contract is part of the combined, unpublished DuckDB 2.0 protocol
transition. It can be implemented and tested while the native adapter still
builds against DuckDB 1.5. It does not change the published protocol version.

## Offer and proposal

`BindResponse.projection_expression_profiles` is an ordered list of semantic
profiles that the provider is willing to consider for this particular bind.
The default is empty. An empty offer means ordinary scans and local expression
evaluation. Existing `FunctionInfo.projection_pushdown` continues to mean
selection of existing columns; it grants no expression-evaluation support.
Offers contain at most 64 distinct versioned profile identifiers, each at most
256 characters. The initial implementation recognizes
`vgi.expression.duckdb.scalar.v1`.

The client may call `table_function_projection` after ordinary bind and before
replacing any local expression. This is a pure planning operation: it must not
scan rows, mutate data, or allocate a resource requiring a release call. A
provider may decline every expression even when it offered the profile.

| Record / field | Meaning |
| --- | --- |
| `TableFunctionProjectionRequest.bind_call` | The original serialized `BindRequest`, including attachment, arguments, owner path, settings, transaction and time-travel context. |
| `bind_result` | The original serialized `BindResponse`, including its exact source schema and opaque provider state. |
| `expressions` | One to 1,024 serialized canonical `SqlExpression` records. Each profile must be offered by this bind. Each owner `schema_path` must equal the bind owner. |
| `result_schema` | An Arrow schema with one requested result field per expression, in the same order. Field names, types, nullability and field metadata are part of the contract. Schema-level metadata must be empty. |
| `TableFunctionProjectionResponse.accepted_indices` | Strictly increasing, unique, zero-based indices into the proposed expressions. The empty list means complete decline. |
| `bind_result` | Null on complete decline; otherwise the complete replacement serialized `BindResponse`. |

The expression payload reuses `SqlExpression` rather than introducing a second
SQL dialect or an untyped metadata bag. It includes SQL text, a versioned
semantic profile, raw owner path and an explicit `QueryEvaluationContext`.
Filter v2 remains the Boolean predicate contract; its call nodes are not
reinterpreted as scalar result expressions.

The request requires an ordinary TABLE bind with an explicit owner and no
relational input schema. Source and result field names must be nonempty,
NUL-free and unique under ASCII case-insensitive comparison across both schemas. All proposed
expressions share one evaluation context. Canonical request/response envelopes
are bounded to 8 MiB, including nested records.

## Result positions and partial acceptance

An accepted schema preserves every original source field, in order and with
its original metadata. The accepted result fields append in the order of
`accepted_indices`. Schema-level metadata is preserved from the source schema.
There is no source-column renumbering or separate result-mapping object.

For example, a source schema `[n, label]` and proposed results `[wide_n,
label_size, other]` with `accepted_indices = [0, 2]` produce exactly
`[n, label, wide_n, other]`. DuckDB replaces only the corresponding accepted
expressions with references to slots 2 and 3. It retains `label_size` and its
inputs for local evaluation. Selection/reordering of the resulting scan fields
continues to use the existing `projection_ids` contract.

The accepted bind result must keep the original profile list and
`repeatability` value unchanged and
must contain no secret-scope request fields. Its `opaque_data` is the complete
replacement provider bind state: the provider must retain any original state
it will need. Existing init, secondary init and HTTP continuation carry that
state and the expanded output schema. No preparation ticket, token registry,
expiration or release method is introduced.

## Validation and fallback

The client validates the entire response before changing its plan. Out-of-range,
duplicate or unordered indices, a missing accepted bind result, state returned
on complete decline, changed original fields, wrong appended fields, changed
offers and secret-scope requests are protocol errors. Exact schema validation
includes field metadata and nullability, not only logical type names.

An unoffered feature or a valid decline leaves the original scan and local
expressions intact. A malformed acceptance or failed RPC is an error. Once
accepted, an execution failure remains an execution failure; the client must
not rerun the scan locally as recovery. This optimization grants no mutation,
retry or repeatability guarantee.

Moving expression evaluation into a scan can change when an error occurs
relative to a local filter. An adapter must prove equivalence before making a
proposal. In particular, deterministic does not imply safe: narrowing casts,
parsing strings, division and overflowing arithmetic may throw for rows that
a local filter would discard. Unsupported candidates remain local.

## Initial native adapter boundary

The first DuckDB 1.5 adapter proof is ordinary catalog table scans and total
`INTEGER` to `BIGINT` casts. It preserves the local projection and accepts
only unambiguous source-column references. Generated-table and direct function-call scans,
nested extraction, throwing casts, arbitrary functions, multi-branch and
partitioned scans require separate qualification. Accepted scans disable
result caching, split enumeration and optimizer hints whose identity or column
coordinates have not been qualified for the expanded schema.

Bind copies must retain accepted schema/state. A serialized/rebound plan must
either retain the accepted contract or reject the operation explicitly; it
must never silently rebind the original scan while retaining references to
appended result slots. These restrictions are adapter limits, not permission
for a provider to return a different result schema.

The initial adapter disables prepared-statement bind reuse when this profile
is offered, before optimization starts. Each prepared execution binds and
negotiates again. Restoration of an accepted serialized scan requires the
original bind identity and complete original bind response, including opaque
provider state, to remain identical; otherwise restoration fails explicitly.

The separate [bound scan repeatability contract](scan-repeatability.md) defines
that promise. Projection acceptance preserves it; accepting an expression does
not itself grant it.

The Go SDK's RPC dependency unwraps the outer request before the SDK handler
receives it. The isolated dependency now has a source fix for trailing data,
extra batches, truncated messages and the raw outer 8 MiB limit. It preserves
valid physical-EOF and explicit-EOS stream endings under the
[Arrow streaming format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format).
The projection parameter type opts into a raw-byte limit before Arrow decoding;
other methods retain their existing size policy. The SDK has a compile-time
dependency on this limit interface, so shipping it requires the corresponding
RPC dependency release/pin. Remote dependency and public-RPC qualification is
pending; this source checkpoint does not claim complete transport conformance
or protocol freeze.
