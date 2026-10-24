# Bound scan repeatability

This is a proposed successor contract in the combined, unpublished DuckDB 2.0
transition. The native implementation continues to target DuckDB 1.5.x.
Source implementation is underway; remote runtime qualification is pending.

## Wire field

Append `BindResponse.repeatability` after `projection_expression_profiles`.
It is a required, non-null Arrow `dictionary<int16, utf8>` column. Dictionary
values use the enum names below, exactly and case-sensitively. Missing, null
or unknown values are malformed successor messages. SDK author APIs default
to `UNKNOWN`; opting into a guarantee is optional, with no capability gate.

| Value | Promise |
| --- | --- |
| `UNKNOWN` | No guarantee that equivalent scans return the same logical rows. |
| `STABLE_WITHIN_QUERY` | Equivalent scans derived from the same complete bound state return the same logical rows and values within one statement. |

The claim is about that bind's arguments, attachment, transaction/time-travel
context and complete provider state. It does not unify separately bound calls
into a common snapshot. Changing the projected columns or applying an admitted
filter must preserve the corresponding logical result. Ordering and batch
boundaries continue to follow the existing order contract. Sampling, dynamic
filters and other execution modifiers require their own engine-side checks.

The promise does not authorize retries, supply cross-query cache validity or
imply serializable transactions. Scalar stability, read-only metadata, cache
TTL and catalog version alone cannot establish it. Existing opaque bind state
or an existing transaction snapshot supplies the provider's consistency
anchor; there is no new query identifier, coordinator, token or release RPC.

## State lifetime

A provider must retain enough state to honor repeated evaluations without
silently refreshing to newer data. Self-contained snapshot state in
`opaque_data` is one implementation; an existing anchored transaction is
another. If the state expires or becomes unavailable, execution fails. It
must not silently obtain a replacement snapshot under the original promise.

Bind copies preserve the entire original state. Serialization/restoration must
either preserve it or reject a changed rebind explicitly. A prepared statement
execution obtains fresh bind state for the new statement. Separate executions
may therefore observe newly committed data without weakening either claim.

Projection acceptance preserves `repeatability` exactly, including `UNKNOWN`.
Its replacement opaque state must retain the original consistency anchor along
with any additional projection state. Accepting an expression cannot upgrade
or downgrade the original promise.

## Native adapter boundary

The pinned DuckDB 1.5 adapter decodes and stores the claim, preserves it in
`FunctionData::Copy`, and checks projection replacement. Stable binds disable
prepared-statement bind reuse. Stable inline catalog bind results trigger a
fresh statement bind, and stable scans bypass the result cache because its
current identity omits opaque provider snapshot state. Serialized table-plan
restoration rejects a changed original request or complete bind result.

DuckDB 1.5 has no `is_repeatable` callback; this change enables no new optimizer
rewrite there. The DuckDB 2.0 port must consult this claim only after its own
sampling/dynamic-filter checks and address the catalog-table shortcut in
`logical_operator_repeatability.cpp`, which classifies a table-backed scan
before consulting the table-function callback. Wiring a callback alone would
not make `UNKNOWN` conservative for VGI catalog tables.

The mutable SQLite reference in `tests/repeatability_reference.py` captures a
typed snapshot during bind. Its tests mutate committed source data between
scans of one bind, then create a fresh bind. They also cover signed HTTP,
continuation state, stale inline metadata and projection preservation. Native
tests cover copies, changed-state rejection, prepared executions and cache
exclusion. These tests are authored; passing runtime evidence must be recorded
after the EC2 build/test host is available.
