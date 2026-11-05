# Partition distribution metadata

Protocol 3.1 describes scan distribution with two independent facts:

- `PlanResponse.key_grouping` says that every row produced by one split has one
  exact transformed key tuple. This is the contract needed by Spark's
  key-grouped input partitions.
- `PlanResponse.range_partitioning` says that splits belong to exact, ordered
  ranges separated by shared split points. This is the contract needed by
  DataFusion's range partitioning.

Neither field implies the other. `sort_order` continues to describe order
*within* a split, while `ScanSplit.column_bounds` contains pruning statistics
only. A client must not promote bounds, Hive-style directory values, or the
deprecated protocol-2 `partitioning` field into an exact distribution claim.

## Transform specification

A `TransformSpec` contains:

- `id`: a versioned registry name such as `vgi.identity/1` or
  `iceberg.bucket/1`;
- `inputs`: one or more serialized `FieldPath` records; and
- optional `parameters`: an Arrow IPC record batch containing exactly one row.

`FieldPath.components` preserves identifier boundaries. For example,
`["customer", "region.code"]` means nested field `region.code` under
`customer`; it is not the three-part dotted path `customer.region.code`.

The one-row parameter batch may have any number of named, typed columns. This
allows a transform to carry, for example, an `int32` bucket count, a string
calendar name, and a Boolean mode flag without adding transform-specific wire
fields. Parameter names, Arrow types, null values, and transform-registry
semantics are all part of the transform identity. Arbitrary recursive
expressions are deliberately outside this contract.

The initial portable profile contains only `vgi.identity/1`, with exactly one
input and no parameter batch. An adapter that does not implement a transform's
registered semantics exactly must report unknown partitioning. It must not
approximate another engine's bucket, truncate, hash, collation, or time-zone
rules.

## Exact key grouping

`KeyGrouping.keys` is the ordered tuple of transforms. When it is present:

1. every ordinary split has a `key_value` batch;
2. that batch has exactly one row and one column per transform, in key order;
3. each column has the registered output type of its transform; and
4. every row emitted by the split evaluates to that tuple under null-safe
   equality.

Several splits may have the same key. A split without a key, or a key whose
shape or type is invalid, makes the grouping claim unusable. The no-split
sentinel does not carry a grouping claim.

Spark maps a supported grouping to `KeyGroupedPartitioning`; every physical
input partition exposes its tuple through `HasPartitionKey`. The current Spark
adapter accepts only the portable identity profile. Mixed scan branches must
advertise the same grouping, otherwise the complete scan reports unknown
partitioning.

The DuckDB 2 adapter uses the same exact tuple for
`SINGLE_VALUE_PARTITIONS`, but only when the function has already declared
that optimization at bind time and its annotated partition fields exactly
match the identity-key sequence. The plan metadata cannot independently opt a
function into the optimization because it arrives after physical planning.
Workers without a supported grouping continue to use the existing per-batch
`vgi_partition_values` metadata path.

## Exact range partitioning

`RangePartitioning.keys` is an ordered tuple of `RangeKey` records. Each range
key combines a transform with an ascending/descending direction and a null
ordering. `split_points` has one column per range key, using each transform's
output type.

For `N` split-point rows there are `N + 1` ranges. The rows must be strictly
ordered by the declared lexicographic comparator. A value equal to split point
`i` belongs to range `i + 1`; ranges are therefore half-open at their lower
boundary except for the first range. Each ordinary split supplies a zero-based
`range_index`, and all of its output belongs to that exact range. More than one
split may share a range, and a range may contain no splits.

DataFusion maps this metadata to physical `RangePartitioning` only when it can
preserve one output partition per range within the requested parallelism and
worker cap. It currently accepts only top-level `vgi.identity/1` keys whose
split-point Arrow types match the output schema. Otherwise it retains the
splits but reports `UnknownPartitioning`.

## Pagination and compatibility

Distribution metadata is a property of the complete plan. Every page reached
through `next_cursors` must repeat byte-identical `key_grouping` and
`range_partitioning` values, and the split-local values on that page must obey
them. A client must discard the distribution claim or reject a malformed plan
if pages disagree; it must never combine inconsistent claims.

Protocol 3.1 producers should use `key_grouping`, `range_partitioning`,
`key_value`, `range_index`, and `column_bounds`. The deprecated
`PlanResponse.partitioning` and `ScanSplit.partition_bounds` fields remain on
the wire for protocol-2 compatibility, but do not carry the exact semantics
required by the new engine mappings.
