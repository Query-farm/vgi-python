# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Protocol-3 partition-distribution record tests."""

from __future__ import annotations

import pyarrow as pa
import pytest

from vgi.protocol import (
    FieldPath,
    KeyGrouping,
    OrderByDirection,
    OrderByNullOrder,
    PartitionTransform,
    PlanResponse,
    RangeKey,
    RangePartitioning,
    ScanSplit,
    TransformSpec,
)


def encoded(value: object) -> bytes:
    """Serialize one Arrow record into its IPC wire bytes."""
    return value.serialize_to_bytes()  # type: ignore[attr-defined, no-any-return]


def identity(name: str) -> TransformSpec:
    """Build an identity transform over one top-level field."""
    return TransformSpec(
        id="vgi.identity/1",
        inputs=[encoded(FieldPath(components=[name]))],
    )


def test_transform_parameters_are_named_typed_and_not_limited_to_one_int64() -> None:
    """Transform parameters retain multiple names and Arrow types."""
    parameters = pa.record_batch(
        [pa.array([16], type=pa.int32()), pa.array(["murmur3"], type=pa.string()), pa.array([True])],
        names=["buckets", "algorithm", "nulls_equal"],
    )
    transform = TransformSpec(
        id="example.bucket/2",
        inputs=[encoded(FieldPath(components=["customer", "id"]))],
        parameters=parameters,
    )

    decoded = TransformSpec.deserialize_from_bytes(encoded(transform))
    assert decoded.id == "example.bucket/2"
    assert decoded.parameters is not None
    assert decoded.parameters.equals(parameters)


def test_protocol_2_partition_transform_remains_decodable_during_migration() -> None:
    """Protocol 3.1 retains the deprecated producer-side compatibility type."""
    transform = PartitionTransform(column="customer_id", transform="bucket", param=16)

    assert PartitionTransform.deserialize_from_bytes(encoded(transform)) == transform


def test_key_grouping_and_range_partitioning_round_trip_independently() -> None:
    """Grouping and range claims coexist without sharing a kind enum."""
    transform = identity("event_time")
    grouping = KeyGrouping(keys=[encoded(transform)])
    range_key = RangeKey(
        transform=encoded(transform),
        direction=OrderByDirection.ASC,
        null_order=OrderByNullOrder.NULLS_LAST,
    )
    ranges = RangePartitioning(
        keys=[encoded(range_key)],
        split_points=pa.record_batch([pa.array([10, 20], type=pa.int64())], names=["event_time"]),
    )
    key_value = pa.record_batch([pa.array([10], type=pa.int64())], names=["event_time"])
    response = PlanResponse(
        splits=[encoded(ScanSplit(payload=b"split-1", key_value=key_value, range_index=1))],
        key_grouping=encoded(grouping),
        range_partitioning=encoded(ranges),
    )

    decoded = PlanResponse.deserialize_from_bytes(encoded(response))
    assert KeyGrouping.deserialize_from_bytes(decoded.key_grouping).keys == grouping.keys  # type: ignore[arg-type]
    decoded_ranges = RangePartitioning.deserialize_from_bytes(decoded.range_partitioning)  # type: ignore[arg-type]
    assert decoded_ranges.split_points.equals(ranges.split_points)
    decoded_split = ScanSplit.deserialize_from_bytes(decoded.splits[0])  # type: ignore[arg-type]
    assert decoded_split.range_index == 1
    assert decoded_split.key_value is not None and decoded_split.key_value.equals(key_value)


def test_distribution_records_reject_ambiguous_shapes() -> None:
    """Malformed parameter batches and split-point widths fail closed."""
    with pytest.raises(ValueError, match="exactly one row"):
        TransformSpec(
            id="example.bucket/1",
            inputs=[encoded(FieldPath(components=["id"]))],
            parameters=pa.record_batch([pa.array([8, 16])], names=["buckets"]),
        )

    with pytest.raises(ValueError, match="width"):
        RangePartitioning(
            keys=[
                encoded(
                    RangeKey(
                        transform=encoded(identity("id")),
                        direction=OrderByDirection.ASC,
                        null_order=OrderByNullOrder.NULLS_LAST,
                    )
                )
            ],
            split_points=pa.record_batch(
                [pa.array([10]), pa.array([20])],
                names=["id", "extra"],
            ),
        )
