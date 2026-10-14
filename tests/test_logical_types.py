# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Portable logical types preserve identity and reject unsafe reinterpretation."""

from decimal import Decimal

import pyarrow as pa
import pytest

from vgi.logical_types import (
    LOGICAL_TYPE_KEY,
    LOGICAL_TYPE_PROFILES,
    TUPLE_PROFILE,
    VARIANT_PROFILE,
    decode_variant,
    encode_variant,
    logical_field,
    require_logical_batch,
    require_logical_profiles,
    require_logical_values,
    select_logical_type_profiles,
    tuple_field,
    validate_logical_schema,
    validate_logical_values,
    variant_field,
)
from vgi.typed_values import TypedValueBatch


def values(field, value, *, metadata=None):
    """Construct a typed scalar with explicit schema, avoiding inference."""
    return TypedValueBatch(
        pa.RecordBatch.from_arrays([pa.array([value], type=field.type)], schema=pa.schema([field], metadata=metadata))
    )


@pytest.mark.parametrize(
    "dtype,value",
    [
        (pa.int8(), -3),
        (pa.int16(), -3),
        (pa.int32(), -3),
        (pa.int64(), -3),
        (pa.uint8(), 255),
        (pa.uint16(), 65535),
        (pa.uint32(), 2**32 - 1),
        (pa.uint64(), 2**64 - 1),
        (pa.float32(), -0.0),
        (pa.float64(), float("inf")),
        (pa.decimal128(23, 7), Decimal("1234567890123456.1234567")),
        (pa.date32(), 17),
        (pa.time64("us"), 123),
        (pa.time64("ns"), 123),
        (pa.timestamp("s"), 17),
        (pa.timestamp("ms"), 17),
        (pa.timestamp("us"), 17),
        (pa.timestamp("ns"), 17),
        (pa.timestamp("us", tz="UTC"), 17),
        (pa.timestamp("ns", tz="UTC"), 17),
        (pa.string(), "é\0"),
        (pa.binary(), b"\xff\0"),
        (pa.bool_(), False),
    ],
)
def test_variant_preserves_exact_scalar_type(dtype, value):
    original = values(
        pa.field("value", dtype, metadata={b"domain": b"retain"}), value, metadata={b"schema-note": b"retain"}
    )
    actual = decode_variant(encode_variant(original))
    assert actual.schema.equals(original.schema, check_metadata=True)
    assert actual.batch.equals(original.batch, check_metadata=True)


@pytest.mark.parametrize(
    "kind,value",
    [
        ("hugeint", (-(2**127)).to_bytes(16, "little", signed=True)),
        ("uhugeint", (2**128 - 1).to_bytes(16, "little")),
        ("bignum", "-" + "9" * 100),
        ("bit", "001001"),
        ("uuid", bytes(range(16))),
        ("time_tz", {"micros": 123, "offset_seconds": -18000}),
        ("interval", {"months": -1, "days": 2, "micros": -3}),
    ],
)
def test_variant_special_scalar_identity(kind, value):
    original = values(logical_field("value", kind), value)
    actual = decode_variant(encode_variant(original))
    assert actual.batch.equals(original.batch, check_metadata=True)


def test_tuple_identity_is_distinct_from_struct_and_survives_nested_ipc():
    child = tuple_field("pair", [pa.field("", pa.int32()), pa.field("", pa.string())])
    field = pa.field("outer", pa.list_(child))
    original = values(field, [{"element1": 12, "element2": None}])
    restored = TypedValueBatch.deserialize_from_bytes(original.serialize_to_bytes())
    validate_logical_values(restored, selected_profiles=[TUPLE_PROFILE])
    assert restored.schema.equals(original.schema, check_metadata=True)
    assert child.metadata[LOGICAL_TYPE_KEY] == b"tuple"
    assert not child.equals(child.remove_metadata(), check_metadata=True)
    with pytest.raises(ValueError, match="not negotiated"):
        validate_logical_schema(restored.schema)


def test_missing_object_member_variant_null_and_sql_null_are_distinct():
    null_payload = encode_variant(values(pa.field("value", pa.null()), None))
    empty_object = encode_variant(values(pa.field("value", pa.struct([])), {}))
    null_member = encode_variant(
        values(pa.field("value", pa.struct([pa.field("present", pa.null())])), {"present": None})
    )
    assert decode_variant(null_payload).schema.field(0).type == pa.null()
    assert decode_variant(empty_object).schema.field(0).type.num_fields == 0
    assert decode_variant(null_member).schema.field(0).type.num_fields == 1
    outer = values(variant_field("value"), None)
    validate_logical_values(outer, selected_profiles=[VARIANT_PROFILE])
    assert not outer.batch.column(0)[0].is_valid
    with pytest.raises(ValueError, match="typed SQL NULL"):
        encode_variant(values(pa.field("value", pa.int64()), None))


def test_heterogeneous_variant_array_and_recursive_metadata():
    integer = encode_variant(values(pa.field("value", pa.uint64()), 2**64 - 1))
    text = encode_variant(values(pa.field("value", pa.string()), "hello"))
    array = values(pa.field("value", pa.list_(variant_field("item"))), [integer, text, None])
    restored = decode_variant(encode_variant(array))
    assert restored.schema.equals(array.schema, check_metadata=True)
    first = restored.batch.column(0)[0].values[0].as_py()
    assert decode_variant(first).schema.field(0).type == pa.uint64()


def test_tuple_inside_variant_requires_both_profiles():
    field = tuple_field("value", [pa.field("", pa.int64())])
    original = values(field, {"element1": 42})
    with pytest.raises(ValueError, match="tuple.*not negotiated"):
        encode_variant(original)
    assert decode_variant(
        encode_variant(original, selected_profiles=LOGICAL_TYPE_PROFILES), selected_profiles=LOGICAL_TYPE_PROFILES
    ).schema.equals(original.schema, check_metadata=True)


@pytest.mark.parametrize(
    "field",
    [
        pa.field("x", pa.struct([pa.field("wrong", pa.int32())]), metadata={LOGICAL_TYPE_KEY: b"tuple"}),
        pa.field("x", pa.int64(), metadata={LOGICAL_TYPE_KEY: b"tuple"}),
        pa.field("x", pa.large_binary(), metadata={LOGICAL_TYPE_KEY: b"variant"}),
        pa.field("x", pa.int64(), metadata={LOGICAL_TYPE_KEY: b"unknown"}),
        pa.field("x", pa.int64(), metadata={b"vgi.logical_type.v2": b"tuple"}),
        pa.field("x", pa.binary(), metadata={b"ARROW:extension:name": b"arrow.parquet.variant"}),
        pa.field("x", pa.struct([pa.field("dup", pa.int32()), pa.field("dup", pa.int32())])),
        pa.field("x", pa.timestamp("us", tz="America/New_York")),
    ],
)
def test_invalid_or_unknown_logical_types_fail_closed(field):
    with pytest.raises(ValueError):
        validate_logical_schema(pa.schema([field]), selected_profiles=LOGICAL_TYPE_PROFILES)


@pytest.mark.parametrize(
    "kind,value",
    [
        ("bignum", "+1"),
        ("bignum", "01"),
        ("bignum", "-0"),
        ("bit", "012"),
        ("time_tz", {"micros": -1, "offset_seconds": 0}),
    ],
)
def test_invalid_special_scalar_values(kind, value):
    with pytest.raises(ValueError):
        encode_variant(values(logical_field("value", kind), value))


def test_variant_rejects_wrong_shape_trailing_data_and_recursive_wrapping():
    payload = encode_variant(values(pa.field("value", pa.int32()), 1))
    for invalid in (b"", payload + b"trailing", values(pa.field("other", pa.int32()), 1).serialize_to_bytes()):
        with pytest.raises((ValueError, pa.ArrowInvalid)):
            decode_variant(invalid)
    with pytest.raises(ValueError, match="directly wrap"):
        encode_variant(values(variant_field("value"), payload))


def test_profile_selection_is_explicit_and_deterministic():
    assert select_logical_type_profiles([], LOGICAL_TYPE_PROFILES) == []
    assert select_logical_type_profiles(
        [VARIANT_PROFILE, "future", TUPLE_PROFILE, VARIANT_PROFILE], LOGICAL_TYPE_PROFILES
    ) == sorted(LOGICAL_TYPE_PROFILES)
    with pytest.raises(ValueError, match="not negotiated"):
        decode_variant(b"anything", selected_profiles=[])


def test_empty_tuple_is_not_an_empty_named_struct():
    field = tuple_field("value", [])
    original = values(field, {})
    validate_logical_values(original, selected_profiles=[TUPLE_PROFILE])
    assert original.schema.field(0).metadata[LOGICAL_TYPE_KEY] == b"tuple"


def test_duplicate_metadata_is_rejected_before_dict_coercion():
    field = pa.field("value", pa.struct([]), metadata=[(LOGICAL_TYPE_KEY, b"tuple"), (LOGICAL_TYPE_KEY, b"variant")])
    with pytest.raises(ValueError, match="unique"):
        validate_logical_schema(pa.schema([field]), selected_profiles=LOGICAL_TYPE_PROFILES)


def test_tiny_ipc_cannot_expand_to_unbounded_null_slots():
    child = pa.nulls(65536)
    array = pa.ListArray.from_arrays(pa.array([0, 65536], type=pa.int32()), child)
    batch = pa.RecordBatch.from_arrays([array], names=["value"])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    payload = sink.getvalue().to_pybytes()
    assert len(payload) < 1024
    with pytest.raises(ValueError, match="too many logical value slots"):
        decode_variant(payload)


def test_boundary_guard_preserves_legacy_types_and_checks_nested_annotations():
    legacy = pa.schema([pa.field("legacy", pa.map_(pa.string(), pa.int32()))])
    require_logical_profiles(legacy)
    annotated = pa.schema([pa.field("legacy", pa.map_(pa.string(), tuple_field("value", [])))])
    with pytest.raises(ValueError, match="not negotiated"):
        require_logical_profiles(annotated)
    require_logical_profiles(annotated, selected_profiles=[TUPLE_PROFILE])


def test_value_boundary_retains_legacy_map_and_checks_nested_variant():
    legacy = values(pa.field("legacy", pa.map_(pa.string(), pa.int32())), [("key", 42)])
    require_logical_values(legacy)
    invalid = values(pa.field("object", pa.struct([variant_field("nested")])), {"nested": b"not ipc"})
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        require_logical_values(invalid, selected_profiles=[VARIANT_PROFILE])
    payload = encode_variant(values(pa.field("value", pa.uint64()), 2**64 - 1))
    valid = values(pa.field("object", pa.struct([variant_field("nested")])), {"nested": payload})
    require_logical_values(valid, selected_profiles=[VARIANT_PROFILE])


def test_output_batch_checks_every_present_variant_before_emission():
    payload = encode_variant(values(pa.field("value", pa.int32()), 42))
    batch = pa.RecordBatch.from_arrays(
        [pa.array([payload, None, b"malformed"], type=pa.binary())], schema=pa.schema([variant_field("result")])
    )
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        require_logical_batch(batch, selected_profiles=[VARIANT_PROFILE])
    require_logical_batch(batch.slice(0, 2), selected_profiles=[VARIANT_PROFILE])
    assert not require_logical_profiles(pa.schema([pa.field("ordinary", pa.map_(pa.string(), pa.int64()))]))
