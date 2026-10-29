# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Lossless one-row Arrow carriers and rejection of ambiguous value payloads."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Annotated, cast

import pyarrow as pa
import pytest
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi.typed_values import TypedValueBatch


def _ipc(schema: pa.Schema, *batches: pa.RecordBatch) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        for batch in batches:
            writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def test_typed_nulls_nested_metadata_and_values_round_trip() -> None:
    """Retain distinctions a conversion through Python scalars would erase."""
    nested = pa.struct([pa.field("a.b", pa.int64(), nullable=False, metadata={b"child": b"annotation"})])
    schema = pa.schema(
        [
            pa.field("record", nested, metadata={b"logical": b"provider.record"}),
            pa.field("amount", pa.decimal128(20, 4)),
            pa.field("time", pa.timestamp("ns", tz="America/New_York")),
            pa.field("optional", pa.int32(), metadata={b"annotation": b"typed null"}),
        ],
        metadata={b"context": b"example"},
    )
    batch = pa.RecordBatch.from_pylist(
        [{"record": {"a.b": 7}, "amount": Decimal("123.4500"), "time": 123456789, "optional": None}],
        schema=schema,
    )
    restored = TypedValueBatch.deserialize_from_bytes(TypedValueBatch(batch).serialize_to_bytes())
    assert restored.batch.equals(batch, check_metadata=True)
    assert restored.schema.field("optional").type == pa.int32()
    assert restored.batch.column("optional")[0].as_py() is None


def test_empty_values_are_one_row() -> None:
    values = TypedValueBatch.empty(metadata={b"context": b"no parameters"})
    restored = TypedValueBatch.deserialize_from_bytes(values.serialize_to_bytes())
    assert restored.batch.num_rows == 1
    assert restored.batch.num_columns == 0
    assert restored.schema.metadata == values.schema.metadata


@pytest.mark.parametrize("ending", [b"", b"\0" * 4, b"\xff" * 4 + b"\0" * 4])
def test_complete_arrow_stream_endings_remain_valid(ending: bytes) -> None:
    values = TypedValueBatch(pa.record_batch({"x": [1]}))
    payload = values.serialize_to_bytes()
    restored = TypedValueBatch.deserialize_from_bytes(payload[:-8] + ending)
    assert restored.batch.equals(values.batch, check_metadata=True)


@pytest.mark.parametrize("length", range(1, 8))
def test_truncated_modern_eos_is_a_value_error(length: int) -> None:
    payload = TypedValueBatch(pa.record_batch({"x": [1]})).serialize_to_bytes()
    with pytest.raises(ValueError):
        TypedValueBatch.deserialize_from_bytes(payload[:-8] + payload[-8:][:length])


@pytest.mark.parametrize("length", range(1, 4))
def test_truncated_legacy_eos_is_a_value_error(length: int) -> None:
    payload = TypedValueBatch(pa.record_batch({"x": [1]})).serialize_to_bytes()
    with pytest.raises(ValueError):
        TypedValueBatch.deserialize_from_bytes(payload[:-8] + b"\0" * length)


def test_truncated_body_is_a_value_error() -> None:
    payload = TypedValueBatch(pa.record_batch({"x": [1]})).serialize_to_bytes()
    with pytest.raises(ValueError):
        TypedValueBatch.deserialize_from_bytes(payload[:-9])


@pytest.mark.parametrize("names", [["x", "x"], [""], ["a\x00b"]])
def test_ambiguous_field_names_reject(names: list[str]) -> None:
    batch = pa.RecordBatch.from_arrays([pa.array([1])] * len(names), names=names)
    with pytest.raises(ValueError, match="field names"):
        TypedValueBatch(batch)


@pytest.mark.parametrize("rows", [[], [1, 2]])
def test_requires_exactly_one_row(rows: list[int]) -> None:
    with pytest.raises(ValueError, match="exactly one row"):
        TypedValueBatch(pa.record_batch({"x": rows}))


@pytest.mark.parametrize("kind", ["missing", "extra", "trailing", "concatenated"])
def test_rejects_incomplete_or_multiple_payloads(kind: str) -> None:
    batch = pa.record_batch({"x": [1]})
    payload = _ipc(batch.schema, batch)
    if kind == "missing":
        payload = _ipc(batch.schema)
    elif kind == "extra":
        payload = _ipc(batch.schema, batch, batch)
    elif kind == "trailing":
        payload += b"ignored?"
    else:
        payload += payload
    with pytest.raises(ValueError, match="no batch|exactly one batch|trailing data"):
        TypedValueBatch.deserialize_from_bytes(payload)


@pytest.mark.parametrize(
    "arrow_type, value",
    [
        (pa.int64(), None),
        (pa.struct([pa.field("child", pa.int64(), nullable=False)]), {"child": None}),
        (pa.list_(pa.field("item", pa.int64(), nullable=False)), [None]),
        (pa.large_list(pa.field("item", pa.int64(), nullable=False)), [None]),
        (pa.list_(pa.field("item", pa.int64(), nullable=False), 1), [None]),
        (pa.list_view(pa.field("item", pa.int64(), nullable=False)), [None]),
        (pa.large_list_view(pa.field("item", pa.int64(), nullable=False)), [None]),
        # Runtime accepts Field so its nullability survives; PyArrow stubs omit this overload.
        (
            cast("Callable[[pa.DataType, pa.Field[pa.DataType]], pa.DataType]", pa.map_)(
                pa.string(), pa.field("value", pa.int64(), nullable=False)
            ),
            [("key", None)],
        ),
    ],
)
def test_rejects_nulls_in_nonnullable_fields(arrow_type: pa.DataType, value: object) -> None:
    schema = pa.schema([pa.field("x", arrow_type, nullable=False)])
    batch = pa.RecordBatch.from_pylist([{"x": value}], schema=schema)
    with pytest.raises(ValueError, match="non-nullable"):
        TypedValueBatch(batch)


def test_null_parent_does_not_require_valid_struct_children() -> None:
    schema = pa.schema([pa.field("x", pa.struct([pa.field("child", pa.int64(), nullable=False)]))])
    batch = pa.RecordBatch.from_pylist([{"x": None}], schema=schema)
    assert TypedValueBatch.deserialize_from_bytes(_ipc(schema, batch)).batch.equals(batch)


@pytest.mark.parametrize("kind", ["dictionary", "extension", "run_end", "union"])
def test_encoded_values_cannot_hide_nullability_violation(kind: str) -> None:
    child = pa.array([None], type=pa.int64())
    array: pa.Array[pa.Scalar[pa.DataType]]
    if kind == "dictionary":
        array = pa.DictionaryArray.from_arrays(pa.array([0], type=pa.int8()), child)
    elif kind == "extension":
        array = pa.ExtensionArray.from_storage(pa.bool8(), pa.array([None], type=pa.int8()))
    elif kind == "run_end":
        array = pa.RunEndEncodedArray.from_arrays(pa.array([1], type=pa.int16()), child)
    else:
        union_type = pa.dense_union([pa.field("child", pa.int64(), nullable=False)])
        array = pa.Array.from_buffers(
            union_type,
            1,
            [None, pa.py_buffer(b"\x00"), pa.py_buffer(b"\x00" * 4)],
            children=[child],
        )
    schema = pa.schema([pa.field("x", array.type, nullable=(kind == "union"))])
    with pytest.raises(ValueError, match="non-nullable"):
        TypedValueBatch(pa.RecordBatch.from_arrays([array], schema=schema))


@pytest.mark.parametrize(
    "expected",
    [
        pa.schema([pa.field("other", pa.int64())]),
        pa.schema([pa.field("x", pa.int32())]),
        pa.schema([pa.field("x", pa.int64(), nullable=False)]),
        pa.schema([pa.field("x", pa.int64(), metadata={b"logical": b"different"})]),
        pa.schema([pa.field("x", pa.int64())], metadata={b"context": b"different"}),
    ],
)
def test_schema_validation_rejects_rebinding_changes(expected: pa.Schema) -> None:
    values = TypedValueBatch(pa.record_batch({"x": [1]}))
    with pytest.raises(ValueError, match="expected schema"):
        values.validate_schema(expected)
    values.validate_schema(pa.record_batch({"x": [2]}).schema)


@dataclass(frozen=True, kw_only=True)
class _Envelope(ArrowSerializableDataclass):
    values: Annotated[TypedValueBatch, ArrowType(pa.binary())]


def test_custom_codec_works_inside_rpc_dataclass() -> None:
    values = TypedValueBatch(pa.record_batch({"x": [1]}))
    restored = _Envelope.deserialize_from_bytes(_Envelope(values=values).serialize_to_bytes())
    assert restored.values.batch.equals(values.batch, check_metadata=True)


def test_custom_codec_rejects_extra_inner_batches() -> None:
    """The RPC decoder must call our strict codec, not just read the first batch."""
    inner = pa.record_batch({"x": [1]})
    outer = pa.RecordBatch.from_pylist([{"values": _ipc(inner.schema, inner, inner)}], schema=_Envelope.ARROW_SCHEMA)
    with pytest.raises(ValueError, match="exactly one batch"):
        _Envelope.deserialize_from_bytes(_ipc(outer.schema, outer))
