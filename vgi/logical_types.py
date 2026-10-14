# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Portable TUPLE and VARIANT storage, independent of a DuckDB ABI.

See docs/protocol/logical-types.md. These helpers do not qualify an engine
adapter or implicitly advertise support in an attach handshake.
"""

from __future__ import annotations

import re
from collections.abc import Collection, Mapping, Sequence
from typing import Any, cast

import pyarrow as pa

from vgi.typed_values import TypedValueBatch

LOGICAL_TYPE_KEY = b"vgi.logical_type.v1"
TUPLE_PROFILE = "vgi.type.tuple.v1"
VARIANT_PROFILE = "vgi.type.variant.arrow.v1"
LOGICAL_TYPE_PROFILES = (TUPLE_PROFILE, VARIANT_PROFILE)
MAX_VARIANT_BYTES = 8 * 1024 * 1024
MAX_LOGICAL_DEPTH = 64
MAX_LOGICAL_FIELDS = 4096
MAX_LOGICAL_VALUES = 65536

# A format is not inferred from a field name or the first non-NULL value.
_SPECIAL_STORAGE: dict[bytes, pa.DataType] = {
    b"hugeint": pa.binary(16),
    b"uhugeint": pa.binary(16),
    b"bignum": pa.string(),
    b"bit": pa.string(),
    b"uuid": pa.binary(16),
    b"time_tz": pa.struct(
        [pa.field("micros", pa.int64(), nullable=False), pa.field("offset_seconds", pa.int32(), nullable=False)]
    ),
    b"interval": pa.struct(
        [
            pa.field("months", pa.int32(), nullable=False),
            pa.field("days", pa.int32(), nullable=False),
            pa.field("micros", pa.int64(), nullable=False),
        ]
    ),
    b"geometry": pa.binary(),
}


def logical_field(
    name: str, kind: str, *, nullable: bool = True, metadata: Mapping[bytes, bytes] | None = None
) -> pa.Field[Any]:
    """Declare a fixed special scalar representation from the contract."""
    key = kind.encode("ascii")
    if key not in _SPECIAL_STORAGE:
        raise ValueError("unknown logical scalar kind")
    values = dict(metadata or {})
    if LOGICAL_TYPE_KEY in values and values[LOGICAL_TYPE_KEY] != key:
        raise ValueError("conflicting logical type metadata")
    values[LOGICAL_TYPE_KEY] = key
    return pa.field(name, _SPECIAL_STORAGE[key], nullable=nullable, metadata=values)


def tuple_field(
    name: str,
    children: Sequence[pa.Field[Any]],
    *,
    nullable: bool = True,
    metadata: Mapping[bytes, bytes] | None = None,
) -> pa.Field[Any]:
    """Declare an unnamed tuple; retain each child's type and other metadata."""
    values = dict(metadata or {})
    if LOGICAL_TYPE_KEY in values and values[LOGICAL_TYPE_KEY] != b"tuple":
        raise ValueError("conflicting logical type metadata")
    values[LOGICAL_TYPE_KEY] = b"tuple"
    fields = [field.with_name(f"element{i + 1}") for i, field in enumerate(children)]
    return pa.field(name, pa.struct(fields), nullable=nullable, metadata=values)


def variant_field(name: str, *, nullable: bool = True, metadata: Mapping[bytes, bytes] | None = None) -> pa.Field[Any]:
    """Declare a dynamic typed value stored as a complete scalar IPC stream."""
    values = dict(metadata or {})
    if LOGICAL_TYPE_KEY in values and values[LOGICAL_TYPE_KEY] != b"variant":
        raise ValueError("conflicting logical type metadata")
    values[LOGICAL_TYPE_KEY] = b"variant"
    return pa.field(name, pa.binary(), nullable=nullable, metadata=values)


def select_logical_type_profiles(offered: Collection[str], supported: Collection[str]) -> list[str]:
    """Select known common profiles deterministically; empty offers select none."""
    return sorted(set(offered) & set(supported) & set(LOGICAL_TYPE_PROFILES))


def _field_kind(field: pa.Field[Any]) -> bytes | None:
    if not field.equals(field.with_metadata(cast(Any, field.metadata)), check_metadata=True):
        raise ValueError("logical type field metadata keys must be unique")
    metadata = field.metadata or {}
    if any(key.startswith(b"vgi.logical_type.") and key != LOGICAL_TYPE_KEY for key in metadata):
        raise ValueError("unsupported logical type annotation version")
    if b"ARROW:extension:name" in metadata or isinstance(field.type, pa.BaseExtensionType):
        raise ValueError("extension types require a separately qualified logical type profile")
    return metadata.get(LOGICAL_TYPE_KEY)


def _check_field(field: pa.Field[Any], selected: Collection[str], depth: int, budget: list[int]) -> None:
    if depth > MAX_LOGICAL_DEPTH:
        raise ValueError("logical type nesting is too deep")
    budget[0] += 1
    if budget[0] > MAX_LOGICAL_FIELDS:
        raise ValueError("too many logical type fields")
    kind = _field_kind(field)
    dtype = field.type
    if kind == b"tuple":
        if TUPLE_PROFILE not in selected:
            raise ValueError("tuple logical type profile was not negotiated")
        if not pa.types.is_struct(dtype):
            raise ValueError("tuple requires an Arrow struct")
        if [child.name for child in dtype] != [f"element{i + 1}" for i in range(dtype.num_fields)]:
            raise ValueError("tuple members require positional element1 through elementN names")
    elif kind == b"variant":
        if VARIANT_PROFILE not in selected:
            raise ValueError("variant logical type profile was not negotiated")
        if dtype != pa.binary():
            raise ValueError("variant requires binary Arrow storage")
    elif kind is not None:
        if VARIANT_PROFILE not in selected or kind not in _SPECIAL_STORAGE:
            raise ValueError("unsupported logical type annotation")
        if not dtype.equals(_SPECIAL_STORAGE[kind], check_metadata=False):
            raise ValueError("logical scalar storage does not match its annotation")
        if kind == b"geometry" and b"vgi.geometry.crs" not in (field.metadata or {}):
            raise ValueError("geometry requires explicit CRS metadata (empty means unspecified)")
    elif not (
        pa.types.is_null(dtype)
        or pa.types.is_boolean(dtype)
        or pa.types.is_integer(dtype)
        or pa.types.is_floating(dtype)
        or pa.types.is_decimal(dtype)
        or pa.types.is_string(dtype)
        or pa.types.is_binary(dtype)
        or pa.types.is_date32(dtype)
        or pa.types.is_time64(dtype)
        or pa.types.is_timestamp(dtype)
        or pa.types.is_struct(dtype)
        or pa.types.is_list(dtype)
    ):
        raise ValueError(f"Arrow type is outside the logical type profile: {dtype}")
    if pa.types.is_timestamp(dtype) and dtype.tz not in (None, "UTC"):
        raise ValueError("timestamp timezone must be absent or UTC")
    if pa.types.is_struct(dtype) and len({child.name for child in dtype}) != dtype.num_fields:
        raise ValueError("object field names must be unique")
    for i in range(dtype.num_fields):
        _check_field(dtype.field(i), selected, depth + 1, budget)


def validate_logical_schema(schema: pa.Schema, *, selected_profiles: Collection[str] = ()) -> None:
    """Reject unknown, ambiguous or unnegotiated recursive representations."""
    if not schema.equals(schema.with_metadata(schema.metadata), check_metadata=True):
        raise ValueError("logical type schema metadata keys must be unique")
    budget = [0]
    for field in schema:
        _check_field(field, selected_profiles, 0, budget)


def _validate_values(
    value: pa.Scalar[Any], field: pa.Field[Any], profiles: Collection[str], depth: int, budget: list[int]
) -> None:
    if depth > MAX_LOGICAL_DEPTH:
        raise ValueError("logical value nesting is too deep")
    if not value.is_valid:
        return
    kind = _field_kind(field)
    if kind == b"variant":
        payload = cast(bytes, value.as_py())
        _decode_variant(payload, profiles, depth + 1, budget)
    elif kind == b"bignum":
        if re.fullmatch(r"0|-?[1-9][0-9]*", cast(str, value.as_py())) is None:
            raise ValueError("bignum requires a canonical decimal integer")
    elif kind == b"bit":
        if re.fullmatch(r"[01]*", cast(str, value.as_py())) is None:
            raise ValueError("bit requires a string of zeroes and ones")
    elif kind == b"time_tz":
        data = value.as_py()
        if not 0 <= data["micros"] < 86_400_000_000 or not -57_599 <= data["offset_seconds"] <= 57_599:
            raise ValueError("time_tz components are out of range")
    if isinstance(value, pa.StructScalar):
        for i, child in enumerate(value.type):
            _validate_values(cast(Any, value)[i], child, profiles, depth + 1, budget)
    elif isinstance(value, pa.ListScalar):
        assert value.values is not None
        for child_value in value.values:
            _validate_values(child_value, value.type.value_field, profiles, depth + 1, budget)


def _check_array_size(array: pa.Array[Any], depth: int, budget: list[int]) -> None:
    if depth > MAX_LOGICAL_DEPTH:
        raise ValueError("logical value nesting is too deep")
    budget[1] += len(array)
    if budget[1] > MAX_LOGICAL_VALUES:
        raise ValueError("too many logical value slots")
    if isinstance(array, pa.StructArray):
        for i in range(array.type.num_fields):
            _check_array_size(array.field(i), depth + 1, budget)
    elif isinstance(array, pa.ListArray):
        _check_array_size(array.values, depth + 1, budget)


def _read_variant_batch(payload: bytes, profiles: Collection[str], depth: int, budget: list[int]) -> TypedValueBatch:
    # Bound logical slots BEFORE TypedValueBatch traverses nullable children:
    # a huge NullArray can otherwise have a tiny IPC representation.
    source = pa.BufferReader(payload)
    with pa.ipc.open_stream(source) as reader:
        validate_logical_schema(reader.schema, selected_profiles=profiles)
        try:
            batch = reader.read_next_batch()
        except StopIteration as exc:
            raise ValueError("variant IPC contains no batch") from exc
        if batch.num_rows != 1 or batch.num_columns != 1:
            raise ValueError("variant IPC requires exactly one row and one field")
        batch.validate(full=True)
        if batch.nbytes > MAX_VARIANT_BYTES:
            raise ValueError("expanded variant batch exceeds its byte budget")
        _check_array_size(batch.column(0), depth, budget)
        try:
            reader.read_next_batch()
        except StopIteration:
            pass
        else:
            raise ValueError("variant IPC must contain exactly one batch")
        if source.tell() != len(payload):
            raise ValueError("variant IPC has trailing data")
    return TypedValueBatch(batch)


def _decode_variant(payload: bytes, profiles: Collection[str], depth: int, budget: list[int]) -> TypedValueBatch:
    if not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_VARIANT_BYTES:
        raise ValueError("invalid variant payload size")
    budget[0] += len(payload)
    if budget[0] > MAX_VARIANT_BYTES:
        raise ValueError("expanded variant payload exceeds its byte budget")
    values = _read_variant_batch(payload, profiles, depth, budget)
    if values.schema.names != ["value"]:
        raise ValueError("variant IPC requires exactly one field named value")
    field = values.schema.field(0)
    if not field.nullable:
        raise ValueError("variant payload value field must be nullable")
    if _field_kind(field) == b"variant":
        raise ValueError("a variant payload cannot directly wrap another variant")
    validate_logical_schema(values.schema, selected_profiles=profiles)
    scalar = values.batch.column(0)[0]
    if not scalar.is_valid and not pa.types.is_null(field.type):
        raise ValueError("variant null must use Arrow null type; typed SQL NULL uses outer validity")
    _validate_values(scalar, field, profiles, depth, budget)
    return values


def decode_variant(payload: bytes, *, selected_profiles: Collection[str] = (VARIANT_PROFILE,)) -> TypedValueBatch:
    """Decode and validate one present VARIANT; SQL NULL has no payload."""
    if VARIANT_PROFILE not in selected_profiles:
        raise ValueError("variant logical type profile was not negotiated")
    return _decode_variant(payload, selected_profiles, 0, [0, 0])


def encode_variant(values: TypedValueBatch, *, selected_profiles: Collection[str] = (VARIANT_PROFILE,)) -> bytes:
    """Encode one dynamic value without inference, casts, JSON or engine bytes."""
    payload = values.serialize_to_bytes()
    decode_variant(payload, selected_profiles=selected_profiles)
    return payload


def validate_logical_values(values: TypedValueBatch, *, selected_profiles: Collection[str] = ()) -> None:
    """Validate one typed-value batch, including nested variant IPC payloads."""
    validate_logical_schema(values.schema, selected_profiles=selected_profiles)
    size_budget = [0, 0]
    for array in values.batch.columns:
        _check_array_size(array, 0, size_budget)
    values.__post_init__()
    for i, field in enumerate(values.schema):
        _validate_values(values.batch.column(i)[0], field, selected_profiles, 0, [0, 0])


def _contains_annotation(field: pa.Field[Any]) -> bool:
    if any(key.startswith(b"vgi.logical_type.") for key in (field.metadata or {})):
        return True
    dtype = field.type
    if any(_contains_annotation(dtype.field(i)) for i in range(dtype.num_fields)):
        return True
    if pa.types.is_dictionary(dtype):
        return _contains_annotation(pa.field("dictionary-value", dtype.value_type))
    if isinstance(dtype, pa.BaseExtensionType):
        return _contains_annotation(pa.field("extension-storage", dtype.storage_type))
    return False


def require_logical_profiles(schema: pa.Schema, *, selected_profiles: Collection[str] = ()) -> bool:
    """Validate annotated subtrees; return whether any annotation was present.

    Call this at catalog/bind boundaries with the attachment's selected read
    profiles. Existing maps, dictionaries and unrelated Arrow extensions remain
    legal unless they contain an explicitly annotated portable logical type.
    """

    def visit(field: pa.Field[Any], depth: int = 0) -> None:
        if depth > MAX_LOGICAL_DEPTH:
            raise ValueError("logical type nesting is too deep")
        if any(key.startswith(b"vgi.logical_type.") for key in (field.metadata or {})):
            _check_field(field, selected_profiles, depth, [0])
            return
        dtype = field.type
        for i in range(dtype.num_fields):
            visit(dtype.field(i), depth + 1)
        if pa.types.is_dictionary(dtype):
            visit(pa.field("dictionary-value", dtype.value_type), depth + 1)
        elif isinstance(dtype, pa.BaseExtensionType):
            visit(pa.field("extension-storage", dtype.storage_type), depth + 1)

    annotated = False
    for field in schema:
        if _contains_annotation(field):
            visit(field)
            annotated = True
    return annotated


def _require_annotated_value(
    value: pa.Scalar[Any] | None, field: pa.Field[Any], profiles: Collection[str], depth: int = 0
) -> None:
    if depth > MAX_LOGICAL_DEPTH:
        raise ValueError("logical value nesting is too deep")
    if value is None or not value.is_valid or not _contains_annotation(field):
        return
    if any(key.startswith(b"vgi.logical_type.") for key in (field.metadata or {})):
        _validate_values(value, field, profiles, depth, [0, 0])
        return
    if isinstance(value, pa.StructScalar):
        for i, child in enumerate(value.type):
            _require_annotated_value(cast(Any, value)[i], child, profiles, depth + 1)
    elif isinstance(value, pa.MapScalar):
        assert value.values is not None
        for pair in value.values:
            _require_annotated_value(pair[0], value.type.key_field, profiles, depth + 1)
            _require_annotated_value(pair[1], value.type.item_field, profiles, depth + 1)
    elif isinstance(
        value, (pa.ListScalar, pa.LargeListScalar, pa.FixedSizeListScalar, pa.ListViewScalar, pa.LargeListViewScalar)
    ):
        assert value.values is not None
        for item in value.values:
            _require_annotated_value(item, value.type.value_field, profiles, depth + 1)
    elif isinstance(value, pa.UnionScalar):
        child = value.type[value.type.type_codes.index(cast(int, value.type_code))]
        _require_annotated_value(value.value, child, profiles, depth + 1)
    elif isinstance(value, (pa.DictionaryScalar, pa.ExtensionScalar, pa.RunEndEncodedScalar)):
        decoded = cast("pa.Scalar[Any] | None", value.value)
        if decoded is not None:
            _require_annotated_value(decoded, field.with_type(decoded.type), profiles, depth + 1)


def require_logical_batch(batch: pa.RecordBatch, *, selected_profiles: Collection[str] = ()) -> None:
    """Validate all annotated output rows; leave legacy columns on the fast path."""
    if not require_logical_profiles(batch.schema, selected_profiles=selected_profiles):
        return
    batch.validate(full=True)
    for i, field in enumerate(batch.schema):
        if _contains_annotation(field):
            for scalar in batch.column(i):
                _require_annotated_value(scalar, field, selected_profiles)


def require_logical_values(values: TypedValueBatch, *, selected_profiles: Collection[str] = ()) -> None:
    """Validate annotated values while retaining legacy unannotated Arrow support."""
    require_logical_batch(values.batch, selected_profiles=selected_profiles)
