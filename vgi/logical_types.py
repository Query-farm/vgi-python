# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""TUPLE identity and the canonical Arrow Parquet Variant carrier.

VARIANT bytes follow Apache Parquet's VariantEncoding, never a VGI encoding.
These helpers validate the Arrow carrier only. The consuming backend owns binary
Variant decoding and semantic support; see docs/protocol/logical-types.md.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from typing import Any, cast

import pyarrow as pa

from vgi.typed_values import TypedValueBatch

LOGICAL_TYPE_KEY = b"vgi.logical_type.v1"
EXTENSION_NAME_KEY = b"ARROW:extension:name"
EXTENSION_METADATA_KEY = b"ARROW:extension:metadata"
VARIANT_EXTENSION_NAME = b"arrow.parquet.variant"
TUPLE_PROFILE = "vgi.type.tuple.v1"
VARIANT_PROFILE = "vgi.type.variant.parquet.v1"
LOGICAL_TYPE_PROFILES = (TUPLE_PROFILE, VARIANT_PROFILE)
MAX_VARIANT_BYTES = 8 * 1024 * 1024
MAX_LOGICAL_DEPTH = 64
MAX_LOGICAL_FIELDS = 4096


def tuple_field(
    name: str,
    children: Sequence[pa.Field[Any]],
    *,
    nullable: bool = True,
    metadata: Mapping[bytes, bytes] | None = None,
) -> pa.Field[Any]:
    """Declare an unnamed tuple, retaining all recursive child metadata."""
    values = dict(metadata or {})
    if LOGICAL_TYPE_KEY in values and values[LOGICAL_TYPE_KEY] != b"tuple":
        raise ValueError("conflicting logical type metadata")
    values[LOGICAL_TYPE_KEY] = b"tuple"
    fields = [field.with_name(f"element{i + 1}") for i, field in enumerate(children)]
    return pa.field(name, pa.struct(fields), nullable=nullable, metadata=values)


def variant_field(name: str, *, nullable: bool = True, metadata: Mapping[bytes, bytes] | None = None) -> pa.Field[Any]:
    """Declare canonical unshredded Arrow Parquet Variant storage."""
    values = dict(metadata or {})
    if any(key.startswith(b"vgi.logical_type.") for key in values):
        raise ValueError("Parquet Variant must use the canonical Arrow extension annotation")
    if values.get(EXTENSION_NAME_KEY, VARIANT_EXTENSION_NAME) != VARIANT_EXTENSION_NAME:
        raise ValueError("conflicting Arrow extension name")
    if values.get(EXTENSION_METADATA_KEY, b"") != b"":
        raise ValueError("Parquet Variant extension metadata must be empty")
    values[EXTENSION_NAME_KEY] = VARIANT_EXTENSION_NAME
    values[EXTENSION_METADATA_KEY] = b""
    storage = pa.struct([pa.field("metadata", pa.binary(), nullable=False), pa.field("value", pa.binary())])
    return pa.field(name, storage, nullable=nullable, metadata=values)


def select_logical_type_profiles(offered: Collection[str], supported: Collection[str]) -> list[str]:
    """Select known common profiles; an empty offer never selects a profile."""
    return sorted(set(offered) & set(supported) & set(LOGICAL_TYPE_PROFILES))


def _metadata(field: pa.Field[Any]) -> dict[bytes, bytes]:
    if not field.equals(field.with_metadata(cast(Any, field.metadata)), check_metadata=True):
        raise ValueError("logical type field metadata keys must be unique")
    return field.metadata or {}


def _registered_variant(dtype: pa.DataType) -> bool:
    return isinstance(dtype, pa.BaseExtensionType) and dtype.extension_name == VARIANT_EXTENSION_NAME.decode()


def _is_variant(field: pa.Field[Any]) -> bool:
    return (field.metadata or {}).get(EXTENSION_NAME_KEY) == VARIANT_EXTENSION_NAME or _registered_variant(field.type)


def _storage(dtype: pa.DataType) -> pa.DataType:
    return dtype.storage_type if isinstance(dtype, pa.BaseExtensionType) else dtype


def _binary_type(dtype: pa.DataType, *, allow_encoded: bool = False) -> bool:
    if allow_encoded and (pa.types.is_dictionary(dtype) or pa.types.is_run_end_encoded(dtype)):
        return _binary_type(dtype.value_type, allow_encoded=True)
    return pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype) or pa.types.is_binary_view(dtype)


def _check_variant(field: pa.Field[Any], selected: Collection[str]) -> None:
    if VARIANT_PROFILE not in selected:
        raise ValueError("variant logical type profile was not negotiated")
    metadata = _metadata(field)
    if metadata.get(EXTENSION_METADATA_KEY, b"") != b"":
        raise ValueError("Parquet Variant extension metadata must be empty")
    if isinstance(field.type, pa.BaseExtensionType):
        if not _registered_variant(field.type):
            raise ValueError("conflicting registered Arrow extension type")
        if cast(Any, field.type).__arrow_ext_serialize__() != b"":
            raise ValueError("Parquet Variant extension metadata must be empty")
    if metadata.get(EXTENSION_NAME_KEY, VARIANT_EXTENSION_NAME) != VARIANT_EXTENSION_NAME:
        raise ValueError("conflicting Arrow extension name")
    dtype = _storage(field.type)
    if not pa.types.is_struct(dtype):
        raise ValueError("Parquet Variant requires struct storage")
    names = [child.name for child in dtype]
    if "typed_value" in names:
        raise ValueError("shredded Parquet Variant is not supported by this profile")
    if sorted(names) != ["metadata", "value"]:
        raise ValueError("unshredded Parquet Variant requires exactly metadata and value fields")
    meta_field = dtype.field("metadata")
    value_field = dtype.field("value")
    if meta_field.nullable or not _binary_type(meta_field.type, allow_encoded=True):
        raise ValueError("Parquet Variant metadata must be non-nullable binary storage")
    if not _binary_type(value_field.type):
        raise ValueError("Parquet Variant value must use binary storage")


def _check_field(field: pa.Field[Any], selected: Collection[str], depth: int, budget: list[int]) -> None:
    if depth > MAX_LOGICAL_DEPTH:
        raise ValueError("logical type nesting is too deep")
    budget[0] += 1
    if budget[0] > MAX_LOGICAL_FIELDS:
        raise ValueError("too many logical type fields")
    metadata = _metadata(field)
    if any(key.startswith(b"vgi.logical_type.") and key != LOGICAL_TYPE_KEY for key in metadata):
        raise ValueError("unsupported logical type annotation version")
    kind = metadata.get(LOGICAL_TYPE_KEY)
    if kind is not None:
        if kind != b"tuple":
            raise ValueError("unsupported logical type annotation; VARIANT uses arrow.parquet.variant")
        if TUPLE_PROFILE not in selected:
            raise ValueError("tuple logical type profile was not negotiated")
        if EXTENSION_NAME_KEY in metadata or isinstance(field.type, pa.BaseExtensionType):
            raise ValueError("tuple cannot also declare an Arrow extension")
        if not pa.types.is_struct(field.type):
            raise ValueError("tuple requires an Arrow struct")
        if [child.name for child in field.type] != [f"element{i + 1}" for i in range(field.type.num_fields)]:
            raise ValueError("tuple members require positional element1 through elementN names")
    if _is_variant(field):
        _check_variant(field, selected)
    dtype = _storage(field.type)
    for i in range(dtype.num_fields):
        _check_field(dtype.field(i), selected, depth + 1, budget)
    if pa.types.is_dictionary(dtype):
        _check_field(pa.field("dictionary-value", dtype.value_type), selected, depth + 1, budget)


def validate_logical_schema(schema: pa.Schema, *, selected_profiles: Collection[str] = ()) -> None:
    """Validate recursive annotations without restricting ordinary Arrow types."""
    if not schema.equals(schema.with_metadata(schema.metadata), check_metadata=True):
        raise ValueError("logical type schema metadata keys must be unique")
    budget = [0]
    for field in schema:
        _check_field(field, selected_profiles, 0, budget)


def _contains_annotation(field: pa.Field[Any], depth: int = 0) -> bool:
    if depth > MAX_LOGICAL_DEPTH:
        raise ValueError("logical type nesting is too deep")
    if any(key.startswith(b"vgi.logical_type.") for key in (field.metadata or {})) or _is_variant(field):
        return True
    dtype = _storage(field.type)
    if any(_contains_annotation(dtype.field(i), depth + 1) for i in range(dtype.num_fields)):
        return True
    if pa.types.is_dictionary(dtype):
        return _contains_annotation(pa.field("dictionary-value", dtype.value_type), depth + 1)
    return False


def require_logical_profiles(schema: pa.Schema, *, selected_profiles: Collection[str] = ()) -> bool:
    """Check annotated subtrees; leave legacy unannotated schemas unchanged."""
    annotated = False
    for field in schema:
        if _contains_annotation(field):
            _check_field(field, selected_profiles, 0, [0])
            annotated = True
    return annotated


def _decoded_scalar(value: pa.Scalar[Any] | None) -> pa.Scalar[Any] | None:
    while isinstance(value, (pa.DictionaryScalar, pa.ExtensionScalar, pa.RunEndEncodedScalar)):
        if not value.is_valid:
            return None
        value = cast("pa.Scalar[Any] | None", value.value)
    return value


def _variant_carrier(value: pa.Scalar[Any]) -> None:
    scalar = _decoded_scalar(value)
    if scalar is None or not scalar.is_valid:
        return
    if not isinstance(scalar, pa.StructScalar):
        raise ValueError("Parquet Variant requires a struct scalar")
    total = 0
    for name in ("metadata", "value"):
        child = _decoded_scalar(cast(Any, scalar)[name])
        if child is None or not child.is_valid:
            if name == "metadata" or not scalar.type.field(name).nullable:
                raise ValueError(f"present Parquet Variant requires non-NULL {name}")
            continue
        total += cast(Any, child).as_buffer().size
    if total > MAX_VARIANT_BYTES:
        raise ValueError("Parquet Variant carrier exceeds its byte budget")
    # Deliberately no binary parser here: a backend must validate and decode
    # Apache Parquet VariantEncoding before interpreting these opaque bytes.


def _require_annotated_value(
    value: pa.Scalar[Any] | None, field: pa.Field[Any], profiles: Collection[str], depth: int = 0
) -> None:
    if depth > MAX_LOGICAL_DEPTH:
        raise ValueError("logical value nesting is too deep")
    if not _contains_annotation(field):
        return
    if value is None or not value.is_valid:
        if not field.nullable:
            raise ValueError("NULL supplied for a non-nullable logical field")
        return
    if _is_variant(field):
        _variant_carrier(value)
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
    """Check every annotated output carrier; skip ordinary unannotated columns."""
    if not require_logical_profiles(batch.schema, selected_profiles=selected_profiles):
        return
    batch.validate(full=True)
    for i, field in enumerate(batch.schema):
        if _contains_annotation(field):
            for scalar in batch.column(i):
                _require_annotated_value(scalar, field, selected_profiles)


def require_logical_values(values: TypedValueBatch, *, selected_profiles: Collection[str] = ()) -> None:
    """Check negotiated logical annotations and canonical Variant carriers."""
    require_logical_batch(values.batch, selected_profiles=selected_profiles)


def validate_logical_values(values: TypedValueBatch, *, selected_profiles: Collection[str] = ()) -> None:
    """Validate typed values and carrier shape without decoding Variant bytes."""
    values.__post_init__()
    validate_logical_schema(values.schema, selected_profiles=selected_profiles)
    require_logical_batch(values.batch, selected_profiles=selected_profiles)
