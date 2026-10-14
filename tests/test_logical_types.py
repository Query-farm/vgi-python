# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Canonical Arrow Variant transport and distinct tuple identity."""

import pyarrow as pa
import pytest

from vgi.logical_types import (
    EXTENSION_METADATA_KEY,
    EXTENSION_NAME_KEY,
    LOGICAL_TYPE_KEY,
    LOGICAL_TYPE_PROFILES,
    MAX_VARIANT_BYTES,
    TUPLE_PROFILE,
    VARIANT_EXTENSION_NAME,
    VARIANT_PROFILE,
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

# Apache parquet-format VariantEncoding.md: metadata version1, empty dictionary;
# encoded null; empty object; and a one-key object {"present": null}.
# https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
EMPTY_METADATA = bytes.fromhex("010000")
NULL_VALUE = bytes.fromhex("00")
EMPTY_OBJECT = bytes.fromhex("020000")
PRESENT_METADATA = bytes.fromhex("0101000770726573656e74")
PRESENT_NULL = bytes.fromhex("020100000100")


def values(field, value, *, metadata=None):
    """Construct an explicit one-row Arrow carrier, with no variant encoder."""
    return TypedValueBatch(
        pa.RecordBatch.from_arrays([pa.array([value], type=field.type)], schema=pa.schema([field], metadata=metadata))
    )


def test_variant_field_uses_the_canonical_arrow_extension():
    field = variant_field("v")
    assert field.metadata == {EXTENSION_NAME_KEY: VARIANT_EXTENSION_NAME, EXTENSION_METADATA_KEY: b""}
    assert field.type == pa.struct([pa.field("metadata", pa.binary(), nullable=False), pa.field("value", pa.binary())])
    assert VARIANT_PROFILE == "vgi.type.variant.parquet.v1"
    with pytest.raises(ValueError, match="not negotiated"):
        require_logical_profiles(pa.schema([field]))


@pytest.mark.parametrize(
    "metadata,payload", [(EMPTY_METADATA, NULL_VALUE), (EMPTY_METADATA, EMPTY_OBJECT), (PRESENT_METADATA, PRESENT_NULL)]
)
def test_standard_variant_bytes_and_metadata_survive_arrow_ipc(metadata, payload):
    original = values(variant_field("v", metadata={b"owner": b"preserved"}), {"metadata": metadata, "value": payload})
    restored = TypedValueBatch.deserialize_from_bytes(original.serialize_to_bytes())
    validate_logical_values(restored, selected_profiles=[VARIANT_PROFILE])
    assert restored.schema.equals(original.schema, check_metadata=True)
    assert restored.batch.column(0)[0].as_py() == {"metadata": metadata, "value": payload}


def test_outer_sql_null_present_variant_null_and_absent_member_remain_distinct():
    field = variant_field("v")
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(
                [
                    None,
                    {"metadata": EMPTY_METADATA, "value": NULL_VALUE},
                    {"metadata": EMPTY_METADATA, "value": EMPTY_OBJECT},
                    {"metadata": PRESENT_METADATA, "value": PRESENT_NULL},
                ],
                type=field.type,
            )
        ],
        schema=pa.schema([field]),
    )
    require_logical_batch(batch, selected_profiles=[VARIANT_PROFILE])
    assert not batch.column(0)[0].is_valid
    assert batch.column(0)[1].is_valid
    assert batch.column(0)[1].as_py()["value"] == NULL_VALUE
    assert batch.column(0)[2].as_py()["value"] != batch.column(0)[3].as_py()["value"]


@pytest.mark.parametrize("binary_type", [pa.binary(), pa.large_binary(), pa.binary_view()])
@pytest.mark.parametrize("reverse", [False, True])
def test_canonical_binary_layouts_and_field_order(binary_type, reverse):
    fields = [pa.field("metadata", binary_type, nullable=False), pa.field("value", binary_type)]
    if reverse:
        fields.reverse()
    field = pa.field("v", pa.struct(fields), metadata=variant_field("v").metadata)
    original = values(field, {"metadata": EMPTY_METADATA, "value": NULL_VALUE})
    validate_logical_values(original, selected_profiles=[VARIANT_PROFILE])
    assert original.batch.column(0)[0].as_py()["metadata"] == EMPTY_METADATA


@pytest.mark.parametrize("encoded", ["dictionary", "run_end"])
def test_canonical_metadata_encoding_wrappers(encoded):
    if encoded == "dictionary":
        metadata = pa.DictionaryArray.from_arrays(pa.array([0], type=pa.int8()), pa.array([EMPTY_METADATA]))
    else:
        metadata = pa.RunEndEncodedArray.from_arrays(pa.array([1], type=pa.int16()), pa.array([EMPTY_METADATA]))
    field = pa.field(
        "v",
        pa.struct([pa.field("metadata", metadata.type, nullable=False), pa.field("value", pa.binary())]),
        metadata=variant_field("v").metadata,
    )
    array = pa.StructArray.from_arrays([metadata, pa.array([NULL_VALUE])], fields=list(field.type))
    require_logical_batch(
        pa.RecordBatch.from_arrays([array], schema=pa.schema([field])), selected_profiles=[VARIANT_PROFILE]
    )


class CanonicalVariantType(pa.ExtensionType):
    """Represent an already registered canonical type without a value decoder."""

    def __init__(self):
        """Declare the canonical storage without a binary value decoder."""
        super().__init__(variant_field("v").type, "arrow.parquet.variant")

    def __arrow_ext_serialize__(self):
        """Return the canonical empty extension metadata."""
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        """Restore the canonical schema, preserving the standard marker."""
        assert serialized == b""
        assert storage_type == variant_field("v").type
        return cls()


def test_registered_extension_and_dictionary_wrapping_do_not_bypass_negotiation():
    dtype = CanonicalVariantType()
    storage = pa.array([{"metadata": EMPTY_METADATA, "value": NULL_VALUE}], type=dtype.storage_type)
    extension = pa.ExtensionArray.from_storage(dtype, storage)
    for array in (extension, pa.DictionaryArray.from_arrays(pa.array([0], type=pa.int8()), extension)):
        batch = pa.RecordBatch.from_arrays([array], names=["v"])
        with pytest.raises(ValueError, match="not negotiated"):
            require_logical_batch(batch)
        require_logical_batch(batch, selected_profiles=[VARIANT_PROFILE])


def test_variant_nested_in_tuple_requires_both_profiles():
    field = tuple_field("pair", [variant_field("ignored")])
    original = values(field, {"element1": {"metadata": EMPTY_METADATA, "value": NULL_VALUE}})
    with pytest.raises(ValueError, match="variant.*not negotiated"):
        require_logical_values(original, selected_profiles=[TUPLE_PROFILE])
    validate_logical_values(original, selected_profiles=LOGICAL_TYPE_PROFILES)


def test_variant_payload_semantics_remain_the_backends_responsibility():
    # This helper deliberately does not pretend to implement a Parquet decoder.
    opaque = values(variant_field("v"), {"metadata": b"not-decoded-here", "value": b"opaque-bytes"})
    validate_logical_values(opaque, selected_profiles=[VARIANT_PROFILE])


def test_missing_required_metadata_fails_before_output():
    field = variant_field("v")
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(
                [{"metadata": EMPTY_METADATA, "value": NULL_VALUE}, {"metadata": None, "value": NULL_VALUE}],
                type=field.type,
            )
        ],
        schema=pa.schema([field]),
    )
    with pytest.raises(ValueError, match="non-NULL metadata"):
        require_logical_batch(batch, selected_profiles=[VARIANT_PROFILE])


@pytest.mark.parametrize("row", [{"metadata": EMPTY_METADATA, "value": None}, {"metadata": b"", "value": b""}])
def test_carrier_preserves_nullable_value_and_opaque_empty_bytes(row):
    original = values(variant_field("v"), row)
    require_logical_values(original, selected_profiles=[VARIANT_PROFILE])
    assert original.batch.column(0)[0].as_py() == row


def test_present_variant_byte_budget_applies_to_both_binary_fields():
    field = variant_field("v")
    large = values(field, {"metadata": EMPTY_METADATA, "value": b"x" * MAX_VARIANT_BYTES})
    with pytest.raises(ValueError, match="byte budget"):
        require_logical_values(large, selected_profiles=[VARIANT_PROFILE])


@pytest.mark.parametrize(
    "storage",
    [
        pa.binary(),
        pa.struct([pa.field("metadata", pa.binary()), pa.field("value", pa.binary())]),
        pa.struct([pa.field("metadata", pa.binary(), nullable=False)]),
        pa.struct([pa.field("metadata", pa.binary(), nullable=False), pa.field("value", pa.string())]),
        pa.struct(
            [
                pa.field("metadata", pa.binary(), nullable=False),
                pa.field("value", pa.binary()),
                pa.field("typed_value", pa.int64()),
            ]
        ),
        pa.struct(
            [
                pa.field("metadata", pa.binary(), nullable=False),
                pa.field("metadata", pa.binary()),
                pa.field("value", pa.binary()),
            ]
        ),
    ],
)
def test_invalid_or_shredded_canonical_storage_is_rejected(storage):
    field = pa.field("v", storage, metadata=variant_field("v").metadata)
    with pytest.raises(ValueError):
        validate_logical_schema(pa.schema([field]), selected_profiles=[VARIANT_PROFILE])


@pytest.mark.parametrize(
    "kind", [b"variant", b"hugeint", b"uhugeint", b"bignum", b"bit", b"uuid", b"time_tz", b"interval", b"geometry"]
)
def test_removed_vgi_scalar_and_variant_annotations_are_not_accepted(kind):
    field = pa.field("v", pa.binary(), metadata={LOGICAL_TYPE_KEY: kind})
    with pytest.raises(ValueError, match="unsupported logical type annotation"):
        require_logical_profiles(pa.schema([field]), selected_profiles=LOGICAL_TYPE_PROFILES)


def test_nonempty_extension_metadata_and_duplicate_tags_are_rejected():
    field = variant_field("v")
    for metadata in (
        {EXTENSION_NAME_KEY: VARIANT_EXTENSION_NAME, EXTENSION_METADATA_KEY: b"{}"},
        [(EXTENSION_NAME_KEY, VARIANT_EXTENSION_NAME), (EXTENSION_NAME_KEY, VARIANT_EXTENSION_NAME)],
    ):
        with pytest.raises(ValueError):
            validate_logical_schema(pa.schema([field.with_metadata(metadata)]), selected_profiles=[VARIANT_PROFILE])


def test_tuple_identity_is_distinct_from_struct_and_survives_nested_ipc():
    child = tuple_field("pair", [pa.field("", pa.int32()), pa.field("", pa.string())])
    field = pa.field("outer", pa.list_(child))
    original = values(field, [{"element1": 12, "element2": None}])
    restored = TypedValueBatch.deserialize_from_bytes(original.serialize_to_bytes())
    validate_logical_values(restored, selected_profiles=[TUPLE_PROFILE])
    assert restored.schema.equals(original.schema, check_metadata=True)
    assert not child.equals(child.remove_metadata(), check_metadata=True)
    with pytest.raises(ValueError, match="not negotiated"):
        validate_logical_schema(restored.schema)


def test_empty_tuple_and_legacy_types_remain_distinct():
    original = values(tuple_field("value", []), {})
    validate_logical_values(original, selected_profiles=[TUPLE_PROFILE])
    assert original.schema.field(0).metadata[LOGICAL_TYPE_KEY] == b"tuple"
    legacy = pa.schema([pa.field("legacy", pa.map_(pa.string(), pa.int32()))])
    assert not require_logical_profiles(legacy)
    annotated = pa.schema([pa.field("legacy", pa.map_(pa.string(), tuple_field("value", [])))])
    with pytest.raises(ValueError, match="not negotiated"):
        require_logical_profiles(annotated)
    require_logical_profiles(annotated, selected_profiles=[TUPLE_PROFILE])


def test_profile_selection_excludes_the_removed_custom_encoding():
    assert select_logical_type_profiles([], LOGICAL_TYPE_PROFILES) == []
    assert select_logical_type_profiles(
        ["vgi.type.variant.arrow.v1", VARIANT_PROFILE, TUPLE_PROFILE], LOGICAL_TYPE_PROFILES
    ) == sorted(LOGICAL_TYPE_PROFILES)


def test_output_carrier_respects_nonnullable_value_declaration():
    field = variant_field("v")
    field = field.with_type(pa.struct([field.type.field("metadata"), field.type.field("value").with_nullable(False)]))
    batch = pa.RecordBatch.from_arrays(
        [pa.array([{"metadata": EMPTY_METADATA, "value": None}], type=field.type)], schema=pa.schema([field])
    )
    with pytest.raises(ValueError, match="non-NULL value"):
        require_logical_batch(batch, selected_profiles=[VARIANT_PROFILE])
