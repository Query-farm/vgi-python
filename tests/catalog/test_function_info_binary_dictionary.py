"""Binary metadata dictionaries preserve bytes, typed defaults and batch boundaries."""

from dataclasses import replace

import pyarrow as pa
import pytest

from vgi.catalog import FunctionInfo, FunctionType, SerializedSchema
from vgi.catalog.contents_codec import decode_collection, encode_collection
from vgi.protocol import FunctionsResponse
from vgi.schema_path import SchemaPath

_FIELDS = ("arguments", "output_schema", "parameter_default_values")
_BINARY_DICTIONARY = pa.dictionary(pa.int32(), pa.binary())


def _function(name: str, arguments: bytes, defaults: pa.RecordBatch | None = None) -> FunctionInfo:
    return FunctionInfo(
        name=name,
        comment=None,
        tags={},
        schema_path=SchemaPath(["main"]),
        function_type=FunctionType.SCALAR,
        arguments=SerializedSchema(arguments),
        output_schema=SerializedSchema(b""),
        parameter_default_values=defaults,
    )


@pytest.mark.parametrize("payload", [b"", b"\x00\xff\x80\x00", b"ordinary schema bytes"])
@pytest.mark.parametrize("has_defaults", [False, True])
def test_singleton_and_legacy_list_preserve_typed_binary_metadata(payload: bytes, has_defaults: bool) -> None:
    """Opaque argument/output bytes remain bytes; defaults remain a typed batch."""
    defaults = pa.record_batch({"empty": [b""], "binary": [b"\xff\x00"], "null": [None]}) if has_defaults else None
    original = _function("sample", payload, defaults)
    for name in _FIELDS:
        field = FunctionInfo.ARROW_SCHEMA.field(name)
        assert field.type.equals(_BINARY_DICTIONARY)
        assert field.nullable is (name == "parameter_default_values")
    assert FunctionInfo.deserialize_from_bytes(original.serialize_to_bytes()) == original
    legacy = FunctionsResponse.from_infos([original, replace(original, name="second")])
    assert FunctionsResponse.deserialize_from_bytes(legacy.serialize_to_bytes()).to_infos() == [
        original,
        replace(original, name="second"),
    ]


def test_collection_dictionary_replacements_preserve_values_and_nulls() -> None:
    """Each batch may assign fresh indices, including reused bytes from earlier batches."""
    defaults = pa.record_batch({"binary": [b"\xff\x00"]})
    rows = [
        _function(str(i), value, defaults if i % 2 else None)
        for i, value in enumerate([b"a", b"a", b"\xff", b"\xff", b"a", b""])
    ]
    encoded = encode_collection(FunctionInfo, rows, batch_size=2)
    assert encoded is not None
    batches = list(pa.ipc.open_stream(encoded))
    assert len(batches) == 3
    assert [batch.column("arguments").dictionary.to_pylist() for batch in batches] == [
        [b"a"],
        [b"\xff"],
        [b"a", b""],
    ]
    assert decode_collection(FunctionInfo, encoded) == rows


def test_binary_dictionary_deduplicates_equal_content() -> None:
    """Separate equal byte allocations share one dictionary entry and reduce IPC size."""
    payload = bytes(range(256)) * 64
    defaults = pa.record_batch({"large": [payload]})
    rows = [_function(str(i), bytes(bytearray(payload)), defaults) for i in range(20)]
    encoded = encode_collection(FunctionInfo, rows)
    assert encoded is not None
    batch = pa.ipc.open_stream(encoded).read_next_batch()
    for name in _FIELDS:
        column = batch.column(name)
        assert len(column.dictionary) == 1
        assert column.indices.to_pylist() == [0] * 20
    plain_schema = pa.schema(
        [field.with_type(pa.binary()) if field.name in _FIELDS else field for field in batch.schema]
    )
    plain = pa.RecordBatch.from_arrays(
        [
            column.dictionary_decode() if field.name in _FIELDS else column
            for column, field in zip(batch, batch.schema, strict=True)
        ],
        schema=plain_schema,
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, plain_schema) as writer:
        writer.write_batch(plain)
    assert len(encoded) < sink.tell() / 4
    assert decode_collection(FunctionInfo, encoded) == rows


def test_collection_rejects_null_dictionary_value_in_required_binary_field() -> None:
    """A valid dictionary index cannot hide a null value in nonnullable arguments."""
    encoded = encode_collection(FunctionInfo, [_function("invalid", b"value")])
    assert encoded is not None
    batch = pa.ipc.open_stream(encoded).read_next_batch()
    null_value = pa.DictionaryArray.from_arrays(pa.array([0], type=pa.int32()), pa.array([None], type=pa.binary()))
    index = batch.schema.get_field_index("arguments")
    batch = batch.set_column(index, batch.schema.field(index), null_value)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    with pytest.raises(ValueError, match="arguments contains nulls"):
        decode_collection(FunctionInfo, sink.getvalue().to_pybytes())
