# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Arguments retain Arrow metadata through binding, init, and HTTP transport."""

from __future__ import annotations

from dataclasses import replace

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient
from vgi_rpc.rpc import CallContext

from vgi.arguments import Arguments
from vgi.client import Client
from vgi.invocation import BindResponse, FunctionType
from vgi.protocol import BindRequest, InitRequest
from vgi.serve import create_app
from vgi.typed_values import TypedValueBatch
from vgi.worker import Worker


def _arguments() -> pa.RecordBatch:
    schema = pa.schema(
        [
            pa.field("named_a.b", pa.int32(), metadata={b"logical": b"provider.optional"}),
            pa.field(
                "positional_0",
                pa.struct([pa.field("child", pa.string(), metadata={b"nested": b"preserved"})]),
                nullable=False,
                metadata={b"logical": b"provider.record"},
            ),
        ],
        metadata={b"context": b"preserved"},
    )
    return pa.RecordBatch.from_pylist([{"named_a.b": None, "positional_0": {"child": "value"}}], schema=schema)


def test_arguments_preserve_metadata_and_wire_layout() -> None:
    original = _arguments()
    args = Arguments.from_batch(original)
    assert args.to_batch().equals(original, check_metadata=True)
    payload = args.serialize_to_bytes()
    wire = pa.ipc.open_stream(payload).read_next_batch()
    assert wire.schema.names == ["args"]
    assert wire.num_rows == 1
    assert list(wire.schema.field("args").type) == list(original.schema)
    restored = Arguments.deserialize_from_bytes(payload)
    assert restored.to_batch().equals(original, check_metadata=True)
    assert restored.get("a.b", default=42) is None
    assert restored.get("absent", default=42) == 42
    assert restored.get(0) == {"child": "value"}


def test_struct_decode_preserves_field_metadata() -> None:
    original = _arguments()
    restored = Arguments.decode(original.to_struct_array()[0])
    # Schema metadata lives outside a struct scalar; field metadata is inside.
    assert restored.schema().equals(original.schema.remove_metadata(), check_metadata=True)


def test_explicit_schema_preserves_metadata_for_scalar_callers() -> None:
    schema = pa.schema([pa.field("positional_0", pa.int64(), metadata={b"meaning": b"counter"})])
    args = Arguments(positional=(pa.scalar(42),), argument_schema=schema)
    assert Arguments.deserialize_from_bytes(args.serialize_to_bytes()).schema().equals(schema, check_metadata=True)


def test_replaced_values_must_match_retained_schema() -> None:
    args = Arguments.from_batch(_arguments())
    with pytest.raises(ValueError, match="declared type"):
        replace(args, named={"a.b": pa.scalar("different type")}).serialize_to_bytes()
    with pytest.raises(ValueError, match="exactly the supplied"):
        replace(args, named={"renamed": pa.scalar(None, type=pa.int32())}).serialize_to_bytes()


def test_empty_arguments_preserve_metadata() -> None:
    batch = TypedValueBatch.empty(metadata={b"context": b"no arguments"}).batch
    restored = Arguments.deserialize_from_bytes(Arguments.from_batch(batch).serialize_to_bytes())
    assert restored.to_batch().equals(batch, check_metadata=True)
    assert restored.positional == ()
    assert restored.named is None


def test_bind_and_init_preserve_argument_metadata() -> None:
    original = _arguments()
    bind = BindRequest(function_name="f", arguments=Arguments.from_batch(original), function_type=FunctionType.TABLE)
    restored = BindRequest.deserialize_from_bytes(bind.serialize_to_bytes())
    assert restored.arguments.to_batch().equals(original, check_metadata=True)
    init = InitRequest(bind_call=restored, output_schema=pa.schema([]))
    restored_init = InitRequest.deserialize_from_bytes(init.serialize_to_bytes())
    assert restored_init.bind_call.arguments.to_batch().equals(original, check_metadata=True)


@pytest.mark.parametrize(
    "array",
    [
        pa.array(["a"]).dictionary_encode(),
        pa.ExtensionArray.from_storage(pa.bool8(), pa.array([1], type=pa.int8())),
        pa.RunEndEncodedArray.from_arrays(pa.array([1], type=pa.int16()), pa.array([42])),
        pa.UnionArray.from_dense(pa.array([0], type=pa.int8()), pa.array([0], type=pa.int32()), [pa.array([42])]),
        pa.array([[("a", 1)]], type=pa.map_(pa.string(), pa.int64())),
    ],
)
def test_encoded_arrow_types_retain_values_and_annotations(array: pa.Array[pa.Scalar[pa.DataType]]) -> None:
    schema = pa.schema([pa.field("positional_0", array.type, metadata={b"annotation": b"preserved"})])
    batch = pa.RecordBatch.from_arrays([array], schema=schema)
    values = TypedValueBatch.deserialize_from_bytes(TypedValueBatch(batch).serialize_to_bytes())
    assert values.batch.equals(batch, check_metadata=True)
    args = Arguments.deserialize_from_bytes(Arguments.from_batch(batch).serialize_to_bytes())
    assert args.to_batch().equals(batch, check_metadata=True)


@pytest.mark.parametrize(
    "name", ["positional_-1", "positional_01", "positional_1", "positional_99999999999999999999", "named_", "unknown"]
)
def test_malformed_argument_names_reject(name: str) -> None:
    with pytest.raises(ValueError, match="invalid|contiguous"):
        Arguments.from_batch(pa.record_batch({name: [1]}))


@pytest.mark.parametrize(
    "batch",
    [
        pa.record_batch({"wrong": [1]}),
        pa.record_batch({"args": [1]}),
        pa.record_batch({"args": pa.array([None], type=pa.struct([pa.field("positional_0", pa.int64())]))}),
    ],
)
def test_malformed_argument_envelope_rejects(batch: pa.RecordBatch) -> None:
    with pytest.raises(ValueError, match="args struct|present struct"):
        Arguments.deserialize_from_bytes(TypedValueBatch(batch).serialize_to_bytes())


def test_duckdb_15_null_empty_struct_means_no_arguments() -> None:
    """Native 1.5 table-input functions send this form for their empty constants."""
    batch = pa.record_batch({"args": pa.array([None], type=pa.struct([]))})
    restored = Arguments.deserialize_from_bytes(TypedValueBatch(batch).serialize_to_bytes())
    assert restored.positional == ()
    assert restored.named is None
    assert Arguments.decode(batch.column(0)[0]) == Arguments()


class _MetadataWorker(Worker):
    """Return the decoded argument schema through the real bind RPC."""

    def bind(self, request: BindRequest, ctx: CallContext) -> BindResponse:
        return BindResponse(output_schema=request.arguments.schema())


def test_public_http_bind_preserves_argument_metadata() -> None:
    original = _arguments()
    app = create_app(_MetadataWorker, describe=False, signing_key=b"metadata-test-key")
    with Client.from_http("http://vgi.test", httpx_client=_SyncTestClient(app)) as client:
        response = client.bind(function_name="metadata", schema_path=["main"], arguments=Arguments.from_batch(original))
    assert response.output_schema.equals(original.schema, check_metadata=True)
