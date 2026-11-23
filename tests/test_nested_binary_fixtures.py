# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""The nested-binary corpus is fresh, and the Python reference passes it (large-values spec §3.3-§3.4).

The Python reference is the generator's source, so it must agree with the
generated SDKs: a ``BindRequest`` decoded from bytes keeps them, and every
request embedding it as ``bind_call`` re-emits exactly those bytes.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.errors import StatusError

from vgi.arguments import Arguments
from vgi.codegen import nested_binary_fixtures as nbf
from vgi.invocation import FunctionType
from vgi.protocol import (
    INVALID_ARGUMENTS_REF_KIND,
    INVALID_BIND_REQUEST_KIND,
    INVALID_BIND_REQUEST_MESSAGE,
    ArgumentsLocation,
    BindRequest,
    InitRequest,
    TableFunctionCardinalityRequest,
    TableFunctionDynamicToStringRequest,
    TableFunctionPlanRequest,
    TableFunctionProjectionRequest,
    TableFunctionStatisticsRequest,
)

CORPUS = Path(__file__).resolve().parents[1] / nbf.DIRECTORY
MANIFEST: dict[str, Any] = json.loads((CORPUS / "manifest.json").read_text())
BIND_CALL = (CORPUS / "bind_call.bin").read_bytes()
EXPECTED: dict[str, Any] = json.loads((CORPUS / "expected.json").read_text())["fields"]
RECORDS: dict[str, type[ArrowSerializableDataclass]] = {
    cls.__name__: cls
    for cls in (
        InitRequest,
        TableFunctionCardinalityRequest,
        TableFunctionStatisticsRequest,
        TableFunctionDynamicToStringRequest,
        TableFunctionPlanRequest,
        TableFunctionProjectionRequest,
    )
}


def _column(data: bytes, name: str) -> object:
    return pa.ipc.open_stream(data).read_next_batch().column(name)[0].as_py()


def test_checked_in_corpus_matches_the_generator() -> None:
    built = nbf.build()
    on_disk = {p.name: p.read_bytes() for p in CORPUS.iterdir() if p.is_file()}
    assert sorted(on_disk) == sorted(built), "corpus files differ; run scripts/regen_generated.py"
    stale = sorted(name for name in built if on_disk[name] != built[name])
    assert not stale, f"stale corpus files {stale}; run scripts/regen_generated.py"


def test_manifest_hashes_and_coverage() -> None:
    assert MANIFEST["bind_call"]["sha256"] == hashlib.sha256(BIND_CALL).hexdigest()
    assert sorted(e["record"] for e in MANIFEST["embedding"]) == sorted(RECORDS)
    for entry in [*MANIFEST["embedding"], *MANIFEST["binds"]]:
        assert entry["sha256"] == hashlib.sha256((CORPUS / entry["file"]).read_bytes()).hexdigest()
    assert MANIFEST["error_message"] == INVALID_BIND_REQUEST_MESSAGE


def test_bind_call_is_noncanonical() -> None:
    decoded = BindRequest.deserialize_from_bytes(BIND_CALL)
    assert ArrowSerializableDataclass.serialize_to_bytes(decoded) != BIND_CALL
    assert pa.ipc.open_stream(BIND_CALL).schema.metadata == nbf.NONCANONICAL_METADATA


def test_expected_json_describes_bind_call() -> None:
    assert nbf.wire_fields(BIND_CALL) == EXPECTED
    assert list(EXPECTED) == BindRequest.ARROW_SCHEMA.names


@pytest.mark.parametrize("entry", MANIFEST["embedding"], ids=lambda e: e["file"])
def test_byte_identity_round_trip(entry: dict[str, Any]) -> None:
    data = (CORPUS / entry["file"]).read_bytes()
    record = RECORDS[entry["record"]]
    assert _column(data, "bind_call") == BIND_CALL

    request = record.deserialize_from_bytes(data)
    bind_call = request.bind_call  # type: ignore[attr-defined]
    assert bind_call.wire_bytes == BIND_CALL
    assert bind_call.function_name == EXPECTED["function_name"]
    assert bind_call.function_type.name == EXPECTED["function_type"]
    assert bind_call.schema_path == EXPECTED["schema_path"]
    assert bind_call.argument_names == EXPECTED["argument_names"]
    assert bind_call.require_arguments().serialize_to_bytes() == bytes.fromhex(EXPECTED["arguments"]["hex"])
    assert bind_call.attach_opaque_data == bytes.fromhex(EXPECTED["attach_opaque_data"]["hex"])
    assert bind_call.copy_from is not None and bind_call.copy_from.file_path == EXPECTED["copy_from"]["file_path"]

    re_encoded = request.serialize_to_bytes()
    assert _column(re_encoded, "bind_call") == BIND_CALL
    assert re_encoded == data


def test_typed_construction_round_trip() -> None:
    arguments = Arguments.from_batch(pa.RecordBatch.from_pylist([{"positional_0": "x"}]))
    bind = BindRequest(
        function_name="typed", arguments=arguments, function_type=FunctionType.TABLE, schema_path=["main"]
    )
    first = bind.serialize_to_bytes()
    assert bind.serialize_to_bytes() == first
    init = InitRequest(bind_call=bind, output_schema=pa.schema([pa.field("x", pa.string())]))
    decoded = InitRequest.deserialize_from_bytes(init.serialize_to_bytes())
    assert decoded.bind_call == bind
    assert decoded.bind_call.wire_bytes == first


def test_null_arguments_with_location() -> None:
    entry = next(e for e in MANIFEST["binds"] if e["file"] == "bind-arguments-null.arrow")
    decoded = BindRequest.deserialize_from_bytes((CORPUS / entry["file"]).read_bytes())
    assert decoded.arguments is None
    location = decoded.arguments_location
    assert location is not None
    assert location.ref == bytes.fromhex(entry["arguments_location"]["ref"]["hex"])
    expected = entry["arguments_location"]
    assert (location.size, location.sha256) == (expected["size"], expected["sha256"])
    with pytest.raises(StatusError) as excinfo:
        decoded.require_arguments()
    assert excinfo.value.error_kind == "arguments_not_resolved"


@pytest.mark.parametrize("name", ["bind-both-null.arrow", "bind-both-set.arrow"])
def test_invalid_combinations_are_refused(name: str) -> None:
    with pytest.raises(StatusError) as excinfo:
        BindRequest.deserialize_from_bytes((CORPUS / name).read_bytes())
    assert excinfo.value.error_kind == INVALID_BIND_REQUEST_KIND
    assert excinfo.value.error_code == "INVALID_ARGUMENT"
    assert str(excinfo.value) == INVALID_BIND_REQUEST_MESSAGE


@pytest.mark.parametrize(
    ("size", "sha256"),
    [(0, "a" * 64), (-1, "a" * 64), (1, "A" * 64), (1, "a" * 63), (1, "g" * 64), (True, "a" * 64)],
)
def test_arguments_location_validation(size: int, sha256: str) -> None:
    with pytest.raises(StatusError) as excinfo:
        ArgumentsLocation(ref=b"r", size=size, sha256=sha256)
    assert excinfo.value.error_kind == INVALID_ARGUMENTS_REF_KIND


def test_field_order_is_normative() -> None:
    assert BindRequest.ARROW_SCHEMA.names[-1] == "arguments_location"
    assert ArgumentsLocation.ARROW_SCHEMA.names == ["ref", "size", "sha256"]
    assert BindRequest.ARROW_SCHEMA.field("arguments").nullable


def test_replace_drops_the_held_bytes_and_resolution_keeps_them() -> None:
    decoded = BindRequest.deserialize_from_bytes(BIND_CALL)
    assert dataclasses.replace(decoded, function_name="other").wire_bytes is None

    located = BindRequest.deserialize_from_bytes((CORPUS / "bind-arguments-null.arrow").read_bytes())
    resolved = located.with_resolved_arguments(Arguments())
    assert resolved.arguments is not None and resolved.arguments_location is None
    # The reference form, never the value, goes back on the wire.
    assert resolved.serialize_to_bytes() == located.wire_bytes
