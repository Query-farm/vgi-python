# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit the nested-binary conformance corpus (vgi docs/large-values-3.4.md §3.4).

Every SDK that decodes a request embedding ``bind_call`` (a ``BindRequest``
carried as a complete Arrow IPC stream in a ``binary`` column) must hold the
received bytes and re-encode them verbatim. This corpus proves it: the
embedded ``bind_call`` bytes here are valid but **non-canonical** -- the nested
stream carries schema-level custom metadata no encoder writes -- so an SDK that
re-serializes the decoded value instead of re-emitting the held bytes fails
the byte comparison.

Files, written to ``conformance/nested-binary/`` in vgi-python and copied into
each SDK checkout (``scripts/regen_generated.py``):

``bind_call.bin``
    The exact non-canonical ``BindRequest`` IPC bytes every embedding fixture
    carries.
``expected.json``
    ``bind_call.bin`` decoded column by column, in wire terms (see
    ``value_encoding`` inside it), so every language compares the same values.
``<kind>-noncanonical.arrow``
    One request of each embedding kind (``init``, ``cardinality``,
    ``statistics``, ``dynamic-to-string``, ``plan``, ``projection``) whose
    ``bind_call`` column holds ``bind_call.bin``.
``bind-arguments-null.arrow``
    A ``BindRequest`` with ``arguments = null`` and an ``arguments_location``.
``bind-both-null.arrow`` / ``bind-both-set.arrow``
    ``BindRequest`` streams a worker must refuse with ``invalid_bind_request``.
``manifest.json``
    The index: each file's role, record type, SHA-256 and expected values.

The conformance tests each SDK runs over this corpus (spec §3.4): decode each
embedding fixture, access ``bind_call`` and compare with ``expected.json``,
re-encode and compare the ``bind_call`` column with ``bind_call.bin`` (both
after access and without access); build a ``BindRequest`` from typed fields,
wrap, embed, encode, decode and compare; decode ``bind-arguments-null.arrow``
and assert ``arguments`` is null; refuse the two invalid binds.

.. code-block:: bash

   uv run --project ~/Development/vgi-python python scripts/regen_generated.py

``tests/test_nested_binary_fixtures.py`` fails when the checked-in corpus and
the generator disagree.
"""

from __future__ import annotations

import functools
import hashlib
import io
import json
import sys
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass

from vgi.arguments import Arguments
from vgi.catalog.capabilities import ROW_SCALAR_EXPRESSION
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.sql import SqlExpression
from vgi.invocation import BindRepeatability, BindResponse, FunctionType
from vgi.protocol import (
    ArgumentsLocation,
    BindRequest,
    CopyFromContext,
    InitRequest,
    TableFunctionCardinalityRequest,
    TableFunctionDynamicToStringRequest,
    TableFunctionPlanRequest,
    TableFunctionProjectionRequest,
    TableFunctionStatisticsRequest,
)

if TYPE_CHECKING:
    from typing import TextIO

GENERATOR_VERSION = "1"

#: Where the corpus lives, relative to each repository root.
DIRECTORY = "conformance/nested-binary"

#: The schema-level custom metadata that makes ``bind_call.bin`` non-canonical.
NONCANONICAL_METADATA = {b"vgi.conformance": b"nested-binary: not what an encoder writes"}


def _ipc(batch: pa.RecordBatch) -> bytes:
    """One-batch IPC stream with its end-of-stream marker."""
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue()


def _batch(record: ArrowSerializableDataclass) -> pa.RecordBatch:
    return pa.ipc.open_stream(ArrowSerializableDataclass.serialize_to_bytes(record)).read_next_batch()


def _schema(*columns: tuple[str, pa.DataType]) -> pa.Schema:
    return pa.schema(list(columns))


def _arguments() -> Arguments:
    schema = _schema(("positional_0", pa.string()), ("positional_1", pa.int64()), ("named_limit", pa.int32()))
    return Arguments.from_batch(
        pa.RecordBatch.from_pylist([{"positional_0": "seattle", "positional_1": 42, "named_limit": 7}], schema=schema)
    )


def _bind_request() -> BindRequest:
    """A bind call that exercises every column kind ``BindRequest`` has."""
    settings = pa.RecordBatch.from_pylist([{"timezone": "UTC", "threads": 4}])
    return BindRequest(
        function_name="nested_binary_fixture",
        arguments=_arguments(),
        function_type=FunctionType.TABLE,
        input_schema=None,
        settings=settings,
        secrets=None,
        attach_opaque_data=b"\x00sealed-attach-envelope\xff",
        transaction_opaque_data=b"txn-7",
        resolved_secrets_provided=True,
        at_unit="VERSION",
        at_value="12",
        copy_from=CopyFromContext(
            format="csv",
            file_path="s3://bucket/input.csv",
            expected_schema=_schema(("city", pa.string()), ("n", pa.int64())),
        ),
        copy_to=None,
        schema_path=["main", "fixtures"],
        argument_names=["city", None, "limit"],
    )


def noncanonical_bind_call() -> bytes:
    """``BindRequest`` IPC bytes that decode fine but differ from any re-encoding."""
    batch = _batch(_bind_request())
    data = _ipc(batch.replace_schema_metadata(NONCANONICAL_METADATA))
    decoded = BindRequest.deserialize_from_bytes(data)
    if ArrowSerializableDataclass.serialize_to_bytes(decoded) == data:
        raise AssertionError("the non-canonical bind_call re-encodes to itself; it proves nothing")
    return data


def _location() -> ArgumentsLocation:
    uploaded = _arguments().serialize_to_bytes()
    return ArgumentsLocation(
        ref=b"\x01sealed-argref-from-the-upload-grant",
        size=len(uploaded),
        sha256=hashlib.sha256(uploaded).hexdigest(),
    )


def _embedding_requests(bind_call: BindRequest) -> dict[str, ArrowSerializableDataclass]:
    """One request of each kind embedding ``bind_call``, keyed by fixture kind."""
    expression = SqlExpression(
        sql="n + 1",
        profile=ROW_SCALAR_EXPRESSION,
        schema_path=["main", "fixtures"],
        evaluation_context=QueryEvaluationContext(),
    ).serialize_to_bytes()
    source = _schema(("city", pa.string()), ("n", pa.int64()))
    return {
        "init": InitRequest(
            bind_call=bind_call,
            output_schema=source,
            bind_opaque_data=b"bind-state",
            projection_ids=[1, 0],
            row_limit=100,
        ),
        "cardinality": TableFunctionCardinalityRequest(bind_call=bind_call, bind_opaque_data=b"bind-state"),
        "statistics": TableFunctionStatisticsRequest(bind_call=bind_call, bind_opaque_data=None),
        "dynamic-to-string": TableFunctionDynamicToStringRequest(
            bind_call=bind_call, bind_opaque_data=b"bind-state", global_execution_id=b"exec-1"
        ),
        "plan": TableFunctionPlanRequest(bind_call=bind_call, bind_opaque_data=b"bind-state", min_splits=2),
        "projection": TableFunctionProjectionRequest(
            bind_call=bind_call,
            bind_result=BindResponse(
                output_schema=source,
                opaque_data=b"bind-state",
                projection_expression_profiles=[ROW_SCALAR_EXPRESSION],
                repeatability=BindRepeatability.UNKNOWN,
            ),
            expressions=[expression],
            result_schema=_schema(("p0", pa.int64())),
        ),
    }


def _bind_variant(*, arguments: bool, location: bool) -> bytes:
    """A ``BindRequest`` stream with the chosen ``arguments``/``arguments_location`` combination.

    Built at the Arrow level: the dataclass refuses the two invalid combinations.
    """
    batch = _batch(_bind_request())
    loc = _batch(BindRequest(**{**_shallow(_bind_request()), "arguments": None, "arguments_location": _location()}))
    columns = []
    for name in batch.schema.names:
        if name == "arguments":
            columns.append(batch.column(name) if arguments else pa.nulls(1, pa.binary()))
        elif name == "arguments_location":
            columns.append(loc.column(name) if location else batch.column(name))
        else:
            columns.append(batch.column(name))
    return _ipc(pa.RecordBatch.from_arrays(columns, schema=batch.schema))


def _shallow(record: BindRequest) -> dict[str, Any]:
    return {name: getattr(record, name) for name in BindRequest.ARROW_SCHEMA.names}


def _wire_value(value: Any) -> Any:
    """One Arrow value in the corpus's language-neutral JSON form."""
    if not value.is_valid:
        return None
    t = value.type
    if pa.types.is_dictionary(t):
        return _wire_value(value.value)
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return {"hex": value.as_py().hex()}
    if pa.types.is_struct(t):
        return {f.name: _wire_value(value[f.name]) for f in t}
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return [_wire_value(v) for v in value.values]
    return value.as_py()


def wire_fields(data: bytes) -> dict[str, Any]:
    """Decode a one-row IPC stream into ``{column: wire value}``, in column order."""
    batch = pa.ipc.open_stream(data).read_next_batch()
    return {name: _wire_value(batch.column(name)[0]) for name in batch.schema.names}


def build() -> dict[str, bytes]:
    """Every corpus file, by name, as bytes."""
    return dict(_build())


@functools.cache
def _build() -> dict[str, bytes]:
    bind_bytes = noncanonical_bind_call()
    bind_call = BindRequest.deserialize_from_bytes(bind_bytes)
    files: dict[str, bytes] = {"bind_call.bin": bind_bytes}
    embedding: list[dict[str, Any]] = []
    for kind, request in _embedding_requests(bind_call).items():
        data = request.serialize_to_bytes()
        # The Python reference keeps the received bytes, so the request re-emits them.
        held = pa.ipc.open_stream(data).read_next_batch().column("bind_call")[0].as_py()
        if held != bind_bytes:
            raise AssertionError(f"{kind}: the Python reference re-serialized bind_call")
        name = f"{kind}-noncanonical.arrow"
        files[name] = data
        embedding.append(
            {
                "file": name,
                "record": type(request).__name__,
                "field": "bind_call",
                "field_bytes": "bind_call.bin",
                "field_expected": "expected.json",
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        )

    files["bind-arguments-null.arrow"] = _bind_variant(arguments=False, location=True)
    files["bind-both-null.arrow"] = _bind_variant(arguments=False, location=False)
    files["bind-both-set.arrow"] = _bind_variant(arguments=True, location=True)

    expected = {
        "record": "BindRequest",
        "source": "bind_call.bin",
        "value_encoding": (
            "Each column by name, in column order. binary -> {'hex': lowercase hex}; utf8 and "
            "dictionary<int16, utf8> -> string; bool, int64 -> JSON scalar; list -> array; "
            "struct -> object; null -> null."
        ),
        "fields": wire_fields(bind_bytes),
    }
    files["expected.json"] = _json(expected)

    location = _location()
    manifest: dict[str, Any] = {
        "corpus": "nested-binary",
        "spec": "vgi docs/large-values-3.4.md §3.3-§3.4",
        "generator": "vgi.codegen.nested_binary_fixtures",
        "generator_version": GENERATOR_VERSION,
        "noncanonical_because": "bind_call.bin carries schema-level custom metadata "
        + json.dumps({k.decode(): v.decode() for k, v in NONCANONICAL_METADATA.items()}),
        "bind_call": {"file": "bind_call.bin", "sha256": hashlib.sha256(bind_bytes).hexdigest()},
        "embedding": embedding,
        "binds": [
            {
                "file": "bind-arguments-null.arrow",
                "record": "BindRequest",
                "valid": True,
                "arguments": None,
                "arguments_location": {
                    "ref": {"hex": location.ref.hex()},
                    "size": location.size,
                    "sha256": location.sha256,
                },
                "fields": wire_fields(files["bind-arguments-null.arrow"]),
            },
            {
                "file": "bind-both-null.arrow",
                "record": "BindRequest",
                "valid": False,
                "error_kind": "invalid_bind_request",
                "error_code": "INVALID_ARGUMENT",
            },
            {
                "file": "bind-both-set.arrow",
                "record": "BindRequest",
                "valid": False,
                "error_kind": "invalid_bind_request",
                "error_code": "INVALID_ARGUMENT",
            },
        ],
        "error_message": "BindRequest must set exactly one of 'arguments' and 'arguments_location'",
    }
    for entry in manifest["binds"]:
        entry["sha256"] = hashlib.sha256(files[entry["file"]]).hexdigest()
    files["manifest.json"] = _json(manifest)
    return files


def _json(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=False, ensure_ascii=True) + "\n").encode()


def targets() -> list[tuple[str, str]]:
    """``(file name, path relative to a repository root)`` for every corpus file."""
    return [(name, f"{DIRECTORY}/{name}") for name in sorted(build())]


def emit_bytes(*, record: str) -> bytes:
    """The bytes of one corpus file (``record`` is its file name)."""
    files = build()
    if record not in files:
        raise KeyError(f"no nested-binary fixture named {record!r}; have {sorted(files)}")
    return files[record]


def emit(out: TextIO, *, record: str = "manifest.json") -> None:
    """Text form for the JSON files (the binary ones need `emit_bytes`)."""
    out.write(emit_bytes(record=record).decode())


def main() -> None:
    """Print the manifest; ``--write DIR`` writes the whole corpus there."""
    if len(sys.argv) == 3 and sys.argv[1] == "--write":
        from pathlib import Path

        root = Path(sys.argv[2])
        root.mkdir(parents=True, exist_ok=True)
        for name, data in build().items():
            (root / name).write_bytes(data)
        return
    sys.stdout.write(emit_bytes(record="manifest.json").decode())


if __name__ == "__main__":
    main()


__all__ = ["DIRECTORY", "build", "emit", "emit_bytes", "noncanonical_bind_call", "targets", "wire_fields"]
