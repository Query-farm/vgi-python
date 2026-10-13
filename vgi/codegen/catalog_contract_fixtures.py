# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Emit shared Arrow IPC codec vectors for the catalog contract records.

Every worker SDK decodes these Python-written records through its real codec.
Binary value payloads must survive byte-for-byte, including nested metadata.
These vectors prove representation; option mutation and path-policy semantics
have separate reference tests and are not implied by a codec round trip.
"""

from __future__ import annotations

import base64
import json
from typing import TextIO

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass

from vgi.catalog.options import OptionChanges, OptionState
from vgi.column_path import RequiredFilterPaths
from vgi.typed_values import TypedValueBatch


def render() -> str:
    """Return deterministic JSON with populated and empty record encodings."""
    schema = pa.schema(
        [
            pa.field("optional", pa.int32(), metadata={b"logical": b"example.optional"}),
            pa.field(
                "record",
                pa.struct([pa.field("a.b", pa.string(), nullable=False, metadata={b"nested": b"retained"})]),
                metadata={b"logical": b"example.record"},
            ),
        ],
        metadata={b"provider": b"catalog-contracts/v1"},
    )
    values = TypedValueBatch(
        pa.RecordBatch.from_pylist([{"optional": None, "record": {"a.b": 'literal."name'}}], schema=schema)
    )
    empty = TypedValueBatch.empty(metadata={b"provider": b"no-options"})
    records: dict[str, TypedValueBatch | ArrowSerializableDataclass] = {
        "values": values,
        "empty_values": empty,
        "changes": OptionChanges(set_values=values, reset_names=["reset.me"]),
        "reset_only": OptionChanges(reset_names=["reset.me"]),
        "state": OptionState(values=values, explicit_names=["optional"]),
        "empty_state": OptionState(values=empty),
        "paths": RequiredFilterPaths(groups=[[["a.b"], ["a", "b"]], [["record", 'literal."name']]]),
        "empty_paths": RequiredFilterPaths(),
    }
    return (
        json.dumps(
            {
                "format_version": 1,
                "generator": "vgi.codegen.catalog_contract_fixtures",
                "records": {
                    name: base64.b64encode(record.serialize_to_bytes()).decode("ascii")
                    for name, record in records.items()
                },
            },
            indent=2,
        )
        + "\n"
    )


def emit(out: TextIO) -> None:
    """Write the corpus through the normal multi-repository generation driver."""
    out.write(render())
