# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D106
"""Native path-identity and malformed-metadata fixture; never a production provider."""

from __future__ import annotations

import os
from dataclasses import fields
from typing import Any

import pyarrow as pa
from vgi_rpc.utils import deserialize_record_batch

from vgi._test_fixtures.table.catalog_scans import _static_scan_function
from vgi.catalog import Catalog, ReadOnlyCatalogInterface, Schema, SchemaObjectType, Table, TableInfo
from vgi.worker import Worker

COLUMNS = pa.schema(
    [
        ("a.b", pa.int64()),
        (
            "a",
            pa.struct(
                [
                    ("b", pa.int64()),
                    ("MixedCase", pa.int64()),
                    ('q."uote', pa.int64()),
                    ("deep", pa.struct([("leaf", pa.int64()), ("other", pa.int64())])),
                ]
            ),
        ),
    ]
)
Scan = _static_scan_function(
    "required_path_scan",
    "Literal and nested identifiers",
    COLUMNS,
    {"a.b": [1], "a": [{"b": 1, "MixedCase": 1, 'q."uote': 1, "deep": {"leaf": 1, "other": 1}}]},
)
CATALOG = Catalog(
    name="paths",
    default_schema="main",
    schemas=[
        Schema(
            path=["main"],
            tables=[
                Table(name="literal", function=Scan, required_filters=((("a.b",),),)),
                Table(name="nested", function=Scan, required_filters=((("a", "b"),),)),
                Table(name="case_path", function=Scan, required_filters=((("a", "MixedCase"),),)),
                Table(name="both_paths", function=Scan, required_filters=((("a.b",),), (("a", "b"),))),
                Table(name="parent_path", function=Scan, required_filters=((("a",),),)),
                Table(name="deep_path", function=Scan, required_filters=((("a", "deep", "leaf"),),)),
                Table(name="quoted_path", function=Scan, required_filters=((("a", 'q."uote'),),)),
            ],
        )
    ],
)


class MalformedInfo(TableInfo):
    """Corrupt only the wire field after normal table metadata construction."""

    def serialize_to_bytes(self) -> bytes:
        batch, _ = deserialize_record_batch(super().serialize_to_bytes())
        mode = os.environ["VGI_REQUIRED_FILTER_MODE"]
        index = batch.schema.get_field_index("required_filters")
        shape = pa.list_(pa.list_(pa.list_(pa.string())))
        values: dict[str, Any] = {
            "null": None,
            "null_group": [None],
            "empty_group": [[]],
            "null_path": [[None]],
            "empty_path": [[[]]],
            "null_component": [[[None]]],
            "empty_component": [[[""]]],
            "nul_component": [[["a\0"]]],
            "missing_component": [[["a", "typo"]]],
            "scalar_parent": [[["a.b", "child"]]],
            "duplicate_path": [[["a.b"], ["a.b"]]],
            "legacy": [["a.b"]],
        }
        if mode == "missing":
            batch = batch.remove_column(index)
        else:
            if mode == "legacy":
                shape = pa.list_(pa.list_(pa.string()))
            batch = batch.set_column(
                index, pa.field("required_filters", shape, nullable=False), pa.array([values[mode]], type=shape)
            )
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        return sink.getvalue().to_pybytes()


def corrupt(info: TableInfo) -> TableInfo:
    """Select opt-in malformed wire metadata without bypassing the normal RPC path."""
    if not os.environ.get("VGI_REQUIRED_FILTER_MODE"):
        return info
    return MalformedInfo(**{f.name: getattr(info, f.name) for f in fields(TableInfo)})


class PathCatalog(ReadOnlyCatalogInterface):
    """Use normal discovery and scans, with an opt-in malformed response."""

    catalog = CATALOG
    supports_catalog_contents = False

    def schema_contents(self, **kwargs: Any) -> Any:
        result = super().schema_contents(**kwargs)
        return [corrupt(info) for info in result] if kwargs["type"] == SchemaObjectType.TABLE else result

    def table_get(self, **kwargs: Any) -> Any:
        info = super().table_get(**kwargs)
        return corrupt(info) if info is not None else None


class PathWorker(Worker):
    """Serve this fixture over the ordinary worker transport."""

    catalog = CATALOG
    catalog_interface = PathCatalog


if __name__ == "__main__":
    PathWorker.main()
