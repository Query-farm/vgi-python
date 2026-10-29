# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D106
"""Native path-identity and malformed-metadata fixture; never a production provider."""

from __future__ import annotations

import os
from dataclasses import fields, replace
from typing import Any

import pyarrow as pa

from vgi._test_fixtures.table.catalog_scans import _static_scan_function
from vgi.catalog import Catalog, ReadOnlyCatalogInterface, Schema, Table
from vgi.protocol import CatalogContentsResponse, TablesResponse
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


def corrupt_wire(raw: bytes | None) -> bytes | None:
    """Corrupt encoded item/collection streams after normal provider validation."""
    mode = os.environ.get("VGI_REQUIRED_FILTER_MODE")
    if raw is None or not mode:
        return raw
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
    batches = []
    with pa.ipc.open_stream(raw) as reader:
        for batch in reader:
            index = batch.schema.get_field_index("required_filters")
            if mode == "missing":
                batch = batch.remove_column(index)
            else:
                shape = (
                    pa.list_(pa.list_(pa.string())) if mode == "legacy" else pa.list_(pa.list_(pa.list_(pa.string())))
                )
                batch = batch.set_column(
                    index,
                    pa.field("required_filters", shape, nullable=False),
                    pa.array([values[mode]] * batch.num_rows, type=shape),
                )
            batches.append(batch)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batches[0].schema) as writer:
        for batch in batches:
            writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


class PathCatalog(ReadOnlyCatalogInterface):
    """Expose valid metadata; corruption happens only at the test wire boundary."""

    catalog = CATALOG

    def catalog_attach(self, **kwargs: Any) -> Any:
        return replace(super().catalog_attach(**kwargs), supports_catalog_contents=False)


class PathWorker(Worker):
    """Serve this fixture over the ordinary worker transport."""

    catalog = CATALOG
    catalog_interface = PathCatalog

    def catalog_table_get(
        self,
        attach_opaque_data: bytes,
        schema_path: list[str],
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
        transaction_opaque_data: bytes | None = None,
    ) -> TablesResponse:
        result = super().catalog_table_get(
            attach_opaque_data, schema_path, name, at_unit, at_value, transaction_opaque_data
        )
        return replace(result, items=[corrupt_wire(item) for item in result.items])

    def catalog_schema_contents_tables(
        self,
        attach_opaque_data: bytes,
        path: list[str],
        transaction_opaque_data: bytes | None = None,
    ) -> TablesResponse:
        result = super().catalog_schema_contents_tables(attach_opaque_data, path, transaction_opaque_data)
        return replace(result, items=[corrupt_wire(item) for item in result.items])

    def catalog_contents(
        self,
        attach_opaque_data: bytes,
        if_none_match: str | None = None,
    ) -> CatalogContentsResponse:
        result = super().catalog_contents(attach_opaque_data, if_none_match)
        return CatalogContentsResponse(
            **{field.name: getattr(result, field.name) for field in fields(CatalogContentsResponse)}
            | {"tables": corrupt_wire(result.tables)}
        )


if __name__ == "__main__":
    PathWorker.main()
