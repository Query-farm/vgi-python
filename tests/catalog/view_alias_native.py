# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D106
"""View metadata fixture for native alias validation; not a production catalog."""

from __future__ import annotations

import os
from dataclasses import fields, replace
from typing import Any

import pyarrow as pa

from vgi.catalog import Catalog, ReadOnlyCatalogInterface, Schema, SchemaObjectType, View
from vgi.protocol import CatalogContentsResponse, ViewsResponse
from vgi.worker import Worker

CATALOG = Catalog(
    name="views",
    default_schema="main",
    schemas=[
        Schema(
            path=["main"], views=[View(name="v", definition="SELECT 11 AS x, 22 AS y", aliases=["a.b", 'with"quote'])]
        )
    ],
)


def corrupt_wire(raw: bytes | None) -> bytes | None:
    """Change encoded item/collection streams after normal metadata validation."""
    mode = os.environ.get("VGI_VIEW_ALIAS_MODE")
    security = os.environ.get("VGI_VIEW_SECURITY_MODE")
    if raw is None or not (mode or security):
        return raw
    batches = []
    with pa.ipc.open_stream(raw) as reader:
        for batch in reader:
            column = "security" if security else "aliases"
            index = batch.schema.get_field_index(column)
            selected = security or mode
            if selected == "missing":
                batch = batch.remove_column(index)
            elif security:
                if security == "wrong_type":
                    field = pa.field("security", pa.int32())
                    value: Any = 1
                else:
                    field = batch.schema.field(index)
                    value = None if security == "null" else security
                batch = batch.set_column(index, field, pa.array([value] * batch.num_rows, type=field.type))
            else:
                assert mode is not None
                values: dict[str, Any] = {
                    "null": None,
                    "null_item": [None, "y"],
                    "empty": ["", "y"],
                    "nul": ["bad\0", "y"],
                }
                batch = batch.set_column(
                    index,
                    batch.schema.field(index),
                    pa.array([values[mode]] * batch.num_rows, type=pa.list_(pa.string())),
                )
            batches.append(batch)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batches[0].schema) as writer:
        for batch in batches:
            writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


class ViewsCatalog(ReadOnlyCatalogInterface):
    """Expose the same view through get and listing."""

    catalog = CATALOG

    def catalog_attach(self, **kwargs: Any) -> Any:
        return replace(
            super().catalog_attach(**kwargs),
            supports_catalog_contents=os.environ.get("VGI_VIEW_METADATA_PATH") == "bulk",
        )

    _building_contents = False

    def catalog_contents(self, **kwargs: Any) -> Any:
        self._building_contents = True
        try:
            result = super().catalog_contents(**kwargs)
        finally:
            self._building_contents = False
        return result

    def view_get(self, **kwargs: Any) -> Any:
        if os.environ.get("VGI_VIEW_METADATA_PATH") in ("listing", "bulk"):
            raise AssertionError("unexpected single-view read in discovery test")
        info = super().view_get(**kwargs)
        return info

    def schema_contents(self, **kwargs: Any) -> Any:
        path = os.environ.get("VGI_VIEW_METADATA_PATH")
        if kwargs["type"] == SchemaObjectType.VIEW and (
            path == "get" or (path == "bulk" and not self._building_contents)
        ):
            raise AssertionError("unexpected schema-list read in view metadata test")
        infos = super().schema_contents(**kwargs)
        return infos


class ViewsWorker(Worker):
    """Serve malformed metadata over the normal transport."""

    catalog = CATALOG
    catalog_interface = ViewsCatalog

    def catalog_view_get(
        self,
        attach_opaque_data: bytes,
        schema_path: list[str],
        name: str,
        transaction_opaque_data: bytes | None = None,
    ) -> ViewsResponse:
        result = super().catalog_view_get(attach_opaque_data, schema_path, name, transaction_opaque_data)
        return replace(result, items=[corrupt_wire(item) for item in result.items])

    def catalog_schema_contents_views(
        self,
        attach_opaque_data: bytes,
        path: list[str],
        transaction_opaque_data: bytes | None = None,
    ) -> ViewsResponse:
        result = super().catalog_schema_contents_views(attach_opaque_data, path, transaction_opaque_data)
        return replace(result, items=[corrupt_wire(item) for item in result.items])

    def catalog_contents(
        self,
        attach_opaque_data: bytes,
        if_none_match: str | None = None,
    ) -> CatalogContentsResponse:
        result = super().catalog_contents(attach_opaque_data, if_none_match)
        return CatalogContentsResponse(
            **{field.name: getattr(result, field.name) for field in fields(CatalogContentsResponse)}
            | {"views": corrupt_wire(result.views)}
        )


if __name__ == "__main__":
    ViewsWorker.main()
