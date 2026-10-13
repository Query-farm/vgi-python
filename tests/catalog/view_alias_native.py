# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D106
"""View metadata fixture for native alias validation; not a production catalog."""

from __future__ import annotations

import os
from dataclasses import fields
from typing import Any

import pyarrow as pa
from vgi_rpc.utils import deserialize_record_batch

from vgi.catalog import Catalog, ReadOnlyCatalogInterface, Schema, SchemaObjectType, View, ViewInfo
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


class WireView(ViewInfo):
    """Change the wire payload after metadata validation."""

    def serialize_to_bytes(self) -> bytes:
        batch, _ = deserialize_record_batch(super().serialize_to_bytes())
        mode = os.environ.get("VGI_VIEW_ALIAS_MODE")
        if not mode:
            return super().serialize_to_bytes()
        index = batch.schema.get_field_index("aliases")
        if mode == "missing":
            batch = batch.remove_column(index)
        else:
            values: dict[str, Any] = {"null": None, "null_item": [None, "y"], "empty": ["", "y"], "nul": ["bad\0", "y"]}
            batch = batch.set_column(
                index, batch.schema.field(index), pa.array([values[mode]], type=pa.list_(pa.string()))
            )
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        return sink.getvalue().to_pybytes()


def wire(info: ViewInfo) -> ViewInfo:
    """Wrap valid metadata with the corrupting test serializer."""
    return WireView(**{f.name: getattr(info, f.name) for f in fields(ViewInfo)})


class ViewsCatalog(ReadOnlyCatalogInterface):
    """Expose the same view through get and listing."""

    catalog = CATALOG
    supports_catalog_contents = False

    def view_get(self, **kwargs: Any) -> Any:
        info = super().view_get(**kwargs)
        return wire(info) if info else None

    def schema_contents(self, **kwargs: Any) -> Any:
        infos = super().schema_contents(**kwargs)
        return [wire(info) for info in infos] if kwargs["type"] == SchemaObjectType.VIEW else infos


class ViewsWorker(Worker):
    """Serve malformed metadata over the normal transport."""

    catalog = CATALOG
    catalog_interface = ViewsCatalog


if __name__ == "__main__":
    ViewsWorker.main()
