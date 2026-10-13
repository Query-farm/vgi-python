# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102
"""Durable view metadata reference; this fixture does not execute view queries.

The barrier read profile promises to preserve the flag and enforce it if a
consumer materializes the view. SQLite persistence alone does not qualify a
DuckDB 2 execution adapter.
"""

from __future__ import annotations

import base64
import json
import sqlite3
from contextlib import closing
from dataclasses import replace
from typing import Any, ClassVar

from vgi.catalog import AttachOpaqueData, SchemaInfo, SchemaObjectType, TransactionOpaqueData, ViewInfo
from vgi.catalog.capabilities import VIEW_BARRIER_CREATE, VIEW_BARRIER_READ, CatalogCapabilities
from vgi.catalog.catalog_interface import CatalogContentsResult, OnConflict, SchemaContentsInfo
from vgi.worker import Worker

from .schema_options_reference import PROFILES as SCHEMA_PROFILES
from .schema_options_reference import SchemaOptionsCatalog, _key

PROFILES = replace(
    SCHEMA_PROFILES,
    read=sorted([*SCHEMA_PROFILES.read, VIEW_BARRIER_READ]),
    ddl=sorted([*SCHEMA_PROFILES.ddl, VIEW_BARRIER_CREATE]),
)


def _view_key(path: list[str], name: str) -> str:
    return "view:" + json.dumps([path, name])


class ViewSecurityCatalog(SchemaOptionsCatalog):
    """Store explicit security modes in the same atomic snapshots as schemas."""

    profiles: ClassVar[CatalogCapabilities] = PROFILES

    def schemas(self, **kwargs: Any) -> list[SchemaInfo]:
        with closing(sqlite3.connect(self.database_path)) as db:
            _, data = self._snapshot(db, kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
        return [self._decode(value, kwargs["attach_opaque_data"]) for key, value in data.items() if key.startswith("[")]

    def _views(self, attach: AttachOpaqueData, tx: TransactionOpaqueData | None) -> list[ViewInfo]:
        with closing(sqlite3.connect(self.database_path)) as db:
            _, data = self._snapshot(db, attach, tx)
        return [
            ViewInfo.deserialize_from_bytes(base64.b64decode(value))
            for key, value in data.items()
            if key.startswith("view:")
        ]

    def view_get(self, **kwargs: Any) -> ViewInfo | None:
        return next(
            (
                v
                for v in self._views(kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
                if v.schema_path == kwargs["schema_path"] and v.name == kwargs["name"]
            ),
            None,
        )

    def schema_contents(self, **kwargs: Any) -> Any:
        if kwargs["type"] != SchemaObjectType.VIEW:
            return []
        return [
            v
            for v in self._views(kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
            if v.schema_path == kwargs["path"]
        ]

    def catalog_contents(self, **kwargs: Any) -> CatalogContentsResult:
        attach = kwargs["attach_opaque_data"]
        views = self._views(attach, None)
        return CatalogContentsResult(
            schemas=[
                SchemaContentsInfo(schema=schema, views=[v for v in views if v.schema_path == schema.path])
                for schema in self.schemas(attach_opaque_data=attach, transaction_opaque_data=None)
            ],
            etag="deliberately-constant-provider-etag",
        )

    def _change_view(self, operation: str, **kwargs: Any) -> None:
        attach, tx = kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"]
        path, name = kwargs["schema_path"], kwargs["name"]
        key = _view_key(path, name)
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, attach, tx)
            if _key(path) not in data:
                raise ValueError("schema does not exist")
            if operation == "create":
                info = ViewInfo(
                    name=name,
                    schema_path=path,
                    definition=kwargs["definition"],
                    aliases=list(kwargs.get("aliases") or []),
                    security=kwargs.get("security", "regular"),
                    comment=None,
                    tags={},
                )
                if key in data:
                    if kwargs["on_conflict"] == OnConflict.IGNORE:
                        return
                    if kwargs["on_conflict"] != OnConflict.REPLACE:
                        raise ValueError("view already exists")
            else:
                if key not in data:
                    if kwargs["ignore_not_found"]:
                        return
                    raise ValueError("view does not exist")
                info = ViewInfo.deserialize_from_bytes(base64.b64decode(data[key]))
                if operation == "rename":
                    new_key = _view_key(path, kwargs["new_name"])
                    if new_key in data:
                        raise ValueError("view already exists")
                    del data[key]
                    key, info = new_key, replace(info, name=kwargs["new_name"])
                elif operation == "comment":
                    info = replace(info, comment=kwargs["comment"])
                else:
                    raise AssertionError(operation)
            data[key] = base64.b64encode(info.serialize_to_bytes()).decode()
            if tx is None:
                db.execute("UPDATE catalog SET version=?, data=? WHERE id=1", (version + 1, json.dumps(data)))
            else:
                db.execute("UPDATE transactions SET version=?, data=? WHERE id=?", (version + 1, json.dumps(data), tx))

    def view_create(self, **kwargs: Any) -> None:
        self._change_view("create", **kwargs)

    def view_rename(self, **kwargs: Any) -> None:
        self._change_view("rename", **kwargs)

    def view_comment_set(self, **kwargs: Any) -> None:
        self._change_view("comment", **kwargs)


class ViewSecurityWorker(Worker):
    """Expose the metadata reference through public RPC and sealed attachments."""

    catalog_interface = ViewSecurityCatalog
