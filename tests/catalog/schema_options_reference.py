# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D107

"""Durable schema-option reference backend used by public-RPC lifecycle tests.

SQLite stores both committed metadata and transaction snapshots. No Python
process-local map is needed for reconnects or transaction completion. This
reference is deliberately limited to schemas; it advertises no mixed DDL/DML,
write replay, or distributed atomicity guarantee.
"""

from __future__ import annotations

import base64
import json
import sqlite3
import uuid
from contextlib import closing
from dataclasses import replace
from typing import Any, ClassVar

import pyarrow as pa

from vgi.catalog import (
    AttachOpaqueData,
    CatalogAttachResult,
    ReadOnlyCatalogInterface,
    SchemaInfo,
    TransactionOpaqueData,
)
from vgi.catalog.capabilities import (
    SCHEMA_METADATA_ALTER,
    SCHEMA_OPTIONS_ALTER,
    SCHEMA_OPTIONS_CREATE,
    SCHEMA_OPTIONS_READ,
    TRANSACTION_DDL,
    CatalogCapabilities,
)
from vgi.catalog.catalog_interface import CatalogContentsResult, OnConflict, SchemaContentsInfo
from vgi.catalog.metadata_changes import CatalogMetadataChanges
from vgi.catalog.options import OptionChanges, OptionDefinitions
from vgi.schema_path import schema_path_key
from vgi.typed_values import TypedValueBatch
from vgi.worker import Worker

SCHEMA = pa.schema(
    [
        pa.field("label", pa.string(), metadata={b"purpose": b"display"}),
        pa.field("limit", pa.int32(), nullable=False),
        pa.field("locked", pa.bool_(), nullable=False),
    ],
    metadata={b"provider": b"schema-options-reference/v1"},
)
DEFINITIONS = OptionDefinitions(
    schema=SCHEMA,
    defaults=TypedValueBatch(
        pa.RecordBatch.from_pylist([{"label": "default", "limit": 10, "locked": True}], schema=SCHEMA)
    ),
    create_names=["label", "limit"],
    alter_names=["label", "limit"],
)
PROFILES = CatalogCapabilities(
    read=[SCHEMA_OPTIONS_READ],
    ddl=sorted([SCHEMA_OPTIONS_CREATE, SCHEMA_OPTIONS_ALTER, SCHEMA_METADATA_ALTER]),
    transaction=[TRANSACTION_DDL],
)


def values(**kwargs: Any) -> TypedValueBatch:
    """Build a typed subset with the provider's full field/schema metadata."""
    schema = pa.schema([SCHEMA.field(name) for name in kwargs]).with_metadata(SCHEMA.metadata)
    return TypedValueBatch(pa.RecordBatch.from_pylist([kwargs], schema=schema))


def _key(path: list[str]) -> str:
    return json.dumps(schema_path_key(path))


class SchemaOptionsCatalog(ReadOnlyCatalogInterface):
    """Serializable snapshot transactions, atomic changes and persistent readback."""

    catalog_name = "options"
    database_path: ClassVar[str]
    profiles: ClassVar[CatalogCapabilities] = PROFILES
    catalog_version_frozen = False
    catalog_contents_attach_independent = True  # Exercise the worker's capability cache guard.

    def __init__(self) -> None:
        super().__init__()
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("CREATE TABLE IF NOT EXISTS catalog (id INTEGER PRIMARY KEY, version INTEGER, data TEXT)")
            db.execute(
                "CREATE TABLE IF NOT EXISTS transactions "
                "(id BLOB PRIMARY KEY, owner BLOB, base INTEGER, version INTEGER, data TEXT, status TEXT)"
            )
            initial = SchemaInfo(
                attach_opaque_data=AttachOpaqueData(b""),
                path=["main"],
                comment=None,
                tags={},
                options=DEFINITIONS.create(),
            )
            db.execute(
                "INSERT OR IGNORE INTO catalog VALUES (1, 1, ?)", (json.dumps({_key(["main"]): self._encode(initial)}),)
            )

    @staticmethod
    def _encode(info: SchemaInfo) -> str:
        return base64.b64encode(info.serialize_to_bytes()).decode()

    @staticmethod
    def _decode(value: str, attach: AttachOpaqueData) -> SchemaInfo:
        return replace(SchemaInfo.deserialize_from_bytes(base64.b64decode(value)), attach_opaque_data=attach)

    def catalog_attach(self, **kwargs: Any) -> CatalogAttachResult:
        result = super().catalog_attach(**kwargs)
        attach = AttachOpaqueData(uuid.uuid4().bytes)
        return replace(
            result,
            attach_opaque_data=attach,
            attach_opaque_data_required=True,
            supports_transactions=True,
            catalog_version_frozen=False,
            catalog_version=self.catalog_version(attach_opaque_data=attach, transaction_opaque_data=None),
        )

    def catalog_capabilities(self, *, attach_opaque_data: AttachOpaqueData) -> CatalogCapabilities:
        return self.profiles

    @staticmethod
    def _snapshot(
        db: sqlite3.Connection, attach: AttachOpaqueData, tx: TransactionOpaqueData | None
    ) -> tuple[int, dict[str, str]]:
        if tx is None:
            version, data = db.execute("SELECT version, data FROM catalog WHERE id=1").fetchone()
        else:
            row = db.execute(
                "SELECT version, data, status FROM transactions WHERE id=? AND owner=?", (tx, attach)
            ).fetchone()
            if row is None or row[2] != "open":
                raise ValueError("transaction is not open")
            version, data, _ = row
        return version, json.loads(data)

    def catalog_version(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        ctx: Any = None,
    ) -> int:
        with closing(sqlite3.connect(self.database_path)) as db:
            return self._snapshot(db, attach_opaque_data, transaction_opaque_data)[0]

    def schemas(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None
    ) -> list[SchemaInfo]:
        with closing(sqlite3.connect(self.database_path)) as db:
            _, data = self._snapshot(db, attach_opaque_data, transaction_opaque_data)
            return [self._decode(value, attach_opaque_data) for value in data.values()]

    def schema_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: list[str],
    ) -> SchemaInfo | None:
        return next(
            (
                info
                for info in self.schemas(
                    attach_opaque_data=attach_opaque_data, transaction_opaque_data=transaction_opaque_data
                )
                if _key(info.path) == _key(path)
            ),
            None,
        )

    def catalog_contents(
        self, *, attach_opaque_data: AttachOpaqueData, if_none_match: str | None = None
    ) -> CatalogContentsResult:
        return CatalogContentsResult(
            schemas=[
                SchemaContentsInfo(schema=info)
                for info in self.schemas(attach_opaque_data=attach_opaque_data, transaction_opaque_data=None)
            ],
            etag="deliberately-constant-provider-etag",
        )

    def schema_option_definitions(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: list[str],
    ) -> OptionDefinitions:
        schema_path_key(path)
        return DEFINITIONS

    def _mutate(
        self,
        attach: AttachOpaqueData,
        tx: TransactionOpaqueData | None,
        path: list[str],
        *,
        create: bool,
        changes: OptionChanges | None = None,
        options: TypedValueBatch | None = None,
        on_conflict: OnConflict = OnConflict.ERROR,
        comment: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        key = _key(path)
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, attach, tx)
            if create:
                candidate = DEFINITIONS.create(options)
                if len(path) > 1 and _key(path[:-1]) not in data:
                    raise ValueError("parent schema does not exist")
                if key in data:
                    if on_conflict == OnConflict.IGNORE:
                        return
                    if on_conflict != OnConflict.REPLACE:
                        raise ValueError("schema already exists")
                info = SchemaInfo(
                    attach_opaque_data=AttachOpaqueData(b""),
                    path=path,
                    comment=comment,
                    tags=tags or {},
                    options=candidate,
                )
            else:
                if key not in data:
                    raise ValueError("schema does not exist")
                info = self._decode(data[key], AttachOpaqueData(b""))
                assert info.options is not None and changes is not None
                info = replace(info, options=DEFINITIONS.apply(info.options, changes))
            data[key] = self._encode(info)
            if tx is None:
                db.execute("UPDATE catalog SET version=?, data=? WHERE id=1", (version + 1, json.dumps(data)))
            else:
                db.execute("UPDATE transactions SET version=?, data=? WHERE id=?", (version + 1, json.dumps(data), tx))

    def schema_create(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: list[str],
        on_conflict: OnConflict = OnConflict.ERROR,
        comment: str | None,
        tags: dict[str, str],
        options: TypedValueBatch | None = None,
    ) -> None:
        self._mutate(
            attach_opaque_data,
            transaction_opaque_data,
            path,
            create=True,
            options=options,
            on_conflict=on_conflict,
            comment=comment,
            tags=tags,
        )

    def schema_alter_metadata(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: list[str],
        changes: CatalogMetadataChanges,
        ignore_not_found: bool = False,
    ) -> None:
        changes.__post_init__()
        key = _key(path)
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, attach_opaque_data, transaction_opaque_data)
            if key not in data:
                if ignore_not_found:
                    return
                raise ValueError("schema does not exist")
            previous = self._decode(data[key], AttachOpaqueData(b""))
            comment, tags = changes.apply_to(previous.comment, previous.tags)
            if comment == previous.comment and tags == previous.tags:
                return
            data[key] = self._encode(replace(previous, comment=comment, tags=tags))
            if transaction_opaque_data is None:
                db.execute("UPDATE catalog SET version=?, data=? WHERE id=1", (version + 1, json.dumps(data)))
            else:
                db.execute(
                    "UPDATE transactions SET version=?, data=? WHERE id=?",
                    (version + 1, json.dumps(data), transaction_opaque_data),
                )

    def schema_alter_options(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: list[str],
        changes: OptionChanges,
    ) -> None:
        self._mutate(attach_opaque_data, transaction_opaque_data, path, create=False, changes=changes)

    def catalog_transaction_begin(self, *, attach_opaque_data: AttachOpaqueData) -> TransactionOpaqueData:
        tx = TransactionOpaqueData(uuid.uuid4().bytes)
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, attach_opaque_data, None)
            db.execute(
                "INSERT INTO transactions VALUES (?, ?, ?, ?, ?, 'open')",
                (tx, attach_opaque_data, version, version, json.dumps(data)),
            )
        return tx

    def _finish(self, attach: AttachOpaqueData, tx: TransactionOpaqueData, status: str) -> None:
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT base, version, data, status FROM transactions WHERE id=? AND owner=?", (tx, attach)
            ).fetchone()
            if row is None:
                raise ValueError("unknown transaction")
            base, version, data, previous = row
            if previous == status:
                return
            if previous != "open":
                raise ValueError("transaction is no longer open")
            if status == "committed" and version != base:
                current = db.execute("SELECT version FROM catalog WHERE id=1").fetchone()[0]
                if current != base:
                    raise ValueError("transaction conflict")
                db.execute("UPDATE catalog SET version=?, data=? WHERE id=1", (version, data))
            db.execute("UPDATE transactions SET status=? WHERE id=?", (status, tx))

    def catalog_transaction_commit(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData
    ) -> None:
        self._finish(attach_opaque_data, transaction_opaque_data, "committed")

    def catalog_transaction_rollback(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData
    ) -> None:
        self._finish(attach_opaque_data, transaction_opaque_data, "rolled_back")


class SchemaOptionsWorker(Worker):
    """Serve the reference through normal protocol dispatch and HTTP sealing."""

    catalog_interface = SchemaOptionsCatalog
