# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D103, D107

"""Public catalog adapter for the finite durable constraint authority.

No scan or write function is advertised. Provider-side row replacements use the
same authority and enforce every constraint; the public surface proves catalog
creation, discovery, mutation and DDL transactions without inventing DML support.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from collections.abc import Sequence
from contextlib import closing
from dataclasses import replace
from typing import Any, ClassVar, Literal, overload

import pyarrow as pa

from vgi.catalog import (
    AttachOpaqueData,
    CatalogAttachResult,
    FunctionInfo,
    IndexInfo,
    MacroInfo,
    OnConflict,
    ReadOnlyCatalogInterface,
    SchemaInfo,
    SchemaObjectType,
    TableInfo,
    TransactionOpaqueData,
    ViewInfo,
)
from vgi.catalog.capabilities import (
    CONSTRAINT_CREATE,
    CONSTRAINT_ENFORCE,
    CONSTRAINT_MUTATE,
    CONSTRAINT_RECORDS_READ,
    ROW_SCALAR_EXPRESSION,
    TRANSACTION_DDL,
    CatalogCapabilities,
)
from vgi.catalog.catalog_interface import CatalogContentsResult, SchemaContentsInfo, SerializedSchema
from vgi.catalog.constraints import (
    ConstraintChange,
    ConstraintDefinition,
    ConstraintEnforcement,
    ConstraintKind,
    ConstraintOperation,
    ConstraintsApplyRequest,
    ConstraintSnapshot,
    ConstraintValidation,
)
from vgi.catalog.dependencies import CatalogObjectRef
from vgi.schema_path import SchemaPath
from vgi.worker import Worker

from .constraints_reference import ConstraintCatalog, _schema

PROFILES = CatalogCapabilities(
    read=sorted([CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION]),
    ddl=sorted([CONSTRAINT_CREATE, CONSTRAINT_MUTATE]),
    write=[CONSTRAINT_ENFORCE],
    transaction=[TRANSACTION_DDL],
)


class ConstraintProvider(ReadOnlyCatalogInterface):
    """Expose one durable authority through ordinary VGI catalog methods."""

    catalog_name = "constraints"
    database_path: ClassVar[str]
    profiles: ClassVar[CatalogCapabilities] = PROFILES
    catalog_version_frozen = False
    catalog_contents_attach_independent = False

    def __init__(self) -> None:
        super().__init__()
        self.authority = ConstraintCatalog(getattr(self, "database_path", "") or os.environ["VGI_CONSTRAINT_DATABASE"])

    def catalog_capabilities(self, *, attach_opaque_data: bytes) -> CatalogCapabilities:
        return self.profiles

    def catalog_attach(self, **kwargs: Any) -> CatalogAttachResult:
        result = super().catalog_attach(**kwargs)
        attach = AttachOpaqueData(uuid.uuid4().bytes)
        return replace(
            result,
            attach_opaque_data=attach,
            attach_opaque_data_required=True,
            supports_transactions=True,
            supports_catalog_contents=True,
            catalog_version_frozen=False,
            catalog_version=self.authority.version(attach),
        )

    def catalog_version(
        self, *, attach_opaque_data: bytes, transaction_opaque_data: bytes | None, **kwargs: Any
    ) -> int:
        return self.authority.version(attach_opaque_data, transaction_opaque_data)

    def schemas(self, *, attach_opaque_data: bytes, transaction_opaque_data: bytes | None) -> list[SchemaInfo]:
        self.authority.version(attach_opaque_data, transaction_opaque_data)
        return [
            SchemaInfo(attach_opaque_data=AttachOpaqueData(attach_opaque_data), path=["main"], comment=None, tags={})
        ]

    def schema_get(self, *, path: list[str], **kwargs: Any) -> SchemaInfo | None:
        return next((item for item in self.schemas(**kwargs) if item.path == path), None)

    @staticmethod
    def _info(record: dict[str, Any]) -> TableInfo:
        snapshot = ConstraintCatalog._readback(record)
        return TableInfo(
            name=snapshot.table.name,
            schema_path=snapshot.table.schema_path,
            columns=SerializedSchema(_schema(record).serialize().to_pybytes()),
            constraints=snapshot.constraints,
            definition_revision=snapshot.definition_revision,
            comment=None,
            tags={},
        )

    def _snapshot_infos(self, attach: bytes, tx: bytes | None) -> tuple[int, list[TableInfo]]:
        with closing(sqlite3.connect(self.authority.database_path)) as db:
            version, data = self.authority._snapshot(db, attach, tx)
            return version, [self._info(record) for record in data.values()]

    def table_get(
        self,
        *,
        attach_opaque_data: bytes,
        transaction_opaque_data: bytes | None,
        schema_path: list[str],
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        if at_unit is not None or at_value is not None:
            raise ValueError("constraint reference does not support time travel")
        table = CatalogObjectRef(kind="table", schema_path=schema_path, name=name)
        with closing(sqlite3.connect(self.authority.database_path)) as db:
            _, data = self.authority._snapshot(db, attach_opaque_data, transaction_opaque_data)
            try:
                record = self.authority._lookup(data, table)
            except ValueError:
                return None
            return self._info(record)

    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: SchemaPath,
        type: Literal[SchemaObjectType.TABLE],
    ) -> Sequence[TableInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: SchemaPath,
        type: Literal[SchemaObjectType.VIEW],
    ) -> Sequence[ViewInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: SchemaPath,
        type: Literal[
            SchemaObjectType.SCALAR_FUNCTION,
            SchemaObjectType.TABLE_FUNCTION,
            SchemaObjectType.AGGREGATE_FUNCTION,
        ],
    ) -> Sequence[FunctionInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: SchemaPath,
        type: Literal[SchemaObjectType.SCALAR_MACRO, SchemaObjectType.TABLE_MACRO],
    ) -> Sequence[MacroInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: SchemaPath,
        type: Literal[SchemaObjectType.INDEX],
    ) -> Sequence[IndexInfo]: ...

    def schema_contents(
        self,
        *,
        attach_opaque_data: bytes,
        transaction_opaque_data: bytes | None,
        path: list[str],
        type: SchemaObjectType,
    ) -> Sequence[TableInfo | ViewInfo | FunctionInfo | MacroInfo | IndexInfo]:
        _, infos = self._snapshot_infos(attach_opaque_data, transaction_opaque_data)
        return [info for info in infos if info.schema_path == path] if type == SchemaObjectType.TABLE else []

    def catalog_contents(self, *, attach_opaque_data: bytes, if_none_match: str | None = None) -> CatalogContentsResult:
        version, infos = self._snapshot_infos(attach_opaque_data, None)
        return CatalogContentsResult(
            schemas=[
                SchemaContentsInfo(
                    schema=self.schemas(attach_opaque_data=attach_opaque_data, transaction_opaque_data=None)[0],
                    tables=infos,
                )
            ],
            etag=str(version),
        )

    def table_create(
        self,
        *,
        attach_opaque_data: bytes,
        transaction_opaque_data: bytes | None,
        schema_path: list[str],
        name: str,
        columns: bytes,
        on_conflict: OnConflict,
        constraint_definitions: list[bytes],
        **kwargs: Any,
    ) -> None:
        if (
            schema_path != ["main"]
            or on_conflict != OnConflict.ERROR
            or any(value is not None for value in kwargs.values())
        ):
            raise ValueError("reference supports main-schema ERROR-on-conflict creation without other table options")
        self.authority.create(
            attach_opaque_data,
            CatalogObjectRef(kind="table", schema_path=schema_path, name=name),
            pa.ipc.read_schema(pa.py_buffer(columns)),
            [ConstraintDefinition.deserialize_from_bytes(value) for value in constraint_definitions],
            tx=transaction_opaque_data,
        )

    def table_constraints_apply(self, request: ConstraintsApplyRequest) -> ConstraintSnapshot:
        return self.authority.apply(request)

    def table_drop(
        self,
        *,
        attach_opaque_data: bytes,
        transaction_opaque_data: bytes | None,
        schema_path: list[str],
        name: str,
        cascade: bool = False,
        ignore_not_found: bool = False,
    ) -> None:
        table = CatalogObjectRef(kind="table", schema_path=schema_path, name=name)
        if (
            ignore_not_found
            and self.table_get(
                attach_opaque_data=attach_opaque_data,
                transaction_opaque_data=transaction_opaque_data,
                schema_path=schema_path,
                name=name,
            )
            is None
        ):
            return
        self.authority.drop_table(attach_opaque_data, table, cascade=cascade, tx=transaction_opaque_data)

    def _not_null(self, add: bool, **kwargs: Any) -> None:
        path = kwargs["column_path"]
        if len(path) != 1:
            raise ValueError("nested NOT_NULL is outside this reference")
        table = CatalogObjectRef(kind="table", schema_path=kwargs["schema_path"], name=kwargs["name"])
        attach, tx = kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"]
        with closing(sqlite3.connect(self.authority.database_path)) as db:
            _, data = self.authority._snapshot(db, attach, tx)
            record = self.authority._lookup(data, table)
            snapshot = self.authority._readback(record)
            schema = _schema(record)
        infos = snapshot.constraint_infos()
        if path[0] not in schema.names:
            raise ValueError("unknown constraint column")
        matches = [
            info
            for info in infos
            if info.definition.kind == ConstraintKind.NOT_NULL and info.definition.column_paths == [path]
        ]
        if add:
            if not schema.field(path[0]).nullable:
                return
            changes = [
                ConstraintChange(
                    operation=ConstraintOperation.ADD,
                    definition=ConstraintDefinition(kind=ConstraintKind.NOT_NULL, column_paths=[path]),
                    enforcement=ConstraintEnforcement.ENFORCED,
                    validation=ConstraintValidation.ALL_ROWS,
                )
            ]
        else:
            if any(
                info.definition.kind == ConstraintKind.PRIMARY_KEY and path in info.definition.column_paths
                for info in infos
            ):
                raise ValueError("cannot drop primary-key-implied NOT_NULL")
            if not matches:
                return
            changes = [
                ConstraintChange(operation=ConstraintOperation.DROP, ref=info.ref, cascade=False) for info in matches
            ]
        self.authority.apply(
            ConstraintsApplyRequest(
                attach_opaque_data=attach,
                transaction_opaque_data=tx,
                table=snapshot.table,
                expected_revision=snapshot.definition_revision,
                changes=[change.serialize_to_bytes() for change in changes],
            )
        )

    def table_not_null_set(self, **kwargs: Any) -> None:
        self._not_null(True, **kwargs)

    def table_not_null_drop(self, **kwargs: Any) -> None:
        self._not_null(False, **kwargs)

    def catalog_transaction_begin(self, *, attach_opaque_data: bytes) -> TransactionOpaqueData:
        return TransactionOpaqueData(self.authority.begin(attach_opaque_data))

    def catalog_transaction_commit(self, *, attach_opaque_data: bytes, transaction_opaque_data: bytes) -> None:
        self.authority.commit(attach_opaque_data, transaction_opaque_data)

    def catalog_transaction_rollback(self, *, attach_opaque_data: bytes, transaction_opaque_data: bytes) -> None:
        self.authority.rollback(attach_opaque_data, transaction_opaque_data)


class ConstraintWorker(Worker):
    """Public worker exposing only the qualified catalog operations."""

    catalog_interface = ConstraintProvider


if __name__ == "__main__":
    ConstraintWorker.main()
