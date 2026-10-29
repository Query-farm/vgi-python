# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D103, D106, D107

"""Durable finite SQLite indexes with atomic catalog/physical DDL publication.

One real table has binary-collated INTEGER/TEXT columns. Regular and UNIQUE
indexes admit top-level raw keys only; associations, PRIMARY, expressions,
options and CASCADE reject. Transaction snapshots use private physical tables
and indexes in the same SQLite authority; commits check the catalog revision.
Public scans are available, while writes remain provider-side test operations.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass, replace
from typing import Any, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.catalog import (
    AttachOpaqueData,
    Catalog,
    CatalogAttachResult,
    IndexInfo,
    OnConflict,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    Schema,
    SchemaInfo,
    SchemaObjectType,
    TableInfo,
    TransactionOpaqueData,
)
from vgi.catalog.capabilities import (
    CONSTRAINT_ENFORCE,
    CONSTRAINT_RECORDS_READ,
    INDEX_DEFINITION_CREATE,
    INDEX_DEFINITION_READ,
    INDEX_DROP,
    TRANSACTION_DDL,
    CatalogCapabilities,
)
from vgi.catalog.catalog_interface import CatalogContentsResult, SchemaContentsInfo, SerializedSchema
from vgi.catalog.dependencies import CatalogObjectRef
from vgi.catalog.indexes import IndexConstraintType, IndexDefinition
from vgi.catalog.options import OptionDefinitions, OptionState
from vgi.invocation import BindResponse, GlobalInitResponse
from vgi.table_function import BindParams, InitParams, ProcessParams, TableFunctionGenerator
from vgi.typed_values import TypedValueBatch
from vgi.worker import Worker

ALGORITHM = "SQLITE.BTREE.V1"
TABLE = CatalogObjectRef(kind="table", schema_path=["main"], name="items")
COLUMN_FIELDS: list[pa.Field[Any]] = [pa.field("id", pa.int64()), pa.field("label", pa.string())]
COLUMNS = pa.schema(COLUMN_FIELDS)
PROFILES = CatalogCapabilities(
    read=sorted([CONSTRAINT_RECORDS_READ, INDEX_DEFINITION_READ]),
    ddl=sorted([INDEX_DEFINITION_CREATE, INDEX_DROP]),
    write=[CONSTRAINT_ENFORCE],
    transaction=[TRANSACTION_DDL],
)


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class IndexAuthority:
    """Store real indexes and their canonical records in the same transaction."""

    def __init__(self, path: str) -> None:
        self.path = path
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("CREATE TABLE IF NOT EXISTS state (version INTEGER NOT NULL)")
            if not db.execute("SELECT 1 FROM state").fetchone():
                db.execute("INSERT INTO state VALUES (0)")
                db.execute("CREATE TABLE items (id INTEGER COLLATE BINARY, label TEXT COLLATE BINARY)")
                db.executemany("INSERT INTO items VALUES (?, ?)", [(1, "one"), (2, "two")])
            db.execute("CREATE TABLE IF NOT EXISTS sessions (token BLOB PRIMARY KEY)")
            db.execute(
                "CREATE TABLE IF NOT EXISTS transactions "
                "(token BLOB PRIMARY KEY, owner BLOB, status TEXT, base INTEGER, physical TEXT)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS index_records "
                "(scope BLOB, name TEXT, physical TEXT UNIQUE, record BLOB, PRIMARY KEY(scope,name))"
            )

    def attach(self) -> bytes:
        token = uuid.uuid4().bytes
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("INSERT INTO sessions VALUES (?)", (token,))
        return token

    @contextmanager
    def state(self, attach: bytes, tx: bytes | None = None) -> Iterator[tuple[sqlite3.Connection, str, bytes]]:
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                if not db.execute("SELECT 1 FROM sessions WHERE token=?", (attach,)).fetchone():
                    raise ValueError("unknown attachment")
                physical, scope = "items", b""
                if tx is not None:
                    row = db.execute("SELECT owner,status,physical FROM transactions WHERE token=?", (tx,)).fetchone()
                    if row is None or row[0] != attach or row[1] != "active":
                        raise ValueError("unknown, foreign or terminal transaction")
                    physical, scope = row[2], tx
                yield db, physical, scope
                db.commit()
            except BaseException:
                db.rollback()
                raise

    @staticmethod
    def _validate(definition: IndexDefinition) -> list[str]:
        definition.__post_init__()
        if definition.table != TABLE or definition.algorithm != ALGORITHM:
            raise ValueError("unsupported table or index algorithm")
        if definition.associated_constraint is not None or definition.constraint_type == IndexConstraintType.PRIMARY:
            raise ValueError("constraint-associated and PRIMARY indexes are unsupported")
        if definition.options.schema.names or definition.options.schema.metadata:
            raise ValueError("index algorithm accepts no options or option metadata")
        keys = definition.key_definitions()
        if any(
            key.column_path is None or len(key.column_path) != 1 or key.column_path[0] not in COLUMNS.names
            for key in keys
        ):
            raise ValueError("index algorithm requires top-level raw id/label keys with established BINARY collation")
        columns = [key.column_path[0] for key in keys if key.column_path is not None]
        if len(set(columns)) != len(columns):
            raise ValueError("duplicate index keys are unsupported")
        return columns

    @staticmethod
    def _infos(db: sqlite3.Connection, scope: bytes) -> list[IndexInfo]:
        infos = []
        for physical, encoded in db.execute(
            "SELECT physical,record FROM index_records WHERE scope=? ORDER BY name", (scope,)
        ):
            info = IndexInfo.deserialize_from_bytes(encoded)
            row = db.execute("SELECT tbl_name FROM sqlite_master WHERE type='index' AND name=?", (physical,)).fetchone()
            if row is None:
                raise ValueError("index metadata has no physical index")
            keys = [item[2] for item in db.execute(f"PRAGMA index_info({_quote(physical)})")]
            expected = IndexAuthority._validate(info.definition)
            if keys != expected:
                raise ValueError("index physical keys disagree with its definition")
            listed = next(item for item in db.execute(f"PRAGMA index_list({_quote(row[0])})") if item[1] == physical)
            if bool(listed[2]) != (info.definition.constraint_type == IndexConstraintType.UNIQUE):
                raise ValueError("index physical uniqueness disagrees with its definition")
            if any(
                item[4] != "BINARY" or item[3] != 0
                for item in db.execute(f"PRAGMA index_xinfo({_quote(physical)})")
                if item[5]
            ):
                raise ValueError("index physical collation/order disagrees with the qualified algorithm")
            infos.append(info)
        return infos

    @staticmethod
    def _construct(db: sqlite3.Connection, table: str, scope: bytes, info: IndexInfo) -> None:
        columns = IndexAuthority._validate(info.definition)
        physical = "vgi_index_" + uuid.uuid4().hex
        unique = "UNIQUE " if info.definition.constraint_type == IndexConstraintType.UNIQUE else ""
        key_sql = ", ".join(_quote(name) + " COLLATE BINARY" for name in columns)
        db.execute(f"CREATE {unique}INDEX {_quote(physical)} ON {_quote(table)} ({key_sql})")
        db.execute(
            "INSERT INTO index_records VALUES (?, ?, ?, ?)", (scope, info.name, physical, info.serialize_to_bytes())
        )

    @staticmethod
    def _remove(db: sqlite3.Connection, scope: bytes, name: str) -> bool:
        row = db.execute("SELECT physical FROM index_records WHERE scope=? AND name=?", (scope, name)).fetchone()
        if row is None:
            return False
        db.execute(f"DROP INDEX {_quote(row[0])}")
        db.execute("DELETE FROM index_records WHERE scope=? AND name=?", (scope, name))
        return True

    def create(
        self, attach: bytes, name: str, definition: IndexDefinition, conflict: OnConflict, tx: bytes | None
    ) -> None:
        self._validate(definition)
        info = IndexInfo(
            name=name,
            schema_path=["main"],
            comment=None,
            tags={},
            definition=definition,
            options=OptionState(values=definition.options, explicit_names=[]),
        )
        with self.state(attach, tx) as (db, physical, scope):
            exists = db.execute("SELECT 1 FROM index_records WHERE scope=? AND name=?", (scope, name)).fetchone()
            if exists:
                if conflict == OnConflict.IGNORE:
                    return
                if conflict == OnConflict.ERROR:
                    raise ValueError("index already exists")
                self._remove(db, scope, name)
            self._construct(db, physical, scope, info)
            if tx is None:
                db.execute("UPDATE state SET version=version+1")

    def drop(self, attach: bytes, name: str, ignore: bool, tx: bytes | None) -> None:
        with self.state(attach, tx) as (db, _, scope):
            if not self._remove(db, scope, name):
                if ignore:
                    return
                raise ValueError("index does not exist")
            if tx is None:
                db.execute("UPDATE state SET version=version+1")

    def infos(self, attach: bytes, tx: bytes | None = None) -> tuple[int, list[IndexInfo]]:
        with self.state(attach, tx) as (db, _, scope):
            return db.execute("SELECT version FROM state").fetchone()[0], self._infos(db, scope)

    def rows(self, attach: bytes, tx: bytes | None = None) -> list[dict[str, Any]]:
        with self.state(attach, tx) as (db, physical, _):
            return [
                dict(zip(COLUMNS.names, row, strict=True))
                for row in db.execute(f"SELECT id,label FROM {_quote(physical)} ORDER BY rowid")
            ]

    def replace_rows(self, attach: bytes, rows: list[dict[str, Any]]) -> None:
        # Provider-side writes prove SQLite enforces every admitted UNIQUE index.
        batch = pa.RecordBatch.from_pylist(rows, schema=COLUMNS)
        with self.state(attach) as (db, physical, _):
            db.execute(f"DELETE FROM {_quote(physical)}")
            db.executemany(
                f"INSERT INTO {_quote(physical)} VALUES (?, ?)",
                [(row["id"], row["label"]) for row in batch.to_pylist()],
            )
            db.execute("UPDATE state SET version=version+1")

    def begin(self, attach: bytes) -> bytes:
        token = uuid.uuid4().bytes
        physical = "vgi_tx_" + token.hex()
        with self.state(attach) as (db, _, scope):
            version = db.execute("SELECT version FROM state").fetchone()[0]
            db.execute(f"CREATE TABLE {_quote(physical)} (id INTEGER COLLATE BINARY, label TEXT COLLATE BINARY)")
            db.execute(f"INSERT INTO {_quote(physical)} SELECT * FROM items")
            for info in self._infos(db, scope):
                self._construct(db, physical, token, info)
            db.execute("INSERT INTO transactions VALUES (?, ?, 'active', ?, ?)", (token, attach, version, physical))
        return token

    def finish(self, attach: bytes, token: bytes, commit: bool) -> None:
        target = "committed" if commit else "rolled_back"
        with self.state(attach) as (db, _, _):
            row = db.execute("SELECT owner,status,base,physical FROM transactions WHERE token=?", (token,)).fetchone()
            if row is None or row[0] != attach:
                raise ValueError("unknown or foreign transaction")
            if row[1] == target:
                return
            if row[1] != "active":
                raise ValueError("terminal transaction")
            if commit:
                if row[2] != db.execute("SELECT version FROM state").fetchone()[0]:
                    raise ValueError("stale transaction revision")
                pending = self._infos(db, token)
                for info in self._infos(db, b""):
                    self._remove(db, b"", info.name)
                for info in pending:
                    self._construct(db, "items", b"", info)
                db.execute("UPDATE state SET version=version+1")
            db.execute(f"DROP TABLE {_quote(row[3])}")
            db.execute("DELETE FROM index_records WHERE scope=?", (token,))
            db.execute("UPDATE transactions SET status=? WHERE token=?", (target, token))


@dataclass
class ScanState(ArrowSerializableDataclass):
    """One bounded snapshot batch."""

    emitted: bool = False


class IndexScan(TableFunctionGenerator[None, ScanState]):
    """Scan actual SQLite rows; no public DML function is advertised."""

    class Meta:
        name = "index_reference_scan"
        projection_pushdown = False
        filter_pushdown = False

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        return BindResponse(output_schema=COLUMNS)

    @classmethod
    def on_init(cls, params: InitParams[None]) -> GlobalInitResponse:
        return GlobalInitResponse(max_workers=1)

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> ScanState:
        return ScanState()

    @classmethod
    def process(cls, params: ProcessParams[None], state: ScanState, out: OutputCollector) -> None:
        if state.emitted:
            out.finish()
            return
        if params.attach_opaque_data is None or params.init_call is None:
            raise ValueError("reference scan requires an attachment")
        tx = params.init_call.bind_call.transaction_opaque_data
        rows = IndexProvider().authority.rows(params.attach_opaque_data, tx)
        out.emit(pa.RecordBatch.from_pylist(rows, schema=params.output_schema))
        state.emitted = True


CATALOG = Catalog(name="indexes", default_schema="main", schemas=[Schema(path=["main"], functions=[IndexScan])])


class IndexProvider(ReadOnlyCatalogInterface):
    """Expose the finite authority through normal catalog and scan methods."""

    catalog = CATALOG
    database_path: ClassVar[str]
    catalog_version_frozen = False
    catalog_contents_attach_independent = False

    def __init__(self) -> None:
        super().__init__()
        self.authority = IndexAuthority(getattr(self, "database_path", "") or os.environ["VGI_INDEX_DATABASE"])

    def catalog_capabilities(self, **kwargs: Any) -> CatalogCapabilities:
        return PROFILES

    def catalog_attach(self, **kwargs: Any) -> CatalogAttachResult:
        result = super().catalog_attach(**kwargs)
        attach = AttachOpaqueData(self.authority.attach())
        return replace(
            result,
            attach_opaque_data=attach,
            attach_opaque_data_required=True,
            supports_transactions=True,
            supports_catalog_contents=True,
            catalog_version_frozen=False,
            catalog_version=self.authority.infos(attach)[0],
        )

    def catalog_version(
        self, *, attach_opaque_data: bytes, transaction_opaque_data: bytes | None, **kwargs: Any
    ) -> int:
        return self.authority.infos(attach_opaque_data, transaction_opaque_data)[0]

    @staticmethod
    def _schema(attach: bytes, index_count: int) -> SchemaInfo:
        return SchemaInfo(
            attach_opaque_data=AttachOpaqueData(attach),
            path=["main"],
            comment=None,
            tags={},
            estimated_object_count={"table": 1, "table_function": 1, "index": index_count},
        )

    def schemas(self, *, attach_opaque_data: bytes, transaction_opaque_data: bytes | None) -> list[SchemaInfo]:
        _, infos = self.authority.infos(attach_opaque_data, transaction_opaque_data)
        return [self._schema(attach_opaque_data, len(infos))]

    def schema_get(self, *, path: list[str], **kwargs: Any) -> SchemaInfo | None:
        schemas = self.schemas(**kwargs)
        return schemas[0] if path == ["main"] else None

    def table_get(self, *, schema_path: list[str], name: str, **kwargs: Any) -> TableInfo | None:
        self.authority.infos(kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
        if kwargs.get("at_unit") is not None or kwargs.get("at_value") is not None:
            raise ValueError("reference does not support time travel")
        if schema_path != ["main"] or name != "items":
            return None
        return TableInfo(
            name=name,
            schema_path=schema_path,
            columns=SerializedSchema(COLUMNS.serialize().to_pybytes()),
            constraints=[],
            comment=None,
            tags={},
        )

    def schema_contents(self, **kwargs: Any) -> Any:
        if kwargs["type"] == SchemaObjectType.INDEX:
            return (
                self.authority.infos(kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])[1]
                if kwargs["path"] == ["main"]
                else []
            )
        if kwargs["type"] == SchemaObjectType.TABLE:
            info = self.table_get(
                attach_opaque_data=kwargs["attach_opaque_data"],
                transaction_opaque_data=kwargs["transaction_opaque_data"],
                schema_path=kwargs["path"],
                name="items",
            )
            return [info] if info is not None else []
        return super().schema_contents(**kwargs)

    def catalog_contents(self, *, attach_opaque_data: bytes, if_none_match: str | None = None) -> CatalogContentsResult:
        version, infos = self.authority.infos(attach_opaque_data)
        table = self.table_get(
            attach_opaque_data=attach_opaque_data, transaction_opaque_data=None, schema_path=["main"], name="items"
        )
        assert table is not None
        return CatalogContentsResult(
            schemas=[
                SchemaContentsInfo(
                    schema=self._schema(attach_opaque_data, len(infos)),
                    tables=[table],
                    indexes=infos,
                    table_functions=ReadOnlyCatalogInterface.schema_contents(
                        self,
                        attach_opaque_data=AttachOpaqueData(attach_opaque_data),
                        transaction_opaque_data=None,
                        path=["main"],
                        type=SchemaObjectType.TABLE_FUNCTION,
                    ),
                )
            ],
            etag=str(version),
        )

    def table_scan_function_get(self, **kwargs: Any) -> ScanFunctionResult:
        if self.table_get(**kwargs) is None:
            raise ValueError("table does not exist")
        return ScanFunctionResult(function_name="index_reference_scan", positional_arguments=[], named_arguments={})

    def index_option_definitions(
        self,
        *,
        attach_opaque_data: bytes,
        transaction_opaque_data: bytes | None,
        table: CatalogObjectRef,
        algorithm: str,
    ) -> OptionDefinitions:
        self.authority.infos(attach_opaque_data, transaction_opaque_data)
        if table != TABLE or algorithm != ALGORITHM:
            raise ValueError("unsupported table or index algorithm")
        return OptionDefinitions(schema=pa.schema([]), defaults=TypedValueBatch.empty())

    def index_get(
        self, *, attach_opaque_data: bytes, transaction_opaque_data: bytes | None, schema_path: list[str], name: str
    ) -> IndexInfo | None:
        infos = self.authority.infos(attach_opaque_data, transaction_opaque_data)[1]
        return next((info for info in infos if info.schema_path == schema_path and info.name == name), None)

    def index_create(
        self,
        *,
        attach_opaque_data: bytes,
        transaction_opaque_data: bytes | None,
        schema_path: list[str],
        name: str,
        definition: IndexDefinition,
        on_conflict: OnConflict,
    ) -> None:
        if schema_path != ["main"] or not name or "\0" in name:
            raise ValueError("unsupported index namespace or name")
        self.authority.create(attach_opaque_data, name, definition, on_conflict, transaction_opaque_data)

    def index_drop(
        self,
        *,
        attach_opaque_data: bytes,
        transaction_opaque_data: bytes | None,
        schema_path: list[str],
        name: str,
        ignore_not_found: bool,
        cascade: bool = False,
    ) -> None:
        if cascade or schema_path != ["main"]:
            raise ValueError("reference does not support CASCADE or another schema")
        self.authority.drop(attach_opaque_data, name, ignore_not_found, transaction_opaque_data)

    def catalog_transaction_begin(self, *, attach_opaque_data: bytes) -> TransactionOpaqueData:
        return TransactionOpaqueData(self.authority.begin(attach_opaque_data))

    def catalog_transaction_commit(self, *, attach_opaque_data: bytes, transaction_opaque_data: bytes) -> None:
        self.authority.finish(attach_opaque_data, transaction_opaque_data, True)

    def catalog_transaction_rollback(self, *, attach_opaque_data: bytes, transaction_opaque_data: bytes) -> None:
        self.authority.finish(attach_opaque_data, transaction_opaque_data, False)


class IndexWorker(Worker):
    """Public typed index reference worker."""

    catalog_interface = IndexProvider
    catalog = CATALOG


if __name__ == "__main__":
    IndexWorker.main()
