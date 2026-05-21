# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Minimal in-memory writable worker — no transactor, no subcursor.

Skips proper transactional semantics: data is mutated in process memory and
becomes visible to all observers immediately. ``BEGIN`` is a no-op,
``COMMIT`` is a no-op, ``ROLLBACK`` does NOT undo earlier writes. The fixture
exists only to drive the C++ extension's INSERT/UPDATE/DELETE wire path
without depending on the production writable fixture's reliance on the VGI
fork of duckdb-python (subcursor / enable_suspended_queries).

Three pre-defined tables are exposed under the ``main`` schema:

* ``items`` — supports INSERT/UPDATE/DELETE with RETURNING.
* ``items_no_returning`` — supports INSERT/UPDATE/DELETE *without* RETURNING.
  Used to exercise the supports_returning=False rejection path.
* ``items_insert_only`` — supports INSERT only (no UPDATE/DELETE/RETURNING).

State is held module-global, keyed by ``attach_opaque_data``. Per the
"pooled workers don't share per-attach state" gotcha this means the fixture
only behaves consistently when a single subprocess serves all queries for an
attach. The default pool (max=256, idle=5s) reuses the same subprocess for
back-to-back queries in a sqllogictest, so this is fine in practice — but
parallel queries on the same attach may diverge. Don't rely on this fixture
for correctness tests, only wire-protocol tests.

Registered as the ``vgi-fixture-simple-writable-worker`` entry point.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
import tempfile
import threading
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any, Literal, overload

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, Transient
from vgi_rpc.rpc import OutputCollector

from vgi.catalog import (
    AttachOpaqueData,
    CatalogAttachResult,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    SchemaInfo,
    SchemaObjectType,
    SerializedSchema,
    TableInfo,
    TransactionOpaqueData,
)
from vgi.catalog.descriptors import Catalog, Schema
from vgi.invocation import BindResponse, GlobalInitResponse
from vgi.schema_utils import schema as build_schema
from vgi.table_function import BindParams, InitParams, ProcessParams, TableFunctionGenerator
from vgi.table_in_out_function import TableInOutGenerator
from vgi.worker import Worker

if TYPE_CHECKING:
    from vgi.catalog.catalog_interface import (
        FunctionInfo,
        IndexInfo,
        MacroInfo,
        ViewInfo,
    )

__all__ = [
    "SimpleWritableCatalog",
    "SimpleWritableWorker",
    "main",
]


CATALOG_NAME = "simple_writable"

# DuckDB rowid pseudocolumn — extension reads is_row_id metadata to identify it.
_ROWID_FIELD = pa.field("rowid", pa.int64(), metadata={b"is_row_id": b""})

# Output schema for write functions returning affected row counts.
_COUNT_SCHEMA = build_schema(count=pa.int64())


# ============================================================================
# Storage — SQLite file per attach_opaque_data under TMPDIR.
#
# Pooled-worker subprocesses don't share Python state (see CLAUDE.md gotcha),
# so module-globals would lose rows whenever the pool routed a query to a
# fresh process. We persist into a SQLite file keyed by attach_opaque_data hex so the
# data survives subprocess churn for the lifetime of an ATTACH.
# ============================================================================


_SQL_TYPE_MAP: dict[pa.DataType, str] = {
    pa.int64(): "INTEGER",
    pa.int32(): "INTEGER",
    pa.string(): "TEXT",
    pa.float64(): "REAL",
    pa.bool_(): "INTEGER",
}


def _sql_type(arrow_type: pa.DataType) -> str:
    if arrow_type in _SQL_TYPE_MAP:
        return _SQL_TYPE_MAP[arrow_type]
    raise ValueError(f"simple_writable: unsupported Arrow type {arrow_type!r}")


def _table_specs() -> dict[str, pa.Schema]:
    """User-visible schema (no rowid) for each pre-defined table."""
    return {
        "items": build_schema(id=pa.int64(), name=pa.string(), qty=pa.int64()),
        "items_no_returning": build_schema(id=pa.int64(), name=pa.string(), qty=pa.int64()),
        "items_insert_only": build_schema(id=pa.int64(), name=pa.string()),
        # Lies: catalog advertises supports_returning=True but the insert
        # function always emits a (count BIGINT) batch. Used by tests to verify
        # the C++ extension rejects the mismatched batch with a clean IOException
        # instead of crashing inside ArrowToDuckDB.
        "items_broken_returning": build_schema(id=pa.int64(), name=pa.string()),
    }


def _table_supports_returning(name: str) -> bool:
    return name != "items_no_returning"


def _table_supports_update_delete(name: str) -> bool:
    # items_insert_only and items_broken_returning don't expose UPDATE/DELETE.
    return name not in {"items_insert_only", "items_broken_returning"}


_DB_DIR = os.path.join(tempfile.gettempdir(), "vgi-simple-writable")
_INIT_LOCK = threading.Lock()
_INITIALIZED: set[bytes] = set()


def _db_path(attach_opaque_data: bytes) -> str:
    return os.path.join(_DB_DIR, f"{attach_opaque_data.hex()}.sqlite")


def _ensure_init(attach_opaque_data: bytes) -> None:
    """Create per-attach SQLite file + tables if not yet seen by this process.

    Idempotent and process-local: pooled-worker subprocesses each cache once.
    """
    with _INIT_LOCK:
        if attach_opaque_data in _INITIALIZED:
            return
        os.makedirs(_DB_DIR, exist_ok=True)
        conn = sqlite3.connect(_db_path(attach_opaque_data), isolation_level=None)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            for tname, tschema in _table_specs().items():
                cols = ", ".join(f'"{f.name}" {_sql_type(f.type)}' for f in tschema)
                conn.execute(f'CREATE TABLE IF NOT EXISTS "{tname}" ({cols})')
        finally:
            conn.close()
        _INITIALIZED.add(attach_opaque_data)


@contextlib.contextmanager
def _connect(attach_opaque_data: bytes) -> Iterator[sqlite3.Connection]:
    _ensure_init(attach_opaque_data)
    conn = sqlite3.connect(_db_path(attach_opaque_data), isolation_level=None)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        yield conn
    finally:
        conn.close()


def _init_db(attach_opaque_data: bytes) -> None:
    """Eagerly initialize the per-attach store (called at catalog_attach)."""
    _ensure_init(attach_opaque_data)


def _bare_name(qualified: str) -> str:
    return qualified.split(".", 1)[1] if "." in qualified else qualified


def _get_user_schema(qualified: str) -> pa.Schema:
    bare = _bare_name(qualified)
    if bare not in _table_specs():
        raise ValueError(f"Unknown table {qualified!r}; available: {sorted(_table_specs())}")
    return _table_specs()[bare]


# ============================================================================
# Helpers shared by write functions
# ============================================================================


def _qualified_from_bind(params: BindParams[None]) -> str:
    args = params.bind_call.arguments
    if not args.positional or args.positional[0] is None:
        raise ValueError("table_name positional argument is required")
    return str(args.positional[0].as_py())


def _qualified_from_process(params: ProcessParams[None]) -> str:
    assert params.init_call is not None
    args = params.init_call.bind_call.arguments
    if not args.positional or args.positional[0] is None:
        raise ValueError("table_name positional argument is required")
    return str(args.positional[0].as_py())


def _attach_opaque_data_from_bind(params: BindParams[None]) -> bytes:
    aid = params.bind_call.attach_opaque_data
    if aid is None:
        raise ValueError("attach_opaque_data missing")
    return bytes(aid)


def _attach_opaque_data_from_process(params: ProcessParams[None]) -> bytes:
    assert params.init_call is not None
    aid = params.init_call.bind_call.attach_opaque_data
    if aid is None:
        raise ValueError("attach_opaque_data missing")
    return bytes(aid)


def _parse_write_options(params: BindParams[None]) -> dict[str, Any]:
    """Decode the write_options batch passed in named arguments."""
    defaults: dict[str, Any] = {"return_chunks": False, "on_conflict": "throw", "on_conflict_columns": []}
    if not (params.bind_call.arguments and params.bind_call.arguments.named):
        return defaults
    val = params.bind_call.arguments.named.get("write_options")
    if val is None:
        return defaults
    from vgi_rpc.utils import deserialize_record_batch

    batch, _ = deserialize_record_batch(val.as_py())
    out = dict(defaults)
    if "return_chunks" in batch.schema.names:
        out["return_chunks"] = batch.column("return_chunks")[0].as_py()
    if "on_conflict" in batch.schema.names:
        out["on_conflict"] = batch.column("on_conflict")[0].as_py()
    if "on_conflict_columns" in batch.schema.names:
        out["on_conflict_columns"] = batch.column("on_conflict_columns")[0].as_py()
    return out


def _user_schema_from_bind(params: BindParams[None]) -> pa.Schema:
    qualified = _qualified_from_bind(params)
    return _get_user_schema(qualified)


# ============================================================================
# Scan
# ============================================================================


class SimpleScan(TableFunctionGenerator[None, "_ScanState"]):
    """Scan one of the pre-defined tables — emits all current rows once."""

    class Meta:
        name = "simple_writable_scan"
        projection_pushdown = True
        filter_pushdown = False

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        qualified = _qualified_from_bind(params)
        user_schema = _get_user_schema(qualified)
        # Output schema is user_schema + rowid so UPDATE/DELETE can reference rows.
        fields = list(user_schema) + [_ROWID_FIELD]
        return BindResponse(output_schema=pa.schema(fields))

    @classmethod
    def on_init(cls, params: InitParams[None]) -> GlobalInitResponse:
        return GlobalInitResponse(max_workers=1)

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> _ScanState:
        qualified = _qualified_from_process(params)
        attach_opaque_data = _attach_opaque_data_from_process(params)
        bare = _bare_name(qualified)
        # Build SELECT list positionally — DuckDB's planner can request the
        # same column twice (e.g. "id, qty, name, id" for UPDATE...RETURNING),
        # so build one SELECT entry per output_schema field, including `rowid`.
        select_cols = [f.name for f in params.output_schema]
        select_list = ", ".join(f'"{c}"' for c in select_cols) if select_cols else "1"
        with _connect(attach_opaque_data) as conn:
            cur = conn.execute(f'SELECT {select_list} FROM "{bare}" ORDER BY rowid')
            rows = cur.fetchall()
        return _ScanState(rows=rows, schema=params.output_schema)

    @classmethod
    def process(cls, params: ProcessParams[None], state: _ScanState, out: OutputCollector) -> None:
        assert state.rows is not None and state.schema is not None
        if state.cursor >= len(state.rows):
            out.finish()
            return
        # Build column arrays positionally so duplicate field names in the
        # output schema each get the SQL row's value at that position.
        n_cols = len(state.schema)
        col_arrays: list[list[Any]] = [[] for _ in range(n_cols)]
        for row in state.rows[state.cursor :]:
            for i in range(n_cols):
                col_arrays[i].append(row[i])
        state.cursor = len(state.rows)
        arrow_arrays = [pa.array(col, type=state.schema.field(i).type) for i, col in enumerate(col_arrays)]
        out.emit(pa.RecordBatch.from_arrays(arrow_arrays, schema=state.schema))


@dataclass(kw_only=True)
class _ScanState(ArrowSerializableDataclass):
    rows: Annotated[list[tuple[Any, ...]] | None, Transient()] = None
    schema: Annotated[pa.Schema | None, Transient()] = None
    cursor: int = 0


# ============================================================================
# Insert / Update / Delete
# ============================================================================


class SimpleInsert(TableInOutGenerator[None, None]):
    """INSERT handler: append rows, optionally return the inserted rows."""

    class Meta:
        name = "simple_writable_insert"

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        opts = _parse_write_options(params)
        if opts["return_chunks"]:
            return BindResponse(output_schema=_user_schema_from_bind(params))
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[None],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        qualified = _qualified_from_process(params)
        attach_opaque_data = _attach_opaque_data_from_process(params)
        bare = _bare_name(qualified)
        user_schema = _get_user_schema(qualified)
        return_chunks = params.output_schema != _COUNT_SCHEMA

        col_names = [f.name for f in user_schema]
        cols_sql = ", ".join(f'"{c}"' for c in col_names)
        placeholders = ", ".join("?" for _ in col_names)
        rows_to_insert: list[tuple[Any, ...]] = []
        for i in range(batch.num_rows):
            rows_to_insert.append(tuple(batch.column(c)[i].as_py() for c in col_names))

        with _connect(attach_opaque_data) as conn:
            conn.execute("BEGIN")
            conn.executemany(
                f'INSERT INTO "{bare}" ({cols_sql}) VALUES ({placeholders})',
                rows_to_insert,
            )
            conn.execute("COMMIT")

        if return_chunks:
            out_cols: dict[str, list[Any]] = {c: [] for c in col_names}
            for row in rows_to_insert:
                for c, v in zip(col_names, row, strict=True):
                    out_cols[c].append(v)
            out.emit(pa.RecordBatch.from_pydict(out_cols, schema=user_schema))
        else:
            out.emit(pa.RecordBatch.from_pydict({"count": [batch.num_rows]}, schema=_COUNT_SCHEMA))


class SimpleUpdate(TableInOutGenerator[None, None]):
    """UPDATE handler: input batch is (updated_cols..., rowid)."""

    class Meta:
        name = "simple_writable_update"

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        opts = _parse_write_options(params)
        if opts["return_chunks"]:
            return BindResponse(output_schema=_user_schema_from_bind(params))
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[None],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        qualified = _qualified_from_process(params)
        attach_opaque_data = _attach_opaque_data_from_process(params)
        bare = _bare_name(qualified)
        user_schema = _get_user_schema(qualified)
        return_chunks = params.output_schema != _COUNT_SCHEMA

        update_cols = [n for n in batch.schema.names if n != "rowid"]
        set_clause = ", ".join(f'"{c}"=?' for c in update_cols)
        user_col_names = [f.name for f in user_schema]
        select_list = ", ".join(f'"{c}"' for c in user_col_names)

        rowid_col = batch.column("rowid")
        updated: list[tuple[Any, ...]] = []
        with _connect(attach_opaque_data) as conn:
            conn.execute("BEGIN")
            for i in range(batch.num_rows):
                rowid = rowid_col[i].as_py()
                values = tuple(batch.column(c)[i].as_py() for c in update_cols)
                cur = conn.execute(f'UPDATE "{bare}" SET {set_clause} WHERE rowid=?', (*values, rowid))
                if cur.rowcount == 0:
                    conn.execute("ROLLBACK")
                    raise ValueError(f"Update target rowid {rowid} not in table {qualified}")
                row = conn.execute(f'SELECT {select_list} FROM "{bare}" WHERE rowid=?', (rowid,)).fetchone()
                updated.append(row)
            conn.execute("COMMIT")

        if return_chunks:
            cols = {c: [row[i] for row in updated] for i, c in enumerate(user_col_names)}
            out.emit(pa.RecordBatch.from_pydict(cols, schema=user_schema))
        else:
            out.emit(pa.RecordBatch.from_pydict({"count": [batch.num_rows]}, schema=_COUNT_SCHEMA))


class SimpleDelete(TableInOutGenerator[None, None]):
    """DELETE handler: input batch is just (rowid,)."""

    class Meta:
        name = "simple_writable_delete"

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        opts = _parse_write_options(params)
        if opts["return_chunks"]:
            return BindResponse(output_schema=_user_schema_from_bind(params))
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[None],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        qualified = _qualified_from_process(params)
        attach_opaque_data = _attach_opaque_data_from_process(params)
        bare = _bare_name(qualified)
        user_schema = _get_user_schema(qualified)
        return_chunks = params.output_schema != _COUNT_SCHEMA

        user_col_names = [f.name for f in user_schema]
        select_list = ", ".join(f'"{c}"' for c in user_col_names)
        rowid_col = batch.column("rowid")

        deleted: list[tuple[Any, ...]] = []
        with _connect(attach_opaque_data) as conn:
            conn.execute("BEGIN")
            for i in range(batch.num_rows):
                rowid = rowid_col[i].as_py()
                row = conn.execute(f'SELECT {select_list} FROM "{bare}" WHERE rowid=?', (rowid,)).fetchone()
                if row is None:
                    conn.execute("ROLLBACK")
                    raise ValueError(f"Delete target rowid {rowid} not in table {qualified}")
                conn.execute(f'DELETE FROM "{bare}" WHERE rowid=?', (rowid,))
                deleted.append(row)
            conn.execute("COMMIT")

        if return_chunks:
            cols = {c: [row[i] for row in deleted] for i, c in enumerate(user_col_names)}
            out.emit(pa.RecordBatch.from_pydict(cols, schema=user_schema))
        else:
            out.emit(pa.RecordBatch.from_pydict({"count": [batch.num_rows]}, schema=_COUNT_SCHEMA))


class BrokenReturningInsert(TableInOutGenerator[None, None]):
    """Misbehaving INSERT handler that lies about its RETURNING support.

    Claims RETURNING support but always emits a (count BIGINT) batch —
    same shape that triggered the original SIGSEGV in the kafka worker.
    Used to verify the C++ extension's runtime schema validator throws a
    clean IOException instead of crashing inside ArrowToDuckDB.
    """

    class Meta:
        name = "simple_writable_broken_returning_insert"

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        # Always advertise the count surface, even when return_chunks=True.
        # The C++ side will see this at bind via the worker's output schema and
        # tries to route the responses through ArrowToDuckDB on the table-row
        # schema — that mismatch is what we want to catch at runtime.
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[None],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        qualified = _qualified_from_process(params)
        attach_opaque_data = _attach_opaque_data_from_process(params)
        bare = _bare_name(qualified)
        user_schema = _get_user_schema(qualified)

        col_names = [f.name for f in user_schema]
        cols_sql = ", ".join(f'"{c}"' for c in col_names)
        placeholders = ", ".join("?" for _ in col_names)
        rows_to_insert: list[tuple[Any, ...]] = []
        for i in range(batch.num_rows):
            rows_to_insert.append(tuple(batch.column(c)[i].as_py() for c in col_names))

        with _connect(attach_opaque_data) as conn:
            conn.execute("BEGIN")
            conn.executemany(
                f'INSERT INTO "{bare}" ({cols_sql}) VALUES ({placeholders})',
                rows_to_insert,
            )
            conn.execute("COMMIT")
        # Always emit count, regardless of return_chunks — that's the bug.
        out.emit(pa.RecordBatch.from_pydict({"count": [batch.num_rows]}, schema=_COUNT_SCHEMA))


# ============================================================================
# Catalog interface
# ============================================================================


_CATALOG = Catalog(
    name=CATALOG_NAME,
    default_schema="main",
    schemas=[
        Schema(
            name="main",
            functions=[SimpleScan, SimpleInsert, SimpleUpdate, SimpleDelete, BrokenReturningInsert],
            tables=[],
        ),
    ],
)


class SimpleWritableCatalog(ReadOnlyCatalogInterface):
    """Function-only catalog whose pre-defined tables live in process memory."""

    catalog = _CATALOG
    supports_transactions = False
    catalog_version_frozen = True

    def catalog_attach(
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str | None,
        implementation_version: str | None,
        ctx: Any | None = None,
    ) -> CatalogAttachResult:
        del options, data_version_spec, implementation_version, ctx
        if name != CATALOG_NAME:
            raise ValueError(f"Unknown catalog: {name!r}")
        attach_opaque_data = AttachOpaqueData(uuid.uuid4().bytes)
        # Ensure the SQLite file and schema exist before any worker tries to
        # read/write — otherwise a SELECT before the first INSERT would 500.
        _init_db(bytes(attach_opaque_data))
        return CatalogAttachResult(
            attach_opaque_data=attach_opaque_data,
            supports_transactions=False,
            supports_time_travel=False,
            catalog_version_frozen=True,
            catalog_version=1,
            attach_opaque_data_required=True,
            default_schema="main",
            settings=[],
            secret_types=[],
            resolved_data_version=None,
            resolved_implementation_version=None,
        )

    # --------- schema / table discovery ---------

    def schemas(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None
    ) -> list[SchemaInfo]:
        del transaction_opaque_data
        return [SchemaInfo(attach_opaque_data=attach_opaque_data, name="main", comment=None, tags={})]

    def schema_get(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None, name: str
    ) -> SchemaInfo | None:
        del transaction_opaque_data
        if name.lower() != "main":
            return None
        return SchemaInfo(attach_opaque_data=attach_opaque_data, name="main", comment=None, tags={})

    def _build_table_info(self, *, name: str, schema_name: str) -> TableInfo:
        user_schema = _table_specs()[name]
        # Embed rowid at the end, with is_row_id metadata.
        full = pa.schema(list(user_schema) + [_ROWID_FIELD])
        ud = _table_supports_update_delete(name)
        return TableInfo(
            comment=None,
            tags={},
            name=name,
            schema_name=schema_name,
            columns=SerializedSchema(full.serialize().to_pybytes()),
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
            primary_key_constraints=[],
            foreign_key_constraints=[],
            supports_insert=True,
            supports_update=ud,
            supports_delete=ud,
            supports_returning=_table_supports_returning(name),
        )

    def table_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        del attach_opaque_data, transaction_opaque_data, at_unit, at_value
        if schema_name.lower() != "main":
            return None
        if name.lower() not in _table_specs():
            return None
        return self._build_table_info(name=name.lower(), schema_name="main")

    def view_get(self, **kwargs: Any) -> None:
        return None

    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: Literal[SchemaObjectType.TABLE],
    ) -> Sequence[TableInfo]: ...
    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: Literal[SchemaObjectType.VIEW],
    ) -> Sequence[ViewInfo]: ...
    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
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
        name: str,
        type: Literal[SchemaObjectType.SCALAR_MACRO, SchemaObjectType.TABLE_MACRO],
    ) -> Sequence[MacroInfo]: ...
    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: Literal[SchemaObjectType.INDEX],
    ) -> Sequence[IndexInfo]: ...

    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: SchemaObjectType,
    ) -> Sequence[Any]:
        type_enum = type if isinstance(type, SchemaObjectType) else SchemaObjectType(type)
        if name.lower() != "main":
            return []
        if type_enum == SchemaObjectType.TABLE:
            return [self._build_table_info(name=tn, schema_name="main") for tn in sorted(_table_specs())]
        # Functions, views, etc. — fall through to base which uses the static catalog.
        return super().schema_contents(  # type: ignore[call-overload, no-any-return]
            attach_opaque_data=attach_opaque_data, transaction_opaque_data=transaction_opaque_data, name=name, type=type
        )

    # --------- function dispatch ---------

    def _function_get(self, kind: str, *, schema_name: str, name: str) -> ScanFunctionResult:
        qualified = f"{schema_name}.{name}" if schema_name else name
        return ScanFunctionResult(
            function_name=f"simple_writable_{kind}",
            positional_arguments=[pa.scalar(qualified)],
            named_arguments={},
        )

    def table_scan_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanFunctionResult:
        del attach_opaque_data, transaction_opaque_data, at_unit, at_value
        return self._function_get("scan", schema_name=schema_name, name=name)

    def table_insert_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        writable_branch_function_name: str | None = None,
    ) -> ScanFunctionResult:
        del attach_opaque_data, transaction_opaque_data, writable_branch_function_name
        # Route the broken table to the misbehaving insert function. Tests rely
        # on this lying about RETURNING shape so the C++ runtime validator
        # gets exercised.
        if name.lower() == "items_broken_returning":
            qualified = f"{schema_name}.{name}" if schema_name else name
            return ScanFunctionResult(
                function_name="simple_writable_broken_returning_insert",
                positional_arguments=[pa.scalar(qualified)],
                named_arguments={},
            )
        return self._function_get("insert", schema_name=schema_name, name=name)

    def table_update_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
    ) -> ScanFunctionResult:
        del attach_opaque_data, transaction_opaque_data
        return self._function_get("update", schema_name=schema_name, name=name)

    def table_delete_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
    ) -> ScanFunctionResult:
        del attach_opaque_data, transaction_opaque_data
        return self._function_get("delete", schema_name=schema_name, name=name)


class SimpleWritableWorker(Worker):
    """Worker exposing :class:`SimpleWritableCatalog`."""

    catalog_interface = SimpleWritableCatalog
    catalog = _CATALOG


def main() -> None:
    """Run the simple writable worker process."""
    SimpleWritableWorker.main()


if __name__ == "__main__":
    main()
