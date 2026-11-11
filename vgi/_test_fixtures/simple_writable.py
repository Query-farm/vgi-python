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
  Used to exercise the ``count``-only RETURNING rejection path.
* ``items_insert_only`` — supports INSERT only (no UPDATE/DELETE/RETURNING).

``CREATE TABLE`` (and CTAS) in ``main`` adds tables of its own to the attach
(columns of type INTEGER, BIGINT, VARCHAR, DOUBLE or BOOLEAN; scan and INSERT,
no row id), and ``DROP TABLE`` removes them. ``catalog_table_create`` reports
``created`` (protocol 3.3): false when ``IF NOT EXISTS`` found the table, which
CTAS then leaves unchanged. The write functions are listed ``table_only``.

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
import dataclasses
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
    FUNCTION_VISIBILITY_TABLE_ONLY,
    AttachOpaqueData,
    CatalogAttachResult,
    OnConflict,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    SchemaInfo,
    SchemaObjectType,
    SerializedSchema,
    TableInfo,
    TransactionOpaqueData,
)
from vgi.catalog.descriptors import Catalog, Schema
from vgi.catalog.layout import TableLayout
from vgi.invocation import BindResponse, GlobalInitResponse
from vgi.protocol import ClientCapabilities
from vgi.schema_utils import schema as build_schema
from vgi.table_function import BindParams, InitParams, ProcessParams, TableFunctionGenerator
from vgi.table_in_out_function import TableInOutGenerator
from vgi.worker import Worker
from vgi.write_results import WriteResultMode, coerce_write_result_mode, write_changes_batch, write_result_schema

if TYPE_CHECKING:
    from vgi.catalog.catalog_interface import (
        FunctionInfo,
        IndexInfo,
        MacroInfo,
        ViewInfo,
    )
    from vgi.typed_values import TypedValueBatch

__all__ = [
    "SimpleWritableCatalog",
    "SimpleWritableWorker",
    "main",
]


CATALOG_NAME = "simple_writable"

# DuckDB rowid pseudocolumn — extension reads is_row_id metadata to identify it.
_ROWID_FIELD = pa.field("rowid", pa.int64(), metadata={b"is_row_id": b""})

# Output schema for write functions returning affected row counts.
_COUNT_SCHEMA = pa.schema([pa.field("count", pa.int64(), nullable=False)])


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
        # Lies: catalog advertises rows support but the insert
        # function always emits a (count BIGINT) batch. Used by tests to verify
        # the C++ extension rejects the mismatched batch with a clean IOException
        # instead of crashing inside ArrowToDuckDB.
        "items_broken_returning": build_schema(id=pa.int64(), name=pa.string()),
    }


def _table_result_mode(name: str) -> str:
    if name == "items_no_returning":
        return "count"
    if name == "items_broken_returning":
        return "rows"
    return "changes"


def _table_supports_update_delete(name: str) -> bool:
    # items_insert_only and items_broken_returning don't expose UPDATE/DELETE.
    return name not in {"items_insert_only", "items_broken_returning"}


_DB_DIR = os.path.join(tempfile.gettempdir(), "vgi-simple-writable")
#: Per-attach registry of tables created by ``catalog_table_create``: lowercase
#: name -> the user columns as a serialized Arrow schema. Their rows live in a
#: SQLite table of the same name.
_CREATED = "__vgi_created_tables"
#: Per-attach catalog version, bumped by every CREATE/DROP TABLE.
_VERSION = "__vgi_catalog_version"
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
            conn.execute(f'CREATE TABLE IF NOT EXISTS "{_CREATED}" (name TEXT PRIMARY KEY, columns BLOB NOT NULL)')
            conn.execute(f'CREATE TABLE IF NOT EXISTS "{_VERSION}" (one INTEGER PRIMARY KEY, version INTEGER NOT NULL)')
            conn.execute(f'INSERT OR IGNORE INTO "{_VERSION}" (one, version) VALUES (1, 1)')
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


def _created_tables(attach_opaque_data: bytes) -> dict[str, pa.Schema]:
    """The tables ``catalog_table_create`` added to this attach (lowercase name -> user columns)."""
    with _connect(attach_opaque_data) as conn:
        rows = conn.execute(f'SELECT name, columns FROM "{_CREATED}" ORDER BY name').fetchall()
    return {name: pa.ipc.read_schema(pa.py_buffer(columns)) for name, columns in rows}


def _catalog_version(attach_opaque_data: bytes) -> int:
    with _connect(attach_opaque_data) as conn:
        row = conn.execute(f'SELECT version FROM "{_VERSION}" WHERE one = 1').fetchone()
    return int(row[0]) if row else 1


def _get_user_schema(qualified: str, attach_opaque_data: bytes) -> pa.Schema:
    bare = _bare_name(qualified).lower()
    if bare in _table_specs():
        return _table_specs()[bare]
    created = _created_tables(attach_opaque_data)
    if bare in created:
        return created[bare]
    raise ValueError(f"Unknown table {qualified!r}; available: {sorted([*_table_specs(), *created])}")


def _is_created(qualified: str) -> bool:
    return _bare_name(qualified).lower() not in _table_specs()


def _to_arrow(values: list[Any], dtype: pa.DataType) -> pa.Array[Any]:
    """SQLite stores BOOLEAN as 0/1; everything else maps directly."""
    if pa.types.is_boolean(dtype):
        values = [None if v is None else bool(v) for v in values]
    return pa.array(values, type=dtype)


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
    # Unwrapped plaintext attach (storage shards on the sealed form via request).
    aid = params.attach_opaque_data
    if aid is None:
        raise ValueError("attach_opaque_data missing")
    return bytes(aid)


def _attach_opaque_data_from_process(params: ProcessParams[None]) -> bytes:
    aid = params.attach_opaque_data
    if aid is None:
        raise ValueError("attach_opaque_data missing")
    return bytes(aid)


def _parse_write_options(params: BindParams[None] | ProcessParams[None]) -> dict[str, Any]:
    """Decode the write_options batch passed in named arguments."""
    # Protocol 3.3: the batch carries only result_mode.
    defaults: dict[str, Any] = {"result_mode": "count"}
    if isinstance(params, BindParams):
        bind_call = params.bind_call
    else:
        assert params.init_call is not None
        bind_call = params.init_call.bind_call
    if not (bind_call.arguments and bind_call.arguments.named):
        return defaults
    val = bind_call.arguments.named.get("write_options")
    if val is None:
        return defaults
    from vgi_rpc.utils import deserialize_record_batch

    batch, _ = deserialize_record_batch(val.as_py())
    out = dict(defaults)
    if "result_mode" in batch.schema.names:
        out["result_mode"] = coerce_write_result_mode(batch.column("result_mode")[0].as_py())
    return out


def _emit_changes(
    out: OutputCollector,
    table_schema: pa.Schema,
    old_rows: Sequence[tuple[Any, ...] | None],
    new_rows: Sequence[tuple[Any, ...] | None],
) -> None:
    names = table_schema.names

    def as_struct(row: tuple[Any, ...] | None) -> dict[str, Any] | None:
        return None if row is None else dict(zip(names, row, strict=True))

    out.emit(
        write_changes_batch(
            table_schema,
            [as_struct(row) for row in old_rows],
            [as_struct(row) for row in new_rows],
        )
    )


def _user_schema_from_bind(params: BindParams[None]) -> pa.Schema:
    qualified = _qualified_from_bind(params)
    return _get_user_schema(qualified, _attach_opaque_data_from_bind(params))


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
        user_schema = _get_user_schema(qualified, _attach_opaque_data_from_bind(params))
        # Output schema is user_schema + rowid so UPDATE/DELETE can reference rows;
        # created tables have no row id (insert only).
        fields = list(user_schema) + ([] if _is_created(qualified) else [_ROWID_FIELD])
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
        arrow_arrays = [_to_arrow(col, state.schema.field(i).type) for i, col in enumerate(col_arrays)]
        out.emit(pa.RecordBatch.from_arrays(arrow_arrays, schema=state.schema))
        out.finish()


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
        mode = opts["result_mode"]
        return BindResponse(output_schema=write_result_schema(mode, _user_schema_from_bind(params)))

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
        user_schema = _get_user_schema(qualified, attach_opaque_data)
        result_mode: WriteResultMode = _parse_write_options(params)["result_mode"]

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

        if result_mode == "rows":
            out_cols: dict[str, list[Any]] = {c: [] for c in col_names}
            for row in rows_to_insert:
                for c, v in zip(col_names, row, strict=True):
                    out_cols[c].append(v)
            out.emit(pa.RecordBatch.from_pydict(out_cols, schema=user_schema))
        elif result_mode == "changes":
            _emit_changes(out, user_schema, [None] * len(rows_to_insert), rows_to_insert)
        else:
            out.emit(pa.RecordBatch.from_pydict({"count": [batch.num_rows]}, schema=_COUNT_SCHEMA))


class SimpleUpdate(TableInOutGenerator[None, None]):
    """UPDATE handler: input batch is (updated_cols..., rowid)."""

    class Meta:
        name = "simple_writable_update"

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        opts = _parse_write_options(params)
        mode = opts["result_mode"]
        return BindResponse(output_schema=write_result_schema(mode, _user_schema_from_bind(params)))

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
        user_schema = _get_user_schema(qualified, attach_opaque_data)
        result_mode: WriteResultMode = _parse_write_options(params)["result_mode"]

        update_cols = [n for n in batch.schema.names if n != "rowid"]
        set_clause = ", ".join(f'"{c}"=?' for c in update_cols)
        user_col_names = [f.name for f in user_schema]
        select_list = ", ".join(f'"{c}"' for c in user_col_names)

        rowid_col = batch.column("rowid")
        old_rows: list[tuple[Any, ...]] = []
        updated: list[tuple[Any, ...]] = []
        with _connect(attach_opaque_data) as conn:
            conn.execute("BEGIN")
            for i in range(batch.num_rows):
                rowid = rowid_col[i].as_py()
                old_row = conn.execute(f'SELECT {select_list} FROM "{bare}" WHERE rowid=?', (rowid,)).fetchone()
                if old_row is None:
                    conn.execute("ROLLBACK")
                    raise ValueError(f"Update target rowid {rowid} not in table {qualified}")
                values = tuple(batch.column(c)[i].as_py() for c in update_cols)
                cur = conn.execute(f'UPDATE "{bare}" SET {set_clause} WHERE rowid=?', (*values, rowid))
                if cur.rowcount == 0:
                    conn.execute("ROLLBACK")
                    raise ValueError(f"Update target rowid {rowid} not in table {qualified}")
                row = conn.execute(f'SELECT {select_list} FROM "{bare}" WHERE rowid=?', (rowid,)).fetchone()
                old_rows.append(old_row)
                updated.append(row)
            conn.execute("COMMIT")

        if result_mode == "rows":
            cols = {c: [row[i] for row in updated] for i, c in enumerate(user_col_names)}
            out.emit(pa.RecordBatch.from_pydict(cols, schema=user_schema))
        elif result_mode == "changes":
            _emit_changes(out, user_schema, old_rows, updated)
        else:
            out.emit(pa.RecordBatch.from_pydict({"count": [batch.num_rows]}, schema=_COUNT_SCHEMA))


class SimpleDelete(TableInOutGenerator[None, None]):
    """DELETE handler: input batch is just (rowid,)."""

    class Meta:
        name = "simple_writable_delete"

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        opts = _parse_write_options(params)
        mode = opts["result_mode"]
        return BindResponse(output_schema=write_result_schema(mode, _user_schema_from_bind(params)))

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
        user_schema = _get_user_schema(qualified, attach_opaque_data)
        result_mode: WriteResultMode = _parse_write_options(params)["result_mode"]

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

        if result_mode == "rows":
            cols = {c: [row[i] for row in deleted] for i, c in enumerate(user_col_names)}
            out.emit(pa.RecordBatch.from_pydict(cols, schema=user_schema))
        elif result_mode == "changes":
            _emit_changes(out, user_schema, deleted, [None] * len(deleted))
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
        # Always advertise the count surface, even when result_mode=rows.
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
        user_schema = _get_user_schema(qualified, attach_opaque_data)

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
        # Always emit count, regardless of result_mode — that's the bug.
        out.emit(pa.RecordBatch.from_pydict({"count": [batch.num_rows]}, schema=_COUNT_SCHEMA))


# ============================================================================
# Catalog interface
# ============================================================================


_CATALOG = Catalog(
    name=CATALOG_NAME,
    default_schema="main",
    schemas=[
        Schema(
            path=["main"],
            functions=[SimpleScan, SimpleInsert, SimpleUpdate, SimpleDelete, BrokenReturningInsert],
            tables=[],
        ),
    ],
)


#: Write functions: listed so DuckDB has their capabilities, never SQL-callable (protocol 3.3).
_WRITE_FUNCTIONS = frozenset(cls.Meta.name for cls in (SimpleInsert, SimpleUpdate, SimpleDelete, BrokenReturningInsert))


class SimpleWritableCatalog(ReadOnlyCatalogInterface):
    """Function-only catalog whose pre-defined (and created) tables live in a per-attach SQLite file."""

    catalog = _CATALOG
    supports_transactions = False
    # CREATE/DROP TABLE change the catalog, so the version is live (see catalog_version).
    catalog_version_frozen = False

    def catalog_attach(
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str | None,
        implementation_version: str | None,
        client_capabilities: ClientCapabilities | None = None,
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
            catalog_version_frozen=False,
            catalog_version=_catalog_version(bytes(attach_opaque_data)),
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
        return [SchemaInfo(attach_opaque_data=attach_opaque_data, path=["main"], comment=None, tags={})]

    def schema_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: list[str],
    ) -> SchemaInfo | None:
        del transaction_opaque_data
        if [component.lower() for component in path] != ["main"]:
            return None
        return SchemaInfo(attach_opaque_data=attach_opaque_data, path=["main"], comment=None, tags={})

    def catalog_version(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        ctx: Any | None = None,
    ) -> int:
        del transaction_opaque_data, ctx
        return _catalog_version(bytes(attach_opaque_data))

    def _build_created_table_info(self, *, name: str, columns: pa.Schema, schema_path: list[str]) -> TableInfo:
        # No row id: a created table is scan + INSERT only.
        return TableInfo(
            comment=None,
            tags={},
            name=name,
            schema_path=schema_path,
            columns=SerializedSchema(columns.serialize().to_pybytes()),
            constraints=[],
            write_result_modes={"insert": "rows"},
        )

    def _all_table_infos(self, attach_opaque_data: bytes) -> dict[str, TableInfo]:
        infos = {tn: self._build_table_info(name=tn, schema_path=["main"]) for tn in _table_specs()}
        for tn, columns in _created_tables(attach_opaque_data).items():
            infos[tn] = self._build_created_table_info(name=tn, columns=columns, schema_path=["main"])
        return infos

    # --------- DDL: created tables (CTAS) ---------

    def table_create(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_path: list[str],
        name: str,
        columns: SerializedSchema,
        on_conflict: OnConflict,
        constraint_definitions: list[bytes],
        comment: str | None = None,
        tags: dict[str, str] | None = None,
        options: TypedValueBatch | None = None,
        layout: TableLayout | None = None,
    ) -> bool:
        """Create a table of the attach; ``False`` when IF NOT EXISTS found one (protocol 3.3)."""
        del transaction_opaque_data
        if [component.lower() for component in schema_path] != ["main"]:
            raise ValueError(f"simple_writable: tables can only be created in schema main, not {schema_path!r}")
        if constraint_definitions or any(v is not None for v in (comment, tags, options, layout)):
            raise NotImplementedError("simple_writable: CREATE TABLE supports plain columns only")
        user_schema = pa.ipc.read_schema(pa.py_buffer(bytes(columns)))
        if not len(user_schema):
            raise ValueError("simple_writable: a table needs at least one column")
        for f in user_schema:
            _sql_type(f.type)  # rejects an unsupported type before anything changes
        key = name.lower()
        attach = bytes(attach_opaque_data)
        exists = key in _table_specs() or key in _created_tables(attach)
        if exists:
            if on_conflict == OnConflict.IGNORE:
                return False
            if on_conflict == OnConflict.ERROR or key in _table_specs():
                raise ValueError(f'simple_writable: Table "{name}" already exists')
        plain = pa.schema([pa.field(f.name, f.type) for f in user_schema])
        cols_sql = ", ".join(f'"{f.name}" {_sql_type(f.type)}' for f in plain)
        with _connect(attach) as conn:
            conn.execute("BEGIN IMMEDIATE")
            if exists:  # REPLACE: the old table and its rows go first
                conn.execute(f'DROP TABLE IF EXISTS "{key}"')
            conn.execute(f'CREATE TABLE "{key}" ({cols_sql})')
            conn.execute(
                f'INSERT OR REPLACE INTO "{_CREATED}" (name, columns) VALUES (?, ?)',
                (key, plain.serialize().to_pybytes()),
            )
            conn.execute(f'UPDATE "{_VERSION}" SET version = version + 1 WHERE one = 1')
            conn.execute("COMMIT")
        return True

    def table_drop(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_path: list[str],
        name: str,
        ignore_not_found: bool,
        cascade: bool = False,
    ) -> None:
        """Drop a created table (the pre-defined ones cannot be dropped)."""
        del transaction_opaque_data, cascade
        key = name.lower()
        attach = bytes(attach_opaque_data)
        if [component.lower() for component in schema_path] == ["main"] and key in _table_specs():
            raise ValueError(f"simple_writable: the pre-defined table {name!r} cannot be dropped")
        if [component.lower() for component in schema_path] != ["main"] or key not in _created_tables(attach):
            if ignore_not_found:
                return
            raise ValueError(f'simple_writable: Table "{name}" does not exist')
        with _connect(attach) as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(f'DROP TABLE IF EXISTS "{key}"')
            conn.execute(f'DELETE FROM "{_CREATED}" WHERE name = ?', (key,))
            conn.execute(f'UPDATE "{_VERSION}" SET version = version + 1 WHERE one = 1')
            conn.execute("COMMIT")

    def _build_table_info(self, *, name: str, schema_path: list[str]) -> TableInfo:
        user_schema = _table_specs()[name]
        # Embed rowid at the end, with is_row_id metadata.
        full = pa.schema(list(user_schema) + [_ROWID_FIELD])
        ud = _table_supports_update_delete(name)
        return TableInfo(
            comment=None,
            tags={},
            name=name,
            schema_path=schema_path,
            columns=SerializedSchema(full.serialize().to_pybytes()),
            constraints=[],
            write_result_modes={
                operation: _table_result_mode(name)
                for operation, supported in (("insert", True), ("update", ud), ("delete", ud))
                if supported
            },
        )

    def table_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_path: list[str],
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        del transaction_opaque_data, at_unit, at_value
        if [component.lower() for component in schema_path] != ["main"]:
            return None
        return self._all_table_infos(bytes(attach_opaque_data)).get(name.lower())

    def view_get(self, **kwargs: Any) -> None:
        return None

    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: list[str],
        type: Literal[SchemaObjectType.TABLE],
    ) -> Sequence[TableInfo]: ...
    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: list[str],
        type: Literal[SchemaObjectType.VIEW],
    ) -> Sequence[ViewInfo]: ...
    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: list[str],
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
        path: list[str],
        type: Literal[SchemaObjectType.SCALAR_MACRO, SchemaObjectType.TABLE_MACRO],
    ) -> Sequence[MacroInfo]: ...
    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: list[str],
        type: Literal[SchemaObjectType.INDEX],
    ) -> Sequence[IndexInfo]: ...

    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: list[str],
        type: SchemaObjectType,
    ) -> Sequence[Any]:
        type_enum = type if isinstance(type, SchemaObjectType) else SchemaObjectType(type)
        if [component.lower() for component in path] != ["main"]:
            return []
        if type_enum == SchemaObjectType.TABLE:
            infos = self._all_table_infos(bytes(attach_opaque_data))
            return [infos[tn] for tn in sorted(infos)]
        # Functions, views, etc. — fall through to base which uses the static catalog.
        result = super().schema_contents(  # type: ignore[call-overload]
            attach_opaque_data=attach_opaque_data, transaction_opaque_data=transaction_opaque_data, path=path, type=type
        )
        if type_enum == SchemaObjectType.TABLE_FUNCTION:
            # Protocol 3.3: write functions are listed (DuckDB needs has_finalize and
            # their schemas) but table_only: not in duckdb_functions(), not callable.
            result = [
                dataclasses.replace(info, visibility=FUNCTION_VISIBILITY_TABLE_ONLY)
                if info.name in _WRITE_FUNCTIONS
                else info
                for info in result
            ]
        return result  # type: ignore[no-any-return]

    # --------- function dispatch ---------

    def _function_get(self, kind: str, *, schema_path: list[str], name: str) -> ScanFunctionResult:
        del schema_path
        return ScanFunctionResult(
            function_name=f"simple_writable_{kind}",
            positional_arguments=[pa.scalar(name)],
            named_arguments={},
        )

    def table_scan_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_path: list[str],
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanFunctionResult:
        del attach_opaque_data, transaction_opaque_data, at_unit, at_value
        return self._function_get("scan", schema_path=schema_path, name=name)

    def table_insert_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_path: list[str],
        name: str,
        writable_branch_function_name: str | None = None,
    ) -> ScanFunctionResult:
        del attach_opaque_data, transaction_opaque_data, writable_branch_function_name
        # Route the broken table to the misbehaving insert function. Tests rely
        # on this lying about RETURNING shape so the C++ runtime validator
        # gets exercised.
        if name.lower() == "items_broken_returning":
            return ScanFunctionResult(
                function_name="simple_writable_broken_returning_insert",
                positional_arguments=[pa.scalar(name)],
                named_arguments={},
            )
        return self._function_get("insert", schema_path=schema_path, name=name)

    def table_update_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_path: list[str],
        name: str,
    ) -> ScanFunctionResult:
        del attach_opaque_data, transaction_opaque_data
        return self._function_get("update", schema_path=schema_path, name=name)

    def table_delete_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_path: list[str],
        name: str,
    ) -> ScanFunctionResult:
        del attach_opaque_data, transaction_opaque_data
        return self._function_get("delete", schema_path=schema_path, name=name)


class SimpleWritableWorker(Worker):
    """Worker exposing :class:`SimpleWritableCatalog`."""

    catalog_interface = SimpleWritableCatalog
    catalog = _CATALOG


def main() -> None:
    """Run the simple writable worker process."""
    SimpleWritableWorker.main()


if __name__ == "__main__":
    main()
