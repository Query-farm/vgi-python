"""Schema-reconcile fixture worker.

Hosted inside the consolidated ``vgi-fixture-worker`` (entry point in
pyproject.toml) alongside the other reproducer catalogs. Used by the
``test/sql/integration/schema_reconcile.test`` regression test in
``~/Development/vgi`` to exercise the C++ ``ReconcileBatchToSchema`` helper
across INSERT, UPDATE, DELETE, and SELECT batch flows.

Three writable tables, each with a different rowid type — covering every
rowid shape that exercises a separate ReconcileBatchToSchema code path:

  - ``demo``        : rowid int64 NOT NULL — primitive integer rowid.
  - ``ts_demo``     : rowid timestamp[ms, tz=UTC] NOT NULL — TZ-aware
                      timestamp as the rowid; exercises the value cast on
                      the rowid itself (DuckDB collapses TIMESTAMP_TZ to
                      timestamp[us, tz=session]).
  - ``struct_demo`` : rowid struct{a int64 NOT NULL, b string nullable} NOT NULL
                      — struct rowid with mixed nullability inside;
                      exercises recursive nullability reshape on a rowid.

User columns (id/ts/nested/tags) are identical across tables.
"""

from __future__ import annotations

import os
import pickle
import sqlite3
import threading
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from vgi_rpc.rpc import OutputCollector

from vgi import Worker
from vgi.catalog import Catalog, Schema
from vgi.catalog.catalog_interface import (
    AttachId,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    SchemaObjectType,
    SerializedSchema,
    TableInfo,
    TransactionId,
)
from vgi.invocation import BindResponse, GlobalInitResponse
from vgi.table_function import (
    BindParams,
    InitParams,
    ProcessParams,
    TableFunctionGenerator,
)
from vgi.table_in_out_function import TableInOutGenerator

CATALOG_NAME = "schema_reconcile"
_SCHEMA_NAME = "main"


# ---------------------------------------------------------------------------
# Declared user-facing columns — identical across all three tables.
# Every facet (NOT NULL primitive, TZ-aware ms timestamp, NOT NULL leaf
# inside a struct, NOT NULL item inside list-of-struct) is something
# DuckDB's Arrow round-trip cannot preserve.
# ---------------------------------------------------------------------------

USER_FIELDS: list[pa.Field[Any]] = [
    pa.field("id", pa.int64(), nullable=False),
    pa.field("ts", pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field(
        "nested",
        pa.struct(
            [
                pa.field("a", pa.int32(), nullable=False),
                pa.field("b", pa.string(), nullable=True),
                pa.field("ts2", pa.timestamp("ms", tz="UTC"), nullable=True),
            ]
        ),
        nullable=False,
    ),
    pa.field(
        "tags",
        pa.list_(
            pa.field(
                "item",
                pa.struct(
                    [
                        pa.field("k", pa.string(), nullable=False),
                        pa.field("v", pa.binary(), nullable=True),
                    ]
                ),
                nullable=False,
            )
        ),
        nullable=False,
    ),
]
USER_SCHEMA: pa.Schema = pa.schema(USER_FIELDS)


# ---------------------------------------------------------------------------
# Per-table specs — each table gets its own rowid type.
# ---------------------------------------------------------------------------


def _rowid_field(arrow_type: pa.DataType) -> pa.Field[Any]:
    """A rowid field with the ``is_row_id`` metadata that the C++ side keys on.

    Always declared NOT NULL to exercise the rowid reshape path in
    ReconcileBatchToSchema.
    """
    return pa.field("rowid", arrow_type, nullable=False, metadata={b"is_row_id": b""})


_INT64_ROWID = _rowid_field(pa.int64())

_TS_ROWID = _rowid_field(pa.timestamp("ms", tz="UTC"))

_STRUCT_ROWID = _rowid_field(
    pa.struct(
        [
            pa.field("a", pa.int64(), nullable=False),
            pa.field("b", pa.string(), nullable=True),
        ]
    )
)


@dataclass(frozen=True)
class TableSpec:
    name: str
    rowid_field: pa.Field[Any]
    storage_table: str  # Underlying SQLite table name.

    @property
    def table_schema(self) -> pa.Schema:
        return pa.schema(USER_FIELDS + [self.rowid_field])

    @property
    def delete_input_schema(self) -> pa.Schema:
        return pa.schema([self.rowid_field])


TABLES: dict[str, TableSpec] = {
    spec.name: spec
    for spec in (
        TableSpec("demo", _INT64_ROWID, "demo_rows"),
        TableSpec("ts_demo", _TS_ROWID, "ts_demo_rows"),
        TableSpec("struct_demo", _STRUCT_ROWID, "struct_demo_rows"),
    )
}


_COUNT_SCHEMA: pa.Schema = pa.schema([pa.field("count", pa.int64(), nullable=False)])


# ---------------------------------------------------------------------------
# Storage — SQLite. Each logical table gets its own row-store SQLite table.
# Rowid is opaque (pickled tuple), so this works for int, timestamp, and
# struct rowids alike. The Arrow schema (the thing under test) is
# reconstructed from TABLES on read.
# ---------------------------------------------------------------------------

_lock = threading.Lock()


def _db_path() -> str:
    return os.environ.get("VGI_SCHEMA_RECONCILE_DB", "/tmp/vgi_schema_reconcile.sqlite")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path(), timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    for spec in TABLES.values():
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {spec.storage_table} (  rid_blob BLOB PRIMARY KEY,  payload BLOB NOT NULL)"
        )
    return conn


def _rid_key(rid: Any) -> bytes:
    return pickle.dumps(rid)


def _all_rows(spec: TableSpec) -> list[tuple[Any, dict[str, Any]]]:
    with _lock, _connect() as conn:
        out: list[tuple[Any, dict[str, Any]]] = []
        for rid_blob, payload in conn.execute(f"SELECT rid_blob, payload FROM {spec.storage_table}"):
            out.append((pickle.loads(rid_blob), pickle.loads(payload)))
        return out


def _insert_row(spec: TableSpec, rid: Any, payload: dict[str, Any]) -> None:
    with _lock, _connect() as conn:
        conn.execute(
            f"INSERT OR REPLACE INTO {spec.storage_table} (rid_blob, payload) VALUES (?, ?)",
            (_rid_key(rid), pickle.dumps(payload)),
        )


def _update_row(spec: TableSpec, rid: Any, updates: dict[str, Any]) -> bool:
    with _lock, _connect() as conn:
        row = conn.execute(
            f"SELECT payload FROM {spec.storage_table} WHERE rid_blob = ?",
            (_rid_key(rid),),
        ).fetchone()
        if row is None:
            return False
        payload = pickle.loads(row[0])
        payload.update(updates)
        conn.execute(
            f"UPDATE {spec.storage_table} SET payload = ? WHERE rid_blob = ?",
            (pickle.dumps(payload), _rid_key(rid)),
        )
        return True


def _delete_row(spec: TableSpec, rid: Any) -> bool:
    with _lock, _connect() as conn:
        cur = conn.execute(
            f"DELETE FROM {spec.storage_table} WHERE rid_blob = ?",
            (_rid_key(rid),),
        )
        return cur.rowcount > 0


def _next_int_rowid(spec: TableSpec) -> int:
    """For the int64-rowid table, autoincrement-ish."""
    with _lock, _connect() as conn:
        max_row = conn.execute(f"SELECT payload FROM {spec.storage_table}").fetchall()
        # The int rowid is stored in the payload as ``__rid__`` for convenience
        # of monotonic generation.
        existing = [pickle.loads(p[0]).get("__rid__", 0) for p in max_row]
        return (max(existing) + 1) if existing else 1


def _reset_storage() -> None:
    """Test hook — drop the on-disk store."""
    p = _db_path()
    for ext in ("", "-wal", "-shm"):
        if os.path.exists(p + ext):
            os.unlink(p + ext)


# ---------------------------------------------------------------------------
# Strict schema verifier
# ---------------------------------------------------------------------------


def _strict_assert_schema(label: str, actual: pa.Schema, expected: pa.Schema) -> None:
    """Hard-fail if ``actual`` doesn't bit-for-bit equal ``expected``.

    The vgi C++ ``ReconcileBatchToSchema`` helper is what makes these
    schemas equal — DuckDB on its own emits batches with all-nullable
    fields, ``timestamp[us, tz=session]`` for TZ timestamps, and so on.
    A mismatch here means reconciliation regressed.
    """
    if actual.equals(expected, check_metadata=False):
        return

    detail = []
    if len(actual) != len(expected):
        detail.append(f"field count: actual={len(actual)} expected={len(expected)}")
    for i in range(min(len(actual), len(expected))):
        af = actual.field(i)
        ef = expected.field(i)
        if af.name != ef.name:
            detail.append(f"field[{i}].name: actual={af.name!r} expected={ef.name!r}")
        if af.nullable != ef.nullable:
            detail.append(f"field[{i}={af.name!r}].nullable: actual={af.nullable} expected={ef.nullable}")
        if not af.type.equals(ef.type):
            detail.append(f"field[{i}={af.name!r}].type: actual={af.type} expected={ef.type}")
    raise ValueError(
        f"[schema_reconcile] {label} batch schema mismatch (reconciliation regression?):\n"
        + "\n".join(f"  - {d}" for d in detail)
        + f"\n  actual:   {actual}\n  expected: {expected}"
    )


# ---------------------------------------------------------------------------
# Handler functions
# ---------------------------------------------------------------------------


def _emit_count(out: OutputCollector, n: int) -> None:
    out.emit(pa.RecordBatch.from_pydict({"count": [n]}, schema=_COUNT_SCHEMA))


def _spec_from_args(positional: tuple[Any, ...]) -> TableSpec:
    if not positional or positional[0] is None:
        raise ValueError("schema_reconcile handler: missing table_name positional[0]")
    name = str(positional[0].as_py())
    spec = TABLES.get(name)
    if spec is None:
        raise ValueError(f"schema_reconcile handler: unknown table {name!r}")
    return spec


def _row_to_dict(batch: pa.RecordBatch, i: int, fields: list[str]) -> dict[str, Any]:
    return {name: batch.column(name)[i].as_py() for name in fields}


def _generate_rowid(spec: TableSpec, payload: dict[str, Any]) -> Any:
    """Synthesize a rowid for INSERT (no rowid column on input)."""
    if spec.rowid_field.type.equals(pa.int64()):
        rid = _next_int_rowid(spec)
        payload["__rid__"] = rid
        return rid
    if isinstance(spec.rowid_field.type, pa.TimestampType):
        # Use the row's `ts` column as a rowid — guaranteed unique enough
        # for tests since tests insert distinct timestamps. Stored as the
        # Python ``datetime`` value the user inserted.
        return payload["ts"]
    if isinstance(spec.rowid_field.type, pa.StructType):
        # Project ``id`` -> a (NOT NULL int64) and ``nested.b`` -> b (nullable string).
        return {"a": payload["id"], "b": payload["nested"].get("b")}
    raise ValueError(f"unhandled rowid type: {spec.rowid_field.type}")


class SchemaReconcileInsert(TableInOutGenerator[None, None]):
    """INSERT handler — asserts the input batch matches USER_SCHEMA exactly."""

    class Meta:
        name = "schema_reconcile_insert"
        description = "INSERT handler for the schema_reconcile fixture"

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[None],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        assert params.init_call is not None
        spec = _spec_from_args(params.init_call.bind_call.arguments.positional)
        _strict_assert_schema(f"INSERT[{spec.name}]", batch.schema, USER_SCHEMA)
        names = [f.name for f in USER_SCHEMA]
        for i in range(batch.num_rows):
            payload = _row_to_dict(batch, i, names)
            rid = _generate_rowid(spec, payload)
            _insert_row(spec, rid, payload)
        _emit_count(out, batch.num_rows)


class SchemaReconcileUpdate(TableInOutGenerator[None, None]):
    """UPDATE handler — asserts batch is rowid + selected user columns,
    every field with the worker-declared flags/types intact.
    """

    class Meta:
        name = "schema_reconcile_update"
        description = "UPDATE handler for the schema_reconcile fixture"

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[None],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        assert params.init_call is not None
        spec = _spec_from_args(params.init_call.bind_call.arguments.positional)
        cols = batch.schema.names
        if "rowid" not in cols:
            raise ValueError(f"[schema_reconcile] UPDATE[{spec.name}] missing rowid column; got: {cols}")
        full = spec.table_schema
        for f in batch.schema:
            expected = full.field(full.get_field_index(f.name))
            if f.nullable != expected.nullable or not f.type.equals(expected.type):
                raise ValueError(
                    f"[schema_reconcile] UPDATE[{spec.name}] field {f.name!r} mismatch "
                    f"(reconciliation regression?): "
                    f"actual=({f.type}, nullable={f.nullable}) "
                    f"expected=({expected.type}, nullable={expected.nullable})"
                )

        update_cols = [c for c in cols if c != "rowid"]
        n = 0
        for i in range(batch.num_rows):
            rid = batch.column("rowid")[i].as_py()
            updates = {c: batch.column(c)[i].as_py() for c in update_cols}
            if _update_row(spec, rid, updates):
                n += 1
        _emit_count(out, n)


class SchemaReconcileDelete(TableInOutGenerator[None, None]):
    """DELETE handler — asserts batch is rowid-only with declared flag/type."""

    class Meta:
        name = "schema_reconcile_delete"
        description = "DELETE handler for the schema_reconcile fixture"

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[None],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        assert params.init_call is not None
        spec = _spec_from_args(params.init_call.bind_call.arguments.positional)
        _strict_assert_schema(f"DELETE[{spec.name}]", batch.schema, spec.delete_input_schema)
        n = 0
        for i in range(batch.num_rows):
            rid = batch.column("rowid")[i].as_py()
            if _delete_row(spec, rid):
                n += 1
        _emit_count(out, n)


class SchemaReconcileScan(TableFunctionGenerator[None, None]):
    """SELECT handler — emits the table's stored rows in its declared schema."""

    class Meta:
        name = "schema_reconcile_scan"
        description = "SCAN handler for the schema_reconcile fixture"
        projection_pushdown = True

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        spec = _spec_from_args(params.bind_call.arguments.positional)
        return BindResponse(output_schema=spec.table_schema)

    @classmethod
    def on_init(cls, params: InitParams[None]) -> GlobalInitResponse:
        return GlobalInitResponse(max_workers=1)

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, out: OutputCollector) -> None:
        assert params.init_call is not None
        spec = _spec_from_args(params.init_call.bind_call.arguments.positional)
        out_schema = params.output_schema
        rows: list[dict[str, Any]] = []
        for rid, payload in _all_rows(spec):
            full = {**payload, "rowid": rid}
            # Don't emit the bookkeeping ``__rid__`` column.
            full.pop("__rid__", None)
            rows.append({name: full[name] for name in out_schema.names})
        out.emit(pa.RecordBatch.from_pylist(rows, schema=out_schema))
        out.finish()


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------


def _serialize_schema(schema: pa.Schema) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema):
        pass
    return sink.getvalue().to_pybytes()


_FUNCTIONS = [
    SchemaReconcileInsert,
    SchemaReconcileUpdate,
    SchemaReconcileDelete,
    SchemaReconcileScan,
]


_CATALOG = Catalog(
    name=CATALOG_NAME,
    default_schema=_SCHEMA_NAME,
    schemas=[
        Schema(
            name=_SCHEMA_NAME,
            comment="Schema-reconcile fixture catalog",
            functions=list(_FUNCTIONS),
            tables=[],
        ),
    ],
)


class SchemaReconcileCatalog(ReadOnlyCatalogInterface):
    """Catalog exposing the three writable schema-reconcile tables."""

    catalog = _CATALOG
    catalog_name = CATALOG_NAME

    def _table_info(self, spec: TableSpec) -> TableInfo:
        return TableInfo(
            comment=f"Schema-reconcile {spec.name} (rowid type {spec.rowid_field.type})",
            tags={},
            name=spec.name,
            schema_name=_SCHEMA_NAME,
            columns=SerializedSchema(_serialize_schema(spec.table_schema)),
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
            supports_insert=True,
            supports_update=True,
            supports_delete=True,
        )

    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: Any,
    ) -> Any:
        if name.lower() == _SCHEMA_NAME and type == SchemaObjectType.TABLE:
            return [self._table_info(spec) for spec in TABLES.values()]
        return super().schema_contents(attach_id=attach_id, transaction_id=transaction_id, name=name, type=type)

    def table_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        if schema_name.lower() != _SCHEMA_NAME:
            return None
        spec = TABLES.get(name.lower())
        return self._table_info(spec) if spec else None

    def _route(self, fn_name: str, schema_name: str, name: str) -> ScanFunctionResult:
        return ScanFunctionResult(
            function_name=fn_name,
            positional_arguments=[pa.scalar(name, type=pa.string())],
            named_arguments={},
            required_extensions=[],
        )

    def table_scan_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanFunctionResult:
        return self._route("schema_reconcile_scan", schema_name, name)

    def table_insert_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ScanFunctionResult:
        return self._route("schema_reconcile_insert", schema_name, name)

    def table_update_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ScanFunctionResult:
        return self._route("schema_reconcile_update", schema_name, name)

    def table_delete_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ScanFunctionResult:
        return self._route("schema_reconcile_delete", schema_name, name)


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------


class SchemaReconcileWorker(Worker):
    """Worker exposing the schema-reconcile fixture catalog."""

    catalog_interface = SchemaReconcileCatalog
    catalog_name = CATALOG_NAME
    catalog = _CATALOG
    functions = list(_FUNCTIONS)
