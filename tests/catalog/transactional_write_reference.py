# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D103, D106
"""Single-process SQLite authority for native transactional INSERT/CTAS tests.

Set VGI_TRANSACTIONAL_WRITE_DIR to an isolated absolute directory and run
``python -m tests.catalog.transactional_write_reference --port 0``. The launcher
prints ``PORT:<actual>`` and serves signed HTTP on loopback. Its SQLite database
starts with ``items(id BIGINT, label VARCHAR)`` containing ``(-1, 'baseline')``.

control.json selects one subsequent write: ``{"phase": "input", "table":
"fail_target", "after_batches": 2}``. Supported phases are init/input/finalize;
the optional table selector leaves other writes unaffected. A matching control
is consumed at execution init, so later control changes cannot alter an earlier
write. INPUT errors happen *after* inserting the selected batch into SQLite.
Neither an injected error nor stream cancellation rolls the transaction back:
the native client must invoke the catalog rollback method. Independent readers
of data.sqlite therefore observe actual committed state, not diagnostic files.

This deliberately serializes all access in one HTTP worker process. It claims
no multi-process coordination, retries, crash recovery or concurrent isolation.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import threading
import uuid
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, ClassVar
from wsgiref.simple_server import make_server

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.simple_writable import _parse_write_options
from vgi.catalog import (
    AttachOpaqueData,
    Catalog,
    CatalogAttachResult,
    OnConflict,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    Schema,
    SchemaInfo,
    SchemaObjectType,
    TableInfo,
    TransactionOpaqueData,
)
from vgi.invocation import BindResponse, GlobalInitResponse
from vgi.table_function import (
    BindParams,
    InitParams,
    ProcessParams,
    TableFunctionGenerator,
    TableInOutFunctionInitPhase,
)
from vgi.table_in_out_function import TableInOutGenerator
from vgi.worker import Worker
from vgi.write_results import write_result_schema

_ROWS = pa.schema([("id", pa.int64()), ("label", pa.string())])
_COUNT = write_result_schema("count", _ROWS)
_PREFIX = "transactional_write_reference:"


def _error(message: str) -> RuntimeError:
    return RuntimeError(f"{_PREFIX} {message}")


def _root() -> Path:
    value = os.environ.get("VGI_TRANSACTIONAL_WRITE_DIR")
    if not value or not Path(value).is_absolute():
        raise _error("VGI_TRANSACTIONAL_WRITE_DIR must name an isolated absolute directory")
    root = Path(value)
    root.mkdir(parents=True, exist_ok=True)
    return root


def _identifier(name: str) -> str:
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) is None:
        raise _error("this test provider accepts simple table names only")
    return f'"{name}"'


def _table_argument(params: BindParams[None] | InitParams[None] | ProcessParams[None]) -> str:
    bind = params.bind_call if isinstance(params, BindParams) else params.init_call.bind_call
    arguments = bind.arguments.positional
    if len(arguments) != 1 or arguments[0] is None:
        raise _error("one table-name argument is required")
    name = arguments[0].as_py()
    if not isinstance(name, str):
        raise _error("table name must be a string")
    _identifier(name)
    return name


def _execution_id(params: ProcessParams[None]) -> bytes:
    if params.init_response is None:
        raise _error("write callback has no execution id")
    return params.init_response.execution_id


@dataclass
class _Transaction:
    attach: bytes
    connection: sqlite3.Connection


@dataclass
class _Execution:
    attach: bytes
    token: bytes
    table: str
    phase: str | None
    after_batches: int
    batches: int = 0
    rows: int = 0


class _Authority:
    """One process owns all SQLite connections; the lock serializes callbacks."""

    lock: ClassVar[Any] = threading.RLock()
    root: ClassVar[Path | None] = None
    attachments: ClassVar[set[bytes]] = set()
    transactions: ClassVar[dict[bytes, _Transaction]] = {}
    executions: ClassVar[dict[bytes, _Execution]] = {}

    @classmethod
    def initialize(cls) -> None:
        with cls.lock:
            root = _root()
            if cls.root is not None:
                if cls.root != root:
                    raise _error("storage directory cannot change within this worker process")
                return
            cls.root = root
            with closing(cls.connect()) as db:
                db.execute("PRAGMA journal_mode=WAL")
                if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='items'").fetchone() is None:
                    db.execute("BEGIN")
                    db.execute('CREATE TABLE "items" (id BIGINT, label VARCHAR)')
                    db.execute('INSERT INTO "items" VALUES (?, ?)', (-1, "baseline"))
                    db.execute("COMMIT")

    @classmethod
    def connect(cls) -> sqlite3.Connection:
        assert cls.root is not None
        return sqlite3.connect(cls.root / "data.sqlite", isolation_level=None, check_same_thread=False, timeout=10)

    @classmethod
    def event(
        cls,
        event: str,
        *,
        attach: bytes,
        token: bytes | None = None,
        execution_id: bytes | None = None,
        phase: str = "completed",
        **details: Any,
    ) -> None:
        assert cls.root is not None
        with (cls.root / "events.jsonl").open("a", encoding="utf-8") as target:
            target.write(
                json.dumps(
                    {
                        "event": event,
                        "phase": phase,
                        "attach": attach.hex(),
                        "token": token.hex() if token is not None else None,
                        "execution_id": execution_id.hex() if execution_id is not None else None,
                        "pid": os.getpid(),
                        "active_transactions": len(cls.transactions),
                        **details,
                    }
                )
                + "\n"
            )

    @classmethod
    def transaction(cls, attach: bytes, token: bytes | None) -> _Transaction:
        if attach not in cls.attachments:
            raise _error("unknown attachment")
        transaction = cls.transactions.get(token or b"")
        if transaction is None or transaction.attach != attach:
            raise _error("transaction is inactive or belongs to another attachment")
        return transaction

    @classmethod
    @contextmanager
    def connection(cls, attach: bytes, token: bytes | None) -> Iterator[sqlite3.Connection]:
        with cls.lock:
            if attach not in cls.attachments:
                raise _error("unknown attachment")
            try:
                if token is not None:
                    yield cls.transaction(attach, token).connection
                else:
                    with closing(cls.connect()) as db:
                        yield db
            except sqlite3.Error as exc:
                # Do not rollback here: a native rollback RPC must undo the real changes.
                raise _error(str(exc)) from exc

    @classmethod
    def control(cls, table: str) -> tuple[str | None, int]:
        assert cls.root is not None
        path = cls.root / "control.json"
        data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        if not data or data.get("table", table).casefold() != table.casefold():
            return None, 2
        phase = data.get("phase")
        after_batches = data.get("after_batches", 2)
        if phase not in {"init", "input", "finalize"}:
            raise _error("control phase must be init, input or finalize")
        if not isinstance(after_batches, int) or isinstance(after_batches, bool) or after_batches < 1:
            raise _error("after_batches must be a positive integer")
        temporary = path.with_name(f".control-{uuid.uuid4().hex}.tmp")
        temporary.write_text("{}", encoding="utf-8")
        temporary.replace(path)
        return phase, after_batches

    @classmethod
    def execution(cls, params: ProcessParams[None]) -> _Execution:
        execution = cls.executions.get(_execution_id(params))
        if execution is None:
            raise _error("execution was not initialized")
        if execution.attach != params.attach_opaque_data or execution.token != params.transaction_opaque_data:
            raise _error("execution belongs to another attachment or transaction")
        cls.transaction(execution.attach, execution.token)
        return execution

    @classmethod
    def failure(cls, execution_id: bytes, execution: _Execution, phase: str) -> None:
        cls.event(
            "failure",
            attach=execution.attach,
            token=execution.token,
            execution_id=execution_id,
            phase=phase,
            table=execution.table,
            batch_index=execution.batches,
            rows=execution.rows,
            stored_rows=execution.rows,
        )
        raise _error(f"{phase} injected failure after {execution.batches} batches and {execution.rows} stored rows")


@dataclass(kw_only=True)
class _ScanState(ArrowSerializableDataclass):
    offset: int = 0


class TransactionalScan(TableFunctionGenerator[None, _ScanState]):
    """Read the real table from the transaction's connection."""

    class Meta:
        name = "transactional_write_scan"
        max_workers = 1
        projection_pushdown = False
        filter_pushdown = False

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        _table_argument(params)
        return BindResponse(output_schema=_ROWS)

    @classmethod
    def on_init(cls, params: InitParams[None]) -> GlobalInitResponse:
        del params
        return GlobalInitResponse(max_workers=1)

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> _ScanState:
        del params
        return _ScanState()

    @classmethod
    def process(cls, params: ProcessParams[None], state: _ScanState, out: OutputCollector) -> None:
        table = _table_argument(params)
        attach = bytes(params.attach_opaque_data or b"")
        token = params.transaction_opaque_data
        with _Authority.connection(attach, token) as db:
            rows = db.execute(
                f"SELECT id, label FROM {_identifier(table)} ORDER BY rowid LIMIT 2048 OFFSET ?", (state.offset,)
            ).fetchall()
            _Authority.event("scan", attach=attach, token=token, table=table, rows=len(rows))
        if not rows:
            out.finish()
            return
        state.offset += len(rows)
        out.emit(
            pa.RecordBatch.from_pydict(
                {"id": [row[0] for row in rows], "label": [row[1] for row in rows]}, schema=_ROWS
            )
        )


class TransactionalInsert(TableInOutGenerator[None, None]):
    """Insert into the open SQLite transaction without committing or rolling back."""

    class Meta:
        name = "transactional_write_insert"
        max_workers = 1

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        _table_argument(params)
        options = _parse_write_options(params)
        if options["result_mode"] not in {"count", "rows"} or options["on_conflict"] != "throw":
            raise _error("only count/rows INSERT with ordinary conflict behavior is supported")
        return BindResponse(output_schema=write_result_schema(options["result_mode"], _ROWS))

    @classmethod
    def on_init(cls, params: InitParams[None]) -> GlobalInitResponse:
        table = _table_argument(params)
        attach = bytes(params.attach_opaque_data or b"")
        token = params.transaction_opaque_data
        with _Authority.lock:
            _Authority.transaction(attach, token)
            assert token is not None
            execution = _Authority.executions.get(params.execution_id)
            if execution is None:
                phase, after_batches = _Authority.control(table)
                execution = _Execution(attach, token, table, phase, after_batches)
                _Authority.executions[params.execution_id] = execution
            elif (execution.attach, execution.token, execution.table) != (attach, token, table):
                raise _error("execution id was reused for another write")
            finalize = params.init_call.phase == TableInOutFunctionInitPhase.FINALIZE
            _Authority.event(
                "init",
                attach=attach,
                token=token,
                execution_id=params.execution_id,
                phase="finalize" if finalize else "input",
                table=table,
            )
            if execution.phase == "init" and not finalize:
                _Authority.failure(params.execution_id, execution, "init")
        return GlobalInitResponse(max_workers=1)

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, batch: pa.RecordBatch, out: OutputCollector) -> None:
        del state
        with _Authority.lock:
            execution = _Authority.execution(params)
            rows = list(zip(batch.column("id").to_pylist(), batch.column("label").to_pylist(), strict=True))
            with _Authority.connection(execution.attach, execution.token) as db:
                db.executemany(f"INSERT INTO {_identifier(execution.table)} (id, label) VALUES (?, ?)", rows)
            execution.batches += 1
            execution.rows += len(rows)
            _Authority.event(
                "input",
                attach=execution.attach,
                token=execution.token,
                execution_id=_execution_id(params),
                table=execution.table,
                batch_index=execution.batches,
                batch_rows=len(rows),
                rows=len(rows),
                stored_rows=execution.rows,
            )
            if execution.phase == "input" and execution.batches >= execution.after_batches:
                _Authority.failure(_execution_id(params), execution, "input")
        if _parse_write_options(params)["result_mode"] == "rows":
            out.emit(
                pa.RecordBatch.from_pydict(
                    {"id": [row[0] for row in rows], "label": [row[1] for row in rows]}, schema=_ROWS
                )
            )
        else:
            out.emit(pa.RecordBatch.from_pydict({"count": [len(rows)]}, schema=_COUNT))

    @classmethod
    def finalize(cls, params: ProcessParams[None]) -> list[pa.RecordBatch]:
        with _Authority.lock:
            execution = _Authority.execution(params)
            _Authority.event(
                "finalize",
                attach=execution.attach,
                token=execution.token,
                execution_id=_execution_id(params),
                table=execution.table,
                batches=execution.batches,
                rows=execution.rows,
            )
            if execution.phase == "finalize":
                _Authority.failure(_execution_id(params), execution, "finalize")
        if _parse_write_options(params)["result_mode"] == "rows":
            return []
        return [pa.RecordBatch.from_pydict({"count": [0]}, schema=_COUNT)]


CATALOG = Catalog(
    name="transactional_writes",
    default_schema="main",
    schemas=[Schema(path=["main"], functions=[TransactionalScan, TransactionalInsert], tables=[])],
)


class TransactionalWriteCatalog(ReadOnlyCatalogInterface):
    """Dynamic SQLite tables with genuine transactional DDL and row changes."""

    catalog = CATALOG
    supports_transactions = True
    catalog_version_frozen = False
    catalog_contents_attach_independent = False

    def catalog_attach(self, **kwargs: Any) -> CatalogAttachResult:
        _Authority.initialize()
        result = super().catalog_attach(**kwargs)
        attach = AttachOpaqueData(uuid.uuid4().bytes)
        with _Authority.lock:
            _Authority.attachments.add(attach)
            _Authority.event("attach", attach=attach)
        return replace(
            result,
            attach_opaque_data=attach,
            attach_opaque_data_required=True,
            supports_transactions=True,
            supports_catalog_contents=False,
            catalog_version_frozen=False,
            catalog_version=self.catalog_version(attach_opaque_data=attach, transaction_opaque_data=None),
        )

    def catalog_version(
        self, *, attach_opaque_data: bytes, transaction_opaque_data: bytes | None = None, **kwargs: Any
    ) -> int:
        with _Authority.connection(attach_opaque_data, transaction_opaque_data) as db:
            return int(db.execute("PRAGMA schema_version").fetchone()[0])

    def catalog_transaction_begin(self, *, attach_opaque_data: bytes) -> TransactionOpaqueData:
        with _Authority.lock:
            if attach_opaque_data not in _Authority.attachments:
                raise _error("unknown attachment")
            token = TransactionOpaqueData(uuid.uuid4().bytes)
            db = _Authority.connect()
            db.execute("BEGIN")
            _Authority.transactions[token] = _Transaction(attach_opaque_data, db)
            _Authority.event("begin", attach=attach_opaque_data, token=token)
            return token

    def _finish(self, operation: str, attach: bytes, token: bytes) -> None:
        with _Authority.lock:
            transaction = _Authority.transaction(attach, token)
            _Authority.event("finish_attempt", attach=attach, token=token, phase=operation)
            try:
                transaction.connection.execute(operation.upper())
            except sqlite3.Error as exc:
                raise _error(str(exc)) from exc
            transaction.connection.close()
            del _Authority.transactions[token]
            for execution_id in [key for key, value in _Authority.executions.items() if value.token == token]:
                del _Authority.executions[execution_id]
            _Authority.event(operation, attach=attach, token=token)

    def catalog_transaction_commit(self, *, attach_opaque_data: bytes, transaction_opaque_data: bytes) -> None:
        self._finish("commit", attach_opaque_data, transaction_opaque_data)

    def catalog_transaction_rollback(self, *, attach_opaque_data: bytes, transaction_opaque_data: bytes) -> None:
        self._finish("rollback", attach_opaque_data, transaction_opaque_data)

    def schemas(self, *, attach_opaque_data: bytes, transaction_opaque_data: bytes | None) -> list[SchemaInfo]:
        self.catalog_version(attach_opaque_data=attach_opaque_data, transaction_opaque_data=transaction_opaque_data)
        return [
            SchemaInfo(attach_opaque_data=AttachOpaqueData(attach_opaque_data), path=["main"], comment=None, tags={})
        ]

    def schema_get(self, *, path: list[str], **kwargs: Any) -> SchemaInfo | None:
        return self.schemas(**kwargs)[0] if path == ["main"] else None

    @staticmethod
    def _info(name: str) -> TableInfo:
        return TableInfo(
            name=name,
            schema_path=["main"],
            columns=_ROWS.serialize().to_pybytes(),
            constraints=[],
            comment=None,
            tags={},
            write_result_modes={"insert": "rows"},
        )

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
            raise _error("time travel is unsupported")
        if schema_path != ["main"]:
            return None
        with _Authority.connection(attach_opaque_data, transaction_opaque_data) as db:
            row = db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=? COLLATE NOCASE", (name,)
            ).fetchone()
            return self._info(row[0]) if row else None

    def schema_contents(
        self,
        *,
        attach_opaque_data: bytes,
        transaction_opaque_data: bytes | None,
        path: list[str],
        type: SchemaObjectType,
    ) -> Any:
        if path != ["main"]:
            return []
        if type == SchemaObjectType.TABLE:
            with _Authority.connection(attach_opaque_data, transaction_opaque_data) as db:
                return [
                    self._info(row[0])
                    for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                ]
        return super().schema_contents(
            attach_opaque_data=AttachOpaqueData(attach_opaque_data),
            transaction_opaque_data=TransactionOpaqueData(transaction_opaque_data) if transaction_opaque_data else None,
            path=path,
            type=type,
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
        schema = pa.ipc.read_schema(pa.BufferReader(columns))
        if (
            schema_path != ["main"]
            or on_conflict != OnConflict.ERROR
            or constraint_definitions
            or any(value is not None for value in kwargs.values())
            or not schema.equals(_ROWS, check_metadata=False)
        ):
            raise _error("CREATE requires unconstrained nullable (id BIGINT, label VARCHAR) without options")
        with _Authority.lock:
            _Authority.transaction(attach_opaque_data, transaction_opaque_data)
            with _Authority.connection(attach_opaque_data, transaction_opaque_data) as db:
                db.execute(f"CREATE TABLE {_identifier(name)} (id BIGINT, label VARCHAR)")
            _Authority.event("table_create", attach=attach_opaque_data, token=transaction_opaque_data, table=name)

    def _function(self, kind: str, **kwargs: Any) -> ScanFunctionResult:
        kwargs.pop("writable_branch_function_name", None)
        info = self.table_get(**kwargs)
        if info is None:
            raise _error("unknown table")
        return ScanFunctionResult(
            function_name=f"transactional_write_{kind}",
            schema_path=["main"],
            positional_arguments=[pa.scalar(info.name)],
            named_arguments={},
        )

    def table_scan_function_get(self, **kwargs: Any) -> ScanFunctionResult:
        return self._function("scan", **kwargs)

    def table_insert_function_get(self, **kwargs: Any) -> ScanFunctionResult:
        return self._function("insert", **kwargs)


class TransactionalWriteWorker(Worker):
    """Serve the test authority through the unchanged public protocol."""

    catalog = CATALOG
    catalog_interface = TransactionalWriteCatalog


def create_reference_app() -> Any:
    """Build a signed WSGI application intended for exactly one worker process."""
    root = _root()
    os.environ["VGI_WORKER_SHARED_STORAGE"] = "sqlite"
    os.environ["VGI_WORKER_SQLITE_PATH"] = str(root / "framework.sqlite")
    _Authority.initialize()
    from vgi.serve import create_app

    return create_app(TransactionalWriteWorker, signing_key=bytes(range(32)))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    with make_server("127.0.0.1", args.port, create_reference_app()) as server:
        print(f"PORT:{server.server_port}", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
