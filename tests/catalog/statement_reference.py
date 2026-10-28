# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D103, D107
"""Finite transactional statement example over the existing query executor.

Run with an isolated VGI_STATEMENT_REFERENCE_DIR and ``python -m
tests.catalog.statement_reference --port 0``. One process owns all DuckDB
connections and receipts; this is NOT a portable multi-process provider, SQL
sandbox, retry service or recovery ledger. A process loss loses these receipts.
Only the exact SQL constants below are supported. There is no native DuckDB
write integration, general DDL/DML parser, external effect or transaction SQL.

Prepare/bind describe known statements without executing them. Start executes
once on the transaction's real connection and materializes bounded fragments.
EOS completes a statement, not its transaction. Pending, cancelled, failed or
expired executions prevent commit; the caller must roll back the whole
transaction. Normal transaction end/detach/expiry releases all owned resources.
"""

from __future__ import annotations

import argparse
import os
import threading
import time
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, ClassVar, Literal, overload
from wsgiref.simple_server import make_server

import duckdb
import pyarrow as pa
from vgi_rpc.rpc import CallContext

from vgi.catalog import (
    AttachOpaqueData,
    CatalogAttachResult,
    FunctionInfo,
    IndexInfo,
    MacroInfo,
    ReadOnlyCatalogInterface,
    SchemaInfo,
    SchemaObjectType,
    SerializedSchema,
    TableInfo,
    TransactionOpaqueData,
    ViewInfo,
)
from vgi.catalog.query import (
    QUERY_ENCODING,
    QUERY_EXPLICIT,
    QUERY_STATEMENT,
    STATEMENT_RESULT_KEY,
    CatalogQueryPrepareRequest,
    CatalogQueryPrepareResult,
    ClientQueryCapabilities,
    QueryCapabilities,
    QueryEvaluationContext,
)
from vgi.protocol import ClientCapabilities
from vgi.query_parameters import PARAMETER_MODE_KEY, QueryParameters, read_parameter_schema
from vgi.query_provider import CatalogQueryProvider, QueryExecution, QueryOwner, QueryPreparation
from vgi.schema_path import SchemaPath
from vgi.worker import Worker

from .typing_helpers import present

INSERT = "INSERT INTO items VALUES ($id, $label)"
INSERT_RETURNING = INSERT + " RETURNING id, label"
UPDATE = "UPDATE items SET label = $label WHERE id = $id"
UPDATE_RETURNING = "UPDATE items SET label = $label RETURNING id, label"
DELETE = "DELETE FROM items WHERE id = $id"
CREATE = "CREATE TABLE added (value BIGINT)"
DROP = "DROP TABLE added"
SELECT = "SELECT id, label FROM items ORDER BY id"
BASELINE = [(0, "zero"), (1, "one"), (2, "two")]
ROW_FIELDS: list[pa.Field[Any]] = [pa.field("id", pa.int64()), pa.field("label", pa.string())]
ROWS = pa.schema(ROW_FIELDS)
COMMAND = pa.schema([("affected_rows", pa.int64())], metadata={STATEMENT_RESULT_KEY: b"command"})
RETURNING = ROWS.with_metadata({STATEMENT_RESULT_KEY: b"rows"})
TYPES = {pa.int64(): duckdb.sqltypes.BIGINT, pa.string(): duckdb.sqltypes.VARCHAR}
REVISION = "finite-statement-reference-1"
SIGNING_KEY = b"statement-reference-test-key"
OFFER = ClientQueryCapabilities(
    encodings=[QUERY_ENCODING], semantic_profiles=[QUERY_EXPLICIT, QUERY_STATEMENT], engine_version="fixture"
)


@dataclass(frozen=True)
class _Statement:
    parameters: tuple[tuple[str, pa.DataType], ...]
    output: pa.Schema
    ddl: bool = False
    read_only: bool = False


STATEMENTS = {
    INSERT: _Statement((("id", pa.int64()), ("label", pa.string())), COMMAND),
    INSERT_RETURNING: _Statement((("id", pa.int64()), ("label", pa.string())), RETURNING),
    UPDATE: _Statement((("id", pa.int64()), ("label", pa.string())), COMMAND),
    UPDATE_RETURNING: _Statement((("label", pa.string()),), RETURNING),
    DELETE: _Statement((("id", pa.int64()),), COMMAND),
    CREATE: _Statement((), COMMAND, ddl=True),
    DROP: _Statement((), COMMAND, ddl=True),
    SELECT: _Statement((), ROWS, read_only=True),
}


def now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass
class _Transaction:
    attach: bytes
    connection: duckdb.DuckDBPyConnection
    expires: int
    failed: bool = False
    pending: set[bytes] = field(default_factory=set)
    started: set[bytes] = field(default_factory=set)


@dataclass
class _Result:
    scope: bytes
    transaction: bytes
    batches: list[pa.RecordBatch]
    expires: int
    fail_read_at: int | None
    eof: bool = False


@dataclass(frozen=True)
class _Completion:
    attach: bytes
    operation: str
    expires: int


class StatementAuthority:
    """A bounded, locked owner of real DuckDB transactions in one process."""

    lifetime_ms = 120_000
    max_resources = 64
    max_result_rows = 10_000
    max_result_bytes = 1024 * 1024

    def __init__(self, path: Path) -> None:
        self.lock = threading.RLock()
        self.connection = duckdb.connect(str(path), config={"enable_external_access": False, "threads": 1})
        self.connection.execute("CREATE TABLE IF NOT EXISTS _statement_version (version BIGINT)")
        row = self.connection.execute("SELECT count(*) FROM _statement_version").fetchone()
        assert row is not None
        if row[0] == 0:
            self.connection.execute("INSERT INTO _statement_version VALUES (1)")
            self.connection.execute("CREATE TABLE items (id BIGINT, label VARCHAR)")
            self.connection.executemany("INSERT INTO items VALUES (?, ?)", BASELINE)
        self.attachments: set[bytes] = set()
        self.transactions: dict[bytes, _Transaction] = {}
        # In-process idempotency only, bounded together with active transactions.
        # No receipt survives process loss, detach or this fixture's finite lease.
        self.completed: dict[bytes, _Completion] = {}
        self.preparations: dict[bytes, tuple[bytes, bytes, QueryPreparation]] = {}
        self.executions: dict[bytes, _Result] = {}
        self.starts = 0
        # Test-only fault is copied into the next execution, never looked up on replay.
        self.fail_read_at: int | None = None

    def authorize(self, attach: bytes) -> None:
        if attach not in self.attachments:
            raise ValueError("statement attachment is closed")

    def transaction(self, attach: bytes, token: bytes | None, *, allow_failed: bool = False) -> _Transaction:
        self.authorize(attach)
        tx = None if token is None else self.transactions.get(token)
        if tx is None or tx.attach != attach:
            raise ValueError("statement transaction is inactive or belongs to another attachment")
        if tx.expires <= now_ms():
            tx.failed = True
        if tx.failed and not allow_failed:
            raise ValueError("statement transaction failed; rollback required")
        return tx

    @contextmanager
    def reader(self, attach: bytes, token: bytes | None = None) -> Iterator[duckdb.DuckDBPyConnection]:
        with self.lock:
            self.authorize(attach)
            if token is not None:
                yield self.transaction(attach, token).connection
            else:
                connection = self.connection.cursor()
                try:
                    yield connection
                finally:
                    connection.close()

    def remove_transaction(self, token: bytes) -> None:
        tx = self.transactions.pop(token)
        tx.connection.close()
        self.preparations = {key: value for key, value in self.preparations.items() if value[1] != token}
        self.executions = {key: value for key, value in self.executions.items() if value.transaction != token}

    def sweep(self) -> None:
        with self.lock:
            for token, tx in list(self.transactions.items()):
                if tx.expires <= now_ms():
                    tx.connection.execute("ROLLBACK")
                    self.remove_transaction(token)
            self.preparations = {
                key: value
                for key, value in self.preparations.items()
                if (value[2].result.expires_at_epoch_ms or 0) > now_ms()
            }
            self.completed = {key: value for key, value in self.completed.items() if value.expires > now_ms()}
            for handle, result in list(self.executions.items()):
                if result.expires <= now_ms():
                    tx = self.transactions[result.transaction]
                    tx.failed = True
                    tx.pending.discard(handle)
                    del self.executions[handle]

    def close(self) -> None:
        with self.lock:
            for token in list(self.transactions):
                self.transactions[token].connection.execute("ROLLBACK")
                self.remove_transaction(token)
            self.connection.close()


class StatementReferenceProvider(CatalogQueryProvider):
    """Describe finite SQL, then execute it on its owner's transaction connection."""

    def __init__(self, authority: StatementAuthority) -> None:
        self.authority = authority

    def capabilities(self, *, owner: QueryOwner, offer: ClientQueryCapabilities) -> QueryCapabilities | None:
        with self.authority.lock:
            self.authority.authorize(owner.attach_opaque_data)
            return QueryCapabilities(
                encoding=QUERY_ENCODING,
                semantic_profiles=[QUERY_EXPLICIT, QUERY_STATEMENT],
                execution_modes=["explicit", "statement"],
                backend_engine="duckdb",
                backend_version=duckdb.__version__,
                capability_revision=REVISION,
                default_evaluation_context=QueryEvaluationContext(),
            )

    def prepare(self, *, owner: QueryOwner, request: CatalogQueryPrepareRequest) -> CatalogQueryPrepareResult:
        authority = self.authority
        with authority.lock:
            authority.sweep()
            tx = authority.transaction(owner.attach_opaque_data, owner.transaction_opaque_data)
            spec = STATEMENTS.get(request.query)
            if spec is None or request.mode != ("explicit" if spec.read_only else "statement"):
                return CatalogQueryPrepareResult(status="declined", reason_code="unsupported_statement")
            expected = pa.schema(spec.parameters, metadata={PARAMETER_MODE_KEY: b"named"})
            if not read_parameter_schema(request.parameter_schema).equals(expected, check_metadata=True):
                raise ValueError("statement parameter schema must match exact declared names, types and metadata")
            if len(authority.preparations) >= authority.max_resources:
                raise ValueError("statement preparation quota exceeded")
            # Only metadata lookup: no SQL from request is run during prepare/bind.
            row = tx.connection.execute("SELECT version FROM _statement_version").fetchone()
            assert row is not None
            version = int(row[0])
            result = CatalogQueryPrepareResult(
                status="accepted",
                prepared_query_token=uuid.uuid4().bytes,
                output_schema=spec.output.serialize().to_pybytes(),
                result_ordering="query_order" if spec.read_only else "unspecified",
                validated_catalog_version=version,
                context_digest=request.evaluation_context.digest(request.encoding, request.semantic_profile),
                expires_at_epoch_ms=min(tx.expires, now_ms() + authority.lifetime_ms),
            )
            assert result.prepared_query_token is not None
            assert owner.transaction_opaque_data is not None
            preparation = QueryPreparation(request=request, result=result)
            authority.preparations[result.prepared_query_token] = (
                owner.scope,
                owner.transaction_opaque_data,
                preparation,
            )
            return result

    def resolve(self, *, owner: QueryOwner, recipe: bytes) -> QueryPreparation:
        with self.authority.lock:
            self.authority.transaction(owner.attach_opaque_data, owner.transaction_opaque_data)
            saved = self.authority.preparations.get(recipe)
            if saved is None or saved[0] != owner.scope or (saved[2].result.expires_at_epoch_ms or 0) <= now_ms():
                raise ValueError("statement preparation is missing, expired or belongs to another owner")
            return saved[2]

    def start(
        self, *, owner: QueryOwner, preparation: QueryPreparation, parameters: QueryParameters, execution_id: bytes
    ) -> QueryExecution:
        authority = self.authority
        with authority.lock:
            assert preparation.result.prepared_query_token is not None
            assert owner.transaction_opaque_data is not None
            saved = self.resolve(owner=owner, recipe=preparation.result.prepared_query_token)
            if saved != preparation:
                raise ValueError("statement preparation changed")
            parameters.validate_prepared_schema(saved.request.parameter_schema)
            if parameters.batch.nbytes > 8192:
                raise ValueError("statement parameter values exceed this finite provider's 8 KiB limit")
            tx = authority.transaction(owner.attach_opaque_data, owner.transaction_opaque_data)
            if tx.pending:
                raise ValueError("statement result is still pending; consume EOS or rollback")
            if execution_id in tx.started:
                raise ValueError("statement execution cannot be restarted")
            if len(tx.started) >= authority.max_resources:
                raise ValueError("statement execution quota exceeded")
            tx.started.add(execution_id)
            spec = STATEMENTS[saved.request.query]
            try:
                authority.starts += 1
                values = {
                    definition.name: duckdb.Value(parameters.batch.column(i)[0].as_py(), TYPES[definition.type])
                    for i, definition in enumerate(parameters.schema)
                }
                cursor = tx.connection.execute(saved.request.query, values)
                if spec.output == COMMAND:
                    row = None if spec.ddl else cursor.fetchone()
                    assert spec.ddl or row is not None
                    count = None if row is None else row[0]
                    cursor.fetchall()
                    batches = [pa.record_batch([pa.array([count], type=pa.int64())], schema=COMMAND)]
                    if spec.ddl:
                        tx.connection.execute("UPDATE _statement_version SET version=version+1")
                else:
                    batches = []
                    rows = 0
                    result_bytes = 0
                    while chunk := cursor.fetchmany(2):
                        rows += len(chunk)
                        if rows > authority.max_result_rows:
                            raise ValueError("statement result row quota exceeded")
                        batch = pa.record_batch(
                            [
                                pa.array(column, type=f.type)
                                for column, f in zip(zip(*chunk, strict=True), spec.output, strict=True)
                            ],
                            schema=spec.output,
                        )
                        result_bytes += batch.nbytes
                        if result_bytes > authority.max_result_bytes:
                            raise ValueError("statement result byte quota exceeded")
                        batches.append(batch)
                if sum(batch.nbytes for batch in batches) > authority.max_result_bytes:
                    raise ValueError("statement result byte quota exceeded")
                handle = uuid.uuid4().bytes
                expires = min(tx.expires, now_ms() + authority.lifetime_ms)
                authority.executions[handle] = _Result(
                    owner.scope, owner.transaction_opaque_data, batches, expires, authority.fail_read_at
                )
                authority.fail_read_at = None
                tx.pending.add(handle)
                return QueryExecution(handle=handle, expires_at_epoch_ms=expires)
            except Exception:
                tx.failed = True
                raise

    def read(self, *, owner: QueryOwner, handle: bytes, position: int) -> pa.RecordBatch | None:
        with self.authority.lock:
            tx = self.authority.transaction(owner.attach_opaque_data, owner.transaction_opaque_data)
            result = self.authority.executions.get(handle)
            if result is None or result.scope != owner.scope:
                raise ValueError("statement execution is missing; it cannot be restarted")
            if result.expires <= now_ms() or not 0 <= position <= len(result.batches):
                tx.failed = True
                raise ValueError("statement execution expired or result position is invalid")
            if position == result.fail_read_at:
                tx.failed = True
                raise RuntimeError("injected statement result failure after execution")
            if position == len(result.batches):
                result.eof = True
                return None
            return result.batches[position]

    def close(self, *, owner: QueryOwner, handle: bytes, reason: str) -> None:
        with self.authority.lock:
            result = self.authority.executions.get(handle)
            if result is None:
                return
            if result.scope != owner.scope:
                raise ValueError("statement execution belongs to another owner")
            tx = self.authority.transaction(owner.attach_opaque_data, owner.transaction_opaque_data, allow_failed=True)
            if reason != "eos" or not result.eof:
                tx.failed = True
            tx.pending.discard(handle)
            del self.authority.executions[handle]

    def release(self, *, owner: QueryOwner, recipe: bytes) -> None:
        with self.authority.lock:
            saved = self.authority.preparations.get(recipe)
            if saved is not None:
                if saved[0] != owner.scope:
                    raise ValueError("statement preparation belongs to another owner")
                del self.authority.preparations[recipe]


class StatementReferenceCatalog(ReadOnlyCatalogInterface):
    """Catalog transaction methods own statement effects, metadata and cleanup."""

    catalog_name = "statements"
    catalog_version_frozen = False
    catalog_contents_attach_independent = False
    authority: ClassVar[StatementAuthority | None] = None

    def __init__(self) -> None:
        super().__init__()
        if type(self).authority is None:
            root = Path(os.environ["VGI_STATEMENT_REFERENCE_DIR"])
            if not root.is_absolute():
                raise ValueError("VGI_STATEMENT_REFERENCE_DIR must name an isolated absolute directory")
            root.mkdir(parents=True, exist_ok=True)
            type(self).authority = StatementAuthority(root / "data.duckdb")
        assert type(self).authority is not None
        self._authority = present(type(self).authority)
        self.query_provider = StatementReferenceProvider(self._authority)

    def catalog_attach(
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str | None,
        implementation_version: str | None,
        client_capabilities: ClientCapabilities | None = None,
        ctx: CallContext | None = None,
    ) -> CatalogAttachResult:
        result = super().catalog_attach(
            name=name,
            options=options,
            data_version_spec=data_version_spec,
            implementation_version=implementation_version,
            client_capabilities=client_capabilities,
            ctx=ctx,
        )
        with self._authority.lock:
            self._authority.sweep()
            if len(self._authority.attachments) >= self._authority.max_resources:
                raise ValueError("statement attachment quota exceeded")
            attach = AttachOpaqueData(uuid.uuid4().bytes)
            self._authority.attachments.add(attach)
            return replace(
                result,
                attach_opaque_data=attach,
                attach_opaque_data_required=True,
                supports_transactions=True,
                supports_catalog_contents=False,
                catalog_version_frozen=False,
                catalog_version=self.catalog_version(attach_opaque_data=attach),
            )

    def catalog_transaction_begin(self, *, attach_opaque_data: AttachOpaqueData) -> TransactionOpaqueData:
        with self._authority.lock:
            self._authority.sweep()
            self._authority.authorize(attach_opaque_data)
            if len(self._authority.transactions) + len(self._authority.completed) >= self._authority.max_resources:
                raise ValueError("statement transaction quota exceeded")
            connection = self._authority.connection.cursor()
            try:
                connection.execute("BEGIN")
            except Exception:
                connection.close()
                raise
            token = TransactionOpaqueData(uuid.uuid4().bytes)
            self._authority.transactions[token] = _Transaction(
                attach_opaque_data, connection, now_ms() + self._authority.lifetime_ms
            )
            return token

    def _finish(self, attach: bytes, token: bytes, operation: str) -> None:
        with self._authority.lock:
            self._authority.authorize(attach)
            completed = self._authority.completed.get(token)
            if completed is not None:
                if completed.attach != attach or completed.expires <= now_ms():
                    raise ValueError("statement completion is expired or belongs to another attachment")
                if completed.operation != operation:
                    raise ValueError("statement transaction already completed with the opposite operation")
                return
            tx = self._authority.transaction(attach, token, allow_failed=True)
            if operation == "COMMIT" and (tx.failed or tx.pending):
                raise ValueError("statement transaction failed or result is pending; rollback required")
            try:
                tx.connection.execute(operation)
            except Exception:
                tx.failed = True
                raise
            self._authority.remove_transaction(token)
            self._authority.completed[token] = _Completion(attach, operation, now_ms() + self._authority.lifetime_ms)

    def catalog_transaction_commit(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData
    ) -> None:
        self._finish(attach_opaque_data, transaction_opaque_data, "COMMIT")

    def catalog_transaction_rollback(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData
    ) -> None:
        self._finish(attach_opaque_data, transaction_opaque_data, "ROLLBACK")

    def catalog_detach(self, *, attach_opaque_data: AttachOpaqueData) -> None:
        with self._authority.lock:
            for token, tx in list(self._authority.transactions.items()):
                if tx.attach == attach_opaque_data:
                    self._finish(attach_opaque_data, token, "ROLLBACK")
            self._authority.completed = {
                key: value for key, value in self._authority.completed.items() if value.attach != attach_opaque_data
            }
            self._authority.attachments.discard(attach_opaque_data)

    def catalog_version(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None = None,
        ctx: CallContext | None = None,
    ) -> int:
        with self._authority.reader(attach_opaque_data, transaction_opaque_data) as connection:
            row = connection.execute("SELECT version FROM _statement_version").fetchone()
            assert row is not None
            return int(row[0])

    def table_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_path: SchemaPath,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        if schema_path != ["main"] or name not in {"items", "added"}:
            return None
        if at_unit is not None or at_value is not None:
            raise ValueError("statement reference does not support time travel")
        with self._authority.reader(attach_opaque_data, transaction_opaque_data) as connection:
            if not connection.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema='main' AND table_name=?", [name]
            ).fetchone():
                return None
            schema = ROWS if name == "items" else pa.schema([("value", pa.int64())])
            return TableInfo(
                name=name,
                schema_path=["main"],
                columns=SerializedSchema(schema.serialize().to_pybytes()),
                constraints=[],
                comment=None,
                tags={},
            )

    def schemas(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None
    ) -> list[SchemaInfo]:
        self.catalog_version(attach_opaque_data=attach_opaque_data, transaction_opaque_data=transaction_opaque_data)
        return [SchemaInfo(path=["main"], comment=None, tags={})]

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
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: SchemaPath,
        type: SchemaObjectType,
    ) -> Sequence[TableInfo | ViewInfo | FunctionInfo | MacroInfo | IndexInfo]:
        if path != ["main"] or type != SchemaObjectType.TABLE:
            return []
        return [
            info
            for name in ["items", "added"]
            if (
                info := self.table_get(
                    attach_opaque_data=attach_opaque_data,
                    transaction_opaque_data=transaction_opaque_data,
                    schema_path=path,
                    name=name,
                )
            )
            is not None
        ]


class StatementReferenceWorker(Worker):
    """Use ordinary public query/transaction dispatch, including signed HTTP."""

    catalog_interface = StatementReferenceCatalog


def main() -> None:
    from vgi.serve import create_app

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    app = create_app(StatementReferenceWorker, describe=False, signing_key=SIGNING_KEY)
    with make_server("127.0.0.1", args.port, app) as server:
        print(f"PORT:{server.server_port}", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
