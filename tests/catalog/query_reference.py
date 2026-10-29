# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D103, D107
"""Finite read-query reference with portable SQLite preparations and result fragments.

This is a lifecycle fixture, not a general SQL sandbox. Only the exact queries
below are supported. DuckDB binds typed NULLs for metadata during preparation;
only start fetches result rows. Results are materialized before returning init,
so another process can continue without serializing a cursor or rerunning SQL.
The storage/latency cost is intentional and is not a production performance claim.
"""

from __future__ import annotations

import sqlite3
import time
import uuid
from contextlib import closing
from dataclasses import replace
from typing import Any, ClassVar

import duckdb
import pyarrow as pa
from vgi_rpc.rpc import CallContext
from vgi_rpc.utils import deserialize_record_batch, serialize_record_batch_bytes

from vgi.catalog import AttachOpaqueData, CatalogAttachResult, TransactionOpaqueData
from vgi.catalog.query import (
    QUERY_ENCODING,
    QUERY_EXPLICIT,
    CatalogQueryPrepareRequest,
    CatalogQueryPrepareResult,
    ClientQueryCapabilities,
    QueryCapabilities,
    QueryEvaluationContext,
    read_query_schema,
)
from vgi.protocol import ClientCapabilities
from vgi.query_parameters import PARAMETER_MODE_KEY, QueryParameters, read_parameter_schema
from vgi.query_provider import CatalogQueryProvider, QueryExecution, QueryOwner, QueryPreparation
from vgi.worker import Worker

from .schema_options_reference import SchemaOptionsCatalog

QUERIES = {
    "SELECT $value AS result": ["value"],
    "SELECT $value + $value AS result": ["value"],
    "SELECT $1 AS result": ["1"],
    "SELECT i * $factor AS result FROM numbers ORDER BY i": ["factor"],
    "SELECT i AS result FROM numbers WHERE false": [],
    "SELECT i AS result FROM numbers ORDER BY i": [],
}
TYPES = {pa.int32(): duckdb.sqltypes.INTEGER, pa.int64(): duckdb.sqltypes.BIGINT, pa.string(): duckdb.sqltypes.VARCHAR}
ARROW_TYPES = {str(v): k for k, v in TYPES.items()}


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class QueryReferenceProvider(CatalogQueryProvider):
    """Bind a finite query set and store ordered results with bounded leases."""

    revision = "finite-reference-1"
    enabled = True
    lifetime_ms = 120_000
    max_resources = 64
    max_bytes = 16 * 1024 * 1024

    def __init__(self, catalog: QueryReferenceCatalog) -> None:
        self.catalog = catalog

    def _db(self) -> sqlite3.Connection:
        return sqlite3.connect(self.catalog.database_path)

    def _authorize(self, db: sqlite3.Connection, owner: QueryOwner) -> int:
        if not db.execute("SELECT 1 FROM query_attachments WHERE id=?", (owner.attach_opaque_data,)).fetchone():
            raise PermissionError("query attachment is closed")
        return self.catalog._snapshot(
            db,
            AttachOpaqueData(owner.attach_opaque_data),
            None if owner.transaction_opaque_data is None else TransactionOpaqueData(owner.transaction_opaque_data),
        )[0]

    @staticmethod
    def _sweep(db: sqlite3.Connection) -> None:
        db.execute("DELETE FROM query_preparations WHERE expires<=?", (now_ms(),))
        db.execute("DELETE FROM query_executions WHERE expires<=?", (now_ms(),))
        db.execute("DELETE FROM query_fragments WHERE handle NOT IN (SELECT handle FROM query_executions)")

    def sweep(self) -> None:
        with closing(self._db()) as db, db:
            self._sweep(db)

    def _quota(self, db: sqlite3.Connection) -> None:
        count = sum(
            db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ["query_preparations", "query_executions"]
        )
        size = db.execute("SELECT coalesce(sum(length(payload)), 0) FROM query_fragments").fetchone()[0]
        if count >= self.max_resources or size >= self.max_bytes:
            raise RuntimeError("query resource quota exceeded")

    def capabilities(self, *, owner: QueryOwner, offer: ClientQueryCapabilities) -> QueryCapabilities | None:
        with closing(self._db()) as db:
            self._authorize(db, owner)
        if not self.enabled:
            return None
        return QueryCapabilities(
            encoding=QUERY_ENCODING,
            semantic_profiles=[QUERY_EXPLICIT],
            execution_modes=["explicit"],
            backend_engine="duckdb",
            backend_version=duckdb.__version__,
            capability_revision=self.revision,
            default_evaluation_context=QueryEvaluationContext(),
        )

    @staticmethod
    def _connection() -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(config={"enable_external_access": False, "threads": 1})
        con.register("numbers", pa.table({"i": pa.array(range(7), type=pa.int64())}))
        return con

    @staticmethod
    def _parameters(schema: pa.Schema, values: QueryParameters | None = None) -> dict[str, duckdb.Value]:
        return {
            field.name: duckdb.Value(None if values is None else values.batch.column(i)[0].as_py(), TYPES[field.type])
            for i, field in enumerate(schema)
        }

    @staticmethod
    def _sql(query: str, schema: pa.Schema) -> str:
        assert query in QUERIES
        for field in schema:
            query = query.replace(f"${field.name}", f"CAST(${field.name} AS {TYPES[field.type]})")
        return query

    @staticmethod
    def _output(relation: duckdb.DuckDBPyRelation) -> pa.Schema:
        return pa.schema(
            [
                pa.field(name, ARROW_TYPES[str(dtype)])
                for name, dtype in zip(relation.columns, relation.types, strict=True)
            ]
        )

    def prepare(self, *, owner: QueryOwner, request: CatalogQueryPrepareRequest) -> CatalogQueryPrepareResult:
        with closing(self._db()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version = self._authorize(db, owner)
            self._sweep(db)
            self._quota(db)
            if request.query not in QUERIES or request.mode != "explicit":
                return CatalogQueryPrepareResult(status="declined", reason_code="unsupported_query")
            schema = read_parameter_schema(request.parameter_schema)
            if schema.names != QUERIES[request.query]:
                raise ValueError("query placeholder mapping does not match parameter schema")
            if any(f.type not in TYPES or f.metadata for f in schema) or set(schema.metadata or {}) != {
                PARAMETER_MODE_KEY
            }:
                return CatalogQueryPrepareResult(status="declined", reason_code="unsupported_parameter_type")
            with self._connection() as con:
                relation = con.sql(self._sql(request.query, schema), params=self._parameters(schema))
                assert relation is not None
                output = self._output(relation)  # No fetch/execute or result data is touched.
            result = CatalogQueryPrepareResult(
                status="accepted",
                prepared_query_token=uuid.uuid4().bytes,
                output_schema=output.serialize().to_pybytes(),
                result_ordering="query_order",
                validated_catalog_version=version,
                context_digest=request.evaluation_context.digest(request.encoding, request.semantic_profile),
                expires_at_epoch_ms=now_ms() + self.lifetime_ms,
            )
            result.validate_request(request)
            db.execute(
                "INSERT INTO query_preparations VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    result.prepared_query_token,
                    owner.scope,
                    owner.attach_opaque_data,
                    owner.transaction_opaque_data,
                    result.expires_at_epoch_ms,
                    request.serialize_to_bytes(),
                    result.serialize_to_bytes(),
                ),
            )
            return result

    def _resolve(self, db: sqlite3.Connection, owner: QueryOwner, recipe: bytes) -> QueryPreparation:
        self._authorize(db, owner)
        row = db.execute(
            "SELECT request, result, expires FROM query_preparations WHERE handle=? AND scope=?", (recipe, owner.scope)
        ).fetchone()
        if row is None or row[2] <= now_ms():
            raise ValueError("query preparation is missing or expired")
        return QueryPreparation(
            request=CatalogQueryPrepareRequest.deserialize_from_bytes(row[0]),
            result=CatalogQueryPrepareResult.deserialize_from_bytes(row[1]),
        )

    def resolve(self, *, owner: QueryOwner, recipe: bytes) -> QueryPreparation:
        with closing(self._db()) as db:
            return self._resolve(db, owner, recipe)

    def start(
        self, *, owner: QueryOwner, preparation: QueryPreparation, parameters: QueryParameters, execution_id: bytes
    ) -> QueryExecution:
        with closing(self._db()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            self._sweep(db)
            assert preparation.result.prepared_query_token is not None
            saved = self._resolve(db, owner, preparation.result.prepared_query_token)
            if saved != preparation:
                raise ValueError("query preparation changed")
            self._quota(db)
            parameters.validate_prepared_schema(saved.request.parameter_schema)
            with self._connection() as con:
                relation = con.sql(
                    self._sql(saved.request.query, parameters.schema),
                    params=self._parameters(parameters.schema, parameters),
                )
                assert relation is not None
                assert saved.result.output_schema is not None
                if not self._output(relation).equals(
                    read_query_schema(saved.result.output_schema), check_metadata=True
                ):
                    raise ValueError("query output changed")
                batches = list(relation.to_arrow_reader(batch_size=2))
            fragments = [serialize_record_batch_bytes(batch) for batch in batches]
            existing = db.execute("SELECT coalesce(sum(length(payload)),0) FROM query_fragments").fetchone()[0]
            if existing + sum(map(len, fragments)) > self.max_bytes:
                raise RuntimeError("query result quota exceeded")
            handle = uuid.uuid4().bytes
            expires = now_ms() + self.lifetime_ms
            db.execute(
                "INSERT INTO query_executions VALUES (?, ?, ?, ?, ?, ?)",
                (
                    handle,
                    owner.scope,
                    owner.attach_opaque_data,
                    owner.transaction_opaque_data,
                    expires,
                    len(fragments),
                ),
            )
            db.executemany(
                "INSERT INTO query_fragments VALUES (?, ?, ?)",
                [(handle, i, payload) for i, payload in enumerate(fragments)],
            )
            db.execute("UPDATE query_audit SET starts=starts+1")
            return QueryExecution(handle=handle, expires_at_epoch_ms=expires)

    def read(self, *, owner: QueryOwner, handle: bytes, position: int) -> pa.RecordBatch | None:
        with closing(self._db()) as db:
            self._authorize(db, owner)
            row = db.execute(
                "SELECT expires, batches FROM query_executions WHERE handle=? AND scope=?", (handle, owner.scope)
            ).fetchone()
            if row is None or row[0] <= now_ms():
                raise ValueError("query execution is missing or expired; it cannot be restarted")
            if not 0 <= position <= row[1]:
                raise ValueError("query position out of bounds")
            if position == row[1]:
                return None
            payload = db.execute(
                "SELECT payload FROM query_fragments WHERE handle=? AND position=?", (handle, position)
            ).fetchone()
            if payload is None:
                raise ValueError("query execution lost a result fragment")
            return deserialize_record_batch(payload[0])[0]

    def close(self, *, owner: QueryOwner, handle: bytes, reason: str) -> None:
        with closing(self._db()) as db, db:
            db.execute("DELETE FROM query_executions WHERE handle=? AND scope=?", (handle, owner.scope))
            self._sweep(db)

    def release(self, *, owner: QueryOwner, recipe: bytes) -> None:
        with closing(self._db()) as db, db:
            db.execute("DELETE FROM query_preparations WHERE handle=? AND scope=?", (recipe, owner.scope))
            self._sweep(db)


class QueryReferenceCatalog(SchemaOptionsCatalog):
    """Give query resources durable attachment and transaction lifetimes."""

    catalog_name = "queries"
    database_path: ClassVar[str]
    # Schema transactions supply durable transaction identity; the query fixture
    # reads an immutable seven-row relation and claims no DDL or DML integration.

    def __init__(self) -> None:
        super().__init__()
        self.query_provider = QueryReferenceProvider(self)
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("CREATE TABLE IF NOT EXISTS query_attachments (id BLOB PRIMARY KEY)")
            db.execute(
                "CREATE TABLE IF NOT EXISTS query_preparations "
                "(handle BLOB PRIMARY KEY, scope BLOB, attach BLOB, tx BLOB, "
                "expires INTEGER, request BLOB, result BLOB)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS query_executions "
                "(handle BLOB PRIMARY KEY, scope BLOB, attach BLOB, tx BLOB, expires INTEGER, batches INTEGER)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS query_fragments "
                "(handle BLOB, position INTEGER, payload BLOB, PRIMARY KEY(handle, position))"
            )
            db.execute("CREATE TABLE IF NOT EXISTS query_audit (id INTEGER PRIMARY KEY, starts INTEGER)")
            db.execute("INSERT OR IGNORE INTO query_audit VALUES (1, 0)")

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
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("INSERT INTO query_attachments VALUES (?)", (result.attach_opaque_data,))
        return replace(result, attach_opaque_data_required=True)

    def catalog_detach(self, *, attach_opaque_data: AttachOpaqueData) -> None:
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("DELETE FROM query_attachments WHERE id=?", (attach_opaque_data,))
            for table in ["query_preparations", "query_executions"]:
                db.execute(f"DELETE FROM {table} WHERE attach=?", (attach_opaque_data,))
            QueryReferenceProvider._sweep(db)

    def _finish(self, attach: AttachOpaqueData, tx: TransactionOpaqueData, status: str) -> None:
        super()._finish(attach, tx, status)
        with closing(sqlite3.connect(self.database_path)) as db, db:
            for table in ["query_preparations", "query_executions"]:
                db.execute(f"DELETE FROM {table} WHERE attach=? AND tx=?", (attach, tx))
            QueryReferenceProvider._sweep(db)


class QueryReferenceWorker(Worker):
    """Serve the query fixture through normal VGI dispatch."""

    catalog_interface = QueryReferenceCatalog


OFFER = ClientQueryCapabilities(
    encodings=[QUERY_ENCODING], semantic_profiles=[QUERY_EXPLICIT], engine_version="fixture"
)
