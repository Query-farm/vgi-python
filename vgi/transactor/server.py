# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""db-transactor server — multi-database DuckDB transaction manager.

Runs as a long-lived subprocess, accepting ``vgi_rpc`` connections over a
Unix domain socket. Manages multiple DuckDB databases, one per catalog
attachment (identified by ``attach_opaque_data``).

Usage::

    vgi-transactor --db-dir /path/to/databases --socket /tmp/vgi-transactor.sock

"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import uuid
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from vgi_rpc import AnnotatedBatch, OutputCollector, RpcServer
from vgi_rpc.rpc import CallContext, ExchangeState, ProducerState, Stream, StreamState, serve_unix

from vgi._duckdb import connect as engine_connect
from vgi.schema_utils import schema
from vgi.transactor._duckdb_compat import subcursor
from vgi.transactor.protocol import TransactorProtocol

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger("vgi.transactor")

_COUNT_SCHEMA = schema(count=pa.int64())


class TransactorImpl:
    """Implementation of the TransactorProtocol backed by DuckDB.

    Manages multiple databases (one per attach_opaque_data). Each transaction gets
    its own DuckDB cursor, allowing multiple concurrent transactions per
    database.
    """

    def __init__(self, db_dir: str) -> None:
        """Initialize with the directory for database files."""
        self._db_dir = db_dir
        os.makedirs(db_dir, exist_ok=True)
        self._lock = threading.Lock()
        self._databases: dict[bytes, duckdb.DuckDBPyConnection] = {}
        self._catalog_names: dict[bytes, str] = {}  # attach_opaque_data → catalog name (for view SQL stripping)
        self._catalog_versions: dict[bytes, int] = {}  # attach_opaque_data → version (incremented on DDL)
        # Transactions nested by attach_opaque_data: {attach_opaque_data: {tx_id: cursor}}
        self._transactions: dict[bytes, dict[bytes, duckdb.DuckDBPyConnection]] = {}
        self._tx_locks: dict[bytes, dict[bytes, threading.Lock]] = {}
        logger.info("Transactor started: db_dir=%s", db_dir)

    # ========== Helpers ==========

    def _get_db_conn(self, attach_opaque_data: bytes) -> duckdb.DuckDBPyConnection:
        """Get the main connection for a database, raising if not registered."""
        with self._lock:
            conn = self._databases.get(attach_opaque_data)
        if conn is None:
            msg = f"No registered database: {attach_opaque_data.hex()}"
            raise ValueError(msg)
        return conn

    def _get_tx_conn(self, attach_opaque_data: bytes, tx_id: bytes) -> duckdb.DuckDBPyConnection:
        """Get the cursor for a transaction within a database."""
        with self._lock:
            db_txns = self._transactions.get(attach_opaque_data, {})
            conn = db_txns.get(tx_id)
        if conn is None:
            msg = f"No active transaction: {tx_id.hex()} in db {attach_opaque_data.hex()}"
            raise ValueError(msg)
        return conn

    def _get_tx_lock(self, attach_opaque_data: bytes, tx_id: bytes) -> threading.Lock:
        """Get the per-transaction lock."""
        with self._lock:
            db_locks = self._tx_locks.setdefault(attach_opaque_data, {})
            if tx_id not in db_locks:
                db_locks[tx_id] = threading.Lock()
            return db_locks[tx_id]

    def _table_schema(self, qualified_name: str, attach_opaque_data: bytes, tx_id: bytes) -> pa.Schema:
        """Get the Arrow schema for a table using a subcursor."""
        conn = self._get_tx_conn(attach_opaque_data, tx_id)
        tx_lock = self._get_tx_lock(attach_opaque_data, tx_id)
        with tx_lock:
            sub = subcursor(conn)
            sql = f"SELECT * FROM {qualified_name} LIMIT 0"  # noqa: S608
            result = sub.execute(sql)
            schema: pa.Schema = result.to_arrow_table().schema
            sub.close()
            return schema

    # ========== Database lifecycle ==========

    def register(
        self, attach_opaque_data: bytes, catalog_name: str = "", ddl_statements: list[str] | None = None
    ) -> None:
        """Create a new database for this attach_opaque_data and run initial DDL."""
        db_path = os.path.join(self._db_dir, f"{attach_opaque_data.hex()}.duckdb")
        conn = engine_connect(db_path)
        with self._lock:
            self._databases[attach_opaque_data] = conn
            self._catalog_names[attach_opaque_data] = catalog_name
            self._catalog_versions[attach_opaque_data] = 1
        if ddl_statements:
            for sql in ddl_statements:
                conn.execute(sql)
        logger.info("Database registered: %s (catalog=%s) -> %s", attach_opaque_data.hex()[:8], catalog_name, db_path)

    def catalog_version(self, attach_opaque_data: bytes) -> int:
        """Return the catalog version for the database."""
        with self._lock:
            return self._catalog_versions.get(attach_opaque_data, 1)

    # ========== Transaction lifecycle ==========

    def begin(self, attach_opaque_data: bytes) -> bytes:
        """Begin a transaction on the database. Returns the tx_id."""
        db_conn = self._get_db_conn(attach_opaque_data)
        tx_id = uuid.uuid4().bytes
        cursor = db_conn.cursor()
        cursor.execute("SET enable_suspended_queries = true")
        cursor.begin()
        with self._lock:
            self._transactions.setdefault(attach_opaque_data, {})[tx_id] = cursor
            self._tx_locks.setdefault(attach_opaque_data, {})[tx_id] = threading.Lock()
        logger.info("Transaction begun: %s (db %s)", tx_id.hex()[:8], attach_opaque_data.hex()[:8])
        return tx_id

    def commit(self, attach_opaque_data: bytes, tx_id: bytes) -> None:
        """Commit a transaction."""
        conn = self._get_tx_conn(attach_opaque_data, tx_id)
        conn.commit()
        conn.close()
        with self._lock:
            self._transactions.get(attach_opaque_data, {}).pop(tx_id, None)
            self._tx_locks.get(attach_opaque_data, {}).pop(tx_id, None)
        logger.info("Transaction committed: %s", tx_id.hex()[:8])

    def rollback(self, attach_opaque_data: bytes, tx_id: bytes) -> None:
        """Rollback a transaction."""
        conn = self._get_tx_conn(attach_opaque_data, tx_id)
        conn.rollback()
        conn.close()
        with self._lock:
            self._transactions.get(attach_opaque_data, {}).pop(tx_id, None)
            self._tx_locks.get(attach_opaque_data, {}).pop(tx_id, None)
        logger.info("Transaction rolled back: %s", tx_id.hex()[:8])

    # ========== Write operations (streaming exchange) ==========

    def insert(
        self,
        attach_opaque_data: bytes,
        tx_id: bytes,
        table_name: str,
        schema_name: str = "",
        returning: bool = False,
    ) -> Stream[StreamState]:
        """Create an insert exchange stream."""
        conn = self._get_tx_conn(attach_opaque_data, tx_id)
        tx_lock = self._get_tx_lock(attach_opaque_data, tx_id)
        qualified = f"{schema_name}.{table_name}" if schema_name else table_name
        table_schema = self._table_schema(qualified, attach_opaque_data, tx_id)

        input_fields = [f for f in table_schema if f.name != "rowid"]
        input_schema = pa.schema(input_fields)
        output_schema = input_schema if returning else _COUNT_SCHEMA

        sub = subcursor(conn)
        state = _InsertState(
            conn=sub,
            qualified_name=qualified,
            returning=returning,
            table_schema=input_schema,
            tx_lock=tx_lock,
        )
        return Stream(output_schema=output_schema, state=state, input_schema=input_schema)

    def delete(
        self, attach_opaque_data: bytes, tx_id: bytes, table_name: str, schema_name: str = "", returning: bool = False
    ) -> Stream[StreamState]:
        """Create a delete exchange stream."""
        conn = self._get_tx_conn(attach_opaque_data, tx_id)
        tx_lock = self._get_tx_lock(attach_opaque_data, tx_id)
        qualified = f"{schema_name}.{table_name}" if schema_name else table_name
        table_schema = self._table_schema(qualified, attach_opaque_data, tx_id)

        input_schema = schema(rowid=pa.int64())
        ret_fields = [f for f in table_schema if f.name != "rowid"]
        ret_schema = pa.schema(ret_fields)
        output_schema = ret_schema if returning else _COUNT_SCHEMA

        sub = subcursor(conn)
        state = _DeleteState(
            conn=sub, qualified_name=qualified, returning=returning, table_schema=ret_schema, tx_lock=tx_lock
        )
        return Stream(output_schema=output_schema, state=state, input_schema=input_schema)

    def update(
        self,
        attach_opaque_data: bytes,
        tx_id: bytes,
        table_name: str,
        schema_name: str = "",
        columns: list[str] | None = None,
        returning: bool = False,
    ) -> Stream[StreamState]:
        """Create an update exchange stream."""
        conn = self._get_tx_conn(attach_opaque_data, tx_id)
        tx_lock = self._get_tx_lock(attach_opaque_data, tx_id)
        qualified = f"{schema_name}.{table_name}" if schema_name else table_name
        table_schema = self._table_schema(qualified, attach_opaque_data, tx_id)

        if columns:
            fields = [table_schema.field(c) for c in columns if table_schema.get_field_index(c) >= 0]
            fields.append(pa.field("rowid", pa.int64()))
            input_schema = pa.schema(fields)
        else:
            input_schema = table_schema

        ret_fields = [f for f in table_schema if f.name != "rowid"]
        ret_schema = pa.schema(ret_fields)
        output_schema = ret_schema if returning else _COUNT_SCHEMA

        sub = subcursor(conn)
        state = _UpdateState(
            conn=sub, qualified_name=qualified, returning=returning, table_schema=ret_schema, tx_lock=tx_lock
        )
        return Stream(output_schema=output_schema, state=state, input_schema=input_schema)

    # ========== Read (streaming producer) ==========

    def scan(
        self,
        attach_opaque_data: bytes,
        tx_id: bytes,
        table_name: str,
        columns: list[str],
        schema_name: str = "",
        pushdown_filters: bytes | None = None,
    ) -> Stream[StreamState]:
        """Create a scan producer stream within the transaction."""
        conn = self._get_tx_conn(attach_opaque_data, tx_id)
        tx_lock = self._get_tx_lock(attach_opaque_data, tx_id)
        qualified = f"{schema_name}.{table_name}" if schema_name else table_name
        col_list = ", ".join(columns) if columns else "*"

        sql = f"SELECT {col_list} FROM {qualified}"  # noqa: S608
        bind_params: list[object] = []
        if pushdown_filters is not None:
            from vgi.table_filter_pushdown import deserialize_filters

            pf_reader = pa.ipc.open_stream(pushdown_filters)
            pf_batch = pf_reader.read_next_batch()
            pf = deserialize_filters(pf_batch)
            if pf and pf.filters:
                where_clause, bind_params = pf.to_sql()
                sql += f" WHERE {where_clause}"

        with tx_lock:
            schema_sub = subcursor(conn)
            schema_sql = f"SELECT {col_list} FROM {qualified} LIMIT 0"  # noqa: S608
            output_schema = schema_sub.execute(schema_sql).to_arrow_table().schema
            schema_sub.close()

            scan_cursor = subcursor(conn)
            result = scan_cursor.execute(sql, bind_params) if bind_params else scan_cursor.execute(sql)
            reader = result.to_arrow_reader(batch_size=50_000)

        state = _ScanState(reader=reader, tx_lock=tx_lock)
        return Stream(output_schema=output_schema, state=state)

    # ========== DDL ==========

    def execute_ddl(self, attach_opaque_data: bytes, sql: str) -> None:
        """Execute DDL statement on the database (non-transactional)."""
        conn = self._get_db_conn(attach_opaque_data)
        with self._lock:
            conn.execute(sql)
            self._catalog_versions[attach_opaque_data] = self._catalog_versions.get(attach_opaque_data, 1) + 1
            logger.debug("DDL executed: %s", sql[:100])

    def execute_ddl_tx(
        self, attach_opaque_data: bytes, tx_id: bytes, sql: str, strip_catalog: str | None = None
    ) -> None:
        """Execute DDL within a transaction.

        If strip_catalog is provided it overrides the registered catalog name.
        Otherwise the catalog name from register() is used automatically.
        """
        catalog_name = strip_catalog
        if catalog_name is None:
            with self._lock:
                catalog_name = self._catalog_names.get(attach_opaque_data, "")
        if catalog_name:
            sql = self.strip_catalog_refs(sql, catalog_name)
        conn = self._get_tx_conn(attach_opaque_data, tx_id)
        tx_lock = self._get_tx_lock(attach_opaque_data, tx_id)
        with tx_lock:
            conn.execute(sql)
        with self._lock:
            self._catalog_versions[attach_opaque_data] = self._catalog_versions.get(attach_opaque_data, 1) + 1
        logger.debug("DDL (tx) executed: %s", sql[:100])

    def strip_catalog_refs(self, sql: str, catalog_name: str) -> str:
        """Strip external catalog references from SQL using AST transformation."""
        import sqlglot
        from sqlglot import exp

        try:
            parsed = sqlglot.parse_one(sql, dialect="duckdb")
        except sqlglot.errors.ParseError:
            logger.warning("strip_catalog_refs: failed to parse SQL, returning as-is: %s", sql[:100])
            return sql

        for table in parsed.find_all(exp.Table):
            if table.catalog and table.catalog.lower() == catalog_name.lower():
                table.set("catalog", None)
                if table.args.get("db") and table.args["db"].name.lower() == "main":
                    table.set("db", None)

        return parsed.sql(dialect="duckdb")

    # ========== Metadata ==========

    def _query_list(
        self, attach_opaque_data: bytes, tx_id: bytes, sql: str, params: list[object] | None = None
    ) -> list[str]:
        """Execute a query within a transaction and return the first column as a list."""
        conn = self._get_tx_conn(attach_opaque_data, tx_id)
        tx_lock = self._get_tx_lock(attach_opaque_data, tx_id)
        with tx_lock:
            result = conn.execute(sql, params or [])
            return [row[0] for row in result.fetchall()]

    def list_schemas(self, attach_opaque_data: bytes, tx_id: bytes) -> list[str]:
        """List schema names within a transaction."""
        return self._query_list(
            attach_opaque_data, tx_id, "SELECT schema_name FROM duckdb_schemas() WHERE NOT internal"
        )

    def list_user_tables(self, attach_opaque_data: bytes, tx_id: bytes, schema_name: str = "main") -> list[str]:
        """List user tables in the given schema within a transaction."""
        return self._query_list(
            attach_opaque_data,
            tx_id,
            "SELECT table_name FROM information_schema.tables WHERE table_schema=? AND table_type='BASE TABLE'",
            [schema_name],
        )

    def table_schema(self, attach_opaque_data: bytes, table_name: str, tx_id: bytes) -> bytes:
        """Get Arrow schema for a table as serialized IPC bytes within a transaction."""
        conn = self._get_tx_conn(attach_opaque_data, tx_id)
        tx_lock = self._get_tx_lock(attach_opaque_data, tx_id)
        bare_name = table_name.rsplit(".", 1)[-1] if "." in table_name else table_name
        with tx_lock:
            sub = subcursor(conn)
            if "." in table_name:
                schema_part = table_name.rsplit(".", 1)[0]
                row = sub.execute(
                    "SELECT COUNT(*) FROM duckdb_tables() WHERE schema_name = ? AND table_name = ?",
                    [schema_part, bare_name],
                ).fetchone()
                is_table = row is not None and row[0] > 0
            else:
                row = sub.execute(
                    "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = ?",
                    [bare_name],
                ).fetchone()
                is_table = row is not None and row[0] > 0
            if not is_table:
                sub.close()
                raise ValueError(f"'{table_name}' is not a table")
            schema = sub.execute(f"SELECT * FROM {table_name} LIMIT 0").to_arrow_table().schema  # noqa: S608

            col_meta_result = sub.execute(
                "SELECT column_name, comment, column_default FROM duckdb_columns() WHERE table_name = ?",
                [bare_name],
            ).fetchall()
            sub.close()
            meta_updates: dict[str, dict[bytes | str, bytes | str]] = {}
            for row in col_meta_result:
                col_name, comment, default = row[0], row[1], row[2]
                updates: dict[bytes | str, bytes | str] = {}
                if comment is not None:
                    updates[b"comment"] = comment.encode("utf-8")
                if default is not None:
                    updates[b"default"] = default.encode("utf-8")
                if updates:
                    meta_updates[col_name] = updates
            if meta_updates:
                fields = list(schema)
                for i, f in enumerate(fields):
                    if f.name in meta_updates:
                        metadata: dict[bytes | str, bytes | str] = (
                            dict(cast("dict[bytes | str, bytes | str]", f.metadata)) if f.metadata else {}
                        )
                        metadata.update(meta_updates[f.name])
                        fields[i] = f.with_metadata(metadata)
                schema = pa.schema(fields)

        with tx_lock:
            sub2 = subcursor(conn)
            constraint_rows = sub2.execute(
                "SELECT constraint_type, constraint_column_names, constraint_text, "
                "referenced_table, referenced_column_names "
                "FROM duckdb_constraints() WHERE table_name = ?",
                [bare_name],
            ).fetchall()
            sub2.close()

        import json

        constraints_json = json.dumps(
            [
                {
                    "type": row[0],
                    "columns": row[1],
                    "text": row[2],
                    "referenced_table": row[3],
                    "referenced_columns": row[4],
                }
                for row in constraint_rows
            ]
        )
        schema_meta: dict[bytes | str, bytes | str] = {b"vgi.constraints": constraints_json.encode("utf-8")}

        rowid_field = pa.field("rowid", pa.int64(), metadata={b"is_row_id": b""})
        result_schema = pa.schema([rowid_field, *schema], metadata=schema_meta)
        return result_schema.serialize().to_pybytes()

    def table_comment(self, attach_opaque_data: bytes, table_name: str, tx_id: bytes) -> str | None:
        """Get the comment on a table within a transaction."""
        conn = self._get_tx_conn(attach_opaque_data, tx_id)
        tx_lock = self._get_tx_lock(attach_opaque_data, tx_id)
        bare_name = table_name.rsplit(".", 1)[-1] if "." in table_name else table_name
        with tx_lock:
            result = conn.execute(
                "SELECT comment FROM duckdb_tables() WHERE table_name = ?",
                [bare_name],
            ).fetchone()
        if result and result[0]:
            return str(result[0])
        return None

    def list_user_views(self, attach_opaque_data: bytes, tx_id: bytes, schema_name: str = "main") -> list[str]:
        """List user-created view names in the given schema within a transaction."""
        return self._query_list(
            attach_opaque_data,
            tx_id,
            "SELECT view_name FROM duckdb_views() WHERE schema_name = ? AND NOT internal",
            [schema_name],
        )

    def view_info(self, attach_opaque_data: bytes, view_name: str, tx_id: bytes) -> str:
        """Return view info as JSON (definition, comment)."""
        conn = self._get_tx_conn(attach_opaque_data, tx_id)
        tx_lock = self._get_tx_lock(attach_opaque_data, tx_id)
        with tx_lock:
            sub = subcursor(conn)
            result = sub.execute(
                "SELECT sql, comment FROM duckdb_views() WHERE view_name = ?",
                [view_name],
            ).fetchone()
            sub.close()
        if result is None:
            raise ValueError(f"View '{view_name}' not found")
        import json
        import re

        definition = result[0] or ""
        match = re.match(r"CREATE\s+VIEW\s+\S+\s+AS\s+", definition, re.IGNORECASE)
        if match:
            definition = definition[match.end() :]
            definition = definition.rstrip().rstrip(";")
        return json.dumps({"definition": definition, "comment": result[1]})

    # ========== Lifecycle ==========

    def ping(self) -> None:
        """Health check."""

    def shutdown(self) -> None:
        """Graceful shutdown — rollback active transactions and close all databases."""
        logger.info("Shutdown requested")
        with self._lock:
            for attach_opaque_data, db_txns in list(self._transactions.items()):
                for tx_id, conn in list(db_txns.items()):
                    try:
                        conn.rollback()
                        conn.close()
                        logger.info("Rolled back orphan tx: %s (db %s)", tx_id.hex()[:8], attach_opaque_data.hex()[:8])
                    except Exception:
                        logger.exception("Failed to rollback tx: %s", tx_id.hex()[:8])
            self._transactions.clear()
            self._tx_locks.clear()
            for attach_opaque_data, conn in list(self._databases.items()):
                try:
                    conn.close()
                except Exception:
                    logger.exception("Failed to close database: %s", attach_opaque_data.hex()[:8])
            self._databases.clear()
        sys.exit(0)


# ============================================================================
# Stream state implementations
# ============================================================================


def _read_result_batch(result: duckdb.DuckDBPyConnection) -> pa.RecordBatch:
    """Read DML result as a single Arrow batch."""
    table = result.to_arrow_table()
    batches = table.to_batches()
    if not batches:
        return pa.record_batch({f.name: [] for f in table.schema}, schema=table.schema)
    if len(batches) == 1:
        return batches[0]
    return pa.Table.from_batches(batches, schema=table.schema).combine_chunks().to_batches()[0]


_batch_counter = 0
_batch_counter_lock = threading.Lock()


def _unique_batch_name(prefix: str) -> str:
    """Generate a unique registered batch name to avoid collisions across transactions."""
    global _batch_counter  # noqa: PLW0603
    with _batch_counter_lock:
        _batch_counter += 1
        return f"__{prefix}_{_batch_counter}__"


class _InsertState(ExchangeState):
    """Insert exchange: receives row batches, inserts into table, returns count or RETURNING rows."""

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        qualified_name: str,
        returning: bool,
        table_schema: pa.Schema,
        tx_lock: threading.Lock,
    ) -> None:
        self.conn = conn
        self.qualified_name = qualified_name
        self.returning = returning
        self.table_schema = table_schema
        self.tx_lock = tx_lock

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        with self.tx_lock:
            batch = input.batch
            col_names = ", ".join(batch.schema.names)
            view_name = _unique_batch_name("insert")
            sql = f"INSERT INTO {self.qualified_name} ({col_names}) SELECT * FROM {view_name}"  # noqa: S608
            if self.returning:
                ret_cols = ", ".join(self.table_schema.names)
                sql += f" RETURNING {ret_cols}"
            self.conn.register(view_name, batch)
            result = self.conn.execute(sql)
            result_batch = _read_result_batch(result)
            self.conn.unregister(view_name)
        self._emit_result(result_batch, out)

    def _emit_result(self, result_batch: pa.RecordBatch, out: OutputCollector) -> None:
        if self.returning:
            out.emit(
                result_batch
                if result_batch.num_rows > 0
                else pa.record_batch({c: [] for c in self.table_schema.names}, schema=self.table_schema)
            )
        else:
            count = result_batch.column("Count")[0].as_py() if result_batch.num_rows > 0 else 0
            out.emit(pa.record_batch({"count": [count]}, schema=_COUNT_SCHEMA))


class _DeleteState(ExchangeState):
    """Delete exchange: receives rowid batches, deletes matching rows."""

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        qualified_name: str,
        returning: bool,
        table_schema: pa.Schema,
        tx_lock: threading.Lock,
    ) -> None:
        self.conn = conn
        self.qualified_name = qualified_name
        self.returning = returning
        self.table_schema = table_schema
        self.tx_lock = tx_lock

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        with self.tx_lock:
            batch = input.batch
            view_name = _unique_batch_name("delete")
            self.conn.register(view_name, batch)
            if self.returning:
                ret_cols = ", ".join(f"{self.qualified_name}.{c}" for c in self.table_schema.names)
                select_sql = (
                    f"SELECT {ret_cols} FROM {self.qualified_name} "  # noqa: S608
                    f"JOIN {view_name} ON {self.qualified_name}.rowid = {view_name}.rowid"
                )
                result_batch = _read_result_batch(self.conn.execute(select_sql))
                self.conn.execute(
                    f"DELETE FROM {self.qualified_name} "  # noqa: S608
                    f"USING {view_name} WHERE {self.qualified_name}.rowid = {view_name}.rowid",
                )
            else:
                result = self.conn.execute(
                    f"DELETE FROM {self.qualified_name} "  # noqa: S608
                    f"USING {view_name} WHERE {self.qualified_name}.rowid = {view_name}.rowid",
                )
                result_batch = _read_result_batch(result)
            self.conn.unregister(view_name)

        if self.returning:
            out.emit(
                result_batch
                if result_batch.num_rows > 0
                else pa.record_batch({c: [] for c in self.table_schema.names}, schema=self.table_schema)
            )
        else:
            count = result_batch.column("Count")[0].as_py() if result_batch.num_rows > 0 else 0
            out.emit(pa.record_batch({"count": [count]}, schema=_COUNT_SCHEMA))


class _UpdateState(ExchangeState):
    """Update exchange: receives rowid + updated columns, updates matching rows."""

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        qualified_name: str,
        returning: bool,
        table_schema: pa.Schema,
        tx_lock: threading.Lock,
    ) -> None:
        self.conn = conn
        self.qualified_name = qualified_name
        self.returning = returning
        self.table_schema = table_schema
        self.tx_lock = tx_lock

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        with self.tx_lock:
            batch = input.batch
            view_name = _unique_batch_name("update")
            update_cols = [name for name in batch.schema.names if name != "rowid"]
            set_clause = ", ".join(f"{col} = {view_name}.{col}" for col in update_cols)
            sql = (
                f"UPDATE {self.qualified_name} SET {set_clause} "  # noqa: S608
                f"FROM {view_name} WHERE {self.qualified_name}.rowid = {view_name}.rowid"
            )
            if self.returning:
                ret_cols = ", ".join(self.table_schema.names)
                sql += f" RETURNING {ret_cols}"
            self.conn.register(view_name, batch)
            result = self.conn.execute(sql)
            result_batch = _read_result_batch(result)
            self.conn.unregister(view_name)

        if self.returning:
            out.emit(
                result_batch
                if result_batch.num_rows > 0
                else pa.record_batch({c: [] for c in self.table_schema.names}, schema=self.table_schema)
            )
        else:
            count = result_batch.column("Count")[0].as_py() if result_batch.num_rows > 0 else 0
            out.emit(pa.record_batch({"count": [count]}, schema=_COUNT_SCHEMA))


class _ScanState(ProducerState):
    """Scan producer: streams Arrow batches from a query result."""

    def __init__(self, reader: pa.RecordBatchReader, tx_lock: threading.Lock) -> None:
        self._reader = reader
        self._tx_lock = tx_lock

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        with self._tx_lock:
            while True:
                try:
                    batch = self._reader.read_next_batch()
                except StopIteration:
                    out.finish()
                    return
                if batch.num_rows > 0:
                    out.emit(batch)
                    return


def main() -> None:
    """Entry point for the vgi-transactor command."""
    # sqlglot is imported lazily inside ``strip_catalog_refs``, but DDL paths
    # require it. Surface a clear install message at startup instead of
    # blowing up mid-transaction.
    try:
        import sqlglot  # noqa: F401
    except ImportError:
        import sys as _sys

        _sys.exit("vgi-transactor requires the transactor extra. Install with: pip install 'vgi-python[transactor]'")

    parser = argparse.ArgumentParser(description="VGI db-transactor server")
    parser.add_argument("--db-dir", required=True, help="Directory for DuckDB database files")
    parser.add_argument("--socket", required=True, help="Unix domain socket path to listen on")
    parser.add_argument("--log-file", default=None, help="Log file path (default: derived from socket path)")
    args = parser.parse_args()

    log_path = args.log_file or args.socket.replace(".sock", ".log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        filename=log_path,
    )

    impl = TransactorImpl(args.db_dir)
    server = RpcServer(TransactorProtocol, impl)
    logger.info("Serving on %s", args.socket)
    serve_unix(server, args.socket, threaded=True)


if __name__ == "__main__":
    main()
