"""db-transactor server — single-process DuckDB transaction manager.

Runs as a long-lived subprocess, accepting ``vgi_rpc`` connections over a
Unix domain socket. Owns a single DuckDB connection and serializes all
operations through it.

Usage::

    vgi-transactor --db-path /path/to/store.duckdb --socket /tmp/vgi-transactor.sock

"""

from __future__ import annotations

import argparse
import logging
import sys
import threading

import duckdb
import pyarrow as pa
from vgi_rpc import AnnotatedBatch, OutputCollector, RpcServer
from vgi_rpc.rpc import CallContext, ExchangeState, ProducerState, Stream, StreamState, serve_unix

from vgi.transactor.protocol import TransactorProtocol

logger = logging.getLogger("vgi.transactor")

_COUNT_SCHEMA = pa.schema([("count", pa.int64())])


class TransactorImpl:
    """Implementation of the TransactorProtocol backed by DuckDB.

    Each transaction gets its own DuckDB connection (via cursor()), allowing
    multiple concurrent transactions against the same database. Operations
    look up the connection by tx_id.
    """

    def __init__(self, db_path: str) -> None:
        """Open DuckDB connection and initialize."""
        self._db_path = db_path
        self._conn = duckdb.connect(db_path)
        self._lock = threading.Lock()
        self._transactions: dict[bytes, duckdb.DuckDBPyConnection] = {}
        self._tx_locks: dict[bytes, threading.Lock] = {}
        logger.info("Transactor started: %s", db_path)

    # ========== Helpers ==========

    def _table_schema(self, qualified_name: str, tx_id: bytes) -> pa.Schema:
        """Get the Arrow schema for a table using a subcursor to avoid interfering with open scans."""
        conn = self._get_tx_conn(tx_id)
        tx_lock = self._get_tx_lock(tx_id)
        with tx_lock:
            sub = conn.subcursor()
            sql = f"SELECT * FROM {qualified_name} LIMIT 0"  # noqa: S608
            result = sub.execute(sql)
            schema = result.to_arrow_table().schema
            sub.close()
            return schema

    def _get_tx_conn(self, tx_id: bytes) -> duckdb.DuckDBPyConnection:
        """Get the connection for a transaction, raising if not found."""
        with self._lock:
            conn = self._transactions.get(tx_id)
        if conn is None:
            msg = f"No active transaction: {tx_id.hex()}"
            raise ValueError(msg)
        return conn

    def _get_tx_lock(self, tx_id: bytes) -> threading.Lock:
        """Get or create a per-transaction lock to serialize operations."""
        with self._lock:
            if tx_id not in self._tx_locks:
                self._tx_locks[tx_id] = threading.Lock()
            return self._tx_locks[tx_id]

    # ========== Transaction lifecycle ==========

    def begin(self, tx_id: bytes) -> None:
        """Begin a transaction — creates a new connection for this tx_id."""
        with self._lock:
            if tx_id in self._transactions:
                msg = f"Transaction already active: {tx_id.hex()}"
                raise ValueError(msg)
        conn = self._conn.cursor()
        conn.execute("SET enable_suspended_queries = true")
        conn.begin()
        with self._lock:
            self._transactions[tx_id] = conn
        logger.info("Transaction begun: %s", tx_id.hex())

    def commit(self, tx_id: bytes) -> None:
        """Commit a transaction."""
        conn = self._get_tx_conn(tx_id)
        conn.commit()
        conn.close()
        with self._lock:
            del self._transactions[tx_id]
            self._tx_locks.pop(tx_id, None)
        logger.info("Transaction committed: %s", tx_id.hex())

    def rollback(self, tx_id: bytes) -> None:
        """Rollback a transaction."""
        conn = self._get_tx_conn(tx_id)
        conn.rollback()
        conn.close()
        with self._lock:
            del self._transactions[tx_id]
            self._tx_locks.pop(tx_id, None)
        logger.info("Transaction rolled back: %s", tx_id.hex())

    # ========== Write operations (streaming exchange) ==========

    def insert(
        self,
        tx_id: bytes,
        table_name: str,
        schema_name: str = "",
        returning: bool = False,
    ) -> Stream[StreamState]:
        """Create an insert exchange stream."""
        conn = self._get_tx_conn(tx_id)
        tx_lock = self._get_tx_lock(tx_id)
        qualified = f"{schema_name}.{table_name}" if schema_name else table_name
        table_schema = self._table_schema(qualified, tx_id)

        # Input schema excludes rowid (DuckDB pseudocolumn, not a real column)
        input_fields = [f for f in table_schema if f.name != "rowid"]
        input_schema = pa.schema(input_fields)

        # Output schema for RETURNING also excludes rowid
        output_schema = input_schema if returning else _COUNT_SCHEMA

        sub = conn.subcursor()
        state = _InsertState(
            conn=sub,
            qualified_name=qualified,
            returning=returning,
            table_schema=input_schema,
            tx_lock=tx_lock,
        )

        return Stream(output_schema=output_schema, state=state, input_schema=input_schema)

    def delete(
        self, tx_id: bytes, table_name: str, schema_name: str = "", returning: bool = False
    ) -> Stream[StreamState]:
        """Create a delete exchange stream."""
        conn = self._get_tx_conn(tx_id)
        tx_lock = self._get_tx_lock(tx_id)
        qualified = f"{schema_name}.{table_name}" if schema_name else table_name
        table_schema = self._table_schema(qualified, tx_id)

        # Input: rowid column only
        input_schema = pa.schema([("rowid", pa.int64())])

        # Output: table columns (sans rowid) if RETURNING, else count
        ret_fields = [f for f in table_schema if f.name != "rowid"]
        ret_schema = pa.schema(ret_fields)
        output_schema = ret_schema if returning else _COUNT_SCHEMA

        sub = conn.subcursor()
        state = _DeleteState(
            conn=sub, qualified_name=qualified, returning=returning, table_schema=ret_schema, tx_lock=tx_lock
        )
        return Stream(output_schema=output_schema, state=state, input_schema=input_schema)

    def update(
        self,
        tx_id: bytes,
        table_name: str,
        schema_name: str = "",
        columns: list[str] | None = None,
        returning: bool = False,
    ) -> Stream[StreamState]:
        """Create an update exchange stream."""
        conn = self._get_tx_conn(tx_id)
        tx_lock = self._get_tx_lock(tx_id)
        qualified = f"{schema_name}.{table_name}" if schema_name else table_name
        table_schema = self._table_schema(qualified, tx_id)

        # Build input schema from the update columns + rowid
        if columns:
            fields = [table_schema.field(c) for c in columns if table_schema.get_field_index(c) >= 0]
            fields.append(pa.field("rowid", pa.int64()))
            input_schema = pa.schema(fields)
        else:
            input_schema = table_schema

        # Output: table columns (sans rowid) if RETURNING, else count
        ret_fields = [f for f in table_schema if f.name != "rowid"]
        ret_schema = pa.schema(ret_fields)
        output_schema = ret_schema if returning else _COUNT_SCHEMA

        sub = conn.subcursor()
        state = _UpdateState(
            conn=sub, qualified_name=qualified, returning=returning, table_schema=ret_schema, tx_lock=tx_lock
        )
        return Stream(output_schema=output_schema, state=state, input_schema=input_schema)

    # ========== Read (streaming producer) ==========

    def scan(
        self,
        tx_id: bytes,
        table_name: str,
        columns: list[str],
        schema_name: str = "",
        pushdown_filters: bytes | None = None,
    ) -> Stream[StreamState]:
        """Create a scan producer stream within the transaction."""
        conn = self._get_tx_conn(tx_id)
        tx_lock = self._get_tx_lock(tx_id)
        qualified = f"{schema_name}.{table_name}" if schema_name else table_name
        col_list = ", ".join(columns) if columns else "*"

        # Build SQL with optional WHERE clause from pushdown filters
        sql = f"SELECT {col_list} FROM {qualified}"  # noqa: S608
        bind_params: list[object] = []
        if pushdown_filters is not None:
            from vgi.table_filter_pushdown import deserialize_filters

            reader = pa.ipc.open_stream(pushdown_filters)
            pf_batch = reader.read_next_batch()
            pf = deserialize_filters(pf_batch)
            if pf and pf.filters:
                where_clause, bind_params = pf.to_sql()
                sql += f" WHERE {where_clause}"

        # Acquire the transaction lock for schema probe + scan execute.
        # All subcursor operations must be serialized to prevent concurrent
        # access to the shared DuckDB connection.
        with tx_lock:
            schema_sub = conn.subcursor()
            schema_sql = f"SELECT {col_list} FROM {qualified} LIMIT 0"  # noqa: S608
            output_schema = schema_sub.execute(schema_sql).to_arrow_table().schema
            schema_sub.close()

            scan_cursor = conn.subcursor()
            result = scan_cursor.execute(sql, bind_params) if bind_params else scan_cursor.execute(sql)
            reader = result.to_arrow_reader(batch_size=50_000)

        state = _ScanState(reader=reader, tx_lock=tx_lock)
        return Stream(output_schema=output_schema, state=state)

    # ========== DDL ==========

    def execute_ddl(self, sql: str) -> None:
        """Execute DDL statement."""
        with self._lock:
            self._conn.execute(sql)
            logger.debug("DDL executed: %s", sql[:100])

    def execute_ddl_tx(self, tx_id: bytes, sql: str, strip_catalog: str | None = None) -> None:
        """Execute DDL within a transaction.

        Args:
            tx_id: Transaction identifier.
            sql: DDL SQL to execute.
            strip_catalog: If provided, strip this catalog name from qualified
                references in the SQL before executing. Used for view definitions
                where DuckDB's binder qualifies table names with the full catalog
                path (e.g., ``writable.main.t1`` → ``t1``).
        """
        if strip_catalog:
            sql = self.strip_catalog_refs(sql, strip_catalog)
        conn = self._get_tx_conn(tx_id)
        tx_lock = self._get_tx_lock(tx_id)
        with tx_lock:
            conn.execute(sql)
        logger.debug("DDL (tx) executed: %s", sql[:100])

    def strip_catalog_refs(self, sql: str, catalog_name: str) -> str:
        """Strip external catalog references from SQL using AST transformation.

        DuckDB's binder qualifies table references with the full catalog path
        (e.g., ``writable.main.t1``). Since the transactor's DuckDB only has
        local tables, we parse the SQL, walk table references, and strip the
        catalog prefix from any that match the given catalog name.
        """
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
                # Also strip schema if it's "main" (default)
                if table.args.get("db") and table.args["db"].name.lower() == "main":
                    table.set("db", None)

        return parsed.sql(dialect="duckdb")

    # ========== Metadata ==========

    def list_schemas(self, tx_id: bytes) -> list[str]:
        """List schema names within a transaction."""
        conn = self._get_tx_conn(tx_id)
        tx_lock = self._get_tx_lock(tx_id)
        with tx_lock:
            result = conn.execute(
                "SELECT schema_name FROM duckdb_schemas() "
                "WHERE NOT internal"
            )
            return [row[0] for row in result.fetchall()]

    def list_user_tables(self, tx_id: bytes, schema_name: str = "main") -> list[str]:
        """List user tables in the given schema within a transaction."""
        conn = self._get_tx_conn(tx_id)
        tx_lock = self._get_tx_lock(tx_id)
        with tx_lock:
            result = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema=? AND table_type='BASE TABLE'",
                [schema_name],
            )
            return [row[0] for row in result.fetchall()]

    def table_schema(self, table_name: str, tx_id: bytes) -> bytes:
        """Get Arrow schema for a table as serialized IPC bytes within a transaction.

        Prepends the rowid pseudocolumn with is_row_id metadata, matching
        how static writable tables report their schema.
        """
        conn = self._get_tx_conn(tx_id)
        tx_lock = self._get_tx_lock(tx_id)
        # Extract bare name for metadata queries (duckdb_tables, duckdb_columns use bare names)
        bare_name = table_name.rsplit(".", 1)[-1] if "." in table_name else table_name
        with tx_lock:
            sub = conn.subcursor()
            # Verify this is a table (not a view) — views don't have rowid and shouldn't
            # be returned as writable tables
            if "." in table_name:
                schema_part = table_name.rsplit(".", 1)[0]
                is_table = sub.execute(
                    "SELECT COUNT(*) FROM duckdb_tables() WHERE schema_name = ? AND table_name = ?",
                    [schema_part, bare_name],
                ).fetchone()[0] > 0
            else:
                is_table = sub.execute(
                    "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = ?",
                    [bare_name],
                ).fetchone()[0] > 0
            if not is_table:
                sub.close()
                raise ValueError(f"'{table_name}' is not a table")
            schema = sub.execute(f"SELECT * FROM {table_name} LIMIT 0").to_arrow_table().schema  # noqa: S608

            # Query column metadata (comments, defaults) via subcursor so we see
            # uncommitted catalog changes from the same transaction
            col_meta_result = sub.execute(
                "SELECT column_name, comment, column_default FROM duckdb_columns() WHERE table_name = ?",
                [bare_name],
            ).fetchall()
            sub.close()
            meta_updates: dict[str, dict[bytes, bytes]] = {}
            for row in col_meta_result:
                col_name, comment, default = row[0], row[1], row[2]
                updates: dict[bytes, bytes] = {}
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
                        metadata = dict(f.metadata) if f.metadata else {}
                        metadata.update(meta_updates[f.name])
                        fields[i] = f.with_metadata(metadata)
                schema = pa.schema(fields)

        # Query constraints and embed as schema-level metadata (JSON)
        with tx_lock:
            sub2 = conn.subcursor()
            constraint_rows = sub2.execute(
                "SELECT constraint_type, constraint_column_names, constraint_text, "
                "referenced_table, referenced_column_names "
                "FROM duckdb_constraints() WHERE table_name = ?",
                [bare_name],
            ).fetchall()
            sub2.close()

        import json
        constraints_json = json.dumps([
            {
                "type": row[0],
                "columns": row[1],
                "text": row[2],
                "referenced_table": row[3],
                "referenced_columns": row[4],
            }
            for row in constraint_rows
        ])
        schema_meta = {b"vgi.constraints": constraints_json.encode("utf-8")}

        # Prepend rowid pseudocolumn with is_row_id metadata
        rowid_field = pa.field("rowid", pa.int64(), metadata={b"is_row_id": b""})
        result_schema = pa.schema([rowid_field, *schema], metadata=schema_meta)
        return result_schema.serialize().to_pybytes()

    def table_comment(self, table_name: str, tx_id: bytes) -> str | None:
        """Get the comment on a table within a transaction."""
        conn = self._get_tx_conn(tx_id)
        tx_lock = self._get_tx_lock(tx_id)
        bare_name = table_name.rsplit(".", 1)[-1] if "." in table_name else table_name
        with tx_lock:
            result = conn.execute(
                "SELECT comment FROM duckdb_tables() WHERE table_name = ?",
                [bare_name],
            ).fetchone()
        if result and result[0]:
            return result[0]
        return None

    def list_user_views(self, tx_id: bytes, schema_name: str = "main") -> list[str]:
        """List user-created view names in the given schema within a transaction."""
        conn = self._get_tx_conn(tx_id)
        tx_lock = self._get_tx_lock(tx_id)
        with tx_lock:
            sub = conn.subcursor()
            result = sub.execute(
                "SELECT view_name FROM duckdb_views() WHERE schema_name = ? AND NOT internal",
                [schema_name],
            ).fetchall()
            sub.close()
        return [row[0] for row in result]

    def view_info(self, view_name: str, tx_id: bytes) -> str:
        """Return view info as JSON (definition, comment).

        The ``sql`` column from ``duckdb_views()`` returns the full
        ``CREATE VIEW ... AS SELECT ...`` statement. We strip the
        ``CREATE VIEW <name> AS`` prefix so the definition is just the
        SELECT statement, which is what the VGI catalog expects.
        """
        conn = self._get_tx_conn(tx_id)
        tx_lock = self._get_tx_lock(tx_id)
        with tx_lock:
            sub = conn.subcursor()
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
        # Strip "CREATE VIEW <name> AS " prefix to get just the SELECT
        match = re.match(r"CREATE\s+VIEW\s+\S+\s+AS\s+", definition, re.IGNORECASE)
        if match:
            definition = definition[match.end():]
            # Strip trailing semicolon if present
            definition = definition.rstrip().rstrip(";")
        return json.dumps({"definition": definition, "comment": result[1]})

    # ========== Lifecycle ==========

    def ping(self) -> None:
        """Health check."""

    def shutdown(self) -> None:
        """Graceful shutdown — rollback active transactions and close connections."""
        logger.info("Shutdown requested")
        with self._lock:
            for tx_id, conn in list(self._transactions.items()):
                try:
                    conn.rollback()
                    conn.close()
                    logger.info("Rolled back orphan transaction: %s", tx_id.hex())
                except Exception:
                    logger.exception("Failed to rollback transaction: %s", tx_id.hex())
            self._transactions.clear()
        self._conn.close()
        sys.exit(0)


# ============================================================================
# Stream state implementations
# ============================================================================


def _read_result_batch(result: duckdb.DuckDBPyConnection) -> pa.RecordBatch:
    """Read DML result as a single Arrow batch.

    Uses to_arrow_table() because to_arrow_reader() doesn't work for
    DML RETURNING on DuckDB cursors. Concatenates multiple batches if needed.
    """
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
        """Initialize insert state."""
        self.conn = conn
        self.qualified_name = qualified_name
        self.returning = returning
        self.table_schema = table_schema
        self.tx_lock = tx_lock

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Insert the input batch and return count or RETURNING rows."""
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
        """Initialize delete state."""
        self.conn = conn
        self.qualified_name = qualified_name
        self.returning = returning
        self.table_schema = table_schema
        self.tx_lock = tx_lock

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Delete rows by rowid."""
        with self.tx_lock:
            batch = input.batch
            view_name = _unique_batch_name("delete")

            self.conn.register(view_name, batch)
            if self.returning:
                # Workaround: DuckDB DELETE RETURNING returns empty for rows
                # inserted in the same transaction. SELECT first, then DELETE.
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
                self.conn.unregister(view_name)
            else:
                result = self.conn.execute(
                    f"DELETE FROM {self.qualified_name} "  # noqa: S608
                    f"USING {view_name} WHERE {self.qualified_name}.rowid = {view_name}.rowid",
                )
                self.conn.unregister(view_name)
                result_batch = _read_result_batch(result)

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
        """Initialize update state."""
        self.conn = conn
        self.qualified_name = qualified_name
        self.returning = returning
        self.table_schema = table_schema
        self.tx_lock = tx_lock

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Update rows by rowid with new column values."""
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
    """Scan producer: streams Arrow batches from a query result.

    The query is executed at init time and a RecordBatchReader is created.
    Each produce() call reads the next batch from the reader, enabling
    streaming without materializing the full result set.
    """

    def __init__(
        self,
        reader: pa.RecordBatchReader,
        tx_lock: threading.Lock,
    ) -> None:
        """Initialize with a pre-created Arrow reader."""
        self._reader = reader
        self._tx_lock = tx_lock

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Read the next batch from the Arrow reader.

        Skips zero-row batches internally to avoid returning without
        calling emit() or finish(), which would cause the protocol to spin.
        Holds the transaction lock to prevent concurrent subcursor operations.
        """
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
    parser = argparse.ArgumentParser(description="VGI db-transactor server")
    parser.add_argument("--db-path", required=True, help="Path to the DuckDB database file")
    parser.add_argument("--socket", required=True, help="Unix domain socket path to listen on")
    parser.add_argument("--log-file", default=None, help="Log file path (default: derived from socket path)")
    args = parser.parse_args()

    log_path = args.log_file or args.socket.replace(".sock", ".log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        filename=log_path,
    )

    impl = TransactorImpl(args.db_path)
    server = RpcServer(TransactorProtocol, impl)
    logger.info("Serving on %s", args.socket)
    serve_unix(server, args.socket, threaded=True)


if __name__ == "__main__":
    main()
