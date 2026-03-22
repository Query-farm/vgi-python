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
from vgi_rpc import OutputCollector, RpcServer
from vgi_rpc.rpc import CallContext, Stream, serve_unix

from vgi.transactor.protocol import (
    _COUNT_SCHEMA,
    DeleteExchangeState,
    InsertExchangeState,
    ScanProducerState,
    TransactorProtocol,
    UpdateExchangeState,
)

logger = logging.getLogger("vgi.transactor")


class TransactorImpl:
    """Implementation of the TransactorProtocol backed by a single DuckDB connection.

    All operations are serialized via a threading lock. Only one transaction
    can be active at a time (matching DuckDB's single-writer model).
    """

    def __init__(self, db_path: str) -> None:
        """Open DuckDB connection and initialize."""
        self._db_path = db_path
        self._conn = duckdb.connect(db_path)
        self._lock = threading.Lock()
        self._active_tx: bytes | None = None
        self._tx_condition = threading.Condition(self._lock)
        logger.info("Transactor started: %s", db_path)

    # ========== Transaction lifecycle ==========

    def begin(self, tx_id: bytes) -> None:
        """Begin a transaction."""
        with self._tx_condition:
            # Wait if another transaction is active
            while self._active_tx is not None:
                logger.debug("Waiting for active transaction to complete")
                self._tx_condition.wait()
            self._active_tx = tx_id
            self._conn.begin()
            logger.info("Transaction begun: %s", tx_id.hex())

    def commit(self, tx_id: bytes) -> None:
        """Commit the active transaction."""
        with self._tx_condition:
            if self._active_tx != tx_id:
                raise ValueError(f"Cannot commit: transaction {tx_id.hex()} is not active")
            self._conn.commit()
            self._active_tx = None
            self._tx_condition.notify_all()
            logger.info("Transaction committed: %s", tx_id.hex())

    def rollback(self, tx_id: bytes) -> None:
        """Rollback the active transaction."""
        with self._tx_condition:
            if self._active_tx != tx_id:
                raise ValueError(f"Cannot rollback: transaction {tx_id.hex()} is not active")
            self._conn.rollback()
            self._active_tx = None
            self._tx_condition.notify_all()
            logger.info("Transaction rolled back: %s", tx_id.hex())

    # ========== Write operations (streaming exchange) ==========

    def insert(
        self,
        tx_id: bytes,
        schema_name: str,
        table_name: str,
        returning: bool = False,
    ) -> Stream:
        """Create an insert exchange stream."""
        qualified = f"{schema_name}.{table_name}" if schema_name else table_name

        state = _InsertState(
            conn=self._conn,
            lock=self._lock,
            qualified_name=qualified,
            returning=returning,
        )
        # Input schema will be determined by the first batch
        return Stream(output_schema=_COUNT_SCHEMA, state=state)

    def delete(self, tx_id: bytes, schema_name: str, table_name: str) -> Stream:
        """Create a delete exchange stream."""
        qualified = f"{schema_name}.{table_name}" if schema_name else table_name

        state = _DeleteState(conn=self._conn, lock=self._lock, qualified_name=qualified)
        return Stream(output_schema=_COUNT_SCHEMA, state=state)

    def update(self, tx_id: bytes, schema_name: str, table_name: str) -> Stream:
        """Create an update exchange stream."""
        qualified = f"{schema_name}.{table_name}" if schema_name else table_name

        state = _UpdateState(conn=self._conn, lock=self._lock, qualified_name=qualified)
        return Stream(output_schema=_COUNT_SCHEMA, state=state)

    # ========== Read (streaming producer) ==========

    def scan(self, tx_id: bytes, schema_name: str, table_name: str, columns: list[str]) -> Stream:
        """Create a scan producer stream."""
        qualified = f"{schema_name}.{table_name}" if schema_name else table_name
        col_list = ", ".join(columns) if columns else "*"

        state = _ScanState(conn=self._conn, lock=self._lock, qualified_name=qualified, col_list=col_list)

        # Determine output schema by querying with LIMIT 0
        with self._lock:
            result = self._conn.execute(f"SELECT {col_list} FROM {qualified} LIMIT 0")  # noqa: S608
            output_schema = result.fetch_arrow_table().schema

        return Stream(output_schema=output_schema, state=state)

    # ========== DDL ==========

    def execute_ddl(self, sql: str) -> None:
        """Execute DDL statement."""
        with self._lock:
            self._conn.execute(sql)
            logger.debug("DDL executed: %s", sql[:100])

    # ========== Lifecycle ==========

    def ping(self) -> None:
        """Health check."""

    def shutdown(self) -> None:
        """Graceful shutdown."""
        logger.info("Shutdown requested")
        self._conn.close()
        sys.exit(0)


# ============================================================================
# Stream state implementations
# ============================================================================


class _InsertState(InsertExchangeState):
    """Insert exchange state that executes INSERT SQL per batch."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, lock: threading.Lock, qualified_name: str, returning: bool):
        """Initialize insert state."""
        self.conn = conn
        self.lock = lock
        self.qualified_name = qualified_name
        self.returning = returning
        self._output_schema: pa.Schema | None = None

    def exchange(self, input: pa.RecordBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Insert the input batch and return count or RETURNING rows."""
        columns = input.schema.names
        placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))
        col_names = ", ".join(columns)

        if self.returning:
            sql = f"INSERT INTO {self.qualified_name} ({col_names}) VALUES ({placeholders}) RETURNING *"  # noqa: S608
        else:
            sql = f"INSERT INTO {self.qualified_name} ({col_names}) VALUES ({placeholders})"  # noqa: S608

        total_count = 0
        returning_rows: list[tuple] = []

        with self.lock:
            for i in range(input.num_rows):
                params = [input.column(col)[i].as_py() for col in columns]
                result = self.conn.execute(sql, params)
                if self.returning:
                    row = result.fetchone()
                    if row:
                        returning_rows.append(row)
                        total_count += 1
                else:
                    total_count += 1

        if self.returning and returning_rows:
            # Build RETURNING batch from result rows
            if self._output_schema is None:
                # Determine schema from first RETURNING result
                with self.lock:
                    desc = (
                        self.conn.execute(
                            f"SELECT * FROM {self.qualified_name} LIMIT 0"  # noqa: S608
                        )
                        .fetch_arrow_table()
                        .schema
                    )
                    self._output_schema = desc

            arrays = {}
            for col_idx, field in enumerate(self._output_schema):
                arrays[field.name] = [row[col_idx] for row in returning_rows]
            batch = pa.record_batch(arrays, schema=self._output_schema)
            out.emit(batch)
        else:
            out.emit(pa.record_batch({"count": [total_count]}, schema=_COUNT_SCHEMA))


class _DeleteState(DeleteExchangeState):
    """Delete exchange state that executes DELETE SQL per batch of row_ids."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, lock: threading.Lock, qualified_name: str):
        """Initialize delete state."""
        self.conn = conn
        self.lock = lock
        self.qualified_name = qualified_name

    def exchange(self, input: pa.RecordBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Delete rows by row_id."""
        row_id_col = input.column("row_id")
        count = 0
        with self.lock:
            for i in range(input.num_rows):
                row_id = row_id_col[i].as_py()
                result = self.conn.execute(
                    f"DELETE FROM {self.qualified_name} WHERE row_id = $1 RETURNING row_id",  # noqa: S608
                    [row_id],
                )
                if result.fetchone() is not None:
                    count += 1
        out.emit(pa.record_batch({"count": [count]}, schema=_COUNT_SCHEMA))


class _UpdateState(UpdateExchangeState):
    """Update exchange state that executes UPDATE SQL per batch."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, lock: threading.Lock, qualified_name: str):
        """Initialize update state."""
        self.conn = conn
        self.lock = lock
        self.qualified_name = qualified_name

    def exchange(self, input: pa.RecordBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Update rows by row_id with new column values."""
        columns = [name for name in input.schema.names if name != "row_id"]
        count = 0

        with self.lock:
            for i in range(input.num_rows):
                row_id = input.column("row_id")[i].as_py()
                set_parts = []
                params: list = []
                for j, col in enumerate(columns):
                    set_parts.append(f"{col} = ${j + 1}")
                    params.append(input.column(col)[i].as_py())
                params.append(row_id)
                set_clause = ", ".join(set_parts)

                result = self.conn.execute(
                    f"UPDATE {self.qualified_name} SET {set_clause} WHERE row_id = ${len(params)} RETURNING row_id",  # noqa: S608
                    params,
                )
                if result.fetchone() is not None:
                    count += 1

        out.emit(pa.record_batch({"count": [count]}, schema=_COUNT_SCHEMA))


class _ScanState(ScanProducerState):
    """Scan producer state that queries all rows from a table."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, lock: threading.Lock, qualified_name: str, col_list: str):
        """Initialize scan state."""
        self.conn = conn
        self.lock = lock
        self.qualified_name = qualified_name
        self.col_list = col_list
        self._produced = False

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce all rows from the table."""
        if self._produced:
            out.finish()
            return

        self._produced = True
        with self.lock:
            result = self.conn.execute(f"SELECT {self.col_list} FROM {self.qualified_name}")  # noqa: S608
            table = result.fetch_arrow_table()

        if table.num_rows > 0:
            # Emit as a single batch
            out.emit(table.to_batches()[0] if table.num_rows <= 2048 else table.to_batches()[0])
            # For large tables, emit multiple batches
            for batch in table.to_batches()[1:]:
                out.emit(batch)

        out.finish()


def main() -> None:
    """Entry point for the vgi-transactor command."""
    parser = argparse.ArgumentParser(description="VGI db-transactor server")
    parser.add_argument("--db-path", required=True, help="Path to the DuckDB database file")
    parser.add_argument("--socket", required=True, help="Unix domain socket path to listen on")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    impl = TransactorImpl(args.db_path)
    server = RpcServer(TransactorProtocol, impl)
    logger.info("Serving on %s", args.socket)
    serve_unix(server, args.socket, threaded=True)


if __name__ == "__main__":
    main()
