"""TransactorProtocol — RPC interface for the db-transactor.

Defines the Protocol class that both the server and client use.
Uses ``vgi_rpc`` streaming patterns:

- **Exchange streams** (table-in-out) for INSERT/UPDATE/DELETE
- **Producer streams** (table function) for SELECT/SCAN
- **Unary calls** for transaction lifecycle and DDL

"""

from __future__ import annotations

from typing import Protocol

import pyarrow as pa
from vgi_rpc.rpc import ExchangeState, ProducerState, Stream


class TransactorProtocol(Protocol):
    """RPC interface for the db-transactor subprocess."""

    # ========== Transaction lifecycle (unary) ==========

    def begin(self, tx_id: bytes) -> None:
        """Begin a transaction. Opens a DuckDB transaction internally."""
        ...

    def commit(self, tx_id: bytes) -> None:
        """Commit a transaction."""
        ...

    def rollback(self, tx_id: bytes) -> None:
        """Rollback a transaction."""
        ...

    # ========== Write operations (streaming exchange) ==========

    def insert(
        self,
        tx_id: bytes,
        table_name: str,
        schema_name: str = "",
        returning: bool = False,
    ) -> Stream[ExchangeState]:
        """Insert rows into a table via lockstep exchange.

        Client sends Arrow RecordBatches of rows to insert.
        Server responds with count batches (or inserted rows if returning=True).
        """
        ...

    def delete(
        self,
        tx_id: bytes,
        table_name: str,
        schema_name: str = "",
        returning: bool = False,
    ) -> Stream[ExchangeState]:
        """Delete rows from a table via lockstep exchange.

        Client sends Arrow RecordBatches with row_id column.
        Server responds with count or RETURNING batches.
        """
        ...

    def update(
        self,
        tx_id: bytes,
        table_name: str,
        schema_name: str = "",
        columns: list[str] | None = None,
        returning: bool = False,
    ) -> Stream[ExchangeState]:
        """Update rows in a table via lockstep exchange.

        Client sends Arrow RecordBatches with row_id + updated columns.
        Server responds with count or RETURNING batches.
        """
        ...

    # ========== Read (streaming producer) ==========

    def scan(
        self,
        tx_id: bytes,
        table_name: str,
        columns: list[str],
        schema_name: str = "",
        pushdown_filters: bytes | None = None,
    ) -> Stream[ProducerState]:
        """Scan rows from a table with optional predicate pushdown.

        Server produces Arrow RecordBatches with the requested columns.
        ``pushdown_filters`` is a serialized filter RecordBatch (same format
        as VGI's ``InitRequest.pushdown_filters``), deserialized and converted
        to a SQL WHERE clause by the transactor.
        """
        ...

    # ========== DDL (unary) ==========

    def execute_ddl(self, sql: str) -> None:
        """Execute a DDL statement (CREATE TABLE, etc.)."""
        ...

    def execute_ddl_tx(self, tx_id: bytes, sql: str, strip_catalog: str | None = None) -> None:
        """Execute DDL within a transaction.

        If ``strip_catalog`` is provided, external catalog references are
        stripped from the SQL before execution (used for view definitions).
        """
        ...

    # ========== Metadata (unary) ==========

    def list_schemas(self, tx_id: bytes) -> list[str]:
        """List schema names within a transaction."""
        ...

    def list_user_tables(self, tx_id: bytes, schema_name: str = "main") -> list[str]:
        """List user-created table names in the given schema within a transaction."""
        ...

    def table_schema(self, table_name: str, tx_id: bytes) -> bytes:
        """Get Arrow schema for a table as serialized IPC bytes, with rowid prepended and marked via is_row_id metadata."""
        ...

    def table_comment(self, table_name: str, tx_id: bytes) -> str | None:
        """Get the comment on a table, or None if no comment is set."""
        ...

    def list_user_views(self, tx_id: bytes, schema_name: str = "main") -> list[str]:
        """List user-created view names in the given schema within a transaction."""
        ...

    def view_info(self, view_name: str, tx_id: bytes) -> str:
        """Get view info as JSON (definition, comment)."""
        ...

    # ========== Lifecycle (unary) ==========

    def ping(self) -> None:
        """Health check."""
        ...

    def shutdown(self) -> None:
        """Graceful shutdown."""
        ...
