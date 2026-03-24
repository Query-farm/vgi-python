"""TransactorProtocol — RPC interface for the db-transactor.

Defines the Protocol class that both the server and client use.
Uses ``vgi_rpc`` streaming patterns:

- **Exchange streams** (table-in-out) for INSERT/UPDATE/DELETE
- **Producer streams** (table function) for SELECT/SCAN
- **Unary calls** for transaction lifecycle and DDL

"""

from __future__ import annotations

from typing import Protocol

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

    # ========== Lifecycle (unary) ==========

    def ping(self) -> None:
        """Health check."""
        ...

    def shutdown(self) -> None:
        """Graceful shutdown."""
        ...
