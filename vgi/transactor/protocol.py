"""TransactorProtocol — RPC interface for the db-transactor.

Defines the Protocol class that both the server and client use.
Uses ``vgi_rpc`` streaming patterns:

- **Exchange streams** (table-in-out) for INSERT/UPDATE/DELETE
- **Producer streams** (table function) for SELECT/SCAN
- **Unary calls** for transaction lifecycle and DDL

"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

import pyarrow as pa
from vgi_rpc import ExchangeState, OutputCollector, ProducerState
from vgi_rpc.rpc import CallContext, Stream


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
        schema_name: str,
        table_name: str,
        returning: bool = False,
    ) -> Stream:
        """Insert rows into a table via lockstep exchange.

        Client sends Arrow RecordBatches of rows to insert.
        Server responds with count batches (or inserted rows if returning=True).
        """
        ...

    def delete(
        self,
        tx_id: bytes,
        schema_name: str,
        table_name: str,
    ) -> Stream:
        """Delete rows from a table via lockstep exchange.

        Client sends Arrow RecordBatches with row_id column.
        Server responds with count batches.
        """
        ...

    def update(
        self,
        tx_id: bytes,
        schema_name: str,
        table_name: str,
        columns: list[str] | None = None,
    ) -> Stream:
        """Update rows in a table via lockstep exchange.

        Client sends Arrow RecordBatches with row_id + updated columns.
        Server responds with count batches.
        """
        ...

    # ========== Read (streaming producer) ==========

    def scan(
        self,
        tx_id: bytes,
        schema_name: str,
        table_name: str,
        columns: list[str],
    ) -> Stream:
        """Scan all rows from a table.

        Server produces Arrow RecordBatches with the requested columns.
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


# ============================================================================
# Stream state classes for the transactor server
# ============================================================================

_COUNT_SCHEMA = pa.schema([("count", pa.int64())])


@dataclass
class InsertExchangeState(ExchangeState):
    """Exchange state for INSERT operations on the transactor."""

    _conn: object = field(repr=False)  # DuckDB connection
    _schema_name: str = ""
    _table_name: str = ""
    _returning: bool = False

    def exchange(self, input: pa.RecordBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Insert the input batch into the table."""
        raise NotImplementedError("Implemented in server.py")


@dataclass
class DeleteExchangeState(ExchangeState):
    """Exchange state for DELETE operations on the transactor."""

    _conn: object = field(repr=False)
    _schema_name: str = ""
    _table_name: str = ""

    def exchange(self, input: pa.RecordBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Delete rows identified by row_id in the input batch."""
        raise NotImplementedError("Implemented in server.py")


@dataclass
class UpdateExchangeState(ExchangeState):
    """Exchange state for UPDATE operations on the transactor."""

    _conn: object = field(repr=False)
    _schema_name: str = ""
    _table_name: str = ""

    def exchange(self, input: pa.RecordBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Update rows identified by row_id with new column values."""
        raise NotImplementedError("Implemented in server.py")


@dataclass
class ScanProducerState(ProducerState):
    """Producer state for SCAN operations on the transactor."""

    _conn: object = field(repr=False)
    _schema_name: str = ""
    _table_name: str = ""
    _columns: list[str] = field(default_factory=list)
    _produced: bool = False

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce all rows from the table."""
        raise NotImplementedError("Implemented in server.py")
