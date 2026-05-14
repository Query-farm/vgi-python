"""TransactorProtocol — RPC interface for the db-transactor.

Defines the Protocol class that both the server and client use.
Uses ``vgi_rpc`` streaming patterns:

- **Exchange streams** (table-in-out) for INSERT/UPDATE/DELETE
- **Producer streams** (table function) for SELECT/SCAN
- **Unary calls** for transaction lifecycle and DDL

All methods that operate on a database take ``attach_opaque_data`` to identify
which database to use. The transactor manages multiple databases, one
per catalog attachment.
"""

from __future__ import annotations

from typing import Protocol

from vgi_rpc.rpc import ExchangeState, ProducerState, Stream


class TransactorProtocol(Protocol):
    """RPC interface for the db-transactor subprocess."""

    # ========== Database lifecycle (unary) ==========

    def register(
        self, attach_opaque_data: bytes, catalog_name: str = "", ddl_statements: list[str] | None = None
    ) -> None:
        """Register a new database for this attach_opaque_data and run initial DDL."""
        ...

    def catalog_version(self, attach_opaque_data: bytes) -> int:
        """Return the catalog version for the database (incremented on DDL)."""
        ...

    # ========== Transaction lifecycle (unary) ==========

    def begin(self, attach_opaque_data: bytes) -> bytes:
        """Begin a transaction. Returns the transactor-generated tx_id."""
        ...

    def commit(self, attach_opaque_data: bytes, tx_id: bytes) -> None:
        """Commit a transaction."""
        ...

    def rollback(self, attach_opaque_data: bytes, tx_id: bytes) -> None:
        """Rollback a transaction."""
        ...

    # ========== Write operations (streaming exchange) ==========

    def insert(
        self,
        attach_opaque_data: bytes,
        tx_id: bytes,
        table_name: str,
        schema_name: str = "",
        returning: bool = False,
    ) -> Stream[ExchangeState]:
        """Insert rows into a table via lockstep exchange."""
        ...

    def delete(
        self,
        attach_opaque_data: bytes,
        tx_id: bytes,
        table_name: str,
        schema_name: str = "",
        returning: bool = False,
    ) -> Stream[ExchangeState]:
        """Delete rows from a table via lockstep exchange."""
        ...

    def update(
        self,
        attach_opaque_data: bytes,
        tx_id: bytes,
        table_name: str,
        schema_name: str = "",
        columns: list[str] | None = None,
        returning: bool = False,
    ) -> Stream[ExchangeState]:
        """Update rows in a table via lockstep exchange."""
        ...

    # ========== Read (streaming producer) ==========

    def scan(
        self,
        attach_opaque_data: bytes,
        tx_id: bytes,
        table_name: str,
        columns: list[str],
        schema_name: str = "",
        pushdown_filters: bytes | None = None,
    ) -> Stream[ProducerState]:
        """Scan rows from a table with optional predicate pushdown."""
        ...

    # ========== DDL (unary) ==========

    def execute_ddl(self, attach_opaque_data: bytes, sql: str) -> None:
        """Execute a DDL statement on the database (non-transactional)."""
        ...

    def execute_ddl_tx(
        self, attach_opaque_data: bytes, tx_id: bytes, sql: str, strip_catalog: str | None = None
    ) -> None:
        """Execute DDL within a transaction."""
        ...

    # ========== Metadata (unary) ==========

    def list_schemas(self, attach_opaque_data: bytes, tx_id: bytes) -> list[str]:
        """List schema names within a transaction."""
        ...

    def list_user_tables(self, attach_opaque_data: bytes, tx_id: bytes, schema_name: str = "main") -> list[str]:
        """List user-created table names in the given schema within a transaction."""
        ...

    def table_schema(self, attach_opaque_data: bytes, table_name: str, tx_id: bytes) -> bytes:
        """Get Arrow schema for a table as serialized IPC bytes."""
        ...

    def table_comment(self, attach_opaque_data: bytes, table_name: str, tx_id: bytes) -> str | None:
        """Get the comment on a table, or None if no comment is set."""
        ...

    def list_user_views(self, attach_opaque_data: bytes, tx_id: bytes, schema_name: str = "main") -> list[str]:
        """List user-created view names in the given schema within a transaction."""
        ...

    def view_info(self, attach_opaque_data: bytes, view_name: str, tx_id: bytes) -> str:
        """Get view info as JSON (definition, comment)."""
        ...

    # ========== Lifecycle (unary) ==========

    def ping(self) -> None:
        """Health check."""
        ...

    def shutdown(self) -> None:
        """Graceful shutdown."""
        ...
