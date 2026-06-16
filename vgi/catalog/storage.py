# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Storage for VGI catalog state.

This module provides a storage protocol and implementation for persisting
catalog attach_opaque_data and transaction_opaque_data state across worker processes.

Protocol:
    [`CatalogStorage`][]: Protocol for catalog state persistence.

Implementation:
    [`CatalogStorageSqlite`][]: SQLite-backed storage implementation.

"""

import random
import sqlite3
import uuid
from typing import Any, Protocol

from vgi.catalog.catalog_interface import AttachOpaqueData, TransactionOpaqueData

__all__ = [
    "CatalogStorage",
    "CatalogStorageSqlite",
]


def _get_default_db_path() -> str:
    """Return the default SQLite database path for catalog storage."""
    from pathlib import Path

    from platformdirs import user_state_dir

    state_dir = Path(user_state_dir("vgi"))
    state_dir.mkdir(parents=True, exist_ok=True)
    return str((state_dir / "vgi_catalog.db").resolve())


class CatalogStorage(Protocol):
    """Storage protocol for VGI catalog state persistence.

    Provides two access patterns for catalog state:

    **Attachments** - Track catalog attachments with their options.
    Stores the mapping from attach_opaque_data to catalog name and options.

    **Transactions** - Track active transactions.
    Stores transaction state for catalogs that support transactions.

    """

    # --- Attachment State ---

    def attach_put(self, attach_opaque_data: AttachOpaqueData, catalog_name: str, options: dict[str, Any]) -> None:
        """Store attachment state.

        Args:
            attach_opaque_data: Unique identifier for the attachment.
            catalog_name: Name of the attached catalog.
            options: Options passed during attachment.

        """
        ...

    def attach_get(self, attach_opaque_data: AttachOpaqueData) -> tuple[str, dict[str, Any]] | None:
        """Retrieve attachment state by attach_opaque_data.

        Args:
            attach_opaque_data: Unique identifier for the attachment.

        Returns:
            Tuple of (catalog_name, options), or None if not found.

        """
        ...

    def attach_delete(self, attach_opaque_data: AttachOpaqueData) -> None:
        """Delete attachment state.

        Args:
            attach_opaque_data: Unique identifier for the attachment.

        """
        ...

    def attach_list(self) -> list[AttachOpaqueData]:
        """List all active attachments.

        Returns:
            List of all attach opaque data values in storage.

        """
        ...

    # --- Transaction State ---

    def transaction_put(
        self, transaction_opaque_data: TransactionOpaqueData, attach_opaque_data: AttachOpaqueData, state: bytes
    ) -> None:
        """Store transaction state.

        Args:
            transaction_opaque_data: Unique identifier for the transaction.
            attach_opaque_data: Attachment the transaction belongs to.
            state: Serialized transaction state.

        """
        ...

    def transaction_get(self, transaction_opaque_data: TransactionOpaqueData) -> tuple[AttachOpaqueData, bytes] | None:
        """Retrieve transaction state.

        Args:
            transaction_opaque_data: Unique identifier for the transaction.

        Returns:
            Tuple of (attach_opaque_data, state bytes), or None if not found.

        """
        ...

    def transaction_delete(self, transaction_opaque_data: TransactionOpaqueData) -> None:
        """Delete transaction state.

        Args:
            transaction_opaque_data: Unique identifier for the transaction.

        """
        ...


class CatalogStorageSqlite:
    """SQLite-backed storage for VGI catalog state.

    This implementation uses SQLite with WAL mode to allow multiple worker
    processes to share catalog state. It manages two tables:

    - catalog_attachments: Maps attach_opaque_data to catalog name and options
    - catalog_transactions: Tracks active transactions

    Attributes:
        db_path: Filesystem path to the backing SQLite database file.
    """

    db_path: str

    def __init__(self, db_path: str | None = None) -> None:
        """Initialize SQLite catalog storage.

        Args:
            db_path: Path to the SQLite database file. If None, uses a default
                location in the user's state directory.

        """
        self.db_path = db_path if db_path is not None else _get_default_db_path()
        self._ensure_tables()

    def _connect(self) -> sqlite3.Connection:
        """Create a new database connection."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _ensure_tables(self) -> None:
        """Create all storage tables if they don't exist."""
        conn = self._connect()
        try:
            # Attachment table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS catalog_attachments (
                    attach_opaque_data BLOB PRIMARY KEY,
                    catalog_name TEXT NOT NULL,
                    options_json TEXT NOT NULL,
                    created_at REAL DEFAULT (julianday('now'))
                )
            """)
            # Transaction table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS catalog_transactions (
                    transaction_opaque_data BLOB PRIMARY KEY,
                    attach_opaque_data BLOB NOT NULL,
                    state_data BLOB NOT NULL,
                    created_at REAL DEFAULT (julianday('now')),
                    FOREIGN KEY (attach_opaque_data) REFERENCES catalog_attachments(attach_opaque_data)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_transactions_attach
                ON catalog_transactions(attach_opaque_data)
            """)
            conn.commit()
        finally:
            conn.close()

    # --- Attachment State ---

    def attach_put(self, attach_opaque_data: AttachOpaqueData, catalog_name: str, options: dict[str, Any]) -> None:
        """Store attachment state."""
        import json

        # Opportunistically clean old entries (1% of calls)
        if random.random() < 0.01:
            self.cleanup_old_entries(max_age_days=7.0)

        options_json = json.dumps(options)

        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO catalog_attachments
                (attach_opaque_data, catalog_name, options_json, created_at)
                VALUES (?, ?, ?, julianday('now'))
                """,
                (attach_opaque_data, catalog_name, options_json),
            )
            conn.commit()
        finally:
            conn.close()

    def attach_get(self, attach_opaque_data: AttachOpaqueData) -> tuple[str, dict[str, Any]] | None:
        """Retrieve attachment state by attach_opaque_data."""
        import json

        conn = self._connect()
        try:
            cursor = conn.execute(
                """SELECT catalog_name, options_json
                FROM catalog_attachments WHERE attach_opaque_data = ?""",
                (attach_opaque_data,),
            )
            row = cursor.fetchone()
        finally:
            conn.close()

        if row is None:
            return None

        catalog_name: str = row[0]
        options: dict[str, Any] = json.loads(row[1])
        return (catalog_name, options)

    def attach_delete(self, attach_opaque_data: AttachOpaqueData) -> None:
        """Delete attachment state."""
        conn = self._connect()
        try:
            # Delete associated transactions first
            conn.execute(
                "DELETE FROM catalog_transactions WHERE attach_opaque_data = ?",
                (attach_opaque_data,),
            )
            conn.execute(
                "DELETE FROM catalog_attachments WHERE attach_opaque_data = ?",
                (attach_opaque_data,),
            )
            conn.commit()
        finally:
            conn.close()

    def attach_list(self) -> list[AttachOpaqueData]:
        """List all active attachment IDs."""
        conn = self._connect()
        try:
            cursor = conn.execute("SELECT attach_opaque_data FROM catalog_attachments")
            return [AttachOpaqueData(row[0]) for row in cursor.fetchall()]
        finally:
            conn.close()

    # --- Transaction State ---

    def transaction_put(
        self, transaction_opaque_data: TransactionOpaqueData, attach_opaque_data: AttachOpaqueData, state: bytes
    ) -> None:
        """Store transaction state."""
        # Opportunistically clean old entries (1% of calls)
        if random.random() < 0.01:
            self.cleanup_old_entries(max_age_days=7.0)

        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO catalog_transactions
                (transaction_opaque_data, attach_opaque_data, state_data, created_at)
                VALUES (?, ?, ?, julianday('now'))
                """,
                (transaction_opaque_data, attach_opaque_data, state),
            )
            conn.commit()
        finally:
            conn.close()

    def transaction_get(self, transaction_opaque_data: TransactionOpaqueData) -> tuple[AttachOpaqueData, bytes] | None:
        """Retrieve transaction state."""
        conn = self._connect()
        try:
            cursor = conn.execute(
                """SELECT attach_opaque_data, state_data
                FROM catalog_transactions WHERE transaction_opaque_data = ?""",
                (transaction_opaque_data,),
            )
            row = cursor.fetchone()
        finally:
            conn.close()

        if row is None:
            return None

        return (AttachOpaqueData(row[0]), row[1])

    def transaction_delete(self, transaction_opaque_data: TransactionOpaqueData) -> None:
        """Delete transaction state."""
        conn = self._connect()
        try:
            conn.execute(
                "DELETE FROM catalog_transactions WHERE transaction_opaque_data = ?",
                (transaction_opaque_data,),
            )
            conn.commit()
        finally:
            conn.close()

    # --- Utility Methods ---

    def generate_attach_opaque_data(self) -> AttachOpaqueData:
        """Generate a new unique attach_opaque_data.

        Returns:
            A new `AttachOpaqueData` based on UUID4.

        """
        return AttachOpaqueData(uuid.uuid4().bytes)

    def generate_transaction_opaque_data(self) -> TransactionOpaqueData:
        """Generate a new unique transaction_opaque_data.

        Returns:
            A new `TransactionOpaqueData` based on UUID4.

        """
        return TransactionOpaqueData(uuid.uuid4().bytes)

    # --- Maintenance ---

    def cleanup_old_entries(self, max_age_days: float = 7.0) -> int:
        """Remove entries older than the specified age from all tables.

        Args:
            max_age_days: Maximum age in days for entries to keep.

        Returns:
            Total number of entries deleted.

        """
        conn = self._connect()
        try:
            # Delete old transactions first (foreign key constraint)
            cursor1 = conn.execute(
                """
                DELETE FROM catalog_transactions
                WHERE julianday('now') - created_at > ?
                """,
                (max_age_days,),
            )
            cursor2 = conn.execute(
                """
                DELETE FROM catalog_attachments
                WHERE julianday('now') - created_at > ?
                """,
                (max_age_days,),
            )
            conn.commit()
            return int(cursor1.rowcount) + int(cursor2.rowcount)
        finally:
            conn.close()
