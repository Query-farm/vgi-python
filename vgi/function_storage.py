"""Storage for VGI function state.

This module provides a storage protocol and implementation for sharing state
across worker processes in distributed VGI function execution.

Protocol:
    FunctionStorage: Unified protocol for all VGI state storage needs.

Implementation:
    FunctionStorageSqlite: SQLite-backed storage implementation.

"""

import random
import sqlite3
import uuid
from typing import Protocol

__all__ = [
    "FunctionStorage",
    "FunctionStorageSqlite",
    "UnknownInvocationError",
]


class UnknownInvocationError(Exception):
    """Raised when a queue operation references an unknown invocation ID.

    This error indicates that a client is attempting to interact with a queue
    for an invocation that was never registered (via queue_push) or has already
    been cleared (via queue_clear).
    """


def _get_default_db_path() -> str:
    """Return the default SQLite database path for VGI storage."""
    from pathlib import Path

    from platformdirs import user_state_dir

    state_dir = Path(user_state_dir("vgi"))
    state_dir.mkdir(parents=True, exist_ok=True)
    return str((state_dir / "vgi_storage.db").resolve())


class FunctionStorage(Protocol):
    """Storage protocol for VGI distributed function execution.

    Provides three access patterns for distributed function state:

    **Global State** - Initialization data shared across all workers.
    Primary worker stores data and receives a unique key, secondary workers
    retrieve data using that key.

    **Worker State** - Partial results from each worker process.
    Each worker stores its state during processing, primary worker collects
    all states during finalization for aggregation.

    **Work Queue** - Atomic work distribution across workers.
    Primary worker enqueues work items, workers atomically claim items
    from the queue for processing.

    """

    # --- Global State (init data shared by all workers) ---

    def global_put(self, value: bytes) -> bytes:
        """Store global value and return unique key.

        Args:
            value: Serialized value bytes.

        Returns:
            Unique key for retrieving the value.

        """
        ...

    def global_get(self, key: bytes) -> bytes:
        """Retrieve global value by key.

        Args:
            key: Key returned from global_put().

        Returns:
            The stored value bytes.

        Raises:
            KeyError: If no value exists for this key.

        """
        ...

    def global_delete(self, key: bytes) -> None:
        """Delete global value by key."""
        ...

    def global_exists(self, key: bytes) -> bool:
        """Check if global key exists."""
        ...

    # --- Worker State (per-worker partial results) ---

    def worker_put(self, invocation_id: bytes, worker_id: int, state: bytes) -> None:
        """Store state for a specific worker.

        If state already exists for this (invocation_id, worker_id) pair,
        it will be replaced.

        Args:
            invocation_id: Unique identifier for the function invocation.
            worker_id: Process ID of the worker storing the state.
            state: Serialized state bytes.

        """
        ...

    def worker_collect(self, invocation_id: bytes) -> list[bytes]:
        """Atomically collect and delete all worker states.

        This is typically called by the primary worker during finalization
        to collect all worker states for aggregation.

        Args:
            invocation_id: Unique identifier for the function invocation.

        Returns:
            List of serialized state bytes from all workers.

        """
        ...

    # --- Work Queue (distributed work items) ---

    def queue_push(self, invocation_id: bytes, items: list[bytes]) -> int:
        """Add work items to the queue and register the invocation.

        This method registers the invocation_id as valid, allowing subsequent
        queue_pop calls. Even if items is empty, the invocation is registered.

        Args:
            invocation_id: Unique identifier for the function invocation.
            items: List of serialized work item bytes.

        Returns:
            Number of items added.

        """
        ...

    def queue_pop(self, invocation_id: bytes) -> bytes | None:
        """Atomically claim one work item from the queue.

        The invocation_id must have been previously registered via queue_push.
        This allows detection of badly coded clients or clients attempting to
        interact after their executions have been completed.

        Args:
            invocation_id: Unique identifier for the function invocation.

        Returns:
            Serialized work item bytes, or None if queue is empty.

        Raises:
            UnknownInvocationError: If the invocation_id was never registered
                via queue_push or has been cleared via queue_clear.

        """
        ...

    def queue_clear(self, invocation_id: bytes) -> int:
        """Clear all remaining work items and unregister the invocation.

        After calling this method, subsequent queue_pop calls for this
        invocation_id will raise UnknownInvocationError.

        Args:
            invocation_id: Unique identifier for the function invocation.

        Returns:
            Number of items deleted.

        """
        ...


class FunctionStorageSqlite:
    """SQLite-backed storage for VGI function state.

    This implementation uses SQLite with WAL mode to allow multiple worker
    processes to share state. It manages three tables:

    - global_state_storage: Key-value store for init data
    - worker_state: Per-worker partial state keyed by (invocation_id, worker_id)
    - work_queue: FIFO queue of work items per invocation

    """

    def __init__(self, db_path: str | None = None) -> None:
        """Initialize SQLite storage.

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
            # Global state table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS global_state_storage (
                    key BLOB PRIMARY KEY,
                    value BLOB NOT NULL,
                    created_at REAL DEFAULT (julianday('now'))
                )
            """)
            # Worker state table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS worker_state (
                    invocation_id BLOB NOT NULL,
                    process_id INTEGER NOT NULL,
                    state_data BLOB NOT NULL,
                    created_at REAL DEFAULT (julianday('now')),
                    PRIMARY KEY (invocation_id, process_id)
                )
            """)
            # Work queue table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS work_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    invocation_id BLOB NOT NULL,
                    work_item BLOB NOT NULL,
                    created_at REAL DEFAULT (julianday('now'))
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_work_queue_invocation
                ON work_queue(invocation_id)
            """)
            # Invocation registry - tracks valid invocation IDs for queue operations
            conn.execute("""
                CREATE TABLE IF NOT EXISTS invocation_registry (
                    invocation_id BLOB PRIMARY KEY,
                    created_at REAL DEFAULT (julianday('now'))
                )
            """)
            conn.commit()
        finally:
            conn.close()

    # --- Global State ---

    def global_put(self, value: bytes) -> bytes:
        """Store global value and return unique key."""
        # Opportunistically clean old entries (1% of calls)
        if random.random() < 0.01:
            self.cleanup_old_entries(max_age_days=1.0)

        key = uuid.uuid4().bytes

        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO global_state_storage (key, value) VALUES (?, ?)",
                (key, value),
            )
            conn.commit()
        finally:
            conn.close()

        return key

    def global_get(self, key: bytes) -> bytes:
        """Retrieve global value by key."""
        conn = self._connect()
        try:
            cursor = conn.execute(
                "SELECT value FROM global_state_storage WHERE key = ?",
                (key,),
            )
            row = cursor.fetchone()
        finally:
            conn.close()

        if row is None:
            raise KeyError(f"Key {key.hex()} not found in FunctionStorageSqlite")

        result: bytes = row[0]
        return result

    def global_delete(self, key: bytes) -> None:
        """Delete global value by key."""
        conn = self._connect()
        try:
            conn.execute("DELETE FROM global_state_storage WHERE key = ?", (key,))
            conn.commit()
        finally:
            conn.close()

    def global_exists(self, key: bytes) -> bool:
        """Check if global key exists."""
        conn = self._connect()
        try:
            cursor = conn.execute(
                "SELECT 1 FROM global_state_storage WHERE key = ?",
                (key,),
            )
            return cursor.fetchone() is not None
        finally:
            conn.close()

    # --- Worker State ---

    def worker_put(self, invocation_id: bytes, worker_id: int, state: bytes) -> None:
        """Store state for a specific worker."""
        # Opportunistically clean old entries (1% of calls)
        if random.random() < 0.01:
            self.cleanup_old_entries(max_age_days=1.0)

        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO worker_state
                (invocation_id, process_id, state_data, created_at)
                VALUES (?, ?, ?, julianday('now'))
                """,
                (invocation_id, worker_id, state),
            )
            conn.commit()
        finally:
            conn.close()

    def worker_collect(self, invocation_id: bytes) -> list[bytes]:
        """Atomically collect and delete all worker states."""
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                DELETE FROM worker_state
                WHERE invocation_id = ?
                RETURNING state_data
                """,
                (invocation_id,),
            )
            states = [row[0] for row in cursor.fetchall()]
            conn.commit()
            return states
        finally:
            conn.close()

    # --- Work Queue ---

    def queue_push(self, invocation_id: bytes, items: list[bytes]) -> int:
        """Add work items to the queue and register the invocation."""
        conn = self._connect()
        try:
            # Register the invocation_id (idempotent)
            conn.execute(
                "INSERT OR IGNORE INTO invocation_registry (invocation_id) VALUES (?)",
                (invocation_id,),
            )
            # Add work items if any
            if items:
                conn.executemany(
                    """
                    INSERT INTO work_queue (invocation_id, work_item)
                    VALUES (?, ?)
                    """,
                    [(invocation_id, item) for item in items],
                )
            conn.commit()
            return len(items)
        finally:
            conn.close()

    def queue_pop(self, invocation_id: bytes) -> bytes | None:
        """Atomically claim one work item from the queue.

        Raises:
            UnknownInvocationError: If invocation_id was never registered via
                queue_push or has been cleared via queue_clear.

        """
        conn = self._connect()
        try:
            # Check if invocation is registered
            reg_cursor = conn.execute(
                "SELECT 1 FROM invocation_registry WHERE invocation_id = ?",
                (invocation_id,),
            )
            if reg_cursor.fetchone() is None:
                raise UnknownInvocationError(
                    f"Invocation {invocation_id.hex()} is not registered. "
                    "Call queue_push first to register the invocation."
                )

            cursor = conn.execute(
                """
                DELETE FROM work_queue
                WHERE id = (
                    SELECT id FROM work_queue
                    WHERE invocation_id = ?
                    LIMIT 1
                )
                RETURNING work_item
                """,
                (invocation_id,),
            )
            row = cursor.fetchone()
            conn.commit()
            return row[0] if row else None
        finally:
            conn.close()

    def queue_clear(self, invocation_id: bytes) -> int:
        """Clear all remaining work items and unregister the invocation."""
        conn = self._connect()
        try:
            cursor = conn.execute(
                "DELETE FROM work_queue WHERE invocation_id = ?",
                (invocation_id,),
            )
            # Unregister the invocation
            conn.execute(
                "DELETE FROM invocation_registry WHERE invocation_id = ?",
                (invocation_id,),
            )
            conn.commit()
            return cursor.rowcount
        finally:
            conn.close()

    # --- Maintenance (not part of protocol) ---

    def cleanup_old_entries(self, max_age_days: float = 1.0) -> int:
        """Remove entries older than the specified age from all tables.

        Args:
            max_age_days: Maximum age in days for entries to keep.

        Returns:
            Total number of entries deleted.

        """
        conn = self._connect()
        try:
            cursor1 = conn.execute(
                """
                DELETE FROM global_state_storage
                WHERE julianday('now') - created_at > ?
                """,
                (max_age_days,),
            )
            cursor2 = conn.execute(
                """
                DELETE FROM worker_state
                WHERE julianday('now') - created_at > ?
                """,
                (max_age_days,),
            )
            cursor3 = conn.execute(
                """
                DELETE FROM work_queue
                WHERE julianday('now') - created_at > ?
                """,
                (max_age_days,),
            )
            cursor4 = conn.execute(
                """
                DELETE FROM invocation_registry
                WHERE julianday('now') - created_at > ?
                """,
                (max_age_days,),
            )
            conn.commit()
            return (
                int(cursor1.rowcount)
                + int(cursor2.rowcount)
                + int(cursor3.rowcount)
                + int(cursor4.rowcount)
            )
        finally:
            conn.close()
