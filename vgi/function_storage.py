"""SQLite-backed storage for VGI function state.

This module provides storage implementations for sharing state across
worker processes in distributed VGI function execution.

Classes:
    SqliteInitStorage: Storage for initialization data shared across processes.
    SqliteWorkerStateStorage: Storage for worker state in distributed functions.

"""

import random
import sqlite3
import uuid

__all__ = [
    "SqliteInitStorage",
    "SqliteWorkerStateStorage",
]


def _get_default_db_path() -> str:
    """Return the default SQLite database path for VGI storage."""
    from pathlib import Path

    from platformdirs import user_state_dir

    state_dir = Path(user_state_dir("vgi"))
    state_dir.mkdir(parents=True, exist_ok=True)
    return str((state_dir / "vgi_storage.db").resolve())


class SqliteInitStorage:
    """SQLite-backed storage for init values shared across processes.

    This storage implementation uses SQLite with a well-known file location
    to allow multiple worker processes to share initialization state. This
    is necessary for distributed/parallel execution where workers run in
    separate subprocesses.

    The storage uses bytes-in-bytes-out, delegating serialization to callers.

    """

    def __init__(self, db_path: str | None = None) -> None:
        """Initialize SQLite storage.

        Args:
            db_path: Path to the SQLite database file. If None, uses a default
                location in the user's state directory.

        """
        self.db_path = db_path if db_path is not None else _get_default_db_path()
        self._ensure_table()

    def _connect(self) -> sqlite3.Connection:
        """Create a new database connection."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _ensure_table(self) -> None:
        """Create the storage table if it doesn't exist."""
        conn = self._connect()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS init_storage (
                    key BLOB PRIMARY KEY,
                    value BLOB NOT NULL,
                    created_at REAL DEFAULT (julianday('now'))
                )
            """)
            conn.commit()
        finally:
            conn.close()

    def create(self, value: bytes) -> bytes:
        """Store a value and return its unique key.

        Args:
            value: Serialized value bytes.

        Returns:
            Unique key for retrieving the value.

        """
        # Opportunistically clean old entries (1% of calls)
        if random.random() < 0.01:
            self.cleanup_old_entries(max_age_days=1.0)

        key = uuid.uuid4().bytes

        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO init_storage (key, value) VALUES (?, ?)",
                (key, value),
            )
            conn.commit()
        finally:
            conn.close()

        return key

    def get(self, key: bytes) -> bytes:
        """Retrieve a value by key, raising KeyError if not found.

        Args:
            key: Key returned from create().

        Returns:
            The stored value bytes.

        Raises:
            KeyError: If no value exists for this key.

        """
        conn = self._connect()
        try:
            cursor = conn.execute(
                "SELECT value FROM init_storage WHERE key = ?",
                (key,),
            )
            row = cursor.fetchone()
        finally:
            conn.close()

        if row is None:
            raise KeyError(f"Key {key.hex()} not found in SqliteInitStorage")

        value: bytes = row[0]
        return value

    def delete(self, key: bytes) -> None:
        """Delete a value by key if it exists."""
        conn = self._connect()
        try:
            conn.execute("DELETE FROM init_storage WHERE key = ?", (key,))
            conn.commit()
        finally:
            conn.close()

    def has(self, key: bytes) -> bool:
        """Check if a key exists in storage."""
        conn = self._connect()
        try:
            cursor = conn.execute(
                "SELECT 1 FROM init_storage WHERE key = ?",
                (key,),
            )
            return cursor.fetchone() is not None
        finally:
            conn.close()

    def cleanup_old_entries(self, max_age_days: float = 1.0) -> int:
        """Remove entries older than the specified age.

        Args:
            max_age_days: Maximum age in days for entries to keep.

        Returns:
            Number of entries deleted.

        """
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                DELETE FROM init_storage
                WHERE julianday('now') - created_at > ?
                """,
                (max_age_days,),
            )
            conn.commit()
            return int(cursor.rowcount)
        finally:
            conn.close()


class SqliteWorkerStateStorage:
    """SQLite storage for worker state in distributed functions.

    This storage allows distributed workers to persist their intermediate state
    (e.g., partial aggregations) which can later be collected by the primary
    worker during finalization.

    Each worker stores its state keyed by (invocation_id, process_id). The
    primary worker can then collect all states for a given invocation and
    delete them atomically.

    """

    def __init__(self, db_path: str | None = None) -> None:
        """Initialize SQLite worker state storage.

        Args:
            db_path: Path to the SQLite database file. If None, uses a default
                location in the user's state directory.

        """
        self.db_path = db_path if db_path is not None else _get_default_db_path()
        self._ensure_table()

    def _connect(self) -> sqlite3.Connection:
        """Create a new database connection."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _ensure_table(self) -> None:
        """Create the worker_state and work_queue tables if they don't exist."""
        conn = self._connect()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS worker_state (
                    invocation_id BLOB NOT NULL,
                    process_id INTEGER NOT NULL,
                    state_data BLOB NOT NULL,
                    created_at REAL DEFAULT (julianday('now')),
                    PRIMARY KEY (invocation_id, process_id)
                )
            """)
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
            conn.commit()
        finally:
            conn.close()

    def store(self, invocation_id: bytes, process_id: int, state_data: bytes) -> None:
        """Store or update state for a worker.

        If state already exists for this (invocation_id, process_id) pair,
        it will be replaced.

        Args:
            invocation_id: Unique identifier for the function invocation.
            process_id: Process ID of the worker storing the state.
            state_data: Serialized state bytes.

        """
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
                (invocation_id, process_id, state_data),
            )
            conn.commit()
        finally:
            conn.close()

    def collect_and_delete(self, invocation_id: bytes) -> list[bytes]:
        """Atomically fetch all states for an invocation and delete them.

        This is typically called by the primary worker during finalization
        to collect all worker states for aggregation.

        Args:
            invocation_id: Unique identifier for the function invocation.

        Returns:
            List of serialized state bytes from all workers.

        """
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

    def enqueue_work(self, invocation_id: bytes, work_items: list[bytes]) -> int:
        """Add work items to the queue for an invocation.

        Args:
            invocation_id: Unique identifier for the function invocation.
            work_items: List of serialized work item bytes (opaque to storage).

        Returns:
            Number of items enqueued.

        """
        if not work_items:
            return 0
        conn = self._connect()
        try:
            conn.executemany(
                """
                INSERT INTO work_queue (invocation_id, work_item)
                VALUES (?, ?)
                """,
                [(invocation_id, item) for item in work_items],
            )
            conn.commit()
            return len(work_items)
        finally:
            conn.close()

    def dequeue_work(self, invocation_id: bytes) -> bytes | None:
        """Atomically claim and delete one work item from the queue.

        Returns None if the queue is empty.

        Args:
            invocation_id: Unique identifier for the function invocation.

        Returns:
            Serialized work item bytes, or None if queue is empty.

        """
        conn = self._connect()
        try:
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

    def cleanup_queue(self, invocation_id: bytes) -> int:
        """Delete all remaining work items for an invocation.

        Args:
            invocation_id: Unique identifier for the function invocation.

        Returns:
            Number of items deleted.

        """
        conn = self._connect()
        try:
            cursor = conn.execute(
                "DELETE FROM work_queue WHERE invocation_id = ?",
                (invocation_id,),
            )
            conn.commit()
            return cursor.rowcount
        finally:
            conn.close()

    def cleanup_old_entries(self, max_age_days: float = 1.0) -> int:
        """Remove worker state and work queue entries older than the specified age.

        Args:
            max_age_days: Maximum age in days for entries to keep.

        Returns:
            Number of entries deleted (from both tables).

        """
        conn = self._connect()
        try:
            cursor1 = conn.execute(
                """
                DELETE FROM worker_state
                WHERE julianday('now') - created_at > ?
                """,
                (max_age_days,),
            )
            cursor2 = conn.execute(
                """
                DELETE FROM work_queue
                WHERE julianday('now') - created_at > ?
                """,
                (max_age_days,),
            )
            conn.commit()
            return int(cursor1.rowcount) + int(cursor2.rowcount)
        finally:
            conn.close()
