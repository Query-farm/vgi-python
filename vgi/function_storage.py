"""Storage for VGI function state.

This module provides a storage protocol and implementation for sharing state
across worker processes in distributed VGI function execution.

Protocol:
    FunctionStorage: Unified protocol for all VGI state storage needs.

Implementations:
    FunctionStorageSqlite: SQLite-backed storage (local/subprocess transport).
    FunctionStorageAzureSql: Azure SQL Database-backed storage (cloud deployments).
        See ``vgi.function_storage_azure_sql`` for details.
    FunctionStorageCfDo: Cloudflare Durable Object-backed storage (edge deployments).
        See ``vgi.function_storage_cf_do`` for details.

"""

import os
import random
import sqlite3
from typing import Protocol

import pyarrow as pa

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

    **Worker State** - Partial results from each worker process.
    Each worker stores its state during processing, primary worker collects
    all states during finalization for aggregation.

    **Work Queue** - Atomic work distribution across workers.
    Primary worker enqueues work items, workers atomically claim items
    from the queue for processing.

    """

    def worker_put(self, execution_id: bytes, worker_id: int, state: bytes) -> None:
        """Store state for a specific worker.

        If state already exists for this (execution_id, worker_id) pair,
        it will be replaced.

        Args:
            execution_id: Unique identifier for the function invocation.
            worker_id: Process ID of the worker storing the state.
            state: Serialized state bytes.

        """
        ...

    def worker_collect(self, execution_id: bytes) -> list[bytes]:
        """Atomically collect and delete all worker states.

        This is typically called by the primary worker during finalization
        to collect all worker states for aggregation.

        Args:
            execution_id: Unique identifier for the function invocation.

        Returns:
            List of serialized state bytes from all workers.

        """
        ...

    # --- Work Queue (distributed work items) ---

    def queue_push(self, execution_id: bytes, items: list[bytes]) -> int:
        """Add work items to the queue and register the invocation.

        This method registers the execution_id as valid, allowing subsequent
        queue_pop calls. Even if items is empty, the invocation is registered.

        Args:
            execution_id: Unique identifier for the function invocation.
            items: List of serialized work item bytes.

        Returns:
            Number of items added.

        """
        ...

    def queue_pop(self, execution_id: bytes) -> bytes | None:
        """Atomically claim one work item from the queue.

        The execution_id must have been previously registered via queue_push.
        This allows detection of badly coded clients or clients attempting to
        interact after their executions have been completed.

        Args:
            execution_id: Unique identifier for the function invocation.

        Returns:
            Serialized work item bytes, or None if queue is empty.

        Raises:
            UnknownInvocationError: If the execution_id was never registered
                via queue_push or has been cleared via queue_clear.

        """
        ...

    def queue_clear(self, execution_id: bytes) -> int:
        """Clear all remaining work items and unregister the invocation.

        After calling this method, subsequent queue_pop calls for this
        execution_id will raise UnknownInvocationError.

        Args:
            execution_id: Unique identifier for the function invocation.

        Returns:
            Number of items deleted.

        """
        ...


class BoundStorage:
    def __init__(self, storage: FunctionStorage, execution_id: bytes):
        self._base = storage
        self._execution_id = execution_id

    def put(self, state: bytes) -> None:
        """Store state for a specific worker."""
        self._base.worker_put(self._execution_id, os.getpid(), state)

    def collect(self) -> list[bytes]:
        """Atomically collect and delete all worker states."""
        return self._base.worker_collect(self._execution_id)

    def queue_push(self, items: list[bytes]) -> int:
        """Add work items to the queue and register the invocation."""
        return self._base.queue_push(self._execution_id, items)

    def queue_push_batches(self, batches: list[pa.RecordBatch]) -> int:
        """Serialize and push RecordBatches as work items."""
        return self.queue_push([self.serialize_record_batch(b) for b in batches])

    def queue_pop(self) -> bytes | None:
        """Atomically claim one work item from the queue."""
        return self._base.queue_pop(self._execution_id)

    def queue_pop_batch(self) -> pa.RecordBatch | None:
        """Pop and deserialize one work item as a RecordBatch."""
        data = self.queue_pop()
        if data is None:
            return None
        return self.deserialize_record_batch(data)

    def queue_clear(self) -> int:
        """Clear all remaining work items and unregister the invocation."""
        return self._base.queue_clear(self._execution_id)

    @staticmethod
    def serialize_record_batch(batch: pa.RecordBatch) -> bytes:
        """Serialize a RecordBatch to Arrow IPC stream bytes."""
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        return sink.getvalue().to_pybytes()

    @staticmethod
    def deserialize_record_batch(data: bytes) -> pa.RecordBatch:
        with pa.ipc.open_stream(data) as ipc_reader:
            return ipc_reader.read_next_batch()


class FunctionStorageSqlite:
    """SQLite-backed storage for VGI function state.

    This implementation uses SQLite with WAL mode to allow multiple worker
    processes to share state. It manages three tables:

    - global_state_storage: Key-value store for init data
    - worker_state: Per-worker partial state keyed by (execution_id, worker_id)
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
        """Create all storage tables if they don't exist.

        Handles schema migration from older versions (e.g. invocation_id → execution_id)
        by dropping and recreating tables with stale schemas. The data in these tables
        is ephemeral (in-progress worker state), so dropping is safe.
        """
        conn = self._connect()
        try:
            # Drop tables with stale schema (e.g. invocation_id instead of execution_id)
            for table, required_col in [
                ("worker_state", "execution_id"),
                ("work_queue", "execution_id"),
                ("invocation_registry", "execution_id"),
            ]:
                cursor = conn.execute(f"PRAGMA table_info({table})")  # noqa: S608
                columns = {row[1] for row in cursor.fetchall()}
                if columns and required_col not in columns:
                    conn.execute(f"DROP TABLE IF EXISTS {table}")  # noqa: S608

            # Also drop old table names from previous versions
            conn.execute("DROP TABLE IF EXISTS init_storage")

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
                    execution_id BLOB NOT NULL,
                    process_id INTEGER NOT NULL,
                    state_data BLOB NOT NULL,
                    created_at REAL DEFAULT (julianday('now')),
                    PRIMARY KEY (execution_id, process_id)
                )
            """)
            # Work queue table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS work_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id BLOB NOT NULL,
                    work_item BLOB NOT NULL,
                    created_at REAL DEFAULT (julianday('now'))
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_work_queue_invocation
                ON work_queue(execution_id)
            """)
            # Invocation registry - tracks valid invocation IDs for queue operations
            conn.execute("""
                CREATE TABLE IF NOT EXISTS invocation_registry (
                    execution_id BLOB PRIMARY KEY,
                    created_at REAL DEFAULT (julianday('now'))
                )
            """)
            conn.commit()
        finally:
            conn.close()

    # --- Worker State ---

    def worker_put(self, execution_id: bytes, worker_id: int, state: bytes) -> None:
        """Store state for a specific worker."""
        # Opportunistically clean old entries (1% of calls)
        if random.random() < 0.01:
            self.cleanup_old_entries(max_age_days=1.0)

        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO worker_state
                (execution_id, process_id, state_data, created_at)
                VALUES (?, ?, ?, julianday('now'))
                """,
                (execution_id, worker_id, state),
            )
            conn.commit()
        finally:
            conn.close()

    def worker_collect(self, execution_id: bytes) -> list[bytes]:
        """Atomically collect and delete all worker states."""
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                DELETE FROM worker_state
                WHERE execution_id = ?
                RETURNING state_data
                """,
                (execution_id,),
            )
            states = [row[0] for row in cursor.fetchall()]
            conn.commit()
            return states
        finally:
            conn.close()

    # --- Work Queue ---

    def queue_push(self, execution_id: bytes, items: list[bytes]) -> int:
        """Add work items to the queue and register the invocation."""
        conn = self._connect()
        try:
            # Register the execution_id (idempotent)
            conn.execute(
                "INSERT OR IGNORE INTO invocation_registry (execution_id) VALUES (?)",
                (execution_id,),
            )
            # Add work items if any
            if items:
                conn.executemany(
                    """
                    INSERT INTO work_queue (execution_id, work_item)
                    VALUES (?, ?)
                    """,
                    [(execution_id, item) for item in items],
                )
            conn.commit()
            return len(items)
        finally:
            conn.close()

    def queue_pop(self, execution_id: bytes) -> bytes | None:
        """Atomically claim one work item from the queue.

        Raises:
            UnknownInvocationError: If execution_id was never registered via
                queue_push or has been cleared via queue_clear.

        """
        conn = self._connect()
        try:
            # Check if invocation is registered
            reg_cursor = conn.execute(
                "SELECT 1 FROM invocation_registry WHERE execution_id = ?",
                (execution_id,),
            )
            if reg_cursor.fetchone() is None:
                raise UnknownInvocationError(
                    f"Invocation {execution_id.hex()} is not registered. "
                    "Call queue_push first to register the invocation."
                )

            cursor = conn.execute(
                """
                DELETE FROM work_queue
                WHERE id = (
                    SELECT id FROM work_queue
                    WHERE execution_id = ?
                    ORDER BY id ASC
                    LIMIT 1
                )
                RETURNING work_item
                """,
                (execution_id,),
            )
            row = cursor.fetchone()
            conn.commit()
            return row[0] if row else None
        finally:
            conn.close()

    def queue_clear(self, execution_id: bytes) -> int:
        """Clear all remaining work items and unregister the invocation."""
        conn = self._connect()
        try:
            cursor = conn.execute(
                "DELETE FROM work_queue WHERE execution_id = ?",
                (execution_id,),
            )
            # Unregister the invocation
            conn.execute(
                "DELETE FROM invocation_registry WHERE execution_id = ?",
                (execution_id,),
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
            return int(cursor1.rowcount) + int(cursor2.rowcount) + int(cursor3.rowcount) + int(cursor4.rowcount)
        finally:
            conn.close()
