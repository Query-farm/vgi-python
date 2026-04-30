"""Azure SQL Database storage for VGI function state.

This module provides a FunctionStorage implementation backed by Azure SQL
Database (Serverless). It is a near-direct port of FunctionStorageSqlite
to T-SQL via pymssql.

Implementation:
    FunctionStorageAzureSql: Azure SQL-backed storage implementation.

Usage:
    Set ``VGI_WORKER_SHARED_STORAGE=azure-sql`` plus ``VGI_AZURE_SQL_SERVER``
    and ``VGI_AZURE_SQL_DATABASE`` environment variables to enable. Provide
    ``VGI_AZURE_SQL_USER`` / ``VGI_AZURE_SQL_PASSWORD`` for SQL auth, or
    omit them to use ``DefaultAzureCredential`` (managed identity).

"""

import contextlib
import logging
import os
import random
import struct
import time
from collections.abc import Callable

import pymssql

from vgi.function_storage import UnknownInvocationError

__all__ = [
    "FunctionStorageAzureSql",
    "MissingTablesError",
]

# SQL Server error codes
_ERR_INVALID_OBJECT_NAME = 208


class MissingTablesError(Exception):
    """Raised when storage tables don't exist in the database."""


_logger = logging.getLogger("vgi.storage.azure_sql")

# If VGI_AZURE_SQL_DEBUG_LOG is set, write debug logs to that file
# regardless of the root logger configuration.
_debug_log_path = os.environ.get("VGI_AZURE_SQL_DEBUG_LOG")
if _debug_log_path:
    _fh = logging.FileHandler(_debug_log_path)
    _fh.setLevel(logging.DEBUG)
    _fh.setFormatter(logging.Formatter("%(asctime)s %(process)d %(message)s"))
    _logger.addHandler(_fh)
    _logger.setLevel(logging.DEBUG)

# Azure AD resource for SQL Database token auth
_SQL_AZURE_RESOURCE = "https://database.windows.net/"


class FunctionStorageAzureSql:
    """Azure SQL Database-backed storage for VGI function state.

    This implementation uses Azure SQL Database with the same table schema
    as FunctionStorageSqlite. It manages three tables:

    - worker_state: Per-worker partial state keyed by (execution_id, process_id)
    - work_queue: FIFO queue of work items per invocation
    - invocation_registry: Tracks valid invocation IDs for queue operations

    Connection modes:
        - SQL auth: provide ``user`` and ``password``
        - Managed identity: omit ``user``/``password``, optionally pass a
          ``credential`` (falls back to ``DefaultAzureCredential``)

    """

    def __init__(
        self,
        *,
        server: str,
        database: str,
        user: str | None = None,
        password: str | None = None,
        credential: object | None = None,
    ) -> None:
        """Initialize Azure SQL storage.

        Args:
            server: Azure SQL server hostname.
            database: Database name.
            user: SQL auth username. If None, token-based auth is used.
            password: SQL auth password.
            credential: Optional TokenCredential for Azure AD auth.
                Falls back to DefaultAzureCredential if omitted.

        """
        self._server = server
        self._database = database
        self._user = user
        self._password = password
        self._credential = credential
        self._conn: pymssql.Connection | None = None

    def _new_connection(self) -> pymssql.Connection:
        """Create a new database connection."""
        t0 = time.monotonic()
        if self._user is not None and self._password is not None:
            conn = pymssql.connect(
                server=self._server,
                user=self._user,
                password=self._password,
                database=self._database,
                login_timeout=30,
                as_dict=False,
            )
        else:
            # Token-based auth (managed identity / DefaultAzureCredential)
            token = self._get_access_token()
            token_bytes = _encode_access_token(token)
            conn = pymssql.connect(  # type: ignore[call-overload]
                server=self._server,
                password=token_bytes,
                database=self._database,
                login_timeout=30,
                as_dict=False,
            )
        elapsed_ms = (time.monotonic() - t0) * 1000
        _logger.debug("connect server=%s elapsed_ms=%.1f", self._server, elapsed_ms)
        return conn

    def _connect(self) -> pymssql.Connection:
        """Return a persistent connection, creating one if needed.

        Callers that catch exceptions should call ``_reconnect()``
        before retrying so the dead connection is replaced.
        """
        if self._conn is None:
            self._conn = self._new_connection()
        return self._conn

    def _reconnect(self) -> None:
        """Drop the current connection so the next ``_connect()`` creates a fresh one."""
        if self._conn is not None:
            with contextlib.suppress(Exception):
                self._conn.close()
            self._conn = None

    def _execute_with_retry[T](self, fn: "Callable[[pymssql.Connection], T]") -> T:
        """Execute a function with the persistent connection, retrying once on failure.

        On the first failure, the connection is dropped and a fresh one
        is created for the retry.  ``Invalid object name`` errors are
        translated to :class:`MissingTablesError` with a helpful message.
        """
        for attempt in range(2):
            try:
                return fn(self._connect())
            except pymssql.OperationalError as exc:
                self._check_missing_tables(exc)
                if attempt == 0:
                    _logger.debug("retry after OperationalError: %s", exc)
                    self._reconnect()
                else:
                    raise
            except pymssql.InterfaceError as exc:
                if attempt == 0:
                    _logger.debug("retry after InterfaceError: %s", exc)
                    self._reconnect()
                else:
                    raise
        raise RuntimeError("unreachable")  # pragma: no cover

    @staticmethod
    def _check_missing_tables(exc: Exception) -> None:
        """Raise MissingTablesError if the exception indicates missing tables."""
        if hasattr(exc, "args") and exc.args and exc.args[0] == _ERR_INVALID_OBJECT_NAME:
            raise MissingTablesError(
                "Storage tables do not exist in the database. "
                "Run FunctionStorageAzureSql.ensure_tables() during deployment "
                "to create them."
            ) from exc

    def _get_access_token(self) -> str:
        """Acquire an Azure AD access token for SQL Database."""
        if self._credential is None:
            from azure.identity import DefaultAzureCredential

            self._credential = DefaultAzureCredential()
        token = self._credential.get_token(_SQL_AZURE_RESOURCE)  # type: ignore[attr-defined]
        return str(token.token)

    def ensure_tables(self) -> None:
        """Create all storage tables if they don't exist.

        Call this once during deployment or migration — not on every worker
        start. All DDL is sent as a single batch to minimize round-trips.
        """
        t0 = time.monotonic()
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'worker_state')
            CREATE TABLE worker_state (
                execution_id VARBINARY(16) NOT NULL,
                process_id INT NOT NULL,
                state_data VARBINARY(MAX) NOT NULL,
                created_at DATETIME2 DEFAULT GETUTCDATE(),
                PRIMARY KEY (execution_id, process_id)
            );

            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'work_queue')
            CREATE TABLE work_queue (
                id BIGINT IDENTITY(1,1) PRIMARY KEY,
                execution_id VARBINARY(16) NOT NULL,
                work_item VARBINARY(MAX) NOT NULL,
                created_at DATETIME2 DEFAULT GETUTCDATE()
            );

            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_work_queue_execution')
            CREATE INDEX idx_work_queue_execution ON work_queue(execution_id);

            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'invocation_registry')
            CREATE TABLE invocation_registry (
                execution_id VARBINARY(16) PRIMARY KEY,
                created_at DATETIME2 DEFAULT GETUTCDATE()
            );
        """)
        conn.commit()
        elapsed_ms = (time.monotonic() - t0) * 1000
        _logger.debug("ensure_tables elapsed_ms=%.1f", elapsed_ms)

    # --- Worker State ---

    def worker_put(self, execution_id: bytes, worker_id: int, state: bytes) -> None:
        """Store state for a specific worker."""
        if random.random() < 0.01:
            self.cleanup_old_entries(max_age_days=1.0)

        def _do(conn: pymssql.Connection) -> None:
            t0 = time.monotonic()
            cursor = conn.cursor()
            cursor.execute(
                """
                MERGE worker_state AS t
                USING (VALUES (CAST(%s AS VARBINARY(16)), %s, CAST(%s AS VARBINARY(MAX))))
                    AS s(execution_id, process_id, state_data)
                ON t.execution_id = s.execution_id AND t.process_id = s.process_id
                WHEN MATCHED THEN
                    UPDATE SET state_data = s.state_data, created_at = GETUTCDATE()
                WHEN NOT MATCHED THEN
                    INSERT (execution_id, process_id, state_data)
                    VALUES (s.execution_id, s.process_id, s.state_data);
                """,
                (execution_id, worker_id, state),
            )
            conn.commit()
            _logger.debug(
                "worker_put eid=%s worker_id=%d state_bytes=%d elapsed_ms=%.1f",
                execution_id.hex()[:8],
                worker_id,
                len(state),
                (time.monotonic() - t0) * 1000,
            )

        self._execute_with_retry(_do)

    def worker_collect(self, execution_id: bytes) -> list[bytes]:
        """Atomically collect and delete all worker states."""

        def _do(conn: pymssql.Connection) -> list[bytes]:
            t0 = time.monotonic()
            cursor = conn.cursor()
            cursor.execute(
                """
                DELETE FROM worker_state
                OUTPUT deleted.state_data
                WHERE execution_id = CAST(%s AS VARBINARY(16))
                """,
                (execution_id,),
            )
            states: list[bytes] = [row[0] for row in cursor.fetchall()]  # type: ignore[misc]
            conn.commit()
            _logger.debug(
                "worker_collect eid=%s states_returned=%d elapsed_ms=%.1f",
                execution_id.hex()[:8],
                len(states),
                (time.monotonic() - t0) * 1000,
            )
            return states

        return self._execute_with_retry(_do)

    # --- Work Queue ---

    def queue_push(self, execution_id: bytes, items: list[bytes]) -> int:
        """Add work items to the queue and register the invocation."""

        def _do(conn: pymssql.Connection) -> int:
            t0 = time.monotonic()
            cursor = conn.cursor()
            cursor.execute(
                """
                MERGE invocation_registry AS t
                USING (VALUES (CAST(%s AS VARBINARY(16)))) AS s(execution_id)
                ON t.execution_id = s.execution_id
                WHEN NOT MATCHED THEN
                    INSERT (execution_id) VALUES (s.execution_id);
                """,
                (execution_id,),
            )
            if items:
                cursor.executemany(
                    """
                    INSERT INTO work_queue (execution_id, work_item)
                    VALUES (CAST(%s AS VARBINARY(16)), CAST(%s AS VARBINARY(MAX)))
                    """,
                    [(execution_id, item) for item in items],
                )
            conn.commit()
            _logger.debug(
                "queue_push eid=%s items=%d elapsed_ms=%.1f",
                execution_id.hex()[:8],
                len(items),
                (time.monotonic() - t0) * 1000,
            )
            return len(items)

        return self._execute_with_retry(_do)

    def queue_pop(self, execution_id: bytes) -> bytes | None:
        """Atomically claim one work item from the queue.

        Raises:
            UnknownInvocationError: If execution_id was never registered via
                queue_push or has been cleared via queue_clear.

        """

        def _do(conn: pymssql.Connection) -> bytes | None:
            t0 = time.monotonic()
            cursor = conn.cursor()
            # Combined registry check + atomic claim in a single round-trip.
            # Uses OUTPUT INTO a table variable to avoid multiple result sets.
            # Returns exactly one row:
            #   (0, NULL)     → invocation not registered
            #   (1, NULL)     → queue empty but registered
            #   (1, <bytes>)  → item popped
            cursor.execute(
                """
                DECLARE @eid VARBINARY(16) = CAST(%s AS VARBINARY(16));
                DECLARE @registered BIT = 0;
                DECLARE @result TABLE (work_item VARBINARY(MAX));

                IF EXISTS (SELECT 1 FROM invocation_registry WHERE execution_id = @eid)
                BEGIN
                    SET @registered = 1;
                    ;WITH cte AS (
                        SELECT TOP (1) *
                        FROM work_queue WITH (ROWLOCK, UPDLOCK, READPAST)
                        WHERE execution_id = @eid
                        ORDER BY id ASC
                    )
                    DELETE FROM cte
                    OUTPUT deleted.work_item INTO @result;
                END

                SELECT @registered AS registered, r.work_item
                FROM (SELECT 1 AS x) AS dummy
                LEFT JOIN @result AS r ON 1=1;
                """,
                (execution_id,),
            )
            row = cursor.fetchone()
            conn.commit()
            elapsed_ms = (time.monotonic() - t0) * 1000
            if row is None or not row[0]:
                _logger.debug(
                    "queue_pop eid=%s result=unregistered elapsed_ms=%.1f",
                    execution_id.hex()[:8],
                    elapsed_ms,
                )
                raise UnknownInvocationError(
                    f"Invocation {execution_id.hex()} is not registered. "
                    "Call queue_push first to register the invocation."
                )
            got_item = row[1] is not None
            _logger.debug(
                "queue_pop eid=%s result=%s elapsed_ms=%.1f",
                execution_id.hex()[:8],
                "item" if got_item else "empty",
                elapsed_ms,
            )
            result: bytes | None = row[1]  # type: ignore[assignment]
            return result

        return self._execute_with_retry(_do)

    def queue_clear(self, execution_id: bytes) -> int:
        """Clear all remaining work items and unregister the invocation."""

        def _do(conn: pymssql.Connection) -> int:
            t0 = time.monotonic()
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM work_queue WHERE execution_id = CAST(%s AS VARBINARY(16))",
                (execution_id,),
            )
            cleared = cursor.rowcount
            cursor.execute(
                "DELETE FROM invocation_registry WHERE execution_id = CAST(%s AS VARBINARY(16))",
                (execution_id,),
            )
            conn.commit()
            _logger.debug(
                "queue_clear eid=%s cleared=%d elapsed_ms=%.1f",
                execution_id.hex()[:8],
                cleared,
                (time.monotonic() - t0) * 1000,
            )
            return cleared

        return self._execute_with_retry(_do)

    # --- Maintenance ---

    def cleanup_old_entries(self, max_age_days: float = 1.0) -> int:
        """Remove entries older than the specified age from all tables."""

        def _do(conn: pymssql.Connection) -> int:
            t0 = time.monotonic()
            max_age_seconds = int(max_age_days * 86400)
            cursor = conn.cursor()
            total = 0
            for table in ("worker_state", "work_queue", "invocation_registry"):
                cursor.execute(
                    f"DELETE FROM {table} WHERE DATEDIFF(SECOND, created_at, GETUTCDATE()) > %s",  # noqa: S608
                    (max_age_seconds,),
                )
                total += cursor.rowcount
            conn.commit()
            _logger.debug(
                "cleanup_old_entries max_age_days=%.1f deleted=%d elapsed_ms=%.1f",
                max_age_days,
                total,
                (time.monotonic() - t0) * 1000,
            )
            return total

        return self._execute_with_retry(_do)

    # --- Aggregate State ---

    def aggregate_state_get(self, execution_id: bytes, group_ids: list[int]) -> list[tuple[int, bytes] | None]:
        """Not yet supported on Azure SQL."""
        raise NotImplementedError("Aggregate functions are not yet supported with the Azure SQL storage backend.")

    def aggregate_state_put(self, execution_id: bytes, data: list[tuple[int, bytes]]) -> None:
        """Not yet supported on Azure SQL."""
        raise NotImplementedError("Aggregate functions are not yet supported with the Azure SQL storage backend.")

    def aggregate_state_clear(self, execution_id: bytes) -> None:
        """Not yet supported on Azure SQL."""
        raise NotImplementedError("Aggregate functions are not yet supported with the Azure SQL storage backend.")

    # --- Transaction State ---

    def transaction_state_get(self, transaction_id: bytes, keys: list[bytes]) -> list[bytes | None]:
        """Not yet supported on Azure SQL."""
        raise NotImplementedError("Transaction state is not yet supported with the Azure SQL storage backend.")

    def transaction_state_put(self, transaction_id: bytes, items: list[tuple[bytes, bytes]]) -> None:
        """Not yet supported on Azure SQL."""
        raise NotImplementedError("Transaction state is not yet supported with the Azure SQL storage backend.")

    def transaction_state_clear(self, transaction_id: bytes) -> None:
        """Not yet supported on Azure SQL."""
        raise NotImplementedError("Transaction state is not yet supported with the Azure SQL storage backend.")

    def aggregate_window_partition_put(self, execution_id: bytes, partition_id: int, data: bytes) -> None:
        """Not yet supported on Azure SQL."""
        raise NotImplementedError(
            "Aggregate window functions are not yet supported with the Azure SQL storage backend."
        )

    def aggregate_window_partition_get(self, execution_id: bytes, partition_id: int) -> bytes | None:
        """Not yet supported on Azure SQL."""
        raise NotImplementedError(
            "Aggregate window functions are not yet supported with the Azure SQL storage backend."
        )

    def aggregate_window_partition_delete(self, execution_id: bytes, partition_id: int) -> None:
        """Not yet supported on Azure SQL."""
        raise NotImplementedError(
            "Aggregate window functions are not yet supported with the Azure SQL storage backend."
        )

    def aggregate_window_partition_clear(self, execution_id: bytes) -> None:
        """Not yet supported on Azure SQL."""
        raise NotImplementedError(
            "Aggregate window functions are not yet supported with the Azure SQL storage backend."
        )

    # --- Factory ---

    @classmethod
    def from_env(cls) -> "FunctionStorageAzureSql":
        """Create an instance from environment variables.

        Required:
            VGI_AZURE_SQL_SERVER: Azure SQL server hostname.
            VGI_AZURE_SQL_DATABASE: Database name.

        Optional (SQL auth):
            VGI_AZURE_SQL_USER: SQL auth username.
            VGI_AZURE_SQL_PASSWORD: SQL auth password.

        If user/password are omitted, DefaultAzureCredential is used.

        """
        server = os.environ.get("VGI_AZURE_SQL_SERVER")
        database = os.environ.get("VGI_AZURE_SQL_DATABASE")
        if not server or not database:
            raise ValueError(
                "VGI_AZURE_SQL_SERVER and VGI_AZURE_SQL_DATABASE environment "
                "variables are required when VGI_WORKER_SHARED_STORAGE=azure-sql"
            )
        return cls(
            server=server,
            database=database,
            user=os.environ.get("VGI_AZURE_SQL_USER") or None,
            password=os.environ.get("VGI_AZURE_SQL_PASSWORD") or None,
        )


def _encode_access_token(token: str) -> bytes:
    """Encode an Azure AD access token for TDS token-based auth.

    SQL Server expects the token as a UTF-16-LE encoded byte string
    with a 4-byte little-endian length prefix.
    """
    token_bytes = token.encode("UTF-16-LE")
    return struct.pack("<I", len(token_bytes)) + token_bytes
