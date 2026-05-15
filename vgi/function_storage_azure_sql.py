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
import struct
import time
from collections.abc import Callable

import pymssql

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

            -- Unified state_* tables. scope_id holds either execution_id
            -- or transaction_opaque_data; ns is a caller-chosen namespace
            -- (b"agg", b"win", b"buf", b"txn", etc.). last_attempt_id +
            -- drained_at/drained_by_attempt power internal replay-detection
            -- (silent no-op for state_put_many retries; read-back for
            -- state_drain retries). VARBINARY(255) because scope_id /
            -- ns / key shapes vary across callers (16-byte UUIDs for
            -- execution_id, ASCII for transaction-state keys).
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'function_state')
            CREATE TABLE function_state (
                scope_id VARBINARY(255) NOT NULL,
                ns VARBINARY(255) NOT NULL,
                [key] VARBINARY(255) NOT NULL,
                value VARBINARY(MAX) NOT NULL,
                last_attempt_id VARBINARY(16) NOT NULL,
                drained_at DATETIME2 DEFAULT NULL,
                drained_by_attempt VARBINARY(16) DEFAULT NULL,
                created_at DATETIME2 DEFAULT GETUTCDATE(),
                PRIMARY KEY (scope_id, ns, [key])
            );

            -- function_state_log: append-only log keyed by (scope, ns, key).
            -- IDENTITY column gives a global monotonic ordinal per row;
            -- (scope, ns, key, attempt_id) is unique so a retried
            -- state_append maps back to its original ordinal.
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'function_state_log')
            CREATE TABLE function_state_log (
                id BIGINT IDENTITY(1,1) PRIMARY KEY,
                scope_id VARBINARY(255) NOT NULL,
                ns VARBINARY(255) NOT NULL,
                [key] VARBINARY(255) NOT NULL,
                value VARBINARY(MAX) NOT NULL,
                attempt_id VARBINARY(16) NOT NULL,
                created_at DATETIME2 DEFAULT GETUTCDATE()
            );

            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_function_state_log_lookup')
            CREATE INDEX idx_function_state_log_lookup
                ON function_state_log(scope_id, ns, [key], id);

            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_function_state_log_replay')
            CREATE UNIQUE INDEX idx_function_state_log_replay
                ON function_state_log(scope_id, ns, [key], attempt_id);
        """)
        conn.commit()
        elapsed_ms = (time.monotonic() - t0) * 1000
        _logger.debug("ensure_tables elapsed_ms=%.1f", elapsed_ms)

    # --- Work Queue ---

    def queue_push(self, execution_id: bytes, items: list[bytes], *, shard_key: str = "") -> int:
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

    def queue_pop(self, execution_id: bytes, *, shard_key: str = "") -> bytes | None:
        """Atomically claim one work item from the queue.

        Returns None when the queue is empty *or* the execution_id was
        never pushed — see the base-class docstring.
        """

        def _do(conn: pymssql.Connection) -> bytes | None:
            t0 = time.monotonic()
            cursor = conn.cursor()
            # Atomic claim of the oldest work_queue row for this eid.
            # OUTPUT deleted.work_item returns the claimed item (or no row
            # when the queue is empty / unregistered — both surface as
            # None to the caller).
            cursor.execute(
                """
                DECLARE @eid VARBINARY(16) = CAST(%s AS VARBINARY(16));
                ;WITH cte AS (
                    SELECT TOP (1) *
                    FROM work_queue WITH (ROWLOCK, UPDLOCK, READPAST)
                    WHERE execution_id = @eid
                    ORDER BY id ASC
                )
                DELETE FROM cte
                OUTPUT deleted.work_item;
                """,
                (execution_id,),
            )
            row = cursor.fetchone()
            conn.commit()
            elapsed_ms = (time.monotonic() - t0) * 1000
            got_item = row is not None and row[0] is not None
            _logger.debug(
                "queue_pop eid=%s result=%s elapsed_ms=%.1f",
                execution_id.hex()[:8],
                "item" if got_item else "empty",
                elapsed_ms,
            )
            if not got_item:
                return None
            result: bytes = row[0]  # type: ignore[index, assignment]
            return result

        return self._execute_with_retry(_do)

    def queue_clear(self, execution_id: bytes, *, shard_key: str = "") -> int:
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
            for table in ("work_queue", "invocation_registry",
                          "function_state", "function_state_log"):
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

    # ========================================================================
    # Unified state_* implementation
    # ========================================================================
    #
    # Mirrors the SQLite backend's contract. Every mutating call generates
    # an internal attempt_id (UUID4 bytes); replay-detection in
    # state_put_many checks whether the FIRST item's last_attempt_id
    # matches; state_drain checks drained_by_attempt and returns prior
    # tombstoned values on retry. T-SQL MERGE for upsert; IDENTITY column
    # on function_state_log gives global ordinals.

    def state_get_many(
        self,
        scope_id: bytes,
        ns: bytes,
        keys: list[bytes],
        *,
        shard_key: str = "",
    ) -> list[bytes | None]:
        """Batched read by key list. Returns parallel list with None for misses."""
        del shard_key
        if not keys:
            return []

        def _do(conn: pymssql.Connection) -> list[bytes | None]:
            cursor = conn.cursor()
            placeholders = ",".join("%s" for _ in keys)
            cursor.execute(
                f"""
                SELECT [key], value FROM function_state
                WHERE scope_id = %s AND ns = %s AND [key] IN ({placeholders})
                  AND drained_at IS NULL
                """,  # noqa: S608
                (scope_id, ns, *keys),
            )
            found: dict[bytes, bytes] = {bytes(k): bytes(v) for k, v in cursor.fetchall()}
            return [found.get(bytes(k)) for k in keys]

        return self._execute_with_retry(_do)

    def state_put_many(
        self,
        scope_id: bytes,
        ns: bytes,
        items: list[tuple[bytes, bytes]],
        *,
        shard_key: str = "",
    ) -> None:
        """Atomic batched upsert. First-key replay-detection on attempt_id."""
        del shard_key
        if not items:
            return
        import uuid

        attempt_id = uuid.uuid4().bytes

        def _do(conn: pymssql.Connection) -> None:
            cursor = conn.cursor()
            # Replay-detection: did the first item already land with our
            # attempt_id? Mirrors the CfDo aggregate_state_put first-item
            # check (`index.ts:618`); first key is sufficient because the
            # batch is atomic per-call.
            first_key, _ = items[0]
            cursor.execute(
                """
                SELECT 1 FROM function_state
                WHERE scope_id = %s AND ns = %s AND [key] = %s AND last_attempt_id = %s
                """,
                (scope_id, ns, first_key, attempt_id),
            )
            if cursor.fetchone() is not None:
                return  # Replay — silent no-op.
            for k, v in items:
                cursor.execute(
                    """
                    MERGE function_state AS t
                    USING (VALUES (CAST(%s AS VARBINARY(255)),
                                   CAST(%s AS VARBINARY(255)),
                                   CAST(%s AS VARBINARY(255)),
                                   CAST(%s AS VARBINARY(MAX)),
                                   CAST(%s AS VARBINARY(16))))
                        AS s(scope_id, ns, [key], value, last_attempt_id)
                    ON t.scope_id = s.scope_id AND t.ns = s.ns AND t.[key] = s.[key]
                    WHEN MATCHED THEN
                        UPDATE SET value = s.value,
                                   last_attempt_id = s.last_attempt_id,
                                   created_at = GETUTCDATE(),
                                   drained_at = NULL,
                                   drained_by_attempt = NULL
                    WHEN NOT MATCHED THEN
                        INSERT (scope_id, ns, [key], value, last_attempt_id)
                        VALUES (s.scope_id, s.ns, s.[key], s.value, s.last_attempt_id);
                    """,
                    (scope_id, ns, k, v, attempt_id),
                )
            conn.commit()

        self._execute_with_retry(_do)

    def state_scan(
        self,
        scope_id: bytes,
        ns: bytes,
        *,
        shard_key: str = "",
    ) -> list[tuple[bytes, bytes]]:
        """Non-destructive scan of all live (key, value) in a namespace."""
        del shard_key

        def _do(conn: pymssql.Connection) -> list[tuple[bytes, bytes]]:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT [key], value FROM function_state
                WHERE scope_id = %s AND ns = %s AND drained_at IS NULL
                """,
                (scope_id, ns),
            )
            return [(bytes(k), bytes(v)) for k, v in cursor.fetchall()]

        return self._execute_with_retry(_do)

    def state_drain(
        self,
        scope_id: bytes,
        ns: bytes,
        *,
        shard_key: str = "",
    ) -> list[tuple[bytes, bytes]]:
        """Destructive scan-and-tombstone. Replay returns prior tombstoned values."""
        del shard_key
        import uuid

        attempt_id = uuid.uuid4().bytes

        def _do(conn: pymssql.Connection) -> list[tuple[bytes, bytes]]:
            cursor = conn.cursor()
            # Read-back replay: any rows already tombstoned with our
            # attempt_id? Return them.
            cursor.execute(
                """
                SELECT [key], value FROM function_state
                WHERE scope_id = %s AND ns = %s AND drained_by_attempt = %s
                ORDER BY [key]
                """,
                (scope_id, ns, attempt_id),
            )
            replay = cursor.fetchall()
            if replay:
                return [(bytes(k), bytes(v)) for k, v in replay]
            # Fresh drain: tombstone live rows for this attempt_id, then
            # read them back. T-SQL doesn't support UPDATE..RETURNING the
            # same way SQLite does; use OUTPUT clause.
            cursor.execute(
                """
                UPDATE function_state
                SET drained_at = GETUTCDATE(),
                    drained_by_attempt = %s
                OUTPUT inserted.[key], inserted.value
                WHERE scope_id = %s AND ns = %s AND drained_at IS NULL
                """,
                (attempt_id, scope_id, ns),
            )
            rows = cursor.fetchall()
            conn.commit()
            return [(bytes(k), bytes(v)) for k, v in rows]

        return self._execute_with_retry(_do)

    def state_delete(
        self,
        scope_id: bytes,
        ns: bytes,
        keys: list[bytes] | None = None,
        *,
        shard_key: str = "",
    ) -> int:
        """Delete by key list, or whole namespace if keys is None. Returns count deleted."""
        del shard_key

        def _do(conn: pymssql.Connection) -> int:
            cursor = conn.cursor()
            if keys is None:
                cursor.execute(
                    "DELETE FROM function_state WHERE scope_id = %s AND ns = %s",
                    (scope_id, ns),
                )
            else:
                if not keys:
                    return 0
                placeholders = ",".join("%s" for _ in keys)
                cursor.execute(
                    f"""
                    DELETE FROM function_state
                    WHERE scope_id = %s AND ns = %s AND [key] IN ({placeholders})
                    """,  # noqa: S608
                    (scope_id, ns, *keys),
                )
            count = int(cursor.rowcount)
            conn.commit()
            return count

        return self._execute_with_retry(_do)

    def execution_clear(
        self,
        scope_id: bytes,
        *,
        shard_key: str = "",
    ) -> int:
        """Wipe all state and log rows for scope_id across every namespace."""
        del shard_key

        def _do(conn: pymssql.Connection) -> int:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM function_state WHERE scope_id = %s", (scope_id,))
            n1 = int(cursor.rowcount)
            cursor.execute("DELETE FROM function_state_log WHERE scope_id = %s", (scope_id,))
            n2 = int(cursor.rowcount)
            conn.commit()
            return n1 + n2

        return self._execute_with_retry(_do)

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
