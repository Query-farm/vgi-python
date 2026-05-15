"""Tests for vgi.function_storage_azure_sql module."""

from __future__ import annotations

from unittest.mock import patch

import pytest

pymssql = pytest.importorskip("pymssql")

from vgi.function_storage_azure_sql import FunctionStorageAzureSql  # noqa: E402


class _MockCursor:
    """Reusable mock cursor that tracks SQL and simulates fetchone/fetchall."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self._results: list[tuple[object, ...]] = []
        self._rowcount = 0

    def execute(self, sql: str, params: tuple[object, ...] = ()) -> None:
        self.executed.append((sql.strip(), params))

    def executemany(self, sql: str, params_list: list[tuple[object, ...]]) -> None:
        for params in params_list:
            self.executed.append((sql.strip(), params))

    def fetchone(self) -> tuple[object, ...] | None:
        return self._results.pop(0) if self._results else None

    def fetchall(self) -> list[tuple[object, ...]]:
        results = list(self._results)
        self._results.clear()
        return results

    @property
    def rowcount(self) -> int:
        return self._rowcount

    @rowcount.setter
    def rowcount(self, value: int) -> None:
        self._rowcount = value

    def set_results(self, results: list[tuple[object, ...]]) -> None:
        self._results = list(results)
        self._rowcount = len(results)


class _MockConnection:
    """Mock connection that returns a controllable cursor."""

    def __init__(self, cursor: _MockCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _MockCursor:
        return self._cursor

    def commit(self) -> None:
        pass

    def close(self) -> None:
        pass


@pytest.fixture
def mock_cursor() -> _MockCursor:
    """Create a fresh mock cursor."""
    return _MockCursor()


@pytest.fixture
def storage(mock_cursor: _MockCursor) -> FunctionStorageAzureSql:
    """Create a storage instance with mocked pymssql.connect."""
    mock_conn = _MockConnection(mock_cursor)
    s = FunctionStorageAzureSql(
        server="test.database.windows.net",
        database="testdb",
        user="testuser",
        password="testpass",
    )
    # Patch _connect to return our mock for subsequent calls
    s._connect = lambda: mock_conn  # type: ignore[assignment,return-value]
    return s


class TestFunctionStorageAzureSql:
    """Tests for FunctionStorageAzureSql."""

    # --- Worker State Tests ---

    def test_worker_put_and_collect(self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor) -> None:
        """Test storing and collecting worker states."""
        execution_id = b"\x01" * 16

        # Put states from 3 workers
        storage.worker_put(execution_id, worker_id=1, state=b"state1")
        storage.worker_put(execution_id, worker_id=2, state=b"state2")
        storage.worker_put(execution_id, worker_id=3, state=b"state3")

        # Verify MERGE was used (3 puts)
        merge_calls = [sql for sql, _ in mock_cursor.executed if "MERGE worker_state" in sql]
        assert len(merge_calls) == 3

        # Simulate collect returning states
        mock_cursor.executed.clear()
        mock_cursor.set_results([(b"state1",), (b"state2",), (b"state3",)])

        states = storage.worker_collect(execution_id)
        assert len(states) == 3
        assert set(states) == {b"state1", b"state2", b"state3"}

        # Verify DELETE OUTPUT was used
        delete_calls = [sql for sql, _ in mock_cursor.executed if "DELETE FROM worker_state" in sql]
        assert len(delete_calls) == 1
        assert "OUTPUT deleted.state_data" in delete_calls[0]

    def test_worker_put_replaces_existing(self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor) -> None:
        """Test that worker_put replaces existing state for same worker."""
        execution_id = b"\x01" * 16

        storage.worker_put(execution_id, worker_id=1, state=b"old_state")
        storage.worker_put(execution_id, worker_id=1, state=b"new_state")

        # Both should use MERGE (upsert semantics)
        merge_calls = [sql for sql, _ in mock_cursor.executed if "MERGE worker_state" in sql]
        assert len(merge_calls) == 2
        # Second call has new_state
        assert mock_cursor.executed[-1][1] == (execution_id, 1, b"new_state")

    def test_worker_collect_empty(self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor) -> None:
        """Test collecting when no states exist."""
        mock_cursor.set_results([])
        states = storage.worker_collect(b"\x01" * 16)
        assert states == []

    # --- Work Queue Tests ---

    def test_queue_push_and_pop(self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor) -> None:
        """Test pushing and popping work items."""
        execution_id = b"\x02" * 16
        items = [b"item1", b"item2", b"item3"]

        count = storage.queue_push(execution_id, items)
        assert count == 3

        # Verify MERGE for registry + executemany INSERT for items
        merge_calls = [sql for sql, _ in mock_cursor.executed if "MERGE invocation_registry" in sql]
        insert_calls = [sql for sql, _ in mock_cursor.executed if "INSERT INTO work_queue" in sql]
        assert len(merge_calls) == 1
        assert len(insert_calls) >= 1  # executemany may show as 1 or N calls

    def test_queue_pop_returns_item(self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor) -> None:
        """Test that queue_pop returns an item when available."""
        execution_id = b"\x02" * 16

        # OUTPUT deleted.work_item returns one row with the item bytes.
        mock_cursor.set_results([(b"item1",)])

        result = storage.queue_pop(execution_id)
        assert result == b"item1"

        # Verify CTE with locking hints was used
        cte_calls = [sql for sql, _ in mock_cursor.executed if "UPDLOCK, READPAST" in sql]
        assert len(cte_calls) == 1

    def test_queue_pop_empty_queue(self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor) -> None:
        """Test popping from registered but empty queue returns None."""
        execution_id = b"\x02" * 16

        # No row deleted → no OUTPUT row.
        mock_cursor.set_results([])

        result = storage.queue_pop(execution_id)
        assert result is None

    def test_queue_pop_never_pushed_returns_none(
        self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor
    ) -> None:
        """Popping an id that was never pushed returns None.

        No distinction from drained queue per the contract.
        """
        # Same wire shape as empty queue — no OUTPUT row.
        mock_cursor.set_results([])
        assert storage.queue_pop(b"\xff" * 16) is None

    def test_queue_push_empty_list(self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor) -> None:
        """Test pushing empty list returns 0."""
        count = storage.queue_push(b"\x02" * 16, [])
        assert count == 0

    def test_queue_push_empty_still_registers(self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor) -> None:
        """Test that pushing empty list still registers invocation."""
        execution_id = b"\x02" * 16
        storage.queue_push(execution_id, [])

        # Verify MERGE for registry was called
        merge_calls = [sql for sql, _ in mock_cursor.executed if "MERGE invocation_registry" in sql]
        assert len(merge_calls) == 1

    def test_queue_clear(self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor) -> None:
        """Test clearing the work queue."""
        execution_id = b"\x02" * 16
        mock_cursor.rowcount = 3

        cleared = storage.queue_clear(execution_id)
        assert cleared == 3

        # Verify both deletes happened
        delete_calls = [sql for sql, _ in mock_cursor.executed if "DELETE FROM" in sql]
        assert len(delete_calls) == 2
        tables_deleted = [sql for sql, _ in mock_cursor.executed]
        assert any("work_queue" in sql for sql in tables_deleted)
        assert any("invocation_registry" in sql for sql in tables_deleted)

    def test_queue_clear_unregisters_invocation(
        self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor
    ) -> None:
        """Test that queue_clear unregisters the invocation."""
        execution_id = b"\x02" * 16
        mock_cursor.rowcount = 0
        storage.queue_clear(execution_id)

        # Verify invocation_registry delete was called
        registry_deletes = [sql for sql, _ in mock_cursor.executed if "DELETE FROM invocation_registry" in sql]
        assert len(registry_deletes) == 1

    def test_queue_clear_empty_queue(self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor) -> None:
        """Test clearing an empty queue returns 0."""
        mock_cursor.rowcount = 0
        cleared = storage.queue_clear(b"\x02" * 16)
        assert cleared == 0

    # --- Cleanup Tests ---

    def test_cleanup_old_entries(self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor) -> None:
        """Test cleanup runs DELETE on every state-bearing table."""
        mock_cursor.rowcount = 0
        deleted = storage.cleanup_old_entries(max_age_days=1.0)
        assert deleted >= 0

        # Verify every state-bearing table was cleaned: legacy three plus
        # the unified state_* pair.
        delete_calls = [sql for sql, _ in mock_cursor.executed if "DELETE FROM" in sql]
        assert len(delete_calls) == 5
        tables = {sql.split("DELETE FROM ")[1].split(" ")[0] for sql, _ in mock_cursor.executed if "DELETE FROM" in sql}
        assert tables == {"worker_state", "work_queue", "invocation_registry",
                          "function_state", "function_state_log"}

    # --- Factory Tests ---

    def test_from_env_sql_auth(self) -> None:
        """Test from_env reads SQL auth env vars."""
        env = {
            "VGI_WORKER_SHARED_STORAGE": "azure-sql",
            "VGI_AZURE_SQL_SERVER": "myserver.database.windows.net",
            "VGI_AZURE_SQL_DATABASE": "mydb",
            "VGI_AZURE_SQL_USER": "myuser",
            "VGI_AZURE_SQL_PASSWORD": "mypass",
        }
        with patch.dict("os.environ", env, clear=False):
            s = FunctionStorageAzureSql.from_env()
            assert s._server == "myserver.database.windows.net"
            assert s._database == "mydb"
            assert s._user == "myuser"
            assert s._password == "mypass"

    def test_from_env_managed_identity(self) -> None:
        """Test from_env without user/password uses managed identity path."""
        env = {
            "VGI_WORKER_SHARED_STORAGE": "azure-sql",
            "VGI_AZURE_SQL_SERVER": "myserver.database.windows.net",
            "VGI_AZURE_SQL_DATABASE": "mydb",
        }
        clear_vars = {"VGI_AZURE_SQL_USER": "", "VGI_AZURE_SQL_PASSWORD": ""}
        with patch.dict("os.environ", {**env, **clear_vars}, clear=False):
            s = FunctionStorageAzureSql.from_env()
            assert s._user is None
            assert s._password is None

    def test_from_env_missing_vars(self) -> None:
        """Test from_env raises ValueError when required vars missing."""
        env = {"VGI_WORKER_SHARED_STORAGE": "azure-sql"}
        with patch.dict("os.environ", env, clear=True), pytest.raises(ValueError, match="VGI_AZURE_SQL_SERVER"):
            FunctionStorageAzureSql.from_env()

    # --- Ensure Tables Tests ---

    def test_ensure_tables_creates_schema(self, mock_cursor: _MockCursor) -> None:
        """Test that ensure_tables creates all required tables."""
        mock_conn = _MockConnection(mock_cursor)
        s = FunctionStorageAzureSql(
            server="test.database.windows.net",
            database="testdb",
            user="testuser",
            password="testpass",
        )
        s._connect = lambda: mock_conn  # type: ignore[assignment,return-value]
        s.ensure_tables()

        # Verify table creation SQL was executed
        sql_statements = [sql for sql, _ in mock_cursor.executed]
        assert any("worker_state" in sql and "CREATE TABLE" in sql for sql in sql_statements)
        assert any("work_queue" in sql and "CREATE TABLE" in sql for sql in sql_statements)
        assert any("invocation_registry" in sql and "CREATE TABLE" in sql for sql in sql_statements)
        assert any("idx_work_queue_execution" in sql for sql in sql_statements)


class TestLazyStorageDescriptor:
    """Tests for the _DefaultStorageDescriptor in vgi.function."""

    def test_resolves_to_sqlite_by_default(self) -> None:
        """Test that default resolution produces FunctionStorageSqlite."""
        from vgi.function import _resolve_storage
        from vgi.function_storage import FunctionStorageSqlite

        with patch.dict("os.environ", {}, clear=False):
            # Ensure VGI_WORKER_SHARED_STORAGE is not set
            import os

            os.environ.pop("VGI_WORKER_SHARED_STORAGE", None)
            s = _resolve_storage()
            assert isinstance(s, FunctionStorageSqlite)

    def test_unknown_backend_raises(self) -> None:
        """Test that unknown backend raises ValueError."""
        from vgi.function import _resolve_storage

        env = {"VGI_WORKER_SHARED_STORAGE": "unknown"}
        with patch.dict("os.environ", env), pytest.raises(ValueError, match="unknown"):
            _resolve_storage()

    def test_subclass_override_shadows_descriptor(self) -> None:
        """Test that setting storage on a subclass works."""
        from vgi.function import Function
        from vgi.function_storage import FunctionStorageSqlite

        mock_storage = FunctionStorageSqlite.__new__(FunctionStorageSqlite)

        # Create a subclass with explicit storage
        class MyFunction(Function):
            storage = mock_storage

            def compute(self) -> None:
                pass

        assert MyFunction.storage is mock_storage


class TestFunctionStorageAzureSqlStateUnified:
    """Tests for the unified state_* API on the Azure SQL backend."""

    @pytest.fixture
    def mock_cursor(self) -> _MockCursor:
        """Fresh mock cursor per test."""
        return _MockCursor()

    @pytest.fixture
    def storage(self, mock_cursor: _MockCursor) -> FunctionStorageAzureSql:
        """Storage instance with mocked pymssql.connect."""
        mock_conn = _MockConnection(mock_cursor)
        s = FunctionStorageAzureSql(
            server="t.example", database="db", user="u", password="p",
        )
        s._connect = lambda: mock_conn  # type: ignore[assignment,return-value]
        return s

    def test_state_get_many_emits_in_clause(
        self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor,
    ) -> None:
        """state_get_many issues an IN-clause SELECT against function_state."""
        mock_cursor.set_results([(b"k1", b"v1"), (b"k2", b"v2")])
        result = storage.state_get_many(b"exec1", b"agg", [b"k1", b"k2", b"miss"])
        assert result == [b"v1", b"v2", None]
        sql, params = mock_cursor.executed[0]
        assert "function_state" in sql
        assert "drained_at IS NULL" in sql
        assert params[:2] == (b"exec1", b"agg")
        assert b"k1" in params and b"k2" in params and b"miss" in params

    def test_state_get_many_empty_keys_no_sql(
        self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor,
    ) -> None:
        """Empty key list short-circuits without touching SQL."""
        result = storage.state_get_many(b"exec1", b"agg", [])
        assert result == []
        assert mock_cursor.executed == []

    def test_state_put_many_uses_merge(
        self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor,
    ) -> None:
        """state_put_many issues one MERGE statement per item."""
        mock_cursor.set_results([])  # replay-detection SELECT misses → fresh write
        storage.state_put_many(b"exec1", b"agg", [(b"k1", b"v1"), (b"k2", b"v2")])
        merge_sqls = [s for s, _ in mock_cursor.executed if "MERGE function_state" in s]
        assert len(merge_sqls) == 2  # one MERGE per item

    def test_state_put_many_replay_silent_no_op(
        self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor,
    ) -> None:
        """A replay (first item already present with our attempt_id) is silent."""
        mock_cursor.set_results([(1,)])
        storage.state_put_many(b"exec1", b"agg", [(b"k1", b"v1"), (b"k2", b"v2")])
        merge_sqls = [s for s, _ in mock_cursor.executed if "MERGE function_state" in s]
        assert merge_sqls == []  # nothing written

    def test_state_drain_emits_update_with_output(
        self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor,
    ) -> None:
        """state_drain uses UPDATE..OUTPUT for atomic tombstone-and-read-back."""
        mock_cursor.set_results([])  # replay miss
        result = storage.state_drain(b"exec1", b"agg")
        assert result == []
        update_sqls = [s for s, _ in mock_cursor.executed if "UPDATE function_state" in s]
        assert len(update_sqls) == 1
        assert "OUTPUT inserted." in update_sqls[0]
        assert "drained_by_attempt" in update_sqls[0]

    def test_state_delete_specific_keys(
        self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor,
    ) -> None:
        """state_delete with a key list emits an IN-clause DELETE."""
        mock_cursor.rowcount = 2
        deleted = storage.state_delete(b"exec1", b"agg", [b"k1", b"k2"])
        assert deleted == 2
        sql, params = mock_cursor.executed[0]
        assert "DELETE FROM function_state" in sql
        assert b"k1" in params and b"k2" in params

    def test_state_delete_namespace(
        self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor,
    ) -> None:
        """state_delete with keys=None wipes the whole namespace."""
        mock_cursor.rowcount = 7
        deleted = storage.state_delete(b"exec1", b"agg", None)
        assert deleted == 7
        sql, params = mock_cursor.executed[0]
        assert sql == "DELETE FROM function_state WHERE scope_id = %s AND ns = %s"
        assert params == (b"exec1", b"agg")

    def test_execution_clear_wipes_both_tables(
        self, storage: FunctionStorageAzureSql, mock_cursor: _MockCursor,
    ) -> None:
        """execution_clear deletes from both function_state and function_state_log."""
        mock_cursor.rowcount = 3
        deleted = storage.execution_clear(b"exec1")
        # Two DELETEs, each reports rowcount 3 → 6 total.
        assert deleted == 6
        sqls = [s for s, _ in mock_cursor.executed]
        assert any("DELETE FROM function_state " in s for s in sqls)
        assert any("DELETE FROM function_state_log " in s for s in sqls)

    def test_legacy_aggregate_state_get_now_raises_with_migration_hint(
        self, storage: FunctionStorageAzureSql,
    ) -> None:
        """Legacy aggregate_state_get raises with a clear migration message."""
        with pytest.raises(NotImplementedError, match="state_get_many"):
            storage.aggregate_state_get(b"e", [1, 2])
