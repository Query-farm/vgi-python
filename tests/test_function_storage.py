"""Tests for vgi.function_storage module."""

from pathlib import Path

import pytest

from vgi.function_storage import FunctionStorageSqlite


class TestFunctionStorageSqlite:
    """Tests for FunctionStorageSqlite."""

    @pytest.fixture
    def storage(self, tmp_path: Path) -> FunctionStorageSqlite:
        """Create a temporary storage instance."""
        db_path = str(tmp_path / "test_storage.db")
        return FunctionStorageSqlite(db_path)

    # --- Worker State Tests ---

    def test_worker_put_and_collect(self, storage: FunctionStorageSqlite) -> None:
        """Test storing and collecting worker states."""
        invocation_id = b"inv123"

        # Store states from multiple workers
        storage.worker_put(invocation_id, worker_id=1, state=b"state1")
        storage.worker_put(invocation_id, worker_id=2, state=b"state2")
        storage.worker_put(invocation_id, worker_id=3, state=b"state3")

        # Collect all states
        states = storage.worker_collect(invocation_id)

        assert len(states) == 3
        assert set(states) == {b"state1", b"state2", b"state3"}

        # Verify collect is atomic - second collect should return empty
        states2 = storage.worker_collect(invocation_id)
        assert states2 == []

    def test_worker_put_replaces_existing(self, storage: FunctionStorageSqlite) -> None:
        """Test that worker_put replaces existing state for same worker."""
        invocation_id = b"inv123"

        storage.worker_put(invocation_id, worker_id=1, state=b"old_state")
        storage.worker_put(invocation_id, worker_id=1, state=b"new_state")

        states = storage.worker_collect(invocation_id)
        assert states == [b"new_state"]

    def test_worker_scan_non_destructive(self, storage: FunctionStorageSqlite) -> None:
        """worker_scan returns (worker_id, state) pairs without deleting."""
        invocation_id = b"inv-scan"

        storage.worker_put(invocation_id, worker_id=11, state=b"a")
        storage.worker_put(invocation_id, worker_id=22, state=b"b")

        first = sorted(storage.worker_scan(invocation_id))
        assert first == [(11, b"a"), (22, b"b")]

        # Repeat read returns the same data — non-destructive.
        second = sorted(storage.worker_scan(invocation_id))
        assert second == first

        # And worker_collect after scan still drains.
        collected = sorted(storage.worker_collect(invocation_id))
        assert collected == [b"a", b"b"]
        assert storage.worker_scan(invocation_id) == []

    def test_worker_scan_isolates_executions(self, storage: FunctionStorageSqlite) -> None:
        """worker_scan only returns rows for the given execution_id."""
        storage.worker_put(b"exec-A", worker_id=1, state=b"a1")
        storage.worker_put(b"exec-B", worker_id=1, state=b"b1")

        assert storage.worker_scan(b"exec-A") == [(1, b"a1")]
        assert storage.worker_scan(b"exec-B") == [(1, b"b1")]
        assert storage.worker_scan(b"exec-missing") == []

    # --- Work Queue Tests ---

    def test_queue_push_and_pop(self, storage: FunctionStorageSqlite) -> None:
        """Test pushing and popping work items."""
        invocation_id = b"inv123"

        items = [b"item1", b"item2", b"item3"]
        count = storage.queue_push(invocation_id, items)
        assert count == 3

        # Pop items - must come back in FIFO order
        popped = []
        while (item := storage.queue_pop(invocation_id)) is not None:
            popped.append(item)

        assert popped == [b"item1", b"item2", b"item3"]

    def test_queue_push_empty_list(self, storage: FunctionStorageSqlite) -> None:
        """Test pushing empty list returns 0 (line 337)."""
        invocation_id = b"inv123"
        count = storage.queue_push(invocation_id, [])
        assert count == 0

    def test_queue_pop_empty_queue(self, storage: FunctionStorageSqlite) -> None:
        """Test popping from registered but empty queue returns None."""
        invocation_id = b"inv123"
        # Register the invocation first
        storage.queue_push(invocation_id, [])
        result = storage.queue_pop(invocation_id)
        assert result is None

    def test_queue_pop_never_pushed_returns_none(self, storage: FunctionStorageSqlite) -> None:
        """Test popping an id that was never pushed returns None.

        No distinction from drained queue per the contract.
        """
        unknown_id = b"never_seen_before"
        assert storage.queue_pop(unknown_id) is None

    def test_queue_clear(self, storage: FunctionStorageSqlite) -> None:
        """Test clearing the work queue."""
        invocation_id = b"inv123"

        # Add items
        storage.queue_push(invocation_id, [b"item1", b"item2", b"item3"])

        # Clear the queue
        cleared = storage.queue_clear(invocation_id)
        assert cleared == 3

        # After clear, pop returns None (queue is empty/unknown)
        assert storage.queue_pop(invocation_id) is None

    def test_queue_push_empty_still_registers(self, storage: FunctionStorageSqlite) -> None:
        """Test that pushing empty list still registers invocation_id."""
        invocation_id = b"inv123"
        storage.queue_push(invocation_id, [])

        # Pop should return None (empty but known), not raise
        assert storage.queue_pop(invocation_id) is None

    def test_queue_clear_empty_queue(self, storage: FunctionStorageSqlite) -> None:
        """Test clearing an empty queue returns 0."""
        invocation_id = b"inv123"
        cleared = storage.queue_clear(invocation_id)
        assert cleared == 0

    # --- Cleanup Tests ---

    def test_cleanup_old_entries(self, storage: FunctionStorageSqlite) -> None:
        """Test cleanup doesn't error with no old entries."""
        # Just verify it doesn't raise
        deleted = storage.cleanup_old_entries(max_age_days=0.0)
        # With max_age_days=0, all entries (even fresh ones) would be deleted
        # but we haven't added any
        assert deleted >= 0

    # --- Default Path Tests ---

    def test_default_db_path(self) -> None:
        """Test that default db path is created correctly."""
        # Create storage with default path
        storage = FunctionStorageSqlite()
        assert storage.db_path.endswith("vgi_storage.db")
        assert "vgi" in storage.db_path
