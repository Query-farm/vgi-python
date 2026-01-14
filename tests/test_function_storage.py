"""Tests for vgi.function_storage module."""

from pathlib import Path

import pytest

from vgi.function_storage import FunctionStorageSqlite, UnknownInvocationError


class TestFunctionStorageSqlite:
    """Tests for FunctionStorageSqlite."""

    @pytest.fixture
    def storage(self, tmp_path: Path) -> FunctionStorageSqlite:
        """Create a temporary storage instance."""
        db_path = str(tmp_path / "test_storage.db")
        return FunctionStorageSqlite(db_path)

    # --- Global State Tests ---

    def test_global_put_and_get(self, storage: FunctionStorageSqlite) -> None:
        """Test basic put and get of global state."""
        value = b"test value"
        key = storage.global_put(value)

        retrieved = storage.global_get(key)
        assert retrieved == value

    def test_global_get_nonexistent_key_raises(
        self, storage: FunctionStorageSqlite
    ) -> None:
        """Test that getting a nonexistent key raises KeyError (line 266)."""
        fake_key = b"nonexistent_key_12345678"

        with pytest.raises(KeyError) as exc_info:
            storage.global_get(fake_key)

        assert "not found" in str(exc_info.value)

    def test_global_delete(self, storage: FunctionStorageSqlite) -> None:
        """Test deleting global state (lines 273-278)."""
        value = b"value to delete"
        key = storage.global_put(value)

        # Verify it exists
        assert storage.global_get(key) == value

        # Delete it
        storage.global_delete(key)

        # Verify it's gone
        with pytest.raises(KeyError):
            storage.global_get(key)

    def test_global_delete_nonexistent_key_no_error(
        self, storage: FunctionStorageSqlite
    ) -> None:
        """Test that deleting a nonexistent key doesn't raise."""
        fake_key = b"nonexistent_key_12345678"
        # Should not raise
        storage.global_delete(fake_key)

    def test_global_exists_true(self, storage: FunctionStorageSqlite) -> None:
        """Test global_exists returns True for existing key (lines 282-290)."""
        value = b"test value"
        key = storage.global_put(value)

        assert storage.global_exists(key) is True

    def test_global_exists_false(self, storage: FunctionStorageSqlite) -> None:
        """Test global_exists returns False for nonexistent key."""
        fake_key = b"nonexistent_key_12345678"
        assert storage.global_exists(fake_key) is False

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

    # --- Work Queue Tests ---

    def test_queue_push_and_pop(self, storage: FunctionStorageSqlite) -> None:
        """Test pushing and popping work items."""
        invocation_id = b"inv123"

        items = [b"item1", b"item2", b"item3"]
        count = storage.queue_push(invocation_id, items)
        assert count == 3

        # Pop items
        popped = []
        while (item := storage.queue_pop(invocation_id)) is not None:
            popped.append(item)

        assert len(popped) == 3
        assert set(popped) == {b"item1", b"item2", b"item3"}

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

    def test_queue_pop_unknown_invocation_raises(
        self, storage: FunctionStorageSqlite
    ) -> None:
        """Test popping from unknown invocation raises error."""
        unknown_id = b"never_seen_before"
        with pytest.raises(UnknownInvocationError):
            storage.queue_pop(unknown_id)

    def test_queue_clear(self, storage: FunctionStorageSqlite) -> None:
        """Test clearing the work queue."""
        invocation_id = b"inv123"

        # Add items
        storage.queue_push(invocation_id, [b"item1", b"item2", b"item3"])

        # Clear the queue
        cleared = storage.queue_clear(invocation_id)
        assert cleared == 3

        # After clear, invocation is unregistered so pop should raise
        with pytest.raises(UnknownInvocationError):
            storage.queue_pop(invocation_id)

    def test_queue_clear_unregisters_invocation(
        self, storage: FunctionStorageSqlite
    ) -> None:
        """Test that queue_clear unregisters the invocation_id."""
        invocation_id = b"inv123"

        # Register by pushing
        storage.queue_push(invocation_id, [b"item1"])

        # Pop should work (known invocation)
        storage.queue_pop(invocation_id)

        # Clear unregisters
        storage.queue_clear(invocation_id)

        # Now pop should raise (unknown after clear)
        with pytest.raises(UnknownInvocationError):
            storage.queue_pop(invocation_id)

    def test_queue_push_empty_still_registers(
        self, storage: FunctionStorageSqlite
    ) -> None:
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
