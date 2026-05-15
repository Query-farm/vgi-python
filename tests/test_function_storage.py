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

    # ========================================================================
    # Unified state_* API tests
    # ========================================================================

    def test_state_put_many_get_many_roundtrip(self, storage: FunctionStorageSqlite) -> None:
        """Batched put then batched get returns the values in input-key order."""
        scope = b"exec1"
        ns = b"agg"
        storage.state_put_many(scope, ns, [(b"k1", b"v1"), (b"k2", b"v2"), (b"k3", b"v3")])
        result = storage.state_get_many(scope, ns, [b"k2", b"k3", b"k1", b"missing"])
        assert result == [b"v2", b"v3", b"v1", None]

    def test_state_put_many_overwrites_existing(self, storage: FunctionStorageSqlite) -> None:
        """Re-put on the same (scope, ns, key) replaces the value."""
        scope = b"exec1"
        ns = b"agg"
        storage.state_put_many(scope, ns, [(b"k1", b"old")])
        storage.state_put_many(scope, ns, [(b"k1", b"new")])
        assert storage.state_get_many(scope, ns, [b"k1"]) == [b"new"]

    def test_state_namespaces_are_isolated(self, storage: FunctionStorageSqlite) -> None:
        """Same key in different namespaces is two distinct rows."""
        scope = b"exec1"
        storage.state_put_many(scope, b"ns_a", [(b"k", b"a-val")])
        storage.state_put_many(scope, b"ns_b", [(b"k", b"b-val")])
        assert storage.state_get_many(scope, b"ns_a", [b"k"]) == [b"a-val"]
        assert storage.state_get_many(scope, b"ns_b", [b"k"]) == [b"b-val"]

    def test_state_scopes_are_isolated(self, storage: FunctionStorageSqlite) -> None:
        """Same (ns, key) in different scope_ids is two distinct rows."""
        storage.state_put_many(b"exec1", b"agg", [(b"k", b"v1")])
        storage.state_put_many(b"exec2", b"agg", [(b"k", b"v2")])
        assert storage.state_get_many(b"exec1", b"agg", [b"k"]) == [b"v1"]
        assert storage.state_get_many(b"exec2", b"agg", [b"k"]) == [b"v2"]

    def test_state_scan_returns_all_in_namespace(self, storage: FunctionStorageSqlite) -> None:
        """state_scan emits every (key, value) for one (scope, ns)."""
        scope = b"exec1"
        ns = b"agg"
        storage.state_put_many(scope, ns, [(b"k1", b"v1"), (b"k2", b"v2"), (b"k3", b"v3")])
        storage.state_put_many(scope, b"other_ns", [(b"k1", b"other")])
        rows = sorted(storage.state_scan(scope, ns))
        assert rows == [(b"k1", b"v1"), (b"k2", b"v2"), (b"k3", b"v3")]

    def test_state_scan_is_non_destructive(self, storage: FunctionStorageSqlite) -> None:
        """Two consecutive scans return the same rows."""
        storage.state_put_many(b"exec1", b"agg", [(b"k", b"v")])
        first = storage.state_scan(b"exec1", b"agg")
        second = storage.state_scan(b"exec1", b"agg")
        assert first == second == [(b"k", b"v")]

    def test_state_drain_empties_and_returns_rows(self, storage: FunctionStorageSqlite) -> None:
        """state_drain removes rows from subsequent reads and returns them."""
        scope = b"exec1"
        ns = b"agg"
        storage.state_put_many(scope, ns, [(b"k1", b"v1"), (b"k2", b"v2")])
        drained = sorted(storage.state_drain(scope, ns))
        assert drained == [(b"k1", b"v1"), (b"k2", b"v2")]
        assert storage.state_scan(scope, ns) == []
        assert storage.state_get_many(scope, ns, [b"k1", b"k2"]) == [None, None]

    def test_state_drain_doesnt_touch_other_namespaces(self, storage: FunctionStorageSqlite) -> None:
        """Draining ns A leaves ns B intact."""
        scope = b"exec1"
        storage.state_put_many(scope, b"ns_a", [(b"k", b"a-val")])
        storage.state_put_many(scope, b"ns_b", [(b"k", b"b-val")])
        storage.state_drain(scope, b"ns_a")
        assert storage.state_scan(scope, b"ns_b") == [(b"k", b"b-val")]

    def test_state_delete_specific_keys(self, storage: FunctionStorageSqlite) -> None:
        """state_delete with a key list removes only those keys."""
        scope = b"exec1"
        ns = b"agg"
        storage.state_put_many(scope, ns, [(b"k1", b"v1"), (b"k2", b"v2"), (b"k3", b"v3")])
        deleted = storage.state_delete(scope, ns, [b"k1", b"k3"])
        assert deleted == 2
        assert storage.state_scan(scope, ns) == [(b"k2", b"v2")]

    def test_state_delete_namespace(self, storage: FunctionStorageSqlite) -> None:
        """state_delete with keys=None wipes the namespace."""
        scope = b"exec1"
        storage.state_put_many(scope, b"ns_a", [(b"k1", b"v1"), (b"k2", b"v2")])
        storage.state_put_many(scope, b"ns_b", [(b"k", b"v")])
        deleted = storage.state_delete(scope, b"ns_a")
        assert deleted == 2
        assert storage.state_scan(scope, b"ns_a") == []
        assert storage.state_scan(scope, b"ns_b") == [(b"k", b"v")]

    def test_state_delete_naturally_idempotent(self, storage: FunctionStorageSqlite) -> None:
        """Deleting an already-deleted row returns 0."""
        scope = b"exec1"
        ns = b"agg"
        storage.state_put_many(scope, ns, [(b"k", b"v")])
        assert storage.state_delete(scope, ns, [b"k"]) == 1
        assert storage.state_delete(scope, ns, [b"k"]) == 0

    def test_execution_clear_wipes_all_namespaces(self, storage: FunctionStorageSqlite) -> None:
        """execution_clear deletes every row across every namespace for a scope."""
        scope = b"exec1"
        storage.state_put_many(scope, b"ns_a", [(b"k1", b"v1"), (b"k2", b"v2")])
        storage.state_put_many(scope, b"ns_b", [(b"k", b"v")])
        storage.state_put_many(b"exec2", b"ns_a", [(b"k", b"v")])
        deleted = storage.execution_clear(scope)
        assert deleted == 3
        assert storage.state_scan(scope, b"ns_a") == []
        assert storage.state_scan(scope, b"ns_b") == []
        assert storage.state_scan(b"exec2", b"ns_a") == [(b"k", b"v")]

    def test_execution_clear_naturally_idempotent(self, storage: FunctionStorageSqlite) -> None:
        """Clearing an already-clear execution returns 0."""
        assert storage.execution_clear(b"never-existed") == 0

    def test_state_get_many_empty_keys(self, storage: FunctionStorageSqlite) -> None:
        """Empty key list returns empty result without touching SQL."""
        assert storage.state_get_many(b"exec1", b"agg", []) == []

    def test_state_put_many_empty_items(self, storage: FunctionStorageSqlite) -> None:
        """Empty item list is a no-op."""
        storage.state_put_many(b"exec1", b"agg", [])

    # --- Facade (BoundStorage) tests ---

    def test_facade_state_put_get_roundtrip(self, storage: FunctionStorageSqlite) -> None:
        """BoundStorage.state_put / state_get convenience wrappers work."""
        from vgi.function_storage import BoundStorage

        bs = BoundStorage(storage, b"exec1", attach_opaque_data=b"a")
        bs.state_put(b"agg", b"k1", b"v1")
        assert bs.state_get(b"agg", b"k1") == b"v1"
        assert bs.state_get(b"agg", b"missing") is None

    def test_facade_pack_int_key(self) -> None:
        """pack_int_key is little-endian, signed, 8 bytes — round-trips to int.from_bytes."""
        from vgi.function_storage import BoundStorage

        for n in [0, 1, -1, 2**62, -(2**62)]:
            packed = BoundStorage.pack_int_key(n)
            assert len(packed) == 8
            assert int.from_bytes(packed, "little", signed=True) == n

    # --- state_append / state_log_scan (append-only log) ---

    def test_state_append_then_log_scan_single_key(self, storage: FunctionStorageSqlite) -> None:
        """Appended values come back in append order via state_log_scan."""
        scope = b"exec-log"
        storage.state_append(scope, b"buf", b"k", b"a")
        storage.state_append(scope, b"buf", b"k", b"b")
        storage.state_append(scope, b"buf", b"k", b"c")
        assert storage.state_log_scan(scope, b"buf", b"k") == [b"a", b"b", b"c"]

    def test_state_append_returns_monotone_ordinals_per_key(self, storage: FunctionStorageSqlite) -> None:
        """Per-key ordinals strictly increase across appends."""
        scope = b"exec-log"
        ord1 = storage.state_append(scope, b"buf", b"k", b"a")
        ord2 = storage.state_append(scope, b"buf", b"k", b"b")
        ord3 = storage.state_append(scope, b"buf", b"k", b"c")
        assert ord1 < ord2 < ord3

    def test_state_log_scan_isolates_keys(self, storage: FunctionStorageSqlite) -> None:
        """Logs for distinct keys in the same namespace stay separate."""
        scope = b"exec-log"
        storage.state_append(scope, b"buf", b"k1", b"a")
        storage.state_append(scope, b"buf", b"k2", b"x")
        storage.state_append(scope, b"buf", b"k1", b"b")
        assert storage.state_log_scan(scope, b"buf", b"k1") == [b"a", b"b"]
        assert storage.state_log_scan(scope, b"buf", b"k2") == [b"x"]

    def test_state_log_scan_isolates_namespaces(self, storage: FunctionStorageSqlite) -> None:
        """Logs for the same key in different namespaces stay separate."""
        scope = b"exec-log"
        storage.state_append(scope, b"ns-a", b"k", b"a1")
        storage.state_append(scope, b"ns-b", b"k", b"b1")
        assert storage.state_log_scan(scope, b"ns-a", b"k") == [b"a1"]
        assert storage.state_log_scan(scope, b"ns-b", b"k") == [b"b1"]

    def test_state_log_scan_isolates_scopes(self, storage: FunctionStorageSqlite) -> None:
        """Logs for the same key in different scopes stay separate."""
        storage.state_append(b"exec-A", b"buf", b"k", b"a")
        storage.state_append(b"exec-B", b"buf", b"k", b"b")
        assert storage.state_log_scan(b"exec-A", b"buf", b"k") == [b"a"]
        assert storage.state_log_scan(b"exec-B", b"buf", b"k") == [b"b"]

    def test_state_log_scan_is_non_destructive(self, storage: FunctionStorageSqlite) -> None:
        """Repeated scans return the same data."""
        scope = b"exec-log"
        storage.state_append(scope, b"buf", b"k", b"a")
        storage.state_append(scope, b"buf", b"k", b"b")
        first = storage.state_log_scan(scope, b"buf", b"k")
        second = storage.state_log_scan(scope, b"buf", b"k")
        assert first == second == [b"a", b"b"]

    def test_state_log_scan_empty_returns_empty(self, storage: FunctionStorageSqlite) -> None:
        """Scan of a (scope, ns, key) that was never appended returns []."""
        assert storage.state_log_scan(b"exec-x", b"buf", b"k") == []

    def test_execution_clear_wipes_log(self, storage: FunctionStorageSqlite) -> None:
        """execution_clear removes function_state_log rows for the scope."""
        scope = b"exec-log"
        storage.state_append(scope, b"buf", b"k", b"a")
        storage.state_append(scope, b"buf", b"k", b"b")
        storage.execution_clear(scope)
        assert storage.state_log_scan(scope, b"buf", b"k") == []

    def test_facade_state_append_log_scan_roundtrip(self, storage: FunctionStorageSqlite) -> None:
        """BoundStorage.state_append / state_log_scan wrappers work."""
        from vgi.function_storage import BoundStorage

        bs = BoundStorage(storage, b"exec-log", attach_opaque_data=b"a")
        ord1 = bs.state_append(b"buf", b"k", b"a")
        ord2 = bs.state_append(b"buf", b"k", b"b")
        assert ord1 < ord2
        assert bs.state_log_scan(b"buf", b"k") == [b"a", b"b"]
