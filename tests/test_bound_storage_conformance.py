# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Shared conformance suite for the BoundStorage facade.

``test_function_storage_conformance.py`` pins the raw ``FunctionStorage``
protocol (explicit ``scope_id``). This module pins the **facade** layer function
code actually uses: ``BoundStorage`` binds ``scope_id``, resolves ``shard_key``,
coerces namespaces, and exposes the ``transaction()`` view, the queue/Arrow
helpers, and the counter facade. Run against every in-process SQL backend so the
facade stays in lockstep; Azure / the DO need a live endpoint and are covered by
their own suites.
"""

from collections.abc import Callable, Iterator
from pathlib import Path

import pyarrow as pa
import pytest

from vgi.function_storage import (
    BoundStorage,
    FrameworkNS,
    FunctionStorageSqlite,
    ShardedSqliteStorage,
)

BackendFactory = Callable[[Path], object]

_BACKENDS: dict[str, BackendFactory] = {
    "sqlite-memory": lambda _tmp: FunctionStorageSqlite(db_path=":memory:"),
    "sqlite-file": lambda tmp: FunctionStorageSqlite(db_path=str(tmp / "conf.db")),
    "sharded-sqlite": lambda _tmp: ShardedSqliteStorage(db_path=":memory:"),
}

EXEC = b"exec-default"
NS = b"ns"


class _Harness:
    """A backend plus a factory for BoundStorage handles over it."""

    def __init__(self, store: object) -> None:
        self.store = store

    def bound(self, execution_id: bytes = EXEC, *, attach_plaintext: bytes | None = None) -> BoundStorage:
        """Build a BoundStorage over the shared store."""
        return BoundStorage(self.store, execution_id, attach_plaintext=attach_plaintext)  # type: ignore[arg-type]


@pytest.fixture(params=list(_BACKENDS), ids=list(_BACKENDS))
def harness(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[_Harness]:
    """Yield a harness wrapping a fresh backend for each parametrized engine."""
    store = _BACKENDS[request.param](tmp_path)
    try:
        yield _Harness(store)
    finally:
        close = getattr(store, "close", None)
        if close is not None:
            close()


# --- scope binding & isolation ---


def test_scope_isolation_between_execution_ids(harness: _Harness) -> None:
    """Two BoundStorage with different execution_ids don't see each other."""
    a, b = harness.bound(b"A"), harness.bound(b"B")
    a.state_put(NS, b"k", b"va")
    a.counter_add(NS, b"c", 5)
    a.state_append(NS, b"log", b"x")
    assert b.state_get(NS, b"k") is None
    assert b.counter_get(NS, b"c") == 0
    assert b.state_log_scan(NS, b"log") == []
    # The owning scope still sees its own data.
    assert a.state_get(NS, b"k") == b"va"
    assert a.counter_get(NS, b"c") == 5


def test_same_execution_id_shares_state(harness: _Harness) -> None:
    """A second handle with the same execution_id sees the same data."""
    harness.bound(EXEC).state_put(NS, b"k", b"v")
    assert harness.bound(EXEC).state_get(NS, b"k") == b"v"


# --- namespace coercion ---


def test_reserved_ns_prefix_rejected(harness: _Harness) -> None:
    """A user namespace under the reserved _vgi/ prefix raises ValueError."""
    bs = harness.bound()
    with pytest.raises(ValueError, match="reserved"):
        bs.state_put(b"_vgi/mine", b"k", b"v")
    with pytest.raises(ValueError, match="reserved"):
        bs.counter_add(b"_vgi/mine", b"c", 1)


def test_framework_ns_members_pass_through_and_are_distinct(harness: _Harness) -> None:
    """FrameworkNS members are accepted and don't collide with each other."""
    bs = harness.bound()
    bs.state_put(FrameworkNS.TIO_STATE, b"k", b"tio")
    bs.state_put(FrameworkNS.AGGREGATE_STATE, b"k", b"agg")
    assert bs.state_get(FrameworkNS.TIO_STATE, b"k") == b"tio"
    assert bs.state_get(FrameworkNS.AGGREGATE_STATE, b"k") == b"agg"


def test_non_bytes_ns_type_error(harness: _Harness) -> None:
    """A non-bytes, non-FrameworkNS namespace raises TypeError."""
    bs = harness.bound()
    with pytest.raises(TypeError):
        bs.state_put(123, b"k", b"v")  # type: ignore[arg-type]


# --- state K/V facade round-trips ---


def test_state_get_put_and_many(harness: _Harness) -> None:
    """state_put/get and the batched _many forms round-trip."""
    bs = harness.bound()
    bs.state_put(NS, b"a", b"A")
    assert bs.state_get(NS, b"a") == b"A"
    assert bs.state_get(NS, b"missing") is None
    bs.state_put_many(NS, [(b"b", b"B"), (b"c", b"C")])
    assert bs.state_get_many(NS, [b"b", b"missing", b"c"]) == [b"B", None, b"C"]


def test_state_scan_range_reverse_limit_through_facade(harness: _Harness) -> None:
    """The facade threads start/end/reverse/limit into the scan."""
    bs = harness.bound()
    bs.state_put_many(NS, [(b"a", b"A"), (b"b", b"B"), (b"c", b"C"), (b"d", b"D")])
    keys = lambda **kw: [k for k, _ in bs.state_scan(NS, **kw)]  # noqa: E731
    assert keys() == [b"a", b"b", b"c", b"d"]
    assert keys(reverse=True) == [b"d", b"c", b"b", b"a"]
    assert keys(start=b"b", end=b"d") == [b"b", b"c"]
    assert keys(limit=2) == [b"a", b"b"]


def test_state_drain_reads_and_clears(harness: _Harness) -> None:
    """state_drain returns everything in the ns and empties it."""
    bs = harness.bound()
    bs.state_put_many(NS, [(b"a", b"A"), (b"b", b"B")])
    drained = dict(bs.state_drain(NS))
    assert drained == {b"a": b"A", b"b": b"B"}
    assert list(bs.state_scan(NS)) == []


def test_state_delete_keys_range_and_all(harness: _Harness) -> None:
    """The facade supports key-list, ranged, and whole-namespace delete."""
    bs = harness.bound()
    bs.state_put_many(NS, [(b"a", b"A"), (b"b", b"B"), (b"c", b"C"), (b"d", b"D")])
    assert bs.state_delete(NS, [b"a"]) == 1
    assert bs.state_delete(NS, start=b"b", end=b"d") == 2  # b, c
    assert [k for k, _ in bs.state_scan(NS)] == [b"d"]
    assert bs.state_delete(NS) == 1  # wipe remainder
    assert list(bs.state_scan(NS)) == []


def test_append_log_scan_cursor(harness: _Harness) -> None:
    """state_append ordinals are monotonic and state_log_scan cursors."""
    bs = harness.bound()
    o1 = bs.state_append(NS, b"k", b"a")
    o2 = bs.state_append(NS, b"k", b"b")
    assert o1 < o2
    assert bs.state_log_scan(NS, b"k") == [(o1, b"a"), (o2, b"b")]
    assert bs.state_log_scan(NS, b"k", after_id=o1, limit=1) == [(o2, b"b")]


def test_counter_facade_roundtrip(harness: _Harness) -> None:
    """counter_get/add/set/delete behave through the facade."""
    bs = harness.bound()
    assert bs.counter_get(NS, b"c") == 0
    assert bs.counter_add(NS, b"c", 5) == 5
    assert bs.counter_add(NS, b"c", 3) == 8
    bs.counter_set(NS, b"c", 100)
    assert bs.counter_get(NS, b"c") == 100
    bs.counter_delete(NS, b"c")
    assert bs.counter_get(NS, b"c") == 0


def test_execution_clear_wipes_state_log_counters_and_is_scoped(harness: _Harness) -> None:
    """execution_clear wipes state + log + counters for its scope only."""
    bs = harness.bound(b"A")
    other = harness.bound(b"B")
    bs.state_put(NS, b"k", b"v")
    bs.state_append(NS, b"log", b"x")
    bs.counter_add(NS, b"c", 5)
    other.state_put(NS, b"k", b"keep")
    cleared = bs.execution_clear()
    assert cleared == 3  # 1 state + 1 log + 1 counter
    assert bs.state_get(NS, b"k") is None
    assert bs.state_log_scan(NS, b"log") == []
    assert bs.counter_get(NS, b"c") == 0
    # A different execution_id is untouched.
    assert other.state_get(NS, b"k") == b"keep"


# --- transaction() view ---


def test_transaction_view_roundtrip_isolation_and_clear(harness: _Harness) -> None:
    """The transaction() view round-trips, is isolated from the exec scope, and clears."""
    bs = harness.bound()
    txn = bs.transaction(b"txn-1")
    txn.put_one(b"watermark", b"42")
    txn.put([(b"a", b"A"), (b"b", b"B")])
    assert txn.get_one(b"watermark") == b"42"
    assert txn.get([b"a", b"b"]) == [b"A", b"B"]
    # Transaction scope is separate from the execution scope.
    assert bs.state_get(b"txn", b"watermark") is None
    # A different transaction id is isolated.
    assert bs.transaction(b"txn-2").get_one(b"watermark") is None
    txn.clear()
    assert txn.get_one(b"watermark") is None


# --- queue facade ---


def test_queue_facade_fifo(harness: _Harness) -> None:
    """queue_push/pop/clear preserve FIFO order and clear drains."""
    bs = harness.bound()
    assert bs.queue_push([b"i1", b"i2", b"i3"]) == 3
    assert bs.queue_pop() == b"i1"
    assert bs.queue_clear() == 2
    assert bs.queue_pop() is None


def test_queue_batch_roundtrip(harness: _Harness) -> None:
    """queue_push_batches / queue_pop_batch round-trip RecordBatches."""
    bs = harness.bound()
    batch = pa.record_batch({"x": pa.array([1, 2, 3], type=pa.int64())})
    bs.queue_push_batches([batch])
    popped = bs.queue_pop_batch()
    assert popped is not None
    assert popped.equals(batch)
    assert bs.queue_pop_batch() is None


# --- static helpers ---


def test_serialize_deserialize_record_batch(harness: _Harness) -> None:
    """The Arrow IPC (de)serialization helpers round-trip a batch."""
    batch = pa.record_batch({"x": pa.array([1, 2], type=pa.int64())})
    blob = BoundStorage.serialize_record_batch(batch)
    assert BoundStorage.deserialize_record_batch(blob).equals(batch)


def test_pack_int_key(harness: _Harness) -> None:
    """pack_int_key is the canonical 8-byte little-endian signed encoding."""
    assert BoundStorage.pack_int_key(5) == (5).to_bytes(8, "little", signed=True)
    assert BoundStorage.pack_int_key(-1) == (-1).to_bytes(8, "little", signed=True)


# --- shard isolation through the facade (sharded backend only) ---


def test_shard_isolation_via_attach_plaintext(tmp_path: Path) -> None:
    """Distinct attach_plaintext routes to distinct shards even at one execution_id.

    The facade resolves shard_key from the attach UUID prefix; ShardedSqliteStorage
    partitions on it. Same execution_id + different attach → isolated; same attach → shared.
    """
    import uuid

    store = ShardedSqliteStorage(db_path=":memory:")
    try:
        u1, u2 = uuid.uuid4().bytes, uuid.uuid4().bytes
        a = BoundStorage(store, EXEC, attach_plaintext=u1 + b"catalog")
        b = BoundStorage(store, EXEC, attach_plaintext=u2 + b"catalog")
        a.state_put(NS, b"k", b"va")
        a.counter_add(NS, b"c", 7)
        assert b.state_get(NS, b"k") is None  # different shard
        assert b.counter_get(NS, b"c") == 0
        # A second handle on the SAME attach shares the shard.
        a2 = BoundStorage(store, EXEC, attach_plaintext=u1 + b"catalog")
        assert a2.state_get(NS, b"k") == b"va"
        assert a2.counter_get(NS, b"c") == 7
    finally:
        store.close()
