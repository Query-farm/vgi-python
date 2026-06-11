# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Shared conformance suite for FunctionStorage backends.

One parametrized module run against every in-process SQL backend so they stay
in lockstep on observable behavior — instead of relying on per-backend tests +
code review (which let ``state_scan`` ordering silently diverge before).

It pins the contracts the accumulate primitives depend on: defined scan key
order, half-open range/limit/reverse on ``state_scan`` and ``state_delete``, and
the ``state_counter_*`` family (init-on-absent, accumulation, set/delete).

Azure SQL and the Cloudflare DO are SQL engines too, but need a live
endpoint/emulator; this module covers the in-process backends. The DO's own
vitest suite covers its endpoints + counter_add replay.
"""

from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from vgi.function_storage import (
    FunctionStorageSqlite,
    ShardedSqliteStorage,
)

# Each backend is built fresh per test via a factory taking the test's tmp_path.
BackendFactory = Callable[[Path], object]

_BACKENDS: dict[str, BackendFactory] = {
    "sqlite-memory": lambda _tmp: FunctionStorageSqlite(db_path=":memory:"),
    "sqlite-file": lambda tmp: FunctionStorageSqlite(db_path=str(tmp / "conf.db")),
    "sharded-sqlite": lambda _tmp: ShardedSqliteStorage(db_path=":memory:"),
}

SCOPE = b"scope"
NS = b"ns"


@pytest.fixture(params=list(_BACKENDS), ids=list(_BACKENDS))
def backend(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[object]:
    """Yield a fresh backend instance for each parametrized engine."""
    store = _BACKENDS[request.param](tmp_path)
    try:
        yield store
    finally:
        close = getattr(store, "close", None)
        if close is not None:
            close()


def _put(backend: object, *pairs: tuple[bytes, bytes]) -> None:
    """Upsert ``pairs`` into the shared (SCOPE, NS) namespace."""
    backend.state_put_many(SCOPE, NS, list(pairs))  # type: ignore[attr-defined]


def _keys(rows: object) -> list[bytes]:
    """Project the keys out of a list of (key, value) rows."""
    return [k for k, _ in rows]  # type: ignore[union-attr]


# --- scan: order, range, reverse, limit ---


def test_scan_orders_by_key_ascending(backend: object) -> None:
    """state_scan returns rows in ascending key (memcmp) order."""
    _put(backend, (b"d", b"D"), (b"a", b"A"), (b"c", b"C"), (b"b", b"B"))
    rows = list(backend.state_scan(SCOPE, NS))  # type: ignore[attr-defined]
    assert _keys(rows) == [b"a", b"b", b"c", b"d"]
    assert dict(rows)[b"c"] == b"C"  # values travel with keys


def test_scan_reverse(backend: object) -> None:
    """reverse=True yields descending key order."""
    _put(backend, (b"a", b"A"), (b"b", b"B"), (b"c", b"C"))
    rows = list(backend.state_scan(SCOPE, NS, reverse=True))  # type: ignore[attr-defined]
    assert _keys(rows) == [b"c", b"b", b"a"]


def test_scan_half_open_range(backend: object) -> None:
    """[start, end) is start-inclusive, end-exclusive."""
    _put(backend, (b"a", b"A"), (b"b", b"B"), (b"c", b"C"), (b"d", b"D"))
    rows = list(backend.state_scan(SCOPE, NS, start=b"b", end=b"d"))  # type: ignore[attr-defined]
    assert _keys(rows) == [b"b", b"c"]


def test_scan_open_bounds(backend: object) -> None:
    """A single open bound scans from/to the edge."""
    _put(backend, (b"a", b"A"), (b"b", b"B"), (b"c", b"C"))
    assert _keys(list(backend.state_scan(SCOPE, NS, start=b"b"))) == [b"b", b"c"]  # type: ignore[attr-defined]
    assert _keys(list(backend.state_scan(SCOPE, NS, end=b"b"))) == [b"a"]  # type: ignore[attr-defined]


def test_scan_limit(backend: object) -> None:
    """Limit caps the row count, honoring order."""
    _put(backend, (b"a", b"A"), (b"b", b"B"), (b"c", b"C"))
    assert _keys(list(backend.state_scan(SCOPE, NS, limit=2))) == [b"a", b"b"]  # type: ignore[attr-defined]
    assert _keys(list(backend.state_scan(SCOPE, NS, reverse=True, limit=1))) == [b"c"]  # type: ignore[attr-defined]


def test_scan_empty_namespace(backend: object) -> None:
    """Scanning an empty namespace yields nothing."""
    assert list(backend.state_scan(SCOPE, NS)) == []  # type: ignore[attr-defined]


# --- delete: range + mutual exclusion ---


def test_delete_half_open_range(backend: object) -> None:
    """Ranged delete removes [start, end) and returns the count."""
    _put(backend, (b"a", b"A"), (b"b", b"B"), (b"c", b"C"), (b"d", b"D"))
    removed = backend.state_delete(SCOPE, NS, start=b"b", end=b"d")  # type: ignore[attr-defined]
    assert removed == 2
    assert _keys(list(backend.state_scan(SCOPE, NS))) == [b"a", b"d"]  # type: ignore[attr-defined]


def test_delete_range_open_end(backend: object) -> None:
    """An open end deletes from start to the edge."""
    _put(backend, (b"a", b"A"), (b"b", b"B"), (b"c", b"C"))
    removed = backend.state_delete(SCOPE, NS, start=b"b")  # type: ignore[attr-defined]
    assert removed == 2
    assert _keys(list(backend.state_scan(SCOPE, NS))) == [b"a"]  # type: ignore[attr-defined]


def test_delete_range_is_idempotent(backend: object) -> None:
    """Re-deleting an already-empty range removes nothing."""
    _put(backend, (b"a", b"A"), (b"b", b"B"))
    assert backend.state_delete(SCOPE, NS, start=b"a", end=b"c") == 2  # type: ignore[attr-defined]
    assert backend.state_delete(SCOPE, NS, start=b"a", end=b"c") == 0  # type: ignore[attr-defined]


def test_delete_keys_and_range_mutually_exclusive(backend: object) -> None:
    """Passing both keys and a range raises ValueError."""
    _put(backend, (b"a", b"A"))
    with pytest.raises(ValueError, match="mutually exclusive"):
        backend.state_delete(SCOPE, NS, [b"a"], start=b"a")  # type: ignore[attr-defined]


def test_delete_keys_still_works(backend: object) -> None:
    """The key-list delete path is unchanged."""
    _put(backend, (b"a", b"A"), (b"b", b"B"), (b"c", b"C"))
    assert backend.state_delete(SCOPE, NS, [b"a", b"c"]) == 2  # type: ignore[attr-defined]
    assert _keys(list(backend.state_scan(SCOPE, NS))) == [b"b"]  # type: ignore[attr-defined]


def test_delete_whole_namespace_still_works(backend: object) -> None:
    """keys=None with no range wipes the namespace."""
    _put(backend, (b"a", b"A"), (b"b", b"B"))
    assert backend.state_delete(SCOPE, NS) == 2  # type: ignore[attr-defined]
    assert list(backend.state_scan(SCOPE, NS)) == []  # type: ignore[attr-defined]


# --- counters ---


def test_counter_absent_reads_zero(backend: object) -> None:
    """An unset counter reads as 0."""
    assert backend.state_counter_get(SCOPE, NS, b"k") == 0  # type: ignore[attr-defined]


def test_counter_add_accumulates_and_returns_new(backend: object) -> None:
    """Add returns the post-add value and accumulates across calls."""
    assert backend.state_counter_add(SCOPE, NS, b"k", 5) == 5  # type: ignore[attr-defined]
    assert backend.state_counter_add(SCOPE, NS, b"k", 3) == 8  # type: ignore[attr-defined]
    assert backend.state_counter_get(SCOPE, NS, b"k") == 8  # type: ignore[attr-defined]


def test_counter_add_negative(backend: object) -> None:
    """A negative delta subtracts."""
    backend.state_counter_add(SCOPE, NS, b"k", 10)  # type: ignore[attr-defined]
    assert backend.state_counter_add(SCOPE, NS, b"k", -4) == 6  # type: ignore[attr-defined]


def test_counter_set_overwrites(backend: object) -> None:
    """Set replaces the stored value."""
    backend.state_counter_add(SCOPE, NS, b"k", 5)  # type: ignore[attr-defined]
    backend.state_counter_set(SCOPE, NS, b"k", 100)  # type: ignore[attr-defined]
    assert backend.state_counter_get(SCOPE, NS, b"k") == 100  # type: ignore[attr-defined]


def test_counter_set_on_absent(backend: object) -> None:
    """Set initializes an absent counter."""
    backend.state_counter_set(SCOPE, NS, b"k", 42)  # type: ignore[attr-defined]
    assert backend.state_counter_get(SCOPE, NS, b"k") == 42  # type: ignore[attr-defined]


def test_counter_delete(backend: object) -> None:
    """Delete resets to absent (0) and is a no-op if already gone."""
    backend.state_counter_add(SCOPE, NS, b"k", 7)  # type: ignore[attr-defined]
    backend.state_counter_delete(SCOPE, NS, b"k")  # type: ignore[attr-defined]
    assert backend.state_counter_get(SCOPE, NS, b"k") == 0  # type: ignore[attr-defined]
    backend.state_counter_delete(SCOPE, NS, b"k")  # type: ignore[attr-defined]


def test_counters_are_independent_by_key(backend: object) -> None:
    """Distinct keys hold independent counters."""
    backend.state_counter_add(SCOPE, NS, b"a", 1)  # type: ignore[attr-defined]
    backend.state_counter_add(SCOPE, NS, b"b", 2)  # type: ignore[attr-defined]
    assert backend.state_counter_get(SCOPE, NS, b"a") == 1  # type: ignore[attr-defined]
    assert backend.state_counter_get(SCOPE, NS, b"b") == 2  # type: ignore[attr-defined]


def test_counter_separate_from_state_kv(backend: object) -> None:
    """Counters live in their own table and don't collide with state K/V."""
    backend.state_put_many(SCOPE, NS, [(b"k", b"opaque")])  # type: ignore[attr-defined]
    backend.state_counter_add(SCOPE, NS, b"k", 9)  # type: ignore[attr-defined]
    assert backend.state_counter_get(SCOPE, NS, b"k") == 9  # type: ignore[attr-defined]
    assert backend.state_get_many(SCOPE, NS, [b"k"]) == [b"opaque"]  # type: ignore[attr-defined]
