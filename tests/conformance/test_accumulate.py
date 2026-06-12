# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance tests for ``vgi/test/sql/integration/accumulate/``.

Mirrors the C++ sqllogictest suite driving the accumulate fixture
(``vgi/_test_fixtures/accumulate/worker.py``): bind validation, accumulation
semantics, result modes, per-ATTACH scoping, TTL/max_row_size eviction, and
clear — the end-to-end exercise of the ``BoundStorage`` interfaces.

The persistent collection lives on the worker's ``FunctionStorage``
(``cls.storage``); these tests inject a throwaway file-backed
``FunctionStorageSqlite`` for it, and build a real execution-scoped
``BoundStorage`` for the per-query ``params.storage`` (the buffering
Sink->Combine->Source handoff). End-to-end behavior over the real VGI
transport is covered by the sqllogictest suite in the C++ repo.
"""

from __future__ import annotations

import contextlib
from datetime import timedelta
from types import SimpleNamespace

import pyarrow as pa
import pytest

from vgi._test_fixtures.accumulate import worker as m
from vgi.function_storage import BoundStorage, FunctionStorage, FunctionStorageSqlite

INPUT_SCHEMA = pa.schema([pa.field("x", pa.int64()), pa.field("label", pa.string())])
OUTPUT_SCHEMA = m._output_schema(INPUT_SCHEMA)

SCOPE_A = b"session-a"
SCOPE_B = b"session-b"

_FUNCS = (m.AccumulateFunction, m.AccumulateReadFunction, m.AccumulateClearFunction)
_exec_counter = [0]


def _set_storage(fs: FunctionStorage) -> None:
    for cls in _FUNCS:
        cls.storage = fs


@pytest.fixture(autouse=True)
def _storage(tmp_path):
    """Inject a throwaway file-backed FunctionStorage for the persistent collection."""
    path = str(tmp_path / "fs.db")
    _set_storage(FunctionStorageSqlite(db_path=path))
    yield SimpleNamespace(path=path)
    for cls in _FUNCS:
        with contextlib.suppress(AttributeError):
            del cls.storage


class FakeOut:
    """Minimal OutputCollector stand-in capturing emitted batches."""

    def __init__(self) -> None:
        """Start with no batches and the stream unfinished."""
        self.batches: list[pa.RecordBatch] = []
        self.finished = False

    def emit(self, batch: pa.RecordBatch) -> None:
        """Capture one emitted batch."""
        self.batches.append(batch)

    def finish(self) -> None:
        """Mark the stream finished."""
        self.finished = True


def _exec_storage():
    """Build a fresh execution-scoped BoundStorage (mimics a distinct query)."""
    _exec_counter[0] += 1
    eid = f"exec-{_exec_counter[0]}".encode()
    return eid, BoundStorage(m.AccumulateFunction.storage, eid)


def _bind_params(name, input_schema=INPUT_SCHEMA, opaque=SCOPE_A):
    return SimpleNamespace(
        args=SimpleNamespace(name=name, data=None, ttl=None, max_row_size=0, result="all"),
        bind_call=SimpleNamespace(input_schema=input_schema),
        attach_opaque_data=opaque,
    )


def _buf_params(name, *, ttl=None, max_row_size=0, result="all", output_schema=OUTPUT_SCHEMA, opaque=SCOPE_A):
    eid, storage = _exec_storage()
    return SimpleNamespace(
        args=SimpleNamespace(name=name, data=None, ttl=ttl, max_row_size=max_row_size, result=result),
        output_schema=output_schema,
        attach_opaque_data=opaque,
        storage=storage,
        execution_id=eid,
    )


def _input_batch(xs):
    return pa.RecordBatch.from_arrays(
        [pa.array(xs, pa.int64()), pa.array([f"r{x}" for x in xs], pa.string())],
        schema=INPUT_SCHEMA,
    )


def _drain(fn) -> FakeOut:
    out = FakeOut()
    guard = 0
    while not out.finished:
        fn(out)
        guard += 1
        assert guard < 1_000_000, "stream did not terminate"
    return out


def _run(name, *, batches, ttl=None, max_row_size=0, result="all", opaque=SCOPE_A) -> pa.Table:
    resp = m.AccumulateFunction.on_bind(_bind_params(name, opaque=opaque))
    pp = _buf_params(
        name, ttl=ttl, max_row_size=max_row_size, result=result, output_schema=resp.output_schema, opaque=opaque
    )
    for b in batches:
        assert m.AccumulateFunction.process(b, pp) == pp.execution_id
    fids = m.AccumulateFunction.combine([pp.execution_id], pp)
    assert len(fids) == 1
    state = m.AccumulateFunction.initial_finalize_state(fids[0], pp)
    out = _drain(lambda o: m.AccumulateFunction.finalize(pp, fids[0], state, o))
    return (
        pa.Table.from_batches(out.batches, schema=resp.output_schema)
        if out.batches
        else resp.output_schema.empty_table()
    )


def _call(name, xs, *, ttl=None, max_row_size=0, result="all", opaque=SCOPE_A) -> pa.Table:
    return _run(name, batches=[_input_batch(xs)], ttl=ttl, max_row_size=max_row_size, result=result, opaque=opaque)


def _read_batches(name, *, opaque=SCOPE_A) -> list[pa.RecordBatch]:
    resp = m.AccumulateReadFunction.on_bind(_bind_params(name, opaque=opaque))
    pp = _buf_params(name, output_schema=resp.output_schema, opaque=opaque)
    state = m.AccumulateReadFunction.initial_state(pp)
    return _drain(lambda o: m.AccumulateReadFunction.process(pp, state, o)).batches


def _read(name, *, opaque=SCOPE_A) -> pa.Table:
    resp = m.AccumulateReadFunction.on_bind(_bind_params(name, opaque=opaque))
    batches = _read_batches(name, opaque=opaque)
    return pa.Table.from_batches(batches, schema=resp.output_schema) if batches else resp.output_schema.empty_table()


def _clear(name, *, opaque=SCOPE_A) -> pa.RecordBatch:
    pp = _buf_params(name, output_schema=m.CLEAR_SCHEMA, opaque=opaque)
    state = m.AccumulateClearFunction.initial_state(pp)
    out = FakeOut()
    m.AccumulateClearFunction.process(pp, state, out)
    m.AccumulateClearFunction.process(pp, state, out)  # second tick -> finish
    assert out.finished and len(out.batches) == 1
    return out.batches[0]


def _xs(table) -> list[int]:
    return sorted(table.column("x").to_pylist())


def _total(name, opaque=SCOPE_A) -> int:
    ps = m._store(m.AccumulateFunction.storage, opaque)
    sch = m._get_schema(ps, name.encode())
    return 0 if sch is None else m._read_collection(ps, name.encode(), sch).num_rows


# --- helpers ---------------------------------------------------------------


def test_output_schema_appends_timestamp() -> None:
    """The output schema is the input columns plus a microsecond _timestamp."""
    assert OUTPUT_SCHEMA.names == ["x", "label", "_timestamp"]
    assert OUTPUT_SCHEMA.field("_timestamp").type == pa.timestamp("us")


def test_interval_to_timedelta_months_days_nanos() -> None:
    """MonthDayNano intervals convert with months approximated as 30 days."""
    iv = pa.scalar((1, 2, 3_000_000_000), type=pa.month_day_nano_interval()).as_py()
    assert m._interval_to_timedelta(iv) == timedelta(days=32, seconds=3)


# --- on_bind ---------------------------------------------------------------


def test_on_bind_adds_timestamp_and_creates_collection() -> None:
    """Binding pins the collection schema and appends the _timestamp column."""
    resp = m.AccumulateFunction.on_bind(_bind_params("a"))
    assert resp.output_schema.names == ["x", "label", "_timestamp"]
    assert m._get_schema(m._store(m.AccumulateFunction.storage, SCOPE_A), b"a") is not None


def test_on_bind_rejects_reserved_timestamp_column() -> None:
    """An input column named _timestamp is rejected at bind."""
    schema = pa.schema([pa.field("_timestamp", pa.int64())])
    with pytest.raises(ValueError, match="reserved '_timestamp' column"):
        m.AccumulateFunction.on_bind(_bind_params("a", schema))


def test_on_bind_rejects_schema_mismatch_for_same_name() -> None:
    """An input schema differing from the pinned one errors at bind."""
    m.AccumulateFunction.on_bind(_bind_params("a"))
    other = pa.schema([pa.field("x", pa.int64())])  # missing 'label'
    with pytest.raises(ValueError, match="does not match"):
        m.AccumulateFunction.on_bind(_bind_params("a", other))


@pytest.mark.parametrize("name", ["", "   "])
def test_on_bind_rejects_blank_name(name) -> None:
    """Empty or blank collection names are rejected by all three functions."""
    with pytest.raises(ValueError, match="non-empty"):
        m.AccumulateFunction.on_bind(_bind_params(name))
    with pytest.raises(ValueError, match="non-empty"):
        m.AccumulateReadFunction.on_bind(_bind_params(name))
    with pytest.raises(ValueError, match="non-empty"):
        m.AccumulateClearFunction.on_bind(_bind_params(name))


def test_on_bind_rejects_oversized_name() -> None:
    """Names beyond the byte limit are rejected at bind."""
    name = "x" * (m._MAX_NAME_BYTES + 1)
    with pytest.raises(ValueError, match="at most"):
        m.AccumulateFunction.on_bind(_bind_params(name))
    with pytest.raises(ValueError, match="at most"):
        m.AccumulateClearFunction.on_bind(_bind_params(name))


# --- accumulation semantics ------------------------------------------------


def test_returns_input_rows_with_timestamp() -> None:
    """A first call returns its own rows with a non-null _timestamp."""
    t = _call("a", [1, 2])
    assert _xs(t) == [1, 2]
    assert t.column("_timestamp").null_count == 0


def test_accumulates_across_calls_including_new_rows() -> None:
    """A second call returns the prior rows plus the newly added ones."""
    _call("a", [1, 2])
    assert _xs(_call("a", [3])) == [1, 2, 3]


def test_one_timestamp_per_call_even_with_multiple_input_batches() -> None:
    """All rows staged across multiple input batches share one call timestamp."""
    t = _run("a", batches=[_input_batch([1, 2]), _input_batch([3, 4])])
    assert _xs(t) == [1, 2, 3, 4]
    assert len(set(t.column("_timestamp").to_pylist())) == 1


def test_collections_are_independent_by_name() -> None:
    """Accumulating under one name does not affect another."""
    _call("a", [1, 2])
    assert _xs(_call("b", [9])) == [9]
    assert _total("a") == 2


# --- result option ---------------------------------------------------------


def test_result_all_returns_full_collection() -> None:
    """result='all' returns the entire collection."""
    _call("a", [1, 2])
    assert _xs(_call("a", [3], result="all")) == [1, 2, 3]


def test_result_new_returns_only_added_rows() -> None:
    """result='new' returns just this call's rows while still appending."""
    _call("a", [1, 2])
    assert _xs(_call("a", [3, 4], result="new")) == [3, 4]
    assert _total("a") == 4


def test_result_none_returns_nothing_but_appends() -> None:
    """result='none' emits no rows but the append still happens."""
    t = _call("a", [1, 2], result="none")
    assert t.num_rows == 0
    assert _total("a") == 2


# --- accumulate_read -------------------------------------------------------


def test_read_returns_collection_without_mutating() -> None:
    """accumulate_read returns the rows and leaves the collection unchanged."""
    _call("a", [1, 2])
    _call("a", [3])
    assert _xs(_read("a")) == [1, 2, 3]
    assert _total("a") == 3
    assert _xs(_read("a")) == [1, 2, 3]


def test_read_unknown_name_raises() -> None:
    """Reading a name never accumulated in this session errors at bind."""
    with pytest.raises(ValueError, match="no accumulation named"):
        m.AccumulateReadFunction.on_bind(_bind_params("ghost"))


def test_read_is_scoped_to_attach_session() -> None:
    """Reads only see the collection of their own attach scope."""
    _call("a", [1, 2], opaque=SCOPE_A)
    _call("a", [9], opaque=SCOPE_B)
    assert _xs(_read("a", opaque=SCOPE_A)) == [1, 2]
    assert _xs(_read("a", opaque=SCOPE_B)) == [9]


# --- persistence & scoping -------------------------------------------------


def test_persists_across_reconnect(tmp_path) -> None:
    """Collections survive a worker restart (fresh handle on the same DB file)."""
    _call("a", [1, 2])
    # Simulate a worker restart: a fresh storage handle on the same DB file.
    _set_storage(FunctionStorageSqlite(db_path=str(tmp_path / "fs.db")))
    assert _xs(_call("a", [3])) == [1, 2, 3]


def test_attach_scopes_are_isolated() -> None:
    """Two attach scopes accumulate independently under the same name."""
    _call("a", [1, 2], opaque=SCOPE_A)
    assert _xs(_call("a", [9], opaque=SCOPE_B)) == [9]
    assert _xs(_call("a", [3], opaque=SCOPE_A)) == [1, 2, 3]


# --- eviction --------------------------------------------------------------


def test_max_row_size_keeps_newest_fifo_across_calls() -> None:
    """max_row_size drops the oldest rows first across calls."""
    _call("a", [1, 2])
    _call("a", [3])
    t = _call("a", [4], max_row_size=2)
    assert _xs(t) == [3, 4]  # newest two by ingest time (distinct timestamps)
    assert _total("a") == 2


def test_max_row_size_trims_to_cap_within_a_call() -> None:
    """max_row_size enforces the cap even within a single call's segment."""
    # One call's rows share a timestamp, so which two survive is unspecified —
    # assert the cap is enforced, not the exact rows.
    t = _call("a", [1, 2, 3, 4, 5], max_row_size=2)
    assert t.num_rows == 2
    assert set(t.column("x").to_pylist()).issubset({1, 2, 3, 4, 5})
    assert _total("a") == 2


def test_ttl_zero_keeps_only_current_call() -> None:
    """A zero TTL evicts everything older than the current call."""
    _call("a", [1, 2])
    zero = pa.scalar((0, 0, 0), type=pa.month_day_nano_interval()).as_py()
    assert _xs(_call("a", [3], ttl=zero)) == [3]


def test_ttl_large_keeps_everything() -> None:
    """A large TTL retains all rows."""
    _call("a", [1, 2])
    big = pa.scalar((0, 365, 0), type=pa.month_day_nano_interval()).as_py()
    assert _xs(_call("a", [3], ttl=big)) == [1, 2, 3]


# --- large input & batched output ------------------------------------------


def test_large_input_round_trips_all_rows() -> None:
    """A 10k-row input is accumulated and returned in full."""
    t = _call("big", list(range(10_000)))
    assert t.num_rows == 10_000
    assert _xs(t)[:3] == [0, 1, 2]
    assert len(set(t.column("_timestamp").to_pylist())) == 1


def test_output_returned_in_bounded_batches(monkeypatch) -> None:
    """Output is staged and drained in batches bounded by OUT_BATCH_ROWS."""
    monkeypatch.setattr(m, "OUT_BATCH_ROWS", 100)
    _call("big", list(range(1000)), result="none")
    batches = _read_batches("big")
    assert all(b.num_rows <= 100 for b in batches)
    assert sum(b.num_rows for b in batches) == 1000
    assert len(batches) >= 5


def test_many_small_appends_round_trip() -> None:
    """200 single-row appends (200 segments) read back in order and in full."""
    for i in range(200):  # 200 single-row appends -> 200 segments
        _call("drip", [i], result="none")
    assert _total("drip") == 200
    assert _xs(_read("drip")) == list(range(200))


# --- accumulate_clear ------------------------------------------------------


def test_clear_returns_rows_removed_and_drops_collection() -> None:
    """Clearing reports the removed row count and drops the pinned schema."""
    _call("a", [1, 2, 3])
    batch = _clear("a")
    assert batch.schema == m.CLEAR_SCHEMA
    assert batch.column("name").to_pylist() == ["a"]
    assert batch.column("rows_cleared").to_pylist() == [3]
    assert m._get_schema(m._store(m.AccumulateFunction.storage, SCOPE_A), b"a") is None


def test_clear_unknown_name_returns_zero() -> None:
    """Clearing an unknown name reports zero rows removed."""
    assert _clear("nope").column("rows_cleared").to_pylist() == [0]


def test_clear_is_scoped_to_attach_session() -> None:
    """Clearing in one attach scope leaves another scope's collection intact."""
    _call("a", [1, 2], opaque=SCOPE_A)
    _call("a", [9], opaque=SCOPE_B)
    assert _clear("a", opaque=SCOPE_B).column("rows_cleared").to_pylist() == [1]
    assert _total("a", SCOPE_A) == 2


def test_clear_then_reaccumulate_allows_new_schema() -> None:
    """After a clear, the name is free to accumulate a different schema."""
    _call("a", [1, 2])
    _clear("a")
    other = pa.schema([pa.field("y", pa.float64())])
    resp = m.AccumulateFunction.on_bind(_bind_params("a", other))
    assert resp.output_schema.names == ["y", "_timestamp"]
