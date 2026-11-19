# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for the opt-in FunctionStorage profiler (``VGI_STORAGE_PROFILE``)."""

from __future__ import annotations

import json
import logging
import os
from collections import Counter, defaultdict
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

import vgi._storage_profile as sp
from tests.conftest import SUBPROCESS_FIXTURE_WORKER, filter_non_empty
from vgi.client import Client
from vgi.function_storage import BoundStorage, FunctionStorageSqlite, ShardedSqliteStorage
from vgi.function_storage_cf_do import FunctionStorageCfDo

_PROFILING_ENV = os.environ.get("VGI_STORAGE_PROFILE") == "1"
_WORKER_PY = os.path.join(os.path.dirname(sp.__file__), "worker.py")


def _fresh(tmp_path: Path, name: str = "st.db") -> tuple[sp._StorageProfiler, Any]:
    """A private profiler and an instrumented sqlite subclass recording into it."""
    prof = sp._StorageProfiler()

    class _Sqlite(FunctionStorageSqlite):
        pass

    sp.instrument_class(_Sqlite, prof)
    return prof, _Sqlite(db_path=str(tmp_path / name))


# A stand-in for a Worker RPC: compiled with vgi/worker.py as its filename so the
# profiler classifies it exactly like the real dispatch frames.
_FAKE_WORKER_SRC = """
class Worker:
    def table_buffering_process(self, storage):
        storage.state_get(b"init", b"k")
        storage.state_append(b"partial", b"", b"x" * 100)
"""


def _fake_worker() -> Any:
    ns: dict[str, Any] = {}
    exec(compile(_FAKE_WORKER_SRC, _WORKER_PY, "exec"), ns)
    return ns["Worker"]()


@pytest.mark.skipif(_PROFILING_ENV, reason="asserts the profiler-off state")
class TestZeroCostWhenOff:
    """With VGI_STORAGE_PROFILE unset nothing is wrapped and no frame is walked."""

    def test_backends_not_wrapped(self) -> None:
        """No backend operation carries the profiling wrapper."""
        for cls in (FunctionStorageSqlite, ShardedSqliteStorage, FunctionStorageCfDo):
            for op in sp.STORAGE_OPS:
                fn = getattr(cls, op, None)
                assert fn is None or not getattr(fn, "_vgi_storage_profiled", False), (cls, op)

    def test_profile_backend_class_is_identity(self) -> None:
        """``profile_backend_class`` returns the class untouched."""

        class _B(FunctionStorageSqlite):
            pass

        before = dict(vars(_B))
        assert sp.profile_backend_class(_B) is _B
        assert dict(vars(_B)) == before

    def test_no_frame_walk(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """A storage op never reaches the frame walker or the profiler."""

        def boom(*_a: Any, **_k: Any) -> Any:
            raise AssertionError("profiler touched while disabled")

        monkeypatch.setattr(sp, "_call_context", boom)
        monkeypatch.setattr(sp._profiler, "begin", boom)
        storage = BoundStorage(FunctionStorageSqlite(db_path=str(tmp_path / "s.db")), b"e" * 16)
        storage.state_put(b"ns", b"k", b"v")
        assert storage.state_get(b"ns", b"k") == b"v"
        storage.state_append(b"log", b"", b"x")
        assert [v for _, v in storage.state_log_scan(b"log", b"")] == [b"x"]


class TestAttribution:
    """Call sites, RPC labels and per-invocation counts."""

    def test_call_site_is_first_frame_outside_storage(self, tmp_path: Path) -> None:
        """The site names this test's line, not BoundStorage or the backend."""
        prof, backend = _fresh(tmp_path)
        storage = BoundStorage(backend, b"\x01" * 16)

        def caller() -> int:
            storage.state_put(b"ns", b"k", b"value")
            return caller.__code__.co_firstlineno + 1

        line = caller()
        (site,) = {site for (_op, site) in prof._by_site}
        assert site.endswith(
            f"test_storage_profile.py:{line}:TestAttribution."
            "test_call_site_is_first_frame_outside_storage.<locals>.caller"
        )

    def test_rpc_histogram_counts_calls_per_invocation(self, tmp_path: Path) -> None:
        """Two storage calls per fake process RPC shows as ``{2: n}``."""
        prof, backend = _fresh(tmp_path)
        storage = BoundStorage(backend, b"\x02" * 16)
        worker = _fake_worker()
        for _ in range(5):
            worker.table_buffering_process(storage)
        storage.state_put(b"ns", b"k", b"v")  # outside any RPC
        hist = prof.rpc_histogram()
        assert hist["Worker.table_buffering_process"] == {2: 5}
        sites = Counter({k: s.n for k, s in prof._by_site.items()})
        assert sites[("state_get_many", "vgi/worker.py:4:Worker.table_buffering_process")] == 5
        assert sites[("state_append", "vgi/worker.py:5:Worker.table_buffering_process")] == 5

    def test_nested_backends_count_once(self, tmp_path: Path) -> None:
        """Sharded sqlite delegating to sqlite is one record, not two."""
        prof = sp._StorageProfiler()

        class _Inner(FunctionStorageSqlite):
            pass

        class _Sharded(ShardedSqliteStorage):
            pass

        sp.instrument_class(_Inner, prof)
        sp.instrument_class(_Sharded, prof)
        sharded = _Sharded(str(tmp_path / "sh.db"))
        sharded._inner = _Inner(db_path=str(tmp_path / "sh.db"))
        sharded.state_put_many(b"s" * 16, b"ns", [(b"k", b"v")], shard_key="loc-x")
        assert sum(s.n for s in prof._by_site.values()) == 1


class TestAggregation:
    """Per-scope stats, round trips, lazy results, JSONL and the reports."""

    def test_scope_summary(self, tmp_path: Path) -> None:
        """Counts, bytes and percentiles per (shard, scope)."""
        prof, backend = _fresh(tmp_path)
        scope = b"\x03" * 16
        storage = BoundStorage(backend, scope)
        for i in range(10):
            storage.state_append(b"log", b"", b"x" * 50)
            storage.state_get(b"ns", sp.__name__.encode() + bytes([i]))
        s = prof.scope_summary(storage._shard_key, scope.hex())
        assert s is not None
        assert s["n"] == 20
        assert s["round_trips"] == 20
        assert s["bytes_in"] >= 10 * 50
        assert s["ops"]["state_append"]["n"] == 10
        assert s["ops"]["state_get_many"]["n"] == 10
        assert 0 < s["p50_ms"] <= s["p95_ms"] <= s["max_ms"]
        assert s["max_inflight"] == 1
        assert s["busy_ms"] <= s["span_ms"]

    def test_execution_clear_emits_scope_line(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        """The end of a scope logs its own summary line."""
        prof, backend = _fresh(tmp_path)
        storage = BoundStorage(backend, b"\x04" * 16)
        storage.state_put(b"ns", b"k", b"v")
        with caplog.at_level(logging.WARNING, logger="vgi.storage.profile"):
            storage.execution_clear()
        lines = [json.loads(r.getMessage()) for r in caplog.records if "storage_profile_execution" in r.getMessage()]
        # One line from this profiler (plus one from the global profiler when the
        # suite itself runs with VGI_STORAGE_PROFILE=1).
        assert lines
        assert all(line["scope"] == (b"\x04" * 16).hex() and line["n"] == 2 for line in lines)

    def test_concurrency_math(self) -> None:
        """Max, time-weighted mean, busy wall and span from synthetic times."""
        c = sp._Concurrency()
        c.start(10.0)
        c.start(11.0)
        c.end(12.0)
        c.end(14.0)
        c.start(20.0)
        c.end(21.0)
        s = c.summary()
        assert s["max_inflight"] == 2
        assert s["busy_ms"] == 5000.0  # [10,14] + [20,21]
        assert s["mean_inflight"] == round(6 / 5, 2)  # 1*1 + 2*1 + 1*2 + 1*1 over 5 s
        assert s["span_ms"] == 11000.0

    def test_lazy_result_keeps_round_trips(self) -> None:
        """A generator op (cf-do scans) closes at exhaustion with its pages counted."""
        prof = sp._StorageProfiler()

        class _Paged:
            def state_scan(self, scope_id: bytes, ns: bytes, *, shard_key: str = "") -> Iterator[tuple[bytes, bytes]]:
                for page in range(3):
                    prof.note_round_trip(1000)
                    yield (b"k%d" % page, b"v" * 10)

        sp.instrument_class(_Paged, prof, ops=("state_scan",))
        rows = list(_Paged().state_scan(b"\x05" * 16, b"ns", shard_key="loc-p"))
        assert len(rows) == 3
        s = prof.scope_summary("loc-p", (b"\x05" * 16).hex())
        assert s is not None
        assert s["n"] == 1
        assert s["round_trips"] == 3
        assert s["bytes_out"] == 3 * (2 + 10)

    def test_jsonl_dump(self, tmp_path: Path) -> None:
        """One JSON line per call with the analysis fields."""
        prof = sp._StorageProfiler(jsonl_path=str(tmp_path / "p-{pid}.jsonl"))

        class _S(FunctionStorageSqlite):
            pass

        sp.instrument_class(_S, prof)
        storage = BoundStorage(_S(db_path=str(tmp_path / "j.db")), b"\x06" * 16)
        _fake_worker().table_buffering_process(storage)
        (path,) = list(tmp_path.glob("p-*.jsonl"))
        recs = [json.loads(line) for line in path.read_text().splitlines()]
        assert [r["op"] for r in recs] == ["state_get_many", "state_append"]
        for r in recs:
            assert r["rpc"] == "Worker.table_buffering_process"
            assert r["scope"] == (b"\x06" * 16).hex()
            for k in ("ts", "dur_ms", "site", "rpc_id", "bytes_in", "bytes_out", "round_trips", "inflight", "pid"):
                assert k in r
        assert recs[0]["rpc_id"] == recs[1]["rpc_id"]

    def test_reports(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        """The legacy per-shard JSON line keeps its keys; the table has its sections."""
        prof, backend = _fresh(tmp_path)
        storage = BoundStorage(backend, b"\x07" * 16)
        _fake_worker().table_buffering_process(storage)
        with caplog.at_level(logging.WARNING, logger="vgi.storage.profile"):
            prof.dump()
        (line,) = [json.loads(r.getMessage()) for r in caplog.records]
        assert set(line) == {"msg", "shard", "round_trips", "storage_s", "ops"}
        assert line["msg"] == "storage_profile"
        assert line["round_trips"] == 2
        for op in line["ops"].values():
            assert set(op) == {"n", "total_ms", "avg_ms", "max_ms", "io_kb", "max_call_kb"}
        report = prof.format_report()
        assert "by call site:" in report
        assert "Worker.table_buffering_process: 1 RPCs, 2 calls, 2.00/RPC  [2x1]" in report
        assert "by scope" in report
        assert "concurrency, process" in report

    def test_report_from_jsonl_matches_live(self, tmp_path: Path) -> None:
        """The offline report from the JSONL agrees with the live one on counts."""
        prof = sp._StorageProfiler(jsonl_path=str(tmp_path / "r.jsonl"))

        class _S(FunctionStorageSqlite):
            pass

        sp.instrument_class(_S, prof)
        storage = BoundStorage(_S(db_path=str(tmp_path / "r.db")), b"\x08" * 16)
        worker = _fake_worker()
        for _ in range(3):
            worker.table_buffering_process(storage)
        offline = sp.report_from_jsonl([str(tmp_path / "r.jsonl")])
        assert "storage profile" in offline
        assert "6 calls, 6 round trips" in offline
        assert "Worker.table_buffering_process: 3 RPCs, 6 calls, 2.00/RPC  [2x3]" in offline


def test_end_to_end_distributed_sum(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The profiled worker attributes the buffered sum's storage calls exactly.

    300k rows in 2,048-row batches (147 batches, DuckDB's vector size): every
    ``table_buffering_process`` RPC makes exactly one call, the fixture's
    ``state_append``. The init payload is cached per execution, so its read in
    ``Worker._load_table_buffering_params`` happens at most once per process
    (here never: the init ran in the same worker and seeded the cache).
    """
    profile_file = tmp_path / "profile-{pid}.jsonl"
    monkeypatch.setenv("VGI_STORAGE_PROFILE", "1")
    monkeypatch.setenv("VGI_STORAGE_PROFILE_FILE", str(profile_file))
    monkeypatch.setenv("VGI_WORKER_SQLITE_PATH", str(tmp_path / "storage.db"))
    rows, per_batch = 300_000, 2048
    batches = [
        pa.RecordBatch.from_pydict({"a": list(range(s, min(s + per_batch, rows)))}) for s in range(0, rows, per_batch)
    ]
    assert len(batches) == 147
    with Client(SUBPROCESS_FIXTURE_WORKER, pool=None) as client:
        out = list(
            client.table_buffering_function(
                function_name="sum_all_columns_simple_distributed",
                schema_path=["main"],
                input=iter(batches),
            )
        )
    assert filter_non_empty(out)[0].to_pydict() == {"a": [sum(range(rows))]}

    recs = [json.loads(line) for p in tmp_path.glob("profile-*.jsonl") for line in p.read_text().splitlines()]
    by_site = Counter((r["op"], r["site"].rsplit(":", 2)[0], r["site"].rsplit(":", 1)[1]) for r in recs)
    assert by_site[("state_get_many", "vgi/worker.py", "Worker._load_table_buffering_params")] <= 1
    fixture = ("state_append", "vgi/_test_fixtures/table_in_out.py", "SumAllColumnsSimpleDistributed.process")
    assert by_site[fixture] == 147

    per_rpc: dict[int, list[str]] = defaultdict(list)
    for r in recs:
        if r["rpc"] == "Worker.table_buffering_process":
            per_rpc[r["rpc_id"]].append(r["op"])
    assert len(per_rpc) == 147
    assert all(ops == ["state_append"] for ops in per_rpc.values())
