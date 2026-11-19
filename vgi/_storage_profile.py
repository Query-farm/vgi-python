# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Backend-agnostic FunctionStorage profiler (opt-in).

Enable with ``VGI_STORAGE_PROFILE=1``. Every storage backend (sqlite,
azure-sql, cloudflare-do, and the debug sharded sqlite) passes its class to
`profile_backend_class` at import time; when profiling is on that wraps each
`FunctionStorage` operation so one logical call is one record. When it is off
the function returns at once: no wrapper is installed, no frame is walked and
no clock is read, so the hot path is byte-for-byte the unprofiled code.

Each record carries:

* ``op`` -- the FunctionStorage method (``state_get_many``, ``state_append``...),
  which is also the cloudflare-do endpoint it issues;
* ``site`` -- the first frame outside the storage modules
  (``vgi/worker.py:4111:Worker._load_table_buffering_params``);
* ``rpc`` / ``rpc_id`` -- the VGI RPC the call ran under: the outermost
  ``vgi/worker.py`` frame (else the outermost ``vgi`` frame), and an id that is
  distinct per invocation, so "2 calls per ``table_buffering_process``" is a
  histogram rather than an inference;
* ``shard`` / ``scope`` -- the attach shard key and the execution (or
  transaction) id the call is scoped to;
* bytes in (request payload) and out (response payload), latency, and
  ``round_trips``: the HTTP round trips the call made. The cloudflare-do client
  reports its pages through `note_round_trip`; for a backend that does not,
  one call counts as one round trip.

In-flight concurrency is tracked online per process, per shard and per scope:
the maximum and time-weighted mean number of calls in flight, and the wall time
with at least one call in flight against the span from the first call to the
last. When the busy time is close to the span, the scope's wall time is
storage latency times its call count.

Output, all under the ``vgi.storage.profile`` logger:

* the per-shard ``storage_profile`` JSON line, every 5 s and at exit (the
  format predates this module's rewrite and is kept);
* a ``storage_profile_execution`` JSON line per scope when its
  ``execution_clear`` returns -- the end of a query or transaction;
* a text summary table at exit (`format_report`);
* with ``VGI_STORAGE_PROFILE_FILE=path`` (``{pid}`` is substituted), one JSON
  line per call for offline analysis. ``python -m vgi._storage_profile_report
  FILE...`` rebuilds the summary table from those files -- the dependable route
  for a server, which a SIGTERM stops without running ``atexit``.
"""

from __future__ import annotations

import atexit
import bisect
import functools
import itertools
import json
import logging
import os
import sys
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from types import FrameType
from typing import Any

_PROFILE_ON = os.environ.get("VGI_STORAGE_PROFILE") == "1"
_PROFILE_FILE = os.environ.get("VGI_STORAGE_PROFILE_FILE") or None
_profile_logger = logging.getLogger("vgi.storage.profile")

# The FunctionStorage protocol operations. Each one is exactly one
# cloudflare-do endpoint of the same name (paginated scans may take several
# round trips, which the cf-do client reports via note_round_trip).
STORAGE_OPS: tuple[str, ...] = (
    "queue_push",
    "queue_pop",
    "queue_clear",
    "state_get_many",
    "state_put_many",
    "state_scan",
    "state_drain",
    "state_delete",
    "execution_clear",
    "state_append",
    "state_log_scan",
    "state_counter_get",
    "state_counter_add",
    "state_counter_set",
    "state_counter_delete",
)

# Per-key latency samples kept for percentiles; past this a key keeps its
# count/total/max exactly and its percentiles over the first N samples.
_MAX_SAMPLES = 20_000
# Bounded per-scope and per-RPC-invocation tables, so a long-running profiled
# worker does not grow without limit. Oldest entries are dropped first.
_MAX_SCOPES = 2_000
_MAX_RPCS = 200_000

_VGI_DIR = os.path.dirname(os.path.abspath(__file__))
_VGI_PARENT = os.path.dirname(_VGI_DIR)
_STORAGE_MODULES = frozenset(
    {
        "function_storage.py",
        "function_storage_cf_do.py",
        "function_storage_azure_sql.py",
        "_storage_profile.py",
    }
)


# ---------------------------------------------------------------------------
# Frame classification (only ever reached with profiling on)
# ---------------------------------------------------------------------------


_STDLIB_DIR = os.path.dirname(os.path.abspath(os.__file__))

# _classify kinds.
_K_STORAGE = 0  # a storage module: skipped when choosing the call site
_K_VGI = 1  # vgi framework code: part of the RPC
_K_WORKER = 2  # vgi/worker.py: part of the RPC, and names it
_K_USER = 3  # user / fixture code: transparent (a function's process() runs inside the RPC)
_K_LIBRARY = 4  # stdlib or site-packages: the boundary (vgi-rpc's dispatch, a thread pool)


@functools.lru_cache(maxsize=4096)
def _classify(filename: str) -> tuple[str, int]:
    """Return ``(display_path, kind)`` for a code object's filename."""
    path = os.path.abspath(filename)
    if path.startswith(_VGI_DIR + os.sep):
        rel = os.path.relpath(path, _VGI_PARENT)
        base = os.path.basename(path)
        if os.path.dirname(path) == _VGI_DIR and base in _STORAGE_MODULES:
            return rel, _K_STORAGE
        if rel.startswith(os.path.join("vgi", "_test_fixtures") + os.sep):
            return rel, _K_USER
        return rel, _K_WORKER if rel == os.path.join("vgi", "worker.py") else _K_VGI
    parts = path.split(os.sep)
    for marker in ("site-packages", "dist-packages"):
        if marker in parts:
            return os.sep.join(parts[parts.index(marker) + 1 :]), _K_LIBRARY
    if path.startswith(_STDLIB_DIR + os.sep) or filename.startswith("<"):
        return os.sep.join(parts[-2:]), _K_LIBRARY
    return os.sep.join(parts[-3:]), _K_USER


def _frame_label(frame: FrameType) -> str:
    code = frame.f_code
    return f"{_classify(code.co_filename)[0]}:{frame.f_lineno}:{code.co_qualname}"


def _call_context(start: FrameType | None) -> tuple[str, str, FrameType | None]:
    """Walk outward from ``start``: the call site, the RPC label and entry frame.

    The call site is the first frame not in a storage module. The RPC is the
    innermost run of vgi frames around the call: walking outward, it ends at
    the first library frame (vgi-rpc's dispatch, a thread pool) once a vgi
    frame has been seen, so a long-lived serve loop further out never counts.
    Its outermost frame is the entry, which identifies the invocation; the
    label prefers the outermost ``vgi/worker.py`` frame
    (``Worker.table_buffering_process``) over a MetaWorker or stream-state
    wrapper.
    """
    site = ""
    entry: FrameType | None = None
    worker_frame: FrameType | None = None
    f = start
    while f is not None:
        _, kind = _classify(f.f_code.co_filename)
        if not site and kind != _K_STORAGE:
            site = _frame_label(f)
        if kind == _K_LIBRARY and entry is not None:
            break
        if kind in (_K_VGI, _K_WORKER):
            entry = f
            if kind == _K_WORKER:
                worker_frame = f
        f = f.f_back
    label_frame = worker_frame or entry
    rpc = label_frame.f_code.co_qualname if label_frame is not None else ""
    return site, rpc, entry


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


@dataclass
class _Stat:
    """Counts, bytes and latency for one aggregation key."""

    n: int = 0
    round_trips: int = 0
    total_s: float = 0.0
    max_s: float = 0.0
    bytes_in: int = 0
    bytes_out: int = 0
    max_call_bytes: int = 0
    samples: list[float] = field(default_factory=list)

    def add(self, seconds: float, round_trips: int, bytes_in: int, bytes_out: int) -> None:
        self.n += 1
        self.round_trips += round_trips
        self.total_s += seconds
        self.max_s = max(self.max_s, seconds)
        self.bytes_in += bytes_in
        self.bytes_out += bytes_out
        self.max_call_bytes = max(self.max_call_bytes, bytes_in, bytes_out)
        if len(self.samples) < _MAX_SAMPLES:
            bisect.insort(self.samples, seconds)

    def pct_ms(self, q: float) -> float:
        if not self.samples:
            return 0.0
        i = min(len(self.samples) - 1, int(q * len(self.samples)))
        return round(self.samples[i] * 1000, 2)

    def summary(self) -> dict[str, Any]:
        return {
            "n": self.n,
            "round_trips": self.round_trips,
            "total_ms": round(self.total_s * 1000, 2),
            "mean_ms": round(self.total_s / self.n * 1000, 2) if self.n else 0,
            "p50_ms": self.pct_ms(0.50),
            "p95_ms": self.pct_ms(0.95),
            "max_ms": round(self.max_s * 1000, 2),
            "bytes_in": self.bytes_in,
            "bytes_out": self.bytes_out,
        }


@dataclass
class _Concurrency:
    """Online in-flight tracker: max, time-weighted mean, busy wall, span."""

    inflight: int = 0
    max_inflight: int = 0
    busy_s: float = 0.0
    area_s: float = 0.0
    first_t: float = 0.0
    last_t: float = 0.0
    last_end_t: float = 0.0

    def _advance(self, now: float) -> None:
        if self.inflight > 0:
            dt = now - self.last_t
            self.busy_s += dt
            self.area_s += dt * self.inflight
        self.last_t = now

    def start(self, now: float) -> int:
        if not self.first_t:
            self.first_t = now
        self._advance(now)
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        return self.inflight

    def end(self, now: float) -> None:
        self._advance(now)
        self.inflight -= 1
        self.last_end_t = now

    def summary(self) -> dict[str, Any]:
        span = max(0.0, self.last_end_t - self.first_t)
        return {
            "max_inflight": self.max_inflight,
            "mean_inflight": round(self.area_s / self.busy_s, 2) if self.busy_s else 0,
            "busy_ms": round(self.busy_s * 1000, 1),
            "span_ms": round(span * 1000, 1),
            "busy_pct": round(100 * self.busy_s / span, 1) if span else 0,
        }


@dataclass
class _Scope:
    """Everything recorded for one (shard, scope_id) pair."""

    shard: str
    scope: str
    stat: _Stat = field(default_factory=_Stat)
    conc: _Concurrency = field(default_factory=_Concurrency)
    by_op: dict[str, _Stat] = field(default_factory=dict)
    by_site: dict[tuple[str, str], int] = field(default_factory=dict)


class _CallRecord:
    """One in-progress logical storage call."""

    __slots__ = (
        "bytes_in",
        "bytes_out",
        "inflight",
        "op",
        "round_trips",
        "rpc",
        "rpc_id",
        "scope",
        "shard",
        "site",
        "t0",
        "wall0",
        "wire_bytes",
    )

    def __init__(self, op: str, shard: str, scope: str, site: str, rpc: str, rpc_id: int, bytes_in: int) -> None:
        self.op = op
        self.shard = shard
        self.scope = scope
        self.site = site
        self.rpc = rpc
        self.rpc_id = rpc_id
        self.bytes_in = bytes_in
        self.bytes_out = 0
        self.round_trips = 0
        self.wire_bytes = 0
        self.inflight = 0
        self.t0 = 0.0
        self.wall0 = 0.0


class _StorageProfiler:
    def __init__(self, jsonl_path: str | None = None) -> None:
        self._lock = threading.Lock()
        self._tls = threading.local()
        self._rpc_ids = itertools.count(1)
        # Legacy per-shard view: (shard, op) -> _Stat.
        self._shard_ops: dict[tuple[str, str], _Stat] = {}
        self._by_site: dict[tuple[str, str], _Stat] = {}
        self._process = _Concurrency()
        self._shards: dict[str, _Concurrency] = {}
        self._scopes: OrderedDict[tuple[str, str], _Scope] = OrderedDict()
        # rpc_id -> [label, storage calls]; folded into _rpc_done when evicted.
        self._rpcs: OrderedDict[int, list[Any]] = OrderedDict()
        self._rpc_done: dict[str, dict[int, int]] = {}
        self._jsonl_fd: int | None = None
        self._jsonl_path = jsonl_path
        self._started = False

    # -- recording -----------------------------------------------------------

    def _rpc_for(self, entry: FrameType | None, label: str) -> int:
        """Return a per-invocation id for the RPC whose entry frame is ``entry``.

        The thread keeps a strong reference to the last entry frame, so a later
        RPC cannot be handed a recycled frame address and be mistaken for it.
        """
        tls = self._tls
        if entry is None:
            return 0
        if getattr(tls, "entry", None) is entry:
            return int(tls.rpc_id)
        tls.entry = entry
        tls.rpc_id = next(self._rpc_ids)
        with self._lock:
            self._rpcs[tls.rpc_id] = [label, 0]
            while len(self._rpcs) > _MAX_RPCS:
                _, (old_label, n) = self._rpcs.popitem(last=False)
                hist = self._rpc_done.setdefault(old_label, {})
                hist[n] = hist.get(n, 0) + 1
        return int(tls.rpc_id)

    def begin(
        self, op: str, shard: str, scope: bytes | str, bytes_in: int, frame: FrameType | None
    ) -> _CallRecord | None:
        """Open a record for a logical call; ``None`` when already inside one."""
        tls = self._tls
        if getattr(tls, "current", None) is not None:
            return None  # nested backend call (e.g. sharded -> sqlite): outermost wins
        site, rpc, entry = _call_context(frame)
        rpc_id = self._rpc_for(entry, rpc)
        scope_s = scope.hex() if isinstance(scope, (bytes, bytearray)) else str(scope)
        rec = _CallRecord(op, shard or "", scope_s, site, rpc, rpc_id, bytes_in)
        tls.current = rec
        rec.wall0 = time.time()
        rec.t0 = time.monotonic()
        with self._lock:
            rec.inflight = self._process.start(rec.t0)
            self._shards.setdefault(rec.shard, _Concurrency()).start(rec.t0)
            self._scope(rec.shard, rec.scope).conc.start(rec.t0)
        return rec

    def suspend(self) -> _CallRecord | None:
        """Detach the current record from this thread (a lazy result escapes)."""
        rec: _CallRecord | None = getattr(self._tls, "current", None)
        self._tls.current = None
        return rec

    def resume(self, rec: _CallRecord) -> None:
        """Re-attach ``rec`` so round trips made while iterating count to it."""
        self._tls.current = rec

    def note_round_trip(self, wire_bytes: int) -> None:
        """Count one transport round trip against the current logical call.

        A round trip outside any logical call (a direct backend ``_post`` from
        a test) has nothing to attach to and is not counted.
        """
        rec: _CallRecord | None = getattr(self._tls, "current", None)
        if rec is not None:
            rec.round_trips += 1
            rec.wire_bytes += wire_bytes

    def end(self, rec: _CallRecord, bytes_out: int) -> None:
        """Close ``rec`` and fold it into every aggregate."""
        t1 = time.monotonic()
        if getattr(self._tls, "current", None) is rec:
            self._tls.current = None
        rec.bytes_out += bytes_out
        seconds = t1 - rec.t0
        rts = rec.round_trips or 1
        with self._lock:
            self._process.end(t1)
            self._shards.setdefault(rec.shard, _Concurrency()).end(t1)
            sc = self._scope(rec.shard, rec.scope)
            sc.conc.end(t1)
            sc.stat.add(seconds, rts, rec.bytes_in, rec.bytes_out)
            sc.by_op.setdefault(rec.op, _Stat()).add(seconds, rts, rec.bytes_in, rec.bytes_out)
            sc.by_site[(rec.op, rec.site)] = sc.by_site.get((rec.op, rec.site), 0) + 1
            wire = max(rec.wire_bytes, rec.bytes_in, rec.bytes_out)
            self._shard_ops.setdefault((rec.shard, rec.op), _Stat()).add(seconds, rts, wire, 0)
            self._by_site.setdefault((rec.op, rec.site), _Stat()).add(seconds, rts, rec.bytes_in, rec.bytes_out)
            if rec.rpc_id in self._rpcs:
                self._rpcs[rec.rpc_id][1] += 1
        if self._jsonl_path is not None:
            self._write_jsonl(rec, seconds, rts)
        if rec.op == "execution_clear":
            self._emit_scope(rec.shard, rec.scope)

    def _scope(self, shard: str, scope: str) -> _Scope:
        key = (shard, scope)
        sc = self._scopes.get(key)
        if sc is None:
            sc = self._scopes[key] = _Scope(shard, scope)
            while len(self._scopes) > _MAX_SCOPES:
                self._scopes.popitem(last=False)
        return sc

    def _write_jsonl(self, rec: _CallRecord, seconds: float, rts: int) -> None:
        line = json.dumps(
            {
                "ts": round(rec.wall0, 6),
                "dur_ms": round(seconds * 1000, 3),
                "op": rec.op,
                "shard": rec.shard,
                "scope": rec.scope,
                "site": rec.site,
                "rpc": rec.rpc,
                "rpc_id": rec.rpc_id,
                "bytes_in": rec.bytes_in,
                "bytes_out": rec.bytes_out,
                "round_trips": rts,
                "wire_bytes": rec.wire_bytes,
                "inflight": rec.inflight,
                "pid": os.getpid(),
                "tid": threading.get_ident(),
            }
        )
        with self._lock:
            if self._jsonl_fd is None:
                assert self._jsonl_path is not None
                path = self._jsonl_path.replace("{pid}", str(os.getpid()))
                self._jsonl_fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
            # One O_APPEND write per line keeps lines whole across processes.
            os.write(self._jsonl_fd, (line + "\n").encode())

    # -- reporting -----------------------------------------------------------

    def rpc_histogram(self) -> dict[str, dict[int, int]]:
        """Storage calls per RPC invocation: ``{label: {calls: invocations}}``.

        Only RPCs that made at least one storage call are seen.
        """
        with self._lock:
            out = {k: dict(v) for k, v in self._rpc_done.items()}
            for label, n in self._rpcs.values():
                hist = out.setdefault(label, {})
                hist[n] = hist.get(n, 0) + 1
        return out

    def scope_summary(self, shard: str, scope: str) -> dict[str, Any] | None:
        """Summary for one (shard, scope) pair, or ``None`` if unseen."""
        with self._lock:
            sc = self._scopes.get((shard, scope))
            if sc is None:
                return None
            return {
                "shard": sc.shard,
                "scope": sc.scope,
                **sc.stat.summary(),
                **sc.conc.summary(),
                "ops": {op: s.summary() for op, s in sorted(sc.by_op.items(), key=lambda kv: -kv[1].n)},
                "sites": [
                    {"op": op, "site": site, "n": n}
                    for (op, site), n in sorted(sc.by_site.items(), key=lambda kv: -kv[1])
                ],
            }

    def _emit_scope(self, shard: str, scope: str) -> None:
        summary = self.scope_summary(shard, scope)
        if summary is not None:
            _profile_logger.warning(json.dumps({"msg": "storage_profile_execution", **summary}))

    def dump(self) -> None:
        """Log the per-shard ``storage_profile`` JSON line (format kept stable)."""
        with self._lock:
            by_shard: dict[str, dict[str, _Stat]] = {}
            for (shard, op), s in self._shard_ops.items():
                by_shard.setdefault(shard, {})[op] = s
            snapshot = {
                shard: {
                    op: (s.n, s.round_trips, s.total_s, s.max_s, s.bytes_in, s.max_call_bytes) for op, s in ops.items()
                }
                for shard, ops in by_shard.items()
            }
        for shard, ops in snapshot.items():
            round_trips = sum(e[1] for e in ops.values())
            storage_s = sum(e[2] for e in ops.values())
            op_summary = {
                op: {
                    "n": e[0],
                    "total_ms": round(e[2] * 1000, 1),
                    "avg_ms": round(e[2] / e[0] * 1000, 1) if e[0] else 0,
                    "max_ms": round(e[3] * 1000, 1),
                    "io_kb": round(e[4] / 1024, 1),
                    # largest single body (req or resp) -- headroom vs
                    # provider size caps.
                    "max_call_kb": round(e[5] / 1024, 1),
                }
                for op, e in sorted(ops.items(), key=lambda kv: -kv[1][2])
            }
            _profile_logger.warning(
                json.dumps(
                    {
                        "msg": "storage_profile",
                        "shard": shard,
                        "round_trips": round_trips,
                        "storage_s": round(storage_s, 2),
                        "ops": op_summary,
                    }
                )
            )

    def format_report(self) -> str:
        """Human-readable summary: by call site, by RPC, by scope, concurrency."""
        hist = self.rpc_histogram()
        with self._lock:
            sites = sorted(self._by_site.items(), key=lambda kv: -kv[1].n)
            process = self._process.summary()
            shards = {k: v.summary() for k, v in self._shards.items()}
            scopes = [
                (sc.shard, sc.scope, sc.stat.summary(), sc.conc.summary()) for sc in self._scopes.values() if sc.stat.n
            ]
            total = sum(s.n for _, s in sites)
            rts = sum(s.round_trips for _, s in sites)
        lines = [f"storage profile (pid {os.getpid()}): {total} calls, {rts} round trips"]
        lines.append("")
        lines.append("by call site:")
        lines.append(f"  {'n':>7} {'mean_ms':>8} {'p95_ms':>7} {'in_B':>9} {'out_B':>9}  op @ site")
        for (op, site), s in sites:
            lines.append(
                f"  {s.n:7} {s.total_s / s.n * 1000:8.2f} {s.pct_ms(0.95):7.2f} "
                f"{s.bytes_in // s.n:9} {s.bytes_out // s.n:9}  {op} @ {site}"
            )
        lines.append("")
        lines.append("storage calls per RPC invocation (calls x invocations):")
        for label, h in sorted(hist.items(), key=lambda kv: -sum(k * v for k, v in kv[1].items())):
            dist = ", ".join(f"{k}x{v}" for k, v in sorted(h.items()))
            n_rpcs = sum(h.values())
            n_calls = sum(k * v for k, v in h.items())
            lines.append(f"  {label}: {n_rpcs} RPCs, {n_calls} calls, {n_calls / n_rpcs:.2f}/RPC  [{dist}]")
        lines.append("")
        lines.append("by scope (shard / scope id):")
        for shard, scope, st, cc in scopes:
            lines.append(
                f"  {shard or '-'} / {scope[:16]}: {st['n']} calls, {st['round_trips']} rt, "
                f"in {st['bytes_in']} B, out {st['bytes_out']} B, mean {st['mean_ms']} ms, "
                f"p50 {st['p50_ms']} ms, p95 {st['p95_ms']} ms; inflight max {cc['max_inflight']} "
                f"mean {cc['mean_inflight']}; busy {cc['busy_ms']} of {cc['span_ms']} ms ({cc['busy_pct']}%)"
            )
        lines.append("")
        lines.append(
            f"concurrency, process: inflight max {process['max_inflight']} mean {process['mean_inflight']}; "
            f"busy {process['busy_ms']} of {process['span_ms']} ms ({process['busy_pct']}%)"
        )
        for shard, cc in sorted(shards.items()):
            lines.append(
                f"concurrency, shard {shard or '-'}: inflight max {cc['max_inflight']} mean {cc['mean_inflight']}; "
                f"busy {cc['busy_ms']} of {cc['span_ms']} ms ({cc['busy_pct']}%)"
            )
        return "\n".join(lines)

    def _final(self) -> None:
        self.dump()
        if self._by_site:
            _profile_logger.warning("%s", self.format_report())

    def start(self, interval: float = 5.0) -> None:
        if self._started:
            return
        self._started = True

        def _loop() -> None:
            while True:
                time.sleep(interval)
                self.dump()

        threading.Thread(target=_loop, daemon=True, name="vgi-storage-profile").start()
        atexit.register(self._final)


_profiler = _StorageProfiler(jsonl_path=_PROFILE_FILE)
if _PROFILE_ON:
    _profiler.start()


def _byte_size(obj: object) -> int:
    """Best-effort serialized size of a storage payload, without consuming generators.

    bytes -> len; list/tuple -> summed size of elements (recursing one level so
    ``[(k, v), ...]`` and ``[item, ...]`` are both counted); everything else
    (ints, None, namespaces, generators/iterators) -> 0.
    """
    if isinstance(obj, (bytes, bytearray)):
        return len(obj)
    if isinstance(obj, (list, tuple)):
        total = 0
        for item in obj:
            if isinstance(item, (bytes, bytearray)):
                total += len(item)
            elif isinstance(item, tuple):
                total += sum(len(x) for x in item if isinstance(x, (bytes, bytearray)))
        return total
    return 0


def io_call_bytes(args: tuple[Any, ...], kwargs: dict[str, Any], result: object) -> int:
    """Largest single-direction body for one storage call.

    Writes carry the payload in the args (items / value / appended item);
    reads carry it in the result. Provider request/response size caps apply
    per direction, so the relevant figure is the max of the two.
    """
    request = sum(_byte_size(a) for a in args) + sum(_byte_size(v) for v in kwargs.values())
    return max(request, _byte_size(result))


class _ProfiledIter:
    """Wrap a lazy backend result so the call closes when it is exhausted.

    The cloudflare-do scans are generators: their round trips happen while the
    caller iterates, after the method has returned. Re-attaching the record on
    each ``next`` keeps those round trips (and the bytes read) on the call.
    """

    def __init__(self, profiler: _StorageProfiler, rec: _CallRecord, inner: Iterator[Any]) -> None:
        self._profiler = profiler
        self._rec: _CallRecord | None = rec
        self._inner = inner

    def __iter__(self) -> _ProfiledIter:
        return self

    def __next__(self) -> Any:
        rec = self._rec
        if rec is None:
            raise StopIteration
        self._profiler.resume(rec)
        try:
            item = next(self._inner)
        except BaseException:
            self.close()
            raise
        self._profiler.suspend()
        rec.bytes_out += _byte_size(item) if isinstance(item, tuple) else _byte_size([item])
        return item

    def close(self) -> None:
        rec, self._rec = self._rec, None
        if rec is not None:
            self._profiler.end(rec, 0)

    def __del__(self) -> None:
        self.close()


def _shard_of(kwargs: dict[str, Any]) -> str:
    return str(kwargs.get("shard_key", "") or "")


def instrument_class(cls: type, profiler: _StorageProfiler, ops: tuple[str, ...] = STORAGE_OPS) -> type:
    """Wrap ``cls``'s FunctionStorage operations to record into ``profiler``.

    Unconditional (used by tests); backends call `profile_backend_class`.

    Args:
        cls: The backend class; its operations are replaced in place.
        profiler: Where records go.
        ops: Operation names to wrap; ones the class lacks are skipped.

    Returns:
        ``cls``, for use as a decorator.
    """
    for name in ops:
        fn = getattr(cls, name, None)
        if fn is None or getattr(fn, "_vgi_storage_profiler", None) is profiler:
            continue
        setattr(cls, name, _wrap_op(name, fn, profiler))
    return cls


def _wrap_op(op: str, fn: Callable[..., Any], profiler: _StorageProfiler) -> Callable[..., Any]:
    @functools.wraps(fn)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        scope = args[0] if args else kwargs.get("execution_id", kwargs.get("scope_id", b""))
        bytes_in = sum(_byte_size(a) for a in args[1:]) + sum(
            _byte_size(v) for k, v in kwargs.items() if k != "shard_key"
        )
        rec = profiler.begin(op, _shard_of(kwargs), scope, bytes_in, sys._getframe(1))
        if rec is None:
            return fn(self, *args, **kwargs)
        try:
            result = fn(self, *args, **kwargs)
        except BaseException:
            profiler.end(rec, 0)
            raise
        if hasattr(result, "__next__") and not isinstance(result, (list, tuple)):
            profiler.suspend()
            return _ProfiledIter(profiler, rec, result)
        profiler.end(rec, _byte_size(result))
        return result

    wrapper._vgi_storage_profiled = True  # type: ignore[attr-defined]
    wrapper._vgi_storage_profiler = profiler  # type: ignore[attr-defined]
    return wrapper


def profile_backend_class(cls: type) -> type:
    """Instrument a storage backend class when ``VGI_STORAGE_PROFILE=1``.

    A no-op otherwise: nothing is wrapped, so profiling costs nothing when off.
    Third-party backends may call it on their own class to be profiled too.

    Args:
        cls: The backend class.

    Returns:
        ``cls`` unchanged when profiling is off, instrumented when it is on.
    """
    if not _PROFILE_ON:
        return cls
    return instrument_class(cls, _profiler)


def note_round_trip(wire_bytes: int) -> None:
    """Count one transport round trip against the current logical call.

    Called by transports that may make several round trips per logical call
    (the cloudflare-do client's paginated scans). Only call it when
    ``_PROFILE_ON`` is true.

    Args:
        wire_bytes: Largest single-direction body of the round trip.
    """
    _profiler.note_round_trip(wire_bytes)


def report_from_jsonl(paths: list[str]) -> str:
    """Rebuild the summary table from ``VGI_STORAGE_PROFILE_FILE`` dumps.

    The at-exit table only appears when the worker exits cleanly (a SIGTERM'd
    server never runs ``atexit``); the JSONL is written per call, so this is
    the dependable way to get the report for a server, or to merge several
    processes' files. Concurrency is replayed from each call's start and
    duration.

    Args:
        paths: JSONL files written with ``VGI_STORAGE_PROFILE_FILE``.

    Returns:
        The same text `format_report` produces for a live profiler.
    """
    prof = _StorageProfiler()
    recs: list[dict[str, Any]] = []
    for path in paths:
        with open(path, encoding="utf-8") as fh:
            recs.extend(json.loads(line) for line in fh if line.strip())
    events: list[tuple[float, int, int]] = []
    for i, r in enumerate(recs):
        t0 = float(r["ts"])
        events.append((t0, 1, i))
        events.append((t0 + float(r["dur_ms"]) / 1000, 0, i))
    events.sort()
    for t, is_start, i in events:
        r = recs[i]
        shard_conc = prof._shards.setdefault(r["shard"], _Concurrency())
        for g in (prof._process, shard_conc, prof._scope(r["shard"], r["scope"]).conc):
            if is_start:
                g.start(t)
            else:
                g.end(t)
    for r in recs:
        seconds = float(r["dur_ms"]) / 1000
        rts, b_in, b_out = int(r["round_trips"]), int(r["bytes_in"]), int(r["bytes_out"])
        sc = prof._scope(r["shard"], r["scope"])
        sc.stat.add(seconds, rts, b_in, b_out)
        sc.by_op.setdefault(r["op"], _Stat()).add(seconds, rts, b_in, b_out)
        sc.by_site[(r["op"], r["site"])] = sc.by_site.get((r["op"], r["site"]), 0) + 1
        prof._by_site.setdefault((r["op"], r["site"]), _Stat()).add(seconds, rts, b_in, b_out)
        if r["rpc_id"]:
            key = hash((r["pid"], r["rpc_id"]))
            prof._rpcs.setdefault(key, [r["rpc"], 0])[1] += 1
    return prof.format_report()
