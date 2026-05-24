# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Backend-agnostic per-shard storage round-trip profiler (opt-in).

Enable with ``VGI_STORAGE_PROFILE=1``. Recording happens at the
``BoundStorage`` facade so it works against *any* backend — in-process sqlite
included — which lets the suite be profiled locally without a Cloudflare
deployment. Backends that already self-profile at their transport layer (the
cloudflare-do ``_post``, which sees per-page round-trips) set
``_profiles_at_transport = True`` so ``BoundStorage`` defers to them and the
two layers never double-count.

Stats are keyed by ``(shard_key, op)``. Each test run does one ATTACH with a
fresh random-nonce sealed envelope, so one ``shard_key`` == one test run — the
per-shard summary is effectively a per-test storage profile. A daemon flusher
logs a JSON summary periodically (and atexit) under ``vgi.storage.profile``.
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import threading
import time

_PROFILE_ON = os.environ.get("VGI_STORAGE_PROFILE") == "1"
_profile_logger = logging.getLogger("vgi.storage.profile")


class _StorageProfiler:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        # (shard, op) -> [count, total_s, max_s, total_bytes, max_bytes]
        # ``max_bytes`` is the largest single-call HTTP body (max of request
        # and response direction) — the number that matters for provider
        # request/response size limits.
        self._stats: dict[tuple[str, str], list[float]] = {}
        self._started = False

    def record(self, shard: str, op: str, seconds: float, io_bytes: int) -> None:
        with self._lock:
            e = self._stats.setdefault((shard, op), [0.0, 0.0, 0.0, 0.0, 0.0])
            e[0] += 1
            e[1] += seconds
            e[2] = max(e[2], seconds)
            e[3] += io_bytes
            e[4] = max(e[4], io_bytes)

    def dump(self) -> None:
        with self._lock:
            by_shard: dict[str, dict[str, list[float]]] = {}
            for (shard, op), e in self._stats.items():
                by_shard.setdefault(shard, {})[op] = list(e)
        for shard, ops in by_shard.items():
            round_trips = int(sum(e[0] for e in ops.values()))
            storage_s = sum(e[1] for e in ops.values())
            op_summary = {
                op: {
                    "n": int(e[0]),
                    "total_ms": round(e[1] * 1000, 1),
                    "avg_ms": round(e[1] / e[0] * 1000, 1) if e[0] else 0,
                    "max_ms": round(e[2] * 1000, 1),
                    "io_kb": round(e[3] / 1024, 1),
                    # largest single HTTP body (req or resp) — headroom vs
                    # provider size caps.
                    "max_call_kb": round(e[4] / 1024, 1),
                }
                for op, e in sorted(ops.items(), key=lambda kv: -kv[1][1])
            }
            _profile_logger.warning(
                json.dumps({
                    "msg": "storage_profile",
                    "shard": shard,
                    "round_trips": round_trips,
                    "storage_s": round(storage_s, 2),
                    "ops": op_summary,
                })
            )

    def start(self, interval: float = 5.0) -> None:
        if self._started:
            return
        self._started = True

        def _loop() -> None:
            while True:
                time.sleep(interval)
                self.dump()

        threading.Thread(target=_loop, daemon=True, name="vgi-storage-profile").start()
        atexit.register(self.dump)


_profiler = _StorageProfiler()
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


def io_call_bytes(args: tuple, kwargs: dict, result: object) -> int:
    """Largest single-direction HTTP body for one BoundStorage call.

    Writes carry the payload in the args (items / value / appended item);
    reads carry it in the result. Provider request/response size caps apply
    per direction, so the relevant figure is the max of the two.
    """
    request = sum(_byte_size(a) for a in args) + sum(_byte_size(v) for v in kwargs.values())
    return max(request, _byte_size(result))
