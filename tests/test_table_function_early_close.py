# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Closing a ``table_function`` generator before its stream ends must not hang.

``Client.table_function`` reads each worker's producer stream on a background
thread. Before the fix, closing the generator early left those threads reading,
and the ``Client.stop()`` that followed drained the same stream from the main
thread: two readers on one pipe, and whichever missed the end of stream blocked
forever (roughly one run in five). Each case here repeats the early close, each
repetition under its own watchdog, then checks the pooled worker still serves a
complete scan.

The watchdog bounds one scan, not the loop: a hang never finishes, so any bound
far above a healthy scan catches it, while a bound on the whole loop measures
throughput. A 24-way scan respawns the workers the pool could not keep idle
(``max_idle=8``), ~1.2 s idle and up to ~5.4 s with the host's CPUs
oversubscribed twice over; 25 of those overran a 120 s whole-loop budget.
"""

from __future__ import annotations

import threading
from collections.abc import Callable

import pyarrow as pa
import pytest

from tests.conftest import SUBPROCESS_FIXTURE_WORKER
from vgi.arguments import Arguments
from vgi.client.client import Client, _default_pool

_ITERATIONS = 25
# Per scan. A healthy one takes ~1.5 s idle, ~6 s at worst under 2x CPU oversubscription.
_WATCHDOG_SECONDS = 60.0


def _run_with_watchdog(body: Callable[[], None]) -> None:
    """Run ``body`` on a daemon thread; fail (rather than hang the suite) if it doesn't finish."""
    errors: list[BaseException] = []

    def target() -> None:
        try:
            body()
        except BaseException as e:  # noqa: BLE001 - re-raised on the test thread below
            errors.append(e)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(_WATCHDOG_SECONDS)
    assert not thread.is_alive(), "closing a table_function scan early hung"
    if errors:
        raise errors[0]


def _close_after_first_batch(function_name: str, arguments: Arguments) -> None:
    with Client(SUBPROCESS_FIXTURE_WORKER, pool=_default_pool) as client:
        scan = client.table_function(function_name=function_name, schema_path=["main"], arguments=arguments)
        first = next(scan)
        assert first.num_rows > 0
        scan.close()


def _full_scan_still_works() -> None:
    with Client(SUBPROCESS_FIXTURE_WORKER, pool=_default_pool) as client:
        batches = list(
            client.table_function(
                function_name="sequence",
                schema_path=["main"],
                arguments=Arguments(positional=(pa.scalar(4),)),
            )
        )
    assert pa.Table.from_batches(batches).column("n").to_pylist() == [0, 1, 2, 3]


@pytest.mark.parametrize(
    ("function_name", "arguments"),
    [
        # One worker, many small batches.
        ("sequence", Arguments(positional=(pa.scalar(100_000),), named={"batch_size": pa.scalar(10)})),
        # Several workers (one per partition), each with batches still to come.
        ("partitioned_sequence", Arguments(positional=(pa.scalar(30_000),))),
    ],
    ids=["single_worker", "multi_worker"],
)
def test_early_close_does_not_hang(function_name: str, arguments: Arguments) -> None:
    """A scan closed after its first batch stops cleanly, and the worker pool stays usable."""
    for _ in range(_ITERATIONS):
        _run_with_watchdog(lambda: _close_after_first_batch(function_name, arguments))
    _run_with_watchdog(_full_scan_still_works)


def test_secondary_workers_stop_concurrently(monkeypatch: pytest.MonkeyPatch) -> None:
    """Secondary workers stop in parallel, and every one is stopped even when one fails.

    Each stop can wait ~0.2 s on a process exit (its own, or a pool eviction's),
    so stopping a 24-way scan's workers in turn spent ~4 s closing it.
    """
    client = Client(SUBPROCESS_FIXTURE_WORKER, pool=_default_pool)
    workers = [object() for _ in range(8)]
    stopped: list[object] = []
    barrier = threading.Barrier(len(workers), timeout=10)

    def fake_stop(worker: object, *, force: bool = False) -> int:
        barrier.wait()  # breaks (and fails the test) unless all stops run at once
        stopped.append(worker)
        if worker is workers[3]:
            raise RuntimeError("stop failed")
        return 0

    monkeypatch.setattr(client, "_stop_worker", fake_stop)
    client._additional_workers = workers  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="stop failed"):
        client._close_secondary_workers()
    assert sorted(map(id, stopped)) == sorted(map(id, workers))
    assert client._additional_workers == []
