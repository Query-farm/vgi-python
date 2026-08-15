# Copyright 2025, 2026 Query Farm LLC - https://query.farm

# ruff: noqa: S101, D102, D103
"""Tests for ``Client.stop(force=True)``.

A graceful ``stop()`` shuts the RPC connection down and waits for the worker, so it cannot
be used to abandon a call that has overrun its budget — the wait is exactly what the caller
is trying to escape. ``force=True`` SIGKILLs a direct subprocess worker first, which unblocks
any thread waiting on its output immediately.

Only *direct* subprocess workers own their process; pooled workers are returned to the pool
and HTTP/TCP connections are already prompt to close, so those paths must accept ``force``
without changing behaviour.
"""

from __future__ import annotations

import subprocess
import sys
import time
from typing import Any

import pyarrow as pa
import pytest

from vgi.arguments import Arguments
from vgi.client.client import Client, _default_pool

# Long enough that a graceful teardown would have to wait on it if the worker were stuck.
_SLEEP_MS = 5_000


def _started_client(**kwargs: Any) -> Client:
    client = Client("vgi-fixture-worker", **kwargs)
    client.start()
    return client


def test_force_kills_a_direct_subprocess(tmp_path: Any) -> None:
    """The worker process is signalled, not asked to exit."""
    client = _started_client(pool=None)
    assert client._primary is not None  # noqa: SLF001 - a started client always has one
    proc = client._primary.proc  # noqa: SLF001 - asserting the process really died
    assert proc is not None
    assert proc.poll() is None, "worker should be running"

    returncode = client.stop(force=True)
    assert returncode is not None
    if sys.platform == "win32":
        # Windows has no signals: TerminateProcess sets an ordinary non-zero code.
        assert returncode != 0, "a killed process should not report a clean exit"
    else:
        assert returncode < 0, "a killed process reports a negative (signal) exit code"
    assert proc.poll() is not None, "worker process should be reaped"


def test_force_returns_promptly_mid_stream(tmp_path: Any) -> None:
    """Killing while a slow scan is in flight must not wait for the scan."""
    probe = tmp_path / "probe.txt"
    client = _started_client(pool=None)
    gen = client.table_function(
        function_name="slow_cancellable",
        schema_name="main",
        arguments=Arguments(positional=(pa.scalar(str(probe)),), named={"sleep_ms": pa.scalar(_SLEEP_MS)}),
    )
    next(gen)  # worker is now mid-stream, sleeping between batches

    started = time.monotonic()
    client.stop(force=True)
    elapsed = time.monotonic() - started
    assert elapsed < _SLEEP_MS / 1000.0, f"forced stop waited {elapsed:.2f}s on a sleeping worker"


def test_graceful_stop_is_unchanged() -> None:
    """Force defaults to False and still reports a clean exit."""
    client = _started_client(pool=None)
    assert client.stop() == 0


def test_force_is_accepted_by_pooled_workers() -> None:
    """Pooled workers have no proc to kill, so force must be inert rather than an error."""
    client = _started_client(pool=_default_pool)
    assert client._primary is not None  # noqa: SLF001 - a started client always has one
    assert client._primary.proc is None  # noqa: SLF001 - documents why force is inert here
    assert client.stop(force=True) == 0


def test_stop_still_requires_a_started_client() -> None:
    client = Client("vgi-fixture-worker", pool=None)
    with pytest.raises(Exception, match="not started"):
        client.stop(force=True)


def test_client_can_restart_after_a_forced_stop() -> None:
    """Force must leave the client in the same reusable state as a graceful stop."""
    client = _started_client(pool=None)
    client.stop(force=True)
    client.start()
    try:
        assert client._primary is not None  # noqa: SLF001 - restart sanity check
    finally:
        client.stop()


def test_graceful_stop_kills_a_worker_that_overruns_the_wait() -> None:
    """A worker that outlives the exit wait is killed, not raised out of ``stop()``.

    ``proc.wait(timeout=...)`` raises ``TimeoutExpired``, and letting that escape
    would leak the process *and* mask whatever exception was already unwinding
    through ``__exit__``. Forcing the timed wait to raise is deterministic;
    shrinking ``PROCESS_WAIT_TIMEOUT`` instead would race a worker that happens
    to exit promptly.
    """
    client = _started_client(pool=None)
    assert client._primary is not None  # noqa: SLF001 - a started client always has one
    proc = client._primary.proc  # noqa: SLF001 - asserting the process really died
    assert proc is not None

    real_wait = proc.wait
    timed_waits: list[float | None] = []

    def wait_that_times_out(timeout: float | None = None) -> int:
        if timeout is None:  # the untimed reap after the kill
            return real_wait()
        timed_waits.append(timeout)
        raise subprocess.TimeoutExpired(proc.args, timeout)

    proc.wait = wait_that_times_out  # type: ignore[method-assign]

    returncode = client.stop()

    assert timed_waits, "stop() should have waited on the worker with a timeout"
    assert proc.poll() is not None, "worker process should be reaped, not left running"
    if sys.platform != "win32":
        assert returncode < 0, "the timeout path kills, so the exit code is a signal"
