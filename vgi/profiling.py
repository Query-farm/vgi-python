# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Opt-in CPU profiling for VGI worker processes.

A worker is normally a subprocess someone else launched — a container, a test
harness, a DuckDB client spawning it over stdio — so the usual ways of profiling
Python (``python -m cProfile``, an interactive session) are not available to
whoever needs the answer. Setting one environment variable is.

Shared by every worker entry point rather than living in one CLI, so that
``vgi-serve`` and the fixture servers profile identically.
"""

from __future__ import annotations

import os
import pathlib
import sys

__all__ = ["maybe_start_profile"]


def maybe_start_profile() -> None:
    """Start a ``cProfile`` profiler when ``VGI_WORKER_PROFILE`` names a path.

    Set ``VGI_WORKER_PROFILE`` to a directory (or a file path) and the worker
    writes ``pstats`` data on exit, readable with :mod:`pstats`, snakeviz, or
    ``python -m pstats``. A directory gets one file per process
    (``worker-<pid>.prof``), which is what you want when the thing under
    investigation spawns more than one worker.

    **All threads are covered by the single profiler.** That matters because
    under ``--http`` a worker runs on waitress, which serves every request on a
    pool thread — a main-thread-only profile would faithfully report time spent
    in ``select()`` and nothing about the work being measured. Since Python 3.12
    ``cProfile`` is built on :mod:`sys.monitoring`, whose tool registration is
    process-global rather than per-thread, so one profiler observes the pool
    threads too.

    The corollary is that there can be only **one** active profiler in the
    process: ``sys.monitoring`` allows a given tool id to be claimed once, so a
    profiler-per-thread raises ``ValueError: Another profiling tool is already
    active`` in every waitress thread — and because that happens as the thread
    starts, the server never accepts a connection at all.

    ``atexit`` alone would not be enough: a worker is normally stopped with
    ``SIGTERM``, whose default action terminates without unwinding, so the
    profile would be lost on every normal shutdown. A handler turns it into an
    ordinary exit — but only if nothing else has claimed ``SIGTERM``, since
    stealing another component's shutdown path would be a worse bug than a
    missing profile.
    """
    dest = os.environ.get("VGI_WORKER_PROFILE")
    if not dest:
        return

    import atexit
    import cProfile
    import signal

    path = pathlib.Path(dest)
    if path.is_dir() or dest.endswith(os.sep):
        path.mkdir(parents=True, exist_ok=True)
        path = path / f"worker-{os.getpid()}.prof"
    else:
        path.parent.mkdir(parents=True, exist_ok=True)

    profiler = cProfile.Profile()
    profiler.enable()
    dumped = False

    def _dump() -> None:
        # atexit and the SIGTERM handler can both reach this; dumping twice
        # would truncate the file the first call just wrote.
        nonlocal dumped
        if dumped:
            return
        dumped = True
        try:
            profiler.disable()
            profiler.dump_stats(str(path))
        except Exception as exc:  # noqa: BLE001 — never fail a shutdown over telemetry
            sys.stderr.write(f"vgi: could not write profile: {exc}\n")
            return
        sys.stderr.write(f"vgi: wrote profile to {path}\n")

    atexit.register(_dump)

    def _on_sigterm(signum: int, frame: object) -> None:
        raise SystemExit(128 + signum)

    if signal.getsignal(signal.SIGTERM) in (signal.SIG_DFL, None):
        signal.signal(signal.SIGTERM, _on_sigterm)
