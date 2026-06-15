# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Shared helpers for spawning ``vgi-fixture-http`` as a subprocess.

Lifted from ``tests/test_http_demo_storage.py`` so that conformance tests and
other HTTP-dependent tests can reuse a single lifecycle.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator, Sequence
from contextlib import ExitStack, contextmanager
from io import StringIO


def free_port() -> int:
    """Allocate an available localhost TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@contextmanager
def run_example_http_server(
    *,
    port: int,
    extra_args: Sequence[str] = (),
    env: dict[str, str] | None = None,
) -> Iterator[None]:
    """Run ``vgi-fixture-http`` in a subprocess on ``port``.

    ``extra_args`` are appended verbatim — e.g. ``("--demo-storage",
    "--externalize-threshold-bytes", "4096")``.
    """
    cmd = [
        sys.executable,
        "-m",
        "vgi._test_fixtures.http_server",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        *extra_args,
    ]
    proc_env = os.environ.copy()
    if env is not None:
        proc_env.update(env)
    proc = subprocess.Popen(
        cmd,
        env=proc_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Continuously drain stdout/stderr in background threads. The server logs
    # one or more lines per RPC; left undrained, a chatty run fills the OS pipe
    # buffer and the server blocks on write() to stderr — freezing waitress's
    # single I/O loop so every subsequent request hangs (ReadTimeout). Windows
    # pipe buffers are small enough to hit this within one test file; POSIX's
    # 64 KiB buffers usually hide it. Buffer stderr so the exit-code check below
    # can still surface it.
    captured_stderr = StringIO()

    def _drain(pipe: object, sink: StringIO | None) -> None:
        assert pipe is not None
        for line in pipe:  # type: ignore[attr-defined]
            if sink is not None:
                sink.write(line)

    drain_threads = [
        threading.Thread(target=_drain, args=(proc.stdout, None), daemon=True),
        threading.Thread(target=_drain, args=(proc.stderr, captured_stderr), daemon=True),
    ]
    for t in drain_threads:
        t.start()

    try:
        yield
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        for t in drain_threads:
            t.join(timeout=5)

        # POSIX terminate() -> SIGTERM (-15); Windows terminate() is
        # TerminateProcess(handle, 1), so a cleanly-stopped worker reports 1.
        ok_codes = (0, 1) if sys.platform == "win32" else (0, -15)
        if proc.returncode not in ok_codes:
            raise RuntimeError(f"example HTTP worker exited with code {proc.returncode}: {captured_stderr.getvalue()}")


def start_http_worker(
    stack: ExitStack,
    *,
    extra_args: Sequence[str] = (),
    env: dict[str, str] | None = None,
) -> str:
    """Allocate a port, start ``vgi-fixture-http`` under ``stack``, return base URL."""
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    stack.enter_context(run_example_http_server(port=port, extra_args=tuple(extra_args), env=env))
    wait_for_http_server(base_url)
    return base_url


def wait_for_http_server(base_url: str, timeout: float = 30.0) -> None:
    """Block until the HTTP server responds to a capabilities probe."""
    from vgi_rpc.http import http_capabilities

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            http_capabilities(base_url=base_url)
            return
        except Exception:
            time.sleep(0.25)
    raise TimeoutError(f"Timed out waiting for HTTP server at {base_url}")
