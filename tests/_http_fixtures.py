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
import time
from collections.abc import Iterator, Sequence
from contextlib import ExitStack, contextmanager


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
    try:
        yield
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)

        if proc.returncode not in (0, -15):
            stderr = proc.stderr.read() if proc.stderr is not None else ""
            raise RuntimeError(f"example HTTP worker exited with code {proc.returncode}: {stderr}")


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
