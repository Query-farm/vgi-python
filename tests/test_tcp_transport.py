# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Round-trip tests for the TCP transport.

Spawns ``vgi-fixture-worker --tcp 127.0.0.1:0`` (raw Arrow-IPC framing over a
TCP socket, served by ``vgi_rpc.rpc.run_server``), parses the ``TCP:host:port``
discovery line it prints on stdout, then drives it through
``Client.from_tcp(...)`` — the TCP analog of the HTTP round-trip in
``tests/_http_fixtures.py``.
"""

from __future__ import annotations

import queue
import subprocess
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager

import pyarrow as pa
import pytest

from vgi.arguments import Arguments
from vgi.client import Client


@contextmanager
def run_tcp_worker(*, bind: str = "127.0.0.1:0") -> Iterator[tuple[str, int]]:
    """Run ``vgi-fixture-worker --tcp`` and yield the bound ``(host, port)``.

    The worker prints ``TCP:<host>:<port>`` once bound and then must not write
    further to stdout (the cross-language launcher discovery contract), so we
    read exactly one line to learn the port. stderr is drained in the
    background to keep the worker from blocking on a full pipe buffer.
    """
    proc = subprocess.Popen(
        [sys.executable, "-m", "vgi._test_fixtures.worker", "--tcp", bind],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    def _drain(pipe: object) -> None:
        for _ in pipe:  # type: ignore[attr-defined]
            pass

    stderr_thread = threading.Thread(target=_drain, args=(proc.stderr,), daemon=True)
    stderr_thread.start()

    # Read the discovery line off stdout with a timeout so a worker that never
    # binds fails the test instead of hanging it.
    line_q: queue.Queue[str] = queue.Queue(maxsize=1)

    def _read_line() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            if line.startswith("TCP:"):
                line_q.put(line.strip())
                return

    reader = threading.Thread(target=_read_line, daemon=True)
    reader.start()
    try:
        discovery = line_q.get(timeout=30)
    except queue.Empty:
        proc.terminate()
        raise TimeoutError("worker did not emit a TCP: discovery line within 30s") from None

    _, host, port_str = discovery.split(":", 2)
    try:
        yield host, int(port_str)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        stderr_thread.join(timeout=5)


def test_tcp_round_trip_table_function() -> None:
    """A table function streams rows correctly over the TCP transport."""
    with run_tcp_worker() as (host, port), Client.from_tcp(host, port) as client:
        batches = list(
            client.table_function(
                function_name="sequence",
                schema_name="main",
                arguments=Arguments(positional=(pa.scalar(5),)),
            )
        )

    table = pa.Table.from_batches(batches)
    assert table.column("n").to_pylist() == [0, 1, 2, 3, 4]


def test_tcp_round_trip_catalog_listing() -> None:
    """Catalog discovery works over the TCP transport (catalog_mixin path)."""
    with run_tcp_worker() as (host, port), Client.from_tcp(host, port) as client:
        catalogs = client.catalogs()

    assert any(c.name == "example" for c in catalogs)


class TestTcpConstructorValidation:
    """``transport='tcp'`` argument validation."""

    def test_requires_host_and_port(self) -> None:
        """Tcp transport without host/port is rejected."""
        with pytest.raises(ValueError, match="requires tcp_host and tcp_port"):
            Client(transport="tcp", pool=None)

    def test_rejects_server_path(self) -> None:
        """server_path is subprocess-only."""
        with pytest.raises(ValueError, match="server_path is only meaningful"):
            Client("some-worker", transport="tcp", tcp_host="127.0.0.1", tcp_port=1, pool=None)

    def test_rejects_base_url(self) -> None:
        """base_url is http-only."""
        with pytest.raises(ValueError, match="base_url is only meaningful"):
            Client(transport="tcp", tcp_host="127.0.0.1", tcp_port=1, base_url="http://x", pool=None)
