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
import socket
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

    def test_rejects_tcp_proxy_on_other_transports(self) -> None:
        """TCP_PROXY is consumed by TCP dialing, never treated as a worker option."""
        with pytest.raises(ValueError, match="tcp_proxy is only meaningful"):
            Client("worker", tcp_proxy="socks5h://127.0.0.1:1055", pool=None)


def test_from_tcp_passes_explicit_proxy_without_local_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    """The high-level client delegates the untouched target name to SOCKS5h dialing."""
    seen: dict[str, object] = {}

    class FakeContext:
        def __enter__(self) -> object:
            return object()

        def __exit__(self, *_args: object) -> None:
            return None

    def fake_tcp_connect(_protocol: object, host: str, port: int, **kwargs: object) -> FakeContext:
        seen.update(host=host, port=port, **kwargs)
        return FakeContext()

    def forbidden_resolution(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("the high-level client must not resolve the SOCKS target locally")

    monkeypatch.setattr("vgi_rpc.rpc.tcp_connect", fake_tcp_connect)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden_resolution)
    client = Client.from_tcp(
        "must-not-resolve.invalid",
        9400,
        proxy="socks5h://127.0.0.1:1055",
    )
    connection = client._spawn_tcp_connection(0)
    try:
        assert seen["host"] == "must-not-resolve.invalid"
        assert seen["port"] == 9400
        assert seen["proxy"] == "socks5h://127.0.0.1:1055"
    finally:
        assert connection._tcp_ctx is not None
        connection._tcp_ctx.__exit__(None, None, None)


def test_tcp_catalog_passes_explicit_proxy_without_local_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    """Catalog calls use the same SOCKS5h path as ordinary exchange calls."""
    seen: dict[str, object] = {}
    sentinel = object()

    class FakeContext:
        def __enter__(self) -> object:
            return sentinel

        def __exit__(self, *_args: object) -> None:
            return None

    def fake_tcp_connect(_protocol: object, host: str, port: int, **kwargs: object) -> FakeContext:
        seen.update(host=host, port=port, **kwargs)
        return FakeContext()

    def forbidden_resolution(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("catalog discovery must not resolve the SOCKS target locally")

    monkeypatch.setattr("vgi_rpc.rpc.tcp_connect", fake_tcp_connect)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden_resolution)
    client = Client.from_tcp(
        "must-not-resolve.invalid",
        9400,
        proxy="socks5h://127.0.0.1:1055",
    )
    with client._catalog_connect() as proxy:
        assert proxy is sentinel

    assert seen["host"] == "must-not-resolve.invalid"
    assert seen["port"] == 9400
    assert seen["proxy"] == "socks5h://127.0.0.1:1055"
