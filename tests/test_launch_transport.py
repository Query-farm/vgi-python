# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Round-trip tests for the launcher-managed (``transport="launch"``) transport.

Drives ``vgi._test_fixtures.worker`` (the same fixture module ``test_tcp_transport.py``
uses) through ``Client.from_launch(...)`` — no separate fixture worker needed, since
``MetaWorker.serve`` already passes argv through to ``vgi_rpc.rpc.run_server()``, which
participates in the AF_UNIX launcher path (``--unix PATH --idle-timeout SEC``) the same
way it participates in the TCP path (``--tcp HOST:PORT``).
"""

from __future__ import annotations

import shutil
import socket
import sys
import tempfile
import uuid
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
from vgi_rpc.launcher import status_rows

from vgi.arguments import Arguments
from vgi.client import Client

pytestmark = pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="launch transport requires AF_UNIX")

_WORKER_ARGV = (sys.executable, "-m", "vgi._test_fixtures.worker")


@pytest.fixture
def state_dir() -> Iterator[str]:
    """Per-test state dir under ``/tmp`` to stay under macOS's 104-byte AF_UNIX limit.

    Mirrors ``vgi-rpc``'s own ``test_launcher.py`` fixture — ``tmp_path`` resolves to a
    deeply nested path that, concatenated with ``<hash>.sock``, blows past the cap.
    """
    d = Path(tempfile.gettempdir()) / f"vgi-launch-test-{uuid.uuid4().hex[:8]}"
    d.mkdir(mode=0o700)
    try:
        yield str(d)
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_launch_round_trip_table_function(state_dir: str) -> None:
    """A table function streams rows correctly over the launch transport."""
    with Client.from_launch(_WORKER_ARGV, idle_timeout=10.0, state_dir=state_dir) as client:
        batches = list(
            client.table_function(
                function_name="sequence",
                schema_name="main",
                arguments=Arguments(positional=(pa.scalar(5),)),
            )
        )

    table = pa.Table.from_batches(batches)
    assert table.column("n").to_pylist() == [0, 1, 2, 3, 4]


def test_launch_round_trip_catalog_listing(state_dir: str) -> None:
    """Catalog discovery works over the launch transport (catalog_mixin path)."""
    with Client.from_launch(_WORKER_ARGV, idle_timeout=10.0, state_dir=state_dir) as client:
        catalogs = client.catalogs()

    assert any(c.name == "example" for c in catalogs)


def test_launch_shares_one_worker_across_clients(state_dir: str) -> None:
    """Two `Client`s with the same worker_argv + state_dir share one warm worker.

    The whole point of the launcher over plain subprocess/TCP: a second client
    pointing at the same command must reuse the first client's worker rather than
    spawning its own. Proven by checking exactly one tracked worker exists in the
    shared state dir after both clients have connected, not by any client-visible
    process identity (the client has no API for that, by design — it shouldn't need
    to know or care).
    """
    with Client.from_launch(_WORKER_ARGV, idle_timeout=10.0, state_dir=state_dir) as client_a:
        assert client_a.catalogs()
        with Client.from_launch(_WORKER_ARGV, idle_timeout=10.0, state_dir=state_dir) as client_b:
            assert client_b.catalogs()
            rows = status_rows(Path(state_dir))
            assert len(rows) == 1, f"expected exactly one shared worker, found {len(rows)}: {rows}"
            assert rows[0].alive


class TestLaunchConstructorValidation:
    """``transport='launch'`` argument validation."""

    def test_requires_launch_argv(self) -> None:
        """Launch transport without launch_argv is rejected."""
        with pytest.raises(ValueError, match="requires a non-empty launch_argv"):
            Client(transport="launch", pool=None)

    def test_rejects_empty_launch_argv(self) -> None:
        """An explicitly empty launch_argv is rejected the same as omitting it."""
        with pytest.raises(ValueError, match="requires a non-empty launch_argv"):
            Client(transport="launch", launch_argv=(), pool=None)

    def test_rejects_server_path(self) -> None:
        """server_path is subprocess-only."""
        with pytest.raises(ValueError, match="server_path is only meaningful"):
            Client("some-worker", transport="launch", launch_argv=("x",), pool=None)

    def test_rejects_base_url(self) -> None:
        """base_url is http-only."""
        with pytest.raises(ValueError, match="base_url is only meaningful"):
            Client(transport="launch", launch_argv=("x",), base_url="http://x", pool=None)

    def test_rejects_tcp_host_and_port(self) -> None:
        """tcp_host/tcp_port are tcp-only."""
        with pytest.raises(ValueError, match="tcp_host/tcp_port are only meaningful"):
            Client(transport="launch", launch_argv=("x",), tcp_host="127.0.0.1", tcp_port=1, pool=None)
