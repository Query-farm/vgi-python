# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Resumable table-function scans over HTTP (``Client.table_scan_resumable``).

Proves that a client can drive an upstream scan one batch at a time, capture the
worker's per-batch continuation token, and resume the scan on a *fresh* client
(simulating a load-balanced relay that node-hops mid-scan) with no duplicate or
dropped rows. Also checks the capability gate on the non-resumable transport.
"""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest

from vgi.arguments import Arguments
from vgi.client import Client, ResumeUnsupported

pytest.importorskip("vgi_rpc.http")

# A multi-batch scan: 100 rows in batches of 10 -> 10 data batches, each with its
# own resume token.
_COUNT = 100
_BATCH = 10
_ARGS = Arguments(positional=(pa.scalar(_COUNT),), named={"batch_size": pa.scalar(_BATCH)})


@pytest.fixture(scope="module")
def http_base_url() -> Iterator[str]:
    """Spawn ``vgi-fixture-http`` once for this module and yield its base URL."""
    from contextlib import ExitStack

    from tests._http_fixtures import free_port, run_example_http_server, wait_for_http_server

    stack = ExitStack()
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    stack.enter_context(run_example_http_server(port=port))
    wait_for_http_server(base_url)
    try:
        yield base_url
    finally:
        stack.close()


def _reference_rows(base_url: str) -> list[int]:
    """Read the full, ordered ``sequence`` result the normal way."""
    with Client.from_http(base_url) as client:
        return [
            v
            for b in client.table_function(function_name="sequence", arguments=_ARGS)
            for v in b.column("n").to_pylist()
        ]


def test_resumable_scan_matches_table_function(http_base_url: str) -> None:
    """Driving the cursor to completion yields exactly the same rows as table_function."""
    expected = _reference_rows(http_base_url)
    assert len(expected) == _COUNT

    rows: list[int] = []
    with Client.from_http(http_base_url) as client:
        cur = client.table_scan_resumable(function_name="sequence", arguments=_ARGS)
        while True:
            batch, _token = cur.next()
            if batch is None:
                break
            rows.extend(batch.column("n").to_pylist())
        cur.close()

    assert rows == expected


def test_resume_on_fresh_client_after_node_hop(http_base_url: str) -> None:
    """Persist the token mid-scan, drop the client, resume on a NEW one — no gaps/dupes."""
    expected = _reference_rows(http_base_url)

    rows: list[int] = []
    token: bytes | None = None
    # Read the first few batches, then abandon the connection (as if the LB node died).
    with Client.from_http(http_base_url) as client:
        cur = client.table_scan_resumable(function_name="sequence", arguments=_ARGS)
        for _ in range(3):
            batch, token = cur.next()
            assert batch is not None
            rows.extend(batch.column("n").to_pylist())
        assert token is not None  # per-batch tokens are the default
        cur.close()

    # A brand-new client resumes from the serialized token alone.
    with Client.from_http(http_base_url) as client2:
        cur2 = client2.table_scan_resumable(
            function_name="sequence", arguments=_ARGS, resume_token=token
        )
        while True:
            batch, token = cur2.next()
            if batch is None:
                break
            rows.extend(batch.column("n").to_pylist())
        cur2.close()

    assert rows == expected  # contiguous, complete, no duplicates


def test_continue_on_fresh_client_skips_rebind(http_base_url: str) -> None:
    """``table_scan_continue`` resumes from the token alone — no bind/init round-trip.

    Same node-hop scenario as above, but the fresh client resumes via
    ``table_scan_continue`` (a single ``/init/exchange`` continuation) instead of
    ``table_scan_resumable`` (which re-binds + inits and discards a first turn). The
    rows must still be contiguous and complete, proving the server recovers the
    producer purely from the signed token.
    """
    expected = _reference_rows(http_base_url)

    rows: list[int] = []
    token: bytes | None = None
    with Client.from_http(http_base_url) as client:
        cur = client.table_scan_resumable(function_name="sequence", arguments=_ARGS)
        for _ in range(3):
            batch, token = cur.next()
            assert batch is not None
            rows.extend(batch.column("n").to_pylist())
        assert token is not None
        cur.close()

    # A brand-new client resumes WITHOUT re-binding — continuation-only.
    with Client.from_http(http_base_url) as client2:
        cur2 = client2.table_scan_continue(resume_token=token)
        while True:
            batch, token = cur2.next()
            if batch is None:
                break
            rows.extend(batch.column("n").to_pylist())
        cur2.close()

    assert rows == expected


def test_continue_rejected_on_subprocess() -> None:
    """The capability gate rejects ``table_scan_continue`` on the pipe transport."""
    client = Client("/nonexistent/worker")
    assert client.supports_resumable_scan is False
    with pytest.raises(ResumeUnsupported):
        client.table_scan_continue(resume_token=b"irrelevant")


def test_resumable_scan_rejected_on_subprocess() -> None:
    """The pipe transport has no resume token; the capability gate rejects it.

    The gate fires before the client is even started, so no worker is spawned.
    """
    client = Client("/nonexistent/worker")  # subprocess transport, never started
    assert client.supports_resumable_scan is False
    with pytest.raises(ResumeUnsupported):
        client.table_scan_resumable(function_name="sequence", arguments=_ARGS)
