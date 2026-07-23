# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""End-to-end test: ``Client`` transparently resolves pointer batches.

When an HTTP worker externalizes large output batches (via the demo blob
storage or any ``ExternalStorage`` backend), the batch sent on the wire is
empty and carries ``vgi_rpc.location`` metadata pointing at a URL. The
client must GET the URL, parse the Arrow IPC bytes, and surface a resolved
batch to the caller.

This test drives the full flow through ``Client.from_http(...)`` — the
canonical non-DuckDB path — rather than through ``http_connect`` directly.
If it passes, a TS port that mirrors this client's structure will benefit
from the same transparent resolution.
"""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest

from vgi.arguments import Arguments

pytest.importorskip("vgi_rpc.http")
pytest.importorskip("aiohttp", reason="aiohttp required for external location resolution")


@pytest.fixture(scope="module")
def demo_storage_base_url() -> Iterator[str]:
    """Spawn ``vgi-fixture-http --demo-storage`` with a tiny externalize threshold."""
    from contextlib import ExitStack

    from tests._http_fixtures import free_port, run_example_http_server, wait_for_http_server

    stack = ExitStack()
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    stack.enter_context(
        run_example_http_server(
            port=port,
            extra_args=[
                "--demo-storage",
                "--externalize-threshold-bytes",
                "512",
                "--max-upload-bytes",
                "16384",
            ],
        )
    )
    wait_for_http_server(base_url)
    try:
        yield base_url
    finally:
        stack.close()


def test_pointer_batch_auto_resolved(demo_storage_base_url: str) -> None:
    """A scalar whose output exceeds the threshold round-trips transparently.

    ``random_bytes(seed, n)`` returns an ``n``-byte blob. With ``n`` above
    the externalize threshold the worker returns a pointer batch; the
    ``Client`` should resolve it and hand the caller a normal batch with
    the real bytes.
    """
    from vgi_rpc.external import ExternalLocationConfig

    from vgi.client.client import Client
    from vgi.http.demo_storage import localhost_only_validator

    payload_bytes = 4096
    seed = 42

    input_schema = pa.schema([("dummy", pa.int64())])
    input_batch = pa.RecordBatch.from_pydict({"dummy": [1]}, schema=input_schema)

    # Default validator is https-only. Demo storage uses http://127.0.0.1,
    # so swap in a localhost validator.
    config = ExternalLocationConfig(url_validator=localhost_only_validator)

    with Client.from_http(demo_storage_base_url, external_location=config) as client:
        out = list(
            client.scalar_function(
                function_name="random_bytes",
                schema_name="main",
                arguments=Arguments(positional=(pa.scalar(seed), pa.scalar(payload_bytes))),
                input=iter([input_batch]),
            )
        )

    assert len(out) == 1
    result = out[0].column("result").to_pylist()
    assert len(result) == 1
    assert isinstance(result[0], bytes)
    assert len(result[0]) == payload_bytes


def test_http_client_uses_default_external_config(demo_storage_base_url: str) -> None:
    """A Client with no ``external_location=`` kwarg should still resolve pointers.

    The default ``ExternalLocationConfig`` uses an https-only validator, so
    fetching ``http://127.0.0.1`` URLs fails. That's deliberate — this test
    documents the "safe by default, opt in for localhost" behavior so a
    porter understands why they need to supply a validator in dev.
    """
    from vgi.client.client import Client, ClientError

    payload_bytes = 4096
    input_schema = pa.schema([("dummy", pa.int64())])
    input_batch = pa.RecordBatch.from_pydict({"dummy": [1]}, schema=input_schema)

    with (
        Client.from_http(demo_storage_base_url) as client,
        pytest.raises((ClientError, ValueError, Exception)),  # noqa: B017
    ):
        list(
            client.scalar_function(
                function_name="random_bytes",
                schema_name="main",
                arguments=Arguments(positional=(pa.scalar(0), pa.scalar(payload_bytes))),
                input=iter([input_batch]),
            )
        )
