"""High-level VGI client coverage for native Iroh endpoint routing."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest

from vgi.client import Client

ENDPOINT_ID = "01" * 32


def test_from_iroh_selects_raw_and_http_semantics() -> None:
    """The URI scheme selects stateful or HTTP continuation behavior."""
    raw = Client.from_iroh(f"iroh://{ENDPOINT_ID}")
    http = Client.from_iroh(f"httpi://{ENDPOINT_ID}/vgi")

    assert raw._transport == "iroh"
    assert not raw.supports_resumable_scan
    assert http._transport == "httpi"
    assert http.supports_resumable_scan
    assert http._base_url == f"http://{ENDPOINT_ID}"
    assert http._http_prefix == "/vgi"


@pytest.mark.parametrize(
    "endpoint",
    [
        f"https://{ENDPOINT_ID}",
        f"iroh://{ENDPOINT_ID}/vgi",
        f"httpi://{ENDPOINT_ID}/../vgi",
    ],
)
def test_from_iroh_rejects_noncanonical_endpoints(endpoint: str) -> None:
    """Non-Iroh and non-canonical endpoint strings fail before any I/O."""
    with pytest.raises(ValueError):
        Client.from_iroh(endpoint)


def test_raw_iroh_connection_forwards_native_options(monkeypatch: pytest.MonkeyPatch) -> None:
    """High-level options reach the official native RPC connector unchanged."""
    captured: dict[str, Any] = {}
    proxy = object()

    @contextmanager
    def fake_connect(protocol: Any, endpoint: str, **options: Any):  # type: ignore[no-untyped-def]
        captured.update(protocol=protocol, endpoint=endpoint, **options)
        yield proxy

    monkeypatch.setattr("vgi_rpc.iroh.iroh_connect", fake_connect)
    client = Client.from_iroh(
        f"iroh://{ENDPOINT_ID}",
        relay_urls=["https://relay.example"],
        remote_relay_url="https://relay.example",
        direct_addresses=["127.0.0.1:4433"],
    )

    connection = client._spawn_worker(3)
    assert connection.proxy is proxy
    assert captured["endpoint"] == f"iroh://{ENDPOINT_ID}"
    assert captured["relay_urls"] == ("https://relay.example",)
    assert captured["remote_relay_url"] == "https://relay.example"
    assert captured["direct_addresses"] == ("127.0.0.1:4433",)
    client._stop_worker(connection)
