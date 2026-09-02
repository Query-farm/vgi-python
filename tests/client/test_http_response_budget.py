# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""High-level Client wiring for negotiated HTTP response limits."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest
import vgi_rpc.http

from vgi.client.client import Client


@contextmanager
def _capturing_http_connect(calls: list[dict[str, Any]], _protocol: type[Any], **kwargs: Any) -> Iterator[object]:
    calls.append(kwargs)
    yield object()


@pytest.mark.parametrize(
    "value",
    [False, True, 0, -1, 1, 65535, 1.5, "1024", 1 << 53],
)
def test_invalid_accepted_max_response_bytes_fails_at_construction(value: Any) -> None:
    """Invalid limits fail before a worker is contacted."""
    with pytest.raises(ValueError, match="accepted_max_response_bytes"):
        Client.from_http("https://worker.example", accepted_max_response_bytes=value)


@pytest.mark.parametrize("value", [65536, 256 * 1024 * 1024, (1 << 53) - 1, None])
def test_primary_http_connection_forwards_accepted_limit(monkeypatch: pytest.MonkeyPatch, value: int | None) -> None:
    """Every long-lived high-level HTTP proxy receives the configured limit."""
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        vgi_rpc.http,
        "http_connect",
        lambda protocol, **kwargs: _capturing_http_connect(calls, protocol, **kwargs),
    )
    client = Client.from_http(
        "https://worker.example",
        httpx_client=object(),
        accepted_max_response_bytes=value,
    )

    connection = client._spawn_http_connection(7)
    assert calls == [
        {
            "base_url": "https://worker.example",
            "client": client._httpx_client,
            "on_log": client._on_worker_log,
            "external_location": client._external_location,
            "accepted_max_response_bytes": value,
        }
    ]
    assert connection.worker_index == 7
    assert connection._http_ctx is not None
    connection._http_ctx.__exit__(None, None, None)


def test_default_and_catalog_connections_share_256_mib_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Catalog RPCs cannot silently fall back to the low-level default."""
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        vgi_rpc.http,
        "http_connect",
        lambda protocol, **kwargs: _capturing_http_connect(calls, protocol, **kwargs),
    )
    client = Client.from_http("https://worker.example", httpx_client=object())

    with client._catalog_connect():
        pass

    assert calls[0]["accepted_max_response_bytes"] == 256 * 1024 * 1024
    assert calls[0]["client"] is client._httpx_client


def test_capability_helpers_remain_bodyless_and_surface_new_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    """OPTIONS discovery stays compatible while exposing the new contract."""
    calls: list[dict[str, Any]] = []
    capabilities = SimpleNamespace(
        max_request_bytes=1024,
        max_response_bytes=2048,
        accept_max_response_bytes_support=True,
    )

    def fake_http_capabilities(*, base_url: str | None, client: object) -> object:
        calls.append({"base_url": base_url, "client": client})
        return capabilities

    monkeypatch.setattr(vgi_rpc.http, "http_capabilities", fake_http_capabilities)
    client = Client.from_http(
        "https://worker.example",
        httpx_client=object(),
        accepted_max_response_bytes=65536,
    )

    assert client.server_capabilities() is capabilities
    assert client._get_http_capabilities() is capabilities
    assert client._get_http_capabilities() is capabilities
    assert capabilities.max_response_bytes == 2048
    assert capabilities.accept_max_response_bytes_support is True
    assert calls == [
        {"base_url": "https://worker.example", "client": client._httpx_client},
        {"base_url": "https://worker.example", "client": client._httpx_client},
    ]
