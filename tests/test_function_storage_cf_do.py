"""Tests for vgi.function_storage_cf_do module."""

from __future__ import annotations

import base64
import json
import os
from typing import Any
from unittest.mock import patch

import httpx
import pytest

from vgi.function_storage_cf_do import FunctionStorageCfDo


class _MockTransport:
    """httpx-compatible transport that records requests and returns canned responses.

    Drop-in for ``httpx.MockTransport`` with a richer ergonomic surface:
    callers ``queue_response(status, body)`` and inspect ``requests``
    (list of ``httpx.Request``) afterwards. Each enqueued response is
    consumed in FIFO order.

    """

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self._responses: list[tuple[int, dict[str, object] | str | bytes | None]] = []
        # Optional per-request hook — set in tests that want to inject errors.
        self.on_request: Any = None

    def queue_response(self, status: int, body: dict[str, object] | str | bytes | None) -> None:
        self._responses.append((status, body))

    def _handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.on_request is not None:
            self.on_request(request)
        if not self._responses:
            raise AssertionError(f"Unexpected request {request.method} {request.url.path} — no response queued")
        status, body = self._responses.pop(0)
        if body is None:
            return httpx.Response(status, content=b"")
        if isinstance(body, (str, bytes)):
            return httpx.Response(status, content=body)
        return httpx.Response(status, json=body)

    def as_httpx_transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handler)


@pytest.fixture
def mock_transport() -> _MockTransport:
    """Create a fresh mock transport."""
    return _MockTransport()


def _wrap_with_mock(s: FunctionStorageCfDo, transport: _MockTransport) -> None:
    """Replace the storage's httpx client with one backed by the mock transport.

    Preserves base_url and headers so request inspection still sees the
    Authorization header etc.
    """
    old = s._client
    s._client = httpx.Client(
        base_url=str(old.base_url),
        headers=old.headers,
        timeout=old.timeout,
        transport=transport.as_httpx_transport(),
    )
    old.close()


@pytest.fixture
def storage(mock_transport: _MockTransport) -> FunctionStorageCfDo:
    """Create a storage instance with mocked HTTP transport."""
    s = FunctionStorageCfDo(url="https://vgi-storage.example.workers.dev", token="test-token")
    _wrap_with_mock(s, mock_transport)
    return s


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def _body(req: httpx.Request) -> dict[str, Any]:
    return json.loads(req.content)


class TestFunctionStorageCfDo:
    """Tests for FunctionStorageCfDo."""

    # --- Work Queue Tests ---

    def test_queue_push_and_pop(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Test pushing and popping work items."""
        execution_id = b"\x02" * 16

        mock_transport.queue_response(200, {"count": 3})
        count = storage.queue_push(execution_id, [b"item1", b"item2", b"item3"])
        assert count == 3

        body = _body(mock_transport.requests[0])
        assert len(body["items"]) == 3
        assert base64.b64decode(body["items"][0]) == b"item1"

    def test_queue_pop_returns_item(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Test that queue_pop returns a decoded item."""
        execution_id = b"\x02" * 16

        mock_transport.queue_response(200, {"item": _b64(b"item1")})
        result = storage.queue_pop(execution_id)
        assert result == b"item1"

    def test_queue_pop_empty_queue(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Test popping from registered but empty queue returns None."""
        mock_transport.queue_response(200, {"item": None})
        result = storage.queue_pop(b"\x02" * 16)
        assert result is None

    def test_queue_push_empty_list(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Test pushing empty list returns 0."""
        mock_transport.queue_response(200, {"count": 0})
        count = storage.queue_push(b"\x02" * 16, [])
        assert count == 0

    def test_queue_push_empty_still_registers(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport
    ) -> None:
        """Test that pushing empty list still sends request (DO registers)."""
        mock_transport.queue_response(200, {"count": 0})
        storage.queue_push(b"\x02" * 16, [])
        assert len(mock_transport.requests) == 1
        body = _body(mock_transport.requests[0])
        assert body["items"] == []

    def test_queue_clear(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Test clearing the work queue."""
        mock_transport.queue_response(200, {"cleared": 3})
        cleared = storage.queue_clear(b"\x02" * 16)
        assert cleared == 3

    def test_queue_clear_empty_queue(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Test clearing an empty queue returns 0."""
        mock_transport.queue_response(200, {"cleared": 0})
        cleared = storage.queue_clear(b"\x02" * 16)
        assert cleared == 0

    def test_close_releases_client(self) -> None:
        """close() closes the underlying httpx.Client."""
        s = FunctionStorageCfDo(url="https://vgi-storage.example.workers.dev")
        s.close()
        # Subsequent post on a closed client should raise.
        with pytest.raises(RuntimeError):
            s._client.post("/x", json={})

    # --- Factory Tests ---

    def test_from_env(self) -> None:
        """Test from_env reads env vars."""
        env = {
            "VGI_CF_DO_URL": "https://vgi-storage.example.workers.dev",
            "VGI_CF_DO_TOKEN": "my-token",
        }
        with patch.dict("os.environ", env, clear=False):
            s = FunctionStorageCfDo.from_env()
            assert s._url == "https://vgi-storage.example.workers.dev"
            assert s._token == "my-token"

    def test_from_env_no_token(self) -> None:
        """Test from_env works without token."""
        env = {"VGI_CF_DO_URL": "https://vgi-storage.example.workers.dev"}
        with patch.dict("os.environ", env, clear=False):
            os.environ.pop("VGI_CF_DO_TOKEN", None)
            s = FunctionStorageCfDo.from_env()
            assert s._token is None

    def test_from_env_missing_url(self) -> None:
        """Test from_env raises ValueError when URL missing."""
        env = {"VGI_WORKER_SHARED_STORAGE": "cloudflare-do"}
        with patch.dict("os.environ", env, clear=True), pytest.raises(ValueError, match="VGI_CF_DO_URL"):
            FunctionStorageCfDo.from_env()


class TestLazyStorageDescriptorCfDo:
    """Test that cloudflare-do backend is recognized by _resolve_storage."""

    def test_cloudflare_do_backend(self) -> None:
        """Test that cloudflare-do backend calls FunctionStorageCfDo.from_env."""
        from vgi.function import _resolve_storage

        env = {
            "VGI_WORKER_SHARED_STORAGE": "cloudflare-do",
            "VGI_CF_DO_URL": "https://example.workers.dev",
        }
        with patch.dict("os.environ", env):
            s = _resolve_storage()
            assert isinstance(s, FunctionStorageCfDo)


class TestCfDoStateUnified:
    """Tests for the unified state_* HTTP client."""

    def test_state_get_many_emits_keys(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport,
    ) -> None:
        """state_get_many POSTs scope_id/ns/keys, returns parallel value list."""
        mock_transport.queue_response(200, {"rows": [
            {"value": _b64(b"v1")},
            None,
            {"value": _b64(b"v3")},
        ]})
        result = storage.state_get_many(b"exec1", b"agg", [b"k1", b"k2", b"k3"])
        assert result == [b"v1", None, b"v3"]
        body = _body(mock_transport.requests[0])
        assert body["scope_id"] == _b64(b"exec1")
        assert body["ns"] == _b64(b"agg")
        assert body["keys"] == [_b64(b"k1"), _b64(b"k2"), _b64(b"k3")]
        assert "attempt_id" not in body  # read-only — no replay-detection needed

    def test_state_get_many_empty_keys_no_request(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport,
    ) -> None:
        """Empty key list short-circuits — no HTTP."""
        result = storage.state_get_many(b"exec1", b"agg", [])
        assert result == []
        assert mock_transport.requests == []

    def test_state_put_many_carries_attempt_id(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport,
    ) -> None:
        """state_put_many sends items + attempt_id (replay-detection)."""
        mock_transport.queue_response(200, {})
        storage.state_put_many(b"exec1", b"agg", [(b"k1", b"v1"), (b"k2", b"v2")])
        body = _body(mock_transport.requests[0])
        assert body["scope_id"] == _b64(b"exec1")
        assert body["ns"] == _b64(b"agg")
        assert body["items"] == [
            {"key": _b64(b"k1"), "value": _b64(b"v1")},
            {"key": _b64(b"k2"), "value": _b64(b"v2")},
        ]
        assert "attempt_id" in body
        assert len(body["attempt_id"]) == 32  # uuid.uuid4().hex

    def test_state_scan_no_attempt_id(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport,
    ) -> None:
        """state_scan is read-only — no attempt_id."""
        mock_transport.queue_response(200, {"rows": [
            {"key": _b64(b"k"), "value": _b64(b"v")},
        ]})
        result = storage.state_scan(b"exec1", b"agg")
        assert result == [(b"k", b"v")]
        body = _body(mock_transport.requests[0])
        assert "attempt_id" not in body

    def test_state_drain_carries_attempt_id_for_read_back_replay(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport,
    ) -> None:
        """state_drain sends attempt_id (read-back replay on retry)."""
        mock_transport.queue_response(200, {"rows": [
            {"key": _b64(b"k1"), "value": _b64(b"v1")},
            {"key": _b64(b"k2"), "value": _b64(b"v2")},
        ]})
        result = storage.state_drain(b"exec1", b"agg")
        assert sorted(result) == [(b"k1", b"v1"), (b"k2", b"v2")]
        body = _body(mock_transport.requests[0])
        assert "attempt_id" in body

    def test_state_delete_with_keys(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport,
    ) -> None:
        """state_delete with key list sends keys + returns count."""
        mock_transport.queue_response(200, {"deleted": 2})
        n = storage.state_delete(b"exec1", b"agg", [b"k1", b"k2"])
        assert n == 2
        body = _body(mock_transport.requests[0])
        assert body["keys"] == [_b64(b"k1"), _b64(b"k2")]

    def test_state_delete_namespace_no_keys_field(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport,
    ) -> None:
        """state_delete with keys=None omits the keys field — server interprets as wipe-namespace."""
        mock_transport.queue_response(200, {"deleted": 5})
        n = storage.state_delete(b"exec1", b"agg", None)
        assert n == 5
        body = _body(mock_transport.requests[0])
        assert "keys" not in body

    def test_execution_clear_returns_count(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport,
    ) -> None:
        """execution_clear sends scope_id, returns total deleted across both tables."""
        mock_transport.queue_response(200, {"deleted": 8})
        n = storage.execution_clear(b"exec1")
        assert n == 8
        body = _body(mock_transport.requests[0])
        assert body["scope_id"] == _b64(b"exec1")
        assert "attempt_id" in body  # naturally idempotent but audited
