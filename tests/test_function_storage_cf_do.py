"""Tests for vgi.function_storage_cf_do module."""

from __future__ import annotations

import base64
import json
from unittest.mock import patch

import pytest

from vgi.function_storage import UnknownInvocationError
from vgi.function_storage_cf_do import FunctionStorageCfDo


class _MockResponse:
    """Mock HTTP response."""

    def __init__(self, status: int, body: dict[str, object]) -> None:
        self.status = status
        self._body = json.dumps(body).encode()

    def read(self) -> bytes:
        return self._body


class _MockConnection:
    """Mock HTTP connection that records requests and returns canned responses."""

    def __init__(self) -> None:
        self.requests: list[tuple[str, str, bytes, dict[str, str]]] = []
        self._responses: list[_MockResponse] = []

    def queue_response(self, status: int, body: dict[str, object]) -> None:
        self._responses.append(_MockResponse(status, body))

    def request(self, method: str, path: str, body: bytes | None = None, headers: dict[str, str] | None = None) -> None:
        self.requests.append((method, path, body or b"", headers or {}))

    def getresponse(self) -> _MockResponse:
        return self._responses.pop(0)

    def close(self) -> None:
        pass


@pytest.fixture
def mock_conn() -> _MockConnection:
    """Create a fresh mock connection."""
    return _MockConnection()


@pytest.fixture
def storage(mock_conn: _MockConnection) -> FunctionStorageCfDo:
    """Create a storage instance with mocked HTTP connection."""
    s = FunctionStorageCfDo(url="https://vgi-storage.example.workers.dev", token="test-token")
    s._conn = mock_conn  # type: ignore[assignment]
    # Patch _new_connection to return our mock on reconnect
    s._new_connection = lambda: mock_conn  # type: ignore[assignment,return-value]
    return s


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


class TestFunctionStorageCfDo:
    """Tests for FunctionStorageCfDo."""

    # --- Worker State Tests ---

    def test_worker_put_and_collect(self, storage: FunctionStorageCfDo, mock_conn: _MockConnection) -> None:
        """Test storing and collecting worker states."""
        execution_id = b"\x01" * 16

        # Put 3 states
        for _i in range(3):
            mock_conn.queue_response(200, {})
        storage.worker_put(execution_id, worker_id=1, state=b"state1")
        storage.worker_put(execution_id, worker_id=2, state=b"state2")
        storage.worker_put(execution_id, worker_id=3, state=b"state3")

        # Verify 3 POST requests to worker_put
        put_requests = [(m, p) for m, p, _, _ in mock_conn.requests if "worker_put" in p]
        assert len(put_requests) == 3

        # Collect
        mock_conn.queue_response(
            200,
            {
                "states": [_b64(b"state1"), _b64(b"state2"), _b64(b"state3")],
            },
        )
        states = storage.worker_collect(execution_id)
        assert len(states) == 3
        assert set(states) == {b"state1", b"state2", b"state3"}

    def test_worker_put_replaces_existing(self, storage: FunctionStorageCfDo, mock_conn: _MockConnection) -> None:
        """Test that worker_put sends replace request for same worker."""
        execution_id = b"\x01" * 16

        mock_conn.queue_response(200, {})
        mock_conn.queue_response(200, {})
        storage.worker_put(execution_id, worker_id=1, state=b"old")
        storage.worker_put(execution_id, worker_id=1, state=b"new")

        # Verify both requests were sent
        assert len(mock_conn.requests) == 2
        # Second request has new state
        body = json.loads(mock_conn.requests[1][2])
        assert base64.b64decode(body["state"]) == b"new"

    def test_worker_collect_empty(self, storage: FunctionStorageCfDo, mock_conn: _MockConnection) -> None:
        """Test collecting when no states exist."""
        mock_conn.queue_response(200, {"states": []})
        states = storage.worker_collect(b"\x01" * 16)
        assert states == []

    # --- Work Queue Tests ---

    def test_queue_push_and_pop(self, storage: FunctionStorageCfDo, mock_conn: _MockConnection) -> None:
        """Test pushing and popping work items."""
        execution_id = b"\x02" * 16

        mock_conn.queue_response(200, {"count": 3})
        count = storage.queue_push(execution_id, [b"item1", b"item2", b"item3"])
        assert count == 3

        # Verify items were base64-encoded in request
        body = json.loads(mock_conn.requests[0][2])
        assert len(body["items"]) == 3
        assert base64.b64decode(body["items"][0]) == b"item1"

    def test_queue_pop_returns_item(self, storage: FunctionStorageCfDo, mock_conn: _MockConnection) -> None:
        """Test that queue_pop returns a decoded item."""
        execution_id = b"\x02" * 16

        mock_conn.queue_response(200, {"item": _b64(b"item1")})
        result = storage.queue_pop(execution_id)
        assert result == b"item1"

    def test_queue_pop_empty_queue(self, storage: FunctionStorageCfDo, mock_conn: _MockConnection) -> None:
        """Test popping from registered but empty queue returns None."""
        mock_conn.queue_response(200, {"item": None})
        result = storage.queue_pop(b"\x02" * 16)
        assert result is None

    def test_queue_pop_unknown_invocation_raises(
        self, storage: FunctionStorageCfDo, mock_conn: _MockConnection
    ) -> None:
        """Test popping from unknown invocation raises error."""
        mock_conn.queue_response(404, {"error": "unknown_invocation"})
        with pytest.raises(UnknownInvocationError):
            storage.queue_pop(b"\xff" * 16)

    def test_queue_push_empty_list(self, storage: FunctionStorageCfDo, mock_conn: _MockConnection) -> None:
        """Test pushing empty list returns 0."""
        mock_conn.queue_response(200, {"count": 0})
        count = storage.queue_push(b"\x02" * 16, [])
        assert count == 0

    def test_queue_push_empty_still_registers(self, storage: FunctionStorageCfDo, mock_conn: _MockConnection) -> None:
        """Test that pushing empty list still sends request (DO registers)."""
        mock_conn.queue_response(200, {"count": 0})
        storage.queue_push(b"\x02" * 16, [])
        assert len(mock_conn.requests) == 1
        body = json.loads(mock_conn.requests[0][2])
        assert body["items"] == []

    def test_queue_clear(self, storage: FunctionStorageCfDo, mock_conn: _MockConnection) -> None:
        """Test clearing the work queue."""
        mock_conn.queue_response(200, {"cleared": 3})
        cleared = storage.queue_clear(b"\x02" * 16)
        assert cleared == 3

    def test_queue_clear_unregisters_invocation(self, storage: FunctionStorageCfDo, mock_conn: _MockConnection) -> None:
        """Test that queue_clear causes subsequent pop to raise."""
        # Clear succeeds
        mock_conn.queue_response(200, {"cleared": 0})
        storage.queue_clear(b"\x02" * 16)

        # Pop after clear returns 404
        mock_conn.queue_response(404, {"error": "unknown_invocation"})
        with pytest.raises(UnknownInvocationError):
            storage.queue_pop(b"\x02" * 16)

    def test_queue_clear_empty_queue(self, storage: FunctionStorageCfDo, mock_conn: _MockConnection) -> None:
        """Test clearing an empty queue returns 0."""
        mock_conn.queue_response(200, {"cleared": 0})
        cleared = storage.queue_clear(b"\x02" * 16)
        assert cleared == 0

    # --- Auth Tests ---

    def test_auth_header_sent(self, storage: FunctionStorageCfDo, mock_conn: _MockConnection) -> None:
        """Test that bearer token is sent in Authorization header."""
        mock_conn.queue_response(200, {"states": []})
        storage.worker_collect(b"\x01" * 16)
        _, _, _, headers = mock_conn.requests[0]
        assert headers["Authorization"] == "Bearer test-token"

    def test_no_auth_header_when_no_token(self, mock_conn: _MockConnection) -> None:
        """Test that no Authorization header is sent when token is None."""
        s = FunctionStorageCfDo(url="https://example.com")
        s._conn = mock_conn  # type: ignore[assignment]
        mock_conn.queue_response(200, {"states": []})
        s.worker_collect(b"\x01" * 16)
        _, _, _, headers = mock_conn.requests[0]
        assert "Authorization" not in headers

    def test_auth_failure_raises(self, storage: FunctionStorageCfDo, mock_conn: _MockConnection) -> None:
        """Test that 401 raises PermissionError."""
        mock_conn.queue_response(401, {"error": "unauthorized"})
        with pytest.raises(PermissionError):
            storage.worker_collect(b"\x01" * 16)

    # --- Factory Tests ---

    def test_from_env(self) -> None:
        """Test from_env reads env vars."""
        env = {
            "VGI_CF_DO_URL": "https://vgi-storage.example.workers.dev",
            "VGI_CF_DO_TOKEN": "my-token",
        }
        with patch.dict("os.environ", env, clear=False):
            s = FunctionStorageCfDo.from_env()
            assert s._host == "vgi-storage.example.workers.dev"
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

    # --- Retry Tests ---

    def test_retry_on_connection_error(self, storage: FunctionStorageCfDo, mock_conn: _MockConnection) -> None:
        """Test that connection errors trigger a retry."""
        call_count = 0
        original_request = mock_conn.request

        def failing_then_succeeding(*args: object, **kwargs: object) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("connection reset")
            original_request(*args, **kwargs)  # type: ignore[arg-type]

        mock_conn.request = failing_then_succeeding  # type: ignore[method-assign]
        mock_conn.queue_response(200, {"states": []})

        states = storage.worker_collect(b"\x01" * 16)
        assert states == []
        assert call_count == 2  # first failed, second succeeded


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


# Need os for test_from_env_no_token
import os  # noqa: E402
