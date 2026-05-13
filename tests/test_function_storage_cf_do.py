"""Tests for vgi.function_storage_cf_do module."""

from __future__ import annotations

import base64
import json
import os
from typing import Any
from unittest.mock import patch

import httpx
import pytest

from vgi.function_storage import UnknownInvocationError
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

    # --- Worker State Tests ---

    def test_worker_put_and_collect(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Test storing and collecting worker states."""
        execution_id = b"\x01" * 16

        for _i in range(3):
            mock_transport.queue_response(200, {})
        storage.worker_put(execution_id, worker_id=1, state=b"state1")
        storage.worker_put(execution_id, worker_id=2, state=b"state2")
        storage.worker_put(execution_id, worker_id=3, state=b"state3")

        put_requests = [r for r in mock_transport.requests if "worker_put" in r.url.path]
        assert len(put_requests) == 3

        mock_transport.queue_response(
            200,
            {"states": [_b64(b"state1"), _b64(b"state2"), _b64(b"state3")]},
        )
        states = storage.worker_collect(execution_id)
        assert len(states) == 3
        assert set(states) == {b"state1", b"state2", b"state3"}

    def test_worker_put_replaces_existing(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Test that worker_put sends replace request for same worker."""
        execution_id = b"\x01" * 16

        mock_transport.queue_response(200, {})
        mock_transport.queue_response(200, {})
        storage.worker_put(execution_id, worker_id=1, state=b"old")
        storage.worker_put(execution_id, worker_id=1, state=b"new")

        assert len(mock_transport.requests) == 2
        body = _body(mock_transport.requests[1])
        assert base64.b64decode(body["state"]) == b"new"

    def test_worker_collect_empty(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Test collecting when no states exist."""
        mock_transport.queue_response(200, {"states": []})
        states = storage.worker_collect(b"\x01" * 16)
        assert states == []

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

    def test_queue_pop_unknown_invocation_raises(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport
    ) -> None:
        """Test popping from unknown invocation raises error."""
        mock_transport.queue_response(404, {"error": "unknown_invocation"})
        with pytest.raises(UnknownInvocationError):
            storage.queue_pop(b"\xff" * 16)

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

    def test_queue_clear_unregisters_invocation(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport
    ) -> None:
        """Test that queue_clear causes subsequent pop to raise."""
        mock_transport.queue_response(200, {"cleared": 0})
        storage.queue_clear(b"\x02" * 16)

        mock_transport.queue_response(404, {"error": "unknown_invocation"})
        with pytest.raises(UnknownInvocationError):
            storage.queue_pop(b"\x02" * 16)

    def test_queue_clear_empty_queue(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Test clearing an empty queue returns 0."""
        mock_transport.queue_response(200, {"cleared": 0})
        cleared = storage.queue_clear(b"\x02" * 16)
        assert cleared == 0

    # --- Transaction State Tests ---

    def test_transaction_state_put_and_get(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Round-trip: put two keys, get them back."""
        txn_id = b"\xaa" * 16

        mock_transport.queue_response(200, {})
        storage.transaction_state_put(txn_id, [(b"k1", b"v1"), (b"k2", b"v2")])

        body = _body(mock_transport.requests[0])
        assert base64.b64decode(body["transaction_id"]) == txn_id
        items = body["items"]
        assert len(items) == 2
        assert {(base64.b64decode(it["key"]), base64.b64decode(it["value"])) for it in items} == {
            (b"k1", b"v1"),
            (b"k2", b"v2"),
        }

        mock_transport.queue_response(
            200,
            {"values": [_b64(b"v1"), _b64(b"v2")]},
        )
        result = storage.transaction_state_get(txn_id, [b"k1", b"k2"])
        assert result == [b"v1", b"v2"]

    def test_transaction_state_get_misses_return_none(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport
    ) -> None:
        """Misses surface as None in the parallel result list."""
        mock_transport.queue_response(200, {"values": [_b64(b"hit"), None, _b64(b"hit2")]})
        result = storage.transaction_state_get(b"\xaa" * 16, [b"a", b"missing", b"c"])
        assert result == [b"hit", None, b"hit2"]

    def test_transaction_state_get_empty_keys_short_circuits(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport
    ) -> None:
        """No request should be issued for an empty key list."""
        result = storage.transaction_state_get(b"\xaa" * 16, [])
        assert result == []
        assert len(mock_transport.requests) == 0

    def test_transaction_state_put_empty_short_circuits(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport
    ) -> None:
        """No request should be issued for an empty item list."""
        storage.transaction_state_put(b"\xaa" * 16, [])
        assert len(mock_transport.requests) == 0

    def test_transaction_state_clear(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Clear sends transaction_id and ignores response body."""
        mock_transport.queue_response(200, {"cleared": 5})
        storage.transaction_state_clear(b"\xbb" * 16)
        assert len(mock_transport.requests) == 1
        body = _body(mock_transport.requests[0])
        assert base64.b64decode(body["transaction_id"]) == b"\xbb" * 16

    # --- Worker Scan Tests ---

    def test_worker_scan_returns_pairs(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Non-destructive scan returns (worker_id, state) tuples."""
        mock_transport.queue_response(
            200,
            {
                "rows": [
                    {"worker_id": 11, "state": _b64(b"alpha")},
                    {"worker_id": 22, "state": _b64(b"beta")},
                ],
            },
        )
        rows = storage.worker_scan(b"\x01" * 16)
        assert rows == [(11, b"alpha"), (22, b"beta")]

    def test_worker_scan_empty(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Empty scan returns an empty list, not None."""
        mock_transport.queue_response(200, {"rows": []})
        assert storage.worker_scan(b"\x01" * 16) == []

    # --- Auth Tests ---

    def test_auth_header_sent(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Test that bearer token is sent in Authorization header."""
        mock_transport.queue_response(200, {"states": []})
        storage.worker_collect(b"\x01" * 16)
        assert mock_transport.requests[0].headers.get("Authorization") == "Bearer test-token"

    def test_no_auth_header_when_no_token(self, mock_transport: _MockTransport) -> None:
        """Test that no Authorization header is sent when token is None."""
        s = FunctionStorageCfDo(url="https://example.com")
        _wrap_with_mock(s, mock_transport)
        mock_transport.queue_response(200, {"states": []})
        s.worker_collect(b"\x01" * 16)
        assert "Authorization" not in mock_transport.requests[0].headers

    def test_auth_failure_raises(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Test that 401 raises PermissionError."""
        mock_transport.queue_response(401, {"error": "unauthorized"})
        with pytest.raises(PermissionError):
            storage.worker_collect(b"\x01" * 16)

    # --- Concurrency / Retry Tests ---

    def test_concurrent_requests_thread_safe(self) -> None:
        """``httpx.Client`` is shared across threads — concurrent calls must work.

        Regression: a previous implementation used ``http.client.HTTPConnection``
        with a single shared connection, whose state machine corrupted under
        concurrent use and surfaced as ``ResponseNotReady: Idle``. ``httpx.Client``
        with a connection pool replaces that and must serve concurrent callers
        correctly.
        """
        import threading

        s = FunctionStorageCfDo(url="https://vgi-storage.example.workers.dev")
        transport = _MockTransport()
        for _ in range(20):
            transport.queue_response(200, {"states": []})
        _wrap_with_mock(s, transport)

        errors: list[Exception] = []
        barrier = threading.Barrier(10)

        def call() -> None:
            barrier.wait()
            try:
                s.worker_collect(b"\x01" * 16)
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=call) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert len(transport.requests) == 10

    def test_retry_on_5xx(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """5xx responses are retried up to _POST_ATTEMPTS times."""
        mock_transport.queue_response(503, {"error": "overloaded"})
        mock_transport.queue_response(200, {"states": [_b64(b"x")]})
        states = storage.worker_collect(b"\x01" * 16)
        assert states == [b"x"]
        assert len(mock_transport.requests) == 2

    def test_retry_exhausted_raises(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """If every retry returns 5xx, the last error surfaces."""
        for _ in range(storage._POST_ATTEMPTS):
            mock_transport.queue_response(503, {"error": "overloaded"})
        with pytest.raises(RuntimeError, match="503"):
            storage.worker_collect(b"\x01" * 16)
        assert len(mock_transport.requests) == storage._POST_ATTEMPTS

    def test_retry_on_transport_error(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Transport-level errors (e.g. server-closed keep-alive) trigger retry."""
        call_count = 0

        def maybe_fail(_request: httpx.Request) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.RemoteProtocolError("server disconnected")

        mock_transport.on_request = maybe_fail
        # The first call raises before reaching the queued response;
        # the second consumes the queued response.
        mock_transport.queue_response(200, {"states": []})

        states = storage.worker_collect(b"\x01" * 16)
        assert states == []
        assert call_count == 2

    def test_non_json_response_retries(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """A non-JSON response (e.g. HTML error page) is treated as transient."""
        mock_transport.queue_response(502, b"<html>bad gateway</html>")
        mock_transport.queue_response(200, {"states": []})
        states = storage.worker_collect(b"\x01" * 16)
        assert states == []

    def test_400_raises_value_error_no_retry(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport
    ) -> None:
        """400 surfaces as ValueError (client-contract bug) and does not retry."""
        mock_transport.queue_response(400, {"error": "bad_request", "message": "no good"})
        with pytest.raises(ValueError, match="no good"):
            storage.worker_collect(b"\x01" * 16)
        assert len(mock_transport.requests) == 1

    def test_403_raises_runtime_no_retry(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Non-retryable 4xx (other than 400/401/404 special-cased) raises immediately."""
        mock_transport.queue_response(403, {"error": "forbidden"})
        with pytest.raises(RuntimeError, match="403"):
            storage.worker_collect(b"\x01" * 16)
        assert len(mock_transport.requests) == 1

    def test_close_releases_client(self) -> None:
        """close() closes the underlying httpx.Client."""
        s = FunctionStorageCfDo(url="https://vgi-storage.example.workers.dev")
        s.close()
        # Subsequent post on a closed client should raise.
        with pytest.raises(RuntimeError):
            s._client.post("/x", json={})

    # --- Idempotency / attempt_id Tests ---

    _ATTEMPT_RE = __import__("re").compile(r"^[0-9a-f]{32}$")

    def _destructive_call_cases(
        self,
    ) -> list[tuple[str, str]]:
        """(endpoint_path, method_name) for every destructive public method."""
        return [
            ("worker_put", "worker_put"),
            ("worker_collect", "worker_collect"),
            ("scan_worker_put", "scan_worker_put"),
            ("queue_push", "queue_push"),
            ("queue_pop", "queue_pop"),
            ("queue_clear", "queue_clear"),
            ("transaction_state_put", "transaction_state_put"),
            ("transaction_state_clear", "transaction_state_clear"),
            ("aggregate_state_put", "aggregate_state_put"),
            ("aggregate_state_clear", "aggregate_state_clear"),
            ("aggregate_window_partition_put", "aggregate_window_partition_put"),
            ("aggregate_window_partition_delete", "aggregate_window_partition_delete"),
            ("aggregate_window_partition_clear", "aggregate_window_partition_clear"),
        ]

    def _invoke(
        self,
        storage: FunctionStorageCfDo,
        method_name: str,
    ) -> None:
        """Invoke a destructive method with minimal arguments."""
        eid = b"\x01" * 16
        sid = b"\x02" * 16
        txn = b"\xaa" * 16
        if method_name == "worker_put":
            storage.worker_put(eid, worker_id=1, state=b"x")
        elif method_name == "worker_collect":
            storage.worker_collect(eid)
        elif method_name == "scan_worker_put":
            storage.scan_worker_put(eid, sid, b"x")
        elif method_name == "queue_push":
            storage.queue_push(eid, [b"a", b"b"])
        elif method_name == "queue_pop":
            storage.queue_pop(eid)
        elif method_name == "queue_clear":
            storage.queue_clear(eid)
        elif method_name == "transaction_state_put":
            storage.transaction_state_put(txn, [(b"k", b"v")])
        elif method_name == "transaction_state_clear":
            storage.transaction_state_clear(txn)
        elif method_name == "aggregate_state_put":
            storage.aggregate_state_put(eid, [(1, b"s")])
        elif method_name == "aggregate_state_clear":
            storage.aggregate_state_clear(eid)
        elif method_name == "aggregate_window_partition_put":
            storage.aggregate_window_partition_put(eid, partition_id=1, data=b"p")
        elif method_name == "aggregate_window_partition_delete":
            storage.aggregate_window_partition_delete(eid, partition_id=1)
        elif method_name == "aggregate_window_partition_clear":
            storage.aggregate_window_partition_clear(eid)
        else:  # pragma: no cover - defensive
            raise AssertionError(f"unknown method: {method_name}")

    def _minimal_response(self, endpoint: str) -> dict[str, object]:
        if endpoint == "worker_collect":
            return {"states": []}
        if endpoint == "queue_push":
            return {"count": 2}
        if endpoint == "queue_pop":
            return {"item": None}
        if endpoint in ("queue_clear", "aggregate_state_clear", "aggregate_window_partition_clear"):
            return {"cleared": 0}
        return {}

    def test_attempt_id_present_on_every_destructive_call(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport
    ) -> None:
        """Every destructive method must include a 32-hex attempt_id in the body."""
        for endpoint, method_name in self._destructive_call_cases():
            mock_transport.queue_response(200, self._minimal_response(endpoint))
            self._invoke(storage, method_name)

        for req in mock_transport.requests:
            body = _body(req)
            assert "attempt_id" in body, f"{req.url.path} missing attempt_id"
            assert self._ATTEMPT_RE.match(body["attempt_id"]), (
                f"{req.url.path} attempt_id={body['attempt_id']!r} not 32-hex"
            )

    def test_read_only_calls_do_not_send_attempt_id(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport
    ) -> None:
        """Read-only endpoints must not carry attempt_id."""
        mock_transport.queue_response(200, {"rows": []})
        storage.worker_scan(b"\x01" * 16)
        mock_transport.queue_response(200, {"rows": []})
        storage.scan_worker_scan(b"\x01" * 16)
        mock_transport.queue_response(200, {"values": [None]})
        storage.transaction_state_get(b"\xaa" * 16, [b"k"])
        mock_transport.queue_response(200, {"rows": [None]})
        storage.aggregate_state_get(b"\x01" * 16, [1])
        mock_transport.queue_response(200, {"data": None})
        storage.aggregate_window_partition_get(b"\x01" * 16, partition_id=1)

        for req in mock_transport.requests:
            body = _body(req)
            assert "attempt_id" not in body, f"{req.url.path} unexpectedly carries attempt_id"

    def test_attempt_id_stable_across_retries(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport
    ) -> None:
        """A retried POST must carry the SAME attempt_id as the first attempt.

        This is the load-bearing property for server-side replay: if the
        retry generated a fresh id, the DO would re-execute the operation
        instead of replaying the cached/tombstoned response.
        """
        call_count = 0

        def maybe_fail(_request: httpx.Request) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.RemoteProtocolError("server disconnected")

        mock_transport.on_request = maybe_fail
        # Attempt 2 succeeds.
        mock_transport.queue_response(200, {"states": [_b64(b"x")]})

        states = storage.worker_collect(b"\x01" * 16)
        assert states == [b"x"]
        assert len(mock_transport.requests) == 2
        a1 = _body(mock_transport.requests[0])["attempt_id"]
        a2 = _body(mock_transport.requests[1])["attempt_id"]
        assert a1 == a2, "retry must reuse the original attempt_id"

    def test_attempt_id_unique_per_logical_call(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport
    ) -> None:
        """Different logical calls produce different attempt_ids."""
        mock_transport.queue_response(200, {"states": []})
        mock_transport.queue_response(200, {"states": []})
        storage.worker_collect(b"\x01" * 16)
        storage.worker_collect(b"\x01" * 16)
        a1 = _body(mock_transport.requests[0])["attempt_id"]
        a2 = _body(mock_transport.requests[1])["attempt_id"]
        assert a1 != a2

    # --- Scan Worker State Tests ---

    def test_scan_worker_put_round_trip(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """scan_worker_put sends execution_id/stream_id/state and an attempt_id."""
        eid = b"\x01" * 16
        sid = b"\xde" * 16
        mock_transport.queue_response(200, {})
        storage.scan_worker_put(eid, sid, b"payload")
        body = _body(mock_transport.requests[0])
        assert base64.b64decode(body["execution_id"]) == eid
        assert base64.b64decode(body["stream_id"]) == sid
        assert base64.b64decode(body["state"]) == b"payload"
        assert self._ATTEMPT_RE.match(body["attempt_id"])

    def test_scan_worker_scan_returns_pairs(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """scan_worker_scan returns (stream_id, state) tuples and carries no attempt_id."""
        sid_a = b"\xde" * 16
        sid_b = b"\xad" * 16
        mock_transport.queue_response(
            200,
            {
                "rows": [
                    {"stream_id": _b64(sid_a), "state": _b64(b"alpha")},
                    {"stream_id": _b64(sid_b), "state": _b64(b"beta")},
                ],
            },
        )
        rows = storage.scan_worker_scan(b"\x01" * 16)
        assert rows == [(sid_a, b"alpha"), (sid_b, b"beta")]

    # --- Aggregate State Tests ---

    def test_aggregate_state_put_and_get(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Round-trip aggregate state: put two groups, read both back."""
        eid = b"\x01" * 16

        mock_transport.queue_response(200, {})
        storage.aggregate_state_put(eid, [(1, b"s1"), (2, b"s2")])

        body = _body(mock_transport.requests[0])
        assert base64.b64decode(body["execution_id"]) == eid
        assert self._ATTEMPT_RE.match(body["attempt_id"])
        assert {(it["group_id"], base64.b64decode(it["state"])) for it in body["items"]} == {
            (1, b"s1"),
            (2, b"s2"),
        }

        mock_transport.queue_response(
            200,
            {
                "rows": [
                    {"group_id": 1, "state": _b64(b"s1")},
                    {"group_id": 2, "state": _b64(b"s2")},
                ],
            },
        )
        rows = storage.aggregate_state_get(eid, [1, 2])
        assert rows == [(1, b"s1"), (2, b"s2")]

    def test_aggregate_state_get_misses_return_none(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport
    ) -> None:
        """Misses surface as None in the parallel result list, no attempt_id sent."""
        mock_transport.queue_response(
            200,
            {"rows": [{"group_id": 1, "state": _b64(b"hit")}, None, {"group_id": 5, "state": _b64(b"h2")}]},
        )
        result = storage.aggregate_state_get(b"\x01" * 16, [1, 9, 5])
        assert result == [(1, b"hit"), None, (5, b"h2")]
        body = _body(mock_transport.requests[0])
        assert "attempt_id" not in body  # read-only

    def test_aggregate_state_get_empty_groups_short_circuits(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport
    ) -> None:
        """No request should be issued for an empty group_ids list."""
        result = storage.aggregate_state_get(b"\x01" * 16, [])
        assert result == []
        assert len(mock_transport.requests) == 0

    def test_aggregate_state_put_empty_short_circuits(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport
    ) -> None:
        """No request should be issued for an empty items list."""
        storage.aggregate_state_put(b"\x01" * 16, [])
        assert len(mock_transport.requests) == 0

    def test_aggregate_state_clear(self, storage: FunctionStorageCfDo, mock_transport: _MockTransport) -> None:
        """Clear sends execution_id + attempt_id."""
        mock_transport.queue_response(200, {"cleared": 4})
        storage.aggregate_state_clear(b"\x01" * 16)
        body = _body(mock_transport.requests[0])
        assert base64.b64decode(body["execution_id"]) == b"\x01" * 16
        assert self._ATTEMPT_RE.match(body["attempt_id"])

    # --- Aggregate Window Partition Tests ---

    def test_aggregate_window_partition_put_and_get(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport
    ) -> None:
        """Round-trip a windowed-aggregate partition payload."""
        eid = b"\x01" * 16

        mock_transport.queue_response(200, {})
        storage.aggregate_window_partition_put(eid, partition_id=7, data=b"arrow-ipc-blob")
        body = _body(mock_transport.requests[0])
        assert body["partition_id"] == 7
        assert base64.b64decode(body["data"]) == b"arrow-ipc-blob"
        assert self._ATTEMPT_RE.match(body["attempt_id"])

        mock_transport.queue_response(200, {"data": _b64(b"arrow-ipc-blob")})
        result = storage.aggregate_window_partition_get(eid, partition_id=7)
        assert result == b"arrow-ipc-blob"

    def test_aggregate_window_partition_get_miss(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport
    ) -> None:
        """Miss returns None, no attempt_id sent."""
        mock_transport.queue_response(200, {"data": None})
        result = storage.aggregate_window_partition_get(b"\x01" * 16, partition_id=99)
        assert result is None
        body = _body(mock_transport.requests[0])
        assert "attempt_id" not in body

    def test_aggregate_window_partition_delete(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport
    ) -> None:
        """Delete sends partition_id + attempt_id."""
        mock_transport.queue_response(200, {})
        storage.aggregate_window_partition_delete(b"\x01" * 16, partition_id=7)
        body = _body(mock_transport.requests[0])
        assert body["partition_id"] == 7
        assert self._ATTEMPT_RE.match(body["attempt_id"])

    def test_aggregate_window_partition_clear(
        self, storage: FunctionStorageCfDo, mock_transport: _MockTransport
    ) -> None:
        """Clear sends execution_id + attempt_id."""
        mock_transport.queue_response(200, {"cleared": 3})
        storage.aggregate_window_partition_clear(b"\x01" * 16)
        body = _body(mock_transport.requests[0])
        assert base64.b64decode(body["execution_id"]) == b"\x01" * 16
        assert self._ATTEMPT_RE.match(body["attempt_id"])

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
