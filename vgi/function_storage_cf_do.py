"""Cloudflare Durable Object storage for VGI function state.

This module provides a FunctionStorage implementation backed by a Cloudflare
Worker + Durable Object. The DO runs SQLite internally, providing the same
semantics as FunctionStorageSqlite but accessible over HTTP from any platform.

Implementation:
    FunctionStorageCfDo: HTTP client for the Cloudflare DO storage backend.

Usage:
    Set ``VGI_WORKER_SHARED_STORAGE=cloudflare-do`` plus ``VGI_CF_DO_URL``
    to enable. Optionally set ``VGI_CF_DO_TOKEN`` for bearer auth.

"""

import base64
import json
import logging
import os
import time
from typing import Any

import httpx

from vgi.function_storage import UnknownInvocationError

__all__ = [
    "FunctionStorageCfDo",
]

_logger = logging.getLogger("vgi.storage.cf_do")

# Optional file-based debug logging
_debug_log_path = os.environ.get("VGI_CF_DO_DEBUG_LOG")
if _debug_log_path:
    _fh = logging.FileHandler(_debug_log_path)
    _fh.setLevel(logging.DEBUG)
    _fh.setFormatter(logging.Formatter("%(asctime)s %(process)d %(message)s"))
    _logger.addHandler(_fh)
    _logger.setLevel(logging.DEBUG)


class FunctionStorageCfDo:
    """Cloudflare Durable Object-backed storage for VGI function state.

    Communicates with a Cloudflare Worker that routes requests to a single
    Durable Object running SQLite. The DO is single-threaded, so all
    operations are inherently atomic — no locking needed.

    Uses a single ``httpx.Client`` shared across threads. ``httpx.Client`` is
    thread-safe by design (its connection pool serialises access per-conn),
    so callers from concurrent producer turns can hit this storage instance
    without coordination.

    """

    # Connection-level retries (DNS / TCP / TLS handshake failures).
    # Status- and read-level retries are layered on top in ``_post`` so
    # 5xx responses and mid-response disconnects also recover.
    _CONNECT_RETRIES = 2
    _POST_ATTEMPTS = 3

    def __init__(self, *, url: str, token: str | None = None) -> None:
        """Initialize Cloudflare DO storage client.

        Args:
            url: Base URL of the Cloudflare Worker
                (e.g., ``https://vgi-storage.myaccount.workers.dev``).
            token: Optional bearer token for authentication.

        """
        self._url = url.rstrip("/")
        self._token = token
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self._client = httpx.Client(
            base_url=self._url,
            headers=headers,
            timeout=httpx.Timeout(30.0),
            limits=httpx.Limits(
                max_keepalive_connections=20,
                max_connections=100,
                keepalive_expiry=30.0,
            ),
            transport=httpx.HTTPTransport(retries=self._CONNECT_RETRIES),
        )

    def close(self) -> None:
        """Close the underlying HTTP client and its connection pool."""
        self._client.close()

    def __enter__(self) -> "FunctionStorageCfDo":
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.close()

    def _post(self, endpoint: str, body: dict[str, object]) -> dict[str, Any]:
        """POST JSON to the CF Worker, with retry on transient failure.

        Returns the parsed JSON response. Raises:
            UnknownInvocationError: on 404 with ``error: "unknown_invocation"``
            PermissionError: on 401
            RuntimeError: on other 4xx (non-retryable) and exhausted retries
                          on 5xx (retryable but failed every time)
        """
        path = f"/{endpoint}"
        last_exc: Exception | None = None

        for attempt in range(self._POST_ATTEMPTS):
            try:
                resp = self._client.post(path, json=body)
            except httpx.HTTPError as exc:
                # Connection error, read error, timeout, etc. The transport
                # layer already retried connect-level failures; if we're
                # here it's something the higher-level retry may still help
                # with (e.g. server closed an idle keep-alive between our
                # last response and this request).
                _logger.debug(
                    "post %s attempt=%d transport error: %s: %s",
                    endpoint, attempt, type(exc).__name__, exc,
                )
                last_exc = exc
                continue

            try:
                data: dict[str, Any] = resp.json()
            except (json.JSONDecodeError, ValueError) as exc:
                # Non-JSON response (HTML error page, empty body, etc.) —
                # treat as a transient server problem rather than letting
                # JSONDecodeError bubble up unhelpfully.
                _logger.debug(
                    "post %s attempt=%d non-json status=%d body=%r",
                    endpoint, attempt, resp.status_code, resp.content[:200],
                )
                last_exc = RuntimeError(
                    f"CF DO storage returned non-JSON response "
                    f"(status={resp.status_code}): {resp.content[:200]!r}"
                )
                continue

            if resp.status_code == 404 and data.get("error") == "unknown_invocation":
                raise UnknownInvocationError(
                    data.get(
                        "message",
                        "Invocation is not registered. Call queue_push first to register the invocation.",
                    )
                )
            if resp.status_code == 401:
                raise PermissionError(
                    f"Authentication failed: {data.get('error', 'unauthorized')}"
                )
            if 500 <= resp.status_code < 600:
                # Transient server error — retry.
                _logger.debug(
                    "post %s attempt=%d server error status=%d data=%r",
                    endpoint, attempt, resp.status_code, data,
                )
                last_exc = RuntimeError(
                    f"CF DO storage error {resp.status_code}: {data}"
                )
                continue
            if resp.status_code >= 400:
                # Other 4xx — don't retry, the request itself is bad.
                raise RuntimeError(
                    f"CF DO storage error {resp.status_code}: {data}"
                )
            return data

        assert last_exc is not None
        raise last_exc

    # --- Worker State ---

    def worker_put(self, execution_id: bytes, worker_id: int, state: bytes) -> None:
        """Store state for a specific worker."""
        t0 = time.monotonic()
        self._post(
            "worker_put",
            {
                "execution_id": base64.b64encode(execution_id).decode(),
                "worker_id": worker_id,
                "state": base64.b64encode(state).decode(),
            },
        )
        _logger.debug(
            "worker_put eid=%s worker_id=%d state_bytes=%d elapsed_ms=%.1f",
            execution_id.hex()[:8],
            worker_id,
            len(state),
            (time.monotonic() - t0) * 1000,
        )

    def worker_collect(self, execution_id: bytes) -> list[bytes]:
        """Atomically collect and delete all worker states."""
        t0 = time.monotonic()
        data = self._post(
            "worker_collect",
            {
                "execution_id": base64.b64encode(execution_id).decode(),
            },
        )
        states = [base64.b64decode(s) for s in data["states"]]
        _logger.debug(
            "worker_collect eid=%s states_returned=%d elapsed_ms=%.1f",
            execution_id.hex()[:8],
            len(states),
            (time.monotonic() - t0) * 1000,
        )
        return states

    def worker_scan(self, execution_id: bytes) -> list[tuple[int, bytes]]:
        """Non-destructive read of (worker_id, state) pairs for execution_id."""
        t0 = time.monotonic()
        data = self._post(
            "worker_scan",
            {
                "execution_id": base64.b64encode(execution_id).decode(),
            },
        )
        rows = [(int(r["worker_id"]), base64.b64decode(r["state"])) for r in data["rows"]]
        _logger.debug(
            "worker_scan eid=%s rows=%d elapsed_ms=%.1f",
            execution_id.hex()[:8],
            len(rows),
            (time.monotonic() - t0) * 1000,
        )
        return rows

    # --- Scan Worker State ---

    def scan_worker_put(self, execution_id: bytes, stream_id: bytes, state: bytes) -> None:
        """Store per-(execution_id, stream_id) state."""
        t0 = time.monotonic()
        self._post(
            "scan_worker_put",
            {
                "execution_id": base64.b64encode(execution_id).decode(),
                "stream_id": base64.b64encode(stream_id).decode(),
                "state": base64.b64encode(state).decode(),
            },
        )
        _logger.debug(
            "scan_worker_put eid=%s stream=%s state_bytes=%d elapsed_ms=%.1f",
            execution_id.hex()[:8],
            stream_id.hex()[:8],
            len(state),
            (time.monotonic() - t0) * 1000,
        )

    def scan_worker_scan(self, execution_id: bytes) -> list[tuple[bytes, bytes]]:
        """Non-destructive read of (stream_id, state) pairs for execution_id."""
        t0 = time.monotonic()
        data = self._post(
            "scan_worker_scan",
            {
                "execution_id": base64.b64encode(execution_id).decode(),
            },
        )
        rows = [
            (base64.b64decode(r["stream_id"]), base64.b64decode(r["state"]))
            for r in data["rows"]
        ]
        _logger.debug(
            "scan_worker_scan eid=%s rows=%d elapsed_ms=%.1f",
            execution_id.hex()[:8],
            len(rows),
            (time.monotonic() - t0) * 1000,
        )
        return rows

    # --- Work Queue ---

    def queue_push(self, execution_id: bytes, items: list[bytes]) -> int:
        """Add work items to the queue and register the invocation."""
        t0 = time.monotonic()
        data = self._post(
            "queue_push",
            {
                "execution_id": base64.b64encode(execution_id).decode(),
                "items": [base64.b64encode(item).decode() for item in items],
            },
        )
        count = int(data["count"])
        _logger.debug(
            "queue_push eid=%s items=%d elapsed_ms=%.1f",
            execution_id.hex()[:8],
            count,
            (time.monotonic() - t0) * 1000,
        )
        return count

    def queue_pop(self, execution_id: bytes) -> bytes | None:
        """Atomically claim one work item from the queue.

        Raises:
            UnknownInvocationError: If execution_id was never registered via
                queue_push or has been cleared via queue_clear.

        """
        t0 = time.monotonic()
        data = self._post(
            "queue_pop",
            {
                "execution_id": base64.b64encode(execution_id).decode(),
            },
        )
        result = base64.b64decode(data["item"]) if data["item"] else None
        got_item = result is not None
        _logger.debug(
            "queue_pop eid=%s result=%s elapsed_ms=%.1f",
            execution_id.hex()[:8],
            "item" if got_item else "empty",
            (time.monotonic() - t0) * 1000,
        )
        return result

    def queue_clear(self, execution_id: bytes) -> int:
        """Clear all remaining work items and unregister the invocation."""
        t0 = time.monotonic()
        data = self._post(
            "queue_clear",
            {
                "execution_id": base64.b64encode(execution_id).decode(),
            },
        )
        cleared = int(data["cleared"])
        _logger.debug(
            "queue_clear eid=%s cleared=%d elapsed_ms=%.1f",
            execution_id.hex()[:8],
            cleared,
            (time.monotonic() - t0) * 1000,
        )
        return cleared

    # --- Aggregate State ---

    def aggregate_state_get(self, execution_id: bytes, group_ids: list[int]) -> list[tuple[int, bytes] | None]:
        """Not yet supported on Cloudflare DO."""
        raise NotImplementedError(
            "Aggregate functions are not yet supported with the Cloudflare Durable Object storage backend."
        )

    def aggregate_state_put(self, execution_id: bytes, data: list[tuple[int, bytes]]) -> None:
        """Not yet supported on Cloudflare DO."""
        raise NotImplementedError(
            "Aggregate functions are not yet supported with the Cloudflare Durable Object storage backend."
        )

    def aggregate_state_clear(self, execution_id: bytes) -> None:
        """Not yet supported on Cloudflare DO."""
        raise NotImplementedError(
            "Aggregate functions are not yet supported with the Cloudflare Durable Object storage backend."
        )

    # --- Transaction State ---

    def transaction_state_get(self, transaction_id: bytes, keys: list[bytes]) -> list[bytes | None]:
        """Load transaction-scoped values for the given keys."""
        if not keys:
            return []
        t0 = time.monotonic()
        data = self._post(
            "transaction_state_get",
            {
                "transaction_id": base64.b64encode(transaction_id).decode(),
                "keys": [base64.b64encode(k).decode() for k in keys],
            },
        )
        # ``values`` is parallel to ``keys`` — null for misses, b64 for hits.
        result: list[bytes | None] = [
            base64.b64decode(v) if v is not None else None for v in data["values"]
        ]
        _logger.debug(
            "transaction_state_get txn=%s keys=%d hits=%d elapsed_ms=%.1f",
            transaction_id.hex()[:8],
            len(keys),
            sum(1 for v in result if v is not None),
            (time.monotonic() - t0) * 1000,
        )
        return result

    def transaction_state_put(self, transaction_id: bytes, items: list[tuple[bytes, bytes]]) -> None:
        """Write transaction-scoped values."""
        if not items:
            return
        t0 = time.monotonic()
        self._post(
            "transaction_state_put",
            {
                "transaction_id": base64.b64encode(transaction_id).decode(),
                "items": [
                    {
                        "key": base64.b64encode(k).decode(),
                        "value": base64.b64encode(v).decode(),
                    }
                    for k, v in items
                ],
            },
        )
        _logger.debug(
            "transaction_state_put txn=%s items=%d elapsed_ms=%.1f",
            transaction_id.hex()[:8],
            len(items),
            (time.monotonic() - t0) * 1000,
        )

    def transaction_state_clear(self, transaction_id: bytes) -> None:
        """Drop all state for a transaction."""
        t0 = time.monotonic()
        self._post(
            "transaction_state_clear",
            {
                "transaction_id": base64.b64encode(transaction_id).decode(),
            },
        )
        _logger.debug(
            "transaction_state_clear txn=%s elapsed_ms=%.1f",
            transaction_id.hex()[:8],
            (time.monotonic() - t0) * 1000,
        )

    def aggregate_window_partition_put(self, execution_id: bytes, partition_id: int, data: bytes) -> None:
        """Not yet supported on Cloudflare DO."""
        raise NotImplementedError(
            "Aggregate window functions are not yet supported with the Cloudflare Durable Object storage backend."
        )

    def aggregate_window_partition_get(self, execution_id: bytes, partition_id: int) -> bytes | None:
        """Not yet supported on Cloudflare DO."""
        raise NotImplementedError(
            "Aggregate window functions are not yet supported with the Cloudflare Durable Object storage backend."
        )

    def aggregate_window_partition_delete(self, execution_id: bytes, partition_id: int) -> None:
        """Not yet supported on Cloudflare DO."""
        raise NotImplementedError(
            "Aggregate window functions are not yet supported with the Cloudflare Durable Object storage backend."
        )

    def aggregate_window_partition_clear(self, execution_id: bytes) -> None:
        """Not yet supported on Cloudflare DO."""
        raise NotImplementedError(
            "Aggregate window functions are not yet supported with the Cloudflare Durable Object storage backend."
        )

    # --- Factory ---

    @classmethod
    def from_env(cls) -> "FunctionStorageCfDo":
        """Create an instance from environment variables.

        Required:
            VGI_CF_DO_URL: Base URL of the Cloudflare Worker.

        Optional:
            VGI_CF_DO_TOKEN: Bearer token for authentication.

        """
        url = os.environ.get("VGI_CF_DO_URL")
        if not url:
            raise ValueError(
                "VGI_CF_DO_URL environment variable is required when VGI_WORKER_SHARED_STORAGE=cloudflare-do"
            )
        return cls(
            url=url,
            token=os.environ.get("VGI_CF_DO_TOKEN") or None,
        )
