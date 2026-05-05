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
import contextlib
import http.client
import json
import logging
import os
import time
from typing import Any
from urllib.parse import urlparse

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

    """

    def __init__(self, *, url: str, token: str | None = None) -> None:
        """Initialize Cloudflare DO storage client.

        Args:
            url: Base URL of the Cloudflare Worker
                (e.g., ``https://vgi-storage.myaccount.workers.dev``).
            token: Optional bearer token for authentication.

        """
        parsed = urlparse(url)
        self._scheme = parsed.scheme
        self._host = parsed.hostname or ""
        self._port = parsed.port
        self._path_prefix = (parsed.path or "").rstrip("/")
        self._token = token
        self._conn: http.client.HTTPConnection | None = None

    def _new_connection(self) -> http.client.HTTPConnection:
        """Create a new HTTP connection."""
        t0 = time.monotonic()
        conn: http.client.HTTPConnection
        if self._scheme == "https":
            conn = http.client.HTTPSConnection(self._host, self._port, timeout=30)
        else:
            conn = http.client.HTTPConnection(self._host, self._port or 80, timeout=30)
        elapsed_ms = (time.monotonic() - t0) * 1000
        _logger.debug("connect host=%s elapsed_ms=%.1f", self._host, elapsed_ms)
        return conn

    def _get_conn(self) -> http.client.HTTPConnection:
        """Return a persistent connection, creating one if needed."""
        if self._conn is None:
            self._conn = self._new_connection()
        return self._conn

    def _drop_conn(self) -> None:
        """Close and discard the current connection."""
        if self._conn is not None:
            with contextlib.suppress(Exception):
                self._conn.close()
            self._conn = None

    def _post(self, endpoint: str, body: dict[str, object]) -> dict[str, Any]:
        """POST JSON to the CF Worker, with retry on connection failure.

        Returns the parsed JSON response. Raises UnknownInvocationError
        on 404 with ``error: "unknown_invocation"``.
        """
        path = f"{self._path_prefix}/{endpoint}"
        payload = json.dumps(body).encode()
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"

        for attempt in range(2):
            try:
                conn = self._get_conn()
                conn.request("POST", path, body=payload, headers=headers)
                resp = conn.getresponse()
                resp_body = resp.read()
                data = json.loads(resp_body)

                if resp.status == 404 and data.get("error") == "unknown_invocation":
                    raise UnknownInvocationError(
                        data.get(
                            "message", "Invocation is not registered. Call queue_push first to register the invocation."
                        )
                    )
                if resp.status == 401:
                    raise PermissionError(f"Authentication failed: {data.get('error', 'unauthorized')}")
                if resp.status >= 400:
                    raise RuntimeError(f"CF DO storage error {resp.status}: {data}")
                return data  # type: ignore[no-any-return]
            except UnknownInvocationError:
                raise
            except PermissionError:
                raise
            except (http.client.HTTPException, OSError, ConnectionError) as exc:
                if attempt == 0:
                    _logger.debug("retry after %s: %s", type(exc).__name__, exc)
                    self._drop_conn()
                else:
                    raise

        raise RuntimeError("unreachable")  # pragma: no cover

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
        """Not yet supported on Cloudflare DO."""
        raise NotImplementedError(
            "Transaction state is not yet supported with the Cloudflare Durable Object storage backend."
        )

    def transaction_state_put(self, transaction_id: bytes, items: list[tuple[bytes, bytes]]) -> None:
        """Not yet supported on Cloudflare DO."""
        raise NotImplementedError(
            "Transaction state is not yet supported with the Cloudflare Durable Object storage backend."
        )

    def transaction_state_clear(self, transaction_id: bytes) -> None:
        """Not yet supported on Cloudflare DO."""
        raise NotImplementedError(
            "Transaction state is not yet supported with the Cloudflare Durable Object storage backend."
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
