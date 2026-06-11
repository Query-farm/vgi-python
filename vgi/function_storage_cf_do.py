# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Cloudflare Durable Object storage for VGI function state.

This module provides a FunctionStorage implementation backed by a Cloudflare
Worker + Durable Object. The DO runs SQLite internally, providing the same
semantics as FunctionStorageSqlite but accessible over HTTP from any platform.

Implementation:
    FunctionStorageCfDo: HTTP client for the Cloudflare DO storage backend.

Usage:
    Set ``VGI_WORKER_SHARED_STORAGE=cloudflare-do`` plus ``VGI_CF_DO_URL``
    to enable. ``VGI_CF_DO_TOKEN`` carries the per-worker API key minted by
    the storage service's admin CLI; the multi-tenant deployment requires it.
    The key resolves (server-side) to this worker's tenant, which isolates
    its storage from other workers sharing the same deployment — the key is
    sent as an opaque ``Authorization: Bearer`` value and is never parsed
    client-side.

Workflow contract:
    Every ``execution_id`` (and ``transaction_opaque_data``) has a single linear
    lifecycle: create → push/put repeatedly → terminal op → DONE. The
    terminal op is ``queue_clear``, ``state_drain``, or
    ``execution_clear``. Ids are never reused after their terminal op.

    ``_post``'s retry loop is synchronous: all retries of one logical call
    (same ``attempt_id``) finish or exhaust before the caller can issue
    the next call. Combined with the lifecycle above, no two different
    attempts can write the same row in interleaved order — a retry of
    attempt A lands before any other attempt B can be in flight against
    the same id. That property is what makes the server's column-only
    replay model sound. If you change this client to break lockstep
    (async fire-and-forget retries, multi-coordinator writes to one
    execution_id, etc.) you also need to revisit the server's replay
    semantics in the ``vgi-cloudflare-durable-object-storage`` repo
    (``src/index.ts``).

"""

import base64
import json
import logging
import os
import time
import uuid
from collections.abc import Iterator
from typing import Any

import httpx

__all__ = [
    "FunctionStorageCfDo",
]

_logger = logging.getLogger("vgi.storage.cf_do")


# Per-shard storage round-trip profiler (opt-in via VGI_STORAGE_PROFILE=1).
#
# The shared profiler (vgi._storage_profile) normally records at the
# BoundStorage facade so any backend can be profiled locally. This backend is
# special: a single logical state_scan / state_drain fans out into many _post
# round-trips (pagination), and that per-page network cost is the whole reason
# cloudflare-do is slower than in-process sqlite. So we record at _post here for
# the true round-trip count, and FunctionStorageCfDo sets
# _profiles_at_transport=True so BoundStorage defers to us (no double-count).
from vgi._storage_profile import _PROFILE_ON, _profiler  # noqa: E402

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

    # This backend self-profiles at the transport layer (``_post``), so the
    # BoundStorage facade defers to avoid double-counting. See _storage_profile.
    _profiles_at_transport = True

    # Remote-sharding backend: every request must carry a valid shard_key, so a
    # BoundStorage built without a sealed attach is a hard error rather than a
    # silent collapse onto one DO. See _resolve_shard_key in function_storage.py.
    requires_shard_key = True

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
        # HTTP/1.1 with keep-alive. HTTP/2 was tested (May 2026) and turned out
        # to regress the cold-DO path 2.5× while only marginally helping warm
        # reads (~20% on transaction_state_get/put). The bottleneck is
        # geographic RTT + DO instantiation, not TLS handshake overhead, so
        # HTTP/2 multiplexing brings little upside and Cloudflare's h2 frontend
        # appears to add latency to cold-path calls.
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
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Close the HTTP client on exit."""
        self.close()

    def _post(
        self,
        endpoint: str,
        body: dict[str, object],
        *,
        attempt_id: str | None = None,
        shard_key: str = "",
    ) -> dict[str, Any]:
        """POST JSON to the CF Worker, with retry on transient failure.

        ``attempt_id`` (when provided) is spliced into the body once, before
        the retry loop, so every retry carries the same id. This is what
        gives the server-side idempotency check something to match against:
        a retried write whose previous response was lost on the wire will
        find the prior attempt's tombstone/row and replay the original
        response instead of re-executing the operation.

        ``shard_key`` selects the Durable Object instance via
        ``idFromName(shard_key)`` on the Worker side. Empty/missing falls
        back to ``"loc-anon"`` (single shared DO for anonymous, no-attach
        callers) — see ``_derive_shard_key`` in function_storage.py.

        Returns the parsed JSON response. Raises:
            PermissionError: on 401
            ValueError: on 400 (contract violation — usually a bug)
            RuntimeError: on other 4xx (non-retryable) and exhausted retries
                          on 5xx (retryable but failed every time)
        """
        path = f"/{endpoint}"
        last_exc: Exception | None = None

        # Always send shard_key — the Worker's router rejects requests
        # without one with 400. Default to "loc-anon" so direct CfDo
        # callers (outside BoundStorage) still work.
        body = {**body, "shard_key": shard_key or "loc-anon"}
        if attempt_id is not None:
            body = {**body, "attempt_id": attempt_id}

        for attempt in range(self._POST_ATTEMPTS):
            t0 = time.monotonic()
            try:
                resp = self._client.post(path, json=body)
            except httpx.RequestError as exc:
                # Connection error, read error, timeout, etc. Narrowed to
                # ``RequestError`` (not the broader ``HTTPError``) so we
                # don't accidentally swallow programmer errors like
                # ``InvalidURL``. The transport layer already retried
                # connect-level failures; if we're here it's something the
                # higher-level retry may still help with (e.g. server
                # closed an idle keep-alive between our last response and
                # this request).
                _logger.debug(
                    "post %s attempt=%d transport error: %s: %s",
                    endpoint,
                    attempt,
                    type(exc).__name__,
                    exc,
                )
                last_exc = exc
                continue

            try:
                data: dict[str, Any] = resp.json()
            except (json.JSONDecodeError, ValueError):
                # Non-JSON response (HTML error page, empty body, etc.) —
                # treat as a transient server problem rather than letting
                # JSONDecodeError bubble up unhelpfully.
                _logger.debug(
                    "post %s attempt=%d non-json status=%d body=%r",
                    endpoint,
                    attempt,
                    resp.status_code,
                    resp.content[:200],
                )
                last_exc = RuntimeError(
                    f"CF DO storage returned non-JSON response (status={resp.status_code}): {resp.content[:200]!r}"
                )
                continue

            if resp.status_code == 401:
                raise PermissionError(f"Authentication failed: {data.get('error', 'unauthorized')}")
            if resp.status_code == 400:
                # Client-contract violation (e.g. missing/invalid attempt_id).
                # Not retryable and almost always a bug worth surfacing loudly.
                raise ValueError(f"CF DO storage rejected request: {data.get('message') or data.get('error') or data}")
            if 500 <= resp.status_code < 600:
                # Transient server error — retry.
                _logger.debug(
                    "post %s attempt=%d server error status=%d data=%r",
                    endpoint,
                    attempt,
                    resp.status_code,
                    data,
                )
                last_exc = RuntimeError(f"CF DO storage error {resp.status_code}: {data}")
                continue
            if resp.status_code >= 400:
                # Other 4xx — don't retry, the request itself is bad.
                raise RuntimeError(f"CF DO storage error {resp.status_code}: {data}")
            if _PROFILE_ON:
                # Largest single wire body, either direction — what provider
                # request/response size caps apply to. resp.request.content is
                # the actual serialized (base64-inflated) request payload.
                req_len = len(resp.request.content) if resp.request is not None else 0
                _profiler.record(
                    str(body["shard_key"]),
                    endpoint,
                    time.monotonic() - t0,
                    max(req_len, len(resp.content)),
                )
            return data

        assert last_exc is not None
        raise last_exc

    # --- Work Queue ---

    def queue_push(self, execution_id: bytes, items: list[bytes], *, shard_key: str = "") -> int:
        """Add work items to the queue and register the invocation."""
        t0 = time.monotonic()
        data = self._post(
            "queue_push",
            {
                "execution_id": base64.b64encode(execution_id).decode(),
                "items": [base64.b64encode(item).decode() for item in items],
            },
            attempt_id=uuid.uuid4().hex,
            shard_key=shard_key,
        )
        count = int(data["count"])
        _logger.debug(
            "queue_push eid=%s items=%d elapsed_ms=%.1f",
            execution_id.hex()[:8],
            count,
            (time.monotonic() - t0) * 1000,
        )
        return count

    def queue_pop(self, execution_id: bytes, *, shard_key: str = "") -> bytes | None:
        """Atomically claim one work item from the queue.

        Returns None when the queue is empty *or* the execution_id was
        never pushed — see the base-class docstring.
        """
        t0 = time.monotonic()
        data = self._post(
            "queue_pop",
            {
                "execution_id": base64.b64encode(execution_id).decode(),
            },
            attempt_id=uuid.uuid4().hex,
            shard_key=shard_key,
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

    def queue_clear(self, execution_id: bytes, *, shard_key: str = "") -> int:
        """Clear all remaining work items and unregister the invocation."""
        t0 = time.monotonic()
        data = self._post(
            "queue_clear",
            {
                "execution_id": base64.b64encode(execution_id).decode(),
            },
            attempt_id=uuid.uuid4().hex,
            shard_key=shard_key,
        )
        cleared = int(data["cleared"])
        _logger.debug(
            "queue_clear eid=%s cleared=%d elapsed_ms=%.1f",
            execution_id.hex()[:8],
            cleared,
            (time.monotonic() - t0) * 1000,
        )
        return cleared

    # ========================================================================
    # Unified state_* client (composite-key K/V over (scope_id, ns, key))
    # ========================================================================
    #
    # Mirrors the server-side handlers in
    # vgi-cloudflare-durable-object-storage/src/index.ts. attempt_id is
    # generated client-side and spliced into the request body before the
    # retry loop in _post() so a retried HTTP call carries the same id and
    # the server's replay-detection returns the prior result rather than
    # re-executing. Read-only methods (state_get_many, state_scan,
    # state_log_scan) don't carry attempt_id.

    def state_get_many(
        self,
        scope_id: bytes,
        ns: bytes,
        keys: list[bytes],
        *,
        shard_key: str = "",
    ) -> list[bytes | None]:
        """Batched non-destructive read of values keyed by ``(scope_id, ns, key)``.

        Single HTTP roundtrip regardless of key count.
        """
        if not keys:
            return []
        t0 = time.monotonic()
        data = self._post(
            "state_get_many",
            {
                "scope_id": base64.b64encode(scope_id).decode(),
                "ns": base64.b64encode(ns).decode(),
                "keys": [base64.b64encode(k).decode() for k in keys],
            },
            shard_key=shard_key,
        )
        # Server returns rows as a list parallel to the input keys.
        rows = data["rows"]
        result: list[bytes | None] = [None if r is None else base64.b64decode(r["value"]) for r in rows]
        _logger.debug(
            "state_get_many scope=%s ns=%s n_keys=%d hits=%d elapsed_ms=%.1f",
            scope_id.hex()[:8],
            ns.hex()[:8],
            len(keys),
            sum(1 for r in result if r is not None),
            (time.monotonic() - t0) * 1000,
        )
        return result

    def state_put_many(
        self,
        scope_id: bytes,
        ns: bytes,
        items: list[tuple[bytes, bytes]],
        *,
        shard_key: str = "",
    ) -> None:
        """Batched atomic upsert of ``(key, value)`` pairs in one namespace."""
        if not items:
            return
        t0 = time.monotonic()
        self._post(
            "state_put_many",
            {
                "scope_id": base64.b64encode(scope_id).decode(),
                "ns": base64.b64encode(ns).decode(),
                "items": [
                    {"key": base64.b64encode(k).decode(), "value": base64.b64encode(v).decode()} for k, v in items
                ],
            },
            attempt_id=uuid.uuid4().hex,
            shard_key=shard_key,
        )
        _logger.debug(
            "state_put_many scope=%s ns=%s n_items=%d elapsed_ms=%.1f",
            scope_id.hex()[:8],
            ns.hex()[:8],
            len(items),
            (time.monotonic() - t0) * 1000,
        )

    def state_scan(
        self,
        scope_id: bytes,
        ns: bytes,
        *,
        start: bytes | None = None,
        end: bytes | None = None,
        reverse: bool = False,
        limit: int | None = None,
        shard_key: str = "",
    ) -> Iterator[tuple[bytes, bytes]]:
        """Stream (key, value) in one namespace, paging under the hood.

        Ordered by key (``reverse=True`` descending), bounded to ``[start, end)``
        and capped at ``limit`` rows. The server returns ordered pages bounded by
        a byte budget plus a ``next_after`` continuation cursor (interpreted
        server-side per ``reverse``), so an arbitrarily large range never builds
        an oversized response. Yields lazily; consumers should iterate.
        """
        t0 = time.monotonic()
        after_key: str | None = None
        n = 0
        remaining = limit
        while True:
            body: dict[str, object] = {
                "scope_id": base64.b64encode(scope_id).decode(),
                "ns": base64.b64encode(ns).decode(),
            }
            if start is not None:
                body["start"] = base64.b64encode(start).decode()
            if end is not None:
                body["end"] = base64.b64encode(end).decode()
            if reverse:
                body["reverse"] = True
            if remaining is not None:
                body["limit"] = int(remaining)
            if after_key is not None:
                body["after_key"] = after_key
            data = self._post("state_scan", body, shard_key=shard_key)
            for r in data["rows"]:
                yield (base64.b64decode(r["key"]), base64.b64decode(r["value"]))
                n += 1
                if remaining is not None:
                    remaining -= 1
            after_key = data.get("next_after")
            if not after_key or (remaining is not None and remaining <= 0):
                break
        _logger.debug(
            "state_scan scope=%s ns=%s rows=%d elapsed_ms=%.1f",
            scope_id.hex()[:8],
            ns.hex()[:8],
            n,
            (time.monotonic() - t0) * 1000,
        )

    def state_drain(
        self,
        scope_id: bytes,
        ns: bytes,
        *,
        shard_key: str = "",
    ) -> Iterator[tuple[bytes, bytes]]:
        """Stream-and-tombstone every (key, value) in one namespace, paged.

        A single ``attempt_id`` is minted once and reused across every page so
        the server's snapshot-then-page drain stays atomic and replay-safe: the
        first page tombstones the whole namespace, later pages read the
        tombstoned snapshot. A retried page (same attempt_id + cursor) replays
        identically. Beginning to iterate commits the drain, so consume fully.
        """
        t0 = time.monotonic()
        attempt_id = uuid.uuid4().hex
        after_key: str | None = None
        n = 0
        while True:
            body: dict[str, object] = {
                "scope_id": base64.b64encode(scope_id).decode(),
                "ns": base64.b64encode(ns).decode(),
            }
            if after_key is not None:
                body["after_key"] = after_key
            data = self._post("state_drain", body, attempt_id=attempt_id, shard_key=shard_key)
            for r in data["rows"]:
                yield (base64.b64decode(r["key"]), base64.b64decode(r["value"]))
                n += 1
            after_key = data.get("next_after")
            if not after_key:
                break
        _logger.debug(
            "state_drain scope=%s ns=%s rows=%d elapsed_ms=%.1f",
            scope_id.hex()[:8],
            ns.hex()[:8],
            n,
            (time.monotonic() - t0) * 1000,
        )

    def state_delete(
        self,
        scope_id: bytes,
        ns: bytes,
        keys: list[bytes] | None = None,
        *,
        start: bytes | None = None,
        end: bytes | None = None,
        shard_key: str = "",
    ) -> int:
        """Delete by key list, by ``[start, end)`` range, or wipe the namespace.

        ``keys`` and the range are mutually exclusive. Naturally idempotent — an
        attempt_id is sent for audit but the server doesn't gate on it
        (delete-of-already-deleted is a no-op).
        """
        if keys is not None and (start is not None or end is not None):
            raise ValueError("state_delete: keys and start/end are mutually exclusive")
        t0 = time.monotonic()
        body: dict[str, object] = {
            "scope_id": base64.b64encode(scope_id).decode(),
            "ns": base64.b64encode(ns).decode(),
        }
        mode = "all"
        if keys is not None:
            body["keys"] = [base64.b64encode(k).decode() for k in keys]
            mode = f"n_keys={len(keys)}"
        elif start is not None or end is not None:
            if start is not None:
                body["start"] = base64.b64encode(start).decode()
            if end is not None:
                body["end"] = base64.b64encode(end).decode()
            mode = "range"
        data = self._post(
            "state_delete",
            body,
            attempt_id=uuid.uuid4().hex,
            shard_key=shard_key,
        )
        deleted = int(data["deleted"])
        _logger.debug(
            "state_delete scope=%s ns=%s mode=%s deleted=%d elapsed_ms=%.1f",
            scope_id.hex()[:8],
            ns.hex()[:8],
            mode,
            deleted,
            (time.monotonic() - t0) * 1000,
        )
        return deleted

    def execution_clear(
        self,
        scope_id: bytes,
        *,
        shard_key: str = "",
    ) -> int:
        """Wipe ALL state and log rows for ``scope_id`` across every namespace."""
        t0 = time.monotonic()
        data = self._post(
            "execution_clear",
            {"scope_id": base64.b64encode(scope_id).decode()},
            attempt_id=uuid.uuid4().hex,
            shard_key=shard_key,
        )
        deleted = int(data["deleted"])
        _logger.debug(
            "execution_clear scope=%s deleted=%d elapsed_ms=%.1f",
            scope_id.hex()[:8],
            deleted,
            (time.monotonic() - t0) * 1000,
        )
        return deleted

    def state_append(
        self,
        scope_id: bytes,
        ns: bytes,
        key: bytes,
        item: bytes,
        *,
        shard_key: str = "",
    ) -> int:
        """Append item to (scope_id, ns, key) log; return assigned ordinal.

        Replay: if a prior call with the same attempt_id already inserted,
        the server returns the prior ordinal so retries are idempotent.
        """
        t0 = time.monotonic()
        data = self._post(
            "state_append",
            {
                "scope_id": base64.b64encode(scope_id).decode(),
                "ns": base64.b64encode(ns).decode(),
                "key": base64.b64encode(key).decode(),
                "item": base64.b64encode(item).decode(),
            },
            attempt_id=uuid.uuid4().hex,
            shard_key=shard_key,
        )
        ordinal = int(data["ordinal"])
        _logger.debug(
            "state_append scope=%s ns=%s key=%s ordinal=%d elapsed_ms=%.1f",
            scope_id.hex()[:8],
            ns.hex()[:8],
            key.hex()[:8],
            ordinal,
            (time.monotonic() - t0) * 1000,
        )
        return ordinal

    def state_log_scan(
        self,
        scope_id: bytes,
        ns: bytes,
        key: bytes,
        *,
        after_id: int = -1,
        limit: int | None = None,
        shard_key: str = "",
    ) -> list[tuple[int, bytes]]:
        """Return (id, value) pairs for (scope_id, ns, key) with id > after_id."""
        t0 = time.monotonic()
        body: dict[str, object] = {
            "scope_id": base64.b64encode(scope_id).decode(),
            "ns": base64.b64encode(ns).decode(),
            "key": base64.b64encode(key).decode(),
            "after_id": after_id,
        }
        if limit is not None:
            body["limit"] = int(limit)
        data = self._post("state_log_scan", body, shard_key=shard_key)
        rows = [(int(r["id"]), base64.b64decode(r["value"])) for r in data["rows"]]
        _logger.debug(
            "state_log_scan scope=%s ns=%s key=%s after_id=%d rows=%d elapsed_ms=%.1f",
            scope_id.hex()[:8],
            ns.hex()[:8],
            key.hex()[:8],
            after_id,
            len(rows),
            (time.monotonic() - t0) * 1000,
        )
        return rows

    # --- Atomic int64 counters (function_counter) ---

    def state_counter_get(self, scope_id: bytes, ns: bytes, key: bytes, *, shard_key: str = "") -> int:
        """Read the int64 counter; 0 if absent."""
        data = self._post(
            "state_counter_get",
            {
                "scope_id": base64.b64encode(scope_id).decode(),
                "ns": base64.b64encode(ns).decode(),
                "key": base64.b64encode(key).decode(),
            },
            shard_key=shard_key,
        )
        return int(data["n"])

    def state_counter_add(self, scope_id: bytes, ns: bytes, key: bytes, delta: int, *, shard_key: str = "") -> int:
        """Atomically add ``delta`` and return the new value.

        Carries an ``attempt_id`` so an HTTP retry replays the prior result on
        the server instead of double-adding.
        """
        data = self._post(
            "state_counter_add",
            {
                "scope_id": base64.b64encode(scope_id).decode(),
                "ns": base64.b64encode(ns).decode(),
                "key": base64.b64encode(key).decode(),
                "delta": int(delta),
            },
            attempt_id=uuid.uuid4().hex,
            shard_key=shard_key,
        )
        return int(data["n"])

    def state_counter_set(self, scope_id: bytes, ns: bytes, key: bytes, value: int, *, shard_key: str = "") -> None:
        """Overwrite the counter with ``value`` (idempotent)."""
        self._post(
            "state_counter_set",
            {
                "scope_id": base64.b64encode(scope_id).decode(),
                "ns": base64.b64encode(ns).decode(),
                "key": base64.b64encode(key).decode(),
                "value": int(value),
            },
            attempt_id=uuid.uuid4().hex,
            shard_key=shard_key,
        )

    def state_counter_delete(self, scope_id: bytes, ns: bytes, key: bytes, *, shard_key: str = "") -> None:
        """Delete the counter (no-op if absent)."""
        self._post(
            "state_counter_delete",
            {
                "scope_id": base64.b64encode(scope_id).decode(),
                "ns": base64.b64encode(ns).decode(),
                "key": base64.b64encode(key).decode(),
            },
            attempt_id=uuid.uuid4().hex,
            shard_key=shard_key,
        )

    # --- Factory ---

    @classmethod
    def from_env(cls) -> "FunctionStorageCfDo":
        """Create an instance from environment variables.

        Required:
            VGI_CF_DO_URL: Base URL of the Cloudflare Worker.

        Optional:
            VGI_CF_DO_TOKEN: Per-worker API key (sent as a bearer token).
                Required by the multi-tenant cloudflare-do deployment, where
                it resolves server-side to this worker's tenant.

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
