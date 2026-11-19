# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""In-memory stand-in for the Cloudflare DO storage service, as an httpx2 transport.

Implements the endpoints `FunctionStorageCfDo` calls with the service's
wire shapes (base64 keys and values, ``{rows, next_after}`` pages), its
1 MiB per-value cap (413 ``payload_too_large``) and its ``attempt_id``
replay for writes. One instance is one tenant; state is keyed by
``shard_key`` so sharding bugs show up. Not a model of the DO's
transactional details -- the live suite in
``test_function_storage_cf_do_integration.py`` covers those.
"""

from __future__ import annotations

import base64
import json
import threading
from collections.abc import Callable
from typing import Any

import httpx2

MAX_VALUE_BYTES = 1024 * 1024


def _d(s: str) -> bytes:
    return base64.b64decode(s)


def _e(b: bytes) -> str:
    return base64.b64encode(b).decode()


class FakeCfDo:
    """The fake service: in-memory tables plus request log and fault hooks."""

    def __init__(self, *, page_rows: int = 2) -> None:
        """Create an empty service whose scan/drain pages hold ``page_rows`` rows."""
        self.page_rows = page_rows
        self.queues: dict[tuple[str, bytes], list[bytes]] = {}
        self.state: dict[tuple[str, bytes, bytes, bytes], bytes] = {}
        self.logs: dict[tuple[str, bytes, bytes, bytes], list[tuple[int, bytes]]] = {}
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self._replay: dict[str, dict[str, Any]] = {}
        self._drains: dict[str, list[tuple[bytes, bytes]]] = {}
        self._next_ordinal = 0
        self._lock = threading.Lock()
        #: Called with ``(endpoint, body)`` before handling; may return a status to fail with.
        self.fault: Callable[[str, dict[str, Any]], int | None] | None = None

    def transport(self) -> httpx2.MockTransport:
        """Return an httpx2 transport serving this fake."""
        return httpx2.MockTransport(self._handle)

    def endpoint_counts(self) -> dict[str, int]:
        """Return how many requests each endpoint received."""
        counts: dict[str, int] = {}
        for endpoint, _ in self.requests:
            counts[endpoint] = counts.get(endpoint, 0) + 1
        return counts

    def stored_values(self) -> list[bytes]:
        """Return every stored value as the DO holds it (envelopes)."""
        with self._lock:
            out = [v for q in self.queues.values() for v in q]
            out += list(self.state.values())
            out += [v for log in self.logs.values() for _, v in log]
            return out

    def _handle(self, request: httpx2.Request) -> httpx2.Response:
        endpoint = request.url.path.lstrip("/")
        body: dict[str, Any] = json.loads(request.content)
        self.requests.append((endpoint, body))
        if self.fault is not None:
            status = self.fault(endpoint, body)
            if status is not None:
                return httpx2.Response(status, json={"error": "injected"})
        with self._lock:
            try:
                return httpx2.Response(200, json=self._dispatch(endpoint, body))
            except _TooLarge as exc:
                return httpx2.Response(413, json={"error": "payload_too_large", "message": str(exc)})

    def _dispatch(self, endpoint: str, body: dict[str, Any]) -> dict[str, Any]:
        attempt = body.get("attempt_id")
        replay_key = f"{endpoint}:{attempt}" if attempt and endpoint not in ("state_drain",) else None
        if replay_key is not None and replay_key in self._replay:
            return self._replay[replay_key]
        result: dict[str, Any] = getattr(self, f"_op_{endpoint}")(body["shard_key"], body)
        if replay_key is not None:
            self._replay[replay_key] = result
        return result

    @staticmethod
    def _check(value: bytes, what: str) -> bytes:
        if len(value) > MAX_VALUE_BYTES:
            raise _TooLarge(f"{what} exceeds {MAX_VALUE_BYTES} bytes (got {len(value)})")
        return value

    # --- queue ---

    def _op_queue_push(self, shard: str, body: dict[str, Any]) -> dict[str, Any]:
        items = [self._check(_d(i), "work item") for i in body["items"]]
        self.queues.setdefault((shard, _d(body["execution_id"])), []).extend(items)
        return {"count": len(items)}

    def _op_queue_pop(self, shard: str, body: dict[str, Any]) -> dict[str, Any]:
        q = self.queues.get((shard, _d(body["execution_id"])), [])
        return {"item": _e(q.pop(0)) if q else None}

    def _op_queue_clear(self, shard: str, body: dict[str, Any]) -> dict[str, Any]:
        q = self.queues.pop((shard, _d(body["execution_id"])), [])
        return {"cleared": len(q)}

    # --- state ---

    def _ns_rows(self, shard: str, scope: bytes, ns: bytes) -> list[tuple[bytes, bytes]]:
        return sorted((k[3], v) for k, v in self.state.items() if k[:3] == (shard, scope, ns))

    def _op_state_get_many(self, shard: str, body: dict[str, Any]) -> dict[str, Any]:
        scope, ns = _d(body["scope_id"]), _d(body["ns"])
        rows = []
        for k in body["keys"]:
            v = self.state.get((shard, scope, ns, _d(k)))
            rows.append(None if v is None else {"value": _e(v)})
        return {"rows": rows}

    def _op_state_put_many(self, shard: str, body: dict[str, Any]) -> dict[str, Any]:
        scope, ns = _d(body["scope_id"]), _d(body["ns"])
        for item in body["items"]:
            self.state[(shard, scope, ns, _d(item["key"]))] = self._check(_d(item["value"]), "value")
        return {}

    def _page(self, rows: list[tuple[bytes, bytes]], after: str | None) -> dict[str, Any]:
        start = 0
        if after is not None:
            start = next((i + 1 for i, (k, _) in enumerate(rows) if _e(k) == after), len(rows))
        page = rows[start : start + self.page_rows]
        more = start + self.page_rows < len(rows)
        return {
            "rows": [{"key": _e(k), "value": _e(v)} for k, v in page],
            "next_after": _e(page[-1][0]) if more and page else None,
        }

    def _op_state_scan(self, shard: str, body: dict[str, Any]) -> dict[str, Any]:
        rows = self._ns_rows(shard, _d(body["scope_id"]), _d(body["ns"]))
        return self._page(rows, body.get("after_key"))

    def _op_state_drain(self, shard: str, body: dict[str, Any]) -> dict[str, Any]:
        scope, ns = _d(body["scope_id"]), _d(body["ns"])
        attempt = body["attempt_id"]
        if body.get("after_key") is None and attempt not in self._drains:
            self._drains[attempt] = self._ns_rows(shard, scope, ns)
            for k in [k for k in self.state if k[:3] == (shard, scope, ns)]:
                del self.state[k]
        return self._page(self._drains[attempt], body.get("after_key"))

    def _op_state_delete(self, shard: str, body: dict[str, Any]) -> dict[str, Any]:
        scope, ns = _d(body["scope_id"]), _d(body["ns"])
        keys = [k for k in self.state if k[:3] == (shard, scope, ns)]
        if "keys" in body:
            wanted = {_d(k) for k in body["keys"]}
            keys = [k for k in keys if k[3] in wanted]
        for k in keys:
            del self.state[k]
        return {"deleted": len(keys)}

    def _op_execution_clear(self, shard: str, body: dict[str, Any]) -> dict[str, Any]:
        scope = _d(body["scope_id"])
        keys = [k for k in self.state if k[:2] == (shard, scope)]
        logs = [k for k in self.logs if k[:2] == (shard, scope)]
        for k in keys:
            del self.state[k]
        for k in logs:
            del self.logs[k]
        return {"deleted": len(keys) + len(logs)}

    def _op_state_append(self, shard: str, body: dict[str, Any]) -> dict[str, Any]:
        key = (shard, _d(body["scope_id"]), _d(body["ns"]), _d(body["key"]))
        item = self._check(_d(body["item"]), "log item")
        self._next_ordinal += 1
        self.logs.setdefault(key, []).append((self._next_ordinal, item))
        return {"ordinal": self._next_ordinal}

    def _op_state_log_scan(self, shard: str, body: dict[str, Any]) -> dict[str, Any]:
        key = (shard, _d(body["scope_id"]), _d(body["ns"]), _d(body["key"]))
        rows = [(i, v) for i, v in self.logs.get(key, []) if i > body["after_id"]]
        if body.get("limit") is not None:
            rows = rows[: body["limit"]]
        return {"rows": [{"id": i, "value": _e(v)} for i, v in rows]}


class _TooLarge(Exception):
    pass
