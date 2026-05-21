# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Live integration tests for FunctionStorageCfDo against a running DO.

These tests require a real Cloudflare Worker + Durable Object to be running.
Locally that's ``wrangler dev``; in CI it would point at a staging deploy.

Set ``VGI_CF_DO_INTEGRATION_URL`` (e.g. ``http://localhost:8787``) to enable.
Skipped otherwise so the default ``pytest -n auto`` doesn't trip on it.

The mocked tests in ``test_function_storage_cf_do.py`` validate the *client
contract* — does Python send ``attempt_id``? does it reuse it on retry? These
tests validate the *server* contract — does the DO actually replay correctly?
does it persist? do BEGIN/COMMIT-equivalent transactions roll back? — which
the mocks cannot.
"""

from __future__ import annotations

import os
import secrets
import uuid

import httpx
import pytest

from vgi.function_storage_cf_do import FunctionStorageCfDo

# Skip the entire module unless explicitly enabled.
_URL = os.environ.get("VGI_CF_DO_INTEGRATION_URL")
pytestmark = pytest.mark.skipif(
    not _URL,
    reason="set VGI_CF_DO_INTEGRATION_URL to a running wrangler dev / deploy",
)


def _eid() -> bytes:
    """Per-test execution id so tests don't bleed state into each other."""
    return secrets.token_bytes(16)


@pytest.fixture
def storage() -> FunctionStorageCfDo:
    """Build a real client pointed at the running DO."""
    s = FunctionStorageCfDo(url=_URL or "", token=os.environ.get("VGI_CF_DO_TOKEN"))
    yield s
    s.close()


# ---------------------------------------------------------------------------
# Work queue
# ---------------------------------------------------------------------------


def test_queue_push_then_pop_in_fifo_order(storage: FunctionStorageCfDo) -> None:
    """Push three items and verify pop returns them in FIFO order."""
    eid = _eid()
    count = storage.queue_push(eid, [b"a", b"b", b"c"])
    assert count == 3

    assert storage.queue_pop(eid) == b"a"
    assert storage.queue_pop(eid) == b"b"
    assert storage.queue_pop(eid) == b"c"
    assert storage.queue_pop(eid) is None  # registered but empty


def test_queue_push_replay_does_not_duplicate(storage: FunctionStorageCfDo) -> None:
    """Verify pushing twice with the same attempt_id is a no-op the second time.

    Queue length after both calls must equal one push's worth, not double.
    """
    import base64

    eid = _eid()
    attempt = uuid.uuid4().hex
    body = {
        "execution_id": base64.b64encode(eid).decode(),
        "items": [base64.b64encode(b"a").decode(), base64.b64encode(b"b").decode()],
    }
    r1 = storage._post("queue_push", body, attempt_id=attempt)
    r2 = storage._post("queue_push", body, attempt_id=attempt)
    assert r1 == r2 == {"count": 2}

    drained = []
    while (item := storage.queue_pop(eid)) is not None:
        drained.append(item)
    assert drained == [b"a", b"b"], "queue must contain one push's worth, not two"


def test_queue_pop_replay_returns_same_item(storage: FunctionStorageCfDo) -> None:
    """Verify popping twice with the same attempt_id returns the SAME item.

    The tombstone is keyed by the popping attempt id, so a replay hits the
    replay-check SELECT before any new UPDATE…RETURNING runs.
    """
    import base64

    eid = _eid()
    storage.queue_push(eid, [b"a", b"b", b"c"])

    attempt = uuid.uuid4().hex
    body = {"execution_id": base64.b64encode(eid).decode()}
    r1 = storage._post("queue_pop", body, attempt_id=attempt)
    r2 = storage._post("queue_pop", body, attempt_id=attempt)

    assert r1 == r2
    assert base64.b64decode(r1["item"]) == b"a"

    # Fresh attempt → next item, confirming the tombstone is not "consumed"
    # by a replay.
    assert storage.queue_pop(eid) == b"b"


def test_queue_pop_never_pushed_returns_none(storage: FunctionStorageCfDo) -> None:
    """Pop against a never-pushed execution_id returns None.

    No distinction from drained queue per the contract.
    """
    assert storage.queue_pop(_eid()) is None


def test_queue_clear_then_pop_returns_none(storage: FunctionStorageCfDo) -> None:
    """Clear removes queue rows; subsequent pop returns None (queue empty)."""
    eid = _eid()
    storage.queue_push(eid, [b"a", b"b"])
    cleared = storage.queue_clear(eid)
    assert cleared == 2

    assert storage.queue_pop(eid) is None


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_missing_attempt_id_returns_400(storage: FunctionStorageCfDo) -> None:
    """Server enforces the attempt_id contract on destructive endpoints."""
    import base64

    eid = _eid()
    body = {"execution_id": base64.b64encode(eid).decode()}
    with pytest.raises(ValueError, match="attempt_id"):
        # Bypass the public method (which always generates one).
        storage._post("queue_pop", body, attempt_id=None)


def test_invalid_attempt_id_format_returns_400(storage: FunctionStorageCfDo) -> None:
    """The server validates the 32-hex shape, not just presence."""
    import base64

    eid = _eid()
    body = {
        "execution_id": base64.b64encode(eid).decode(),
        "attempt_id": "not-hex",
        # Worker rejects missing shard_key first; supply a placeholder so
        # the attempt_id validation downstream is what we actually exercise.
        "shard_key": "loc-anon",
    }
    # Drive raw httpx so we can ship an arbitrary attempt_id string.
    resp = storage._client.post("/queue_pop", json=body)
    assert resp.status_code == 400
    assert "attempt_id" in resp.text


def test_concurrent_pops_no_dupes(storage: FunctionStorageCfDo) -> None:
    """Verify concurrent pops never return the same item twice.

    Single DO is serialized, so N concurrent pops over M items must return
    exactly M unique items + (N-M) Nones, never the same item twice.
    """
    import threading

    eid = _eid()
    n_items = 50
    storage.queue_push(eid, [f"item-{i}".encode() for i in range(n_items)])

    results: list[bytes | None] = []
    lock = threading.Lock()

    def pop() -> None:
        r = storage.queue_pop(eid)
        with lock:
            results.append(r)

    threads = [threading.Thread(target=pop) for _ in range(n_items + 10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    items = [r for r in results if r is not None]
    assert len(items) == n_items
    assert len(set(items)) == n_items, "no item must be popped twice"


def test_health_check_get_is_rejected(storage: FunctionStorageCfDo) -> None:
    """Sanity: GET is rejected at the router.

    Accept either 405 (when no auth token is configured) or 401 (when one
    is — the router validates the bearer token before method).
    """
    resp = httpx.get(_URL or "")
    assert resp.status_code in (401, 405)


