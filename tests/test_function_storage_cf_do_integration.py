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
# Worker state
# ---------------------------------------------------------------------------


def test_worker_put_and_collect_round_trip(storage: FunctionStorageCfDo) -> None:
    """Put three worker states, collect them, and confirm the table is empty after."""
    eid = _eid()
    storage.worker_put(eid, worker_id=1, state=b"alpha")
    storage.worker_put(eid, worker_id=2, state=b"beta")
    storage.worker_put(eid, worker_id=3, state=b"gamma")

    states = storage.worker_collect(eid)
    assert sorted(states) == [b"alpha", b"beta", b"gamma"]

    # After collect, a non-destructive scan must see nothing — tombstones are
    # filtered by `collected_at IS NULL` in worker_scan.
    assert storage.worker_scan(eid) == []


def test_worker_collect_replays_same_states_on_retry(storage: FunctionStorageCfDo) -> None:
    """Verify same attempt_id returns the SAME tombstoned rows.

    This is the load-bearing idempotency property. We bypass the public
    method (which generates a fresh attempt_id per call) and drive `_post`
    directly so we can fix the attempt_id across two calls — simulating a
    retried request whose first response was lost on the wire.
    """
    import base64

    eid = _eid()
    storage.worker_put(eid, worker_id=1, state=b"alpha")
    storage.worker_put(eid, worker_id=2, state=b"beta")

    attempt = uuid.uuid4().hex
    body = {"execution_id": base64.b64encode(eid).decode()}

    first = storage._post("worker_collect", body, attempt_id=attempt)
    second = storage._post("worker_collect", body, attempt_id=attempt)

    assert first == second, "replay must return byte-identical response"
    assert sorted(base64.b64decode(s) for s in first["states"]) == [b"alpha", b"beta"]

    # An intervening put should NOT leak into a retry of the original collect.
    storage.worker_put(eid, worker_id=3, state=b"gamma")
    replay = storage._post("worker_collect", body, attempt_id=attempt)
    assert replay == first, "tombstone replay must not include later writes"

    # A fresh attempt_id picks up the new put (and any not-yet-collected rows).
    fresh = storage.worker_collect(eid)
    assert b"gamma" in fresh


def test_worker_put_replay_short_circuits(storage: FunctionStorageCfDo) -> None:
    """Verify replaying worker_put with the same attempt_id is a no-op.

    Models the real retry case: the same attempt fires twice in immediate
    succession (because the first response was lost on the wire). The server
    short-circuits on (eid, process_id, last_attempt_id), so the second call
    returns 200 without touching the row.

    Note: this guarantee is "latest-attempt-only" — a stale retry that lands
    after a *different* attempt has written can still clobber. See the
    comment on ``workerPut`` in index.ts. The Python client never produces
    that interleaving because ``_post``'s retry loop holds the caller.
    """
    import base64

    eid = _eid()
    attempt = uuid.uuid4().hex
    body = {
        "execution_id": base64.b64encode(eid).decode(),
        "worker_id": 1,
        "state": base64.b64encode(b"v1").decode(),
    }
    # Two posts with same attempt_id, no other writes interleaved.
    storage._post("worker_put", body, attempt_id=attempt)
    storage._post("worker_put", body, attempt_id=attempt)

    scan = dict(storage.worker_scan(eid))
    assert scan == {1: b"v1"}, "replay must produce exactly one row with v1"


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
# Scan worker state
# ---------------------------------------------------------------------------


def test_stream_state_put_and_scan_round_trip(storage: FunctionStorageCfDo) -> None:
    """Round-trip scan worker state for two distinct stream_ids."""
    eid = _eid()
    sid_a = secrets.token_bytes(16)
    sid_b = secrets.token_bytes(16)
    storage.stream_state_put(eid, sid_a, b"alpha")
    storage.stream_state_put(eid, sid_b, b"beta")

    rows = dict(storage.stream_state_scan(eid))
    assert rows == {sid_a: b"alpha", sid_b: b"beta"}


# ---------------------------------------------------------------------------
# Transaction state
# ---------------------------------------------------------------------------


def test_transaction_state_put_get_clear(storage: FunctionStorageCfDo) -> None:
    """Put two keys, read them back including a miss, then clear."""
    txn = secrets.token_bytes(16)
    storage.transaction_state_put(txn, [(b"k1", b"v1"), (b"k2", b"v2")])

    values = storage.transaction_state_get(txn, [b"k1", b"k2", b"missing"])
    assert values == [b"v1", b"v2", None]

    storage.transaction_state_clear(txn)
    values = storage.transaction_state_get(txn, [b"k1", b"k2"])
    assert values == [None, None]


def test_transaction_state_put_replay_idempotent(storage: FunctionStorageCfDo) -> None:
    """Verify replay short-circuits via last_attempt_id on the first item's row.

    Models the real retry case: same attempt fires twice in succession.
    The first call writes; the second call hits the short-circuit and is a
    no-op. Same "latest-attempt-only" limitation as ``worker_put`` (see
    that test).
    """
    import base64

    txn = secrets.token_bytes(16)
    attempt = uuid.uuid4().hex
    body = {
        "transaction_opaque_data": base64.b64encode(txn).decode(),
        "items": [
            {"key": base64.b64encode(b"k").decode(), "value": base64.b64encode(b"v1").decode()},
        ],
    }
    storage._post("transaction_state_put", body, attempt_id=attempt)
    storage._post("transaction_state_put", body, attempt_id=attempt)
    assert storage.transaction_state_get(txn, [b"k"]) == [b"v1"]


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


# ---------------------------------------------------------------------------
# Aggregate state
# ---------------------------------------------------------------------------


def test_aggregate_state_put_get_round_trip(storage: FunctionStorageCfDo) -> None:
    """Put three groups, read four back (one miss) preserves order."""
    eid = _eid()
    storage.aggregate_state_put(eid, [(1, b"s1"), (2, b"s2"), (5, b"s5")])

    rows = storage.aggregate_state_get(eid, [1, 2, 9, 5])
    assert rows == [(1, b"s1"), (2, b"s2"), None, (5, b"s5")]


def test_aggregate_state_put_replaces(storage: FunctionStorageCfDo) -> None:
    """A second put for the same group_id overwrites the first."""
    eid = _eid()
    storage.aggregate_state_put(eid, [(1, b"old")])
    storage.aggregate_state_put(eid, [(1, b"new")])
    assert storage.aggregate_state_get(eid, [1]) == [(1, b"new")]


def test_aggregate_state_clear_removes_all(storage: FunctionStorageCfDo) -> None:
    """Clear drops every state for the execution_id."""
    eid = _eid()
    storage.aggregate_state_put(eid, [(1, b"s1"), (2, b"s2")])
    storage.aggregate_state_clear(eid)
    assert storage.aggregate_state_get(eid, [1, 2]) == [None, None]


def test_aggregate_state_put_replay_idempotent(storage: FunctionStorageCfDo) -> None:
    """Replaying an aggregate put with the same attempt_id is a no-op."""
    import base64

    eid = _eid()
    attempt = uuid.uuid4().hex
    body = {
        "execution_id": base64.b64encode(eid).decode(),
        "items": [{"group_id": 1, "state": base64.b64encode(b"v1").decode()}],
    }
    storage._post("aggregate_state_put", body, attempt_id=attempt)
    storage._post("aggregate_state_put", body, attempt_id=attempt)
    assert storage.aggregate_state_get(eid, [1]) == [(1, b"v1")]


# ---------------------------------------------------------------------------
# Aggregate window partition
# ---------------------------------------------------------------------------


def test_aggregate_window_partition_round_trip(storage: FunctionStorageCfDo) -> None:
    """Put then get returns the same payload bytes."""
    eid = _eid()
    storage.aggregate_window_partition_put(eid, partition_id=0, data=b"arrow-ipc-blob-0")
    storage.aggregate_window_partition_put(eid, partition_id=1, data=b"arrow-ipc-blob-1")

    assert storage.aggregate_window_partition_get(eid, 0) == b"arrow-ipc-blob-0"
    assert storage.aggregate_window_partition_get(eid, 1) == b"arrow-ipc-blob-1"
    assert storage.aggregate_window_partition_get(eid, 99) is None


def test_aggregate_window_partition_delete_then_get(storage: FunctionStorageCfDo) -> None:
    """Delete on one partition leaves others intact."""
    eid = _eid()
    storage.aggregate_window_partition_put(eid, partition_id=0, data=b"a")
    storage.aggregate_window_partition_put(eid, partition_id=1, data=b"b")

    storage.aggregate_window_partition_delete(eid, partition_id=0)
    assert storage.aggregate_window_partition_get(eid, 0) is None
    assert storage.aggregate_window_partition_get(eid, 1) == b"b"

    # Idempotent delete (no row): no error.
    storage.aggregate_window_partition_delete(eid, partition_id=0)


def test_aggregate_window_partition_clear(storage: FunctionStorageCfDo) -> None:
    """Clear removes every partition for the execution_id."""
    eid = _eid()
    storage.aggregate_window_partition_put(eid, partition_id=0, data=b"a")
    storage.aggregate_window_partition_put(eid, partition_id=1, data=b"b")

    storage.aggregate_window_partition_clear(eid)
    assert storage.aggregate_window_partition_get(eid, 0) is None
    assert storage.aggregate_window_partition_get(eid, 1) is None
