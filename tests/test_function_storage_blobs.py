# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Value envelope and blob-vendor externalization in the Cloudflare DO client.

Runs `ExternalizingStorage` over `FunctionStorageCfDo` against an in-memory fake of the DO service
(``tests/_fake_cf_do.py``) and vgi-rpc's fake blob vendor.
"""

from __future__ import annotations

import hashlib
import os
import secrets
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import httpx2
import pyarrow as pa
import pytest
from vgi_rpc.conformance.fake_vendor import FakeVendorServer, serve_fake_vendor
from vgi_rpc.external_fetch import FetchConfig
from vgi_rpc.vendor import VendorBlobClient, VendorError

from tests._fake_cf_do import FakeCfDo
from vgi import _storage_profile as sp
from vgi._storage_seal import StoragePayloadError
from vgi.function_storage import BoundStorage, FrameworkNS
from vgi.function_storage_blobs import (
    DEFAULT_EXTERNALIZE_BYTES,
    TAG_INLINE,
    TAG_POINTER,
    ExternalizingStorage,
    StorageEnvelopeError,
    StorageValueTooLargeError,
)
from vgi.function_storage_cf_do import MAX_VALUE_BYTES, FunctionStorageCfDo

THRESHOLD = 4096
SHARD = "att-" + "ab" * 16
SCOPE = b"\x11" * 16
NS = b"ns"


@pytest.fixture
def vendor() -> Iterator[FakeVendorServer]:
    """A fake blob vendor on a background thread."""
    with serve_fake_vendor() as server:
        yield server


def _client(vendor: FakeVendorServer) -> VendorBlobClient:
    return VendorBlobClient(vendor.url, "test-token", fetch_config=FetchConfig(max_fetch_bytes=1 << 32))


def _storage(
    do: FakeCfDo,
    vendor: FakeVendorServer | None,
    *,
    threshold: int = THRESHOLD,
    cls: type[FunctionStorageCfDo] = FunctionStorageCfDo,
) -> ExternalizingStorage:
    base = cls(url="https://do.example", token="t")
    old = base._client
    base._client = httpx2.Client(base_url=str(old.base_url), headers=old.headers, transport=do.transport())
    old.close()
    return ExternalizingStorage(
        base,
        blob_client=_client(vendor) if vendor is not None else None,
        threshold=threshold,
        max_value_bytes=MAX_VALUE_BYTES,
    )


def _tags(do: FakeCfDo) -> list[int]:
    return [v[0] for v in do.stored_values()]


def _objects(vendor: FakeVendorServer) -> list[str]:
    return vendor.vendor.object_ids()


# ---------------------------------------------------------------------------
# Envelope format
# ---------------------------------------------------------------------------


def test_every_value_is_enveloped_without_vendor() -> None:
    """(a): the envelope is applied even with no vendor configured."""
    do = FakeCfDo()
    s = _storage(do, None)
    s.queue_push(SCOPE, [b"a", b""], shard_key=SHARD)
    s.state_put_many(SCOPE, NS, [(b"k", b"\x01looks-like-a-pointer")], shard_key=SHARD)
    s.state_append(SCOPE, NS, b"log", b"x", shard_key=SHARD)
    assert sorted(do.stored_values()) == sorted([b"\x00a", b"\x00", b"\x00\x01looks-like-a-pointer", b"\x00x"])
    assert s.queue_pop(SCOPE, shard_key=SHARD) == b"a"
    assert s.queue_pop(SCOPE, shard_key=SHARD) == b""
    assert s.state_get_many(SCOPE, NS, [b"k"], shard_key=SHARD) == [b"\x01looks-like-a-pointer"]


def test_user_bytes_starting_with_pointer_tag_round_trip(vendor: FakeVendorServer) -> None:
    """User values beginning with 0x01 (or 0x00) are inline values, not pointers."""
    do = FakeCfDo()
    s = _storage(do, vendor)
    values = [b"\x01" + b"\x00" * 40 + b"abc", b"\x00", b"\x01", b"\x02"]
    s.state_put_many(SCOPE, NS, [(bytes([i]), v) for i, v in enumerate(values)], shard_key=SHARD)
    assert s.state_get_many(SCOPE, NS, [bytes([i]) for i in range(4)], shard_key=SHARD) == values
    assert set(_tags(do)) == {TAG_INLINE}
    assert _objects(vendor) == []


@pytest.mark.parametrize("raw", [b"", b"raw-legacy-value", b"\x02abc", b"\x7f"])
def test_unenveloped_value_fails_clearly(raw: bytes) -> None:
    """A value written by a pre-envelope client (or garbage) is refused, not misread."""
    do = FakeCfDo()
    s = _storage(do, None)
    do.state[(SHARD, SCOPE, NS, b"k")] = raw
    with pytest.raises(StorageEnvelopeError, match="envelope"):
        s.state_get_many(SCOPE, NS, [b"k"], shard_key=SHARD)


def test_malformed_pointer_fails_clearly(vendor: FakeVendorServer) -> None:
    """A truncated pointer or one with a bad object id is refused."""
    do = FakeCfDo()
    s = _storage(do, vendor)
    for bad in (b"\x01" + b"\x00" * 10, b"\x01" + b"\x00" * 40 + b"bad id!"):
        do.state[(SHARD, SCOPE, NS, b"k")] = bad
        with pytest.raises(StorageEnvelopeError, match="malformed blob pointer"):
            s.state_get_many(SCOPE, NS, [b"k"], shard_key=SHARD)


def test_pointer_without_vendor_fails_clearly(vendor: FakeVendorServer) -> None:
    """A pointer read by a process with no vendor names the missing setting."""
    do = FakeCfDo()
    _storage(do, vendor).state_put_many(SCOPE, NS, [(b"k", b"x" * THRESHOLD)], shard_key=SHARD)
    with pytest.raises(StorageEnvelopeError, match="VGI_BLOB_VENDOR_URL"):
        _storage(do, None).state_get_many(SCOPE, NS, [b"k"], shard_key=SHARD)


def test_pointer_layout(vendor: FakeVendorServer) -> None:
    """``0x01 || u64be(size) || sha256 || object_id``."""
    do = FakeCfDo()
    value = secrets.token_bytes(THRESHOLD)
    _storage(do, vendor).state_put_many(SCOPE, NS, [(b"k", value)], shard_key=SHARD)
    (env,) = do.stored_values()
    assert env[0] == TAG_POINTER
    assert int.from_bytes(env[1:9], "big") == len(value)
    assert env[9:41] == hashlib.sha256(value).digest()
    assert env[41:].decode() in _objects(vendor)
    assert len(env) < 100


# ---------------------------------------------------------------------------
# Threshold and round trips
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("size", "external"), [(THRESHOLD - 1, False), (THRESHOLD, True), (THRESHOLD + 1, True)])
def test_threshold_boundary(vendor: FakeVendorServer, size: int, external: bool) -> None:
    """Values at or over the threshold go to the vendor; under it stay inline."""
    do = FakeCfDo()
    s = _storage(do, vendor)
    value = secrets.token_bytes(size)
    s.state_put_many(SCOPE, NS, [(b"k", value)], shard_key=SHARD)
    assert _tags(do) == [TAG_POINTER if external else TAG_INLINE]
    assert len(_objects(vendor)) == (1 if external else 0)
    assert s.state_get_many(SCOPE, NS, [b"k"], shard_key=SHARD) == [value]


def _mixed(n: int) -> list[bytes]:
    return [secrets.token_bytes(THRESHOLD * 2 if i % 2 else 10) for i in range(n)]


def test_queue_round_trip(vendor: FakeVendorServer) -> None:
    """queue_push / queue_pop with mixed inline and external items, FIFO order kept."""
    do = FakeCfDo()
    s = _storage(do, vendor)
    items = _mixed(5)
    assert s.queue_push(SCOPE, items, shard_key=SHARD) == 5
    assert sorted(_tags(do)) == [0, 0, 0, 1, 1]
    popped = [s.queue_pop(SCOPE, shard_key=SHARD) for _ in range(5)]
    assert popped == items
    assert s.queue_pop(SCOPE, shard_key=SHARD) is None
    s.flush_blob_cleanup()
    assert _objects(vendor) == []  # each claimed item's blob was deleted


def test_state_put_get_scan_round_trip(vendor: FakeVendorServer) -> None:
    """state_put_many, state_get_many (with misses) and paged state_scan."""
    do = FakeCfDo(page_rows=2)
    s = _storage(do, vendor)
    values = _mixed(5)
    s.state_put_many(SCOPE, NS, [(bytes([i]), v) for i, v in enumerate(values)], shard_key=SHARD)
    keys = [bytes([i]) for i in range(5)] + [b"missing"]
    assert s.state_get_many(SCOPE, NS, keys, shard_key=SHARD) == [*values, None]
    assert list(s.state_scan(SCOPE, NS, shard_key=SHARD)) == [(bytes([i]), v) for i, v in enumerate(values)]
    assert do.endpoint_counts()["state_scan"] == 3


def test_state_drain_round_trip_deletes_drained_blobs(vendor: FakeVendorServer) -> None:
    """state_drain decodes every page, then deletes the blobs it drained."""
    do = FakeCfDo(page_rows=2)
    s = _storage(do, vendor)
    values = _mixed(5)
    s.state_put_many(SCOPE, NS, [(bytes([i]), v) for i, v in enumerate(values)], shard_key=SHARD)
    s.state_put_many(SCOPE, b"other", [(b"k", b"y" * THRESHOLD)], shard_key=SHARD)
    assert len(_objects(vendor)) == 3
    assert list(s.state_drain(SCOPE, NS, shard_key=SHARD)) == [(bytes([i]), v) for i, v in enumerate(values)]
    s.flush_blob_cleanup()
    assert len(_objects(vendor)) == 1  # the other namespace's blob is untouched
    assert s.state_get_many(SCOPE, b"other", [b"k"], shard_key=SHARD) == [b"y" * THRESHOLD]


def test_state_append_and_log_scan_round_trip(vendor: FakeVendorServer) -> None:
    """state_append then state_log_scan with after_id/limit."""
    do = FakeCfDo()
    s = _storage(do, vendor)
    items = _mixed(4)
    ordinals = [s.state_append(SCOPE, NS, b"log", item, shard_key=SHARD) for item in items]
    rows = s.state_log_scan(SCOPE, NS, b"log", shard_key=SHARD)
    assert rows == list(zip(ordinals, items, strict=True))
    assert s.state_log_scan(SCOPE, NS, b"log", after_id=ordinals[0], limit=2, shard_key=SHARD) == rows[1:3]


# ---------------------------------------------------------------------------
# Retries, integrity, clean-up
# ---------------------------------------------------------------------------


def test_do_retry_after_upload_does_not_reupload(vendor: FakeVendorServer) -> None:
    """A 5xx from the DO is retried with the same body; the blob is uploaded once."""
    do = FakeCfDo()
    s = _storage(do, vendor)
    failures = iter([503])
    do.fault = lambda endpoint, body: next(failures, None) if endpoint == "state_append" else None
    s.state_append(SCOPE, NS, b"log", b"q" * THRESHOLD, shard_key=SHARD)
    appends = [b for e, b in do.requests if e == "state_append"]
    assert len(appends) == 2 and appends[0] == appends[1]
    assert len(_objects(vendor)) == 1
    assert vendor.vendor.stats()["/v1/blobs/put"] == 1


def test_vendor_put_retry_uses_idempotency_key(vendor: FakeVendorServer) -> None:
    """A failed vendor put is retried by the client and still yields one object."""
    do = FakeCfDo()
    s = _storage(do, vendor)
    vendor.vendor.fail_next("/v1/blobs/put", status=503)
    s.queue_push(SCOPE, [b"w" * THRESHOLD], shard_key=SHARD)
    assert len(_objects(vendor)) == 1
    assert s.queue_pop(SCOPE, shard_key=SHARD) == b"w" * THRESHOLD
    s.flush_blob_cleanup()


def test_checksum_mismatch_on_fetch_raises(vendor: FakeVendorServer) -> None:
    """Tampered blob bytes are detected against the pointer's SHA-256."""
    do = FakeCfDo()
    s = _storage(do, vendor)
    value = b"v" * THRESHOLD
    s.state_put_many(SCOPE, NS, [(b"k", value)], shard_key=SHARD)
    (object_id,) = _objects(vendor)
    vendor.vendor.corrupt(object_id, b"V" * THRESHOLD)
    with pytest.raises(VendorError, match="SHA-256 mismatch"):
        s.state_get_many(SCOPE, NS, [b"k"], shard_key=SHARD)


def test_blob_scopes(vendor: FakeVendorServer) -> None:
    """Queue and state blobs live in distinct vendor scopes with hex ids."""
    do = FakeCfDo()
    s = _storage(do, vendor)
    s.queue_push(SCOPE, [b"a" * THRESHOLD], shard_key=SHARD)
    s.state_put_many(SCOPE, NS, [(b"k", b"b" * THRESHOLD)], shard_key=SHARD)
    with vendor.vendor._lock:
        scopes = sorted(o.scope for o in vendor.vendor._objects.values() if o.scope is not None)
    assert scopes == [(SHARD, "q." + SCOPE.hex()), (SHARD, "s." + SCOPE.hex())]


def test_execution_clear_deletes_state_scope_only(vendor: FakeVendorServer) -> None:
    """execution_clear deletes its state scope's blobs; the queue's are left."""
    do = FakeCfDo()
    s = _storage(do, vendor)
    s.queue_push(SCOPE, [b"a" * THRESHOLD], shard_key=SHARD)
    s.state_put_many(SCOPE, NS, [(b"k", b"b" * THRESHOLD)], shard_key=SHARD)
    s.state_append(SCOPE, NS, b"log", b"c" * THRESHOLD, shard_key=SHARD)
    s.state_put_many(b"\x22" * 16, NS, [(b"k", b"d" * THRESHOLD)], shard_key=SHARD)
    assert len(_objects(vendor)) == 4
    s.execution_clear(SCOPE, shard_key=SHARD)
    s.flush_blob_cleanup()
    assert len(_objects(vendor)) == 2
    assert vendor.vendor.stats()["/v1/blobs/delete_scope"] == 1
    s.queue_clear(SCOPE, shard_key=SHARD)
    s.flush_blob_cleanup()
    assert len(_objects(vendor)) == 1  # the other execution's blob remains


def test_clean_up_failure_is_logged_not_raised(vendor: FakeVendorServer, caplog: pytest.LogCaptureFixture) -> None:
    """A vendor error during clean-up never fails the storage call."""
    do = FakeCfDo()
    s = _storage(do, vendor)
    s.blob_client._api._max_attempts = 1  # type: ignore[union-attr]
    vendor.vendor.fail_next("/v1/blobs/delete_scope", status=500)
    assert s.execution_clear(SCOPE, shard_key=SHARD) == 0
    s.flush_blob_cleanup()
    assert "clean-up after execution_clear failed" in caplog.text


def test_long_scope_ids_are_hashed(vendor: FakeVendorServer) -> None:
    """A scope id too long for the vendor's 128-char limit is hashed, not refused."""
    do = FakeCfDo()
    s = _storage(do, vendor)
    scope = b"\x33" * 100
    s.state_put_many(scope, NS, [(b"k", b"e" * THRESHOLD)], shard_key="weird shard/key")
    assert s.state_get_many(scope, NS, [b"k"], shard_key="weird shard/key") == [b"e" * THRESHOLD]
    with vendor.vendor._lock:
        (obj,) = vendor.vendor._objects.values()
    assert obj.scope is not None
    shard, execution_id = obj.scope
    assert execution_id == "s.h" + hashlib.sha256(scope).hexdigest()
    assert shard.startswith("h-")


# ---------------------------------------------------------------------------
# No vendor
# ---------------------------------------------------------------------------


def test_no_vendor_large_value_fails_naming_the_setting() -> None:
    """Without a vendor an over-limit value fails client-side, before the DO."""
    do = FakeCfDo()
    s = _storage(do, None)
    s.state_put_many(SCOPE, NS, [(b"k", b"x" * (MAX_VALUE_BYTES - 1))], shard_key=SHARD)  # fits with its tag
    for call in (
        lambda: s.state_put_many(SCOPE, NS, [(b"k", b"x" * MAX_VALUE_BYTES)], shard_key=SHARD),
        lambda: s.queue_push(SCOPE, [b"x" * (2 * MAX_VALUE_BYTES)], shard_key=SHARD),
        lambda: s.state_append(SCOPE, NS, b"log", b"x" * (MAX_VALUE_BYTES + 5), shard_key=SHARD),
    ):
        with pytest.raises(StorageValueTooLargeError, match="VGI_BLOB_VENDOR_URL"):
            call()
    assert [e for e, _ in do.requests] == ["state_put_many"]


def test_no_vendor_mid_size_values_stay_inline() -> None:
    """Without a vendor, values over the threshold but under 1 MiB are inline."""
    do = FakeCfDo()
    s = _storage(do, None, threshold=DEFAULT_EXTERNALIZE_BYTES)
    value = b"m" * (DEFAULT_EXTERNALIZE_BYTES + 10)
    s.state_put_many(SCOPE, NS, [(b"k", value)], shard_key=SHARD)
    assert _tags(do) == [TAG_INLINE]
    assert s.state_get_many(SCOPE, NS, [b"k"], shard_key=SHARD) == [value]


def test_threshold_validation() -> None:
    """The threshold must be positive and, with a cap, fit the backend (an inline value must fit)."""
    base = FunctionStorageCfDo(url="https://do.example")
    for bad in (0, MAX_VALUE_BYTES + 1):
        with pytest.raises(ValueError, match="threshold"):
            ExternalizingStorage(base, blob_client=None, threshold=bad, max_value_bytes=MAX_VALUE_BYTES)
    assert (
        ExternalizingStorage(base, blob_client=None, threshold=10 * MAX_VALUE_BYTES).threshold == 10 * MAX_VALUE_BYTES
    )


_ENV_NAMES = (
    "VGI_WORKER_SHARED_STORAGE",
    "VGI_BLOB_VENDOR_URL",
    "VGI_BLOB_VENDOR_TOKEN",
    "VGI_CF_DO_TOKEN",
    "VGI_STORAGE_EXTERNALIZE",
    "VGI_STORAGE_EXTERNALIZE_BYTES",
    "VGI_CF_DO_EXTERNALIZE_BYTES",
)


def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


def test_resolve_cf_do_is_always_wrapped(monkeypatch: pytest.MonkeyPatch, vendor: FakeVendorServer) -> None:
    """cloudflare-do: wrapped with or without the vendor; token falls back to VGI_CF_DO_TOKEN."""
    from vgi.function import _resolve_storage

    _clean_env(monkeypatch)
    monkeypatch.setenv("VGI_WORKER_SHARED_STORAGE", "cloudflare-do")
    monkeypatch.setenv("VGI_CF_DO_URL", "https://do.example")
    plain = _resolve_storage()
    assert isinstance(plain, ExternalizingStorage) and plain.blob_client is None
    assert plain.threshold == DEFAULT_EXTERNALIZE_BYTES
    plain.close()

    monkeypatch.setenv("VGI_BLOB_VENDOR_URL", vendor.url)
    with pytest.raises(ValueError, match="VGI_BLOB_VENDOR_TOKEN"):
        _resolve_storage()
    monkeypatch.setenv("VGI_CF_DO_TOKEN", "test-token")
    monkeypatch.setenv("VGI_CF_DO_EXTERNALIZE_BYTES", "100000")  # the alias
    s = _resolve_storage()
    assert isinstance(s, ExternalizingStorage) and s.blob_client is not None
    assert s.threshold == 100000
    assert s.blob_client.put_bytes(b"hello").size == 5  # the fallback token authenticates
    s.close()
    monkeypatch.setenv("VGI_STORAGE_EXTERNALIZE_BYTES", str(4 * MAX_VALUE_BYTES))  # wins; clamped to the cap
    s = _resolve_storage()
    assert isinstance(s, ExternalizingStorage) and s.threshold == MAX_VALUE_BYTES
    s.close()
    monkeypatch.setenv("VGI_STORAGE_EXTERNALIZE_BYTES", "lots")
    with pytest.raises(ValueError, match="VGI_STORAGE_EXTERNALIZE_BYTES"):
        _resolve_storage()


def test_resolve_sqlite_unwrapped_by_default(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """SQLite keeps its on-disk format unless VGI_STORAGE_EXTERNALIZE=1, even with a vendor URL set."""
    from vgi.function import _resolve_storage
    from vgi.function_storage import FunctionStorageSqlite

    _clean_env(monkeypatch)
    monkeypatch.setenv("VGI_WORKER_SQLITE_PATH", str(tmp_path / "s.db"))
    monkeypatch.setenv("VGI_BLOB_VENDOR_URL", "http://127.0.0.1:9")
    assert isinstance(_resolve_storage(), FunctionStorageSqlite)


def test_resolve_sqlite_externalize_needs_vendor(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """VGI_STORAGE_EXTERNALIZE=1 without VGI_BLOB_VENDOR_URL stops startup."""
    from vgi.function import _resolve_storage

    _clean_env(monkeypatch)
    monkeypatch.setenv("VGI_WORKER_SQLITE_PATH", str(tmp_path / "s.db"))
    monkeypatch.setenv("VGI_STORAGE_EXTERNALIZE", "1")
    with pytest.raises(ValueError, match="VGI_BLOB_VENDOR_URL"):
        _resolve_storage()


# ---------------------------------------------------------------------------
# Through BoundStorage: sealed payloads and fixtures
# ---------------------------------------------------------------------------

_ATTACH = b"\xaa" * 16 + b"catalog"
_SIGNING_KEY = b"k" * 32


def _bound(s: ExternalizingStorage, eid: bytes = SCOPE) -> BoundStorage:
    return BoundStorage(s, eid, attach_plaintext=_ATTACH)


def test_sealed_payload_over_threshold_round_trips_and_stays_authenticated(vendor: FakeVendorServer) -> None:
    """Sealing happens before the client: the blob holds the sealed bytes, AAD still binds the slot."""
    do = FakeCfDo()
    s = _storage(do, vendor)
    bound = _bound(s)
    payload = secrets.token_bytes(THRESHOLD * 3)
    sealed = bound.sealed_put(FrameworkNS.BUFFERING_INIT, b"k", payload, signing_key=_SIGNING_KEY)
    (env,) = do.stored_values()
    assert env[0] == TAG_POINTER
    (object_id,) = _objects(vendor)
    assert vendor.vendor.object_data(object_id) == sealed  # ciphertext, not the payload
    assert payload not in sealed
    assert bound.sealed_get(FrameworkNS.BUFFERING_INIT, b"k", signing_key=_SIGNING_KEY) == payload

    # Copy the pointer into another slot: the blob verifies, the seal does not.
    do.state[(bound._shard_key, SCOPE, bytes(FrameworkNS.BUFFERING_INIT), b"other")] = env
    with pytest.raises(StoragePayloadError):
        bound.sealed_get(FrameworkNS.BUFFERING_INIT, b"other", signing_key=_SIGNING_KEY)


class _Out:
    def __init__(self) -> None:
        self.batches: list[pa.RecordBatch] = []
        self.finished = False

    def emit(self, batch: pa.RecordBatch) -> None:
        self.batches.append(batch)

    def finish(self) -> None:
        self.finished = True


def _run_buffered(
    fn: Any, bound: BoundStorage, batches: list[pa.RecordBatch], output_schema: pa.Schema | None = None
) -> list[pa.RecordBatch]:
    """Drive a buffered fixture's process/combine/finalize the way the worker does."""
    params: Any = SimpleNamespace(
        storage=bound,
        execution_id=bound._execution_id,
        arguments=None,
        output_schema=output_schema if output_schema is not None else batches[0].schema,
    )
    state_ids = [fn.process(b, params) for b in batches]
    out = _Out()
    for fid in fn.combine(state_ids, params):
        state = fn.initial_finalize_state(fid, params)
        while not out.finished:
            fn.finalize(params, fid, state, out)
        out.finished = False
    return out.batches


def test_buffer_input_with_large_values(vendor: FakeVendorServer) -> None:
    """The raw-input buffered fixture round-trips batches far over the DO cap."""
    from vgi._test_fixtures.table_in_out import BufferInputFunction

    do = FakeCfDo()
    s = _storage(do, vendor, threshold=DEFAULT_EXTERNALIZE_BYTES)
    row = "r" * 1_000_000
    batches = [pa.RecordBatch.from_pydict({"s": [row] * 4, "i": list(range(n * 4, n * 4 + 4))}) for n in range(4)]
    out = _run_buffered(BufferInputFunction, _bound(s), batches)
    assert pa.Table.from_batches(out).equals(pa.Table.from_batches(batches))
    assert len(_objects(vendor)) == 4
    assert all(len(v) < 100 for v in do.stored_values())
    _bound(s).execution_clear()
    s.flush_blob_cleanup()
    assert _objects(vendor) == []


class _ProfiledCfDo(FunctionStorageCfDo):
    pass


def _distributed_sum_round_trips(do: FakeCfDo, vendor: FakeVendorServer | None) -> tuple[int, dict[str, int]]:
    from vgi._test_fixtures.table_in_out import SumAllColumnsSimpleDistributed

    prof = sp._StorageProfiler()
    cls = type("_P", (_ProfiledCfDo,), {})
    sp.instrument_class(cls, prof)
    s = _storage(do, vendor, cls=cls)
    rows, per = 20_000, 2048
    batches = [pa.RecordBatch.from_pydict({"a": list(range(i, min(i + per, rows)))}) for i in range(0, rows, per)]
    out = _run_buffered(SumAllColumnsSimpleDistributed, _bound(s), batches, pa.schema([("a", pa.int64())]))
    assert pa.Table.from_batches(out).to_pydict() == {"a": [sum(range(rows))]}
    _bound(s).execution_clear()
    s.flush_blob_cleanup()
    calls = sum(st.n for st in prof._shard_ops.values())
    return calls, do.endpoint_counts()


def test_distributed_sum_small_values_make_no_extra_calls(vendor: FakeVendorServer) -> None:
    """Small values: identical DO round trips with or without the vendor, and no vendor data calls."""
    without, counts_without = _distributed_sum_round_trips(FakeCfDo(), None)
    with_vendor, counts_with = _distributed_sum_round_trips(FakeCfDo(), vendor)
    assert with_vendor == without
    assert counts_with == counts_without
    # The only vendor request is execution_clear's background scope delete.
    assert vendor.vendor.stats() == {"object_count": 0, "/v1/blobs/delete_scope": 1}


@pytest.mark.skipif(not os.environ.get("VGI_HUGE_BLOB_TEST"), reason="moves ~2 GB; set VGI_HUGE_BLOB_TEST=1")
def test_2048_rows_of_1mb_strings(vendor: FakeVendorServer) -> None:
    """DuckDB's vector size of 1 MB strings: one ~2 GB value through the buffered fixture.

    Uses a real DO when ``VGI_CF_DO_INTEGRATION_URL`` is set (``wrangler dev``).
    """
    from vgi._test_fixtures.table_in_out import BufferInputFunction

    url = os.environ.get("VGI_CF_DO_INTEGRATION_URL")
    if url:
        s = ExternalizingStorage(
            FunctionStorageCfDo(url=url, token=os.environ.get("VGI_CF_DO_TOKEN")),
            blob_client=_client(vendor),
            max_value_bytes=MAX_VALUE_BYTES,
        )
    else:
        s = _storage(FakeCfDo(), vendor, threshold=DEFAULT_EXTERNALIZE_BYTES)
    row = "x" * 1_000_000
    batch = pa.RecordBatch.from_pydict({"s": pa.array([row] * 2048, pa.string())})
    bound = BoundStorage(s, secrets.token_bytes(16), attach_plaintext=secrets.token_bytes(16) + b"c")
    out = _run_buffered(BufferInputFunction, bound, [batch])
    assert sum(b.num_rows for b in out) == 2048
    assert out[0].column(0)[2047].as_py() == row
    bound.execution_clear()
    s.flush_blob_cleanup()
    assert _objects(vendor) == []
    s.close()


# ---------------------------------------------------------------------------
# Over SQLite (VGI_STORAGE_EXTERNALIZE=1)
# ---------------------------------------------------------------------------


def _sqlite(tmp_path: Any, vendor: FakeVendorServer) -> ExternalizingStorage:
    from vgi.function_storage import FunctionStorageSqlite

    return ExternalizingStorage(
        FunctionStorageSqlite(db_path=str(tmp_path / "s.db")), blob_client=_client(vendor), threshold=THRESHOLD
    )


def test_sqlite_round_trip_every_method(tmp_path: Any, vendor: FakeVendorServer) -> None:
    """Queue, state and log values over SQLite, mixed inline and external."""
    s = _sqlite(tmp_path, vendor)
    assert not s.requires_shard_key
    values = _mixed(6)
    s.queue_push(SCOPE, values[:3])
    assert [s.queue_pop(SCOPE) for _ in range(3)] == values[:3]
    s.state_put_many(SCOPE, NS, [(bytes([i]), v) for i, v in enumerate(values)])
    assert s.state_get_many(SCOPE, NS, [bytes([i]) for i in range(6)] + [b"x"]) == [*values, None]
    assert list(s.state_scan(SCOPE, NS, reverse=True, limit=2)) == [(b"\x05", values[5]), (b"\x04", values[4])]
    ids = [s.state_append(SCOPE, b"log", b"", v) for v in values]
    assert s.state_log_scan(SCOPE, b"log", b"") == list(zip(ids, values, strict=True))
    assert list(s.state_drain(SCOPE, NS)) == [(bytes([i]), v) for i, v in enumerate(values)]
    s.execution_clear(SCOPE)
    s.flush_blob_cleanup()
    assert _objects(vendor) == []
    s.close()


def test_sqlite_stores_envelopes(tmp_path: Any, vendor: FakeVendorServer) -> None:
    """The SQLite rows hold envelopes: tag byte, and only a pointer for large values."""
    s = _sqlite(tmp_path, vendor)
    s.state_put_many(SCOPE, NS, [(b"a", b"tiny"), (b"b", b"B" * THRESHOLD)])
    raw = dict(s.base.state_scan(SCOPE, NS))
    assert raw[b"a"] == b"\x00tiny"
    assert raw[b"b"][0] == TAG_POINTER and len(raw[b"b"]) < 100
    s.close()


def test_sqlite_drain_is_eager(tmp_path: Any, vendor: FakeVendorServer) -> None:
    """An unconsumed state_drain still drains an eager backend, as without the wrapper."""
    s = _sqlite(tmp_path, vendor)
    s.state_put_many(SCOPE, NS, [(b"a", b"x")])
    s.state_drain(SCOPE, NS)
    assert s.state_get_many(SCOPE, NS, [b"a"]) == [None]
    s.close()


def test_sqlite_sealed_payload_and_buffered_fixture(tmp_path: Any, vendor: FakeVendorServer) -> None:
    """Sealed framework payloads and the raw-input buffered fixture over wrapped SQLite."""
    from vgi._test_fixtures.table_in_out import BufferInputFunction

    s = _sqlite(tmp_path, vendor)
    bound = BoundStorage(s, SCOPE)
    payload = secrets.token_bytes(THRESHOLD * 2)
    bound.sealed_put(FrameworkNS.BUFFERING_INIT, b"k", payload, signing_key=_SIGNING_KEY)
    assert bound.sealed_get(FrameworkNS.BUFFERING_INIT, b"k", signing_key=_SIGNING_KEY) == payload
    batches = [pa.RecordBatch.from_pydict({"s": ["y" * 10_000] * 3}) for _ in range(3)]
    out = _run_buffered(BufferInputFunction, bound, batches)
    assert pa.Table.from_batches(out).equals(pa.Table.from_batches(batches))
    s.close()
