# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Framework payloads are sealed before they reach FunctionStorage."""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

import vgi.worker as worker_mod
from vgi import attach_secret_options
from vgi._storage_seal import StoragePayloadError, open_payload, seal_payload
from vgi.function_storage import BoundStorage, FrameworkNS, FunctionStorageSqlite

_SIGNING_KEY = b"k" * 43
_OTHER_KEY = b"z" * 43
_SLOT: dict[str, Any] = {"shard_key": "att-0123", "scope_id": b"\x01" * 16, "ns": b"_vgi/buffering_init", "key": b"k"}


@pytest.fixture(autouse=True)
def _host_local_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the host-local key at a private temp file and forget cached roots."""
    monkeypatch.setenv("VGI_ATTACH_SECRET_KEY_FILE", str(tmp_path / "host.key"))
    monkeypatch.setattr(attach_secret_options, "_host_local_cache", {})
    monkeypatch.setattr(attach_secret_options, "_explicit_root", None)
    monkeypatch.setattr(attach_secret_options, "_signing_root", None)


@pytest.mark.parametrize("signing_key", [_SIGNING_KEY, None], ids=["signing-key", "host-local"])
def test_round_trip(signing_key: bytes | None) -> None:
    """A payload opens for the same slot and key."""
    sealed = seal_payload(b"secret context", signing_key=signing_key, **_SLOT)
    assert b"secret context" not in sealed
    assert sealed[0] == 0x01
    assert len(sealed) == 1 + 12 + len(b"secret context") + 16
    assert open_payload(sealed, signing_key=signing_key, **_SLOT) == b"secret context"


def test_tampered_refused() -> None:
    """Flipping any byte fails authentication."""
    sealed = bytearray(seal_payload(b"payload", signing_key=_SIGNING_KEY, **_SLOT))
    sealed[-1] ^= 1
    with pytest.raises(StoragePayloadError, match="failed authentication"):
        open_payload(bytes(sealed), signing_key=_SIGNING_KEY, **_SLOT)


@pytest.mark.parametrize(
    "field,value",
    [("scope_id", b"\x02" * 16), ("key", b"other"), ("ns", b"_vgi/aggregate_state"), ("shard_key", "att-9999")],
)
def test_moved_payload_refused(field: str, value: Any) -> None:
    """The AAD binds the slot: another execution, key, namespace or shard fails."""
    sealed = seal_payload(b"payload", signing_key=_SIGNING_KEY, **_SLOT)
    with pytest.raises(StoragePayloadError):
        open_payload(sealed, signing_key=_SIGNING_KEY, **{**_SLOT, field: value})


def test_wrong_key_refused() -> None:
    """A payload sealed under another signing key fails."""
    sealed = seal_payload(b"payload", signing_key=_SIGNING_KEY, **_SLOT)
    with pytest.raises(StoragePayloadError):
        open_payload(sealed, signing_key=_OTHER_KEY, **_SLOT)


def test_unknown_version_and_short_refused() -> None:
    """An unknown version byte or a truncated envelope is refused, never treated as plaintext."""
    sealed = seal_payload(b"payload", signing_key=_SIGNING_KEY, **_SLOT)
    with pytest.raises(StoragePayloadError, match="unknown seal version"):
        open_payload(b"\x02" + sealed[1:], signing_key=_SIGNING_KEY, **_SLOT)
    with pytest.raises(StoragePayloadError, match="too short"):
        open_payload(sealed[:20], signing_key=_SIGNING_KEY, **_SLOT)


def test_host_local_and_signing_keys_do_not_cross() -> None:
    """A payload sealed with the host-local key does not open under a signing key."""
    sealed = seal_payload(b"payload", signing_key=None, **_SLOT)
    with pytest.raises(StoragePayloadError):
        open_payload(sealed, signing_key=_SIGNING_KEY, **_SLOT)


# Cross-SDK test vector for sealed framework payload v1 (Go/Java/TS/Rust check
# the same bytes). AES-256-GCM is deterministic for a fixed key and nonce.
VECTOR: dict[str, Any] = {
    "ikm": b"vgi-test-signing-key-0123456789abcdef",
    "shard_key": "att-00112233445566778899aabbccddeeff",
    "scope_id": bytes(range(16)),
    "ns": b"_vgi/buffering_init",
    "key": BoundStorage.pack_int_key(-1),
    "nonce": bytes(range(12)),
    "plaintext": b"vgi sealed framework payload v1",
}
VECTOR_DERIVED_KEY_HEX = "f5f759ad2009e91cf4b963a20b524a800224d2ac4e01472396dc1a2dab78bea9"
VECTOR_AAD_HEX = (
    "7667692e73746f726167652e7061796c6f61642e7631000000246174742d30303131323233333434353536363737383839396161"
    "6262636364646565666600000010000102030405060708090a0b0c0d0e0f000000135f7667692f627566666572696e675f696e6974"
    "00000008ffffffffffffffff"
)
VECTOR_WIRE_HEX = (
    "01000102030405060708090a0bc79698647483a87eb533cf7332d1c527275cc7470dc6a3bdf1f72f624a97e246416d4de55281bd0eee"
    "6d6491796a40"
)


def test_cross_sdk_vector() -> None:
    """The fixed vector seals to the published bytes and opens back."""
    from vgi._storage_seal import aad, derive_key, open_with, seal_with

    v = VECTOR
    slot = {"shard_key": v["shard_key"], "scope_id": v["scope_id"], "ns": v["ns"], "key": v["key"]}
    assert derive_key(v["ikm"]).hex() == VECTOR_DERIVED_KEY_HEX
    assert aad(**slot).hex() == VECTOR_AAD_HEX
    wire = seal_with(v["plaintext"], v["ikm"], nonce=v["nonce"], **slot)
    assert wire.hex() == VECTOR_WIRE_HEX
    assert open_with(bytes.fromhex(VECTOR_WIRE_HEX), v["ikm"], **slot) == v["plaintext"]


def test_unsealed_legacy_payload_refused() -> None:
    """An old pickled (unsealed) payload fails cleanly, never reaching pickle."""
    legacy = pickle.dumps({"init_call_bytes": b"x", "init_response_bytes": b"y"})
    with pytest.raises(StoragePayloadError):
        open_payload(legacy, signing_key=_SIGNING_KEY, **_SLOT)
    with pytest.raises(StoragePayloadError):
        open_payload(b"", signing_key=_SIGNING_KEY, **_SLOT)


def _init_payload(secret_value: str) -> tuple[Any, Any]:
    """An InitRequest whose bind_call carries a resolved secret."""
    from vgi.arguments import Arguments
    from vgi.invocation import FunctionType, GlobalInitResponse
    from vgi.protocol import BindRequest, InitRequest

    secrets = pa.RecordBatch.from_pydict({"api_token": [secret_value]})
    bind = BindRequest(function_name="f", arguments=Arguments(), function_type=FunctionType.TABLE, secrets=secrets)
    req = InitRequest(bind_call=bind, output_schema=pa.schema([("a", pa.int64())]))
    return req, GlobalInitResponse(execution_id=b"\x03" * 16)


def test_init_framing_has_no_pickle(monkeypatch: pytest.MonkeyPatch) -> None:
    """The buffering-init payload round-trips without pickle."""

    def no_pickle(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("pickle used on the storage path")

    monkeypatch.setattr(pickle, "loads", no_pickle)
    monkeypatch.setattr(pickle, "dumps", no_pickle)
    req, resp = _init_payload("dummy")
    call, response = worker_mod._decode_table_buffering_init(worker_mod._encode_table_buffering_init(req, resp))
    assert call.bind_call.function_name == "f"
    assert response.execution_id == b"\x03" * 16
    with pytest.raises(StoragePayloadError):
        worker_mod._decode_table_buffering_init(b"\x80\x05junk")


def test_secret_never_plaintext_in_sqlite(tmp_path: Path) -> None:
    """A sealed init with a dummy secret leaves no plaintext in the SQLite file."""
    marker = "DUMMY-SECRET-7f3c9e1d"
    db = tmp_path / "storage.db"
    backend = FunctionStorageSqlite(db_path=str(db))
    storage = BoundStorage(backend, b"\x04" * 16)
    req, resp = _init_payload(marker)
    plain = worker_mod._encode_table_buffering_init(req, resp)
    assert marker.encode() in plain  # the secret really is in the payload
    key = BoundStorage.pack_int_key(-1)
    storage.sealed_put(FrameworkNS.BUFFERING_INIT, key, plain, signing_key=_SIGNING_KEY)
    assert storage.sealed_get(FrameworkNS.BUFFERING_INIT, key, signing_key=_SIGNING_KEY) == plain
    backend.close()
    blob = b"".join(p.read_bytes() for p in tmp_path.glob("storage.db*"))
    assert blob  # the file (and any WAL) was written
    assert marker.encode() not in blob


def test_bound_storage_refuses_payload_from_other_execution(tmp_path: Path) -> None:
    """A sealed value copied between executions in the same backend fails to open."""
    backend = FunctionStorageSqlite(db_path=str(tmp_path / "s.db"))
    a = BoundStorage(backend, b"\x05" * 16)
    b = BoundStorage(backend, b"\x06" * 16)
    key = BoundStorage.pack_int_key(-2)
    sealed = a.sealed_put(FrameworkNS.AGGREGATE_STATE, key, b"const args", signing_key=_SIGNING_KEY)
    b.state_put(FrameworkNS.AGGREGATE_STATE, key, sealed)
    with pytest.raises(StoragePayloadError):
        b.sealed_get(FrameworkNS.AGGREGATE_STATE, key, signing_key=_SIGNING_KEY)
    # An unsealed value written directly is refused too, not passed through.
    b.state_put(FrameworkNS.AGGREGATE_STATE, key, b"plain")
    with pytest.raises(StoragePayloadError):
        b.sealed_get(FrameworkNS.AGGREGATE_STATE, key, signing_key=_SIGNING_KEY)
