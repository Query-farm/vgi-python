# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Authenticated encryption of framework payloads written to FunctionStorage.

The framework persists some of its own context to shared storage so that any
worker process can serve any RPC of an execution: the buffered-table init
(the whole ``InitRequest``, including ``bind_call.secrets``, the resolved
secret values, and the sealed attach and transaction blobs), aggregate
constant arguments, and the streaming-aggregate session. Storage may be a
SQLite file, Azure SQL or a Cloudflare Durable Object reached over the
network, so none of that may sit there in plaintext, and nothing read back
may be trusted unauthenticated.

Sealed framework payload v1 (the same format in every VGI SDK):

* ``K = HKDF-SHA256(ikm, salt=b"", info=b"vgi.storage.payload.v1", L=32)``.
  ``ikm`` is the worker's signing key exactly as it resolved it (the UTF-8
  bytes of ``VGI_SIGNING_KEY``, or of the key ``vgi-serve`` minted and
  exported to its children, so a pre-fork pool agrees). A worker with no
  signing key (subprocess and unix transports) uses the host-local key file
  shared by every VGI worker process of the user on the host, the one secret
  ATTACH options use (`vgi.attach_secret_options.host_local_key_path`):
  one query's RPCs are spread over several worker processes that share one
  SQLite file, so a per-process key would not do.
* AES-256-GCM, random 12-byte nonce, 16-byte tag.
* ``AAD = "vgi.storage.payload.v1" || u32be(len) || shard_key ||
  u32be(len) || scope_id || u32be(len) || namespace || u32be(len) || key``:
  a payload copied to another execution, slot or attach shard fails to open.
* Wire: ``0x01 || nonce(12) || ciphertext || tag(16)``.

A payload that fails to open (unknown version, wrong length, tampered, wrong
key, moved, or an unsealed value written by an older release) raises
`StoragePayloadError`, an ``OSError`` like the "unknown execution_id" error
the same reads raise when the payload is missing. There is no plaintext
fallback. Executions are short-lived and their ids are never reused, so no
migration of previously stored payloads is needed.

Data user code writes to storage (``process()`` appends, aggregate group
states, window partitions, finalize output, table-in-out state, work queue
items) is the function's own data and is not sealed.
"""

from __future__ import annotations

import functools
import os

from vgi import attach_secret_options as _aso

__all__ = ["INFO", "StoragePayloadError", "open_payload", "seal_payload"]

INFO = b"vgi.storage.payload.v1"
VERSION = 0x01
_KEY_LEN = 32
_NONCE_LEN = 12
_TAG_LEN = 16


class StoragePayloadError(OSError):
    """A framework payload read from storage could not be authenticated."""


@functools.lru_cache(maxsize=16)
def derive_key(ikm: bytes) -> bytes:
    """``HKDF-SHA256(ikm, salt="", info="vgi.storage.payload.v1", 32)``."""
    from Crypto.Hash import SHA256
    from Crypto.Protocol.KDF import HKDF

    key = HKDF(ikm, _KEY_LEN, b"", SHA256, context=INFO)
    assert isinstance(key, bytes)
    return key


def _lp(b: bytes) -> bytes:
    return len(b).to_bytes(4, "big") + b


def aad(shard_key: str, scope_id: bytes, ns: bytes, key: bytes) -> bytes:
    """The associated data binding a payload to its storage slot."""
    return INFO + _lp(shard_key.encode()) + _lp(bytes(scope_id)) + _lp(bytes(ns)) + _lp(bytes(key))


def _ikm(signing_key: bytes | None) -> bytes:
    if signing_key is not None:
        return bytes(signing_key)
    return _aso._load_host_local_key()


def seal_with(
    payload: bytes, ikm: bytes, *, shard_key: str, scope_id: bytes, ns: bytes, key: bytes, nonce: bytes | None = None
) -> bytes:
    """Seal under an explicit ``ikm`` (``nonce`` only for test vectors)."""
    from Crypto.Cipher import AES

    nonce = os.urandom(_NONCE_LEN) if nonce is None else nonce
    if len(nonce) != _NONCE_LEN:
        raise ValueError("nonce must be 12 bytes")
    cipher = AES.new(derive_key(ikm), AES.MODE_GCM, nonce=nonce, mac_len=_TAG_LEN)
    cipher.update(aad(shard_key, scope_id, ns, key))
    ciphertext, tag = cipher.encrypt_and_digest(payload)
    return bytes([VERSION]) + nonce + ciphertext + tag


def open_with(blob: bytes, ikm: bytes, *, shard_key: str, scope_id: bytes, ns: bytes, key: bytes) -> bytes:
    """Open under an explicit ``ikm`` (`StoragePayloadError` on any failure)."""
    from Crypto.Cipher import AES

    what = f"framework storage payload {bytes(ns)!r} for execution {bytes(scope_id).hex()}"
    if len(blob) < 1 + _NONCE_LEN + _TAG_LEN:
        raise StoragePayloadError(f"{what} is too short to be a sealed payload")
    if blob[0] != VERSION:
        raise StoragePayloadError(f"{what} has unknown seal version {blob[0]:#04x}")
    nonce, body = blob[1 : 1 + _NONCE_LEN], blob[1 + _NONCE_LEN :]
    cipher = AES.new(derive_key(ikm), AES.MODE_GCM, nonce=nonce, mac_len=_TAG_LEN)
    cipher.update(aad(shard_key, scope_id, ns, key))
    try:
        return bytes(cipher.decrypt_and_verify(body[:-_TAG_LEN], body[-_TAG_LEN:]))
    except ValueError as exc:
        msg = f"{what} failed authentication (tampered, moved, or sealed under another key)"
        raise StoragePayloadError(msg) from exc


def seal_payload(
    payload: bytes,
    *,
    signing_key: bytes | None,
    shard_key: str,
    scope_id: bytes,
    ns: bytes,
    key: bytes,
) -> bytes:
    """Seal a framework payload bound to its storage slot.

    Args:
        payload: The plaintext bytes.
        signing_key: The worker's signing key, or None (subprocess / unix).
        shard_key: The attach shard the slot lives in.
        scope_id: The execution (or transaction) id.
        ns: The storage namespace.
        key: The key within the namespace.

    Returns:
        The envelope to store.
    """
    return seal_with(payload, _ikm(signing_key), shard_key=shard_key, scope_id=scope_id, ns=ns, key=key)


def open_payload(
    blob: bytes,
    *,
    signing_key: bytes | None,
    shard_key: str,
    scope_id: bytes,
    ns: bytes,
    key: bytes,
) -> bytes:
    """Open an envelope from `seal_payload` for the same slot.

    Args:
        blob: The stored envelope.
        signing_key: The worker's signing key, or None.
        shard_key: The attach shard the slot lives in.
        scope_id: The execution (or transaction) id.
        ns: The storage namespace.
        key: The key within the namespace.

    Returns:
        The plaintext payload.

    Raises:
        StoragePayloadError: On any failure to authenticate, or when the
            host-local key (the no-signing-key case) is unusable.
    """
    try:
        ikm = _ikm(signing_key)
    except _aso.AttachSecretKeyError as exc:
        raise StoragePayloadError(f"no key to open framework storage payloads: {exc}") from exc
    return open_with(blob, ikm, shard_key=shard_key, scope_id=scope_id, ns=ns, key=key)
