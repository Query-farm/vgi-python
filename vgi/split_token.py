# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Split-token envelope: the framework's wrapper around a worker's split payload.

A split token *names* a unit of scan work so a distributed engine can re-request
exactly the work it was handed. The worker supplies only ``payload``; everything
around it is stamped here, so an author cannot forget the consistency anchor or
mis-bind the fingerprint, and never writes crypto.

Layout (little-endian, fixed prefix):

    offset  size  field
    0       1     format_version      currently 1
    1       1     flags               bit0 = payload_sealed; bits 1-7 reserved, MUST be 0
    2       2     anchor_len          u16 LE
    4       16    bind_fingerprint    truncated SHA-256 of the bind identity
    20      var   consistency_anchor  anchor_len bytes
    20+n    var   payload             the worker's own bytes

**The header is plaintext on every transport; only ``payload`` is sealed.** That is
not a preference — ``Worker._signing_key`` is ``None`` on subprocess and unix, which
is DuckDB's primary path, so a header readable only through AEAD would be unreadable
exactly where DuckDB runs. It also matters for streaming: a checkpointed position
must survive key rotation, and a header verifiable *only* via AEAD would not.

.. warning::

   ``flags`` is attacker-controlled plaintext. A parser that reads bit 0 to decide
   whether to unseal is ``alg:none``: clear the bit, append a plaintext payload
   naming any work you like, and a fully-keyed worker would redeem it without ever
   calling ``open_bytes``. The keyed/keyless decision therefore comes from **the
   worker's own key state**, never from the token — see :func:`open_split_token`,
   which refuses an unsealed token whenever a key exists.

Sealing binds the caller principal, not just the header, mirroring the
``attach_opaque_data`` envelope this codebase already ships: a value minted for one
principal must not be replayable by another, and a split token names data.
"""

from __future__ import annotations

import hashlib
import struct
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vgi_rpc.rpc import AuthContext

    from vgi.protocol import BindRequest

__all__ = [
    "FORMAT_VERSION",
    "FLAG_PAYLOAD_SEALED",
    "SplitTokenError",
    "SplitTokenInvalid",
    "SplitSnapshotExpired",
    "SplitTransactionEnded",
    "bind_fingerprint",
    "build_split_token",
    "open_split_token",
    "split_token_aad",
]

FORMAT_VERSION = 1
"""Envelope format version. Checked unconditionally, before anything else."""

FLAG_PAYLOAD_SEALED = 0x01
"""bit0 of ``flags``: the payload is an AEAD-sealed blob rather than plaintext."""

_RESERVED_FLAGS_MASK = 0xFE
"""bits 1-7 are reserved and MUST be zero; a set bit is a forward-compat violation."""

_HEADER_STRUCT = struct.Struct("<BBH")
"""format_version | flags | anchor_len"""

_FINGERPRINT_LEN = 16
_HEADER_LEN = _HEADER_STRUCT.size + _FINGERPRINT_LEN  # 4 + 16 = 20

_AAD_PREFIX = b"vgi.split_token.v1\x00"


class SplitTokenError(Exception):
    """Base class for split-token failures."""

    error_kind: str = "SPLIT_TOKEN_INVALID"

    def __str__(self) -> str:
        """Prefix the stable kind, so it survives the trip to the client.

        Only the message crosses the wire — the exception class does not — and the
        kind is what a connector switches on to decide whether re-running the
        query could possibly help. Without it in the text, every refusal looks
        alike on the far side and the whole point of having distinct kinds is lost
        at exactly the boundary where it matters.
        """
        return f"[{self.error_kind}] {super().__str__()}"


class SplitTokenInvalid(SplitTokenError):
    """The token is malformed, bound to a different bind, or forged.

    Non-retriable, and distinct from expiry: a connector must not re-run the query
    on this, because re-running would produce the same token.
    """

    error_kind = "SPLIT_TOKEN_INVALID"


class SplitSnapshotExpired(SplitTokenError):
    """The consistency anchor this token names is gone.

    Non-retriable but *distinct* from :class:`SplitTokenInvalid`, so a connector can
    surface "re-run the query" rather than retrying four times into a stage failure.
    Keeping the anchor in the plaintext header rather than in the AAD is what makes
    this distinction expressible at all — inside the AAD both failures collapse into
    one indistinguishable tag-check failure.
    """

    error_kind = "SPLIT_SNAPSHOT_EXPIRED"


class SplitTransactionEnded(SplitTokenError):
    """A transaction-scoped token was redeemed after commit or rollback."""

    error_kind = "SPLIT_TRANSACTION_ENDED"


def bind_fingerprint(bind_call: BindRequest) -> bytes:
    """Derive the 16-byte binding check for a bind call.

    Minted **and** verified by the same worker, so it needs self-consistency only —
    it does not have to agree with any client, and the cross-SDK byte fixtures do not
    cover it. Do not try to reuse the C++ ``VgiResultCacheKey`` canonicalization: it
    is client-side, spans 19 fields, and includes ``identity_scope`` / ``worker_path``
    / ``attach_options`` that a worker cannot compute.

    16 bytes is a binding check, not a MAC — forgery resistance comes from the seal
    where a key exists, and from the uid trust boundary where one does not.
    """
    h = hashlib.sha256()
    h.update(_AAD_PREFIX)

    def _feed(label: bytes, value: object) -> None:
        h.update(label)
        h.update(b"\x00")
        h.update(repr(value).encode("utf-8", "surrogatepass"))
        h.update(b"\x00")

    _feed(b"schema_name", getattr(bind_call, "schema_name", None))
    _feed(b"function_name", getattr(bind_call, "function_name", None))
    _feed(b"arguments", getattr(bind_call, "arguments", None))
    _feed(b"settings", getattr(bind_call, "settings", None))
    _feed(b"projection_ids", getattr(bind_call, "projection_ids", None))
    return h.digest()[:_FINGERPRINT_LEN]


def split_token_aad(header: bytes, auth: AuthContext | None) -> bytes:
    """AAD for a sealed split payload: the plaintext header plus the caller identity.

    The identity half is load-bearing, not incidental — it is what stops a token
    minted for one principal being replayed by another, exactly as the attach
    envelope does.
    """
    from vgi.worker import _identity_tail

    return header + _identity_tail(auth)


def build_split_token(
    *,
    payload: bytes,
    fingerprint: bytes,
    anchor: bytes,
    signing_key: bytes | None = None,
    auth: AuthContext | None = None,
) -> bytes:
    """Stamp (and, when a key exists, seal) a worker payload into a split token."""
    if len(fingerprint) != _FINGERPRINT_LEN:
        msg = f"bind_fingerprint must be {_FINGERPRINT_LEN} bytes, got {len(fingerprint)}"
        raise ValueError(msg)
    if len(anchor) > 0xFFFF:
        msg = f"consistency_anchor too long: {len(anchor)} bytes exceeds u16"
        raise ValueError(msg)

    flags = FLAG_PAYLOAD_SEALED if signing_key is not None else 0
    header = _HEADER_STRUCT.pack(FORMAT_VERSION, flags, len(anchor)) + fingerprint
    body = header + anchor

    if signing_key is None:
        return body + payload

    from vgi_rpc import crypto

    sealed = crypto.seal_bytes(payload, signing_key, aad=split_token_aad(body, auth))
    return body + sealed


def open_split_token(
    token: bytes,
    *,
    signing_key: bytes | None = None,
    auth: AuthContext | None = None,
    expected_fingerprint: bytes | None = None,
    current_anchor: bytes | None = None,
) -> bytes:
    """Verify a split token and return the worker's payload.

    Raises :class:`SplitTokenInvalid` for anything structurally wrong or wrongly
    bound, and :class:`SplitSnapshotExpired` when the anchor no longer matches the
    catalog — a distinction a connector needs, because only one of them means
    "re-run the query".
    """
    if len(token) < _HEADER_LEN:
        msg = f"split token too short: {len(token)} bytes, need at least {_HEADER_LEN}"
        raise SplitTokenInvalid(msg)

    version, flags, anchor_len = _HEADER_STRUCT.unpack_from(token, 0)
    if version != FORMAT_VERSION:
        msg = f"unsupported split-token format_version {version}; this worker speaks {FORMAT_VERSION}"
        raise SplitTokenInvalid(msg)
    if flags & _RESERVED_FLAGS_MASK:
        msg = f"split token sets reserved flag bits (flags=0x{flags:02x})"
        raise SplitTokenInvalid(msg)

    sealed = bool(flags & FLAG_PAYLOAD_SEALED)

    # ---- The alg:none refusal. Load-bearing; do not relax. ----
    # `flags` is attacker-controlled plaintext, so it may say "not sealed" on a token
    # an attacker wrote by hand. A keyed worker that honoured that would redeem forged
    # work without ever calling open_bytes. The key state decides, never the token.
    if signing_key is not None and not sealed:
        msg = (
            "split token is unsealed but this worker holds a signing key; refusing. "
            "An unsealed token cannot be authenticated, so accepting one here would "
            "let any caller forge a split (alg:none)."
        )
        raise SplitTokenInvalid(msg)
    if signing_key is None and sealed:
        msg = "split token is sealed but this worker holds no signing key; cannot open it"
        raise SplitTokenInvalid(msg)

    end_of_anchor = _HEADER_LEN + anchor_len
    if len(token) < end_of_anchor:
        msg = f"split token truncated: anchor_len={anchor_len} exceeds token length {len(token)}"
        raise SplitTokenInvalid(msg)

    fingerprint = token[_HEADER_STRUCT.size : _HEADER_STRUCT.size + _FINGERPRINT_LEN]
    anchor = token[_HEADER_LEN:end_of_anchor]
    body = token[:end_of_anchor]
    rest = token[end_of_anchor:]

    if expected_fingerprint is not None and fingerprint != expected_fingerprint:
        msg = "split token was minted for a different bind (fingerprint mismatch)"
        raise SplitTokenInvalid(msg)

    # Anchor check AFTER the bind check, and as its own error kind: "read version N"
    # is a different situation from "this token is not yours".
    if current_anchor is not None and anchor != current_anchor:
        msg = "split snapshot expired; re-run the query"
        raise SplitSnapshotExpired(msg)

    if not sealed:
        return rest

    from vgi_rpc import crypto

    # Narrowing for the type checker: the sealed/keyless combinations are already
    # rejected above, so a sealed token here implies a key.
    assert signing_key is not None

    try:
        return crypto.open_bytes(rest, signing_key, aad=split_token_aad(body, auth))
    except Exception as exc:  # noqa: BLE001 - normalized to the typed error below
        msg = f"split token failed authentication: {exc}"
        raise SplitTokenInvalid(msg) from exc
