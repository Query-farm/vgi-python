# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Attach tickets: a user's ATTACH, sealed so a runner can replay it later as that user.

A ticket is the *what* half of an unattended session; a sealed grant
(``vgi_rpc.Identity.v1`` ``issue_grant``) is the *who*. While the user is
attached and logged in, the client asks the worker to seal the options it
attached with -- secret ones included -- into a ticket only this worker can
open. Later any runner holding the user's grant attaches with the single option
``vgi_attach_ticket`` and the worker restores the sealed attach before the
catalog's own ``catalog_attach`` runs. The runner never sees an option.

A ticket carries no authority. It is opened under the *caller's* principal, so
without a grant for the same principal it attaches nothing.

Normative spec: ``docs/protocol/vgi-attach-tickets.md``. Byte-exact vectors:
``vgi/_test_fixtures/attach_ticket_vectors.json``.

Token::

    "vgia1." || base64url_nopad( envelope )

    envelope = version(1)=0x01 || nonce(24) || XChaCha20-Poly1305(payload, aad)
    key      = VGI_SIGNING_KEY (the attach_opaque_data key), normalized as vgi_rpc.crypto does
    aad      = "vgi.attach_ticket.v1\x00" || UTF-8(principal)

    payload (little-endian) =
        issued_at               int64   seconds since the Unix epoch
        expires_at              int64   0 = no expiry
        ticket_id               u16 len || UTF-8   (32 lowercase hex)
        catalog_name            u16 len || UTF-8   (non-empty)
        data_version_spec       u16 len || UTF-8   ("" = none)
        implementation_version  u16 len || UTF-8   ("" = none)
        options                 u32 len || Arrow IPC stream of the one-row options record
                                           (0 bytes = no options; at most 16 KiB)

The AAD binds the **principal only**, not the ``(domain, principal)`` pair the
attach envelope binds: a ticket is sealed while the user is logged in (domain
``jwt``, say) and opened when a runner presents their grant (domain ``grant``).
"""

from __future__ import annotations

import base64
import dataclasses
import math
import os
import re
import secrets
import struct
import sys
import time
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Protocol

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType
from vgi_rpc.errors import (
    BadRequest,
    Code,
    ErrorInfo,
    FieldViolation,
    PreconditionFailure,
    PreconditionViolation,
    StatusError,
)

from vgi.catalog.attach_option import RESERVED_ATTACH_OPTION

if TYPE_CHECKING:
    from vgi_rpc.rpc import AuthContext, CallContext

    from vgi.catalog.attach_option import AttachOptionSpec
    from vgi.protocol import CatalogAttachRequest

__all__ = [
    "ATTACH_TICKETS_PROTOCOL_NAME",
    "ATTACH_TICKET_OPTION",
    "ATTACH_TICKET_PREFIX",
    "AttachTicket",
    "AttachTicketClaims",
    "AttachTicketExpiredError",
    "AttachTicketInvalidError",
    "AttachTickets",
    "AttachTicketsImpl",
    "SealAttachRequest",
    "attach_ticket_aad",
    "mint_attach_ticket",
    "open_attach_ticket",
    "redeem_attach_ticket",
    "resolve_ticket_max_ttl",
]

#: Token prefix. The version is in the prefix, so an incompatible format is a
#: different prefix -- never half-parsed.
ATTACH_TICKET_PREFIX = "vgia1."

#: The reserved ATTACH option a runner presents a ticket in. No catalog may
#: declare an attach option with this name (case-insensitive).
ATTACH_TICKET_OPTION = RESERVED_ATTACH_OPTION

#: Wire name of the protocol hosting ``seal_attach``.
ATTACH_TICKETS_PROTOCOL_NAME = "vgi.attach_tickets.v1"

#: The envelope's version byte. Fixed by this format, independent of the
#: ``attach_opaque_data`` envelope's own version.
TICKET_ENVELOPE_VERSION = 0x01

#: AAD domain. Distinct from ``vgi.attach_opaque_data.v1``, so an attach
#: envelope can never be opened as a ticket, nor the reverse.
TICKET_AAD_DOMAIN = b"vgi.attach_ticket.v1\x00"

#: Largest options record (serialized Arrow IPC bytes) a ticket may carry.
MAX_OPTIONS_BYTES = 16 * 1024

#: Longest ticket text considered at all, so a verifier is never handed megabytes.
MAX_TICKET_CHARS = 32 * 1024

#: Allowance for clocks disagreeing between the sealing and redeeming worker.
CLOCK_SKEW_SECONDS = 60

_TICKET_ID = re.compile(r"[0-9a-f]{32}")
_B64URL = re.compile(r"[A-Za-z0-9_-]+")
_MAX_TEXT = 0xFFFF


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class AttachTicketInvalidError(ValueError):
    """A ``vgi_attach_ticket`` this worker cannot accept.

    One type for every cause -- malformed, wrong prefix, non-canonical, wrong
    key, wrong principal, tampered, bad payload -- so a caller cannot tell a
    forged ticket from another user's. ``INVALID_ARGUMENT`` / kind
    ``attach_ticket_invalid``. The message never contains the ticket.
    """

    error_code: ClassVar[Code] = Code.INVALID_ARGUMENT
    error_kind: ClassVar[str] = "attach_ticket_invalid"

    def __init__(self, detail: str = "attach ticket not accepted") -> None:
        """Build the error; *detail* is operator-facing and never the ticket."""
        super().__init__(detail)
        self.error_details = [
            BadRequest(field_violations=(FieldViolation(field=ATTACH_TICKET_OPTION, description=detail),))
        ]


class AttachTicketExpiredError(Exception):
    """An authentic ticket outside its lifetime.

    ``FAILED_PRECONDITION`` / kind ``attach_ticket_expired``: the remedy is a
    fresh export from a logged-in session, not a retry. Only raised once the
    ticket has opened under the caller's principal, so it reveals nothing a
    forger could use.
    """

    error_code: ClassVar[Code] = Code.FAILED_PRECONDITION
    error_kind: ClassVar[str] = "attach_ticket_expired"

    def __init__(self, detail: str = "attach ticket has expired") -> None:
        """Build the error."""
        super().__init__(detail)
        self.error_details = [
            PreconditionFailure(
                violations=(
                    PreconditionViolation(type="ATTACH_TICKET", subject=ATTACH_TICKET_OPTION, description=detail),
                )
            )
        ]


def _invalid_request(message: str, violations: Sequence[tuple[str, str]]) -> StatusError:
    """``invalid_request`` / ``INVALID_ARGUMENT`` with a ``BadRequest`` detail."""
    return StatusError(
        message,
        code=Code.INVALID_ARGUMENT,
        kind="invalid_request",
        details=[BadRequest(field_violations=tuple(FieldViolation(field=f, description=d) for f, d in violations))],
    )


def _action_denied(message: str) -> StatusError:
    """``action_denied`` / ``PERMISSION_DENIED`` naming the refused action."""
    return StatusError(
        message,
        code=Code.PERMISSION_DENIED,
        kind="action_denied",
        details=[ErrorInfo(metadata={"action": "seal_attach"})],
    )


# ---------------------------------------------------------------------------
# Token format
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class AttachTicketClaims:
    """What a ticket carries.

    Attributes:
        issued_at: Seconds since the Unix epoch.
        expires_at: Seconds since the Unix epoch; ``0`` means no expiry.
        ticket_id: 32 lowercase hex; a correlation handle, not a secret.
        catalog_name: The catalog the user attached.
        data_version_spec: ``""`` when the user gave none.
        implementation_version: ``""`` when the user gave none.
        options_ipc: Arrow IPC stream of the one-row options record, exactly
            as ``CatalogAttachRequest.options`` carries it; ``b""`` for none.

    """

    issued_at: int
    expires_at: int
    ticket_id: str
    catalog_name: str
    data_version_spec: str
    implementation_version: str
    options_ipc: bytes


def attach_ticket_aad(principal: str) -> bytes:
    r"""Return the ticket AAD: ``"vgi.attach_ticket.v1\x00" || UTF-8(principal)``."""
    return TICKET_AAD_DOMAIN + principal.encode("utf-8")


def _pack_text(value: str, field: str) -> bytes:
    raw = value.encode("utf-8")
    if len(raw) > _MAX_TEXT:
        raise ValueError(f"{field} is longer than 65535 bytes")
    return struct.pack("<H", len(raw)) + raw


def _encode_payload(claims: AttachTicketClaims) -> bytes:
    if len(claims.options_ipc) > MAX_OPTIONS_BYTES:
        raise ValueError(f"options are {len(claims.options_ipc)} bytes; a ticket carries at most {MAX_OPTIONS_BYTES}")
    return b"".join(
        (
            struct.pack("<qq", claims.issued_at, claims.expires_at),
            _pack_text(claims.ticket_id, "ticket_id"),
            _pack_text(claims.catalog_name, "catalog_name"),
            _pack_text(claims.data_version_spec, "data_version_spec"),
            _pack_text(claims.implementation_version, "implementation_version"),
            struct.pack("<I", len(claims.options_ipc)),
            claims.options_ipc,
        )
    )


def _decode_payload(payload: bytes) -> AttachTicketClaims:
    """Parse strictly: exact lengths, valid UTF-8, field rules, no trailing bytes.

    Args:
        payload: The opened plaintext.

    Returns:
        The claims it carries.

    Raises:
        AttachTicketInvalidError: For anything else.

    """
    pos = 0

    def take(n: int) -> bytes:
        nonlocal pos
        if pos + n > len(payload):
            raise AttachTicketInvalidError("attach ticket payload is truncated")
        chunk = payload[pos : pos + n]
        pos += n
        return chunk

    def text() -> str:
        (length,) = struct.unpack("<H", take(2))
        try:
            return take(length).decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise AttachTicketInvalidError("attach ticket payload is not UTF-8") from exc

    issued_at, expires_at = struct.unpack("<qq", take(16))
    ticket_id = text()
    catalog_name = text()
    data_version_spec = text()
    implementation_version = text()
    (options_len,) = struct.unpack("<I", take(4))
    if options_len > MAX_OPTIONS_BYTES:
        raise AttachTicketInvalidError("attach ticket options exceed 16 KiB")
    options_ipc = take(options_len)
    if pos != len(payload):
        raise AttachTicketInvalidError("attach ticket payload has trailing bytes")
    if not _TICKET_ID.fullmatch(ticket_id):
        raise AttachTicketInvalidError("attach ticket id is not 32 lowercase hex")
    if not catalog_name:
        raise AttachTicketInvalidError("attach ticket names no catalog")
    if expires_at != 0 and expires_at <= issued_at:
        raise AttachTicketInvalidError("attach ticket lifetime is empty")
    return AttachTicketClaims(
        issued_at=issued_at,
        expires_at=expires_at,
        ticket_id=ticket_id,
        catalog_name=catalog_name,
        data_version_spec=data_version_spec,
        implementation_version=implementation_version,
        options_ipc=bytes(options_ipc),
    )


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_strict(text: str) -> bytes:
    """Decode unpadded base64url, rejecting any spelling but the canonical one."""
    if not _B64URL.fullmatch(text) or len(text) % 4 == 1:
        raise AttachTicketInvalidError("attach ticket is not unpadded base64url")
    raw = base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
    if _b64url(raw) != text:
        raise AttachTicketInvalidError("attach ticket is not canonical base64url")
    return raw


def mint_attach_ticket(
    signing_key: bytes,
    *,
    principal: str,
    catalog_name: str,
    options_ipc: bytes,
    data_version_spec: str = "",
    implementation_version: str = "",
    issued_at: int,
    expires_at: int,
    ticket_id: str | None = None,
    nonce: bytes | None = None,
) -> tuple[str, AttachTicketClaims]:
    """Seal a ticket for *principal*.

    Args:
        signing_key: ``VGI_SIGNING_KEY`` bytes (any length; normalized by
            ``vgi_rpc.crypto`` exactly as the attach envelope's key is).
        principal: The caller the ticket is for. Must be non-empty.
        catalog_name: The catalog attached. Must be non-empty.
        options_ipc: The options record's Arrow IPC bytes, or ``b""``.
        data_version_spec: ``""`` for none.
        implementation_version: ``""`` for none.
        issued_at: Seconds since the Unix epoch.
        expires_at: Seconds since the Unix epoch, or ``0`` for no expiry.
        ticket_id: Override the random id, **for vectors only**.
        nonce: A fixed 24-byte nonce, **for vectors only**.

    Returns:
        The ticket text and the claims it carries.

    Raises:
        ValueError: An empty principal or catalog, an empty lifetime, or a
            field too long to encode.

    """
    from vgi_rpc.crypto import seal_bytes

    if not principal:
        raise ValueError("a ticket needs a principal")
    claims = AttachTicketClaims(
        issued_at=int(issued_at),
        expires_at=int(expires_at),
        ticket_id=secrets.token_hex(16) if ticket_id is None else ticket_id,
        catalog_name=catalog_name,
        data_version_spec=data_version_spec,
        implementation_version=implementation_version,
        options_ipc=bytes(options_ipc),
    )
    if not catalog_name:
        raise ValueError("a ticket needs a catalog name")
    if not _TICKET_ID.fullmatch(claims.ticket_id):
        raise ValueError("ticket_id must be 32 lowercase hex")
    if claims.expires_at != 0 and claims.expires_at <= claims.issued_at:
        raise ValueError("expires_at must be 0 or after issued_at")
    envelope = seal_bytes(
        _encode_payload(claims),
        signing_key,
        aad=attach_ticket_aad(principal),
        version=TICKET_ENVELOPE_VERSION,
        nonce=nonce,
    )
    token = ATTACH_TICKET_PREFIX + _b64url(envelope)
    if len(token) > MAX_TICKET_CHARS:
        raise ValueError(f"the ticket would be {len(token)} characters; at most {MAX_TICKET_CHARS} are accepted")
    return token, claims


def open_attach_ticket(
    signing_key: bytes,
    token: str,
    *,
    principal: str | None,
    now: float | None = None,
) -> AttachTicketClaims:
    """Verify a ticket for the calling *principal* and return what it carries.

    Order, normative: prefix, length, canonical base64url, AEAD open under the
    caller's principal, strict payload parse, then lifetime with a 60 s skew.
    The lifetime is inside the ciphertext, so it is trusted only after the tag
    verified.

    Args:
        signing_key: ``VGI_SIGNING_KEY`` bytes.
        token: The ticket text, exactly as presented.
        principal: The caller's authenticated principal; ``None`` or ``""``
            (anonymous) never opens a ticket.
        now: Override the clock (seconds), for tests and vectors.

    Returns:
        The verified claims.

    Raises:
        AttachTicketInvalidError: Any cause but expiry.
        AttachTicketExpiredError: Authentic but outside its lifetime.

    """
    from vgi_rpc.crypto import SealError, open_bytes

    if not token.startswith(ATTACH_TICKET_PREFIX):
        raise AttachTicketInvalidError("not an attach ticket")
    if len(token) > MAX_TICKET_CHARS:
        raise AttachTicketInvalidError("attach ticket is too long")
    envelope = _b64url_strict(token[len(ATTACH_TICKET_PREFIX) :])
    if not principal:
        raise AttachTicketInvalidError("an anonymous caller cannot redeem an attach ticket")
    try:
        payload = open_bytes(envelope, signing_key, aad=attach_ticket_aad(principal), version=TICKET_ENVELOPE_VERSION)
    except SealError as exc:
        raise AttachTicketInvalidError("attach ticket failed verification") from exc
    claims = _decode_payload(payload)
    current = time.time() if now is None else now
    if claims.issued_at > current + CLOCK_SKEW_SECONDS:
        raise AttachTicketExpiredError("attach ticket is not yet valid")
    if claims.expires_at != 0 and current >= claims.expires_at + CLOCK_SKEW_SECONDS:
        raise AttachTicketExpiredError("attach ticket has expired")
    return claims


# ---------------------------------------------------------------------------
# Redemption: what catalog_attach does with ``vgi_attach_ticket``
# ---------------------------------------------------------------------------


def _caller_principal(auth: AuthContext | None) -> str | None:
    if auth is None or not auth.authenticated:
        return None
    return auth.principal or None


def redeem_attach_ticket(
    request: CatalogAttachRequest,
    options: Mapping[str, Any],
    *,
    signing_key: bytes | None,
    auth: AuthContext | None,
    now: float | None = None,
) -> CatalogAttachRequest | None:
    """Replace a ticket-carrying attach request with the attach it seals.

    Args:
        request: The incoming ``catalog_attach`` request.
        options: Its options, already decoded to a dict.
        signing_key: The worker's ``VGI_SIGNING_KEY``; ``None`` off HTTP,
            where no ticket can open.
        auth: The caller.
        now: Override the clock, for tests.

    Returns:
        ``None`` when *options* carries no ``vgi_attach_ticket`` (the request
        is untouched); otherwise the request the user originally made: the
        sealed catalog name, options and version specs, with this request's
        ``client_capabilities``.

    Raises:
        StatusError: ``invalid_request`` -- another option rides alongside
            the ticket. The sealed options are authoritative, so there is
            nothing to merge.
        AttachTicketInvalidError: The ticket does not open for this caller.
        AttachTicketExpiredError: The ticket is outside its lifetime.

    """
    from vgi_rpc.utils import deserialize_record_batch

    from vgi.protocol import CatalogAttachRequest

    ticket_keys = [key for key in options if key.lower() == ATTACH_TICKET_OPTION]
    if not ticket_keys:
        return None
    others = [key for key in options if key not in ticket_keys[:1]]
    if others:
        raise _invalid_request(
            f"{ATTACH_TICKET_OPTION} must be the only attach option",
            [(f"options.{key}", f"not allowed alongside {ATTACH_TICKET_OPTION}") for key in others],
        )
    token = options[ticket_keys[0]]
    if not isinstance(token, str):
        raise AttachTicketInvalidError(f"{ATTACH_TICKET_OPTION} must be a string")
    if signing_key is None:
        raise AttachTicketInvalidError("this worker does not redeem attach tickets")
    claims = open_attach_ticket(signing_key, token, principal=_caller_principal(auth), now=now)
    restored: pa.RecordBatch | None = None
    if claims.options_ipc:
        try:
            restored, _ = deserialize_record_batch(claims.options_ipc)
        except Exception as exc:
            raise AttachTicketInvalidError("attach ticket options are not an Arrow IPC record") from exc
    return CatalogAttachRequest(
        name=claims.catalog_name,
        options=restored,
        data_version_spec=claims.data_version_spec or None,
        implementation_version=claims.implementation_version or None,
        client_capabilities=request.client_capabilities,
    )


# ---------------------------------------------------------------------------
# vgi.attach_tickets.v1
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class SealAttachRequest(ArrowSerializableDataclass):
    """Request for :meth:`AttachTickets.seal_attach`.

    Attributes:
        catalog_name: The catalog the caller attached.
        options: The options the caller attached with, secret ones included:
            a one-row record serialized as Arrow IPC, exactly as
            ``CatalogAttachRequest.options``. ``None`` for none.
        data_version_spec: As given at ATTACH; ``""`` for none.
        implementation_version: As given at ATTACH; ``""`` for none.
        ttl_seconds: Requested lifetime. ``0`` asks for as long as the worker
            allows; the worker caps it at its grant maximum.

    """

    catalog_name: str
    options: Annotated[pa.RecordBatch | None, ArrowType(pa.binary())] = None
    data_version_spec: str = ""
    implementation_version: str = ""
    ttl_seconds: int = 0


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class AttachTicket(ArrowSerializableDataclass):
    """A sealed attach.

    Attributes:
        ticket: ``vgia1.`` text. Not a credential, but never logged.
        expires_at: Unix seconds after which the worker refuses it; ``+inf``
            when the worker sets no maximum lifetime.

    """

    ticket: str
    expires_at: float


class AttachTickets(Protocol):
    """Sealing a user's ATTACH so a runner holding their grant can replay it.

    Hosted on HTTP only when ``VGI_SIGNING_KEY`` is configured explicitly (a
    per-process key would kill every ticket on restart) and the worker can
    issue grants (a ticket is useless without one).
    """

    protocol_name: ClassVar[str] = ATTACH_TICKETS_PROTOCOL_NAME
    protocol_version: ClassVar[str] = "1.0.0"

    def seal_attach(self, request: SealAttachRequest) -> AttachTicket:
        """Seal the caller's attach of ``request.catalog_name`` into a ticket.

        The caller must be authenticated (anonymous is ``action_denied``); the
        principal sealed is the caller's. Options are validated against the
        catalog's declared attach options (``invalid_request`` with
        ``BadRequest``). No fresh login is required: a ticket carries no
        authority.

        Args:
            request: What to seal.

        Returns:
            The ticket and its expiry.

        """
        ...


def resolve_ticket_max_ttl(grant_keys: Any | None) -> int | None:
    """Return the ticket lifetime ceiling: the worker's grant maximum, or ``None``.

    With grant keys configured it is their ``max_ttl_seconds``. Otherwise (a
    worker minting its own grants) ``VGI_RPC_GRANT_MAX_TTL_SECONDS`` when set,
    else no maximum.

    Args:
        grant_keys: The deployment's ``GrantKeys``, or ``None``.

    Returns:
        The ceiling in seconds, or ``None`` for no maximum.

    Raises:
        SystemExit: The environment value is not a positive integer.

    """
    if grant_keys is not None:
        return int(grant_keys.max_ttl_seconds)
    from vgi_rpc.grants import GRANT_MAX_TTL_ENV

    raw = (os.environ.get(GRANT_MAX_TTL_ENV) or "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        value = 0
    if value <= 0:
        sys.stderr.write(f"Error: {GRANT_MAX_TTL_ENV}={raw!r} must be a positive integer\n")
        sys.exit(1)
    return value


def _declared_specs(worker: Any, catalog_name: str) -> list[AttachOptionSpec] | None:
    """Return the attach options *catalog_name* declares, or ``None`` if unknown."""
    from vgi_rpc.utils import deserialize_record_batch

    from vgi.catalog.attach_option import AttachOptionSpec

    try:
        infos = worker.catalog_catalogs().to_infos()
    except (ValueError, NotImplementedError):
        return None
    for info in infos:
        if info.name == catalog_name:
            specs: list[AttachOptionSpec] = []
            for raw in info.attach_option_specs or ():
                batch, _ = deserialize_record_batch(bytes(raw))
                specs.append(AttachOptionSpec.deserialize(batch))
            return specs
    return None


class AttachTicketsImpl:
    """``vgi.attach_tickets.v1`` over a worker (or ``MetaWorker``).

    Seals with the worker's own ``_signing_key`` -- the key that seals
    ``attach_opaque_data`` -- so tickets live exactly as long as attach
    envelopes do.
    """

    __slots__ = ("_max_ttl_seconds", "_worker")

    def __init__(self, worker: Any, *, max_ttl_seconds: int | None) -> None:
        """Build the implementation.

        Args:
            worker: The ``Worker`` or ``MetaWorker`` serving ``vgi.v2``.
            max_ttl_seconds: Lifetime ceiling, or ``None`` for no maximum.

        """
        self._worker = worker
        self._max_ttl_seconds = max_ttl_seconds

    def seal_attach(self, request: SealAttachRequest, ctx: CallContext) -> AttachTicket:
        """Validate and seal; see :meth:`AttachTickets.seal_attach`."""
        principal = _caller_principal(ctx.auth)
        if principal is None:
            raise _action_denied("an anonymous caller cannot seal an attach ticket")
        key = self._worker._signing_key
        if key is None:
            raise _action_denied("this worker does not seal attach tickets")

        violations: list[tuple[str, str]] = []
        if request.ttl_seconds < 0:
            violations.append(("ttl_seconds", "must be 0 (as long as allowed) or positive"))

        batch = request.options
        options: dict[str, Any] = {}
        if batch is not None:
            if batch.num_rows > 1:
                violations.append(("options", "must be a one-row record"))
            elif batch.num_rows == 1:
                options = batch.to_pylist()[0]

        specs = _declared_specs(self._worker, request.catalog_name)
        if specs is None:
            violations.append(("catalog_name", f"no catalog named {request.catalog_name!r}"))
        else:
            declared = {spec.name.lower() for spec in specs}
            for name in options:
                if name.lower() == ATTACH_TICKET_OPTION:
                    violations.append((f"options.{name}", "a ticket cannot seal another ticket"))
                elif name.lower() not in declared:
                    violations.append((f"options.{name}", "not an attach option this catalog declares"))
            supplied = {name.lower() for name in options}
            for spec in specs:
                if spec.required and spec.name.lower() not in supplied:
                    violations.append((f"options.{spec.name}", "required"))

        from vgi_rpc.utils import serialize_record_batch_bytes

        options_ipc = serialize_record_batch_bytes(batch) if batch is not None and options else b""
        if len(options_ipc) > MAX_OPTIONS_BYTES:
            violations.append(("options", f"{len(options_ipc)} bytes; a ticket carries at most {MAX_OPTIONS_BYTES}"))
        if violations:
            raise _invalid_request("seal_attach request is invalid", violations)

        issued_at = int(time.time())
        ttl = request.ttl_seconds
        ceiling = self._max_ttl_seconds
        # 0 asks for the ceiling; otherwise the request, capped at the ceiling.
        lifetime = ceiling if ttl == 0 else (ttl if ceiling is None else min(ttl, ceiling))
        expires_at = 0 if lifetime is None else issued_at + lifetime
        try:
            token, _claims = mint_attach_ticket(
                key,
                principal=principal,
                catalog_name=request.catalog_name,
                options_ipc=options_ipc,
                data_version_spec=request.data_version_spec,
                implementation_version=request.implementation_version,
                issued_at=issued_at,
                expires_at=expires_at,
            )
        except ValueError as exc:
            raise _invalid_request("seal_attach request is invalid", [("request", str(exc))]) from exc
        return AttachTicket(ticket=token, expires_at=math.inf if expires_at == 0 else float(expires_at))
