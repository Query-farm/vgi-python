# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Regenerate ``vgi/_test_fixtures/attach_ticket_vectors.json``.

Every input is fixed (key, nonce, clock, ticket id), so the output is
byte-identical on every run; ``tests/test_attach_ticket.py`` fails if the file
drifts from what this script produces. Run::

    uv run python scripts/gen_attach_ticket_vectors.py
"""

from __future__ import annotations

import base64
import json
import struct
from pathlib import Path
from typing import Any

from vgi_rpc.crypto import seal_bytes

from vgi.attach_ticket import (
    ATTACH_TICKET_PREFIX,
    CLOCK_SKEW_SECONDS,
    MAX_OPTIONS_BYTES,
    MAX_TICKET_CHARS,
    TICKET_ENVELOPE_VERSION,
    attach_ticket_aad,
    mint_attach_ticket,
)

OUT = Path(__file__).resolve().parent.parent / "vgi" / "_test_fixtures" / "attach_ticket_vectors.json"

KEY = b"attach-ticket-vector-signing-key"  # exactly 32 bytes: used as the AEAD key as-is
OTHER_KEY = b"another-workers-signing-key-0002"
SHORT_KEY = b"short operator key"  # not 32 bytes: SHA-256 normalized, as vgi_rpc.crypto does
NONCE = bytes(range(24))
NONCE_2 = bytes(range(100, 124))
ISSUED_AT = 1_790_000_000
EXPIRES_AT = ISSUED_AT + 3600
TICKET_ID = "0123456789abcdef0123456789abcdef"
TICKET_ID_2 = "fedcba9876543210fedcba9876543210"

assert len(KEY) == 32 and len(OTHER_KEY) == 32 and len(SHORT_KEY) != 32


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


#: ``{"region": "eu-west-2", "api_key": "sk-test-0123456789"}`` as one Arrow IPC
#: record. Frozen as bytes rather than re-serialized on each run: the ticket
#: format treats options as opaque, and a pyarrow upgrade that changed its IPC
#: padding must not change the vectors.
PROBE_OPTIONS = base64.b64decode(
    "/////6AAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAIAAABAAAAABAAAANj///8AAAEFEAAAABgA"
    "AAAEAAAAAAAAAAcAAABhcGlfa2V5AMj///8QABQACAAGAAcADAAAABAAEAAAAAAAAQUQAAAAHAAAAAQAAAAAAAAABgAAAHJlZ2lvbgAA"
    "BAAEAAQAAAAAAAAA/////9gAAAAUAAAAAAAAAAwAFgAGAAUACAAMAAwAAAAAAwQAGAAAADgAAAAAAAAAAAAKABgADAAEAAgACgAAAHwA"
    "AAAQAAAAAQAAAAAAAAAAAAAABgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAACAAAAAAAAAAJAAAAAAAAABgAAAAAAAAA"
    "AAAAAAAAAAAYAAAAAAAAAAgAAAAAAAAAIAAAAAAAAAASAAAAAAAAAAAAAAACAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAA"
    "AAAAAAAACQAAAGV1LXdlc3QtMgAAAAAAAAAAAAAAEgAAAHNrLXRlc3QtMDEyMzQ1Njc4OQAAAAAAAP////8AAAAA"
)


def _text(value: str) -> bytes:
    raw = value.encode("utf-8")
    return struct.pack("<H", len(raw)) + raw


def _payload(
    *,
    issued_at: int = ISSUED_AT,
    expires_at: int = EXPIRES_AT,
    ticket_id: str = TICKET_ID,
    catalog_name: str = "ticket_probe",
    dvs: str = "",
    iv: str = "",
    options: bytes = b"",
    options_len: int | None = None,
) -> bytes:
    """Hand-built payload, so reject cases can break one rule at a time."""
    return b"".join(
        (
            struct.pack("<qq", issued_at, expires_at),
            _text(ticket_id),
            _text(catalog_name),
            _text(dvs),
            _text(iv),
            struct.pack("<I", len(options) if options_len is None else options_len),
            options,
        )
    )


def _seal_raw(payload: bytes, *, principal: str = "alice", key: bytes = KEY, aad: bytes | None = None) -> str:
    envelope = seal_bytes(
        payload,
        key,
        aad=attach_ticket_aad(principal) if aad is None else aad,
        version=TICKET_ENVELOPE_VERSION,
        nonce=NONCE,
    )
    return ATTACH_TICKET_PREFIX + _b64url(envelope)


def _claims_json(claims: Any) -> dict[str, Any]:
    return {
        "issued_at": claims.issued_at,
        "expires_at": claims.expires_at,
        "ticket_id": claims.ticket_id,
        "catalog_name": claims.catalog_name,
        "data_version_spec": claims.data_version_spec,
        "implementation_version": claims.implementation_version,
        "options_ipc_b64": _b64(claims.options_ipc),
    }


def build() -> dict[str, Any]:
    """Build the vectors document."""
    probe_options = PROBE_OPTIONS
    mint_inputs: list[dict[str, Any]] = [
        {
            "name": "probe_with_options",
            "signing_key": KEY,
            "principal": "alice",
            "catalog_name": "ticket_probe",
            "options_ipc": probe_options,
            "data_version_spec": "",
            "implementation_version": "",
            "issued_at": ISSUED_AT,
            "expires_at": EXPIRES_AT,
            "ticket_id": TICKET_ID,
            "nonce": NONCE,
        },
        {
            "name": "no_expiry_no_options_with_versions",
            "signing_key": KEY,
            "principal": "bob@example.com",
            "catalog_name": "ticket_probe",
            "options_ipc": b"",
            "data_version_spec": ">=1.0.0,<2.0.0",
            "implementation_version": "1.2.3",
            "issued_at": ISSUED_AT,
            "expires_at": 0,
            "ticket_id": TICKET_ID_2,
            "nonce": NONCE_2,
        },
        {
            "name": "normalized_key_unicode_principal",
            "signing_key": SHORT_KEY,
            "principal": "zoë",
            "catalog_name": "ticket_probe",
            "options_ipc": probe_options,
            "data_version_spec": "",
            "implementation_version": "",
            "issued_at": ISSUED_AT,
            "expires_at": EXPIRES_AT,
            "ticket_id": TICKET_ID,
            "nonce": NONCE,
        },
    ]

    mint: list[dict[str, Any]] = []
    accept: list[dict[str, Any]] = []
    tokens: dict[str, str] = {}
    for case in mint_inputs:
        token, claims = mint_attach_ticket(
            case["signing_key"],
            principal=case["principal"],
            catalog_name=case["catalog_name"],
            options_ipc=case["options_ipc"],
            data_version_spec=case["data_version_spec"],
            implementation_version=case["implementation_version"],
            issued_at=case["issued_at"],
            expires_at=case["expires_at"],
            ticket_id=case["ticket_id"],
            nonce=case["nonce"],
        )
        tokens[case["name"]] = token
        mint.append(
            {
                "name": case["name"],
                "signing_key_b64": _b64(case["signing_key"]),
                "principal": case["principal"],
                "catalog_name": case["catalog_name"],
                "options_ipc_b64": _b64(case["options_ipc"]),
                "data_version_spec": case["data_version_spec"],
                "implementation_version": case["implementation_version"],
                "issued_at": case["issued_at"],
                "expires_at": case["expires_at"],
                "ticket_id": case["ticket_id"],
                "nonce_hex": case["nonce"].hex(),
                "aad_hex": attach_ticket_aad(case["principal"]).hex(),
                "payload_hex": _payload(
                    issued_at=case["issued_at"],
                    expires_at=case["expires_at"],
                    ticket_id=case["ticket_id"],
                    catalog_name=case["catalog_name"],
                    dvs=case["data_version_spec"],
                    iv=case["implementation_version"],
                    options=case["options_ipc"],
                ).hex(),
                "token": token,
            }
        )
        accept.append(
            {
                "name": f"{case['name']}_opens",
                "signing_key_b64": _b64(case["signing_key"]),
                "token": token,
                "principal": case["principal"],
                "now": case["issued_at"] + 10,
                "claims": _claims_json(claims),
            }
        )

    probe = tokens["probe_with_options"]
    probe_claims = accept[0]["claims"]
    accept += [
        {
            "name": "inside_expiry_skew",
            "token": probe,
            "principal": "alice",
            "now": EXPIRES_AT + CLOCK_SKEW_SECONDS - 1,
            "claims": probe_claims,
        },
        {
            "name": "inside_issue_skew",
            "token": probe,
            "principal": "alice",
            "now": ISSUED_AT - CLOCK_SKEW_SECONDS,
            "claims": probe_claims,
        },
        {
            "name": "no_expiry_far_future",
            "token": tokens["no_expiry_no_options_with_versions"],
            "principal": "bob@example.com",
            "now": ISSUED_AT + 100 * 365 * 86400,
            "claims": accept[1]["claims"],
        },
    ]

    invalid = "attach_ticket_invalid"
    expired = "attach_ticket_expired"
    body = probe[len(ATTACH_TICKET_PREFIX) :]
    # Flip one ciphertext character (well inside the body, so it stays canonical).
    mid = len(body) // 2
    tampered = ATTACH_TICKET_PREFIX + body[:mid] + ("A" if body[mid] != "A" else "B") + body[mid + 1 :]
    # The same bytes with non-zero trailing bits in the last character: decodes
    # to the same envelope, so only the canonical-spelling rule rejects it.
    raw = base64.urlsafe_b64decode(body + "=" * (-len(body) % 4))
    assert len(raw) % 3 != 0, "need a partial final quantum for the non-canonical case"
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    last = alphabet.index(body[-1])
    non_canonical = ATTACH_TICKET_PREFIX + body[:-1] + alphabet[last | 1]
    assert non_canonical != probe
    assert base64.urlsafe_b64decode(non_canonical[len(ATTACH_TICKET_PREFIX) :] + "=" * (-len(body) % 4)) == raw

    # An attach_opaque_data envelope for the same principal, in both its real
    # form (version 2) and with the ticket's version byte, so only the AAD
    # domain separates it.
    attach_aad = b"vgi.attach_opaque_data.v1\x00" + b"\x01" + b"bearer" + b"\x00" + b"alice"
    real_attach = ATTACH_TICKET_PREFIX + _b64url(
        seal_bytes(bytes(16) + b"catalog-bytes", KEY, aad=attach_aad, version=2, nonce=NONCE)
    )
    attach_aad_v1 = _seal_raw(_payload(options=probe_options), aad=attach_aad)
    domain_principal_aad = _seal_raw(
        _payload(options=probe_options), aad=b"vgi.attach_ticket.v1\x00" + b"\x01" + b"bearer" + b"\x00" + b"alice"
    )

    too_long = ATTACH_TICKET_PREFIX + "A" * (MAX_TICKET_CHARS - len(ATTACH_TICKET_PREFIX) + 4)
    reject: list[dict[str, Any]] = [
        {"name": "wrong_principal", "token": probe, "principal": "bob", "error_kind": invalid},
        {"name": "anonymous_caller", "token": probe, "principal": "", "error_kind": invalid},
        {"name": "principal_case_differs", "token": probe, "principal": "Alice", "error_kind": invalid},
        {"name": "tampered_ciphertext", "token": tampered, "principal": "alice", "error_kind": invalid},
        {
            "name": "other_workers_key",
            "signing_key_b64": _b64(OTHER_KEY),
            "token": probe,
            "principal": "alice",
            "error_kind": invalid,
        },
        {
            "name": "expired",
            "token": probe,
            "principal": "alice",
            "now": EXPIRES_AT + CLOCK_SKEW_SECONDS,
            "error_kind": expired,
        },
        {
            "name": "not_yet_valid",
            "token": probe,
            "principal": "alice",
            "now": ISSUED_AT - CLOCK_SKEW_SECONDS - 1,
            "error_kind": expired,
        },
        {
            "name": "expired_for_wrong_principal_is_invalid",
            "token": probe,
            "principal": "bob",
            "now": EXPIRES_AT + 10_000,
            "error_kind": invalid,
        },
        {"name": "attach_envelope_as_ticket", "token": real_attach, "principal": "alice", "error_kind": invalid},
        {
            "name": "attach_aad_with_ticket_version",
            "token": attach_aad_v1,
            "principal": "alice",
            "error_kind": invalid,
        },
        {
            "name": "domain_and_principal_aad",
            "token": domain_principal_aad,
            "principal": "alice",
            "error_kind": invalid,
        },
        {"name": "non_canonical_base64url", "token": non_canonical, "principal": "alice", "error_kind": invalid},
        {"name": "padded_base64url", "token": probe + "=", "principal": "alice", "error_kind": invalid},
        {
            "name": "standard_base64_alphabet",
            "token": ATTACH_TICKET_PREFIX + body.replace("-", "+").replace("_", "/")
            if ("-" in body or "_" in body)
            else ATTACH_TICKET_PREFIX + body + "+",
            "principal": "alice",
            "error_kind": invalid,
        },
        {"name": "wrong_prefix_grant", "token": "vgig1." + body, "principal": "alice", "error_kind": invalid},
        {"name": "wrong_prefix_version", "token": "vgia2." + body, "principal": "alice", "error_kind": invalid},
        {"name": "no_prefix", "token": body, "principal": "alice", "error_kind": invalid},
        {"name": "empty_body", "token": ATTACH_TICKET_PREFIX, "principal": "alice", "error_kind": invalid},
        {
            "name": "envelope_too_short",
            "token": ATTACH_TICKET_PREFIX + _b64url(bytes([TICKET_ENVELOPE_VERSION]) + bytes(30)),
            "principal": "alice",
            "error_kind": invalid,
        },
        {"name": "too_long", "token": too_long, "principal": "alice", "error_kind": invalid},
        {
            "name": "trailing_payload_bytes",
            "token": _seal_raw(_payload(options=probe_options) + b"\x00"),
            "principal": "alice",
            "error_kind": invalid,
        },
        {
            "name": "truncated_payload",
            "token": _seal_raw(_payload(options=probe_options)[:-1]),
            "principal": "alice",
            "error_kind": invalid,
        },
        {
            "name": "options_length_overruns",
            "token": _seal_raw(_payload(options=b"abc", options_len=4)),
            "principal": "alice",
            "error_kind": invalid,
        },
        {
            "name": "options_over_16_kib",
            "token": _seal_raw(_payload(options=b"\x00" * (MAX_OPTIONS_BYTES + 1))),
            "principal": "alice",
            "error_kind": invalid,
        },
        {
            "name": "uppercase_ticket_id",
            "token": _seal_raw(_payload(ticket_id=TICKET_ID.upper())),
            "principal": "alice",
            "error_kind": invalid,
        },
        {
            "name": "short_ticket_id",
            "token": _seal_raw(_payload(ticket_id=TICKET_ID[:-2])),
            "principal": "alice",
            "error_kind": invalid,
        },
        {
            "name": "empty_catalog_name",
            "token": _seal_raw(_payload(catalog_name="")),
            "principal": "alice",
            "error_kind": invalid,
        },
        {
            "name": "invalid_utf8_catalog_name",
            "token": _seal_raw(
                struct.pack("<qq", ISSUED_AT, EXPIRES_AT)
                + _text(TICKET_ID)
                + struct.pack("<H", 2)
                + b"\xc3\x28"
                + _text("")
                + _text("")
                + struct.pack("<I", 0)
            ),
            "principal": "alice",
            "error_kind": invalid,
        },
        {
            "name": "expires_before_issued",
            "token": _seal_raw(_payload(expires_at=ISSUED_AT)),
            "principal": "alice",
            "error_kind": invalid,
        },
    ]

    # Options exactly at the cap are accepted.
    at_cap = _seal_raw(_payload(options=b"\x01" * MAX_OPTIONS_BYTES))
    accept.append(
        {
            "name": "options_at_16_kib",
            "token": at_cap,
            "principal": "alice",
            "now": ISSUED_AT + 10,
            "claims": {
                "issued_at": ISSUED_AT,
                "expires_at": EXPIRES_AT,
                "ticket_id": TICKET_ID,
                "catalog_name": "ticket_probe",
                "data_version_spec": "",
                "implementation_version": "",
                "options_ipc_b64": _b64(b"\x01" * MAX_OPTIONS_BYTES),
            },
        }
    )

    redeem = [
        {
            "name": "ticket_alone_restores_the_attach",
            "options": {"vgi_attach_ticket": probe},
            "principal": "alice",
            "result": {
                "catalog_name": "ticket_probe",
                "data_version_spec": None,
                "implementation_version": None,
                "options": {"region": "eu-west-2", "api_key": "sk-test-0123456789"},
            },
        },
        {
            "name": "option_name_is_case_insensitive",
            "options": {"VGI_Attach_Ticket": probe},
            "principal": "alice",
            "result": {
                "catalog_name": "ticket_probe",
                "data_version_spec": None,
                "implementation_version": None,
                "options": {"region": "eu-west-2", "api_key": "sk-test-0123456789"},
            },
        },
        {
            "name": "versions_and_no_options_restore",
            "options": {"vgi_attach_ticket": tokens["no_expiry_no_options_with_versions"]},
            "principal": "bob@example.com",
            "result": {
                "catalog_name": "ticket_probe",
                "data_version_spec": ">=1.0.0,<2.0.0",
                "implementation_version": "1.2.3",
                "options": {},
            },
        },
        {
            "name": "other_option_alongside",
            "options": {"vgi_attach_ticket": probe, "region": "us-west-1"},
            "principal": "alice",
            "error_kind": "invalid_request",
        },
        {
            "name": "other_option_alongside_even_for_wrong_principal",
            "options": {"vgi_attach_ticket": probe, "api_key": "x"},
            "principal": "bob",
            "error_kind": "invalid_request",
        },
        {
            "name": "two_spellings_of_the_ticket",
            "options": {"vgi_attach_ticket": probe, "VGI_ATTACH_TICKET": probe},
            "principal": "alice",
            "error_kind": "invalid_request",
        },
        {
            "name": "wrong_principal",
            "options": {"vgi_attach_ticket": probe},
            "principal": "bob",
            "error_kind": invalid,
        },
        {
            "name": "anonymous",
            "options": {"vgi_attach_ticket": probe},
            "principal": "",
            "error_kind": invalid,
        },
        {
            "name": "no_ticket_means_untouched",
            "options": {"region": "us-west-1", "api_key": "x"},
            "principal": "alice",
            "result": None,
        },
    ]

    return {
        "description": (
            "Attach-ticket vectors (vgi.attach_tickets.v1). Normative spec: vgi-python "
            "docs/protocol/vgi-attach-tickets.md. Generated by scripts/gen_attach_ticket_vectors.py; "
            "do not edit by hand. Keys and options are standard base64; nonces and payloads hex. "
            "options_ipc bytes are opaque to the ticket format: a port seals the bytes it is given."
        ),
        "defaults": {
            "signing_key_b64": _b64(KEY),
            "now": ISSUED_AT + 10,
            "clock_skew_seconds": CLOCK_SKEW_SECONDS,
            "max_options_bytes": MAX_OPTIONS_BYTES,
            "max_ticket_chars": MAX_TICKET_CHARS,
            "envelope_version": TICKET_ENVELOPE_VERSION,
        },
        "mint": mint,
        "accept": accept,
        "reject": reject,
        "redeem": redeem,
    }


def render() -> str:
    """The file's exact text."""
    return json.dumps(build(), indent=2, ensure_ascii=False) + "\n"


if __name__ == "__main__":
    OUT.write_text(render(), encoding="utf-8")
    print(f"wrote {OUT}")
