"""Split-token envelope: layout, typed errors, and the two security refusals.

The envelope is the one piece of the splits change where five independent SDK
implementations could silently disagree *and* where disagreeing is a
vulnerability, so this file pins both the bytes and the refusals.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from vgi.split_token import (
    _HEADER_STRUCT,
    FLAG_PAYLOAD_SEALED,
    FORMAT_VERSION,
    SplitSnapshotExpired,
    SplitTokenInvalid,
    build_split_token,
    open_split_token,
)

FIXTURES = pathlib.Path(__file__).parent / "data" / "split_tokens"
MANIFEST = json.loads((FIXTURES / "manifest.json").read_text())

KEY = bytes.fromhex(MANIFEST["key_hex"])
FINGERPRINT = bytes.fromhex(MANIFEST["fingerprint_hex"])
ANCHOR = bytes.fromhex(MANIFEST["anchor_hex"])
PAYLOAD = MANIFEST["payload"].encode()


class _Auth:
    def __init__(self, principal: str) -> None:
        self.authenticated = True
        self.domain = "test"
        self.principal = principal


# --------------------------------------------------------------------------- #
# Layout
# --------------------------------------------------------------------------- #


def test_header_layout_is_byte_exact() -> None:
    """The fixed prefix is what every other SDK must agree on, field by field."""
    token = build_split_token(payload=PAYLOAD, fingerprint=FINGERPRINT, anchor=ANCHOR)
    version, flags, anchor_len = _HEADER_STRUCT.unpack_from(token, 0)
    assert version == FORMAT_VERSION
    assert flags == 0
    assert anchor_len == len(ANCHOR)
    assert token[4:20] == FINGERPRINT
    assert token[20 : 20 + anchor_len] == ANCHOR
    assert token[20 + anchor_len :] == PAYLOAD


def test_unsealed_fixture_is_reproducible_byte_for_byte() -> None:
    """Regenerating must reproduce the checked-in bytes.

    This is the check a behavioural test cannot make: two implementations that
    disagree on ``anchor_len`` endianness or fingerprint truncation are each
    self-consistent, and only a byte comparison separates them.
    """
    expected = (FIXTURES / "valid_unsealed.bin").read_bytes()
    actual = build_split_token(payload=PAYLOAD, fingerprint=FINGERPRINT, anchor=ANCHOR)
    assert actual == expected


def test_keyless_and_keyed_round_trip() -> None:
    """Both transports produce a token the same worker can reopen."""
    plain = build_split_token(payload=PAYLOAD, fingerprint=FINGERPRINT, anchor=ANCHOR)
    assert open_split_token(plain, expected_fingerprint=FINGERPRINT, current_anchor=ANCHOR) == PAYLOAD

    sealed = build_split_token(payload=PAYLOAD, fingerprint=FINGERPRINT, anchor=ANCHOR, signing_key=KEY)
    assert sealed[1] & FLAG_PAYLOAD_SEALED
    assert PAYLOAD not in sealed, "a sealed token must not leak its plaintext payload"
    assert (
        open_split_token(sealed, signing_key=KEY, expected_fingerprint=FINGERPRINT, current_anchor=ANCHOR)
        == PAYLOAD
    )


# --------------------------------------------------------------------------- #
# Security refusals
# --------------------------------------------------------------------------- #


def test_keyed_worker_refuses_an_unsealed_token() -> None:
    """The alg:none downgrade.

    ``flags`` is attacker-controlled plaintext. Clearing bit 0 and appending a
    plaintext payload produces a structurally valid token naming any work the
    attacker likes; a worker that trusted the flag would redeem it without ever
    authenticating. The key state decides, never the token.
    """
    forged = build_split_token(payload=b"OTHER TENANT DATA", fingerprint=FINGERPRINT, anchor=ANCHOR)
    with pytest.raises(SplitTokenInvalid, match="alg:none"):
        open_split_token(forged, signing_key=KEY, expected_fingerprint=FINGERPRINT, current_anchor=ANCHOR)


def test_seal_binds_the_caller_principal() -> None:
    """A value minted for one principal must not be replayable by another.

    Mirrors the attach envelope, which binds identity for exactly this reason — and
    a split token names data (files, offsets, tenant partitions).
    """
    token = build_split_token(
        payload=PAYLOAD, fingerprint=FINGERPRINT, anchor=ANCHOR, signing_key=KEY, auth=_Auth("alice")
    )
    assert open_split_token(token, signing_key=KEY, auth=_Auth("alice")) == PAYLOAD
    with pytest.raises(SplitTokenInvalid):
        open_split_token(token, signing_key=KEY, auth=_Auth("bob"))


def test_header_is_covered_by_the_aad() -> None:
    """Flipping any header byte invalidates a sealed token."""
    sealed = build_split_token(payload=PAYLOAD, fingerprint=FINGERPRINT, anchor=ANCHOR, signing_key=KEY)
    tampered = bytearray(sealed)
    tampered[4] ^= 0xFF  # first fingerprint byte
    with pytest.raises(SplitTokenInvalid):
        open_split_token(bytes(tampered), signing_key=KEY)


def test_keyless_worker_cannot_open_a_sealed_token() -> None:
    """A worker with no key must not silently skip verification."""
    sealed = build_split_token(payload=PAYLOAD, fingerprint=FINGERPRINT, anchor=ANCHOR, signing_key=KEY)
    with pytest.raises(SplitTokenInvalid, match="no signing key"):
        open_split_token(sealed)


# --------------------------------------------------------------------------- #
# Typed errors — the distinction must survive, because only one means "re-run"
# --------------------------------------------------------------------------- #


def test_bind_mismatch_and_stale_anchor_are_different_kinds() -> None:
    """Only one of them means 're-run the query'."""
    token = build_split_token(payload=PAYLOAD, fingerprint=FINGERPRINT, anchor=ANCHOR)

    with pytest.raises(SplitTokenInvalid):
        open_split_token(token, expected_fingerprint=b"\xee" * 16, current_anchor=ANCHOR)

    with pytest.raises(SplitSnapshotExpired):
        open_split_token(token, expected_fingerprint=FINGERPRINT, current_anchor=(99).to_bytes(8, "little"))


def test_error_kinds_are_stable_strings() -> None:
    """The C++ side parses these off the wire; renaming one is a wire change."""
    assert SplitTokenInvalid.error_kind == "SPLIT_TOKEN_INVALID"
    assert SplitSnapshotExpired.error_kind == "SPLIT_SNAPSHOT_EXPIRED"


# --------------------------------------------------------------------------- #
# Every checked-in vector reaches its recorded verdict
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("case", MANIFEST["cases"], ids=lambda c: c["name"])
def test_fixture_vectors_reach_their_recorded_verdict(case: dict) -> None:
    """Every checked-in vector must reach the verdict its manifest records."""
    raw = (FIXTURES / f"{case['name']}.bin").read_bytes()
    assert len(raw) == case["size"]

    # The manifest states whether the worker holds a key, rather than each SDK
    # inferring it: the alg:none vector is a structurally VALID unsealed token
    # whose whole point is that a KEYED worker refuses it, so a consumer that
    # guessed from the token would test the opposite of the rule.
    key = KEY if case["worker_keyed"] else None

    if case["verdict"] == "ok":
        opened = open_split_token(
            raw, signing_key=key, expected_fingerprint=FINGERPRINT, current_anchor=ANCHOR
        )
        assert opened == PAYLOAD
        return

    expected = {
        "SPLIT_TOKEN_INVALID": SplitTokenInvalid,
        "SPLIT_SNAPSHOT_EXPIRED": SplitSnapshotExpired,
    }[case["verdict"]]
    with pytest.raises(expected):
        open_split_token(raw, signing_key=key, expected_fingerprint=FINGERPRINT, current_anchor=ANCHOR)


def test_reproducible_vectors_regenerate_identically() -> None:
    """Guards the fixtures themselves against silent drift."""
    import subprocess
    import sys

    before = {p.name: p.read_bytes() for p in FIXTURES.glob("*.bin")}
    subprocess.run([sys.executable, str(FIXTURES / "generate.py")], check=True, cwd=FIXTURES.parents[2])
    for case in MANIFEST["cases"]:
        if not case["reproducible"]:
            continue
        name = f"{case['name']}.bin"
        assert (FIXTURES / name).read_bytes() == before[name], f"{name} drifted"
