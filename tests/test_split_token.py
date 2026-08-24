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
    bind_fingerprint,
    build_split_token,
    open_split_token,
)
from vgi.worker import Worker

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
    assert open_split_token(sealed, signing_key=KEY, expected_fingerprint=FINGERPRINT, current_anchor=ANCHOR) == PAYLOAD


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
        opened = open_split_token(raw, signing_key=key, expected_fingerprint=FINGERPRINT, current_anchor=ANCHOR)
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
    try:
        for case in MANIFEST["cases"]:
            if not case["reproducible"]:
                continue
            name = f"{case['name']}.bin"
            assert (FIXTURES / name).read_bytes() == before[name], f"{name} drifted"
    finally:
        # Put the NON-reproducible vectors back. A sealed token draws a fresh
        # random nonce, so regenerating rewrites its bytes every run and leaves
        # the working tree dirty — which is how unrelated nonce churn ends up
        # staged in someone else's commit. The reproducible ones are restored
        # too: they compared equal, so this is a no-op for them.
        for name, data in before.items():
            path = FIXTURES / name
            if path.read_bytes() != data:
                path.write_bytes(data)


# --------------------------------------------------------------------------- #
# The anchor must be minted from the same place redemption reads it
# --------------------------------------------------------------------------- #


def test_anchor_defaults_to_the_live_catalog_version_not_zero() -> None:
    """A worker that plans without naming a version must still mint a usable token.

    The anchor is compared at redemption against the catalog's live version. Minting
    it from ``response.catalog_version or 0`` meant a worker whose catalog really does
    count versions, but whose ``on_plan`` leaves the field unset, stamped every token
    with 0 and then refused every one of them — and the documented response to
    ``SPLIT_SNAPSHOT_EXPIRED`` is "re-run the query", which re-plans, mints 0 again and
    fails again. A livelock returning no rows, with an error blaming the data for
    moving when it had not.

    Invisible wherever the catalog's version is 0, which is most fixtures — so this
    pins the non-zero case specifically.
    """
    from vgi.protocol import BindRequest as PBindRequest
    from vgi.protocol import PlanResponse, ScanSplit, TableFunctionPlanRequest

    class _VersionedWorker:
        """Stands in for a worker whose catalog reports a non-zero version."""

        _signing_key = None

        def _current_split_anchor(self, request: object, ctx: object) -> bytes:
            return (47).to_bytes(8, "little", signed=True)

        _stamp_split_tokens = Worker._stamp_split_tokens

    bind = PBindRequest(function_name="f", arguments=b"", function_type="TABLE")
    request = TableFunctionPlanRequest(bind_call=bind)

    class _Ctx:
        auth = None

    # on_plan left catalog_version unset — the common case for a worker that has
    # not thought about snapshots.
    planned = PlanResponse(splits=[ScanSplit(payload=b"file=1")])
    stamped = _VersionedWorker()._stamp_split_tokens(planned, request, _Ctx())

    split = ScanSplit.deserialize_from_bytes(stamped.splits[0])
    # Redemption compares against the live version; the token must name it.
    opened = open_split_token(
        split.token,
        expected_fingerprint=bind_fingerprint(bind),
        current_anchor=(47).to_bytes(8, "little", signed=True),
    )
    assert opened == b"file=1"


def test_a_worker_that_names_its_version_is_taken_at_its_word() -> None:
    """An explicit ``catalog_version`` wins: the worker knows which snapshot it planned."""
    from vgi.protocol import BindRequest as PBindRequest
    from vgi.protocol import PlanResponse, ScanSplit, TableFunctionPlanRequest

    class _VersionedWorker:
        _signing_key = None

        def _current_split_anchor(self, request: object, ctx: object) -> bytes:
            return (47).to_bytes(8, "little", signed=True)

        _stamp_split_tokens = Worker._stamp_split_tokens

    bind = PBindRequest(function_name="f", arguments=b"", function_type="TABLE")
    request = TableFunctionPlanRequest(bind_call=bind)

    class _Ctx:
        auth = None

    planned = PlanResponse(splits=[ScanSplit(payload=b"file=1")], catalog_version=11)
    stamped = _VersionedWorker()._stamp_split_tokens(planned, request, _Ctx())
    split = ScanSplit.deserialize_from_bytes(stamped.splits[0])

    with pytest.raises(SplitSnapshotExpired):
        open_split_token(
            split.token,
            expected_fingerprint=bind_fingerprint(bind),
            current_anchor=(47).to_bytes(8, "little", signed=True),
        )
    assert (
        open_split_token(
            split.token,
            expected_fingerprint=bind_fingerprint(bind),
            current_anchor=(11).to_bytes(8, "little", signed=True),
        )
        == b"file=1"
    )


def test_stamping_clears_the_plaintext_payload() -> None:
    """A stamped split must not carry its payload in the clear beside the token.

    The framework seals the payload INTO the token so a caller cannot rewrite which
    work a split names. Forwarding the same bytes in the neighbouring ``payload``
    field made that seal decorative on a keyed worker: anyone reading the plan
    response got the plaintext verbatim, without touching the ciphertext.

    Nothing needs it there. The C++ client reads ``token`` alone and sends only the
    token back, and redemption recovers the payload from inside the envelope — so
    this is a leak with no consumer, which is the easiest kind to leave in place
    for years.
    """
    from vgi.protocol import BindRequest as PBindRequest
    from vgi.protocol import PlanResponse, ScanSplit, TableFunctionPlanRequest

    secret = b"file=/tenant-a/private.parquet;v=47"

    class _KeyedWorker:
        _signing_key = b"k" * 32

        def _current_split_anchor(self, request: object, ctx: object) -> bytes:
            return (0).to_bytes(8, "little", signed=True)

        _stamp_split_tokens = Worker._stamp_split_tokens

    class _Ctx:
        auth = None

    bind = PBindRequest(function_name="f", arguments=b"", function_type="TABLE")
    request = TableFunctionPlanRequest(bind_call=bind)
    stamped = _KeyedWorker()._stamp_split_tokens(PlanResponse(splits=[ScanSplit(payload=secret)]), request, _Ctx())

    # Not in the field...
    split = ScanSplit.deserialize_from_bytes(stamped.splits[0])
    assert split.payload == b""
    # ...and not anywhere else in the serialized record either, which is the
    # assertion that survives someone moving the leak to a different column.
    assert secret not in stamped.splits[0]

    # Still fully recoverable from the token, so clearing the field cost nothing.
    opened = open_split_token(
        split.token,
        signing_key=b"k" * 32,
        expected_fingerprint=bind_fingerprint(bind),
        current_anchor=(0).to_bytes(8, "little", signed=True),
    )
    assert opened == secret
