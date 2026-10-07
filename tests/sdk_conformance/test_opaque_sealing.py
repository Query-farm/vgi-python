# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Cross-SDK conformance: sealing of ``attach_opaque_data`` / ``transaction_opaque_data``.

Checks the rules of ``docs/protocol/vgi-opaque-data-sealing.md`` against any
SDK's fixture worker served over HTTP. The bytes a worker emits are its own
business; what is checked is how it treats bytes it did not issue to *this*
caller: replayed by another principal, tampered, lifted onto another attach, or
forged in the shape an unsealed SDK would produce. Every one must be refused
with the same error, and no secret attach option may be readable in the value.

Environment:

``VGI_SDK_HTTP_URL``
    Base URL of the running fixture worker. **Unset, the whole module skips.**
``VGI_SDK_BEARER_A`` / ``VGI_SDK_BEARER_B``
    Bearer tokens for two *distinct* principals. Default ``vgi-test-alice`` /
    ``vgi-test-bob``, the fixture test bearers every SDK's fixture HTTP server
    accepts (the C++ one only with ``VGI_FIXTURE_TEST_BEARERS=1``).
``VGI_SDK_CATALOG``
    Catalog to attach for the replay / tamper / transaction cases. Default:
    the first advertised catalog (``example`` preferred) that attaches without
    options and returns a non-empty ``attach_opaque_data``; the transaction
    cases use the first such catalog that also begins a transaction.
``VGI_SDK_SECRET_CATALOG``
    Catalog with a ``secret=True`` attach option. Default: ``ticket_probe``
    when advertised, else the first catalog advertising a secret option whose
    required options are all strings. With none, that case skips.
``VGI_SDK_PLAINTEXT_HEX``
    Optional comma-separated hex values to add to the forged-plaintext cases:
    the exact shape *this* SDK would produce unsealed.
``VGI_SDK_WORKER_LOG``
    Optional path to the worker's log file (stdout+stderr). When set, the log
    is checked for raw values and the secret canary.

Run::

    VGI_SDK_HTTP_URL=http://127.0.0.1:8765 uv run pytest tests/sdk_conformance -v
"""

from __future__ import annotations

import base64
import contextlib
import functools
import os
import re
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc.rpc import RpcError

from vgi.catalog import AttachOpaqueData, TransactionOpaqueData
from vgi.catalog.attach_option import AttachOptionSpec
from vgi.client import Client
from vgi.client.catalog_mixin import CatalogClientError

_URL = os.environ.get("VGI_SDK_HTTP_URL", "").strip()
pytestmark = pytest.mark.skipif(not _URL, reason="set VGI_SDK_HTTP_URL to an SDK fixture worker's HTTP URL")

_BEARER_A = os.environ.get("VGI_SDK_BEARER_A", "vgi-test-alice")
_BEARER_B = os.environ.get("VGI_SDK_BEARER_B", "vgi-test-bob")

ATTACH_FIELD = "attach_opaque_data"
TX_FIELD = "transaction_opaque_data"

_SAME_PRINCIPAL_HINT = (
    " (if the SDK does bind the caller, check that VGI_SDK_BEARER_A and VGI_SDK_BEARER_B resolve to "
    "two different principals on this worker; the C++ fixture needs VGI_FIXTURE_TEST_BEARERS=1)"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Rejection:
    """What a refused call put on the wire. Compared whole for uniformity."""

    error_type: str
    error_code: str
    error_kind: str
    error_message: str
    error_details: str

    @property
    def message(self) -> str:
        """The message without the ``"<error_type>: "`` prefix vgi-rpc's Python server puts on it."""
        prefix = f"{self.error_type}: "
        return self.error_message[len(prefix) :] if self.error_message.startswith(prefix) else self.error_message

    def normalized(self) -> tuple[str, str, str, str, str]:
        """The rejection with the field name masked, for comparing across fields."""
        msg = self.error_message.replace(ATTACH_FIELD, "<field>").replace(TX_FIELD, "<field>")
        return (self.error_type, self.error_code, self.error_kind, msg, self.error_details)


#: The wire shape every refusal must have (spec rule 4).
EXPECTED_CODE = "INVALID_ARGUMENT"
EXPECTED_KIND = "opaque_data_not_recognized"


def _shape_problems(rej: Rejection, field: str) -> list[str]:
    """How ``rej`` differs from the required uniform refusal for ``field``; empty when it matches."""
    problems = []
    if rej.error_code != EXPECTED_CODE:
        problems.append(f"error_code {rej.error_code!r} != {EXPECTED_CODE!r}")
    if rej.error_kind != EXPECTED_KIND:
        problems.append(f"error_kind {rej.error_kind!r} != {EXPECTED_KIND!r}")
    if rej.message != f"{field} not recognized":
        problems.append(f"message {rej.message!r} != {field + ' not recognized'!r}")
    if rej.error_details:
        problems.append(f"error_details must be empty, got {rej.error_details}")
    return problems


def _client(bearer: str) -> Client:
    return Client(transport="http", base_url=_URL, bearer_token=bearer or None, pool=None)


def _attach(client: Client, name: str, options: dict[str, Any] | None = None) -> Any:
    return client.catalog_attach(name=name, options=options, data_version_spec=None, implementation_version=None)


def _outcome(call: Callable[[], object]) -> Rejection | None:
    """Run ``call``: its refusal as a `Rejection`, or ``None`` when the worker accepted it."""
    try:
        call()
    except CatalogClientError as exc:
        cause = exc.__cause__
        if isinstance(cause, RpcError):
            return Rejection(
                cause.error_type,
                cause.error_code,
                cause.error_kind,
                cause.error_message,
                repr(cause.error_details) if cause.error_details else "",
            )
        return Rejection(type(exc).__name__, "", "", str(exc), "")
    return None


def _rejected(call: Callable[[], object], field: str, what: str) -> Rejection:
    """Run ``call``, require it to be refused naming ``field``, and return the refusal."""
    rej = _outcome(call)
    if rej is None:
        pytest.fail(f"{what}: the worker accepted it; it must refuse with '{field} not recognized'")
    problems = _shape_problems(rej, field)
    assert not problems, f"{what}: refused, but not with the uniform refusal: {'; '.join(problems)} ({rej})"
    return rej


def _all_rejected(calls: dict[str, Callable[[], object]], field: str) -> None:
    """Every call is refused with one identical ``<field> not recognized`` error; report all offenders."""
    outcomes = {label: _outcome(call) for label, call in calls.items()}
    accepted = sorted(label for label, rej in outcomes.items() if rej is None)
    assert not accepted, f"the worker accepted forged {field} values of shape {accepted}"
    wrong = {label: _shape_problems(rej, field) for label, rej in outcomes.items() if rej}
    wrong = {label: problems for label, problems in wrong.items() if problems}
    assert not wrong, f"forged {field} values refused, but not with the uniform refusal: {wrong}"
    assert len(set(outcomes.values())) == 1, f"forged {field} values refused with different errors: {outcomes}"


def _version(client: Client, attach: bytes, tx: bytes | None = None) -> int:
    """``catalog_version`` with exactly these bytes: the probe every case uses."""
    return client.catalog_version(
        attach_opaque_data=AttachOpaqueData(attach),
        transaction_opaque_data=None if tx is None else TransactionOpaqueData(tx),
    )


def _flip(value: bytes, index: int) -> bytes:
    out = bytearray(value)
    out[index] ^= 0x01
    return bytes(out)


def _flip_positions(value: bytes) -> dict[str, int]:
    return {"first": 0, "middle": len(value) // 2, "last": len(value) - 1}


def _options_ipc(options: dict[str, str]) -> bytes:
    batch = pa.RecordBatch.from_pylist([options])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return bytes(sink.getvalue().to_pybytes())


def _plaintext_candidates(catalog: str) -> dict[str, bytes]:
    """Values of the shapes SDKs produced unsealed, plus an envelope-shaped forgery."""
    u = uuid.uuid4()
    candidates = {
        # Python's framework plaintext: uuid(16) || catalog bytes.
        "uuid-plus-catalog-bytes": u.bytes + catalog.encode(),
        # A bare session id (C#, C++ ids).
        "bare-uuid": u.bytes,
        "uuid-text": str(u).encode(),
        # Go's former plaintext bypass prefix.
        "writable-prefix": b"writable:" + u.hex.encode(),
        # Rust / C++: the merged options as an Arrow IPC stream.
        "arrow-ipc-options": _options_ipc({"catalog": catalog, "region": "us-east-1"}),
        "json": ('{"catalog":"' + catalog + '","session":"' + str(u) + '"}').encode(),
        # Right length and version byte for an XChaCha20-Poly1305 envelope, wrong everything else.
        "envelope-shaped": bytes([2]) + bytes(24) + os.urandom(48),
    }
    for i, raw in enumerate(filter(None, os.environ.get("VGI_SDK_PLAINTEXT_HEX", "").split(","))):
        candidates[f"sdk-supplied-{i}"] = bytes.fromhex(raw.strip())
    return candidates


def _spec_rows(info: Any) -> list[AttachOptionSpec]:
    specs = []
    for raw in info.attach_option_specs or []:
        batch = pa.ipc.open_stream(pa.py_buffer(raw)).read_next_batch()
        specs.append(AttachOptionSpec.deserialize(batch))
    return specs


def find_secret_catalog(infos: list[Any]) -> tuple[tuple[str, dict[str, str], str] | None, str]:
    """Pick a catalog with a secret option: ``((name, options, canary), reason)``.

    ``options`` fills every required option, the secret ones with ``canary``.
    """
    by_name = {i.name: i for i in infos}
    secret: tuple[str, dict[str, str], str] | None = None
    secret_reason = "the worker advertises no catalog with a secret=True attach option"
    wanted = os.environ.get("VGI_SDK_SECRET_CATALOG")
    order = [wanted] if wanted else (["ticket_probe"] if "ticket_probe" in by_name else []) + list(by_name)
    for name in order:
        info = by_name.get(name)
        if info is None:
            continue
        specs = _spec_rows(info)
        if not any(s.secret for s in specs):
            continue
        canary = "vgi-sealing-canary-" + uuid.uuid4().hex
        options: dict[str, str] = {}
        ok = True
        for s in specs:
            if s.secret:
                if not pa.types.is_string(s.type) and not pa.types.is_large_string(s.type):
                    ok = False
                    break
                options[s.name] = canary
            elif s.required:
                if not pa.types.is_string(s.type):
                    ok = False
                    break
                options[s.name] = "us-east-1"
        if ok:
            secret = (name, options, canary)
            break
        secret_reason = f"catalog {name!r} has a secret option but a required option that is not a string"

    return secret, secret_reason


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def alice() -> Iterator[Client]:
    """A client authenticated as the first principal."""
    with _client(_BEARER_A) as c:
        yield c


@pytest.fixture(scope="module")
def bob() -> Iterator[Client]:
    """A client authenticated as the second principal."""
    with _client(_BEARER_B) as c:
        yield c


@dataclass
class Targets:
    """The catalogs this worker offers for each kind of case."""

    attach: str | None
    tx: str | None
    secret: tuple[str, dict[str, str], str] | None  # (catalog, options, canary)
    secret_reason: str


@pytest.fixture(scope="module")
def targets(alice: Client) -> Targets:
    """Discover which catalogs to use (see the module docstring)."""
    infos = alice.catalogs()
    by_name = {i.name: i for i in infos}

    secret, secret_reason = find_secret_catalog(infos)

    attach_name = os.environ.get("VGI_SDK_CATALOG")
    tx_name = attach_name
    if attach_name is None:
        names = (["example"] if "example" in by_name else []) + [n for n in by_name if n != "example"]
        for name in names:
            if any(s.required for s in _spec_rows(by_name[name])):
                continue
            try:
                result = _attach(alice, name)
            except CatalogClientError:
                continue
            if not result.attach_opaque_data:
                continue
            attach_name = attach_name or name
            try:
                tx = alice.catalog_transaction_begin(attach_opaque_data=AttachOpaqueData(result.attach_opaque_data))
            except CatalogClientError:
                tx = None
            if tx:
                tx_name = name
                break
    return Targets(attach=attach_name, tx=tx_name, secret=secret, secret_reason=secret_reason)


@pytest.fixture
def attach_name(targets: Targets) -> str:
    """The catalog for the attach cases."""
    if targets.attach is None:
        pytest.skip("no advertised catalog attaches without options and returns attach_opaque_data")
    return targets.attach


@pytest.fixture
def tx_name(targets: Targets) -> str:
    """The catalog for the transaction cases."""
    if targets.tx is None:
        pytest.skip("no advertised catalog begins a transaction")
    return targets.tx


def _alice_attach(alice: Client, name: str) -> bytes:
    value = _attach(alice, name).attach_opaque_data
    assert value, f"catalog {name!r} returned no attach_opaque_data"
    # Sanity: the owner can use it, or every refusal below is vacuous.
    _version(alice, value)
    return bytes(value)


def _alice_tx(alice: Client, name: str) -> tuple[bytes, bytes]:
    attach = _alice_attach(alice, name)
    tx = alice.catalog_transaction_begin(attach_opaque_data=AttachOpaqueData(attach))
    assert tx, f"catalog {name!r} began no transaction"
    _version(alice, attach, tx)
    return attach, bytes(tx)


# ---------------------------------------------------------------------------
# Rule 2: bound to the caller
# ---------------------------------------------------------------------------


def test_attach_replayed_by_another_principal_is_rejected(alice: Client, bob: Client, attach_name: str) -> None:
    """A value issued to one principal does not open for another."""
    value = _alice_attach(alice, attach_name)
    try:
        _rejected(lambda: _version(bob, value), ATTACH_FIELD, "attach replayed by B")
    except pytest.fail.Exception as exc:
        raise AssertionError(str(exc) + _SAME_PRINCIPAL_HINT) from None


def test_transaction_replayed_by_another_principal_is_rejected(alice: Client, bob: Client, tx_name: str) -> None:
    """B, holding a valid attach of its own, cannot use A's transaction value."""
    _, tx = _alice_tx(alice, tx_name)
    bob_attach = bytes(_attach(bob, tx_name).attach_opaque_data)
    _rejected(
        lambda: _version(bob, bob_attach, tx),
        TX_FIELD,
        "A's transaction under B's attach",
    )


# ---------------------------------------------------------------------------
# Rule 1: sealed (tamper evident)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("where", ["first", "middle", "last"])
def test_attach_with_one_flipped_byte_is_rejected(alice: Client, attach_name: str, where: str) -> None:
    """Changing any single byte of an attach value fails the open."""
    value = _alice_attach(alice, attach_name)
    tampered = _flip(value, _flip_positions(value)[where])
    _rejected(lambda: _version(alice, tampered), ATTACH_FIELD, f"attach, {where} byte flipped")


@pytest.mark.parametrize("where", ["first", "middle", "last"])
def test_transaction_with_one_flipped_byte_is_rejected(alice: Client, tx_name: str, where: str) -> None:
    """Changing any single byte of a transaction value fails the open."""
    attach, tx = _alice_tx(alice, tx_name)
    tampered = _flip(tx, _flip_positions(tx)[where])
    _rejected(
        lambda: _version(alice, attach, tampered),
        TX_FIELD,
        f"transaction, {where} byte flipped",
    )


# ---------------------------------------------------------------------------
# Rule 3: a transaction is bound to its parent attach
# ---------------------------------------------------------------------------


def test_transaction_replayed_under_another_attach_is_rejected(alice: Client, tx_name: str) -> None:
    """Same principal, same catalog, different attach: the transaction value must not open."""
    _, tx = _alice_tx(alice, tx_name)
    other_attach = _alice_attach(alice, tx_name)
    _rejected(
        lambda: _version(alice, other_attach, tx),
        TX_FIELD,
        "transaction replayed under another attach",
    )


# ---------------------------------------------------------------------------
# Rule 4: uniform, with no plaintext fallback
# ---------------------------------------------------------------------------


def test_forged_plaintext_attach_is_rejected(alice: Client, attach_name: str) -> None:
    """Values of every unsealed shape are refused like any other unopenable value."""
    _all_rejected(
        {
            label: functools.partial(_version, alice, value)
            for label, value in _plaintext_candidates(attach_name).items()
        },
        ATTACH_FIELD,
    )


def test_forged_plaintext_transaction_is_rejected(alice: Client, tx_name: str) -> None:
    """A forged transaction value under a genuine attach is refused, never taken as plaintext."""
    attach = _alice_attach(alice, tx_name)
    _all_rejected(
        {
            label: functools.partial(_version, alice, attach, value)
            for label, value in _plaintext_candidates(tx_name).items()
        },
        TX_FIELD,
    )


def test_every_failure_mode_gives_the_identical_error(
    alice: Client, bob: Client, attach_name: str, targets: Targets
) -> None:
    """Wrong caller, tampering, wrong parent attach and forgery are indistinguishable on the wire."""
    value = _alice_attach(alice, attach_name)
    attach_cases = {
        "replayed by B": _rejected(lambda: _version(bob, value), ATTACH_FIELD, "replay"),
        "flipped byte": _rejected(lambda: _version(alice, _flip(value, len(value) // 2)), ATTACH_FIELD, "flip"),
        "forged plaintext": _rejected(
            lambda: _version(alice, uuid.uuid4().bytes + attach_name.encode()),
            ATTACH_FIELD,
            "forged",
        ),
    }
    assert len(set(attach_cases.values())) == 1, f"attach failure modes are distinguishable: {attach_cases}"

    if targets.tx is None:
        return
    attach, tx = _alice_tx(alice, targets.tx)
    other_attach = _alice_attach(alice, targets.tx)
    tx_cases = {
        "under another attach": _rejected(
            lambda: _version(alice, other_attach, tx),
            TX_FIELD,
            "other attach",
        ),
        "flipped byte": _rejected(
            lambda: _version(alice, attach, _flip(tx, len(tx) // 2)),
            TX_FIELD,
            "flip",
        ),
        "forged plaintext": _rejected(
            lambda: _version(alice, attach, uuid.uuid4().bytes),
            TX_FIELD,
            "forged",
        ),
    }
    assert len(set(tx_cases.values())) == 1, f"transaction failure modes are distinguishable: {tx_cases}"
    # The two fields differ only in the field name.
    first_attach = next(iter(attach_cases.values()))
    first_tx = next(iter(tx_cases.values()))
    assert first_attach.normalized() == first_tx.normalized(), (
        f"attach and transaction refusals differ beyond the field name: {first_attach} vs {first_tx}"
    )


# ---------------------------------------------------------------------------
# Rule 5: no secret option in plaintext
# ---------------------------------------------------------------------------


def _assert_canary_absent(value: bytes, canary: str, what: str) -> None:
    needle = canary.encode()
    assert needle not in value, f"the secret option appears in plaintext in {what}"
    assert needle.hex() not in value.hex(), f"the secret option's hex appears in {what}'s hex"
    assert base64.b64encode(needle)[:-4] not in value, f"the secret option appears base64-encoded in {what}"


def test_secret_option_never_appears_in_the_value(alice: Client, targets: Targets) -> None:
    """A ``secret=True`` attach option is not readable from ``attach_opaque_data`` (or its transaction)."""
    if targets.secret is None:
        pytest.skip(targets.secret_reason)
    name, options, canary = targets.secret
    result = _attach(alice, name, options)
    value = bytes(result.attach_opaque_data or b"")
    assert value, f"catalog {name!r} returned no attach_opaque_data"
    _assert_canary_absent(value, canary, "attach_opaque_data")
    if result.supports_transactions:
        tx = alice.catalog_transaction_begin(attach_opaque_data=AttachOpaqueData(value))
        if tx:
            _assert_canary_absent(bytes(tx), canary, "transaction_opaque_data")


# ---------------------------------------------------------------------------
# Rule 7: never log raw (optional: needs the worker's log)
# ---------------------------------------------------------------------------


def test_worker_log_carries_no_raw_value(alice: Client, bob: Client, targets: Targets) -> None:
    """After a session with a secret option and a refused replay, the log holds neither value nor secret."""
    log_path = os.environ.get("VGI_SDK_WORKER_LOG")
    if not log_path:
        pytest.skip("set VGI_SDK_WORKER_LOG to the worker's log file to check rule 7")
    raw_values: list[bytes] = []
    canary = None
    if targets.secret is not None:
        name, options, canary = targets.secret
        raw_values.append(bytes(_attach(alice, name, options).attach_opaque_data or b""))
    if targets.attach is not None:
        value = _alice_attach(alice, targets.attach)
        raw_values.append(value)
        # Exercise the refusal path too; whether it is refused is checked above.
        with contextlib.suppress(CatalogClientError):
            _version(bob, value)
        alice.catalog_detach(attach_opaque_data=AttachOpaqueData(value))
    if targets.tx is not None:
        attach, tx = _alice_tx(alice, targets.tx)
        raw_values += [attach, tx]
    time.sleep(1.0)  # let the worker flush
    with open(log_path, encoding="utf-8", errors="replace") as fh:
        log = fh.read()
    if canary is not None:
        assert canary not in log, "the secret attach option appears in the worker log"
    for raw in filter(None, raw_values):
        hexed = raw.hex()
        # A 24-hex-char (12-byte) window of the value is already identifying; a
        # short hash is 12 hex chars of SHA-256 and never matches.
        for start in range(0, max(1, len(hexed) - 24), 8):
            window = hexed[start : start + 24]
            assert not re.search(window, log, re.IGNORECASE), (
                f"the worker log contains raw hex of an opaque value ({window}...)"
            )
        assert base64.b64encode(raw).decode() not in log, "the worker log contains an opaque value in base64"
