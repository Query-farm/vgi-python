# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for AEAD identity-binding of catalog opaque-data envelopes.

``attach_opaque_data`` and ``transaction_opaque_data`` are implementation-chosen
byte strings the client round-trips back to the worker. On HTTP transport (a
worker with a signing key, authenticating many principals) the worker seals
each value in an AEAD envelope whose AAD binds the caller's ``(domain,
principal)``; the transaction envelope additionally binds its parent attach
envelope. A value sealed for one principal — or one attach — cannot be opened
by another.

These tests drive ``Worker``'s catalog methods directly, simulating the
per-request auth context via the ``_current_transport`` contextvar that the
HTTP transport would normally set.
"""

from __future__ import annotations

import contextlib
import logging
import os
from collections.abc import Iterator

import pytest
from vgi_rpc.rpc import AuthContext
from vgi_rpc.rpc._common import _current_transport, _TransportContext

from vgi._test_fixtures.worker import ExampleWorker
from vgi.protocol import CatalogAttachRequest


@contextlib.contextmanager
def as_principal(domain: str | None, principal: str | None) -> Iterator[None]:
    """Simulate the transport-set auth context for a single request."""
    authed = principal is not None
    auth = AuthContext(domain=domain, authenticated=authed, principal=principal)
    token = _current_transport.set(_TransportContext(auth=auth))
    try:
        yield
    finally:
        _current_transport.reset(token)


@contextlib.contextmanager
def as_anonymous() -> Iterator[None]:
    """Simulate an unauthenticated request."""
    token = _current_transport.set(_TransportContext(auth=AuthContext(domain=None, authenticated=False)))
    try:
        yield
    finally:
        _current_transport.reset(token)


def _attach_request() -> CatalogAttachRequest:
    return CatalogAttachRequest(name="example", options=None, data_version_spec=None, implementation_version=None)


@pytest.fixture
def signing_key() -> bytes:
    """Return a stable 32-byte signing key for an HTTP-mode worker."""
    return os.urandom(32)


@pytest.fixture
def http_worker(signing_key: bytes) -> ExampleWorker:
    """Build a worker configured as it would be on HTTP transport (sealing enabled)."""
    w = ExampleWorker(quiet=True)
    w._signing_key = signing_key
    return w


# ---------------------------------------------------------------------------
# attach_opaque_data
# ---------------------------------------------------------------------------


def test_same_principal_round_trips(http_worker: ExampleWorker) -> None:
    """A principal can attach and then use the envelope it was issued."""
    with as_principal("test", "alice"):
        result = http_worker.catalog_attach(_attach_request())
        envelope = result.attach_opaque_data
        # The envelope is sealed: longer than the plaintext, and not the plaintext.
        assert envelope is not None
        assert len(envelope) > 17
        # Alice can use it for follow-up calls.
        http_worker.catalog_schemas(envelope)
        http_worker.catalog_version(envelope)
        http_worker.catalog_detach(envelope)


def test_different_principal_rejected(http_worker: ExampleWorker) -> None:
    """A different principal cannot open another principal's attach envelope."""
    with as_principal("test", "alice"):
        envelope = http_worker.catalog_attach(_attach_request()).attach_opaque_data
    with as_principal("test", "bob"):
        for call in (
            lambda: http_worker.catalog_schemas(envelope),
            lambda: http_worker.catalog_version(envelope),
            lambda: http_worker.catalog_detach(envelope),
        ):
            with pytest.raises(ValueError, match="attach_opaque_data not recognized"):
                call()


def test_different_domain_rejected(http_worker: ExampleWorker) -> None:
    """The AAD binds the auth domain, not just the principal string."""
    with as_principal("idp-a", "alice"):
        envelope = http_worker.catalog_attach(_attach_request()).attach_opaque_data
    with as_principal("idp-b", "alice"), pytest.raises(ValueError, match="not recognized"):
        http_worker.catalog_schemas(envelope)


def test_anonymous_cannot_open_authenticated_envelope(http_worker: ExampleWorker) -> None:
    """An envelope sealed for a real principal is not openable anonymously."""
    with as_principal("test", "alice"):
        envelope = http_worker.catalog_attach(_attach_request()).attach_opaque_data
    with as_anonymous(), pytest.raises(ValueError, match="not recognized"):
        http_worker.catalog_schemas(envelope)


def test_detach_by_wrong_principal_leaves_attach_usable(http_worker: ExampleWorker) -> None:
    """A failed cross-principal detach must not disturb the real owner's attach."""
    with as_principal("test", "alice"):
        envelope = http_worker.catalog_attach(_attach_request()).attach_opaque_data
    with as_principal("test", "mallory"), pytest.raises(ValueError, match="not recognized"):
        http_worker.catalog_detach(envelope)
    # Alice's attach is untouched.
    with as_principal("test", "alice"):
        http_worker.catalog_schemas(envelope)


def test_tampered_attach_envelope_rejected(http_worker: ExampleWorker) -> None:
    """Flipping any byte of the envelope fails the AEAD tag check."""
    with as_principal("test", "alice"):
        envelope = http_worker.catalog_attach(_attach_request()).attach_opaque_data
        assert envelope is not None
        tampered = bytearray(envelope)
        tampered[-1] ^= 0x01
        with pytest.raises(ValueError, match="not recognized"):
            http_worker.catalog_schemas(bytes(tampered))


def test_garbage_attach_envelope_rejected(http_worker: ExampleWorker) -> None:
    """A value that was never a valid envelope is rejected like any other."""
    with as_principal("test", "alice"), pytest.raises(ValueError, match="not recognized"):
        http_worker.catalog_schemas(b"not an envelope at all")


# ---------------------------------------------------------------------------
# transaction_opaque_data
# ---------------------------------------------------------------------------


def test_transaction_round_trips(http_worker: ExampleWorker) -> None:
    """A transaction envelope opens under the principal + attach it was minted for."""
    with as_principal("test", "alice"):
        attach = http_worker.catalog_attach(_attach_request()).attach_opaque_data
        tx = http_worker.catalog_transaction_begin(attach).transaction_opaque_data
        assert tx is not None
        assert len(tx) > 16  # sealed, longer than the 16-byte uuid plaintext
        # catalog_schemas unwraps the transaction envelope (AAD = principal + attach).
        http_worker.catalog_schemas(attach, tx)


def test_transaction_rejected_for_different_principal(http_worker: ExampleWorker) -> None:
    """A transaction envelope cannot be used by a different principal."""
    with as_principal("test", "alice"):
        attach = http_worker.catalog_attach(_attach_request()).attach_opaque_data
        tx = http_worker.catalog_transaction_begin(attach).transaction_opaque_data
    # bob would also fail the attach unwrap first; force past that by giving bob
    # his own attach and only swapping in alice's transaction envelope.
    with as_principal("test", "bob"):
        bob_attach = http_worker.catalog_attach(_attach_request()).attach_opaque_data
        with pytest.raises(ValueError, match="transaction_opaque_data not recognized"):
            http_worker.catalog_schemas(bob_attach, tx)


def test_transaction_rejected_against_different_attach(http_worker: ExampleWorker) -> None:
    """A transaction envelope is bound to its parent attach — same principal, wrong attach fails."""
    with as_principal("test", "alice"):
        attach_a = http_worker.catalog_attach(_attach_request()).attach_opaque_data
        attach_b = http_worker.catalog_attach(_attach_request()).attach_opaque_data
        tx_under_a = http_worker.catalog_transaction_begin(attach_a).transaction_opaque_data
        # Same principal, but presenting the transaction under a different attach.
        with pytest.raises(ValueError, match="transaction_opaque_data not recognized"):
            http_worker.catalog_schemas(attach_b, tx_under_a)
        # Under its real parent attach it still works.
        http_worker.catalog_schemas(attach_a, tx_under_a)


def test_tampered_transaction_envelope_rejected(http_worker: ExampleWorker) -> None:
    """Flipping a byte of the transaction envelope fails the tag check."""
    with as_principal("test", "alice"):
        attach = http_worker.catalog_attach(_attach_request()).attach_opaque_data
        tx = http_worker.catalog_transaction_begin(attach).transaction_opaque_data
        assert tx is not None
        tampered = bytearray(tx)
        tampered[-1] ^= 0x01
        with pytest.raises(ValueError, match="transaction_opaque_data not recognized"):
            http_worker.catalog_schemas(attach, bytes(tampered))


# ---------------------------------------------------------------------------
# transport gating + error parity + redaction + key lifecycle
# ---------------------------------------------------------------------------


def test_subprocess_worker_passes_through() -> None:
    """With no signing key (subprocess/unix transport) values are not sealed."""
    w = ExampleWorker(quiet=True)  # _signing_key stays None
    assert w._signing_key is None
    with as_principal("test", "alice"):
        result = w.catalog_attach(_attach_request())
        envelope = result.attach_opaque_data
        # No key => no sealing, but the framework still mints a 16-byte shard UUID
        # and prepends it: the value is uuid(16) || the implementation's plaintext.
        assert envelope[16:] == b"readonly-catalog-"
        assert len(envelope) == 16 + len(b"readonly-catalog-")
        # And any "principal" can use the minted attach — no binding without a key.
    with as_principal("test", "bob"):
        w.catalog_schemas(envelope)


def test_error_parity_across_failure_modes(http_worker: ExampleWorker) -> None:
    """Wrong principal, tampered, and garbage all raise the identical error."""
    with as_principal("test", "alice"):
        envelope = http_worker.catalog_attach(_attach_request()).attach_opaque_data
        assert envelope is not None

    messages: set[str] = set()
    # wrong principal
    with as_principal("test", "bob"):
        with pytest.raises(ValueError) as e1:
            http_worker.catalog_schemas(envelope)
        messages.add(str(e1.value))
    # tampered
    with as_principal("test", "alice"):
        tampered = bytearray(envelope)
        tampered[-1] ^= 0x01
        with pytest.raises(ValueError) as e2:
            http_worker.catalog_schemas(bytes(tampered))
        messages.add(str(e2.value))
        # garbage
        with pytest.raises(ValueError) as e3:
            http_worker.catalog_schemas(b"garbage")
        messages.add(str(e3.value))
    # All three failure modes are indistinguishable by message.
    assert messages == {"attach_opaque_data not recognized"}


def test_log_lifecycle_redacts_opaque_data(http_worker: ExampleWorker) -> None:
    """_log_catalog_lifecycle never emits a raw opaque value — only a short hash."""
    import hashlib

    records: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = _Capture()
    lg = logging.getLogger("vgi.worker")
    lg.addHandler(handler)
    lg.setLevel(logging.INFO)
    try:
        with as_principal("test", "alice"):
            result = http_worker.catalog_attach(_attach_request())
            tx = http_worker.catalog_transaction_begin(result.attach_opaque_data).transaction_opaque_data
    finally:
        lg.removeHandler(handler)

    envelope = result.attach_opaque_data
    assert envelope is not None and tx is not None
    attach_event = next(r for r in records if r.msg == "catalog.attach")
    begin_event = next(r for r in records if r.msg == "catalog.transaction.begin")

    # The log record carries the 12-char SHA-256 prefix, never the raw value.
    expected_attach_hash = hashlib.sha256(envelope.hex().encode()).hexdigest()[:12]
    assert attach_event.attach_opaque_data == expected_attach_hash  # type: ignore[attr-defined]
    assert envelope.hex() not in str(vars(attach_event))

    expected_tx_hash = hashlib.sha256(tx.hex().encode()).hexdigest()[:12]
    assert begin_event.transaction_opaque_data == expected_tx_hash  # type: ignore[attr-defined]
    assert tx.hex() not in str(vars(begin_event))


def test_envelope_survives_worker_restart_with_stable_key(signing_key: bytes) -> None:
    """A stable signing key lets an envelope outlive the worker that minted it."""
    w1 = ExampleWorker(quiet=True)
    w1._signing_key = signing_key
    with as_principal("test", "alice"):
        envelope = w1.catalog_attach(_attach_request()).attach_opaque_data

    # Simulate a worker restart: a brand-new instance, same operator key.
    w2 = ExampleWorker(quiet=True)
    w2._signing_key = signing_key
    with as_principal("test", "alice"):
        w2.catalog_schemas(envelope)  # opens fine — bound to identity, not to the process

    # A worker with a *different* key cannot open it.
    w3 = ExampleWorker(quiet=True)
    w3._signing_key = os.urandom(32)
    with as_principal("test", "alice"), pytest.raises(ValueError, match="not recognized"):
        w3.catalog_schemas(envelope)
