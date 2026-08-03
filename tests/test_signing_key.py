# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for shared signing-key resolution (``vgi.serve``).

The signing key seals HTTP state tokens. Every process that might serve a
continuation has to hold the same one, and when they do not the failure is
load-dependent: a pinned connection works, a reconnect mid-stream does not.
These tests pin the resolution rules so that cannot regress into silence.
"""

from __future__ import annotations

import os

import pytest

from vgi.serve import (
    SIGNING_KEY_ENV,
    _warn_if_ephemeral_signing_key,
    resolve_shared_signing_key,
)


def test_configured_key_is_used_verbatim(monkeypatch: pytest.MonkeyPatch) -> None:
    """An operator-set key wins and is reported as not ephemeral."""
    monkeypatch.setenv(SIGNING_KEY_ENV, "a-shared-secret")
    key, ephemeral = resolve_shared_signing_key(propagate_to_children=False)
    assert key == b"a-shared-secret"
    assert ephemeral is False


def test_unset_key_is_minted_and_flagged(monkeypatch: pytest.MonkeyPatch) -> None:
    """With nothing configured we mint one and say so."""
    monkeypatch.delenv(SIGNING_KEY_ENV, raising=False)
    key, ephemeral = resolve_shared_signing_key(propagate_to_children=False)
    assert len(key) >= 32
    assert ephemeral is True
    # Not exported: this process starts no children that would need it.
    assert SIGNING_KEY_ENV not in os.environ


def test_minted_key_is_exported_for_children(monkeypatch: pytest.MonkeyPatch) -> None:
    """A pre-fork parent exports the key so its workers inherit one value.

    This is the whole point: without it each forked worker imports the app
    and mints its own, and tokens stop being portable between them.
    """
    monkeypatch.delenv(SIGNING_KEY_ENV, raising=False)
    key, ephemeral = resolve_shared_signing_key(propagate_to_children=True)
    assert ephemeral is True
    exported = os.environ.get(SIGNING_KEY_ENV)
    assert exported is not None
    assert exported.encode() == key

    # A child re-resolving from the inherited environment gets the same key.
    child_key, child_ephemeral = resolve_shared_signing_key(propagate_to_children=True)
    assert child_key == key
    assert child_ephemeral is False


def test_independent_processes_disagree_without_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two unrelated processes mint different keys -- the bug being prevented.

    Documents *why* propagation exists: absent a configured key and absent
    export, two resolutions produce keys that cannot open each other's
    tokens.
    """
    from vgi_rpc import crypto

    monkeypatch.delenv(SIGNING_KEY_ENV, raising=False)
    key_a, _ = resolve_shared_signing_key(propagate_to_children=False)
    key_b, _ = resolve_shared_signing_key(propagate_to_children=False)
    assert key_a != key_b

    sealed = crypto.seal_bytes(b"stream-cursor", key_a, aad=b"aad", version=5)
    assert crypto.open_bytes(sealed, key_a, aad=b"aad", version=5) == b"stream-cursor"
    with pytest.raises(crypto.SealError):
        crypto.open_bytes(sealed, key_b, aad=b"aad", version=5)


def test_ephemeral_warning_is_loud_for_single_process(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A per-process key warns, naming the env var and the symptom."""
    with caplog.at_level("INFO", logger="vgi.serve"):
        _warn_if_ephemeral_signing_key(is_ephemeral=True, multiprocess=False)
    assert any(r.levelname == "WARNING" for r in caplog.records)
    assert SIGNING_KEY_ENV in caplog.text


def test_configured_key_does_not_warn(caplog: pytest.LogCaptureFixture) -> None:
    """Nothing to say when the operator configured a key."""
    with caplog.at_level("INFO", logger="vgi.serve"):
        _warn_if_ephemeral_signing_key(is_ephemeral=False, multiprocess=False)
    assert not caplog.records
