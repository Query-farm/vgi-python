# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Private capability context inside the attachment's authenticated envelope.

The MetaWorker routing header wraps this context. Below it, storage and catalog
code still receive the original UUID and provider bytes. The agreement travels
with the attachment, including reconnects and HTTP stream rehydration.
"""

from __future__ import annotations

from vgi.catalog.capabilities import CatalogCapabilities

_MAGIC = b"VGI\x00CAP\x00"
_MAX_BYTES = 65536


def encode(plaintext: bytes, selected: CatalogCapabilities) -> bytes:
    """Wrap UUID/provider bytes with a validated, bounded agreement."""
    selected.validate_selected()
    if not selected.names():
        return plaintext
    payload = selected.serialize_to_bytes()
    if len(payload) > _MAX_BYTES or len(plaintext) < 16:
        raise ValueError("invalid attachment capability context")
    return _MAGIC + len(payload).to_bytes(4, "big") + payload + plaintext


def split(plaintext: bytes) -> tuple[bytes | None, bytes]:
    """Return agreement IPC and framework bytes, rejecting malformed contexts.

    A pre-negotiation attachment has no context and selects no new profiles.
    Only a missing marker is legacy; a corrupt marked context never falls back.
    """
    if not plaintext.startswith(_MAGIC):
        return None, plaintext
    at = len(_MAGIC)
    if len(plaintext) < at + 4:
        raise ValueError("truncated attachment capability context")
    size = int.from_bytes(plaintext[at : at + 4], "big")
    at += 4
    if not 0 < size <= _MAX_BYTES or len(plaintext) < at + size + 16:
        raise ValueError("invalid attachment capability context length")
    return plaintext[at : at + size], plaintext[at + size :]


def agreement(plaintext: bytes) -> CatalogCapabilities:
    """Decode the selection from an already authenticated attachment."""
    payload, _ = split(plaintext)
    selected = CatalogCapabilities() if payload is None else CatalogCapabilities.deserialize_from_bytes(payload)
    selected.validate_selected()
    return selected
