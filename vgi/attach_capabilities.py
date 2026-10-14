# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Private capability context inside the attachment's authenticated envelope.

The MetaWorker routing header wraps this context. Below it, storage and catalog
code still receive the original UUID and provider bytes. The agreement travels
with the attachment, including reconnects and HTTP stream rehydration.
"""

from __future__ import annotations

from dataclasses import dataclass

from vgi_rpc import ArrowSerializableDataclass

from vgi.catalog.capabilities import CatalogCapabilities
from vgi.catalog.query import ClientQueryCapabilities, QueryCapabilities
from vgi.typed_values import TypedValueBatch

_MAGIC = b"VGI\x00CAP\x00"
_QUERY_MAGIC = b"VGI\x00CAPQ\x00"
_MAX_BYTES = 65536


@dataclass(frozen=True, kw_only=True)
class _QueryAttachment(ArrowSerializableDataclass):
    catalog: CatalogCapabilities
    offer: ClientQueryCapabilities
    selected: QueryCapabilities


def encode(
    plaintext: bytes,
    selected: CatalogCapabilities,
    *,
    query_offer: ClientQueryCapabilities | None = None,
    query_selected: QueryCapabilities | None = None,
) -> bytes:
    """Wrap UUID/provider bytes with a validated, bounded agreement."""
    selected.validate_selected()
    if query_selected is not None:
        if query_offer is None or query_selected.select(query_offer, supports_transactions=True) != query_selected:
            raise ValueError("query agreement was not selected from the client offer")
        payload = _QueryAttachment(catalog=selected, offer=query_offer, selected=query_selected).serialize_to_bytes()
        magic = _QUERY_MAGIC
    elif selected.names():
        payload = selected.serialize_to_bytes()
        magic = _MAGIC
    else:
        return plaintext
    if len(payload) > _MAX_BYTES or len(plaintext) < 16:
        raise ValueError("invalid attachment capability context")
    return magic + len(payload).to_bytes(4, "big") + payload + plaintext


def split(plaintext: bytes) -> tuple[bytes | None, bytes]:
    """Return agreement IPC and framework bytes, rejecting malformed contexts.

    A pre-negotiation attachment has no context and selects no new profiles.
    Only a missing marker is legacy; a corrupt marked context never falls back.
    """
    magic = _QUERY_MAGIC if plaintext.startswith(_QUERY_MAGIC) else _MAGIC
    if not plaintext.startswith(magic):
        return None, plaintext
    at = len(magic)
    if len(plaintext) < at + 4:
        raise ValueError("truncated attachment capability context")
    size = int.from_bytes(plaintext[at : at + 4], "big")
    at += 4
    if not 0 < size <= _MAX_BYTES or len(plaintext) < at + size + 16:
        raise ValueError("invalid attachment capability context length")
    return plaintext[at : at + size], plaintext[at + size :]


def agreement(plaintext: bytes) -> CatalogCapabilities:
    """Decode the selection from an already authenticated attachment."""
    if plaintext.startswith(_QUERY_MAGIC):
        return _query_attachment(plaintext).catalog
    payload, _ = split(plaintext)
    selected = CatalogCapabilities() if payload is None else CatalogCapabilities.deserialize_from_bytes(payload)
    selected.validate_selected()
    return selected


def _query_attachment(plaintext: bytes) -> _QueryAttachment:
    payload, _ = split(plaintext)
    if payload is None:
        raise ValueError("missing query attachment context")
    batch = TypedValueBatch.deserialize_from_bytes(payload).batch
    if not batch.schema.equals(_QueryAttachment.ARROW_SCHEMA, check_metadata=False):
        raise ValueError("invalid query attachment context schema")
    value = _QueryAttachment.deserialize_from_batch(batch)
    value.catalog.validate_selected()
    if value.selected.select(value.offer, supports_transactions=True) != value.selected:
        raise ValueError("invalid selected query agreement")
    return value


def query_agreement(plaintext: bytes) -> tuple[ClientQueryCapabilities, QueryCapabilities] | None:
    """Recover the immutable query offer/selection from authenticated context."""
    if not plaintext.startswith(_QUERY_MAGIC):
        return None
    value = _query_attachment(plaintext)
    return value.offer, value.selected
