# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Routing header that [`MetaWorker`][] encapsulates inside an attach envelope.

When several [`Worker`][] instances are composed into one process, a call that
carries an ``attach_opaque_data`` has to be routed back to the sub-worker whose
catalog vended it. Nothing in the request identifies the catalog:
``BindRequest`` carries only ``schema_name``, and the catalog's own opaque bytes
are implementation-defined — the built-in read-only catalog returns the class
constant ``b"readonly-catalog-"`` for *every* catalog it serves, so two catalogs
in one process are byte-identical there.

So ``MetaWorker`` records the catalog name itself. At ``catalog_attach`` it opens
the sub-worker's sealed attach, prepends this header to the plaintext, and
re-seals:

    sealed( VGI\0MWC\0 || len(1) || name || uuid(16) || catalog_bytes )

Two properties matter:

* **Inside the seal.** The signing key is process-wide, so a header riding
  *outside* the AEAD could be edited by a client to route an attach minted for
  one catalog into another — every sub-worker's key would open it. Sealing the
  name makes forging it equivalent to forging the envelope.
* **Survives serialization.** The header is part of the plaintext, so it rides
  along in whatever the sub-worker persists into an HTTP stream state. A
  rehydrate landing on a different instance re-reads the name and routes
  correctly — no process-local map, and no dependence on a positional index that
  need not agree across deploys.

``Worker`` strips the header when opening an envelope, so every existing consumer
keeps seeing the framework plaintext (``uuid(16) || catalog_bytes``) unchanged.
"""

from __future__ import annotations

# 8 bytes: long enough that a random uuid4 prefix colliding with it is not worth
# reasoning about (a 6-byte marker would already be ~2^-48 per attach).
_MAGIC = b"VGI\x00MWC\x00"
_LEN_WIDTH = 1
_MAX_NAME_BYTES = (1 << (8 * _LEN_WIDTH)) - 1


def encode(catalog_name: str, plaintext: bytes) -> bytes:
    """Prepend the routing header for ``catalog_name`` to an attach plaintext.

    ``plaintext`` is the framework form the sub-worker minted
    (``uuid(16) || catalog_bytes``); the result is meant to be sealed.
    """
    name = catalog_name.encode("utf-8")
    if len(name) > _MAX_NAME_BYTES:
        msg = f"catalog name too long to encode in an attach header: {len(name)} > {_MAX_NAME_BYTES} bytes"
        raise ValueError(msg)
    return _MAGIC + len(name).to_bytes(_LEN_WIDTH, "big") + name + plaintext


def split(plaintext: bytes | None) -> tuple[str | None, bytes | None]:
    """Split an opened attach plaintext into ``(catalog_name, inner_plaintext)``.

    Returns ``(None, plaintext)`` unchanged when no header is present — the
    single-``Worker`` case, and any attach minted before this header existed.
    Malformed headers are treated the same way rather than raising: the value
    still has to satisfy the AEAD, so a truncated header means a framework bug
    on the mint side, and failing the *call* with a confusing error is worse than
    falling back to the un-routed path (which raises a precise error of its own
    when the function name turns out to be ambiguous).
    """
    if not plaintext or not plaintext.startswith(_MAGIC):
        return None, plaintext
    at = len(_MAGIC)
    if len(plaintext) < at + _LEN_WIDTH:
        return None, plaintext
    name_len = int.from_bytes(plaintext[at : at + _LEN_WIDTH], "big")
    at += _LEN_WIDTH
    if len(plaintext) < at + name_len:
        return None, plaintext
    try:
        name = plaintext[at : at + name_len].decode("utf-8")
    except UnicodeDecodeError:
        return None, plaintext
    return name, plaintext[at + name_len :]
