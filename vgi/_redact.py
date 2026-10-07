# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The one way an opaque value may appear in a log, trace or error report.

``attach_opaque_data`` and ``transaction_opaque_data`` (and anything else a
client stores and hands back) may carry credentials, so their raw bytes, full
hex, and raw-hex *prefixes* never reach a sink. What does is the first 12 hex
characters of SHA-256 over the value's lowercase hex text
(docs/protocol/vgi-opaque-data-sealing.md, rule 7).

Hashing the hex text rather than the bytes matches ``vgi_rpc.sentry.short_hash``,
so a value redacted here correlates with the tag vgi-rpc's dispatch hook puts on
the same request. It is defined here instead of imported because
``vgi_rpc.sentry`` pulls in ``sentry_sdk``, an optional extra, and this module
has to stay importable from the lightest entry points (the transactor).
"""

from __future__ import annotations

import hashlib

SHORT_HASH_LEN = 12


def short_hash(value: bytes | str | None, *, length: int = SHORT_HASH_LEN) -> str | None:
    """Return a stable hex prefix of ``sha256(hex(value))``, never the value itself.

    ``bytes`` are normalised to lowercase hex before hashing, so
    ``short_hash(b)`` equals ``short_hash(b.hex())``. ``None`` passes through.
    """
    if value is None:
        return None
    if isinstance(value, bytes | bytearray | memoryview):
        value = bytes(value).hex()
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:length]
