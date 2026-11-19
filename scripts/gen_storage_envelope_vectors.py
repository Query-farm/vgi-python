# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Regenerate ``vgi/_test_fixtures/storage_envelope_v1.json``.

Language-neutral test vectors for the function-storage value envelope and the
blob-vendor scope names (`vgi.function_storage_blobs`), so another SDK can be
byte-compatible. Every input is fixed, so the output is byte-identical on
every run; ``tests/test_storage_envelope_vectors.py`` fails if the file drifts
from what this script produces. The vgi extension carries a verbatim copy at
``test/vectors/storage_envelope_v1.json``. Run::

    uv run python scripts/gen_storage_envelope_vectors.py
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from vgi_rpc.vendor import BlobRef

from vgi.function_storage_blobs import (
    _OBJECT_ID_RE,
    _POINTER_FIXED,
    _SHARD_RE,
    DEFAULT_EXTERNALIZE_BYTES,
    TAG_INLINE,
    TAG_POINTER,
    _blob_execution_id,
    _blob_shard,
    encode_inline,
    encode_pointer,
)

OUT = Path(__file__).resolve().parent.parent / "vgi" / "_test_fixtures" / "storage_envelope_v1.json"

_INLINE: list[tuple[str, bytes]] = [
    ("empty value", b""),
    ("ascii text", b"hello"),
    ("value starting with the pointer tag byte", b"\x01\x02\x03"),
    ("value starting with the inline tag byte", b"\x00\x00"),
    ("all byte values", bytes(range(256))),
]

_POINTERS: list[tuple[str, str, bytes]] = [
    ("32-hex object id (the vendor's)", "0123456789abcdef0123456789abcdef", b"x" * 4096),
    ("object id using every allowed punctuation", "A-z_0.9:obj", b"payload"),
    ("one-character object id, empty blob", "a", b""),
    ("128-character object id (the maximum)", "o" * 128, b"y" * 1_000_000),
]


def _pointer_case(name: str, object_id: str, blob: bytes) -> dict[str, Any]:
    ref = BlobRef(object_id=object_id, size=len(blob), sha256=hashlib.sha256(blob).hexdigest())
    return {
        "name": name,
        "object_id": object_id,
        "size": ref.size,
        "sha256": ref.sha256,
        "envelope_hex": encode_pointer(ref).hex(),
    }


def _malformed() -> list[dict[str, str]]:
    good = encode_pointer(BlobRef(object_id="abc", size=5, sha256="00" * 32))
    fixed = good[:_POINTER_FIXED]
    return [
        {"name": "empty stored value", "envelope_hex": "", "error": "empty"},
        {"name": "unknown tag 0x02", "envelope_hex": "02616263", "error": "unknown_tag"},
        {"name": "unknown tag 0xff", "envelope_hex": "ff", "error": "unknown_tag"},
        {"name": "pointer tag only", "envelope_hex": "01", "error": "malformed_pointer"},
        {"name": "pointer truncated inside the sha256", "envelope_hex": good[:20].hex(), "error": "malformed_pointer"},
        {"name": "pointer with an empty object id", "envelope_hex": fixed.hex(), "error": "malformed_pointer"},
        {
            "name": "pointer object id with a disallowed character (/)",
            "envelope_hex": (fixed + b"a/b").hex(),
            "error": "malformed_pointer",
        },
        {
            "name": "pointer object id with a space",
            "envelope_hex": (fixed + b"a b").hex(),
            "error": "malformed_pointer",
        },
        {
            "name": "pointer object id of 129 characters",
            "envelope_hex": (fixed + b"o" * 129).hex(),
            "error": "malformed_pointer",
        },
    ]


def _scopes() -> list[dict[str, str]]:
    cases = [
        ("q", b"\x00" * 16),
        ("s", bytes(range(16))),
        ("s", b""),
        ("q", b"\xab" * 63),  # 2 + 126 hex chars = 128: kept
        ("s", b"\xab" * 64),  # 2 + 128 hex chars = 130: hashed
    ]
    return [{"kind": k, "ident_hex": ident.hex(), "execution_id": _blob_execution_id(k, ident)} for k, ident in cases]


def _shards() -> list[dict[str, str]]:
    keys = ["att-" + "ab" * 16, "", "loc-anon", "has space", "a/b", "x" * 128, "x" * 129, "colon:not-allowed"]
    return [{"shard_key": key, "shard": _blob_shard(key)} for key in keys]


def build() -> dict[str, Any]:
    """Return the vectors document."""
    return {
        "format": "vgi.storage_envelope.v1",
        "description": (
            "Function-storage value envelope: every stored value is 0x00||raw (inline) or "
            "0x01||u64be(size)||sha256(32 bytes)||object_id (pointer to a blob-vendor object, purpose fs). "
            "Hex strings are lowercase. See docs/testing/blob-test-catalog.md in the vgi extension."
        ),
        "tag_inline": TAG_INLINE,
        "tag_pointer": TAG_POINTER,
        "pointer_fixed_bytes": _POINTER_FIXED,
        "object_id_regex": _OBJECT_ID_RE.pattern.decode(),
        "default_externalize_bytes": DEFAULT_EXTERNALIZE_BYTES,
        "externalize_rule": "with a vendor, a value of len >= VGI_STORAGE_EXTERNALIZE_BYTES is a pointer, else inline",
        "inline": [
            {"name": name, "value_hex": raw.hex(), "envelope_hex": encode_inline(raw).hex()} for name, raw in _INLINE
        ],
        "pointers": [_pointer_case(*case) for case in _POINTERS],
        "malformed": _malformed(),
        "scope_execution_ids": {
            "rule": (
                "'<kind>.<hex ident>' where kind is q (queue, ident = execution id) or s (state/log, "
                "ident = scope id); if longer than 128 characters, '<kind>.h<sha256(ident) hex>'"
            ),
            "cases": _scopes(),
        },
        "scope_shards": {
            "rule": (
                "the storage shard key; empty becomes 'loc-anon'; a key not matching "
                f"{_SHARD_RE.pattern} becomes 'h-<sha256(utf8 key) hex>'"
            ),
            "regex": _SHARD_RE.pattern,
            "cases": _shards(),
        },
    }


def render() -> str:
    """Return the vectors as the exact file contents."""
    return json.dumps(build(), indent=2) + "\n"


def main() -> None:
    """Write the vectors file."""
    OUT.write_text(render())
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
