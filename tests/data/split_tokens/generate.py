"""Regenerate the cross-SDK split-token byte fixtures.

    uv run python tests/data/split_tokens/generate.py

Every SDK that implements the envelope must *parse* all of these and reach the same
verdict. The **unsealed** vectors must additionally be reproducible byte-for-byte —
that is the part where five independent implementations can silently disagree (on
``anchor_len`` endianness, or on fingerprint truncation) while each stays
self-consistent, which no behavioural test would catch.

Sealed vectors are parse-only by construction: ``seal_bytes`` draws a fresh random
nonce, so the bytes are not reproducible. They still pin the *layout* and the
alg:none refusal.
"""

from __future__ import annotations

import json
import pathlib

from vgi.split_token import _HEADER_STRUCT, build_split_token

HERE = pathlib.Path(__file__).parent

# Fixed inputs so the unsealed vectors are byte-reproducible in any language.
KEY = bytes(range(32))
FINGERPRINT = bytes(range(16))
ANCHOR = (47).to_bytes(8, "little")
PAYLOAD = b"file=3;v=47"


def main() -> None:
    """Write every vector plus the manifest that records its expected verdict."""
    cases: list[dict[str, object]] = []

    def emit(
        name: str,
        raw: bytes,
        *,
        verdict: str,
        note: str,
        reproducible: bool,
        worker_keyed: bool = False,
    ) -> None:
        """Record one vector plus the redemption context its verdict assumes.

        ``worker_keyed`` is not decoration: the alg:none vector is a structurally
        *valid* unsealed token whose whole point is that a KEYED worker must refuse
        it, so a consumer that guessed the key state from the token would test the
        opposite of the rule. Stating it here keeps all five SDKs redeeming each
        vector under the same premise rather than each inventing one.
        """
        (HERE / f"{name}.bin").write_bytes(raw)
        cases.append(
            {
                "name": name,
                "verdict": verdict,
                "note": note,
                "reproducible": reproducible,
                "worker_keyed": worker_keyed,
                "size": len(raw),
            }
        )

    emit(
        "valid_unsealed",
        build_split_token(payload=PAYLOAD, fingerprint=FINGERPRINT, anchor=ANCHOR),
        verdict="ok",
        note="Keyless worker (subprocess/unix - DuckDB's primary path). Payload is plaintext.",
        reproducible=True,
    )
    emit(
        "valid_sealed",
        build_split_token(payload=PAYLOAD, fingerprint=FINGERPRINT, anchor=ANCHOR, signing_key=KEY),
        verdict="ok",
        note="Keyed worker. Parse-only: the AEAD nonce is random, so bytes differ per run.",
        reproducible=False,
        worker_keyed=True,
    )
    emit(
        "bad_flags_unsealed_but_key_present",
        build_split_token(payload=b"OTHER TENANT DATA", fingerprint=FINGERPRINT, anchor=ANCHOR),
        verdict="SPLIT_TOKEN_INVALID",
        note=(
            "THE alg:none CASE. Structurally a valid unsealed token. A worker that holds a "
            "signing key MUST refuse it - flags is attacker-controlled, so the key state "
            "decides whether to unseal, never the token."
        ),
        reproducible=True,
        worker_keyed=True,
    )
    emit(
        "bad_fingerprint",
        build_split_token(payload=PAYLOAD, fingerprint=b"\xee" * 16, anchor=ANCHOR),
        verdict="SPLIT_TOKEN_INVALID",
        note="Minted for a different bind.",
        reproducible=True,
    )
    emit(
        "stale_anchor",
        build_split_token(payload=PAYLOAD, fingerprint=FINGERPRINT, anchor=(1).to_bytes(8, "little")),
        verdict="SPLIT_SNAPSHOT_EXPIRED",
        note=(
            "Anchor names a catalog version that is gone. Distinct kind from INVALID: "
            "only this one means 're-run the query'."
        ),
        reproducible=True,
    )
    emit(
        "truncated",
        build_split_token(payload=PAYLOAD, fingerprint=FINGERPRINT, anchor=ANCHOR)[:10],
        verdict="SPLIT_TOKEN_INVALID",
        note="Shorter than the 20-byte fixed header.",
        reproducible=True,
    )
    emit(
        "reserved_flag_bit",
        _HEADER_STRUCT.pack(1, 0x02, len(ANCHOR)) + FINGERPRINT + ANCHOR + PAYLOAD,
        verdict="SPLIT_TOKEN_INVALID",
        note="bits 1-7 of flags are reserved and MUST be zero.",
        reproducible=True,
    )
    emit(
        "bad_version",
        _HEADER_STRUCT.pack(9, 0, len(ANCHOR)) + FINGERPRINT + ANCHOR + PAYLOAD,
        verdict="SPLIT_TOKEN_INVALID",
        note="format_version is checked unconditionally, before anything else.",
        reproducible=True,
    )
    emit(
        "anchor_len_overrun",
        _HEADER_STRUCT.pack(1, 0, 9999) + FINGERPRINT + ANCHOR + PAYLOAD,
        verdict="SPLIT_TOKEN_INVALID",
        note="anchor_len claims more bytes than the token holds.",
        reproducible=True,
    )

    manifest = {
        "format_version": 1,
        "layout": (
            "format_version:u8 | flags:u8 | anchor_len:u16le | bind_fingerprint:16 | "
            "anchor:anchor_len | payload:rest"
        ),
        # Every vector is opened with expected_fingerprint=fingerprint_hex and
        # current_anchor=anchor_hex; stale_anchor is the one that carries a
        # different anchor inside, which is what makes its verdict distinct.
        "key_hex": KEY.hex(),
        "fingerprint_hex": FINGERPRINT.hex(),
        "anchor_hex": ANCHOR.hex(),
        "payload": PAYLOAD.decode(),
        "cases": cases,
    }
    (HERE / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"wrote {len(cases)} vectors + manifest.json")


if __name__ == "__main__":
    main()
