# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Drift test for `vgi.codegen.protocol_version`.

Validates that the committed ``vgi/protocol_version.txt`` matches
``VgiProtocol.protocol_version`` exactly.

The ``.txt`` file is the cross-language source of truth: Rust/Go/TS
workers ``cat`` it to know what to send on every request batch's
``vgi_rpc.protocol_version`` metadata. If the committed file drifts from
the Python class declaration, those non-Python workers send the wrong
version and the server rejects every dispatched method with a
``ProtocolVersionError`` — a fast, loud failure mode, but one we'd
rather catch at PR time.
"""

from __future__ import annotations

import io
from pathlib import Path

from vgi.codegen.protocol_version import current_protocol_version, emit


def _protocol_version_txt_path() -> Path:
    """Resolve ``vgi/protocol_version.txt`` relative to this test file."""
    return Path(__file__).resolve().parents[1] / "vgi" / "protocol_version.txt"


_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python python -m vgi.codegen.protocol_version \\\n"
    "    > ~/Development/vgi-python/vgi/protocol_version.txt"
)


def test_current_protocol_version_is_canonical_semver() -> None:
    """``current_protocol_version`` returns a non-empty string in MAJOR.MINOR.PATCH form."""
    value = current_protocol_version()
    assert isinstance(value, str)
    parts = value.split(".")
    assert len(parts) == 3, f"expected MAJOR.MINOR.PATCH, got {value!r}"
    for component in parts:
        assert component.isdigit(), f"non-numeric semver component in {value!r}"


def test_generator_is_deterministic() -> None:
    """Running the generator twice produces byte-identical output."""
    out1 = io.StringIO()
    emit(out1)
    out2 = io.StringIO()
    emit(out2)
    assert out1.getvalue() == out2.getvalue()


def test_checked_in_txt_matches_generator() -> None:
    """``vgi/protocol_version.txt`` must match the current generator output exactly."""
    path = _protocol_version_txt_path()
    assert path.exists(), f"missing source-of-truth file {path}\n{_REGEN_HINT}"

    checked_in = path.read_text()
    out = io.StringIO()
    emit(out)
    expected = out.getvalue()

    assert checked_in == expected, (
        f"checked-in {path} differs from generator output.\n"
        f"  checked-in: {checked_in!r}\n"
        f"  expected:   {expected!r}\n"
        f"{_REGEN_HINT}"
    )
