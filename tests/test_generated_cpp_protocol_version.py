"""Drift test for ``vgi.codegen.cpp_protocol_version``.

Mirrors ``test_generated_cpp_constants.py`` but narrower — single string
constant. If the ``.hpp`` checked into the vgi repo drifts from what the
generator would emit right now, the C++ extension would tag every request
batch with a stale ``vgi_rpc.protocol_version`` and the server's
dispatch-boundary check would reject every method — fast and loud, but
catching it at PR time is cheaper than catching it at integration-test time.

Skipped when the ``vgi`` repo isn't checked out next to ``vgi-python``;
the determinism test still runs.
"""

from __future__ import annotations

import io
import os
import re
from pathlib import Path

import pytest

from vgi.codegen.cpp_protocol_version import emit
from vgi.codegen.protocol_version import current_protocol_version


def _vgi_generated_path() -> Path:
    """Locate ``vgi/src/generated/vgi_protocol_version.hpp``."""
    override = os.environ.get("VGI_GENERATED_PROTOCOL_VERSION_HPP")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "vgi" / "src" / "generated" / "vgi_protocol_version.hpp"


_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python python -m vgi.codegen.cpp_protocol_version \\\n"
    "    > ~/Development/vgi/src/generated/vgi_protocol_version.hpp"
)


def test_generator_is_deterministic() -> None:
    """Running the generator twice produces byte-identical output."""
    out1 = io.StringIO()
    emit(out1)
    out2 = io.StringIO()
    emit(out2)
    assert out1.getvalue() == out2.getvalue()


def test_emitted_value_matches_protocol() -> None:
    """The emitted VGI_PROTOCOL_VERSION literal matches VgiProtocol.protocol_version."""
    out = io.StringIO()
    emit(out)
    emitted = out.getvalue()

    match = re.search(r'inline constexpr std::string_view VGI_PROTOCOL_VERSION = "([^"]*)";', emitted)
    assert match is not None, "generator did not emit VGI_PROTOCOL_VERSION"
    assert match.group(1) == current_protocol_version(), (
        f"emitted {match.group(1)!r} != VgiProtocol.protocol_version {current_protocol_version()!r}"
    )


def test_checked_in_generated_hpp_matches_generator() -> None:
    """The .hpp checked into the vgi repo must match the current generator output."""
    path = _vgi_generated_path()
    if not path.exists():
        pytest.skip(
            f"{path} not found; set VGI_GENERATED_PROTOCOL_VERSION_HPP "
            "or check out the vgi repo next to vgi-python"
        )

    checked_in = path.read_text()
    out = io.StringIO()
    emit(out)
    expected = out.getvalue()

    if checked_in != expected:
        for i, (actual_ch, expected_ch) in enumerate(zip(checked_in, expected, strict=False)):
            if actual_ch != expected_ch:
                window_start = max(0, i - 40)
                window_end = i + 40
                raise AssertionError(
                    f"checked-in {path} differs from generator output at offset {i}.\n"
                    f"  checked-in: {checked_in[window_start:window_end]!r}\n"
                    f"  expected:   {expected[window_start:window_end]!r}\n"
                    f"{_REGEN_HINT}"
                )
        raise AssertionError(
            f"checked-in {path} is shorter/longer than generator output "
            f"({len(checked_in)} vs {len(expected)} chars)\n{_REGEN_HINT}"
        )
