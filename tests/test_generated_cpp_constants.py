# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Drift + determinism tests for `vgi.codegen.cpp_constants`.

Sibling of ``test_generated_cpp_schemas.py`` but narrower: validates
the small set of wire byte-constants emitted alongside the generated
Arrow schemas. When these tests fail, the error message names the
regeneration command.

If the ``vgi`` repo isn't checked out next to ``vgi-python``, the drift
test is skipped; the determinism test still runs.
"""

from __future__ import annotations

import io
import os
import re
from pathlib import Path

import pytest
from vgi_rpc import metadata as _rpc_metadata

from vgi.codegen.cpp_constants import _CONSTANTS, emit


def _vgi_generated_path() -> Path:
    """Locate ``vgi/src/generated/vgi_protocol_constants.hpp``."""
    override = os.environ.get("VGI_GENERATED_CONSTANTS_HPP")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "vgi" / "src" / "generated" / "vgi_protocol_constants.hpp"


_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python vgi-gen-cpp-constants \\\n"
    "    > ~/Development/vgi/src/generated/vgi_protocol_constants.hpp"
)


def test_generator_is_deterministic() -> None:
    """Running the generator twice produces byte-identical output."""
    out1 = io.StringIO()
    emit(out1)
    out2 = io.StringIO()
    emit(out2)
    assert out1.getvalue() == out2.getvalue(), (
        "generator is non-deterministic — the _CONSTANTS iteration order or formatter is unstable"
    )


def test_emitted_values_match_vgi_rpc() -> None:
    """Every emitted constant's value matches ``vgi_rpc.metadata``.

    This is the load-bearing correctness assertion: if the generator
    somehow emits the wrong bytes for a key, the C++ extension and the
    Python workers disagree on the wire — a silent data corruption
    bug this test prevents.
    """
    out = io.StringIO()
    emit(out)
    emitted = out.getvalue()

    pattern = re.compile(
        r'inline constexpr std::string_view (\w+) = "([^"]*)";',
    )
    found: dict[str, bytes] = {match.group(1): match.group(2).encode("utf-8") for match in pattern.finditer(emitted)}

    for entry in _CONSTANTS:
        assert entry.cpp_name in found, f"generator did not emit {entry.cpp_name}"
        expected = getattr(_rpc_metadata, entry.python_name)
        assert found[entry.cpp_name] == expected, (
            f"{entry.cpp_name} value drift: emitted {found[entry.cpp_name]!r}, "
            f"vgi_rpc.metadata.{entry.python_name} is {expected!r}"
        )


def test_checked_in_generated_hpp_matches_generator() -> None:
    """The .hpp checked into the vgi repo must match the current output."""
    path = _vgi_generated_path()
    if not path.exists():
        pytest.skip(f"{path} not found; set VGI_GENERATED_CONSTANTS_HPP or check out the vgi repo next to vgi-python")

    checked_in = path.read_text()

    out = io.StringIO()
    emit(out)
    expected = out.getvalue()

    if checked_in != expected:
        # Produce a helpful diff by pointing at the first divergence
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
        # Same prefix, different lengths
        raise AssertionError(
            f"checked-in {path} is shorter/longer than generator output "
            f"({len(checked_in)} vs {len(expected)} chars)\n{_REGEN_HINT}"
        )
