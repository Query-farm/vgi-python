"""Drift + determinism tests for `vgi.codegen.cpp_request_builders`.

Sibling of ``test_generated_cpp_schemas.py``. Enforces that
``vgi/src/generated/vgi_request_builders.hpp`` in the sibling ``vgi`` repo
matches what the generator would emit right now. When it fails, the error
message prints the regeneration command.

If the ``vgi`` repo isn't present next to vgi-python, the drift test is
skipped (the determinism test still runs — it needs no external repo).
"""

from __future__ import annotations

import io
import os
from pathlib import Path

import pytest

from vgi.codegen.cpp_request_builders import emit


def _vgi_generated_path() -> Path:
    """Locate ``vgi/src/generated/vgi_request_builders.hpp`` next to this repo."""
    override = os.environ.get("VGI_REQUEST_BUILDERS_HPP")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "vgi" / "src" / "generated" / "vgi_request_builders.hpp"


_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python python -m vgi.codegen.cpp_request_builders \\\n"
    "    > ~/Development/vgi/src/generated/vgi_request_builders.hpp"
)


def test_generator_is_deterministic() -> None:
    """Running the generator twice produces byte-identical output."""
    out1 = io.StringIO()
    emit(out1)
    out2 = io.StringIO()
    emit(out2)
    assert out1.getvalue() == out2.getvalue(), (
        "generator is non-deterministic — sorting or collection order is unstable"
    )


def test_checked_in_generated_hpp_matches_generator() -> None:
    """The .hpp checked into the vgi repo must match the current generator output."""
    path = _vgi_generated_path()
    if not path.exists():
        pytest.skip(f"{path} not found; set VGI_REQUEST_BUILDERS_HPP or check out vgi repo next to vgi-python")

    expected = io.StringIO()
    emit(expected)
    actual = path.read_text()

    if expected.getvalue() != actual:
        # Identify the first divergence to make the failure scannable.
        exp_lines = expected.getvalue().splitlines()
        act_lines = actual.splitlines()
        diff_index = next(
            (i for i, (e, a) in enumerate(zip(exp_lines, act_lines, strict=False)) if e != a),
            min(len(exp_lines), len(act_lines)),
        )
        context = "\n".join(
            f"  {tag} {line}"
            for tag, line in (
                ("-", act_lines[diff_index] if diff_index < len(act_lines) else "(EOF)"),
                ("+", exp_lines[diff_index] if diff_index < len(exp_lines) else "(EOF)"),
            )
        )
        raise AssertionError(
            f"{path} differs from generator output starting at line {diff_index + 1}:\n{context}\n{_REGEN_HINT}",
        )


def test_emits_at_least_a_few_builders() -> None:
    """Sanity check — generator should produce a non-trivial number of inline definitions.

    Guards against a future refactor that accidentally turns the generator into a no-op.
    The exact count drifts as the protocol evolves, so we only assert "at least 30".
    """
    out = io.StringIO()
    emit(out)
    text = out.getvalue()
    count = text.count("\ninline std::shared_ptr<arrow::RecordBatch> Build")
    assert count >= 30, f"generator emitted only {count} builders — expected at least 30"
