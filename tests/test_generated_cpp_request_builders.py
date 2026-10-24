# Copyright 2025, 2026 Query Farm LLC - https://query.farm

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
import re
from pathlib import Path

import pytest

from vgi.codegen._registry import registry_methods
from vgi.codegen.cpp_request_builders import emit


def _vgi_generated_path() -> Path:
    """Locate ``vgi/src/generated/vgi_request_builders.hpp`` next to this repo."""
    override = os.environ.get("VGI_REQUEST_BUILDERS_HPP")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "vgi" / "src" / "generated" / "vgi_request_builders.hpp"


_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python python scripts/regen_generated.py\n"
    "\n"
    "Do not redirect a generator into its destination with `>`: the shell\n"
    "truncates the file before the generator runs, so any failure destroys\n"
    "the checked-in artifact. The script renders to memory first."
)


def test_native_runtime_registry_covers_protocol_requests_and_responses() -> None:
    """A generated builder alone does not register a method with the native dispatcher."""
    path = _vgi_generated_path().parents[1] / "vgi_schema_registry.cpp"
    if not path.exists():
        pytest.skip(f"native runtime registry not found at {path}")
    source = path.read_text()
    # The dispatcher now derives both contracts from the generated method table;
    # only response policies (dynamic, opaque record, item lists) are handwritten.
    assert "for (const auto &method : AllMethodSchemas())" in source
    assert "entry.request_schema = method.params;" in source
    assert "entry.schema = method.result;" in source
    assert "m.emplace(method.method, std::move(entry));" in source
    records = (path.parent / "generated" / "vgi_protocol_records.cpp").read_text()
    method_table = records.split("&AllMethodSchemas()", 1)[1].split("return methods;", 1)[0]
    declared = {method.name for method in registry_methods()}
    registered = set(re.findall(r'\{"([a-z_]+)",\s*\w+ParamsSchema\(\),', method_table))
    assert registered == declared, (
        f"Native method contracts missing: {declared - registered}; stale: {registered - declared}"
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


def test_emits_builders_for_opaque_nested_records() -> None:
    """C++ can construct nested protocol records without hand-written schemas."""
    out = io.StringIO()
    emit(out)
    text = out.getvalue()
    assert "BuildClientCapabilities(" in text
    assert "BuildForeignKeyInfo(" in text
