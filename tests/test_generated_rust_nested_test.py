# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Drift + shape tests for `vgi.codegen.rust_nested_test`.

The test itself runs in vgi-rust (`cargo test -p vgi-protocol --test
nested_binary`) against the corpus `vgi.codegen.nested_binary_fixtures` copies
there. From here: the emission is deterministic, covers every embedding
record, and the checked-in file is current.

Skipped when the sibling `vgi-rust` checkout is not present;
``VGI_RUST_NESTED_TEST_RS`` overrides the location.
"""

from __future__ import annotations

import io
import os
from pathlib import Path

import pytest

from vgi.codegen import rust_nested_test, rust_types


def _emitted() -> str:
    buf = io.StringIO()
    rust_nested_test.emit(buf)
    return buf.getvalue()


def _checked_in_path() -> Path:
    override = os.environ.get("VGI_RUST_NESTED_TEST_RS")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "vgi-rust" / rust_nested_test.TARGET


def test_generator_is_deterministic() -> None:
    """Calling emit() twice produces byte-identical output."""
    assert _emitted() == _emitted()


def test_every_embedding_record_is_checked() -> None:
    """Each generated record holding a `Nested<BindRequest>` gets a byte-identity case."""
    src = _emitted()
    embedding = [s.name for s in rust_types.build_model() if any(f.nested == "BindRequest" for f in s.fields)]
    assert len(embedding) == 6
    for name in embedding:
        assert f'"{name}" => {{' in src, name
        assert f"check_embedding::<{name}>" in src, name
    for test in (
        "byte_identity_round_trip",
        "typed_construction_round_trips",
        "null_arguments_and_exactly_one",
        "decode_errors_surface_at_access_with_the_field_path",
    ):
        assert f"fn {test}()" in src


def test_emitted_lines_fit_rustfmt_width() -> None:
    """vgi-rust CI runs `cargo fmt --check` (100 columns)."""
    for line in _emitted().splitlines():
        if line.lstrip().startswith("//"):
            continue
        assert len(line) <= 100, line


def test_checked_in_test_matches_generator() -> None:
    """The file committed in vgi-rust must equal what the generator emits now."""
    path = _checked_in_path()
    if not path.exists():
        pytest.skip(f"vgi-rust nested-binary test not found at {path}")
    assert path.read_text(encoding="utf-8") == _emitted(), (
        f"{path} is stale. Run: uv run --project ~/Development/vgi-python python scripts/regen_generated.py"
    )
