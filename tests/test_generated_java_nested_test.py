# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Drift + determinism tests for `vgi.codegen.java_nested_test`.

vgi-java checks in the generated nested-binary conformance test at
``java_nested_test.TARGET``; it must match what the generator emits now.
Skipped when the `vgi-java` checkout isn't present (``VGI_JAVA_ROOT`` overrides
the search, as for `tests/test_generated_java_types.py`).
"""

from __future__ import annotations

from vgi.codegen import java_nested_test

from .test_generated_java_types import _REGEN_HINT, _vgi_java_root


def test_generator_is_deterministic() -> None:
    """Rendering twice produces byte-identical output."""
    assert java_nested_test.render() == java_nested_test.render()


def test_targets_vgi_java() -> None:
    """The test lands in vgi-java's generated test package."""
    assert java_nested_test.REPO == "vgi-java"
    assert java_nested_test.TARGET.endswith("/generated/NestedBinaryConformanceTest.java")


def test_typed_construction_tracks_the_generated_records() -> None:
    """The constructor calls have one argument per generated component."""
    text = java_nested_test.render()
    assert 'new BindRequest(\n                "typed_construction",' in text
    assert "new InitRequest(\n                wrapped," in text


def test_checked_in_test_matches_generator() -> None:
    """Drift check against the vgi-java checkout."""
    import pytest

    path = _vgi_java_root() / java_nested_test.TARGET
    if not path.parent.parent.is_dir():
        pytest.skip(f"{path.parent.parent} not found; set VGI_JAVA_ROOT or check out vgi-java")
    assert path.exists(), f"{path} is missing.\n{_REGEN_HINT}"
    assert path.read_text() == java_nested_test.render(), f"{path} is stale.\n{_REGEN_HINT}"
