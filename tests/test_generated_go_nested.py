# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The Go raw-preserving nested-record wrapper and its generated conformance test.

`vgi.codegen.go_types` emits a ``bind_call`` (a ``BindRequest`` carried as a
binary IPC stream) as ``Nested[BindRequest]`` (vgi docs/large-values-3.4.md
§3.3); `vgi.codegen.go_nested_test` emits vgi-go's test over the
nested-binary corpus. Drift checks skip when the `vgi-go` checkout is not
next to vgi-python; ``VGI_GO_NESTED_TEST_GO`` overrides the test's location.
"""

from __future__ import annotations

import io
import os
from pathlib import Path

import pytest

from vgi.codegen import go_nested_test, go_types

_EMBEDDING = {
    "InitRequest",
    "TableFunctionCardinalityRequest",
    "TableFunctionStatisticsRequest",
    "TableFunctionDynamicToStringRequest",
    "TableFunctionPlanRequest",
    "TableFunctionProjectionRequest",
}


def _types() -> str:
    buf = io.StringIO()
    go_types.emit(buf)
    return buf.getvalue()


def _test() -> str:
    buf = io.StringIO()
    go_nested_test.emit(buf)
    return buf.getvalue()


def test_bind_call_is_the_raw_preserving_wrapper() -> None:
    """Exactly the six embedding requests carry a nested BindRequest, as a binary column."""
    nested = {r.name: f for r in go_types.build_records() for f in r.fields if f.nested is not None}
    assert set(nested) == _EMBEDDING
    for f in nested.values():
        assert (f.wire_name, f.go_type, f.options, f.nested) == (
            "bind_call",
            "Nested[BindRequest]",
            ["binary"],
            "BindRequest",
        )


def test_other_nested_binary_records_stay_bytes() -> None:
    """Only NESTED_BINARY_RECORDS get the wrapper; e.g. the projection's bind_result stays []byte."""
    record = next(r for r in go_types.build_records() if r.name == "TableFunctionProjectionRequest")
    assert {f.wire_name: f.go_type for f in record.fields}["bind_result"] == "[]byte"


def test_wrapper_and_accessors_are_emitted() -> None:
    """One generic wrapper, plus a path-naming accessor per embedding record."""
    text = _types()
    assert text.count("type Nested[T any] struct {") == 1
    for name in _EMBEDDING:
        assert f"func (r {name}) BindCallValue() (BindRequest, error) {{" in text
        assert f'fmt.Errorf("{name}.bind_call: %w", err)' in text
    assert "func (n Nested[T]) MarshalBinary() ([]byte, error) {" in text
    assert "func (n *Nested[T]) UnmarshalBinary(data []byte) error {" in text


def test_conformance_test_covers_every_embedding_record() -> None:
    """The generated test has a case and a schema check for each embedding record."""
    text = _test()
    for name in _EMBEDDING:
        assert f'"{name}": nestedCase[generated.{name}]{{' in text
        assert f'{{"{name}", generated.{name}{{}}, generated.{name}Schema}},' in text
    assert _test() == text


def test_checked_in_conformance_test_matches_generator() -> None:
    """Drift check against vgi-go's checked-in test."""
    override = os.environ.get("VGI_GO_NESTED_TEST_GO")
    path = Path(override) if override else Path(__file__).resolve().parents[2] / "vgi-go" / go_nested_test.TARGET
    if not path.exists():
        pytest.skip(f"{path} not found; set VGI_GO_NESTED_TEST_GO or check out vgi-go next to vgi-python")
    assert path.read_text() == _test(), (
        f"{path} is stale; run uv run --project ~/Development/vgi-python python scripts/regen_generated.py"
    )
