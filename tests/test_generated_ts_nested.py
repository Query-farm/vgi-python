# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The TypeScript raw-preserving ``Nested<T>`` wrapper (vgi docs/large-values-3.4.md §3.3).

Checks what `vgi.codegen.ts_types` emits for a nested record carried as binary
(``bind_call: BindRequest``), that `vgi.codegen.ts_client` keeps ``Transient()``
fields off the wire interfaces, and that the generated conformance test
(`vgi.codegen.ts_nested_test`) matches its checked-in copy in vgi-typescript.
Skipped when that checkout is not next to vgi-python; ``VGI_TS_NESTED_TEST``
overrides the test file's location.
"""

from __future__ import annotations

import dataclasses
import io
import os
from pathlib import Path

import pytest

from vgi.codegen import ts_client, ts_nested_test, ts_types
from vgi.protocol import BindRequest

_EMBEDDING = (
    "InitRequest",
    "TableFunctionCardinalityRequest",
    "TableFunctionStatisticsRequest",
    "TableFunctionDynamicToStringRequest",
    "TableFunctionPlanRequest",
    "TableFunctionProjectionRequest",
)


def _types() -> str:
    buf = io.StringIO()
    ts_types.emit(buf)
    return buf.getvalue()


def test_transient_fields_are_not_wire_fields() -> None:
    """``BindRequest._wire`` is Transient: neither TS generator sees it."""
    assert "_wire" in {f.name for f in dataclasses.fields(BindRequest)}
    assert "_wire" not in {f.name for f in ts_client.wire_fields(BindRequest)}
    buf = io.StringIO()
    ts_client.emit(buf)
    assert "_wire" not in buf.getvalue()


def test_bind_call_is_the_nested_wrapper() -> None:
    """Each embedding request types ``bind_call`` as ``Nested<BindRequest>`` and declares its own interface."""
    by_name = {r.name: r for r in ts_types.build_model()}
    for name in _EMBEDDING:
        record = by_name[name]
        bind_call = next(f for f in record.fields if f.name == "bind_call")
        assert bind_call.ts_type == "Nested<BindRequest>", name
        assert bind_call.nested == "BindRequest"
        assert not record.reexported, name
    # Plain-bytes nested records stay bytes (spec §3.2): bind_result is a BindResponse.
    bind_result = next(f for f in by_name["TableFunctionProjectionRequest"].fields if f.name == "bind_result")
    assert bind_result.ts_type == "Uint8Array"
    assert by_name["BindRequest"].reexported
    assert by_name["ArgumentsLocation"].reexported


def test_codecs_hold_and_re_emit_the_bytes() -> None:
    """Decode wraps without decoding; encode writes the held bytes; one wrapper class and factory."""
    text = _types()
    assert text.count("export class Nested<T> {") == 1
    assert "export function nestedBindRequest(" in text
    assert "export interface InitRequest {" in text
    assert "  bind_call: Nested<BindRequest>;" in text
    assert "    bind_call: v.bind_call.bytes," in text
    assert 'bind_call: Nested.fromBytes(raw.bind_call, decodeBindRequest, "InitRequest.bind_call"),' in text
    # The decoded value comes from a private copy, so mutating it never reaches the held bytes.
    assert "this.#decode(this.#bytes.slice())" in text


def test_checked_in_nested_test_matches_generator() -> None:
    """Checked-in nested-binary.test.ts matches the generator."""
    override = os.environ.get("VGI_TS_NESTED_TEST")
    path = (
        Path(override) if override else Path(__file__).resolve().parents[2] / "vgi-typescript" / ts_nested_test.TARGET
    )
    if not path.exists():
        pytest.skip(f"{path} not found; set VGI_TS_NESTED_TEST or check out vgi-typescript next to vgi-python")
    buf = io.StringIO()
    ts_nested_test.emit(buf)
    assert path.read_text() == buf.getvalue(), "run scripts/regen_generated.py"
