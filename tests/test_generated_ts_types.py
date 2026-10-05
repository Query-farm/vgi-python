# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Drift + behavior tests for `vgi.codegen.ts_types`.

Enforces that `vgi-typescript/src/generated/vgi-protocol-types.ts` in the
sibling repo matches what the generator would emit right now. Skipped when the
`vgi-typescript` checkout is not next to vgi-python; ``VGI_TS_GENERATED_TYPES``
overrides the file's location.
"""

from __future__ import annotations

import io
import os
import re
from pathlib import Path

import pytest

from vgi.codegen import ts_types
from vgi.codegen.ts_types import TS_RECORD_TYPES, build_model, emit


def _vgi_ts_types_path() -> Path:
    override = os.environ.get("VGI_TS_GENERATED_TYPES")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "vgi-typescript" / "src" / "generated" / "vgi-protocol-types.ts"


_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python python scripts/regen_generated.py\n"
    "\n"
    "Do not redirect a generator into its destination with `>`: the shell\n"
    "truncates the file before the generator runs, so any failure destroys\n"
    "the checked-in artifact. The script renders to memory first."
)


def _render() -> str:
    buf = io.StringIO()
    emit(buf)
    return buf.getvalue()


def test_generator_is_deterministic() -> None:
    """Calling emit() twice produces byte-identical output."""
    assert _render() == _render(), "ts_types generator is non-deterministic"


def test_every_record_gets_builder_and_codec() -> None:
    """Each configured record has an Init type, a builder and an encode/decode pair."""
    text = _render()
    for cls in TS_RECORD_TYPES:
        name = cls.__name__
        assert f"export type {name}Init = " in text
        assert f"export function build{name}(init: {name}Init): {name} {{" in text
        assert f"export const encode{name} = " in text
        assert f"export const decode{name} = " in text


def test_builder_fields_follow_arrow_schema_order() -> None:
    """A builder lists every ARROW_SCHEMA column, in wire order, with Python's defaults."""
    text = _render()
    for cls in TS_RECORD_TYPES:
        name = cls.__name__
        body = re.search(rf"export function build{name}\(.*?\n  return \{{\n(.*?)\n  \}};", text, re.DOTALL)
        assert body is not None, name
        names = [line.strip().split(":", 1)[0] for line in body.group(1).splitlines()]
        assert names == list(cls.ARROW_SCHEMA.names), name  # type: ignore[attr-defined]
    assert "    supports_catalog_contents: init.supports_catalog_contents ?? false," in text
    assert '    default_schema: init.default_schema ?? "main",' in text


def test_blob_records_are_emitted_and_client_records_reexported() -> None:
    """SchemaContents (a blob no method reaches) is emitted; ts_client's records are re-exported."""
    by_name = {r.name: r for r in build_model()}
    assert not by_name["SchemaContents"].reexported
    assert by_name["CatalogAttachResult"].reexported
    assert by_name["CatalogContentsResponse"].schema_const == "CatalogContentsResultSchema"
    text = _render()
    assert "export interface SchemaContents {" in text
    assert "export interface CatalogAttachResult {" not in text


def test_unsupported_type_raises() -> None:
    """An Arrow type with no mapping fails loudly instead of emitting a wrong type."""
    import pyarrow as pa

    with pytest.raises(ts_types.GeneratorError):
        ts_types._ts_type(pa.struct([pa.field("a", pa.int32())]), "X.a")


def test_checked_in_types_match_generator() -> None:
    """Checked-in vgi-protocol-types.ts matches what the generator would emit right now."""
    path = _vgi_ts_types_path()
    if not path.exists():
        pytest.skip(f"{path} not found; set VGI_TS_GENERATED_TYPES or check out vgi-typescript next to vgi-python")
    assert path.read_text() == _render(), f"checked-in {path.name} differs from generator output.\n{_REGEN_HINT}"
