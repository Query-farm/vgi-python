# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Drift + behavior tests for `vgi.codegen.go_types`.

vgi-go checks in the protocol records it serves as Go structs (its wire schemas
are derived from their ``vgirpc`` struct tags). The checked-in file must match
what the generator emits right now. vgi-go's own
``vgi/generated/protocol_types_test.go`` checks each struct's derived schema
against the generated schema factories.

Skipped when the `vgi-go` checkout is not next to vgi-python;
``VGI_GO_GENERATED_TYPES_GO`` overrides the file's location.
"""

from __future__ import annotations

import io
import os
import re
from pathlib import Path

import pyarrow as pa
import pytest

from vgi.codegen import go_types
from vgi.codegen._common import GeneratorError

_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python python scripts/regen_generated.py\n"
    "\n"
    "Do not redirect a generator into its destination with `>`: the shell\n"
    "truncates the file before the generator runs, so any failure destroys\n"
    "the checked-in artifact. The script renders to memory first."
)


def _vgi_go_types_path() -> Path:
    override = os.environ.get("VGI_GO_GENERATED_TYPES_GO")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "vgi-go" / "vgi" / "generated" / "protocol_types.go"


def _render() -> str:
    buf = io.StringIO()
    go_types.emit(buf)
    return buf.getvalue()


def test_generator_is_deterministic() -> None:
    """Calling emit() twice produces byte-identical output."""
    assert _render() == _render()


def test_checked_in_file_matches_generator() -> None:
    """Drift check: the checked-in Go matches what the generator produces."""
    path = _vgi_go_types_path()
    if not path.exists():
        pytest.skip(f"{path} not found; set VGI_GO_GENERATED_TYPES_GO or check out vgi-go next to vgi-python")
    assert path.read_text() == _render(), f"{path} is stale.\n{_REGEN_HINT}"


def test_catalog_contents_records_are_emitted() -> None:
    """The records vgi-go serves for catalog_attach / catalog_contents are generated."""
    names = {r.name for r in go_types.build_records()}
    assert {"CatalogAttachResult", "SchemaContents", "CatalogContentsResponse"} <= names


def test_fields_follow_arrow_schema() -> None:
    """Field order, names and nullability come from ARROW_SCHEMA."""
    for record in go_types.build_records():
        assert [f.wire_name for f in record.fields] == record.schema.names, record.name
        for f, arrow_field in zip(record.fields, record.schema, strict=True):
            assert f.go_type.startswith("*") == arrow_field.nullable, (record.name, f.wire_name)


def test_attach_result_shape() -> None:
    """Spot-check the mappings: binary, list<binary>, map, nullable string, trailing flag."""
    record = next(r for r in go_types.build_records() if r.name == "CatalogAttachResult")
    fields = {f.wire_name: f for f in record.fields}
    assert fields["attach_opaque_data"].go_type == "[]byte"
    assert fields["settings"].go_type == "[][]byte"
    assert fields["tags"].go_type == "map[string]string"
    assert fields["comment"].go_type == "*string"
    assert fields["catalog_version"].go_type == "int64"
    last = record.fields[-1]
    assert (last.wire_name, last.name, last.go_type) == ("supports_catalog_contents", "SupportsCatalogContents", "bool")


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        (pa.large_binary(), ("[]byte", ["large_binary"])),
        (pa.list_(pa.large_binary()), ("[][]byte", ["elem=large_binary"])),
        (pa.dictionary(pa.int16(), pa.string()), ("string", ["enum"])),
        (pa.list_(pa.string()), ("[]string", [])),
        (pa.map_(pa.string(), pa.int64()), ("map[string]int64", [])),
    ],
)
def test_type_mapping(dtype: pa.DataType, expected: tuple[str, list[str]]) -> None:
    """Arrow types map to the Go type + tag option vgi-rpc-go derives them from."""
    assert go_types._map(dtype, None, origin="t") == expected


@pytest.mark.parametrize(
    "dtype",
    [
        pa.list_(pa.field("item", pa.binary(), nullable=False)),
        pa.list_(pa.list_(pa.large_binary())),
        pa.struct([pa.field("a", pa.int64())]),
        pa.dictionary(pa.int32(), pa.string()),
    ],
)
def test_inexpressible_types_raise(dtype: pa.DataType) -> None:
    """A type vgi-rpc-go cannot derive is an error, never a silently different struct."""
    with pytest.raises(GeneratorError):
        go_types._map(dtype, None, origin="t")


def test_initialisms() -> None:
    """Go field names spell initialisms the Go way."""
    assert go_types.go_field_name("attach_opaque_data") == "AttachOpaqueData"
    assert go_types.go_field_name("notes_url") == "NotesURL"
    assert go_types.go_field_name("split_token_ttl_seconds") == "SplitTokenTTLSeconds"


def test_output_is_gofmt_shaped() -> None:
    """No tabs-vs-space alignment for gofmt to redo, and no trailing blank line."""
    text = _render()
    assert not text.endswith("\n\n")
    for line in text.splitlines():
        if line.startswith("\t") and "`vgirpc:" in line:
            assert re.fullmatch(r"\t\w+ \S+ `vgirpc:\"[^\"]+\"`", line), line
