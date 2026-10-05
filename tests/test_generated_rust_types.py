# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Drift + behavior tests for `vgi.codegen.rust_types`.

The deep correctness check — that each generated struct's `#[derive(VgiArrow)]`
schema equals its `protocol_schemas.rs` factory — lives on the Rust side, in the
`schema_parity` module the generator emits into `protocol_types.rs` itself: only
the Rust compiler can run the derive. From here we check the shape of the
emission, and that the checked-in file is current.

Skipped when the sibling `vgi-rust` checkout is not present;
``VGI_RUST_GENERATED_TYPES_RS`` overrides the location.
"""

from __future__ import annotations

import io
import os
import re
from pathlib import Path

import pytest

from vgi.codegen import rust_types
from vgi.codegen.rust_schemas import emit as emit_schemas

_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python python scripts/regen_generated.py\n"
    "\n"
    "Do not redirect a generator into its destination with `>`: the shell\n"
    "truncates the file before the generator runs, so any failure destroys\n"
    "the checked-in artifact. The script renders to memory first."
)


def _vgi_rust_generated_path() -> Path:
    override = os.environ.get("VGI_RUST_GENERATED_TYPES_RS")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "vgi-rust" / "vgi-protocol" / "src" / "generated" / "protocol_types.rs"


def _emitted() -> str:
    buf = io.StringIO()
    rust_types.emit(buf)
    return buf.getvalue()


def test_generator_is_deterministic() -> None:
    """Calling emit() twice produces byte-identical output."""
    assert _emitted() == _emitted(), "rust_types generator is non-deterministic"


def test_every_record_is_emitted_with_the_derive() -> None:
    """Each RUST_TYPES record becomes a `#[derive(VgiArrow)]` struct."""
    src = _emitted()
    for cls in rust_types.RUST_TYPES:
        assert f"#[derive(Debug, Clone, VgiArrow)]\npub struct {cls.__name__} {{" in src, cls.__name__


def test_fields_follow_arrow_schema_order_and_nullability() -> None:
    """Field order is column order; nullable columns are Option<T>."""
    for struct, cls in zip(rust_types.build_model(), rust_types.RUST_TYPES, strict=True):
        schema = cls.ARROW_SCHEMA  # type: ignore[attr-defined]
        assert [f.wire_name for f in struct.fields] == schema.names
        for f, arrow in zip(struct.fields, schema, strict=True):
            assert f.rust_type.startswith("Option<") == arrow.nullable, (struct.name, f.wire_name)


def test_known_shapes() -> None:
    """Spot-check the mapping on the records vgi-rust consumes."""
    by_name = {s.name: {f.wire_name: f.rust_type for f in s.fields} for s in rust_types.build_model()}
    attach = by_name["CatalogAttachResult"]
    assert attach["attach_opaque_data"] == "Bytes"
    assert attach["comment"] == "Option<String>"
    assert attach["tags"] == "StrMap"
    assert attach["settings"] == "Vec<Bytes>"
    assert attach["supports_catalog_contents"] == "bool"
    assert by_name["CatalogContentsResponse"] == {"catalog_version": "i64", "schemas": "Vec<Bytes>"}
    assert by_name["SchemaContents"]["schema"] == "Bytes"


def test_parity_module_covers_every_struct_against_an_existing_factory() -> None:
    """The emitted Rust test asserts on every struct, against a factory rust_schemas emits."""
    src = _emitted()
    structs = set(re.findall(r"^pub struct (\w+) \{", src, re.MULTILINE))
    asserted = set(re.findall(r"flat_schema::<(\w+)>\(\)", src))
    assert structs == asserted, f"schema_parity does not cover: {sorted(structs - asserted)}"

    buf = io.StringIO()
    emit_schemas(buf)
    factories = set(re.findall(r"pub fn (\w+_schema)\(\) -> SchemaRef", buf.getvalue()))
    used = set(re.findall(r"protocol_schemas::(\w+_schema)\(\)", src))
    assert used <= factories, f"schema_parity names factories rust_schemas does not emit: {sorted(used - factories)}"


def test_no_python_doc_markup_leaks_into_rustdoc() -> None:
    """Python doc markup (mkdocstrings cross-refs, RST roles) would be broken intra-doc links in rustdoc."""
    src = _emitted()
    assert ":class:" not in src
    assert "][]" not in src
    assert "``" not in src


def test_checked_in_rust_matches_generator() -> None:
    """The file committed in vgi-rust must equal what the generator emits now."""
    path = _vgi_rust_generated_path()
    if not path.exists():
        pytest.skip(f"vgi-rust checkout not found at {path}")
    assert path.read_text(encoding="utf-8") == _emitted(), f"{path} is stale.\n\n{_REGEN_HINT}"
