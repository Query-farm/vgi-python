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
    assert by_name["CatalogContentsResponse"] == {
        "catalog_version": "i64",
        "etag": "Option<String>",
        "not_modified": "bool",
        **{kind: "Option<Bytes>" for kind in ("schemas", "tables", "views", "functions", "macros", "indexes")},
    }
    assert by_name["SchemaContents"]["path"] == "Vec<String>"
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


def test_nested_binary_records_become_raw_preserving_wrappers() -> None:
    """`bind_call` (a BindRequest carried as binary) is `Nested<BindRequest>`; other binary records stay bytes."""
    by_name = {s.name: {f.wire_name: f for f in s.fields} for s in rust_types.build_model()}
    embedding = {name for name, fields in by_name.items() if "bind_call" in fields}
    assert embedding == {
        "InitRequest",
        "TableFunctionCardinalityRequest",
        "TableFunctionStatisticsRequest",
        "TableFunctionDynamicToStringRequest",
        "TableFunctionPlanRequest",
        "TableFunctionProjectionRequest",
    }
    for name in embedding:
        field = by_name[name]["bind_call"]
        assert field.rust_type == "Nested<BindRequest>", name
        assert field.nested == "BindRequest"
    # Records outside NESTED_BINARY_RECORDS keep plain bytes (spec §3.2).
    assert by_name["TableFunctionProjectionRequest"]["bind_result"].rust_type == "Bytes"
    assert by_name["BindRequest"]["arguments"].rust_type == "Option<Bytes>"
    assert by_name["BindRequest"]["arguments_location"].rust_type == "Option<ArgumentsLocation>"
    assert list(by_name["BindRequest"])[-1] == "arguments_location"


def test_nested_runtime_accessors_and_validation_are_emitted() -> None:
    """One `Nested<T>` runtime, a path-naming accessor per wrapped field, and the exactly-one rule."""
    src = _emitted()
    assert src.count("pub struct Nested<T> {") == 1
    assert "impl<T: ::vgi_rpc::VgiArrow> ::vgi_rpc::VgiArrow for Nested<T>" in src
    assert 'get_at("InitRequest.bind_call")' in src
    assert "pub fn bind_call_value(&self) -> ::vgi_rpc::Result<&BindRequest>" in src
    assert "pub fn validate_arguments(&self) -> ::vgi_rpc::Result<()>" in src
    assert '"invalid_bind_request"' in src
    assert '"invalid_arguments_ref"' in src


def test_list_items_typed_optional_in_python_are_option_in_rust() -> None:
    """`list[str | None]` keeps null items: Vec<Option<String>>, same derived schema."""
    by_name = {s.name: {f.wire_name: f.rust_type for f in s.fields} for s in rust_types.build_model()}
    assert by_name["BindRequest"]["argument_names"] == "Option<Vec<Option<String>>>"
    assert by_name["BindRequest"]["schema_path"] == "Option<Vec<String>>"


def test_emitted_lines_fit_rustfmt_width() -> None:
    """vgi-rust CI runs `cargo fmt --check`; nothing generated may exceed rustfmt's 100 columns (doc comments aside)."""
    for line in _emitted().splitlines():
        if line.lstrip().startswith(("//", '"')):  # rustfmt never breaks comments or string literals
            continue
        assert len(line) <= 100, line
