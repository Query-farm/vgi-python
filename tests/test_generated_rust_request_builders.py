# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Drift + determinism tests for `vgi.codegen.rust_request_builders`.

The deep correctness check — that each generated struct's `#[derive(VgiArrow)]`
schema equals `wire::params_schema_for(method)` — lives on the Rust side, in the
`schema_parity` module the generator emits into `request_params.rs` itself. It
has to: only the Rust compiler can run the derive.

What is checkable from here is the shape of the emission, and that the
checked-in file is current.
"""

from __future__ import annotations

import io
import os
import re
from pathlib import Path

import pytest

from vgi.codegen._common import collect_schemas, sanitize_name
from vgi.codegen.rust_request_builders import emit


def _vgi_rust_generated_path() -> Path:
    override = os.environ.get("VGI_RUST_GENERATED_PARAMS_RS")
    if override:
        return Path(override)
    return (
        Path(__file__).resolve().parents[2]
        / "vgi-rust"
        / "vgi-protocol"
        / "src"
        / "generated"
        / "request_params.rs"
    )


_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python \\\n"
    "    python -m vgi.codegen.rust_request_builders \\\n"
    "    > ~/Development/vgi-rust/vgi-protocol/src/generated/request_params.rs"
)


def _emitted() -> str:
    buf = io.StringIO()
    emit(buf)
    return buf.getvalue()


def test_generator_is_deterministic() -> None:
    """Calling emit() twice produces byte-identical output."""
    assert _emitted() == _emitted(), "rust_request_builders generator is non-deterministic"


def _params_by_method() -> dict[str, object]:
    out = {}
    for es in collect_schemas():
        if es.name.endswith("Params"):
            out[es.origin.split("'")[1]] = es.schema
    return out


def test_a_struct_exists_for_every_non_empty_params_method() -> None:
    """Each method with params columns gets a struct; field-less ones are documented instead."""
    src = _emitted()
    by_method = _params_by_method()
    for method, schema in by_method.items():
        struct = sanitize_name(method) + "Params"
        if not len(schema):
            # VgiArrow cannot derive on a field-less struct; these are documented
            # in the module header instead.
            assert f"pub struct {struct} " not in src, f"{struct} has no columns and must not be emitted"
            assert f"//! - `{method}`" in src, f"{method} takes no params and should be named in the docs"
            continue
        assert f"pub struct {struct} {{" in src, f"missing struct for method '{method}'"


def test_keyword_field_names_are_raw_identifiers() -> None:
    """A wire column named `type` must be spelled `r#type` to compile."""
    src = _emitted()
    assert "pub r#type: DictString," in src, (
        "the schema-contents methods carry a `type` column; it must be emitted as a raw identifier"
    )
    assert "pub type:" not in src, "bare `type` is a Rust keyword and will not compile"


def test_nullability_is_carried_by_option() -> None:
    """Nullable columns become Option<T>; required ones do not."""
    src = _emitted()
    # catalog_table_get: at_unit / at_value are nullable, name is not.
    block = re.search(r"pub struct CatalogTableGetParams \{(.*?)\n\}", src, re.DOTALL)
    assert block is not None, "CatalogTableGetParams not found"
    body = block.group(1)
    assert "pub at_unit: Option<String>," in body, body
    assert "pub name: String," in body, body


def test_every_emitted_struct_derives_vgi_arrow() -> None:
    """A struct without the derive would be inert — no schema, no batch."""
    src = _emitted()
    structs = re.findall(r"pub struct (\w+Params) \{", src)
    assert structs, "no structs emitted"
    derives = re.findall(r"#\[derive\(Debug, Clone, VgiArrow\)\]\npub struct (\w+Params) \{", src)
    assert set(structs) == set(derives), (
        f"structs missing the VgiArrow derive: {sorted(set(structs) - set(derives))}"
    )


def test_parity_test_module_covers_every_struct() -> None:
    """The emitted Rust test must assert on each struct it ships beside."""
    src = _emitted()
    structs = set(re.findall(r"pub struct (\w+Params) \{", src))
    asserted = set(re.findall(r"flat_schema::<(\w+Params)>\(\)", src))
    assert structs == asserted, (
        f"schema_parity module does not cover: {sorted(structs - asserted)}"
    )


def test_checked_in_rust_matches_generator() -> None:
    """The file committed in vgi-rust must equal what the generator emits now."""
    path = _vgi_rust_generated_path()
    if not path.exists():
        pytest.skip(f"vgi-rust checkout not found at {path}")
    assert path.read_text(encoding="utf-8") == _emitted(), f"{path} is stale.\n\n{_REGEN_HINT}"
