# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit VGI protocol records as `#[derive(VgiArrow)]` Rust structs, for vgi-rust.

Sister module to `vgi.codegen.csharp_types`. vgi-rust's wire codec
(`vgi_protocol::wire::to_batch` / `from_batch`) *derives* a record's Arrow
schema from its struct: field declaration order is column order, `Option<T>` is
a nullable column, `Vec<Bytes>` is `list<binary>`. Getting any of those wrong in
a hand-written struct is a wire-protocol bug that only shows up at runtime, so
the records listed in `RUST_TYPES` are generated from each record's
``ARROW_SCHEMA`` — the authority — instead.

The emitted file is ``vgi-protocol/src/generated/protocol_types.rs``. Each struct
uses the SDK's existing idiom (`vgi_rpc::{Bytes, DictString, ...}`, the
`StrMap` / `IntMap` aliases from `protocol::dtos`) and the generator emits a
`schema_parity` test beside them asserting that every struct's derived schema
equals the generated schema factory in `protocol_schemas.rs`, so a derive that
disagrees with the protocol fails in vgi-rust's own `cargo test`.

Adding a record is one entry in `RUST_TYPES` (plus deleting its hand-written
twin in vgi-rust). An Arrow type with no mapping raises `GeneratorError` rather
than emitting a struct whose schema would silently differ.

.. code-block:: bash

   uv run --project ~/Development/vgi-python python scripts/regen_generated.py

``tests/test_generated_rust_types.py`` fails when the checked-in file and the
generator disagree.
"""

from __future__ import annotations

import io
import re
import sys
import textwrap
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
from vgi_rpc.rpc._types import MethodType, rpc_methods  # type: ignore[attr-defined]

from vgi.catalog.catalog_interface import CatalogAttachResult
from vgi.codegen._common import GeneratorError, provenance_comment, sanitize_name
from vgi.codegen.csharp_types import _base, _parse_docstring
from vgi.codegen.rust_request_builders import _rust_ident
from vgi.codegen.rust_schemas import snake_case
from vgi.protocol import CatalogContentsResponse, SchemaContents, VgiProtocol

if TYPE_CHECKING:
    from typing import TextIO


GENERATOR_VERSION = "1"

#: The records emitted as Rust structs, in output order. Data-driven: add a
#: record here (it must carry an ``ARROW_SCHEMA`` that `rust_schemas` emits a
#: factory for — an ``EXTRA_RESPONSE_TYPES`` entry or a unary method result) and
#: delete its hand-written counterpart in vgi-rust.
RUST_TYPES: tuple[type, ...] = (
    CatalogAttachResult,
    SchemaContents,
    CatalogContentsResponse,
)


@dataclass
class _Field:
    wire_name: str
    ident: str
    rust_type: str
    doc: str | None


@dataclass
class _Struct:
    name: str
    schema_fn: str
    fields: list[_Field]
    doc: str | None


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------

_SCALARS: list[tuple[pa.DataType, str]] = [
    (pa.bool_(), "bool"),
    (pa.int8(), "i8"),
    (pa.int16(), "i16"),
    (pa.int32(), "i32"),
    (pa.int64(), "i64"),
    (pa.uint8(), "u8"),
    (pa.uint16(), "u16"),
    (pa.uint32(), "u32"),
    (pa.uint64(), "u64"),
    (pa.float32(), "f32"),
    (pa.float64(), "f64"),
    (pa.string(), "String"),
    (pa.binary(), "Bytes"),
    (pa.large_binary(), "LargeBytes"),
]

#: Whole-type aliases the SDK already declares (`protocol::dtos`), matched first.
_ALIASES: list[tuple[pa.DataType, str]] = [
    (pa.dictionary(pa.int16(), pa.string()), "DictString"),
    (pa.map_(pa.string(), pa.string()), "StrMap"),
    (pa.map_(pa.string(), pa.int64()), "IntMap"),
    (pa.timestamp("us", tz="UTC"), "UtcTimestamp"),
]

#: Names imported from `vgi_rpc` / `crate::protocol::dtos` when used.
_VGI_RPC_NAMES = frozenset({"Bytes", "LargeBytes", "DictString", "UtcTimestamp"})
_DTO_NAMES = frozenset({"StrMap", "IntMap"})


def _rust_type(dtype: pa.DataType, *, origin: str) -> str:
    """The Rust type whose `VgiArrow` derive produces *dtype* (nullability aside)."""
    for proto, name in _ALIASES:
        if dtype.equals(proto):
            return name
    for proto, name in _SCALARS:
        if dtype.equals(proto):
            return name
    if pa.types.is_list(dtype):
        item = dtype.value_field
        # vgi_rpc derives every Vec<T> item as a nullable field named "item".
        if item.name != "item" or not item.nullable:
            raise GeneratorError(f"{origin}: VgiArrow derives list items as nullable 'item'; got {item}.")
        return f"Vec<{_rust_type(item.type, origin=f'{origin}[item]')}>"
    raise GeneratorError(
        f"vgi.codegen.rust_types: no Rust type for Arrow type {dtype} at {origin}.\n"
        "Add a mapping to _rust_type() in vgi/codegen/rust_types.py, and make sure the "
        "VgiArrow derive round-trips it (the emitted schema_parity test checks).",
    )


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------


def _schema_fn_names() -> dict[type, str]:
    """Map each record class to the `protocol_schemas.rs` factory emitting its schema."""
    out: dict[type, str] = {}
    methods = rpc_methods(VgiProtocol)
    for method_name in sorted(methods):
        info = methods[method_name]
        if info.method_type != MethodType.UNARY or not info.has_return:
            continue
        result, _ = _base(info.result_type)
        if isinstance(result, type) and result not in out:
            out[result] = snake_case(sanitize_name(method_name) + "Result") + "_schema"
    return out


def _layout(cls: type, schema_fns: dict[type, str]) -> _Struct:
    schema = getattr(cls, "ARROW_SCHEMA", None)
    if not isinstance(schema, pa.Schema):
        raise GeneratorError(f"{cls.__name__} has no ARROW_SCHEMA.")
    summary, attr_docs = _parse_docstring(cls)
    fields: list[_Field] = []
    for f in schema:
        where = f"{cls.__name__}.{f.name}"
        ty = _rust_type(f.type, origin=where)
        if f.nullable:
            ty = f"Option<{ty}>"
        fields.append(_Field(f.name, _rust_ident(f.name), ty, attr_docs.get(f.name)))
    schema_fn = schema_fns.get(cls, snake_case(sanitize_name(cls.__name__)) + "_schema")
    return _Struct(cls.__name__, schema_fn, fields, summary)


def build_model() -> list[_Struct]:
    """Lay out every record in `RUST_TYPES`."""
    schema_fns = _schema_fn_names()
    structs = [_layout(cls, schema_fns) for cls in RUST_TYPES]
    names = [s.name for s in structs]
    if len(names) != len(set(names)):
        raise GeneratorError(f"duplicate Rust struct names: {names}")
    return structs


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _rustdoc_text(text: str) -> str:
    """Docstring prose as rustdoc: mkdocstrings cross-refs and RST literals become code spans."""
    text = re.sub(r"\[`([^`]+)`\]\[[^\]]*\]", r"`\1`", text)
    text = re.sub(r"``([^`]+)``", r"`\1`", text)
    text = re.sub(r":\w+:`([^`]+)`", r"`\1`", text)
    # Any bracket left is prose, not an intra-doc link.
    return text.replace("[", "\\[").replace("]", "\\]")


def _doc_lines(text: str | None, indent: str) -> list[str]:
    if not text:
        return []
    paragraphs = [" ".join(p.split()) for p in re.split(r"\n\s*\n", text) if p.strip()]
    out: list[str] = []
    for i, para in enumerate(paragraphs):
        if i:
            out.append(f"{indent}///")
        wrapped = textwrap.wrap(_rustdoc_text(para), width=88, break_long_words=False, break_on_hyphens=False)
        out.extend(f"{indent}/// {line}" for line in wrapped)
    return out


def _render_struct(s: _Struct) -> str:
    lines = _doc_lines(s.doc, "")
    if lines:
        lines.append("///")
    lines.append(f"/// Derived schema must equal `generated::protocol_schemas::{s.schema_fn}()`.")
    lines.append("#[derive(Debug, Clone, VgiArrow)]")
    lines.append(f"pub struct {s.name} {{")
    for f in s.fields:
        lines += _doc_lines(f.doc, "    ")
        lines.append(f"    pub {f.ident}: {f.rust_type},")
    lines.append("}")
    return "\n".join(lines) + "\n"


def _used_names(structs: list[_Struct]) -> set[str]:
    used: set[str] = set()
    for s in structs:
        for f in s.fields:
            used |= set(re.findall(r"\w+", f.rust_type))
    return used


def emit(out: TextIO) -> None:
    """Emit the generated Rust protocol record structs to *out*."""
    structs = build_model()
    used = _used_names(structs)

    body = io.StringIO()
    body.write("// Copyright 2025, 2026 Query Farm LLC - https://query.farm\n")
    body.write("\n")
    body.write("//! Protocol record structs, generated from the canonical Python records'\n")
    body.write("//! `ARROW_SCHEMA`. Encode and decode them with [`crate::wire::to_batch`] /\n")
    body.write("//! [`crate::wire::from_batch`] (or `to_result_batch` for a unary result).\n")
    body.write("//!\n")
    body.write("//! Field order is column order and `Option<T>` is a nullable column, so each\n")
    body.write("//! struct's derived schema equals its factory in\n")
    body.write("//! [`crate::generated::protocol_schemas`]; the `schema_parity` test below checks.\n")
    body.write("\n")
    rpc_names = sorted((used & _VGI_RPC_NAMES) | {"VgiArrow"})
    body.write(f"use vgi_rpc::{{{', '.join(rpc_names)}}};\n")
    dto_names = sorted(used & _DTO_NAMES)
    if dto_names:
        body.write("\n")
        joined = dto_names[0] if len(dto_names) == 1 else "{" + ", ".join(dto_names) + "}"
        body.write(f"use crate::protocol::dtos::{joined};\n")
    for s in structs:
        body.write("\n")
        body.write(_render_struct(s))

    body.write("\n")
    body.write("#[cfg(test)]\n")
    body.write("mod schema_parity {\n")
    body.write("    //! Every generated struct's derived schema must equal the protocol's\n")
    body.write("    //! schema factory for it. If the `VgiArrow` derive's type mapping ever\n")
    body.write("    //! disagrees with the canonical protocol, this fails here rather than as a\n")
    body.write("    //! malformed batch on the wire.\n")
    body.write("\n")
    body.write("    use super::*;\n")
    body.write("    use crate::generated::protocol_schemas;\n")
    body.write("    use crate::wire::flat_schema;\n")
    body.write("\n")
    body.write("    #[test]\n")
    body.write("    fn derived_schemas_match_the_protocol() {\n")
    for s in structs:
        body.write("        assert_eq!(\n")
        body.write(f"            flat_schema::<{s.name}>(),\n")
        body.write(f"            protocol_schemas::{s.schema_fn}(),\n")
        body.write(f'            "{s.name} derives a schema that does not match {s.schema_fn}()",\n')
        body.write("        );\n")
    body.write("    }\n")
    body.write("}\n")

    out.write(
        provenance_comment(
            generator_module="vgi.codegen.rust_types",
            generator_command="python -m vgi.codegen.rust_types",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=[
                "uv run --project ~/Development/vgi-python python scripts/regen_generated.py",
            ],
            body=body.getvalue(),
        )
    )
    out.write("\n")
    out.write(body.getvalue())


def main() -> None:
    """Console-script entrypoint — write the Rust record structs to stdout."""
    try:
        emit(sys.stdout)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
