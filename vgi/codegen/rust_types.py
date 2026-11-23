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
import typing
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
from vgi_rpc.rpc._types import MethodType, rpc_methods  # type: ignore[attr-defined]

from vgi.catalog.catalog_interface import CatalogAttachResult
from vgi.codegen._common import (
    SHARED_CONTRACT_TYPES,
    GeneratorError,
    nested_binary_record,
    provenance_comment,
    sanitize_name,
)
from vgi.codegen.csharp_types import _base, _hints, _parse_docstring
from vgi.codegen.rust_request_builders import _rust_ident
from vgi.codegen.rust_schemas import snake_case
from vgi.protocol import (
    INVALID_ARGUMENTS_REF_KIND,
    INVALID_BIND_REQUEST_KIND,
    INVALID_BIND_REQUEST_MESSAGE,
    CatalogContentsResponse,
    SchemaContents,
    VgiProtocol,
)

if TYPE_CHECKING:
    from typing import TextIO


GENERATOR_VERSION = "1"

#: The records emitted as Rust structs, in output order. Data-driven: add a
#: record here (it must carry an ``ARROW_SCHEMA`` that `rust_schemas` emits a
#: factory for — an ``EXTRA_RESPONSE_TYPES`` entry or a unary method result) and
#: delete its hand-written counterpart in vgi-rust.
RUST_TYPES: tuple[type, ...] = (
    *SHARED_CONTRACT_TYPES,
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
    #: The record a raw-preserving `Nested<T>` field decodes to, if this is one.
    nested: str | None = None
    nullable: bool = False


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


def _list_item_optional(annotation: object) -> tuple[object, bool]:
    """``(item annotation, item is Optional)`` for a list field's Python annotation."""
    base, _ = _base(annotation)
    args = typing.get_args(base) if base is not None else ()
    if not args:
        return None, False
    item, optional = _base(args[0])
    return (args[0] if item is not None else None), optional


def _rust_type(dtype: pa.DataType, *, origin: str, annotation: object = None) -> str:
    """The Rust type whose `VgiArrow` derive produces *dtype* (nullability aside).

    *annotation* is the field's Python type hint. Arrow marks every list item
    nullable, so only the hint says whether an item may actually be null
    (``list[str | None]``): those become ``Vec<Option<T>>``, which derives the
    same schema but keeps a null item null instead of decoding it as `T`'s
    empty value.
    """
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
        item_annotation, item_optional = _list_item_optional(annotation)
        inner = _rust_type(item.type, origin=f"{origin}[item]", annotation=item_annotation)
        return f"Vec<Option<{inner}>>" if item_optional else f"Vec<{inner}>"
    if pa.types.is_struct(dtype):
        # A `VgiArrow`-derived struct is itself a `Struct<fields>` Arrow type, so
        # a struct column is the emitted record whose schema is this struct.
        for record in RUST_TYPES:
            if pa.struct(list(record.ARROW_SCHEMA)).equals(dtype):  # type: ignore[attr-defined]
                return record.__name__
        raise GeneratorError(f"{origin}: no record in RUST_TYPES has the struct schema {dtype}.")
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
    hints = _hints(cls)
    for f in schema:
        where = f"{cls.__name__}.{f.name}"
        record = nested_binary_record(f.type, hints.get(f.name))
        if record is not None:
            if record not in RUST_TYPES:
                raise GeneratorError(f"{where}: nested record {record.__name__} is not in RUST_TYPES.")
            ty = f"Nested<{record.__name__}>"
        else:
            ty = _rust_type(f.type, origin=where, annotation=hints.get(f.name))
        if f.nullable:
            ty = f"Option<{ty}>"
        fields.append(
            _Field(
                f.name,
                _rust_ident(f.name),
                ty,
                attr_docs.get(f.name),
                nested=record.__name__ if record is not None else None,
                nullable=f.nullable,
            )
        )
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


#: The raw-preserving lazy wrapper of vgi docs/large-values-3.4.md §3.3, emitted
#: once when any record carries a nested record as binary (`NESTED_BINARY_RECORDS`).
#: Each numbered rule there is noted where this code meets it.
_NESTED_RUNTIME = """\
/// A nested protocol record carried as a complete Arrow IPC stream in a `binary`
/// column, kept as the bytes it arrived as (vgi `docs/large-values-3.4.md` §3.3).
///
/// - Holds the received bytes verbatim and never re-serializes them: encoding
///   the embedding record writes exactly these bytes, whether or not the value
///   was read (rules 1, 4, 10), so the extension's byte-identity checks on
///   `bind_call` hold.
/// - Decodes lazily: the first [`Nested::get`] decodes with the generated
///   record decoder and caches the result (or the error) for every later call
///   and every clone; concurrent first calls decode once (rules 2, 3).
/// - The decoded value is only ever lent out immutably (rule 5). To change a
///   field, build a new wrapper with [`Nested::from_record`] (rule 6).
/// - Equality and hashing are over the held bytes (rule 8). An empty value is a
///   decode error at access (rule 9). `Debug` prints the length, never the value.
/// - Opened sealed values (e.g. attach data) belong in a per-call overlay beside
///   the wrapper, never in it (rule 11): nothing here can change the held bytes.
pub struct Nested<T> {
    bytes: ::std::sync::Arc<[u8]>,
    decoded:
        ::std::sync::Arc<::std::sync::OnceLock<::std::result::Result<T, ::std::string::String>>>,
}

impl<T> Nested<T> {
    /// Wrap bytes received elsewhere (e.g. a `bind_call` kept from an earlier
    /// request) without decoding them (rule 7).
    pub fn from_bytes(bytes: impl Into<::std::sync::Arc<[u8]>>) -> Self {
        Self {
            bytes: bytes.into(),
            decoded: ::std::sync::Arc::new(::std::sync::OnceLock::new()),
        }
    }

    /// The held bytes, exactly as received or as first serialized.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Whether the value has been decoded (by this wrapper or a clone of it).
    pub fn is_decoded(&self) -> bool {
        self.decoded.get().is_some()
    }
}

impl<T: ::vgi_rpc::VgiArrow> Nested<T> {
    /// Serialize `record` once with the generated encoder and hold those bytes;
    /// from then on it behaves exactly like a wrapper decoded from them (rule 6).
    pub fn from_record(record: T) -> ::vgi_rpc::Result<Self> {
        let batch = crate::wire::to_batch(record)?;
        Ok(Self::from_bytes(crate::ipc::write_batch(&batch)?))
    }

    /// The decoded record, decoding on first access. A decode error names the
    /// record type; the generated `<field>_value()` accessors name the field path.
    pub fn get(&self) -> ::vgi_rpc::Result<&T> {
        self.get_at(&T::describe_name())
    }

    /// [`Nested::get`], with decode errors prefixed by `path`
    /// (e.g. `InitRequest.bind_call`).
    pub fn get_at(&self, path: &str) -> ::vgi_rpc::Result<&T> {
        match self
            .decoded
            .get_or_init(|| Self::decode(&self.bytes).map_err(|e| e.to_string()))
        {
            Ok(value) => Ok(value),
            Err(cause) => Err(::vgi_rpc::RpcError::type_error(format!("{path}: {cause}"))),
        }
    }

    fn decode(bytes: &[u8]) -> ::vgi_rpc::Result<T> {
        if bytes.is_empty() {
            return Err(::vgi_rpc::RpcError::type_error(
                "nested record value is empty",
            ));
        }
        crate::wire::from_batch::<T>(&crate::ipc::read_batch(bytes)?)
    }
}

impl<T> Clone for Nested<T> {
    /// Shares the held bytes and the decode cache; never re-encodes (rule 10).
    fn clone(&self) -> Self {
        Self {
            bytes: ::std::sync::Arc::clone(&self.bytes),
            decoded: ::std::sync::Arc::clone(&self.decoded),
        }
    }
}

impl<T> ::std::fmt::Debug for Nested<T> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(
            f,
            "Nested<{}>({} bytes)",
            ::std::any::type_name::<T>(),
            self.bytes.len()
        )
    }
}

impl<T> PartialEq for Nested<T> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl<T> Eq for Nested<T> {}

impl<T> ::std::hash::Hash for Nested<T> {
    fn hash<H: ::std::hash::Hasher>(&self, state: &mut H) {
        self.bytes.hash(state);
    }
}

impl<T: ::vgi_rpc::VgiArrow> ::vgi_rpc::VgiArrow for Nested<T> {
    fn arrow_data_type() -> ::arrow_schema::DataType {
        ::arrow_schema::DataType::Binary
    }

    fn describe_name() -> ::std::string::String {
        "bytes".into()
    }

    /// Copies the column value's bytes; does not decode them (rule 1).
    fn read(arr: &dyn ::arrow_array::Array, idx: usize) -> ::vgi_rpc::Result<Self> {
        Ok(Self::from_bytes(
            <Bytes as ::vgi_rpc::VgiArrow>::read(arr, idx)?.0,
        ))
    }

    /// Writes the held bytes verbatim (rule 4).
    fn build_singleton(value: Self) -> ::vgi_rpc::Result<::arrow_array::ArrayRef> {
        <Bytes as ::vgi_rpc::VgiArrow>::build_singleton(Bytes(value.bytes.to_vec()))
    }
}
"""

#: Hand-written validation the protocol attaches to a record, emitted beside its
#: generated struct. Python enforces the same rules in the dataclass
#: (`vgi.protocol.BindRequest.__post_init__`, `ArgumentsLocation.__post_init__`);
#: the error kinds and message are the protocol's (large-values spec §2, §5a).
_VALIDATIONS: dict[str, str] = {
    "BindRequest": f"""\
impl BindRequest {{
    /// Exactly one of `arguments` and `arguments_location` must be set; anything
    /// else is `{INVALID_BIND_REQUEST_KIND}` (`INVALID_ARGUMENT`).
    pub fn validate_arguments(&self) -> ::vgi_rpc::Result<()> {{
        if self.arguments.is_some() == self.arguments_location.is_some() {{
            return Err(::vgi_rpc::RpcError::value_error(
                "{INVALID_BIND_REQUEST_MESSAGE}",
            )
            .with_status(::vgi_rpc::Code::InvalidArgument, "{INVALID_BIND_REQUEST_KIND}"));
        }}
        if let Some(location) = &self.arguments_location {{
            location.validate()?;
        }}
        Ok(())
    }}
}}
""",
    "ArgumentsLocation": f"""\
impl ArgumentsLocation {{
    /// `size` must be at least 1 and `sha256` 64 lowercase hex digits; anything
    /// else is `{INVALID_ARGUMENTS_REF_KIND}` (`INVALID_ARGUMENT`).
    pub fn validate(&self) -> ::vgi_rpc::Result<()> {{
        let refuse = |message: &str| {{
            Err(::vgi_rpc::RpcError::value_error(message)
                .with_status(::vgi_rpc::Code::InvalidArgument, "{INVALID_ARGUMENTS_REF_KIND}"))
        }};
        if self.size < 1 {{
            return refuse("arguments_location.size must be a positive byte count");
        }}
        if self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {{
            return refuse("arguments_location.sha256 must be 64 lowercase hex digits");
        }}
        Ok(())
    }}
}}
""",
}


#: rustfmt's default `max_width`; vgi-rust CI runs `cargo fmt --check` with no
#: config, so generated code must already be formatted.
_RUSTFMT_WIDTH = 100
#: rustfmt's default `chain_width`: a longer method chain goes one call per line.
_RUSTFMT_CHAIN_WIDTH = 60


def _chain(head: str, calls: list[str], indent: int) -> list[str]:
    """A method chain as rustfmt lays it out: one line if it fits, else one call per line."""
    pad = " " * indent
    chain = head + "".join(f".{c}" for c in calls)
    if len(chain) <= _RUSTFMT_CHAIN_WIDTH and indent + len(chain) <= _RUSTFMT_WIDTH:
        return [pad + chain]
    return [pad + head, *(f"{pad}    .{c}" for c in calls)]


def _render_accessors(s: _Struct) -> str:
    """`<field>_value()` accessors for a struct's `Nested<T>` fields (rule 3's field path)."""
    nested = [f for f in s.fields if f.nested is not None]
    if not nested:
        return ""
    lines = [f"impl {s.name} {{"]
    for i, f in enumerate(nested):
        if i:
            lines.append("")
        path = f"{s.name}.{f.wire_name}"
        lines.append(f"    /// `{f.wire_name}` decoded on first access; a decode error names `{path}`.")
        if f.nullable:
            lines.append(f"    pub fn {f.wire_name}_value(&self) -> ::vgi_rpc::Result<Option<&{f.nested}>> {{")
            lines += _chain(f"self.{f.ident}", ["as_ref()", f'map(|n| n.get_at("{path}"))', "transpose()"], 8)
        else:
            lines.append(f"    pub fn {f.wire_name}_value(&self) -> ::vgi_rpc::Result<&{f.nested}> {{")
            lines += _chain(f"self.{f.ident}", [f'get_at("{path}")'], 8)
        lines.append("    }")
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
    if any(f.nested for s in structs for f in s.fields):
        used.add("Bytes")  # the Nested runtime reads and writes through `Bytes`
    rpc_names = sorted((used & _VGI_RPC_NAMES) | {"VgiArrow"})
    body.write(f"use vgi_rpc::{{{', '.join(rpc_names)}}};\n")
    dto_names = sorted(used & _DTO_NAMES)
    if dto_names:
        body.write("\n")
        joined = dto_names[0] if len(dto_names) == 1 else "{" + ", ".join(dto_names) + "}"
        body.write(f"use crate::protocol::dtos::{joined};\n")
    if any(f.nested for s in structs for f in s.fields):
        body.write("\n")
        body.write(_NESTED_RUNTIME)
    for s in structs:
        body.write("\n")
        body.write(_render_struct(s))
        accessors = _render_accessors(s)
        if accessors:
            body.write("\n")
            body.write(accessors)
        if s.name in _VALIDATIONS:
            body.write("\n")
            body.write(_VALIDATIONS[s.name])

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
