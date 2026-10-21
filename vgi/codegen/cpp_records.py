# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit the VGI protocol's records and enums as typed C++ structs, for the DuckDB extension.

Sister module to `vgi.codegen.csharp_types` / `vgi.codegen.rust_types`. The C++
client used to build every request record and parse every response record by
hand, field by field, against schemas this package already knows
(`vgi.codegen.cpp_schemas`). Each hand-written copy is a place where a field
name, a type or a nullability can drift from the protocol, and two such drifts
reached production before `test_request_schema_parity.cpp` existed to catch
them. This module generates the records instead:

- ``vgi_protocol_records.hpp`` — one plain struct per record and one
  ``enum class`` per protocol enum. No Arrow types beyond forward declarations,
  so it is cheap to include anywhere.
- ``vgi_protocol_records.cpp`` — ``ToBatch(record)`` / ``FromBatch(batch, row,
  record, worker_path)`` for every record, plus the struct/enum codecs nested
  records use. The bodies call the small runtime in the extension's
  ``vgi_wire_codec.hpp``, which is the only place that touches Arrow arrays, so
  a different Arrow backend replaces that runtime, not the call sites.

The record set, names and enums are the same ones `csharp_types` derives (the
record's ``ARROW_SCHEMA`` is the authority for every column; the Python
annotation only picks the C++ type — an enum versus a dictionary string, the
nested record a struct column holds). Decoding is by column name: a missing
nullable column reads as ``std::nullopt`` (or the record's default for an
appended column), a missing or null required column is a protocol error, and a
column of the wrong type is a protocol error rather than a silent default.

.. code-block:: bash

   uv run --project ~/Development/vgi-python python scripts/regen_generated.py

``tests/test_generated_cpp_records.py`` fails when the checked-in files and the
generator disagree.
"""

from __future__ import annotations

import enum
import io
import re
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from vgi.codegen import csharp_types as cs
from vgi.codegen._common import (
    DEFAULT_CPP_NAMESPACE,
    GeneratorError,
    close_namespace,
    open_namespace,
    parse_cpp_namespace,
    provenance_comment,
)
from vgi.codegen.cpp_schemas import _na_emit_field

if TYPE_CHECKING:
    from typing import TextIO


GENERATOR_VERSION = "1"

HEADER_NAME = "vgi_protocol_records.hpp"

#: C++ keywords (and the macros Windows headers define) a wire name could collide with.
_RESERVED = frozenset(
    {
        "alignas", "alignof", "and", "asm", "auto", "bool", "break", "case", "catch", "char", "class",
        "const", "constexpr", "continue", "decltype", "default", "delete", "do", "double", "else",
        "enum", "explicit", "export", "extern", "false", "float", "for", "friend", "goto", "if",
        "inline", "int", "long", "mutable", "namespace", "new", "noexcept", "not", "nullptr",
        "operator", "or", "private", "protected", "public", "register", "return", "short", "signed",
        "sizeof", "static", "struct", "switch", "template", "this", "throw", "true", "try",
        "typedef", "typeid", "typename", "union", "unsigned", "using", "virtual", "void",
        "volatile", "while", "xor", "min", "max", "ERROR", "DELETE", "IN", "OUT", "OPTIONAL",
    }
)  # fmt: skip


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

#: C++ names beyond csharp_types.CSHARP_NAMES: method results are named after their
#: method, matching their vgi_protocol_schemas / vgi_secret_protocol_schemas factory.
CPP_NAMES: dict[str, str] = {
    "SecretLookupResponse": "SecretLookupResult",
}


def cpp_name(py_name: str) -> str:
    """The C++ type name for a protocol type's Python name."""
    return CPP_NAMES.get(py_name) or cs.csharp_name(py_name)


class Presence(enum.Enum):
    """How a column's absence or null is decoded."""

    #: Non-nullable column: missing or null is a protocol error.
    REQUIRED = "Required"
    #: Nullable column held as ``std::optional<T>``.
    OPTIONAL = "Optional"
    #: Nullable column the record types as a plain value with a default: an
    #: appended column a peer that predates it does not send. Missing or null
    #: decodes to the default; encoding always writes the value.
    DEFAULTED = "Defaulted"


@dataclass
class _Member:
    wire_name: str
    ident: str
    #: The value type, without the ``std::optional`` a nullable column adds.
    cpp_type: str
    presence: Presence
    initializer: str | None
    doc: str | None

    @property
    def declared_type(self) -> str:
        return f"std::optional<{self.cpp_type}>" if self.presence is Presence.OPTIONAL else self.cpp_type


@dataclass
class _Record:
    name: str
    origins: list[str]
    schema: pa.Schema
    members: list[_Member]
    doc: str | None


@dataclass
class _EnumMember:
    ident: str
    wire_name: str


@dataclass
class _Enum:
    name: str
    origin: str
    members: list[_EnumMember]
    doc: str | None


@dataclass
class _Model:
    records: dict[str, _Record] = field(default_factory=dict)
    enums: dict[str, _Enum] = field(default_factory=dict)
    pending: list[tuple[type, list[pa.Field[Any]], str]] = field(default_factory=list)


def _ident(name: str) -> str:
    ident = re.sub(r"\W", "_", name)
    if ident[:1].isdigit() or ident in _RESERVED:
        ident += "_"
    return ident


_SCALARS: list[tuple[pa.DataType, str]] = [
    (pa.bool_(), "bool"),
    (pa.int8(), "int8_t"),
    (pa.int16(), "int16_t"),
    (pa.int32(), "int32_t"),
    (pa.int64(), "int64_t"),
    (pa.uint8(), "uint8_t"),
    (pa.uint16(), "uint16_t"),
    (pa.uint32(), "uint32_t"),
    (pa.uint64(), "uint64_t"),
    (pa.float32(), "float"),
    (pa.float64(), "double"),
    (pa.string(), "std::string"),
    (pa.large_string(), "std::string"),
    (pa.binary(), "Bytes"),
    (pa.large_binary(), "LargeBytes"),
    (pa.timestamp("us", tz="UTC"), "TimestampMicros"),
]


def _register_enum(cls: type[enum.Enum], model: _Model) -> str:
    name = cpp_name(cls.__name__)
    existing = model.enums.get(name)
    if existing is not None:
        if existing.origin != cls.__qualname__:
            raise GeneratorError(f"two enums map to the C++ name {name!r}.")
        return name
    summary, _ = cs._parse_docstring(cls)
    # The wire value is the member NAME (vgi_rpc dictionary-encodes enums by name).
    members = [_EnumMember(_ident(cs._pascal(m.name)), m.name) for m in cls]
    model.enums[name] = _Enum(name, cls.__qualname__, members, summary)
    return name


def _map(dtype: pa.DataType, annotation: object, *, model: _Model, origin: str) -> str:
    """The C++ value type for one Arrow type (the annotation picks among equivalents)."""
    base, _ = cs._base(annotation)

    if pa.types.is_dictionary(dtype):
        if dtype.equals(pa.dictionary(pa.int32(), pa.binary())):
            return "Bytes"
        if not (dtype.index_type.equals(pa.int16()) and dtype.value_type.equals(pa.string()) and not dtype.ordered):
            raise GeneratorError(
                f"{origin}: only dictionary<int16, utf8> and dictionary<int32, binary> are supported, got {dtype}."
            )
        if isinstance(base, type) and issubclass(base, enum.Enum):
            return _register_enum(base, model)
        return "std::string"

    for proto, name in _SCALARS:
        if dtype.equals(proto):
            return name

    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        item = dtype.value_field
        args = cs._type_args(base)
        item_ann = args[0] if args else None
        inner = _map(item.type, item_ann, model=model, origin=f"{origin}[item]")
        _, item_optional = cs._base(item_ann)
        if item_optional:
            inner = f"std::optional<{inner}>"
        return f"std::vector<{inner}>"

    if pa.types.is_map(dtype):
        args = cs._type_args(base)
        k = _map(dtype.key_type, args[0] if args else None, model=model, origin=f"{origin}[key]")
        v_ann = args[1] if len(args) > 1 else None
        v = _map(dtype.item_type, v_ann, model=model, origin=f"{origin}[value]")
        _, value_optional = cs._base(v_ann)
        if value_optional:
            v = f"std::optional<{v}>"
        return f"std::map<{k}, {v}>"

    if pa.types.is_struct(dtype):
        if not isinstance(base, type):
            raise GeneratorError(f"{origin}: a struct needs an annotated class to name its C++ type.")
        model.pending.append((base, [dtype.field(i) for i in range(dtype.num_fields)], origin))
        return cpp_name(base.__name__)

    raise GeneratorError(
        f"vgi.codegen.cpp_records: unsupported Arrow type {dtype} at {origin}.\n"
        "Add a case to _map() in vgi/codegen/cpp_records.py and to the extension's vgi_wire_codec.hpp.",
    )


def _literal(value: object, cpp_type: str, model: _Model, origin: str) -> str | None:
    """A C++ initializer for a Python default, or ``None`` when value-initialization already is it."""
    if isinstance(value, enum.Enum):
        e = model.enums.get(cpp_type)
        if e is None:
            raise GeneratorError(f"{origin}: enum default {value!r} for non-enum type {cpp_type}.")
        ident = _ident(cs._pascal(value.name))
        return None if ident == e.members[0].ident else f"{cpp_type}::{ident}"
    if isinstance(value, bool):
        return "true" if value else None
    if isinstance(value, int | float):
        return None if value == 0 else repr(value)
    if isinstance(value, str):
        return None if not value else '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
    if isinstance(value, bytes | list | tuple | frozenset | set | dict) and not value:
        return None
    if isinstance(value, list | tuple) and all(isinstance(v, str) for v in value):
        return "{" + ", ".join('"' + v + '"' for v in value) + "}"
    raise GeneratorError(f"{origin}: cannot render the default {value!r} in C++.")


def _layout(cls: type, arrow_fields: list[pa.Field[Any]], origin: str, model: _Model) -> _Record:
    name = cpp_name(cls.__name__)
    hints = cs._hints(cls)
    dc_fields = cs._dataclass_fields(cls)
    summary, attr_docs = cs._parse_docstring(cls)
    members: list[_Member] = []
    for f in arrow_fields:
        where = f"{name}.{f.name}"
        annotation = hints.get(f.name)
        cpp_type = _map(f.type, annotation, model=model, origin=where)
        _, annotated_optional = cs._base(annotation)
        has_default, default = cs._default_of(dc_fields.get(f.name))
        if not f.nullable:
            presence = Presence.REQUIRED
        elif annotation is not None and not annotated_optional and has_default and default is not None:
            presence = Presence.DEFAULTED
        else:
            presence = Presence.OPTIONAL
        initializer = None
        if presence is not Presence.OPTIONAL and has_default and default is not None:
            initializer = _literal(default, cpp_type, model, where)
        members.append(_Member(f.name, _ident(f.name), cpp_type, presence, initializer, attr_docs.get(f.name)))
    idents = [m.ident for m in members]
    if len(idents) != len(set(idents)):
        raise GeneratorError(f"{name}: two columns map to the same C++ member name: {idents}")
    return _Record(name, [origin], pa.schema(arrow_fields), members, cs._MERGED_DOCS.get(name, summary))


def _secret_protocol_roots() -> list[tuple[type, str]]:
    """Unary results of :class:`vgi.secret_protocol.VgiSecretProtocol` (the C++ client reads them too)."""
    from vgi_rpc.rpc._types import MethodType, rpc_methods  # type: ignore[attr-defined]

    from vgi.secret_protocol import VgiSecretProtocol

    roots: list[tuple[type, str]] = []
    methods = rpc_methods(VgiSecretProtocol)
    for method_name in sorted(methods):
        info = methods[method_name]
        if info.method_type != MethodType.UNARY or not info.has_return:
            continue
        result, _ = cs._base(info.result_type)
        if isinstance(getattr(result, "ARROW_SCHEMA", None), pa.Schema):
            roots.append((result, f"secret method '{method_name}' result"))  # type: ignore[arg-type]
    return roots


def build_model() -> _Model:
    """Lay out every record and enum the protocol defines, as C++ sees them."""
    model = _Model()
    for cls, origin in (*cs._root_types(), *_secret_protocol_roots()):
        model.pending.append((cls, list(cls.ARROW_SCHEMA), origin))  # type: ignore[attr-defined]
    while model.pending:
        cls, arrow_fields, origin = model.pending.pop(0)
        record = _layout(cls, arrow_fields, origin, model)
        existing = model.records.get(record.name)
        if existing is None:
            model.records[record.name] = record
            continue
        if not existing.schema.equals(record.schema):
            raise GeneratorError(
                f"{record.name}: {origin} and {existing.origins[0]} map to the same C++ type but their "
                f"schemas differ:\n  {existing.schema}\n  {record.schema}",
            )
        existing.origins.append(origin)
    clash = set(model.records) & set(model.enums)
    if clash:
        raise GeneratorError(f"names used for both a record and an enum: {sorted(clash)}")
    return model


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _doc(text: str | None, indent: str) -> list[str]:
    """The first paragraph of a docstring as ``//!`` lines."""
    if not text:
        return []
    para = " ".join(re.split(r"\n\s*\n", text.strip())[0].split())
    para = re.sub(r"\[`([^`]+)`\]\[[^\]]*\]", r"`\1`", para)
    para = re.sub(r"``([^`]+)``", r"`\1`", para)
    para = re.sub(r":\w+:`([^`]+)`", r"`\1`", para)
    words, lines, line = para.split(), [], ""
    for w in words:
        if line and len(line) + 1 + len(w) > 100:
            lines.append(line)
            line = w
        else:
            line = f"{line} {w}" if line else w
    if line:
        lines.append(line)
    return [f"{indent}//! {ln}" for ln in lines]


def _render_header(model: _Model, namespace: list[str]) -> str:
    out = io.StringIO()
    out.write("#pragma once\n\n")
    out.write("// Typed VGI protocol records. Encode/decode with ToBatch()/FromBatch()\n")
    out.write("// (vgi_protocol_records.cpp), over the nanoarrow-backed types of vgi_arrow.hpp.\n\n")
    out.write('#include "vgi_arrow.hpp"\n\n')
    for inc in ("cstdint", "map", "memory", "optional", "string", "utility", "vector"):
        out.write(f"#include <{inc}>\n")
    out.write("\n")
    out.write(open_namespace(namespace))
    out.write("\n")
    out.write(
        "using ::duckdb::vgi::VgiBatchPtr;\nusing ::duckdb::vgi::VgiBufferPtr;\nusing ::duckdb::vgi::VgiSchemaPtr;\n\n"
    )
    out.write("using Bytes = std::vector<uint8_t>;\n")
    out.write("//! A large_binary value, shared rather than copied (filter and join-key payloads can be large).\n")
    out.write("using LargeBytes = VgiBufferPtr;\n")
    out.write("//! timestamp[us, tz=UTC]: microseconds since the Unix epoch.\n")
    out.write("using TimestampMicros = int64_t;\n\n")

    out.write("// " + "-" * 76 + "\n// Enums. Enumerator order is the wire dictionary order.\n// " + "-" * 76 + "\n\n")
    for e in model.enums.values():
        out.write("\n".join(_doc(e.doc, "")) + ("\n" if e.doc else ""))
        out.write(f"enum class {e.name} : int16_t {{\n")
        for em in e.members:
            out.write(f"\t{em.ident},\n")
        out.write("};\n")
        out.write(f"//! The wire name of a {e.name} value.\n")
        out.write(f"const char *ToWire({e.name} value);\n")
        out.write(f"//! The {e.name} with this wire name, or std::nullopt.\n")
        out.write(f"std::optional<{e.name}> {e.name}FromWire(const std::string &wire);\n\n")

    out.write("// " + "-" * 76 + "\n// Records.\n// " + "-" * 76 + "\n\n")
    # Forward-declare so a record can hold one declared later.
    for r in model.records.values():
        out.write(f"struct {r.name};\n")
    out.write("\n")
    for r in _dependency_order(model):
        out.write("\n".join(_doc(r.doc, "")) + ("\n" if r.doc else ""))
        out.write(f"//! Protocol type: {', '.join(sorted(set(r.origins)))}.\n")
        out.write(f"struct {r.name} {{\n")
        for m in r.members:
            for line in _doc(m.doc, "\t"):
                out.write(line + "\n")
            init = f" = {m.initializer}" if m.initializer else "{}"
            out.write(f"\t{m.declared_type} {m.ident}{init};\n")
        out.write("};\n")
        out.write(f"bool operator==(const {r.name} &a, const {r.name} &b);\n")
        out.write(f"inline bool operator!=(const {r.name} &a, const {r.name} &b) {{\n\treturn !(a == b);\n}}\n")
        out.write(f"//! A one-row batch with {r.name}'s exact protocol schema.\n")
        out.write(f"VgiBatchPtr ToBatch(const {r.name} &value);\n")
        out.write(
            f"//! Decode row `row` of `batch` (protocol errors name `worker_path`).\n"
            f"void FromBatch(const VgiBatchPtr &batch, int64_t row, {r.name} &out, const std::string &worker_path);\n\n"
        )

    out.write("// " + "-" * 76 + "\n// Methods.\n// " + "-" * 76 + "\n\n")
    out.write("//! One VgiProtocol method's wire schemas (vgi_protocol_schemas.hpp factories).\n")
    out.write("struct MethodSchemas {\n")
    out.write("\tconst char *method;\n")
    out.write("\t//! The outer request batch (`<Method>ParamsSchema`).\n")
    out.write("\tVgiSchemaPtr params;\n")
    out.write("\t//! The record a unary result carries (`<Method>ResultSchema`); null when the method\n")
    out.write("\t//! returns nothing, raw bytes, or is a stream.\n")
    out.write("\tVgiSchemaPtr result;\n")
    out.write("\t//! False for a unary method that returns None.\n")
    out.write("\tbool returns_value;\n")
    out.write("};\n")
    out.write("//! Every VgiProtocol method, by name order.\n")
    out.write("const std::vector<MethodSchemas> &AllMethodSchemas();\n\n")
    out.write("// Test hooks (test/cpp/test_protocol_records.cpp).\n")
    out.write("//! Every record's name with a one-row batch of a default-constructed instance.\n")
    out.write("std::vector<std::pair<std::string, VgiBatchPtr>> DefaultRecordBatchesForTesting();\n")
    out.write("//! Records that have a vgi_protocol_schemas.hpp factory: name, record schema, factory schema.\n")
    out.write("struct RecordSchemaPair {\n\tstd::string name;\n\tVgiSchemaPtr record;\n")
    out.write("\tVgiSchemaPtr factory;\n};\n")
    out.write("std::vector<RecordSchemaPair> RecordSchemaPairsForTesting();\n\n")
    out.write("//! Decode row `row` of `batch` as a T (any record above).\n")
    out.write("template <class T>\n")
    out.write("T Decode(const VgiBatchPtr &batch, int64_t row, const std::string &worker_path) {\n")
    out.write("\tT out;\n\tFromBatch(batch, row, out, worker_path);\n\treturn out;\n}\n\n")
    out.write(close_namespace(namespace))
    return out.getvalue()


def _dependency_order(model: _Model) -> list[_Record]:
    """Records ordered so a struct member's type is complete before it is used by value."""
    done: set[str] = set()
    order: list[_Record] = []

    def visit(r: _Record, stack: tuple[str, ...]) -> None:
        if r.name in done:
            return
        if r.name in stack:
            raise GeneratorError(f"recursive record nesting: {' -> '.join((*stack, r.name))}")
        for m in r.members:
            for dep in re.findall(r"\w+", m.cpp_type):
                if dep in model.records and dep != r.name:
                    visit(model.records[dep], (*stack, r.name))
        done.add(r.name)
        order.append(r)

    for r in model.records.values():
        visit(r, ())
    return order


def _render_source(model: _Model, namespace: list[str]) -> str:
    out = io.StringIO()
    out.write(f'#include "{HEADER_NAME}"\n\n')
    out.write('#include "vgi_protocol_schemas.hpp"\n')
    out.write('#include "vgi_secret_protocol_schemas.hpp"\n')
    out.write('#include "vgi_wire_codec.hpp"\n\n')
    out.write(open_namespace(namespace))
    out.write("\nnamespace wire = ::duckdb::vgi::wire;\n")
    out.write(
        "using ::duckdb::vgi::MakeSchema;\nusing ::duckdb::vgi::VgiBatchBuilder;\nusing ::duckdb::vgi::VgiColumn;\n"
    )
    out.write("namespace type = ::duckdb::vgi::type;\n")
    out.write("// The codec's overloads join the generated ones (records and enums find each other by ADL).\n")
    out.write("using wire::ReadValue;\nusing wire::WriteValue;\n\n")

    # Enums.
    for e in model.enums.values():
        wires = ", ".join(f'"{m.wire_name}"' for m in e.members)
        out.write(f"static const char *const k{e.name}Wire[] = {{{wires}}};\n")
        out.write(f"static constexpr int16_t k{e.name}Count = {len(e.members)};\n\n")
        out.write(f"const char *ToWire({e.name} value) {{\n")
        out.write("\tauto i = static_cast<int16_t>(value);\n")
        out.write(f'\treturn i >= 0 && i < k{e.name}Count ? k{e.name}Wire[i] : "";\n}}\n\n')
        out.write(f"std::optional<{e.name}> {e.name}FromWire(const std::string &wire) {{\n")
        out.write(f"\tfor (int16_t i = 0; i < k{e.name}Count; i++) {{\n")
        out.write(f"\t\tif (wire == k{e.name}Wire[i]) {{\n\t\t\treturn static_cast<{e.name}>(i);\n\t\t}}\n\t}}\n")
        out.write("\treturn std::nullopt;\n}\n\n")
        out.write(f"void ReadValue(const VgiColumn &col, int64_t i, {e.name} &out, const wire::Ctx &ctx) {{\n")
        out.write("\tstd::string s;\n\twire::ReadValue(col, i, s, ctx);\n")
        out.write(f"\tauto v = {e.name}FromWire(s);\n")
        out.write(f'\tif (!v) {{\n\t\twire::ThrowBadEnum(ctx, "{e.name}", s);\n\t}}\n')
        out.write("\tout = *v;\n}\n\n")
        out.write(f"void WriteValue(ArrowArray *array, {e.name} value) {{\n")
        out.write(f"\twire::WriteEnum(array, static_cast<int16_t>(value), k{e.name}Wire, k{e.name}Count);\n}}\n\n")

    # Records: schema factories, nested-struct codecs (declared first so any order works), batch codecs.
    out.write("// Nested-struct codecs, declared up front so records can nest in any order.\n")
    for r in model.records.values():
        out.write(f"void ReadValue(const VgiColumn &col, int64_t i, {r.name} &out, const wire::Ctx &ctx);\n")
        out.write(f"void WriteValue(ArrowArray *array, const {r.name} &value);\n")
    out.write("\n")

    for r in model.records.values():
        fields = ",\n\t    ".join(_na_emit_field(f, origin=f"{r.name}.{f.name}") for f in r.schema)
        out.write(f"static const VgiSchemaPtr &{r.name}RecordSchema() {{\n")
        out.write(
            f"\tstatic const auto schema = MakeSchema({{\n\t    {fields},\n\t}});\n"
            if r.members
            else "\tstatic const auto schema = MakeSchema({});\n"
        )
        out.write("\treturn schema;\n}\n\n")

        out.write(f"bool operator==(const {r.name} &a, const {r.name} &b) {{\n")
        if r.members:
            conds = " &&\n\t       ".join(f"wire::Equal(a.{m.ident}, b.{m.ident})" for m in r.members)
            out.write(f"\treturn {conds};\n}}\n\n")
        else:
            out.write("\t(void)a;\n\t(void)b;\n\treturn true;\n}\n\n")

        out.write(f"template <class Reader>\nstatic void Read{r.name}(Reader &r, {r.name} &out) {{\n")
        if not r.members:
            out.write("\t(void)r;\n\t(void)out;\n")
        for m in r.members:
            out.write(f'\tr.{m.presence.value}("{m.wire_name}", out.{m.ident});\n')
        out.write("}\n\n")

        out.write(f"void ReadValue(const VgiColumn &col, int64_t i, {r.name} &out, const wire::Ctx &ctx) {{\n")
        out.write(f'\twire::StructReader r(col, i, ctx, "{r.name}");\n\tRead{r.name}(r, out);\n}}\n\n')
        out.write(f"void WriteValue(ArrowArray *array, const {r.name} &value) {{\n")
        if not r.members:
            out.write("\t(void)value;\n")
        for idx, m in enumerate(r.members):
            out.write(f"\tWriteValue(array->children[{idx}], value.{m.ident});\n")
        out.write("\t::duckdb::vgi::append::FinishElement(array);\n}\n\n")

        out.write(f"VgiBatchPtr ToBatch(const {r.name} &value) {{\n")
        out.write(f"\tVgiBatchBuilder b({r.name}RecordSchema());\n")
        if not r.members:
            out.write("\t(void)value;\n")
        for idx, m in enumerate(r.members):
            out.write(f"\tWriteValue(b.column({idx}), value.{m.ident});\n")
        out.write("\tb.FinishRow();\n\treturn b.Finish();\n}\n\n")
        out.write(
            f"void FromBatch(const VgiBatchPtr &batch, int64_t row, {r.name} &out, const std::string &worker_path) {{\n"
        )
        out.write(f'\twire::RowReader r(batch, row, "{r.name}", worker_path);\n')
        out.write("\twire::CurrentBatchScope scope(batch.get());\n")
        out.write(f"\tRead{r.name}(r, out);\n}}\n\n")

    out.write("const std::vector<MethodSchemas> &AllMethodSchemas() {\n")
    out.write("\tstatic const std::vector<MethodSchemas> methods = {\n")
    for method, params, result, returns in _method_rows():
        result_expr = f"{result}()" if result else "nullptr"
        out.write(f'\t    {{"{method}", {params}(), {result_expr}, {"true" if returns else "false"}}},\n')
    out.write("\t};\n\treturn methods;\n}\n\n")

    factories = _factory_names()
    out.write("std::vector<std::pair<std::string, VgiBatchPtr>> DefaultRecordBatchesForTesting() {\n")
    out.write("\treturn {\n")
    for r in model.records.values():
        out.write(f'\t    {{"{r.name}", ToBatch({r.name} {{}})}},\n')
    out.write("\t};\n}\n\n")
    out.write("std::vector<RecordSchemaPair> RecordSchemaPairsForTesting() {\n\treturn {\n")
    for r in model.records.values():
        if r.name in factories:
            out.write(f'\t    {{"{r.name}", {r.name}RecordSchema(), {r.name}Schema()}},\n')
    out.write("\t};\n}\n\n")
    out.write(close_namespace(namespace))
    return out.getvalue()


def _method_rows() -> list[tuple[str, str, str | None, bool]]:
    """(method, params factory, result factory or None, returns a value) for every VgiProtocol method."""
    from vgi_rpc.rpc._types import MethodType, rpc_methods  # type: ignore[attr-defined]

    from vgi.codegen._common import _resolve_inner_schema, sanitize_name
    from vgi.protocol import VgiProtocol

    rows: list[tuple[str, str, str | None, bool]] = []
    methods = rpc_methods(VgiProtocol)
    for name in sorted(methods):
        info = methods[name]
        stem = sanitize_name(name)
        result = None
        if (
            info.method_type == MethodType.UNARY
            and info.has_return
            and _resolve_inner_schema(info.result_type, name) is not None
        ):
            result = f"{stem}ResultSchema"
        rows.append((name, f"{stem}ParamsSchema", result, bool(info.has_return)))
    return rows


def _factory_names() -> set[str]:
    """Stems of the schema factories vgi_protocol_schemas.hpp emits (see cpp_schemas.emit)."""
    from vgi.codegen._common import EXTRA_RESPONSE_TYPES, REQUEST_TYPES, collect_schemas
    from vgi.secret_protocol import VgiSecretProtocol

    names = {s.name for s in collect_schemas(extra_response_types=(*EXTRA_RESPONSE_TYPES, *REQUEST_TYPES))}
    secret = collect_schemas(VgiSecretProtocol, info_types=(), extra_response_types=(), check_info_subclasses=False)
    return names | {s.name for s in secret}


_REGEN = ["uv run --project ~/Development/vgi-python python scripts/regen_generated.py"]


def emit(out: TextIO, record: str = "hpp", namespace: list[str] | None = None) -> None:
    """Emit the records header (``record="hpp"``) or source (``record="cpp"``) to *out*."""
    ns = namespace if namespace is not None else parse_cpp_namespace(DEFAULT_CPP_NAMESPACE)
    model = build_model()
    if record == "hpp":
        body = _render_header(model, ns)
    elif record == "cpp":
        body = _render_source(model, ns)
    else:
        raise GeneratorError(f"cpp_records: unknown output {record!r} (expected 'hpp' or 'cpp').")
    out.write(
        provenance_comment(
            generator_module="vgi.codegen.cpp_records",
            generator_command="python -m vgi.codegen.cpp_records",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=_REGEN,
            body=body,
        )
    )
    out.write("\n")
    out.write(body)


def main() -> None:
    """Console-script entrypoint: ``python -m vgi.codegen.cpp_records {hpp|cpp}``."""
    which = sys.argv[1] if len(sys.argv) > 1 else "hpp"
    try:
        emit(sys.stdout, which)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
