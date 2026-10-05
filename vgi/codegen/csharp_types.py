# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit the VGI protocol's records and enums as C# classes, for vgi-csharp.

vgi-csharp, like vgi-java, has no schema of its own to validate against: its
transport (vgi-rpc-csharp) *derives* every wire schema from a CLR type by
reflection — property declaration order is field order, ``long?`` is a nullable
``int64``, ``[LargeWidth] byte[]`` is ``large_binary``. So unlike the C++ / Go /
Rust / TypeScript emitters, which emit schema factories, this one emits the
types the schemas are derived *from*. Getting a property's order, nullability or
width wrong there is a wire-protocol bug that fails only at runtime (the C++
client compares response schemas with a strict ``arrow::Schema::Equals``), which
is exactly why they are no longer written by hand.

Field order and Arrow types come from each record's ``ARROW_SCHEMA`` — the
authority — and the Python annotation is read only to choose among CLR types
that derive the same Arrow type: an enum versus a dictionary-encoded string, a
``RecordBatch`` versus raw ``byte[]``, the name of a nested record's class.
Anything vgi-rpc-csharp's derivation cannot express raises ``GeneratorError``
rather than emitting a type whose schema would silently differ;
``vgi.codegen.csharp_schemas`` emits the protocol's schemas into vgi-csharp's
test project so the derivation is checked against them, field for field.

Multirepo workflow, same as the other emitters: modify the dataclass here, then
regenerate and commit the file in vgi-csharp.

.. code-block:: bash

   uv run --project ~/Development/vgi-python python scripts/regen_generated.py

``tests/test_generated_csharp.py`` fails when the checked-in file and the
generator disagree.
"""

from __future__ import annotations

import argparse
import dataclasses
import enum
import inspect
import io
import re
import sys
import textwrap
import typing
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from vgi_rpc.rpc._types import MethodType, rpc_methods  # type: ignore[attr-defined]
from vgi_rpc.utils import _annotation_details

from vgi.catalog.attach_option import AttachOptionSpec
from vgi.catalog.secret_type import SecretTypeSpec
from vgi.catalog.setting import SettingSpec
from vgi.codegen._common import (
    EXTRA_RESPONSE_TYPES,
    INFO_TYPES,
    REQUEST_TYPES,
    GeneratorError,
    provenance_comment,
)
from vgi.protocol import VgiProtocol

if TYPE_CHECKING:
    from typing import TextIO


GENERATOR_VERSION = "1"

DEFAULT_NAMESPACE = "QueryFarm.Vgi.Protocol"

#: Records that ride as opaque ``binary`` blobs inside another record (an
#: ``attach_option_specs: list<binary>`` entry, say), so no annotation reaches
#: them, and that the other SDKs have no generated counterpart for — they read
#: them by name. A C# worker *writes* them, and its derived schema is what the
#: C++ client reads, so they belong here.
CSHARP_BLOB_TYPES: tuple[type, ...] = (
    AttachOptionSpec,
    SettingSpec,
    SecretTypeSpec,
)

#: The C# name of a protocol type, where it is not the Python one.
#:
#: Most of these are vgi-csharp's established public names, which predate
#: generation and are kept so that generating the types is not also a breaking
#: rename: method results are named after their method, as in the C++ client's
#: generated header (``PlanResponse`` is ``TableFunctionPlanResult``), and the
#: enums a worker author writes carry a ``Vgi`` prefix where the bare name would
#: be ambiguous in C#. ``ScanSplit`` is the wire record; vgi-csharp's
#: ``QueryFarm.Vgi.Table.ScanSplit`` is the author-facing type it is built from.
#:
#: The eight catalog items responses are one type in C#. Python stamps them out
#: of one factory, with one schema, and they differ only in the name it gives
#: each; ``_emit_records`` checks that every class mapped to one C# name has
#: the same schema.
CSHARP_NAMES: dict[str, str] = {
    "ScanSplit": "ScanSplitWire",
    "TableCardinality": "TableFunctionCardinalityResult",
    "PlanResponse": "TableFunctionPlanResult",
    "TableFunctionDynamicToStringResponse": "TableFunctionDynamicToStringResult",
    "AggregateBindResponse": "AggregateBindResult",
    "AggregateUpdateResponse": "AggregateUpdateResult",
    "AggregateCombineResponse": "AggregateCombineResult",
    "AggregateFinalizeResponse": "AggregateFinalizeResult",
    "AggregateDestructorResponse": "AggregateDestructorResult",
    "AggregateWindowInitResponse": "AggregateWindowInitResult",
    "AggregateWindowResponse": "AggregateWindowResult",
    "AggregateWindowBatchResponse": "AggregateWindowBatchResult",
    "AggregateWindowDestructorResponse": "AggregateWindowDestructorResult",
    "AggregateStreamingOpenResponse": "AggregateStreamingOpenResult",
    "AggregateStreamingChunkResponse": "AggregateStreamingChunkResult",
    "AggregateStreamingCloseResponse": "AggregateStreamingCloseResult",
    "TableBufferingProcessResponse": "TableBufferingProcessResult",
    "TableBufferingCombineResponse": "TableBufferingCombineResult",
    "TableBufferingDestructorResponse": "TableBufferingDestructorResult",
    "CatalogsResponse": "ItemsResponse",
    "SchemasResponse": "ItemsResponse",
    "TablesResponse": "ItemsResponse",
    "ViewsResponse": "ItemsResponse",
    "FunctionsResponse": "ItemsResponse",
    "MacrosResponse": "ItemsResponse",
    "IndexesResponse": "ItemsResponse",
    "CopyFromFormatsResponse": "ItemsResponse",
    # Records nested as structs, named after what they describe in C#.
    "CatalogDataVersionRelease": "CatalogRelease",
    "CatalogExample": "FunctionExample",
    "SecretLookupEntry": "RequiredSecret",
    # Enums.
    "TableInOutFunctionInitPhase": "VgiInitPhase",
    "OrderByDirection": "VgiOrderByDirection",
    "OrderByNullOrder": "VgiNullOrder",
    "OrderPreservation": "VgiOrderPreservation",
    "PartitionKind": "VgiPartitionKind",
    "NullHandling": "FunctionNullHandling",
    "OrderDependence": "AggregateOrderDependent",
    "DistinctDependence": "AggregateDistinctDependent",
}

#: The XML doc summary of a type the C# name merges, in place of any one
#: member's docstring.
_MERGED_DOCS: dict[str, str] = {
    "ItemsResponse": (
        "The result of every catalog listing and lookup RPC: one IPC-serialized ``Info`` record per "
        "item, each decoded on its own. A lookup that finds nothing returns no items, not an error. "
        "Python names one class per item type (``SchemasResponse``, ``TablesResponse``, ...); they "
        "share this schema."
    ),
}

_REL_NS = "global::"
_ATTR_LARGE = f"[{_REL_NS}QueryFarm.VgiRpc.Reflection.LargeWidth]"
_ATTR_DICT = f"[{_REL_NS}QueryFarm.VgiRpc.Reflection.DictionaryEncoded]"
_ATTR_WIRE_OPTIONAL = f"[{_REL_NS}QueryFarm.Vgi.Internal.WireOptional]"


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------


@dataclass
class _Prop:
    wire_name: str
    name: str
    clr_type: str
    attrs: list[str]
    initializer: str | None
    doc: str | None


@dataclass
class _Record:
    name: str
    origins: list[str]
    schema: pa.Schema
    props: list[_Prop]
    doc: str | None


@dataclass
class _EnumMember:
    name: str
    wire_name: str
    doc: str | None


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
    #: Python classes still to lay out: (class, the Arrow fields it carries, origin).
    pending: list[tuple[type, list[pa.Field[Any]], str]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Naming
# ---------------------------------------------------------------------------


def csharp_name(py_name: str) -> str:
    """The C# type name for a protocol type's Python name."""
    return CSHARP_NAMES.get(py_name, py_name)


def _to_snake_case(identifier: str) -> str:
    """Port of vgi-rpc-csharp's ``WireNaming.ToSnakeCase``, to know when ``[RpcName]`` is needed."""
    out: list[str] = []
    for i, c in enumerate(identifier):
        if c.isupper() and i > 0:
            prev = identifier[i - 1]
            nxt = identifier[i + 1] if i + 1 < len(identifier) else ""
            if prev.islower() or prev.isdigit() or (prev.isupper() and nxt.islower()):
                out.append("_")
        out.append(c.lower())
    return "".join(out)


def _pascal(snake: str) -> str:
    return "".join(part[:1].upper() + part[1:].lower() for part in snake.split("_") if part)


def _rpc_name_attr(wire: str) -> str:
    return f'[{_REL_NS}QueryFarm.VgiRpc.Attributes.RpcName("{wire}")]'


# ---------------------------------------------------------------------------
# Docstrings
# ---------------------------------------------------------------------------

_ATTR_LINE = re.compile(r"^(\w+)(?: \([^)]*\))?:\s*(.*)$")


def _parse_docstring(obj: object) -> tuple[str | None, dict[str, str]]:
    """Split a Google-style docstring into its summary and its ``Attributes:`` entries."""
    raw = inspect.getdoc(obj) if obj is not None else None
    if not raw:
        return None, {}
    lines = raw.splitlines()
    summary: list[str] = []
    attrs: dict[str, list[str]] = {}
    section: str | None = None
    current: str | None = None
    for line in lines:
        stripped = line.strip()
        if re.fullmatch(r"[A-Z][A-Za-z ]*:", stripped) and not line.startswith(" "):
            section = stripped[:-1]
            current = None
            continue
        if section is None:
            summary.append(line)
        elif section == "Attributes":
            if line.startswith("    ") and not line.startswith("        "):
                m = _ATTR_LINE.match(stripped)
                if m:
                    current = m.group(1)
                    attrs[current] = [m.group(2)]
                    continue
            if current is not None and stripped:
                attrs[current].append(stripped)
    text = "\n".join(summary).strip()
    return (text or None), {k: " ".join(v).strip() for k, v in attrs.items()}


def _xml_text(text: str) -> str:
    """Render docstring prose as XML doc text: escaped, with code spans as ``<c>``."""
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    text = re.sub(r"\[`([^`]+)`\]\[[^\]]*\]", r"<c>\1</c>", text)
    text = re.sub(r"``([^`]+)``", r"<c>\1</c>", text)
    text = re.sub(r"`([^`]+)`", r"<c>\1</c>", text)
    return text


def _doc_lines(text: str | None, indent: str) -> list[str]:
    if not text:
        return []
    paragraphs = [" ".join(p.split()) for p in re.split(r"\n\s*\n", text) if p.strip()]
    body: list[str] = []
    for i, para in enumerate(paragraphs):
        if i:
            body.append("<para>")
        body.extend(textwrap.wrap(_xml_text(para), width=96, break_long_words=False, break_on_hyphens=False))
        if i:
            body.append("</para>")
    out = [f"{indent}/// <summary>"]
    out.extend(f"{indent}/// {line}" for line in body)
    out.append(f"{indent}/// </summary>")
    return out


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------

_SCALARS: list[tuple[pa.DataType, str]] = [
    (pa.bool_(), "bool"),
    (pa.int8(), "sbyte"),
    (pa.int16(), "short"),
    (pa.int32(), "int"),
    (pa.int64(), "long"),
    (pa.uint8(), "byte"),
    (pa.uint16(), "ushort"),
    (pa.uint32(), "uint"),
    (pa.uint64(), "ulong"),
    (pa.float32(), "float"),
    (pa.float64(), "double"),
]

_VALUE_TYPES = {name for _, name in _SCALARS} | {"global::System.DateTimeOffset", "global::System.DateTime"}


def _base(annotation: object) -> tuple[object, bool]:
    """``(type, optional)`` with ``Annotated``/``Optional``/``NewType`` peeled away."""
    if annotation is None:
        return None, False
    base, optional, _ = _annotation_details(annotation)
    while hasattr(base, "__supertype__"):
        inner, inner_optional, _ = _annotation_details(base.__supertype__)
        base, optional = inner, optional or inner_optional
    return base, optional


def _type_args(annotation: object) -> tuple[object, ...]:
    return typing.get_args(annotation) if annotation is not None else ()


class _Mapped(typing.NamedTuple):
    clr: str
    #: Attributes the property must carry for the derivation to produce this type.
    attrs: tuple[str, ...]
    is_value: bool
    #: For a non-nullable property: what to initialize it to so a fresh instance
    #: holds a valid value. ``None`` when ``default(T)`` already is one.
    empty: str | None


def _map(dtype: pa.DataType, annotation: object, *, model: _Model, origin: str, element: bool = False) -> _Mapped:
    """Map one Arrow type (and the annotation describing it, if any) to a CLR type."""
    base, _ = _base(annotation)

    if pa.types.is_dictionary(dtype):
        if not (dtype.index_type.equals(pa.int16()) and dtype.value_type.equals(pa.string()) and not dtype.ordered):
            raise GeneratorError(f"{origin}: only dictionary<int16, utf8> is expressible in C#, got {dtype}.")
        if isinstance(base, type) and issubclass(base, enum.Enum):
            return _Mapped(_register_enum(base, model), (), True, None)
        return _Mapped("string", (_ATTR_DICT,), False, '""')

    for proto, name in _SCALARS:
        if dtype.equals(proto):
            return _Mapped(name, (), True, None)

    if pa.types.is_string(dtype):
        return _Mapped("string", (), False, '""')
    if pa.types.is_large_string(dtype):
        return _Mapped("string", (_ATTR_LARGE,), False, '""')
    if pa.types.is_binary(dtype):
        # Every binary column is raw bytes in C#, including the ones Python
        # annotates as `pa.RecordBatch`/`pa.Schema` or a nested record. Python
        # itself is not consistent here (`settings` is a RecordBatch,
        # `input_batch` the same IPC stream as `bytes`), and the raw bytes are
        # what a worker hashes into split-token fingerprints, stores and forwards
        # — re-encoding a decoded batch would not reproduce them reliably.
        return _Mapped("byte[]", (), False, "[]")
    if pa.types.is_large_binary(dtype):
        return _Mapped("byte[]", (_ATTR_LARGE,), False, "[]")
    if pa.types.is_fixed_size_binary(dtype):
        attr = f"[{_REL_NS}QueryFarm.VgiRpc.Reflection.FixedBinary({dtype.byte_width})]"
        return _Mapped("byte[]", (attr,), False, f"new byte[{dtype.byte_width}]")

    if pa.types.is_timestamp(dtype):
        if dtype.unit != "us":
            raise GeneratorError(f"{origin}: vgi-rpc-csharp derives only microsecond timestamps, got {dtype}.")
        if dtype.tz is None:
            return _Mapped("global::System.DateTime", (), True, None)
        if dtype.tz == "UTC":
            return _Mapped("global::System.DateTimeOffset", (), True, None)
        raise GeneratorError(f"{origin}: vgi-rpc-csharp derives only naive or UTC timestamps, got {dtype}.")

    if pa.types.is_list(dtype):
        # A width or encoding attribute on the property reaches every level of a
        # nested list (vgi-rpc-csharp threads it down to the innermost element),
        # so an inner list's attributes hoist to the property like any element's.
        item = dtype.value_field
        if item.name != "item" or not item.nullable:
            raise GeneratorError(
                f"{origin}: vgi-rpc-csharp derives every list item as a nullable field named 'item'; got {item}.",
            )
        args = _type_args(base)
        item_ann = args[0] if args else None
        inner = _map(item.type, item_ann, model=model, origin=f"{origin}[item]", element=True)
        _, item_optional = _base(item_ann)
        clr = inner.clr + ("?" if item_optional else "")
        return _Mapped(f"global::System.Collections.Generic.List<{clr}>", inner.attrs, False, "[]")

    if pa.types.is_map(dtype):
        key, value = dtype.key_field, dtype.item_field
        if key.nullable or not value.nullable or not pa.map_(key.type, value.type).equals(dtype):
            raise GeneratorError(
                f"{origin}: vgi-rpc-csharp derives map<key not null, value nullable> with the default "
                f"field names; got {dtype}.",
            )
        args = _type_args(base)
        k = _map(key.type, args[0] if args else None, model=model, origin=f"{origin}[key]", element=True)
        v = _map(value.type, args[1] if len(args) > 1 else None, model=model, origin=f"{origin}[value]", element=True)
        if k.attrs or v.attrs:
            raise GeneratorError(f"{origin}: a width or encoding attribute cannot reach a map's key or value.")
        _, value_optional = _base(args[1] if len(args) > 1 else None)
        clr = f"global::System.Collections.Generic.Dictionary<{k.clr}, {v.clr}{'?' if value_optional else ''}>"
        return _Mapped(clr, (), False, "[]")

    if pa.types.is_struct(dtype):
        if not isinstance(base, type):
            raise GeneratorError(f"{origin}: a struct needs an annotated class to name its C# type.")
        fields = [dtype.field(i) for i in range(dtype.num_fields)]
        name = csharp_name(base.__name__)
        model.pending.append((base, fields, origin))
        return _Mapped(name, (), False, "new()")

    raise GeneratorError(
        f"vgi.codegen.csharp_types: unsupported Arrow type {dtype} at {origin}.\n"
        "Add a case to _map() in vgi/codegen/csharp_types.py — and check that vgi-rpc-csharp's "
        "SchemaDerivation can derive it.",
    )


def _register_enum(cls: type[enum.Enum], model: _Model) -> str:
    name = csharp_name(cls.__name__)
    if name in model.enums:
        if model.enums[name].origin != cls.__qualname__:
            raise GeneratorError(f"two enums map to the C# name {name!r}.")
        return name
    summary, attr_docs = _parse_docstring(cls)
    members = []
    for member in cls:
        cs_member = _pascal(member.name)
        members.append(_EnumMember(cs_member, member.name, attr_docs.get(member.name)))
    model.enums[name] = _Enum(name, cls.__qualname__, members, summary)
    return name


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------


def _dataclass_fields(cls: type) -> dict[str, dataclasses.Field[Any]]:
    return {f.name: f for f in dataclasses.fields(cls)} if dataclasses.is_dataclass(cls) else {}


def _hints(cls: type) -> dict[str, object]:
    try:
        return typing.get_type_hints(cls, include_extras=True)
    except Exception:  # noqa: BLE001 - a forward ref we cannot resolve just loses its hint
        return {f.name: f.type for f in _dataclass_fields(cls).values()}


def _literal(value: object, mapped: _Mapped, origin: str) -> str | None:
    """A C# initializer for a Python default, or ``None`` when ``default(T)`` already is it."""
    if isinstance(value, enum.Enum):
        first = next(iter(type(value)))
        return None if value is first else f"{mapped.clr}.{_pascal(value.name)}"
    if isinstance(value, bool):
        return "true" if value else None
    if isinstance(value, int | float):
        return None if value == 0 else repr(value)
    if isinstance(value, str):
        return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
    if isinstance(value, bytes | list | tuple | frozenset | set | dict) and not value:
        return "[]"
    if isinstance(value, list | tuple) and all(isinstance(v, str) for v in value):
        return "[" + ", ".join(_literal(v, mapped, origin) or '""' for v in value) + "]"
    raise GeneratorError(f"{origin}: cannot render the default {value!r} in C#.")


def _default_of(dc_field: dataclasses.Field[Any] | None) -> tuple[bool, object]:
    if dc_field is None:
        return False, None
    if dc_field.default is not dataclasses.MISSING:
        return True, dc_field.default
    if dc_field.default_factory is not dataclasses.MISSING:
        value = dc_field.default_factory()
        # A factory that mints a fresh value per instance (a random id) has no
        # constant to render; such a field is treated as having no default.
        if value != dc_field.default_factory():
            return False, None
        return True, value
    return False, None


def _layout(cls: type, arrow_fields: list[pa.Field[Any]], origin: str, model: _Model) -> _Record:
    name = csharp_name(cls.__name__)
    hints = _hints(cls)
    dc_fields = _dataclass_fields(cls)
    summary, attr_docs = _parse_docstring(cls)
    props: list[_Prop] = []
    for f in arrow_fields:
        where = f"{name}.{f.name}"
        annotation = hints.get(f.name)
        mapped = _map(f.type, annotation, model=model, origin=where)
        _, annotated_optional = _base(annotation)
        has_default, default = _default_of(dc_fields.get(f.name))

        attrs = list(mapped.attrs)
        nullable = f.nullable
        # A column the schema calls nullable but the record types as a plain value
        # with a default: an APPENDED column, which a peer that predates it does
        # not send and a null in it reads as the default. vgi-csharp's
        # [WireOptional] is exactly that contract (encode nullable, decode by
        # name), and keeps the property a plain value.
        if (
            f.nullable
            and annotation is not None
            and not annotated_optional
            and mapped.is_value
            and has_default
            and default is not None
        ):
            attrs.append(_ATTR_WIRE_OPTIONAL)
            nullable = False

        clr = mapped.clr + ("?" if nullable else "")
        if nullable:
            initializer = None if not has_default or default is None else _literal(default, mapped, where)
        elif has_default:
            initializer = _literal(default, mapped, where)
            if initializer is None and not mapped.is_value:
                initializer = mapped.empty
        else:
            initializer = mapped.empty

        prop_name = _pascal(f.name)
        if prop_name == name:
            raise GeneratorError(f"{where}: a C# member cannot share its type's name.")
        if _to_snake_case(prop_name) != f.name:
            attrs.append(_rpc_name_attr(f.name))
        props.append(_Prop(f.name, prop_name, clr, attrs, initializer, attr_docs.get(f.name)))

    # [WireOptional] columns must trail every positional one; EmbeddedIpc refuses
    # the type otherwise.
    seen_optional = False
    for p in props:
        if _ATTR_WIRE_OPTIONAL in p.attrs:
            seen_optional = True
        elif seen_optional:
            raise GeneratorError(f"{name}.{p.wire_name} follows an appended optional column.")

    return _Record(name, [origin], pa.schema(arrow_fields), props, _MERGED_DOCS.get(name, summary))


def _root_types() -> list[tuple[type, str]]:
    roots: list[tuple[type, str]] = [
        (cls, cls.__name__) for cls in (*INFO_TYPES, *EXTRA_RESPONSE_TYPES, *REQUEST_TYPES)
    ]
    roots += [(cls, cls.__name__) for cls in CSHARP_BLOB_TYPES]
    methods = rpc_methods(VgiProtocol)
    for method_name in sorted(methods):
        info = methods[method_name]
        if info.method_type != MethodType.UNARY or not info.has_return:
            continue
        result, _ = _base(info.result_type)
        if isinstance(getattr(result, "ARROW_SCHEMA", None), pa.Schema):
            roots.append((result, f"method '{method_name}' result"))  # type: ignore[arg-type]
    return roots


def build_model() -> _Model:
    """Lay out every record and enum the protocol defines, as C# sees them."""
    model = _Model()
    for cls, origin in _root_types():
        model.pending.append((cls, list(cls.ARROW_SCHEMA), origin))  # type: ignore[attr-defined]

    # Flat method parameters are declared by hand on IVgiService, but an enum
    # among them is still a protocol type.
    methods = rpc_methods(VgiProtocol)
    for method_name in sorted(methods):
        hints = typing.get_type_hints(getattr(VgiProtocol, method_name), include_extras=True)
        for param, annotation in hints.items():
            base, _ = _base(annotation)
            if param != "return" and isinstance(base, type) and issubclass(base, enum.Enum):
                _register_enum(base, model)

    while model.pending:
        cls, arrow_fields, origin = model.pending.pop(0)
        record = _layout(cls, arrow_fields, origin, model)
        existing = model.records.get(record.name)
        if existing is None:
            model.records[record.name] = record
            continue
        if not existing.schema.equals(record.schema):
            raise GeneratorError(
                f"{record.name}: {origin} and {existing.origins[0]} map to the same C# type but their "
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


def _render_enum(e: _Enum) -> str:
    lines = _doc_lines(e.doc, "")
    lines.append(f"public enum {e.name}")
    lines.append("{")
    for i, m in enumerate(e.members):
        if i:
            lines.append("")
        lines += _doc_lines(m.doc, "    ")
        if _to_snake_case(m.name).upper() != m.wire_name:
            lines.append(f"    {_rpc_name_attr(m.wire_name)}")
        lines.append(f"    {m.name},")
    lines.append("}")
    return "\n".join(lines) + "\n"


def _render_record(r: _Record) -> str:
    lines = _doc_lines(r.doc, "")
    origins = sorted(set(r.origins))
    lines.append(f"/// <remarks>Protocol type: {_xml_text(', '.join(origins))}.</remarks>")
    lines.append(f"public sealed partial class {r.name}")
    lines.append("{")
    for i, p in enumerate(r.props):
        if i:
            lines.append("")
        lines += _doc_lines(p.doc, "    ")
        lines += [f"    {a}" for a in p.attrs]
        init = f" = {p.initializer};" if p.initializer is not None else ""
        lines.append(f"    public {p.clr_type} {p.name} {{ get; set; }}{init}")
    lines.append("}")
    return "\n".join(lines) + "\n"


def emit(out: TextIO, *, namespace: str = DEFAULT_NAMESPACE) -> None:
    """Emit the generated C# protocol types to *out*."""
    model = build_model()
    body = io.StringIO()
    body.write("// Copyright 2025, 2026 Query Farm LLC - https://query.farm\n")
    body.write("// <auto-generated/>\n")
    body.write("\n")
    # Generated code is outside the nullable context unless it opts in, and
    # without it every reference-typed property would derive as nullable.
    body.write("#nullable enable\n")
    body.write("\n")
    body.write(f"namespace {namespace};\n")
    blocks = [_render_enum(model.enums[n]) for n in sorted(model.enums)]
    blocks += [_render_record(model.records[n]) for n in sorted(model.records)]
    for block in blocks:
        body.write("\n")
        body.write(block)

    out.write(
        provenance_comment(
            generator_module="vgi.codegen.csharp_types",
            generator_command="python -m vgi.codegen.csharp_types",
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
    """Console-script entrypoint — write the C# protocol types to stdout."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE, help="C# namespace of the emitted types")
    args = parser.parse_args()
    try:
        emit(sys.stdout, namespace=args.namespace)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
