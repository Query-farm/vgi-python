# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit vgi-go's ``vgi.v2`` registry: the service interface, its UNIMPLEMENTED base and the method table.

vgi-rpc-go registers a method explicitly -- ``vgirpc.Unary[P, R](s, name,
handler)`` -- and derives its schemas by reflection over the Go types: the
params schema from ``P``'s ``vgirpc:"..."`` struct tags (a pointer is a nullable
column, ``,enum`` is ``dictionary<int16, utf8>``) unless ``P`` declares
``VgiRpcParamsSchema()``; the ``result`` column from ``R`` (a struct is
``binary`` holding its IPC, ``[]byte`` is ``binary``, a pointer makes either
nullable). So for Go the registry is three generated pieces (see
:mod:`vgi.codegen._registry` for the model and the reasons):

- **The params types.** A method whose parameters are plain columns gets a
  ``<Method>Params`` struct, one tagged field per column, mapped by
  :mod:`vgi.codegen.go_types`. A method that carries one packed record in a
  ``request`` column keeps vgi-go's hand-written record struct (which
  vgi-rpc-go unwraps the request into); the generated file gives that struct
  its ``VgiRpcParamsSchema()``, the way ``stringer`` adds ``String()`` to a
  hand-written type, so the advertised ``request: binary`` comes from here.
- **``vgiService``**, every ``vgi.v2`` method with its exact params and result
  types, and **``unimplementedVgiService``**, whose every method answers
  ``methodNotImplemented`` -- ``UNIMPLEMENTED`` / ``method_not_implemented`` /
  ``"<method> is not implemented by this worker"``. vgi-go's concrete service
  embeds it (the gRPC-Go idiom) and defines what it implements.
- **``registerVgiService``**, the registration table. The routable catalog
  methods (:attr:`~vgi.codegen._registry.RegistryMethod.is_catalog`) go
  through vgi-go's ``unaryCatalog`` / ``unaryVoidCatalog`` (opaque-value
  unsealing and routing to sub-catalogs), split out as
  ``registerVgiCatalogMethods`` so a sub-catalog can record them without a
  server.

Two small hand-maintained tables choose Go names; neither can change a schema,
because nullability, pointers and every column type come from the model:

- :data:`GO_RECORD_TYPES` -- the Go struct for a packed record whose name does
  not follow vgi-go's ``<Record>Wire`` convention. A record emitted by
  :mod:`vgi.codegen.go_types` is ``generated.<Record>`` automatically.
- :data:`GO_RAW_RESULTS` -- a method whose Python result is raw IPC ``bytes``
  that vgi-go returns as a typed struct instead (the same non-null ``binary``
  column).

.. code-block:: bash

   uv run --project ~/Development/vgi-python python scripts/regen_generated.py

:class:`GoRegistry` is this language's backend for the one registry
generator (:mod:`vgi.codegen._registry_backend`); its ``derive`` reads the
rendered file back by vgi-rpc-go's rules (struct tags, ``VgiRpcParamsSchema``,
the registered result type).
"""

from __future__ import annotations

import re
import textwrap
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from vgi.codegen import go_schemas, go_types
from vgi.codegen._common import GeneratorError
from vgi.codegen._registry import (
    MethodKind,
    RegistryMethod,
    check_raw_results,
    check_record_table,
    identifiers,
    paragraphs,
    registry_methods,
    require_binary,
)
from vgi.codegen._registry_backend import ArrowDialect, DerivedMethod, RegistryBackend, Tamper, expect

#: The generated registry, relative to the vgi-go checkout.
TARGET = "vgi/vgi_service_gen.go"

#: The Go struct of a packed record that does not follow the ``<Record>Wire``
#: convention. Go identifiers only: whether it is a pointer is the model's call.
GO_RECORD_TYPES: dict[str, str] = {
    # One Go type serves both table-function requests (identical records).
    "TableFunctionCardinalityRequest": "CardinalityRequestWire",
    "TableFunctionStatisticsRequest": "CardinalityRequestWire",
    "TableFunctionPlanRequest": "PlanRequestWire",
    "TableCardinality": "TableCardinality",
    # Every per-kind listing is one `items: list<binary>` record.
    "CopyFromFormatsResponse": "ItemsResponseWire",
    "FunctionsResponse": "ItemsResponseWire",
    "IndexesResponse": "ItemsResponseWire",
    "MacrosResponse": "ItemsResponseWire",
    "SchemasResponse": "ItemsResponseWire",
    "TablesResponse": "ItemsResponseWire",
    "ViewsResponse": "ItemsResponseWire",
}

#: Methods whose Python result is raw IPC ``bytes`` that vgi-go returns as a
#: typed struct (the same non-null ``binary`` column, encoded by vgi-rpc-go).
GO_RAW_RESULTS: dict[str, str] = {
    "catalog_table_delete_function_get": "TableScanFunctionGetResponseWire",
    "catalog_table_insert_function_get": "TableScanFunctionGetResponseWire",
    "catalog_table_scan_branches_get": "TableScanBranchesGetResponseWire",
    "catalog_table_scan_function_get": "TableScanFunctionGetResponseWire",
    "catalog_table_update_function_get": "TableScanFunctionGetResponseWire",
}

#: The schema every wrapped method advertises (``request: binary``).
REQUEST_SCHEMA_VAR = "vgiRequestParamsSchema"

#: vgi-go's catalog registration helpers (unseal opaque values, route).
CATALOG_UNARY = "unaryCatalog"
CATALOG_UNARY_VOID = "unaryVoidCatalog"

#: vgi-go's UNIMPLEMENTED error constructor.
NOT_IMPLEMENTED = "methodNotImplemented"

_IDENT = re.compile(r"^[A-Z][A-Za-z0-9]*$")


@dataclass(frozen=True)
class GoMethod:
    """One ``vgi.v2`` method as vgi-go declares and registers it."""

    method: RegistryMethod
    #: The Go method name (``CatalogSchemas``).
    name: str
    #: The params type: a generated ``<Method>Params`` or a hand-written record struct.
    params_type: str
    #: The generated params struct, or ``None`` for a wrapped record.
    params_struct: go_types.GoRecord | None
    #: The result type, or ``None`` for a void unary (and for streams).
    result_type: str | None
    #: The stream header's schema var, or ``None``.
    header_var: str | None


def _record_type(py_name: str, origin: str) -> str:
    """The Go type of a packed record."""
    if py_name in {cls.__name__ for cls in go_types.GO_TYPE_RECORDS}:
        if py_name in GO_RECORD_TYPES:
            raise GeneratorError(f"{origin}: {py_name} is generated by go_types; drop it from GO_RECORD_TYPES")
        return f"generated.{go_types.go_name(py_name)}"
    return GO_RECORD_TYPES.get(py_name, f"{py_name}Wire")


def _check_tables(methods: Sequence[RegistryMethod]) -> None:
    for table, values in (("GO_RECORD_TYPES", GO_RECORD_TYPES), ("GO_RAW_RESULTS", GO_RAW_RESULTS)):
        bad = sorted(v for v in values.values() if not _IDENT.match(v))
        if bad:
            raise GeneratorError(f"{table} values must be plain exported Go identifiers, got {bad}")
    check_record_table(GO_RECORD_TYPES, methods, what="GO_RECORD_TYPES", headers=False)
    # A struct derives a non-null binary result; anything else would change the schema.
    check_raw_results(GO_RAW_RESULTS, methods, what="GO_RAW_RESULTS", non_null=True)


def _params(m: RegistryMethod, pascal: str, request_field: list[pa.Field[Any]]) -> tuple[str, go_types.GoRecord | None]:
    record = m.params[0].record if len(m.params) == 1 and m.params[0].name == "request" else None
    if record is not None:
        field = m.params[0].field
        require_binary(field, m.name, "a packed request")
        if request_field and not request_field[0].equals(field):
            raise GeneratorError(f"{m.name}: request column {field} differs from {request_field[0]}")
        request_field[:] = [field]
        go = _record_type(record, m.name)
        if "." in go:
            raise GeneratorError(f"{m.name}: cannot declare VgiRpcParamsSchema on {go} from another package")
        return go, None

    name = f"{pascal}Params"
    fields: list[go_types.GoField] = []
    for p in m.params:
        origin = f"{m.name}({p.name})"
        if p.record is not None:
            raise GeneratorError(f"{origin}: a packed record outside a lone `request` column is not supported")
        go, options = go_types._map(p.field.type, p.annotation, origin=origin)
        if p.field.nullable:
            go = "*" + go
        fields.append(go_types.GoField(p.name, go_types.go_field_name(p.name), go, options, None))
    names = [f.name for f in fields]
    if len(set(names)) != len(names):
        raise GeneratorError(f"{m.name}: two parameters map to one Go field name: {names}")
    schema = pa.schema([p.field for p in m.params])
    return name, go_types.GoRecord(name, m.name, schema, fields, None)


def _result(m: RegistryMethod) -> str | None:
    if m.is_stream or m.result_field is None:
        return None
    f = m.result_field
    require_binary(f, m.name)
    star = "*" if f.nullable else ""
    raw = GO_RAW_RESULTS.get(m.name)
    if raw is not None:
        return raw
    if m.result_record is not None:
        return star + _record_type(m.result_record, m.name)
    if m.result_annotation is bytes:
        return star + "[]byte"
    raise GeneratorError(f"{m.name}: unsupported result annotation {m.result_annotation!r}")


def header_var(m: RegistryMethod) -> str:
    """The schema var name for a stream's header."""
    return "vgi" + go_types.go_field_name(m.name) + "HeaderSchema"


def go_methods(methods: Sequence[RegistryMethod] | None = None) -> tuple[list[GoMethod], pa.Field[Any]]:
    """Every ``vgi.v2`` method as vgi-go declares it, and the shared ``request`` column."""
    methods = registry_methods() if methods is None else methods
    _check_tables(methods)
    names = identifiers((m.name for m in methods), go_types.go_field_name, what="Go method")
    request_field: list[pa.Field[Any]] = []
    out: list[GoMethod] = []
    for m in methods:
        pascal = names[m.name]
        params_type, struct = _params(m, pascal, request_field)
        if m.is_stream and m.is_catalog:
            raise GeneratorError(f"{m.name}: a catalog stream has no vgi-go registration helper")
        hv = header_var(m) if m.header_schema is not None else None
        out.append(GoMethod(m, pascal, params_type, struct, _result(m), hv))
    if not request_field:
        raise GeneratorError("no vgi.v2 method carries a packed request")
    return out, request_field[0]


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _comment(paras: list[str], indent: str = "") -> list[str]:
    out: list[str] = []
    for i, para in enumerate(paras):
        if i:
            out.append(f"{indent}//")
        for line in textwrap.wrap(para, width=76, break_long_words=False, break_on_hyphens=False):
            out.append(f"{indent}// {line}")
    return out


def _doc(m: RegistryMethod) -> str:
    first = paragraphs(m.doc)[:1]
    text = go_types._go_text(first[0]) if first else f"The {m.name} method."
    return go_types._sentence(" ".join(text.split()))


def _signature(gm: GoMethod, *, named: bool) -> str:
    args = "ctx context.Context, cc *vgirpc.CallContext, req " if named else "context.Context, *vgirpc.CallContext, "
    if gm.method.is_stream:
        ret = "(*vgirpc.StreamResult, error)"
    elif gm.result_type is None:
        ret = "error"
    else:
        ret = f"({gm.result_type}, error)"
    return f"{gm.name}({args}{gm.params_type}) {ret}"


def _zero(go_type: str) -> str:
    return "nil" if go_type.startswith(("*", "[]")) else go_type + "{}"


def _schema_var(name: str, fields: list[pa.Field[Any]], origin: str) -> list[str]:
    if not fields:
        return [f"var {name} = arrow.NewSchema([]arrow.Field{{}}, nil)"]
    lines = [f"var {name} = arrow.NewSchema([]arrow.Field{{"]
    for f in fields:
        lines.append("\t" + go_schemas._emit_field_literal(f, origin=f"{origin}.{f.name}") + ",")
    lines.append("}, nil)")
    return lines


def _struct(r: go_types.GoRecord) -> list[str]:
    lines = _comment(
        [
            f"{r.name} is the params of vgi.v2 {r.py_name}: one field per params-schema column, in order. "
            "vgi-rpc-go derives the advertised schema from these tags."
        ]
    )
    if not r.fields:
        lines.append(f"type {r.name} struct{{}}")
        return lines
    # gofmt alignment: names, then types, padded with spaces to a common column.
    nw = max(len(f.name) for f in r.fields)
    tw = max(len(f.go_type) for f in r.fields)
    lines.append(f"type {r.name} struct {{")
    for f in r.fields:
        lines.append(f'\t{f.name.ljust(nw)} {f.go_type.ljust(tw)} `vgirpc:"{f.tag}"`')
    lines.append("}")
    return lines


def _registration(gm: GoMethod) -> str:
    m = gm.method
    handler = f"svc.{gm.name}"
    if m.kind is MethodKind.STREAM:
        header = gm.header_var or "nil"
        return f'\tvgirpc.DynamicStreamWithHeader[{gm.params_type}](s, "{m.name}", {header}, {handler})'
    helper = {
        (True, MethodKind.UNARY): CATALOG_UNARY,
        (True, MethodKind.VOID): CATALOG_UNARY_VOID,
        (False, MethodKind.UNARY): "vgirpc.Unary",
        (False, MethodKind.VOID): "vgirpc.UnaryVoid",
    }[m.is_catalog, m.kind]
    type_args = gm.params_type if gm.result_type is None else f"{gm.params_type}, {gm.result_type}"
    worker = "w, " if m.is_catalog else ""
    return f'\t{helper}[{type_args}]({worker}s, "{m.name}", {handler})'


_SERVICE_DOC = [
    "vgiService is the vgi.v2 protocol: every method a VGI worker serves, generated from the "
    "reference (vgi.protocol.VgiProtocol in vgi-python).",
    "registerVgiService hosts every method, so vgi_rpc.Reflection.v1 reports the same vgi.v2 hash "
    "from every SDK: the params and result types below derive the reference's schemas exactly. The "
    "protocol is the unit of optionality -- every method is registered whether or not this worker "
    "implements it. An implementation embeds unimplementedVgiService and defines the methods it "
    "serves; the rest answer UNIMPLEMENTED. Never change a signature here by hand: regenerate it, so "
    "the change comes from the protocol.",
]


def _render_body(model: Sequence[RegistryMethod]) -> str:
    methods, request_field = go_methods(model)
    uses_generated = any("generated." in (gm.result_type or "") + gm.params_type for gm in methods)

    lines = [
        "// Copyright 2025, 2026 Query Farm LLC - https://query.farm",
        "",
        "package vgi",
        "",
        "import (",
        '\t"context"',
        "",
    ]
    if uses_generated:
        lines.append('\t"github.com/Query-farm/vgi-go/vgi/generated"')
    lines += [
        '\t"github.com/Query-farm/vgi-rpc-go/vgirpc"',
        '\t"github.com/apache/arrow-go/v18/arrow"',
        ")",
        "",
    ]

    # Schemas the Go types cannot carry: the wrapped `request` column and stream headers.
    lines += _comment(
        [
            f"{REQUEST_SCHEMA_VAR} is the params schema of every method that carries one packed "
            "record: a single `request` column holding the record's IPC. vgi-rpc-go unwraps it into "
            "the record struct, whose fields describe the inner batch; VgiRpcParamsSchema advertises "
            "this instead of those fields, or a client that builds its request from the advertised "
            "schema (the TypeScript client does) would find none of its keys and send all-nulls."
        ]
    )
    lines += _schema_var(REQUEST_SCHEMA_VAR, [request_field], "request")
    for gm in methods:
        header = gm.method.header_schema
        if gm.header_var is not None and header is not None:
            lines.append("")
            record = gm.method.header_type.__name__  # type: ignore[union-attr]
            lines += _comment([f"{gm.header_var} is the header of the vgi.v2 {gm.method.name} stream ({record})."])
            lines += _schema_var(gm.header_var, list(header), gm.method.name)

    # VgiRpcParamsSchema on each wrapped record struct, once per Go type.
    wrapped: dict[str, list[str]] = {}
    for gm in methods:
        if gm.params_struct is None:
            wrapped.setdefault(gm.params_type, []).append(gm.method.name)
    for go_type, names in sorted(wrapped.items()):
        lines.append("")
        lines += _comment(
            [
                f"VgiRpcParamsSchema advertises the wrapped protocol shape of vgi.v2 {', '.join(names)}: "
                f"one `request` column; vgi-rpc-go unwraps it into {go_type}'s fields."
            ]
        )
        lines.append(f"func ({go_type}) VgiRpcParamsSchema() *arrow.Schema {{")
        lines.append(f"\treturn {REQUEST_SCHEMA_VAR}")
        lines.append("}")

    for gm in methods:
        if gm.params_struct is not None:
            lines.append("")
            lines += _struct(gm.params_struct)

    lines.append("")
    lines += _comment(_SERVICE_DOC)
    lines.append("type vgiService interface {")
    for i, gm in enumerate(methods):
        if i:
            lines.append("")
        kind = "stream" if gm.method.is_stream else "unary"
        lines += _comment([f"{gm.name} is vgi.v2 {gm.method.name} ({kind}). {_doc(gm.method)}"], "\t")
        lines.append(f"\t{_signature(gm, named=True)}")
    lines.append("}")

    lines.append("")
    lines += _comment(
        [
            "unimplementedVgiService answers every vgi.v2 method with UNIMPLEMENTED / "
            'method_not_implemented ("<method> is not implemented by this worker"). Embed it in a '
            "vgiService and define the methods the worker serves."
        ]
    )
    lines.append("type unimplementedVgiService struct{}")
    for gm in methods:
        m = gm.method
        lines.append("")
        lines += _comment([f"{gm.name} answers UNIMPLEMENTED: vgi.v2 {m.name} is not implemented by this worker."])
        lines.append(f"func (unimplementedVgiService) {_signature(gm, named=False)} {{")
        if m.is_stream:
            lines.append(f'\treturn nil, {NOT_IMPLEMENTED}("{m.name}")')
        elif gm.result_type is None:
            lines.append(f'\treturn {NOT_IMPLEMENTED}("{m.name}")')
        else:
            lines.append(f'\treturn {_zero(gm.result_type)}, {NOT_IMPLEMENTED}("{m.name}")')
        lines.append("}")

    lines.append("")
    lines += _comment(
        [
            "registerVgiService hosts every vgi.v2 method on s, each dispatched to svc. The table is "
            "the protocol's whole method set: a method svc does not implement still registers, and "
            "answers UNIMPLEMENTED."
        ]
    )
    lines.append("func registerVgiService(w *Worker, s *vgirpc.Server, svc vgiService) {")
    lines.append("\tregisterVgiCatalogMethods(w, s, svc)")
    for gm in methods:
        if not gm.method.is_catalog:
            lines.append(_registration(gm))
    lines.append("}")

    lines.append("")
    lines += _comment(
        [
            f"registerVgiCatalogMethods hosts the catalog_* methods through {CATALOG_UNARY} / "
            f"{CATALOG_UNARY_VOID}, which unseal opaque values and route an attach to the catalog "
            "that owns it. With a nil s it only records them, for a sub-catalog."
        ]
    )
    lines.append("func registerVgiCatalogMethods(w *Worker, s *vgirpc.Server, svc vgiService) {")
    for gm in methods:
        if gm.method.is_catalog:
            lines.append(_registration(gm))
    lines.append("}")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Derive-back: vgi-rpc-go's rules (struct tags, VgiRpcParamsSchema, result types)
# ---------------------------------------------------------------------------

_SCHEMA_VAR = re.compile(r"^var (\w+) = arrow\.NewSchema\(\[\]arrow\.Field\{\n(.*?)\n\}, nil\)$", re.M | re.S)
_WRAPPED = re.compile(r"^func \((\w+)\) VgiRpcParamsSchema\(\) \*arrow\.Schema \{\n\treturn (\w+)\n\}$", re.M)
_STRUCT = re.compile(r"^type (\w+) struct(?:\{\}|\s\{\n(.*?)\n\})$", re.M | re.S)
_STRUCT_FIELD = re.compile(r'^\t(\w+)\s+(\S+)\s+`vgirpc:"([^"]+)"`$')
_REGISTER = re.compile(
    r"^\t(vgirpc\.Unary|vgirpc\.UnaryVoid|unaryCatalog|unaryVoidCatalog|vgirpc\.DynamicStreamWithHeader)"
    r'\[(.+?)\]\((?:w, )?s, "(\w+)", (?:(\w+), )?svc\.(\w+)\)$',
    re.M,
)
_STUB = re.compile(r"^func \(unimplementedVgiService\) (\w+)\(.*\n\treturn .*methodNotImplemented\(\"(\w+)\"\)$", re.M)
_SCALARS: dict[str, pa.DataType] = {
    "[]byte": pa.binary(),
    "string": pa.string(),
    "bool": pa.bool_(),
    "int64": pa.int64(),
    "[]string": pa.list_(pa.field("item", pa.string(), nullable=True)),
    "map[string]string": pa.map_(pa.string(), pa.string()),
}
_UNITS = {"Second": "s", "Millisecond": "ms", "Microsecond": "us", "Nanosecond": "ns"}

#: arrow-go field literals, as pyarrow: ``arrow.Field{Name: "x", Type: T, Nullable: true}``
#: becomes ``Field(Name="x", Type=T, Nullable=True)``.
ARROW_GO = ArrowDialect(
    rewrites=(
        (r"&?arrow\.(\w+)\.(\w+)", r"\1_\2"),
        (r"&?arrow\.(\w+)", r"\1"),
        (r"(\w+)\{", r"\1("),
        (r"\}", ")"),
        (r"(\w+): ", r"\1="),
    ),
    names={
        "Field": lambda Name, Type, Nullable=False: pa.field(Name, Type, nullable=Nullable),  # noqa: N803
        **{f"BinaryTypes_{k}": v for k, v in (("Binary", pa.binary()), ("String", pa.string()))},
        **{f"BinaryTypes_{k}": v for k, v in (("LargeBinary", pa.large_binary()), ("LargeString", pa.large_string()))},
        **{f"PrimitiveTypes_{t.capitalize()}": getattr(pa, t)() for t in ("int8", "int16", "int32", "int64")},
        **{f"PrimitiveTypes_{t.capitalize()}": getattr(pa, t)() for t in ("uint8", "uint16", "uint32", "uint64")},
        **{f"PrimitiveTypes_{t.capitalize()}": getattr(pa, t)() for t in ("float32", "float64")},
        "BooleanType": pa.bool_,
        "ListOf": pa.list_,
        "ListOfField": pa.list_,
        "LargeListOf": pa.large_list,
        "LargeListOfField": pa.large_list,
        "MapOf": pa.map_,
        "StructOf": lambda *fields: pa.struct(list(fields)),
        "DictionaryType": lambda IndexType, ValueType, Ordered=False: pa.dictionary(  # noqa: N803
            IndexType, ValueType, ordered=Ordered
        ),
        "TimestampType": lambda Unit, TimeZone="": pa.timestamp(Unit, tz=TimeZone or None),  # noqa: N803
        **_UNITS,
    },
)


def _struct_field(name: str, go: str, tag: str) -> pa.Field[Any]:
    """One struct field as vgi-rpc-go derives it: a pointer is nullable, ``,enum`` is a dictionary."""
    wire, *options = tag.split(",")
    nullable = go.startswith("*")
    go = go.removeprefix("*")
    if options == ["enum"] and go == "string":
        return pa.field(wire, pa.dictionary(pa.int16(), pa.string()), nullable=nullable)
    expect(not options, f"{name}: unexpected tag options {options}")
    expect(go in _SCALARS, f"{name}: vgi-rpc-go derives no Arrow type from {go}")
    return pa.field(wire, _SCALARS[go], nullable=nullable)


def _result_field(go: str) -> pa.Field[Any]:
    """A unary result as vgi-rpc-go derives it: a struct or ``[]byte`` is binary, a pointer nullable."""
    nullable = go.startswith("*")
    go = go.removeprefix("*")
    expect(go in _SCALARS or re.fullmatch(r"(?:generated\.)?[A-Z]\w*", go), f"unexpected result type {go}")
    return pa.field("result", _SCALARS.get(go, pa.binary()), nullable=nullable)


class GoRegistry(RegistryBackend):
    """vgi-go's ``vgi_service_gen.go``: params structs, the service, its stubs and the registration."""

    key = "go"
    language = "Go"
    module = "vgi.codegen.go_registry"
    target = TARGET
    repo = "vgi-go"
    root_env = "VGI_GO_ROOT"
    tamper = Tamper(
        start="type CatalogSchemasParams struct {",
        end="}",
        old="TransactionOpaqueData *[]byte",
        new="TransactionOpaqueData  []byte",
    )

    def render_body(self, methods: Sequence[RegistryMethod]) -> str:  # noqa: D102
        return _render_body(methods)

    def derive(self, text: str) -> list[DerivedMethod]:  # noqa: D102
        schemas = {var: list(ARROW_GO.schema(f"[{body}]")) for var, body in _SCHEMA_VAR.findall(text)}
        params = {go_type: schemas[var] for go_type, var in _WRAPPED.findall(text)}
        for name, body in _STRUCT.findall(text):
            fields = []
            for line in body.splitlines():
                fm = _STRUCT_FIELD.match(line)
                expect(fm is not None, f"{name}: unparsed field {line!r}")
                assert fm is not None
                fields.append(_struct_field(*fm.groups()))
            params[name] = fields
        stubs = dict(_STUB.findall(text))
        out: list[DerivedMethod] = []
        for helper, type_args, name, header_var, svc_method in _REGISTER.findall(text):
            expect(svc_method == go_types.go_field_name(name), f"{name} dispatches to svc.{svc_method}")
            expect(stubs.get(svc_method) == name, f"{svc_method}'s UNIMPLEMENTED stub reports {stubs.get(svc_method)}")
            p_type, *r_type = type_args.split(", ")
            expect(p_type in params, f"{name}: no params type {p_type}")
            if helper == "vgirpc.DynamicStreamWithHeader":
                header = None if header_var == "nil" else pa.schema(schemas[header_var])
                out.append(DerivedMethod(name, MethodKind.STREAM, params[p_type], header=header))
            elif helper in ("vgirpc.Unary", "unaryCatalog"):
                out.append(DerivedMethod(name, MethodKind.UNARY, params[p_type], _result_field(r_type[0])))
            else:
                out.append(DerivedMethod(name, MethodKind.VOID, params[p_type]))
        return out


BACKEND = GoRegistry()
emit, render, main = BACKEND.emit, BACKEND.render, BACKEND.main


if __name__ == "__main__":
    main()
