# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit vgi-csharp's ``IVgiService``: the whole ``vgi.v2`` registry, for vgi-csharp.

vgi-rpc-csharp registers a protocol by reflecting over a C# interface: every
method is a wire method (``BindAsync`` is ``bind``), its parameters derive the
params schema (``byte[]?`` is a nullable ``binary``, a protocol enum is
``dictionary<int16, utf8>``, a record is ``binary`` holding the record's IPC),
its return type derives the ``result`` column, and ``[StreamHeader]`` names a
stream's header record. So the interface *is* the registration table, and this
module generates it from :class:`vgi.protocol.VgiProtocol` (see
:mod:`vgi.codegen._registry` for the model and the reasons):

- every ``vgi.v2`` method, typed with the records and enums
  :mod:`vgi.codegen.csharp_types` already generates, so the derivation
  reproduces the reference schemas exactly;
- a default interface method body on every one that throws vgi-csharp's
  ``MethodNotImplementedError`` (``UnimplementedMethod.For``) --
  ``UNIMPLEMENTED`` / ``method_not_implemented`` / ``"<method> is not
  implemented by this worker"``. ``VgiServiceImpl`` implements the interface
  and provides what it implements;
- ``[ProtocolName(VgiProtocol.Name)]``.

Parameters keep the reference's Python defaults (``= null``, ``= false``), and
every method ends with ``ICallContext? ctx = null`` (server-injected, not a wire
field).

The one hand-maintained choice is :data:`CSHARP_RAW_RESULTS`: a method whose
Python result is raw IPC ``bytes`` that vgi-csharp returns as the typed record
instead. Both are the same ``binary`` column.

.. code-block:: bash

   uv run --project ~/Development/vgi-python python scripts/regen_generated.py

:class:`CSharpRegistry` is this language's backend for the one registry
generator (:mod:`vgi.codegen._registry_backend`); its ``derive`` reads the
rendered interface back by vgi-rpc-csharp's ``SchemaDerivation`` rules.
"""

from __future__ import annotations

import enum
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from vgi.codegen import csharp_types
from vgi.codegen._common import GeneratorError
from vgi.codegen._registry import (
    NO_DEFAULT,
    MethodKind,
    RegistryMethod,
    RegistryParam,
    check_raw_results,
    identifiers,
    paragraphs,
    registry_methods,
    require_binary,
)
from vgi.codegen._registry_backend import DerivedMethod, RegistryBackend, Tamper, expect

DEFAULT_NAMESPACE = "QueryFarm.Vgi.Protocol"

#: The generated interface, relative to the vgi-csharp checkout.
TARGET = "src/QueryFarm.Vgi/Protocol/Generated/IVgiService.g.cs"

#: Methods whose Python result is raw IPC ``bytes`` that vgi-csharp returns as
#: a typed record (the same ``binary`` column, encoded by the framework).
CSHARP_RAW_RESULTS: dict[str, str] = {
    "catalog_table_scan_branches_get": "ScanBranchesResult",
}

#: The C# type a stream method returns (inside its ``Task``).
CSHARP_STREAM_TYPE = "RpcStream<StreamState>"

_CTX = "ICallContext? ctx = null"

_GENERIC_NS = "global::System.Collections.Generic."


@dataclass(frozen=True)
class CSharpParam:
    """One rendered C# parameter."""

    wire_name: str
    name: str
    clr_type: str
    attrs: tuple[str, ...]
    default: str | None
    doc: str


@dataclass(frozen=True)
class CSharpMethod:
    """One rendered interface method."""

    method: RegistryMethod
    name: str
    return_type: str
    #: The CLR result type inside the Task (``None`` for ``Task`` and streams).
    result_clr: str | None
    attrs: tuple[str, ...]
    params: tuple[CSharpParam, ...]


def _camel(snake: str) -> str:
    pascal = csharp_types._pascal(snake)
    return pascal[:1].lower() + pascal[1:]


def _default(p: RegistryParam, clr: str) -> str | None:
    if p.default is NO_DEFAULT:
        return None
    value = p.default
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, enum.Enum):
        return f"{clr}.{csharp_types._pascal(value.name)}"
    raise GeneratorError(f"{p.name}: cannot render the default {value!r} in C#")


def _param(m: RegistryMethod, p: RegistryParam, model: csharp_types._Model) -> CSharpParam:
    origin = f"{m.name}({p.name})"
    name = _camel(p.name)
    if csharp_types._to_snake_case(name) != p.name:
        raise GeneratorError(f"{origin}: C# parameter {name} does not convert back to its wire name")
    nullable = "?" if p.field.nullable else ""
    token = f"<c>{csharp_types._xml_text(str(p.field.type))}</c>{', nullable' if p.field.nullable else ''}"
    if p.record is not None:
        require_binary(p.field, origin, "a record's column")
        record = csharp_types.csharp_name(p.record)
        if record not in model.records:
            raise GeneratorError(f"{origin}: {record} is not a generated C# record")
        clr = record
        doc = f"The packed <c>{p.record}</c> ({token})."
        attrs: tuple[str, ...] = ()
    else:
        mapped = csharp_types._map(p.field.type, p.annotation, model=model, origin=origin)
        clr = mapped.clr.replace(_GENERIC_NS, "")
        attrs = mapped.attrs
        doc = f"The <c>{p.name}</c> column ({token})."
    return CSharpParam(p.name, name, clr + nullable, attrs, _default(p, clr), doc)


def _result(m: RegistryMethod, model: csharp_types._Model) -> tuple[str, str | None, tuple[str, ...]]:
    if m.is_stream:
        attrs: tuple[str, ...] = ()
        if m.header_type is not None:
            header = csharp_types.csharp_name(m.header_type.__name__)
            if header not in model.records:
                raise GeneratorError(f"{m.name}: stream header {header} is not a generated C# record")
            attrs = (f"[StreamHeader(typeof({header}))]",)
        return f"Task<{CSHARP_STREAM_TYPE}>", None, attrs
    if m.result_field is None:
        return "Task", None, ()
    require_binary(m.result_field, m.name)
    nullable = "?" if m.result_field.nullable else ""
    raw = CSHARP_RAW_RESULTS.get(m.name)
    if raw is not None:
        clr = raw
    elif m.result_record is not None:
        clr = csharp_types.csharp_name(m.result_record)
    elif m.result_annotation is bytes:
        clr = "byte[]"
    else:
        raise GeneratorError(f"{m.name}: unsupported result annotation {m.result_annotation!r}")
    if clr != "byte[]" and clr not in model.records:
        raise GeneratorError(f"{m.name}: result {clr} is not a generated C# record")
    return f"Task<{clr}{nullable}>", clr + nullable, ()


def _method_name(wire: str) -> str:
    return csharp_types._pascal(wire) + "Async"


def csharp_methods(methods: Sequence[RegistryMethod] | None = None) -> list[CSharpMethod]:
    """Every ``vgi.v2`` method as vgi-csharp declares it."""
    methods = registry_methods() if methods is None else methods
    model = csharp_types.build_model()
    check_raw_results(CSHARP_RAW_RESULTS, methods, what="CSHARP_RAW_RESULTS")
    names = identifiers((m.name for m in methods), _method_name, what="C# method")
    out = []
    for m in methods:
        ret, result_clr, attrs = _result(m, model)
        params = tuple(_param(m, p, model) for p in m.params)
        out.append(CSharpMethod(m, names[m.name], ret, result_clr, attrs, params))
    return out


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _render_method(cm: CSharpMethod) -> list[str]:
    m = cm.method
    lines = csharp_types._doc_lines("\n\n".join(paragraphs(m.doc)) or f"The {m.name} method.", "    ")
    kind = "stream" if m.is_stream else "unary"
    lines.append(
        f"    /// <remarks>Wire method <c>{m.name}</c> ({kind}). Default: answers <c>UNIMPLEMENTED</c>.</remarks>"
    )
    for p in cm.params:
        lines.append(f'    /// <param name="{p.name}">{p.doc}</param>')
    lines.append('    /// <param name="ctx">The caller\'s context, injected by the server (not a wire field).</param>')
    if m.is_stream:
        what = "The output stream"
        if m.header_type is not None:
            what += f", headed by a <c>{m.header_type.__name__}</c>"
        lines.append(f"    /// <returns>{what}.</returns>")
    elif m.result_field is not None:
        null = ", nullable" if m.result_field.nullable else ""
        lines.append(f"    /// <returns>The <c>result</c> column (<c>{m.result_field.type}</c>{null}).</returns>")
    lines += [f"    {a}" for a in cm.attrs]
    lines.append(f"    {cm.return_type} {cm.name}(")
    for p in cm.params:
        default = f" = {p.default}" if p.default is not None else ""
        decl = " ".join((*p.attrs, p.clr_type, p.name))
        lines.append(f"        {decl}{default},")
    lines.append(f"        {_CTX}) =>")
    lines.append(f'        throw UnimplementedMethod.For("{m.name}");')
    return lines


_INTERFACE_DOC = """\
/// <summary>
/// The <c>vgi.v2</c> protocol: every method a VGI worker serves, generated from the reference
/// (<c>vgi.protocol.VgiProtocol</c> in vgi-python).
/// </summary>
/// <remarks>
/// <para>vgi-rpc-csharp reflects over this interface to register the protocol, so the declarations
/// below <em>are</em> the wire contract: a parameter's CLR type and nullability derive its
/// params-schema column, the return type derives the <c>result</c> column, and
/// <c>[StreamHeader]</c> names a stream's header. They reproduce the reference's schemas exactly,
/// which is what makes <c>vgi_rpc.Reflection.v1</c> report one <c>vgi.v2</c> hash in every SDK. The
/// protocol is the unit of optionality: every method is registered, whether or not this worker
/// implements it.</para>
/// <para>Every method has a default body that answers <c>UNIMPLEMENTED</c> /
/// <c>method_not_implemented</c> with <c>"&lt;method&gt; is not implemented by this worker"</c>;
/// an implementation provides the methods it serves. A class that wraps another
/// <see cref="IVgiService"/> must forward every method it wants served: one it does not declare
/// answers with this interface's default, not the wrapped instance's. Never change a signature
/// here by hand: regenerate it, so the change comes from the protocol.</para>
/// <para><c>[ProtocolName]</c> is this port's whole wire identity (see
/// <see cref="VgiProtocol.Name"/>): the server hosts under it and a typed client addresses it.</para>
/// </remarks>"""


def _render_body(model: Sequence[RegistryMethod], namespace: str) -> str:
    methods = csharp_methods(model)
    lines = [
        "// Copyright 2025, 2026 Query Farm LLC - https://query.farm",
        "// <auto-generated/>",
        "",
        "#nullable enable",
        "",
        "using System.Collections.Generic;",
        "using System.Threading.Tasks;",
        "using QueryFarm.Vgi.Internal;",
        "using QueryFarm.VgiRpc.Attributes;",
        "using QueryFarm.VgiRpc.Server;",
        "using QueryFarm.VgiRpc.Streaming;",
        "",
        f"namespace {namespace};",
        "",
    ]
    lines += _INTERFACE_DOC.splitlines()
    lines.append("[ProtocolName(VgiProtocol.Name)]")
    lines.append("public interface IVgiService")
    lines.append("{")
    for i, cm in enumerate(methods):
        if i:
            lines.append("")
        lines += _render_method(cm)
    lines.append("}")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Derive-back: vgi-rpc-csharp's SchemaDerivation rules
# ---------------------------------------------------------------------------

_METHOD = re.compile(
    r"((?:    \[.+\]\n)*)    (Task(?:<.+>)?) (\w+)Async\(\n((?:        .+,\n)*)        ICallContext\? ctx = null\) =>\n"
    r'        throw UnimplementedMethod\.For\("(\w+)"\);'
)
_PARAM = re.compile(r"^((?:\[[^\]]+\] )*)(.+?) (\w+)(?: = .+)?$")
_SIMPLE: dict[str, pa.DataType] = {
    "string": pa.string(),
    "bool": pa.bool_(),
    "long": pa.int64(),
    "List<string>": pa.list_(pa.field("item", pa.string(), nullable=True)),
    "Dictionary<string, string>": pa.map_(pa.string(), pa.string()),
}


def _derived_field(name: str, clr: str, model: csharp_types._Model) -> pa.Field[Any]:
    """A CLR type as vgi-rpc-csharp derives its column: ``?`` is nullable, an enum a dictionary."""
    nullable = clr.endswith("?")
    clr = clr.rstrip("?")
    if clr in model.enums:
        dtype: pa.DataType = pa.dictionary(pa.int16(), pa.string())
    elif clr in model.records or clr == "byte[]":
        dtype = pa.binary()
    else:
        expect(clr in _SIMPLE, f"vgi-rpc-csharp derives no Arrow type from {clr}")
        dtype = _SIMPLE[clr]
    return pa.field(name, dtype, nullable=nullable)


class CSharpRegistry(RegistryBackend):
    """vgi-csharp's ``IVgiService``: the interface vgi-rpc-csharp reflects over."""

    key = "csharp"
    language = "C#"
    module = "vgi.codegen.csharp_registry"
    target = TARGET
    repo = "vgi-csharp"
    root_env = "VGI_CSHARP_ROOT"
    tamper = Tamper(
        start="    Task<ItemsResponse> CatalogSchemasAsync(",
        end="ICallContext? ctx",
        old="byte[]? transactionOpaqueData",
        new="byte[] transactionOpaqueData",
    )

    def __init__(self, namespace: str = DEFAULT_NAMESPACE) -> None:  # noqa: D107
        self.namespace = namespace

    def render_body(self, methods: Sequence[RegistryMethod]) -> str:  # noqa: D102
        return _render_body(methods, self.namespace)

    def derive(self, text: str) -> list[DerivedMethod]:  # noqa: D102
        model = csharp_types.build_model()
        headers = {
            csharp_types.csharp_name(m.header_type.__name__): m.header_schema
            for m in registry_methods()
            if m.header_type is not None
        }
        out: list[DerivedMethod] = []
        for match in _METHOD.finditer(text):
            attrs, ret, pascal_name, params_text, name = match.groups()
            # vgi-rpc-csharp names the wire method by snake-casing the CLR name.
            expect(csharp_types._to_snake_case(pascal_name) == name, f"{pascal_name}Async does not map to {name}")
            params = []
            for line in params_text.splitlines():
                pm = _PARAM.match(line.strip().rstrip(","))
                expect(pm is not None, f"{name}: unparsed parameter {line!r}")
                assert pm is not None
                _, clr, pname = pm.groups()
                params.append(_derived_field(csharp_types._to_snake_case(pname), clr, model))
            inner = ret[len("Task<") : -1] if ret.startswith("Task<") else None
            header = None
            result = None
            if inner is None:
                kind = MethodKind.VOID
            elif inner.startswith("RpcStream<"):
                kind = MethodKind.STREAM
                if hm := re.search(r"\[StreamHeader\(typeof\((\w+)\)\)\]", attrs):
                    expect(hm.group(1) in headers, f"{name}: [StreamHeader] {hm.group(1)} is no vgi.v2 header")
                    header = headers[hm.group(1)]
            else:
                kind = MethodKind.UNARY
                result = _derived_field("result", inner, model)
            out.append(DerivedMethod(name, kind, params, result, header))
        return out


BACKEND = CSharpRegistry()
emit, render, main = BACKEND.emit, BACKEND.render, BACKEND.main


if __name__ == "__main__":
    main()
