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

``tests/test_generated_registry.py`` fails when the checked-in file and the
generator disagree, and derives the protocol hash back from the rendered C#.
"""

from __future__ import annotations

import enum
import io
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

from vgi.codegen import csharp_types
from vgi.codegen._common import GeneratorError, provenance_comment
from vgi.codegen._registry import (
    NO_DEFAULT,
    RegistryMethod,
    RegistryParam,
    is_record,
    paragraphs,
    registry_methods,
)

if TYPE_CHECKING:
    from typing import TextIO


GENERATOR_VERSION = "1"

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

_REGEN_LINE = "uv run --project ~/Development/vgi-python python scripts/regen_generated.py"

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
    if is_record(p.annotation):
        if not p.field.type.equals(pa.binary()):
            raise GeneratorError(f"{origin}: a record binds only a binary column, got {p.field.type}")
        record = csharp_types.csharp_name(p.annotation.__name__)  # type: ignore[attr-defined]
        if record not in model.records:
            raise GeneratorError(f"{origin}: {record} is not a generated C# record")
        clr = record
        doc = f"The packed <c>{p.annotation.__name__}</c> ({token})."  # type: ignore[attr-defined]
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
    if not m.result_field.type.equals(pa.binary()):
        raise GeneratorError(f"{m.name}: result is {m.result_field.type}, expected binary")
    nullable = "?" if m.result_field.nullable else ""
    raw = CSHARP_RAW_RESULTS.get(m.name)
    if raw is not None:
        if m.result_annotation is not bytes:
            raise GeneratorError(f"{m.name}: CSHARP_RAW_RESULTS applies only to a raw-bytes result")
        clr = raw
    elif is_record(m.result_annotation):
        clr = csharp_types.csharp_name(m.result_annotation.__name__)  # type: ignore[attr-defined]
    elif m.result_annotation is bytes:
        clr = "byte[]"
    else:
        raise GeneratorError(f"{m.name}: unsupported result annotation {m.result_annotation!r}")
    if clr != "byte[]" and clr not in model.records:
        raise GeneratorError(f"{m.name}: result {clr} is not a generated C# record")
    return f"Task<{clr}{nullable}>", clr + nullable, ()


def csharp_methods() -> list[CSharpMethod]:
    """Every ``vgi.v2`` method as vgi-csharp declares it."""
    model = csharp_types.build_model()
    unknown = sorted(set(CSHARP_RAW_RESULTS) - {m.name for m in registry_methods()})
    if unknown:
        raise GeneratorError(f"CSHARP_RAW_RESULTS names unknown methods: {unknown}")
    out = []
    for m in registry_methods():
        ret, result_clr, attrs = _result(m, model)
        params = tuple(_param(m, p, model) for p in m.params)
        out.append(CSharpMethod(m, csharp_types._pascal(m.name) + "Async", ret, result_clr, attrs, params))
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


def _render_body(namespace: str) -> str:
    methods = csharp_methods()
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


def emit(out: TextIO, *, namespace: str = DEFAULT_NAMESPACE) -> None:
    """Emit the generated ``IVgiService.g.cs`` to *out*."""
    body = _render_body(namespace)
    out.write(
        provenance_comment(
            generator_module="vgi.codegen.csharp_registry",
            generator_command="python -m vgi.codegen.csharp_registry",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=[_REGEN_LINE],
            body=body,
        )
    )
    out.write("\n")
    out.write(body)


def render() -> str:
    """The full text of the generated file."""
    buf = io.StringIO()
    emit(buf)
    return buf.getvalue()


def main() -> None:
    """Console-script entrypoint -- write ``IVgiService.g.cs`` to stdout."""
    try:
        emit(sys.stdout)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
