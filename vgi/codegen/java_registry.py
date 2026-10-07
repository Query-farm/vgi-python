# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit vgi-java's ``VgiService``: the whole ``vgi.v2`` registry, for vgi-java.

vgi-rpc-java registers a protocol by reflecting over a Java interface: every
public method is a wire method, its parameters derive the params schema
(``byte[]`` is ``binary``, ``@Nullable`` is a nullable column, a record is
``binary`` holding the record's IPC), its return type derives the ``result``
column, and ``@StreamHeader`` names a stream's header record. So the interface
*is* the registration table, and this module generates it from
:class:`vgi.protocol.VgiProtocol` (see :mod:`vgi.codegen._registry` for the
model and the reasons):

- every ``vgi.v2`` method, with types chosen so the derivation reproduces the
  reference schemas exactly;
- a ``default`` body on every method that throws vgi-rpc-java's
  ``MethodNotImplementedError`` -- ``UNIMPLEMENTED`` /
  ``method_not_implemented`` / ``"<method> is not implemented by this
  worker"``. vgi-java's ``VgiServiceImpl`` implements the interface and
  overrides what it implements;
- the ``@ProtocolName`` / ``@ProtocolVersion`` declarations and the constants
  behind them.

Every method takes a trailing ``CallContext`` (server-injected, not a wire
field), so an implementation never has to change a signature to read the
caller's identity.

What stays hand-maintained is :data:`JAVA_TYPES` / :data:`JAVA_RAW_RESULTS`:
which protocol records vgi-java binds as typed records rather than raw
``byte[]``. Both are wire-identical (``binary``); the choice is how much the
framework decodes for the handler, and it is checked against the schema here.
A method whose types are not listed gets ``byte[]``, so a method added to the
protocol is generated without any edit to this file.

.. code-block:: bash

   uv run --project ~/Development/vgi-python python scripts/regen_generated.py

``tests/test_generated_registry.py`` fails when the checked-in file and the
generator disagree, and derives the protocol hash back from the rendered Java.
"""

from __future__ import annotations

import io
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

from vgi.codegen import java_types
from vgi.codegen._common import GeneratorError, provenance_comment
from vgi.codegen._registry import (
    RegistryMethod,
    RegistryParam,
    is_record,
    paragraphs,
    protocol_name,
    protocol_version,
    registry_methods,
)

if TYPE_CHECKING:
    from typing import TextIO


GENERATOR_VERSION = "1"

JAVA_PACKAGE = "farm.query.vgi"

#: The generated interface, relative to the vgi-java checkout.
TARGET = "vgi/src/main/java/farm/query/vgi/VgiService.java"

#: The protocol records vgi-java binds as typed Java records, by Python name.
#:
#: A packed request (``request: binary``) or a record result is ``binary`` on
#: the wire either way; listing it here makes vgi-rpc-java decode/encode the
#: record for the handler. Unlisted records are bound as ``byte[]`` and the
#: handler reads the IPC itself. The Java name is vgi-java's established one.
JAVA_TYPES: dict[str, str] = {
    # Packed requests.
    "BindRequest": "BindRequest",
    "InitRequest": "InitRequest",
    "AggregateBindRequest": "AggregateBindRequest",
    "AggregateUpdateRequest": "AggregateUpdateRequest",
    "AggregateCombineRequest": "AggregateCombineRequest",
    "AggregateFinalizeRequest": "AggregateFinalizeRequest",
    "AggregateDestructorRequest": "AggregateDestructorRequest",
    "CatalogAttachRequest": "CatalogAttachRequest",
    "TableBufferingProcessRequest": "TableBufferingProcessRequest",
    "TableBufferingCombineRequest": "TableBufferingCombineRequest",
    "TableBufferingDestructorRequest": "TableBufferingDestructorRequest",
    # Results.
    "BindResponse": "BindResponse",
    "AggregateBindResponse": "AggregateBindResponse",
    "AggregateUpdateResponse": "AggregateUpdateResponse",
    "AggregateCombineResponse": "AggregateCombineResponse",
    "AggregateFinalizeResponse": "AggregateFinalizeResponse",
    "AggregateDestructorResponse": "AggregateDestructorResponse",
    "TableBufferingProcessResponse": "TableBufferingProcessResponse",
    "TableBufferingCombineResponse": "TableBufferingCombineResponse",
    "TableBufferingDestructorResponse": "TableBufferingDestructorResponse",
    "TableCardinality": "CardinalityResponse",
    "PlanResponse": "PlanResponse",
    "TableFunctionDynamicToStringResponse": "DynamicToStringResponse",
    "CatalogAttachResult": "CatalogAttachResult",
    "CatalogContentsResponse": "CatalogContentsResponse",
    "CatalogVersionResponse": "CatalogVersionResponse",
    "TransactionBeginResponse": "TransactionBeginResponse",
    # The eight catalog item listings share one schema, and one Java record.
    "CatalogsResponse": "ItemsResponse",
    "SchemasResponse": "ItemsResponse",
    "TablesResponse": "ItemsResponse",
    "ViewsResponse": "ItemsResponse",
    "FunctionsResponse": "ItemsResponse",
    "MacrosResponse": "ItemsResponse",
    "IndexesResponse": "ItemsResponse",
    "CopyFromFormatsResponse": "ItemsResponse",
    # Stream header.
    "GlobalInitResponse": "GlobalInitResponse",
}

#: Methods whose Python result is raw IPC ``bytes`` that vgi-java returns as a
#: typed record instead (the same ``binary`` column, encoded by the framework).
JAVA_RAW_RESULTS: dict[str, str] = {
    "catalog_table_scan_function_get": "TableScanFunctionGetResponse",
}

#: The Java type of a stream method's return value.
JAVA_STREAM_TYPE = "RpcStream<? extends StreamState>"

_CTX = "ctx"

_REGEN_LINE = "uv run --project ~/Development/vgi-python python scripts/regen_generated.py"


@dataclass(frozen=True)
class JavaParam:
    """One rendered Java parameter."""

    name: str
    java_type: str
    annotations: tuple[str, ...]
    doc: str


@dataclass(frozen=True)
class JavaMethod:
    """One rendered interface method."""

    method: RegistryMethod
    return_type: str
    method_annotations: tuple[str, ...]
    params: tuple[JavaParam, ...]


def _record_name(annotation: object) -> str | None:
    """The Java record bound for a Python record annotation, or ``None`` for ``byte[]``."""
    if not is_record(annotation):
        return None
    return JAVA_TYPES.get(annotation.__name__)  # type: ignore[attr-defined]


def _param(m: RegistryMethod, p: RegistryParam) -> JavaParam:
    origin = f"{m.name}({p.name})"
    record = _record_name(p.annotation)
    nullable = ("@Nullable",) if p.field.nullable else ()
    token = f"{{@code {p.field.type}}}{', nullable' if p.field.nullable else ''}"
    if record is not None:
        if not p.field.type.equals(pa.binary()):
            raise GeneratorError(f"{origin}: a record binds only a binary column, got {p.field.type}")
        return JavaParam(
            p.name,
            record,
            nullable,
            f"the packed {{@code {p.annotation.__name__}}} ({token})",  # type: ignore[attr-defined]
        )
    component = java_types._component(p.field, m.name)
    if is_record(p.annotation):
        doc = f"the packed {{@code {p.annotation.__name__}}}, as its IPC bytes ({token})"  # type: ignore[attr-defined]
    else:
        doc = f"the {{@code {p.name}}} column ({token})"
    return JavaParam(p.name, component.java_type, component.annotations, doc)


def _result(m: RegistryMethod) -> tuple[str, tuple[str, ...]]:
    if m.is_stream:
        if m.header_type is None:
            return JAVA_STREAM_TYPE, ()
        header = JAVA_TYPES.get(m.header_type.__name__)
        if header is None:
            raise GeneratorError(f"{m.name}: stream header {m.header_type.__name__} has no Java record in JAVA_TYPES")
        return JAVA_STREAM_TYPE, (f"@StreamHeader({header}.class)",)
    if m.result_field is None:
        return "void", ()
    if not m.result_field.type.equals(pa.binary()):
        raise GeneratorError(f"{m.name}: result is {m.result_field.type}, expected binary")
    nullable = ("@Nullable",) if m.result_field.nullable else ()
    raw = JAVA_RAW_RESULTS.get(m.name)
    if raw is not None:
        if m.result_annotation is not bytes:
            raise GeneratorError(f"{m.name}: JAVA_RAW_RESULTS applies only to a raw-bytes result")
        return raw, nullable
    record = _record_name(m.result_annotation)
    return record or "byte[]", nullable


def java_methods() -> list[JavaMethod]:
    """Every ``vgi.v2`` method as vgi-java declares it."""
    out = []
    for m in registry_methods():
        ret, ann = _result(m)
        out.append(JavaMethod(m, ret, ann, tuple(_param(m, p) for p in m.params)))
    return out


def check_mappings() -> None:
    """Refuse a stale :data:`JAVA_TYPES` / :data:`JAVA_RAW_RESULTS` entry (a typo would bind nothing)."""
    methods = registry_methods()
    seen: set[str] = set()
    for m in methods:
        for p in m.params:
            if is_record(p.annotation):
                seen.add(p.annotation.__name__)  # type: ignore[attr-defined]
        if is_record(m.result_annotation):
            seen.add(m.result_annotation.__name__)  # type: ignore[attr-defined]
        if m.header_type is not None:
            seen.add(m.header_type.__name__)
    stale = sorted(set(JAVA_TYPES) - seen)
    if stale:
        raise GeneratorError(f"JAVA_TYPES names classes no vgi.v2 method uses: {stale}")
    unknown = sorted(set(JAVA_RAW_RESULTS) - {m.name for m in methods})
    if unknown:
        raise GeneratorError(f"JAVA_RAW_RESULTS names unknown methods: {unknown}")


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _javadoc(jm: JavaMethod) -> list[str]:
    m = jm.method
    out = ["    /**"]
    paras = paragraphs(m.doc) or [f"The {{@code {m.name}}} method."]
    for i, para in enumerate(paras):
        if i:
            out.append("     *")
        out += java_types._wrap(("<p>" if i else "") + java_types._javadoc_text(para), "     * ", "     * ")
    out.append("     *")
    kind = "stream" if m.is_stream else "unary"
    out += java_types._wrap(
        f"<p>Wire method {{@code {m.name}}} ({kind}). Default: answers {{@code UNIMPLEMENTED}}.",
        "     * ",
        "     * ",
    )
    out.append("     *")
    for p in jm.params:
        out += java_types._wrap(p.doc, f"     * @param {p.name} ", "     *        ")
    out.append(f"     * @param {_CTX} the caller's context, injected by the server (not a wire field)")
    if jm.return_type != "void":
        if m.is_stream:
            what = "the output stream"
            if m.header_type is not None:
                what += f", headed by a {{@code {m.header_type.__name__}}}"
        else:
            assert m.result_field is not None
            what = f"the {{@code result}} column ({{@code {m.result_field.type}}}"
            what += ", nullable)" if m.result_field.nullable else ")"
        out += java_types._wrap(what, "     * @return ", "     *         ")
    out.append("     */")
    return out


def _render_method(jm: JavaMethod) -> list[str]:
    lines = _javadoc(jm)
    lines += [f"    {a}" for a in jm.method_annotations]
    lines.append(f"    default {jm.return_type} {jm.method.name}(")
    for p in jm.params:
        decl = " ".join((*p.annotations, p.java_type, p.name))
        lines.append(f"            {decl},")
    lines.append(f"            CallContext {_CTX}) {{")
    lines.append(f'        throw notImplemented("{jm.method.name}");')
    lines.append("    }")
    return lines


_CLASS_DOC = """\
/**
 * The {@code vgi.v2} protocol: every method a VGI worker serves, generated from
 * the reference ({@code vgi.protocol.VgiProtocol} in vgi-python).
 *
 * <p>vgi-rpc-java reflects over this interface to register the protocol, so the
 * declarations below <em>are</em> the wire contract: a parameter's Java type and
 * {@code @Nullable} derive its params-schema column, the return type derives the
 * {@code result} column, and {@code @StreamHeader} names a stream's header. They
 * reproduce the reference's schemas exactly, which is what makes
 * {@code vgi_rpc.Reflection.v1} report one {@code vgi.v2} hash in every SDK. The
 * protocol is the unit of optionality: every method is registered, whether or not
 * this worker implements it.</p>
 *
 * <p>Every method has a {@code default} body that answers {@code UNIMPLEMENTED} /
 * {@code method_not_implemented} with {@code "<method> is not implemented by this
 * worker"}; an implementation overrides the methods it serves. Never change a
 * signature here by hand: regenerate it, so the change comes from the protocol.</p>
 *
 * <p>Two parameter shapes coexist. <b>Packed</b> methods take one {@code request}
 * column holding an IPC-serialized record, bound either as the record itself or as
 * {@code byte[]} for the handler to decode. <b>Flat</b> methods map their
 * parameters 1:1 to columns by their {@code snake_case} name.</p>
 *
 * <p>{@link #PROTOCOL_NAME} is the wire identity: the {@code vgi_rpc.protocol}
 * routing key on every request and the protocol path segment over HTTP. It is
 * declared rather than derived because no Java identifier can spell it. The major
 * version is in the name, so an incompatible major is a different protocol (a 404
 * every proxy understands) and can be served beside this one.
 * {@link #PROTOCOL_VERSION} is what a client of this interface stamps on every
 * request; a VGI worker refuses a mismatched major+minor.</p>
 */"""


def _render_body() -> str:
    check_mappings()
    methods = java_methods()

    imports = {
        "farm.query.vgirpc.CallContext",
        "farm.query.vgirpc.MethodNotImplementedError",
        "farm.query.vgirpc.schema.ProtocolName",
        "farm.query.vgirpc.schema.ProtocolVersion",
    }
    records: set[str] = set()
    java_util: set[str] = set()
    for jm in methods:
        if jm.method.is_stream:
            imports.update({"farm.query.vgirpc.RpcStream", "farm.query.vgirpc.StreamState"})
        for a in jm.method_annotations:
            if a.startswith("@StreamHeader("):
                imports.add("farm.query.vgirpc.schema.StreamHeader")
                records.add(a[len("@StreamHeader(") : -len(".class)")])
            if a == "@Nullable":
                imports.add("farm.query.vgirpc.schema.Nullable")
        types = [jm.return_type, *(p.java_type for p in jm.params)]
        for t in types:
            if t in set(JAVA_TYPES.values()) | set(JAVA_RAW_RESULTS.values()):
                records.add(t)
            for util in ("List", "Map"):
                if t.startswith(f"{util}<"):
                    java_util.add(f"java.util.{util}")
        for p in jm.params:
            if "@Nullable" in p.annotations:
                imports.add("farm.query.vgirpc.schema.Nullable")
            if any(a.startswith("@ArrowField(") for a in p.annotations):
                imports.update({"farm.query.vgirpc.schema.ArrowField", "farm.query.vgirpc.schema.ArrowFieldType"})

    name = protocol_name()
    version = protocol_version()
    lines = ["// Copyright 2025, 2026 Query Farm LLC - https://query.farm", "", f"package {JAVA_PACKAGE};", ""]
    lines += [f"import farm.query.vgi.protocol.{r};" for r in sorted(records)]
    lines += [f"import {i};" for i in sorted(imports)]
    if java_util:
        lines.append("")
        lines += [f"import {i};" for i in sorted(java_util)]
    lines.append("")
    lines += _CLASS_DOC.splitlines()
    lines.append("@ProtocolName(VgiService.PROTOCOL_NAME)")
    lines.append("@ProtocolVersion(VgiService.PROTOCOL_VERSION)")
    lines.append("public interface VgiService {")
    lines.append("")
    lines.append("    /** The protocol's wire name: the {@code vgi_rpc.protocol} routing key. */")
    lines.append(f'    String PROTOCOL_NAME = "{name}";')
    lines.append("")
    lines.append("    /** The protocol's semver, stamped on every request by a client of this interface. */")
    lines.append(f'    String PROTOCOL_VERSION = "{version}";')
    for jm in methods:
        lines.append("")
        lines += _render_method(jm)
    lines.append("")
    lines += [
        "    /**",
        "     * The error every method answers until an implementation overrides it.",
        "     *",
        "     * @param method the wire method name",
        "     * @return {@code UNIMPLEMENTED} / {@code method_not_implemented}",
        "     */",
        "    private static MethodNotImplementedError notImplemented(String method) {",
        '        return new MethodNotImplementedError(method + " is not implemented by this worker");',
        "    }",
        "}",
    ]
    return "\n".join(lines) + "\n"


def emit(out: TextIO) -> None:
    """Emit the generated ``VgiService.java`` to *out*."""
    body = _render_body()
    out.write(
        provenance_comment(
            generator_module="vgi.codegen.java_registry",
            generator_command="python -m vgi.codegen.java_registry",
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
    """Console-script entrypoint -- write ``VgiService.java`` to stdout."""
    try:
        emit(sys.stdout)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
