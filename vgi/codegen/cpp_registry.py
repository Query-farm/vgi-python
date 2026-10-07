# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit vgi-c++'s ``vgi_service.hpp``: the whole ``vgi.v2`` registry, for the C++ SDK.

vgi-rpc-c++ does not reflect over anything. A method is registered by calling
``ServerBuilder::add_unary`` / ``add_void`` / ``add_exchange`` with its name, its
params schema, its result (or header) schema and a ``std::function`` handler. So
this port needs both halves of the registry generated (see
:mod:`vgi.codegen._registry` for the model and the reasons):

- ``class VgiService``: one ``virtual`` member per ``vgi.v2`` method, every one
  taking ``(const vgi_rpc::Request&, vgi_rpc::CallContext&)`` and returning
  ``vgi_rpc::Result`` (a unary with a return), ``void`` (a unary without one) or
  ``vgi_rpc::Stream`` (a stream). Each default body throws
  ``method_not_implemented``: ``UNIMPLEMENTED`` / ``method_not_implemented`` /
  ``"<method> is not implemented by this worker"``. The SDK's ``Dispatcher``
  derives from it and overrides what it serves.
- ``VGI_METHODS``: the registration table. One row per method, in wire-name
  order, binding the name to the generated params / result / payload / header
  schema factories, the member to call, and the method's summary. The SDK's
  install loop walks it and hands each row to the builder; it never names a
  method.

The params, payload and header schemas are the factories
:mod:`vgi.codegen.cpp_schemas` already emits into ``vgi_protocol_schemas.hpp``
(``BindParamsSchema``, ``BindResultSchema``, ``GlobalInitResponseSchema``), so
the two headers must be generated into the same namespace. The ``result``
column every unary with a return answers in is emitted here, as
``ResultEnvelopeSchema`` / ``OptionalResultEnvelopeSchema``: its nullability is
the reference's, so a ``bytes | None`` return is the only nullable one.

There is no hand-maintained table. The C++ SDK builds every response as a
batch in the generated payload schema, so nothing about a method's C++ types is
a choice.

.. code-block:: bash

   uv run --project ~/Development/vgi-python python scripts/regen_generated.py

:class:`CppRegistry` is this language's backend for the one registry
generator (:mod:`vgi.codegen._registry_backend`); its ``derive`` evaluates,
``arrow::field`` by ``arrow::field``, every schema factory a ``VGI_METHODS``
row names, because vgi-rpc-c++ registers the schemas it is handed.
"""

from __future__ import annotations

import argparse
import io
import re
import sys
import textwrap
from collections.abc import Sequence
from typing import Any

import pyarrow as pa

from vgi.codegen import cpp_schemas
from vgi.codegen._common import (
    GeneratorError,
    close_namespace,
    collect_schemas,
    open_namespace,
    parse_cpp_namespace,
    sanitize_name,
)
from vgi.codegen._registry import MethodKind, RegistryMethod
from vgi.codegen._registry_backend import ArrowDialect, DerivedMethod, RegistryBackend, Tamper, expect
from vgi.codegen.cpp_schemas import _emit_field

#: vgi-c++ generates every protocol header into ``vgi::generated`` (its
#: ``scripts/regenerate_protocol.sh``); the registry must share that namespace
#: with ``vgi_protocol_schemas.hpp``, whose factories it names unqualified.
DEFAULT_NAMESPACE = "vgi::generated"

#: The generated header, relative to the vgi-c++ checkout.
TARGET = "src/generated/vgi_service.hpp"

#: The schema factory for each ``result`` column a unary may declare, by
#: nullability. Every vgi.v2 result is one ``binary`` column (a record's IPC
#: bytes, or raw bytes); a result of any other type is a protocol change this
#: generator has to learn about, not one it should guess at.
_RESULT_ENVELOPES: dict[bool, str] = {
    False: "ResultEnvelopeSchema",
    True: "OptionalResultEnvelopeSchema",
}


def _schema_names() -> set[str]:
    """Every factory stem ``cpp_schemas`` emits (without the ``Schema`` suffix)."""
    from vgi.codegen._common import EXTRA_RESPONSE_TYPES, REQUEST_TYPES

    return {es.name for es in collect_schemas(extra_response_types=(*EXTRA_RESPONSE_TYPES, *REQUEST_TYPES))}


def _factory(stem: str, available: set[str], what: str) -> str:
    if stem not in available:
        raise GeneratorError(f"{what}: vgi_protocol_schemas.hpp has no {stem}Schema()")
    return f"{stem}Schema"


def _cpp_string(text: str) -> str:
    """A C++ string literal holding *text* (UTF-8 passes through)."""
    if any(ord(c) < 0x20 for c in text):
        raise GeneratorError(f"control character in {text!r}")
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


#: The C++ return type of each kind's ``VgiService`` member.
_RETURNS: dict[MethodKind, str] = {
    MethodKind.STREAM: "vgi_rpc::Stream",
    MethodKind.UNARY: "vgi_rpc::Result",
    MethodKind.VOID: "void",
}


def _result_factory(m: RegistryMethod) -> str | None:
    field = m.result_field
    if field is None:
        return None
    if not field.type.equals(pa.binary()):
        raise GeneratorError(f"{m.name}: result column is {field.type}, not binary; teach cpp_registry about it")
    return _RESULT_ENVELOPES[field.nullable]


def _row(m: RegistryMethod, available: set[str]) -> list[str]:
    stems = m.schema_names
    params = _factory(stems.params, available, m.name)
    result = _result_factory(m)
    payload = None
    if m.result_record is not None:
        payload = _factory(stems.result, available, m.name)
    header = None
    if stems.header is not None:
        header = _factory(sanitize_name(stems.header), available, m.name)
    if m.is_stream and header is None:
        raise GeneratorError(f"{m.name}: a vgi.v2 stream without a header")
    fields = [f'.name = "{m.name}"', f".params = {params}"]
    if result is not None:
        fields.append(f".result = {result}")
    if payload is not None:
        fields.append(f".payload = {payload}")
    if header is not None:
        fields.append(f".header = {header}")
    fields.append(f".handler = VgiMethod::{m.kind.value}{{&VgiService::{m.name}}}")
    fields.append(f".doc = {_cpp_string(m.summary or '')}")
    lines = ["    {" + fields[0] + ","]
    lines += [f"     {f}," for f in fields[1:-1]]
    lines.append(f"     {fields[-1]}}},")
    return lines


def _doc_comment(m: RegistryMethod, indent: str) -> list[str]:
    text = m.summary
    if not text:
        return []
    return textwrap.wrap(
        text, width=96 - len(indent), initial_indent=f"{indent}/// ", subsequent_indent=f"{indent}/// "
    )


def _envelope(name: str, nullable: bool) -> str:
    field: pa.Field[Any] = pa.field("result", pa.binary(), nullable=nullable)
    return (
        f"inline const std::shared_ptr<arrow::Schema> &{name}() {{\n"
        f"\tstatic const auto schema = arrow::schema({{{_emit_field(field, origin=name)}}});\n"
        "\treturn schema;\n"
        "}\n"
    )


_PREAMBLE = """\
// The vgi.v2 method registry.
//
// Every vgi.v2 method is registered, with exactly the reference's method type
// and params / result / header schemas, so vgi_rpc.Reflection.v1 reports the
// reference's protocol hash. A method this worker does not override answers
// UNIMPLEMENTED (kind `method_not_implemented`).
//
// `VgiService` declares every method; a worker derives from it and overrides
// what it implements. `VGI_METHODS` binds each method's wire name to its schemas
// and to the member that serves it: register each row with the vgi-rpc
// `ServerBuilder` (`add_unary` for a `Unary` or `Void` handler with or without
// `result`, `add_exchange` or `add_producer` for a `Stream`, with `header`).
//
// Every non-void method answers in the one-column `result` envelope, which holds
// the IPC bytes of a one-row batch in `payload` -- or, when `payload` is null,
// raw bytes. A handler builds its answer in `payload`, never in `result`.

/// What a vgi.v2 method a worker does not implement answers: code
/// UNIMPLEMENTED, kind `method_not_implemented`, "<method> is not implemented by
/// this worker". The body of every `VgiService` default.
[[noreturn]] inline void method_not_implemented(std::string_view method) {
\tthrow vgi_rpc::KindedError(vgi_rpc::ERROR_KIND_METHOD_NOT_IMPLEMENTED, "MethodNotImplementedError",
\t                           std::string(method) + " is not implemented by this worker",
\t                           vgi_rpc::Code::UNIMPLEMENTED);
}

"""


def _render_body(methods: Sequence[RegistryMethod], namespace: list[str]) -> str:
    available = _schema_names()

    body = io.StringIO()
    body.write("#pragma once\n\n")
    for header in ("array", "memory", "string", "string_view", "variant"):
        body.write(f"#include <{header}>\n")
    body.write("\n#include <arrow/type.h>\n")
    for header in ("call_context", "errors", "metadata", "request", "result", "stream"):
        body.write(f"#include <vgi_rpc/{header}.h>\n")
    body.write('\n#include "vgi/generated/vgi_protocol_schemas.hpp"\n\n')
    body.write(open_namespace(namespace))
    body.write("\n")
    body.write(_PREAMBLE)

    body.write("// The `result` column of a unary with a return: non-nullable unless the\n")
    body.write("// reference's return admits None.\n")
    for nullable, name in sorted(_RESULT_ENVELOPES.items()):
        body.write(_envelope(name, nullable))
        body.write("\n")

    body.write("/// Every vgi.v2 method, each defaulting to `method_not_implemented`.\n")
    body.write("class VgiService {\n")
    body.write("public:\n")
    body.write("\tvirtual ~VgiService() = default;\n")
    for m in methods:
        body.write("\n")
        for line in _doc_comment(m, "\t"):
            body.write(line + "\n")
        body.write(f"\tvirtual {_RETURNS[m.kind]} {m.name}(const vgi_rpc::Request &, vgi_rpc::CallContext &) {{\n")
        body.write(f'\t\tmethod_not_implemented("{m.name}");\n')
        body.write("\t}\n")
    body.write("};\n\n")

    body.write("/// One row of the registration table.\n")
    body.write("struct VgiMethod {\n")
    body.write("\tusing Unary = vgi_rpc::Result (VgiService::*)(const vgi_rpc::Request &, vgi_rpc::CallContext &);\n")
    body.write("\tusing Void = void (VgiService::*)(const vgi_rpc::Request &, vgi_rpc::CallContext &);\n")
    body.write("\tusing Stream = vgi_rpc::Stream (VgiService::*)(const vgi_rpc::Request &, vgi_rpc::CallContext &);\n")
    body.write("\tusing SchemaFactory = const std::shared_ptr<arrow::Schema> &(*)();\n\n")
    body.write("\t/// The wire name.\n")
    body.write("\tstd::string_view name;\n")
    body.write("\t/// The params schema.\n")
    body.write("\tSchemaFactory params = nullptr;\n")
    body.write("\t/// The `result` column a `Unary` is registered with; null for `Void` and `Stream`.\n")
    body.write("\tSchemaFactory result = nullptr;\n")
    body.write("\t/// The record `result` holds; null when it holds raw bytes (or there is none).\n")
    body.write("\tSchemaFactory payload = nullptr;\n")
    body.write("\t/// The stream header; null for a unary.\n")
    body.write("\tSchemaFactory header = nullptr;\n")
    body.write("\t/// The member that serves it.\n")
    body.write("\tstd::variant<Unary, Void, Stream> handler;\n")
    body.write("\t/// The reference's summary of the method.\n")
    body.write("\tstd::string_view doc;\n")
    body.write("};\n\n")

    body.write("/// Every vgi.v2 method, by wire name.\n")
    body.write(f"inline constexpr std::array<VgiMethod, {len(methods)}> VGI_METHODS = {{{{\n")
    for m in methods:
        for line in _row(m, available):
            body.write(line + "\n")
    body.write("}};\n\n")
    body.write(close_namespace(namespace))
    # vgi-c++'s indentation (4 spaces), so the header reads like the code beside it.
    return body.getvalue().replace("\t", "    ")


# ---------------------------------------------------------------------------
# Derive-back: vgi-rpc-c++ registers the schemas each row names
# ---------------------------------------------------------------------------

_FACTORY = re.compile(
    r"inline const std::shared_ptr<arrow::Schema> &(\w+)\(\) \{\n"
    r"\s*static const auto schema = arrow::schema\(\{(.*?)\}\);\n",
    re.S,
)
_ROW = re.compile(r"^    \{\.name = \"(\w+)\",\n((?:     \..+\n)+)", re.M)
# Every field line ends with `,`; the last one (`.doc`) also closes the row.
_ROW_FIELD = re.compile(r"^     \.(\w+) = (.+),$")
_HANDLER = re.compile(r"^VgiMethod::(Unary|Void|Stream)\{&VgiService::(\w+)\}$")
_STUB = re.compile(
    r"    virtual (\S+) (\w+)\(const vgi_rpc::Request &, vgi_rpc::CallContext &\) \{\n"
    r'        method_not_implemented\("(\w+)"\);\n    \}'
)

#: Arrow C++, as pyarrow: ``arrow::field("x", arrow::binary(), /*nullable=*/true)``
#: becomes ``A_field("x", A_binary(), true)``; ``{...}`` initializer lists are lists.
ARROW_CPP = ArrowDialect(
    rewrites=(
        (r"/\*\w+=\*/", ""),
        (r"arrow::TimeUnit::(\w+)", r"TU_\1"),
        (r"arrow::", "A_"),
        (r"\{", "["),
        (r"\}", "]"),
    ),
    names={
        "A_field": lambda name, dtype, nullable=True: pa.field(name, dtype, nullable=nullable),
        "A_schema": pa.schema,
        "A_list": pa.list_,
        "A_large_list": pa.large_list,
        "A_map": pa.map_,
        "A_struct_": pa.struct,
        "A_dictionary": lambda index, value, ordered=False: pa.dictionary(index, value, ordered=ordered),
        "A_timestamp": lambda unit, tz=None: pa.timestamp(unit, tz),
        "A_utf8": pa.string,
        "A_large_utf8": pa.large_string,
        "A_binary": pa.binary,
        "A_large_binary": pa.large_binary,
        "A_boolean": pa.bool_,
        "A_null": pa.null,
        **{f"A_{t}": getattr(pa, t) for t in ("int8", "int16", "int32", "int64", "uint8", "uint16", "uint32")},
        **{f"A_{t}": getattr(pa, t) for t in ("uint64", "float32", "float64")},
        **{f"TU_{k}": v for k, v in (("SECOND", "s"), ("MILLI", "ms"), ("MICRO", "us"), ("NANO", "ns"))},
    },
)


def factories(text: str) -> dict[str, pa.Schema]:
    """Every schema factory a rendered header defines, evaluated."""
    return {name: ARROW_CPP.schema(f"[{fields}]") for name, fields in _FACTORY.findall(text)}


def protocol_schemas_text(namespace: list[str]) -> str:
    """The ``vgi_protocol_schemas.hpp`` the registry's rows name factories from."""
    buf = io.StringIO()
    cpp_schemas.emit(buf, namespace=namespace)
    return buf.getvalue()


def rows(text: str) -> dict[str, dict[str, str]]:
    """Each ``VGI_METHODS`` row of a rendered header, as ``{field: value}``, by wire name."""
    out: dict[str, dict[str, str]] = {}
    for name, fields_text in _ROW.findall(text[text.index("VGI_METHODS = {{") :]):
        fields: dict[str, str] = {}
        for line in fields_text.splitlines():
            fm = _ROW_FIELD.match(line)
            expect(fm is not None, f"{name}: unparsed row field {line!r}")
            assert fm is not None
            key, value = fm.groups()
            fields[key] = value.removesuffix("}") if key == "doc" else value
        out[name] = fields
    return out


class CppRegistry(RegistryBackend):
    """vgi-c++'s ``vgi_service.hpp``: ``VgiService`` and the ``VGI_METHODS`` table."""

    key = "cpp"
    language = "C++"
    module = "vgi.codegen.cpp_registry"
    target = TARGET
    repo = "vgi-c++"
    root_env = "VGI_CPP_ROOT"
    banner_head = banner_tail = "// " + "=" * 76 + "\n"
    tamper = Tamper(
        start='    {.name = "catalog_table_scan_function_get",',
        end="    {.name = ",
        old=".result = ResultEnvelopeSchema",
        new=".result = OptionalResultEnvelopeSchema",
    )

    def __init__(self, namespace: str = DEFAULT_NAMESPACE) -> None:  # noqa: D107
        self.namespace = parse_cpp_namespace(namespace)

    def render_body(self, methods: Sequence[RegistryMethod]) -> str:  # noqa: D102
        return _render_body(methods, self.namespace)

    def derive(self, text: str) -> list[DerivedMethod]:  # noqa: D102
        known = {**factories(protocol_schemas_text(self.namespace)), **factories(text)}
        stubs = {name: (ret, stub) for ret, name, stub in _STUB.findall(text)}
        out: list[DerivedMethod] = []
        for name, fields in rows(text).items():
            hm = _HANDLER.match(fields["handler"])
            expect(hm is not None, f"{name}: unparsed handler {fields['handler']!r}")
            assert hm is not None
            kind, member = MethodKind(hm.group(1)), hm.group(2)
            expect(member == name, f"{name} is served by VgiService::{member}")
            expect(stubs.get(name) == (_RETURNS[kind], name), f"{name}'s VgiService default is {stubs.get(name)}")
            for slot in ("params", "result", "header"):
                expect(fields.get(slot, fields["params"]) in known, f"{name}: no factory {fields.get(slot)}")
            result = None
            if kind is MethodKind.UNARY:
                envelope = known[fields["result"]]
                expect(len(envelope) == 1, f"{name}: result {envelope}")
                result = envelope.field(0)
            else:
                expect("result" not in fields, f"{name} is {kind.value} but declares a result")
            header = known[fields["header"]] if "header" in fields else None
            out.append(DerivedMethod(name, kind, list(known[fields["params"]]), result, header))
        return out

    def main(self) -> None:
        """Console-script entry point: write ``vgi_service.hpp`` to stdout."""
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument(
            "--namespace",
            default=DEFAULT_NAMESPACE,
            help=f"C++ namespace, `::`-separated; must match vgi_protocol_schemas.hpp's (default: {DEFAULT_NAMESPACE})",
        )
        args = parser.parse_args()
        try:
            CppRegistry(args.namespace).emit(sys.stdout)
        except GeneratorError as e:
            print(f"\nerror: {e}\n", file=sys.stderr)
            sys.exit(2)


BACKEND = CppRegistry()
emit, render, main = BACKEND.emit, BACKEND.render, BACKEND.main


if __name__ == "__main__":
    main()
