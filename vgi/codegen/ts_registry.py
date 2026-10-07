# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit vgi-typescript's ``vgi-service.ts``: the whole ``vgi.v2`` registry, for vgi-typescript.

``@query-farm/vgi-rpc`` registers methods explicitly --
``protocol.unary(name, {params, result, handler})`` and
``protocol.exchange``/``protocol.producer`` for streams -- with the Arrow
schemas passed as values. Nothing is derived from a TypeScript type, so the
registry has to carry the schemas themselves. This module generates, from
:class:`vgi.protocol.VgiProtocol` (see :mod:`vgi.codegen._registry` for the
model and the reasons):

- ``VGI_V2_METHODS``: the registration table. Every ``vgi.v2`` method with its
  wire name, its ``VgiService`` key, and its params / result / header schemas,
  written in the vgi-typescript Arrow facade's DSL by
  :mod:`vgi.codegen.ts_schemas`' own field renderer, so they are the reference
  schemas exactly. A void unary has an empty result schema (vgi-rpc reports
  ``has_return`` from a non-empty result), a stream without a header has
  ``header: null``.
- ``VgiService``: one member per method, camelCased. A unary is a method
  ``(params, ctx) => reply``, typed from the params schema (one property per
  column, ``| null`` when nullable) and the result column. The stream ``init``
  is a ``VgiStream`` value -- the exchange or producer callbacks -- because a
  vgi-rpc stream *is* a bundle of callbacks, not a call.
- ``UNIMPLEMENTED_VGI_SERVICE``: a ``VgiService`` whose every member answers
  vgi-rpc's ``MethodNotImplementedError`` -- ``UNIMPLEMENTED`` /
  ``method_not_implemented`` / ``"<method> is not implemented by this
  worker"``. vgi-typescript spreads its handler groups over it, so a method it
  does not implement is still registered with the reference's schemas.
- ``registerVgiService`` / ``createVgiProtocol``: the loop that hands the table
  and a service to a ``Protocol``, and the ``Protocol`` constructed with the
  generated ``VGI_PROTOCOL_NAME`` / ``VGI_PROTOCOL_VERSION``.

Every unary takes the server's ``CallContext`` as its second argument,
uniformly. There is no hand-maintained mapping table: vgi-typescript receives
every packed request and returns every record result as raw IPC bytes, so the
TypeScript type of a column follows from its Arrow type alone (see
:func:`_ts_value_type`, run through the core's
:func:`~vgi.codegen._registry.map_arrow_type`, which rejects an unmapped type).

.. code-block:: bash

   uv run --project ~/Development/vgi-python python scripts/regen_generated.py

:class:`TsRegistry` is this language's backend for the one registry
generator (:mod:`vgi.codegen._registry_backend`); its ``derive`` evaluates the
registration table's facade-DSL schemas, which vgi-rpc-typescript registers as
given.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from vgi.codegen import ts_schemas
from vgi.codegen._registry import (
    MethodKind,
    RegistryMethod,
    RegistryParam,
    camel,
    identifiers,
    map_arrow_type,
    paragraphs,
    protocol_name,
    protocol_version,
    registry_methods,
    summary,
)
from vgi.codegen._registry_backend import (
    ArrowDialect,
    DerivedMethod,
    RegistryBackend,
    Tamper,
    VoidResult,
    expect,
)

#: The generated module, relative to the vgi-typescript checkout.
TARGET = "src/generated/vgi-service.ts"


def _ts_mapping(dtype: pa.DataType, recurse: Callable[[pa.DataType, str], str]) -> str | None:
    """The TypeScript type of one decoded column value, as vgi-rpc hands it to a handler.

    vgi-rpc reads each params column with the Arrow backend's ``get(0)``: a
    binary is a ``Uint8Array``, a dictionary-encoded string its decoded string,
    a list an Arrow vector (iterable; arrays on flechette), and a map is passed
    through opaque.
    """
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        return "Uint8Array"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "string"
    if pa.types.is_boolean(dtype):
        return "boolean"
    if pa.types.is_integer(dtype):
        return "number" if dtype.bit_width < 64 else "number | bigint"
    if pa.types.is_dictionary(dtype) and pa.types.is_string(dtype.value_type):
        return "string"
    if pa.types.is_list(dtype):
        item = dtype.value_field
        inner = recurse(item.type, "[item]")
        return f"WireList<{inner}{' | null' if item.nullable else ''}>"
    if pa.types.is_map(dtype):
        return "WireMap"
    return None


def _ts_value_type(dtype: pa.DataType, origin: str) -> str:
    """The TypeScript type of one decoded column value; an unmapped Arrow type is a ``GeneratorError``."""
    return map_arrow_type(dtype, _ts_mapping, origin=origin, language="TypeScript", hint="ts_registry._ts_mapping")


def _field_ts_type(f: pa.Field[Any], origin: str) -> str:
    base = _ts_value_type(f.type, origin)
    return f"{base} | null" if f.nullable else base


@dataclass(frozen=True)
class TsMethod:
    """One rendered ``vgi.v2`` method."""

    method: RegistryMethod
    key: str
    params_type: str
    result_type: str | None


def _result_type(m: RegistryMethod) -> str | None:
    if m.is_stream:
        return None
    if m.result_field is None:
        return "NoResult"
    return f"{{ result: {_field_ts_type(m.result_field, f'{m.name}(result)')} }}"


def ts_methods(methods: Sequence[RegistryMethod] | None = None) -> list[TsMethod]:
    """Every ``vgi.v2`` method as vgi-typescript declares it."""
    methods = registry_methods() if methods is None else methods
    keys = identifiers((m.name for m in methods), camel, what="VgiService key")
    return [TsMethod(m, keys[m.name], m.schema_names.params, _result_type(m)) for m in methods]


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _doc_text(text: str) -> str:
    return text.replace("*/", "*\\/")


def _jsdoc(lines: list[str], indent: str) -> list[str]:
    if len(lines) == 1:
        return [f"{indent}/** {lines[0]} */"]
    return [f"{indent}/**", *(f"{indent} *{(' ' + line) if line else ''}" for line in lines), f"{indent} */"]


def _param_doc(m: RegistryMethod, p: RegistryParam) -> str:
    token = f"`{p.field.type}`{', nullable' if p.field.nullable else ''}"
    if p.record is not None:
        return f"The packed `{p.record}`, as its IPC bytes ({token})."
    return f"The `{p.name}` column ({token})."


def _schema_expr(fields: list[pa.Field[Any]], origin: str, indent: str) -> str:
    if not fields:
        ts_schemas._use("schema")
        return "schema([])"
    ts_schemas._use("schema")
    inner = "".join(f"{indent}  {ts_schemas._emit_field(f, origin=f'{origin}.{f.name}')},\n" for f in fields)
    return f"schema([\n{inner}{indent}])"


def _render_params_interface(tm: TsMethod) -> list[str]:
    m = tm.method
    lines = _jsdoc([f"The params of `{m.name}`, one property per params-schema column."], "")
    if not m.params:
        return [*lines, f"export type {tm.params_type} = Record<string, never>;"]
    lines.append(f"export interface {tm.params_type} {{")
    for p in m.params:
        lines += _jsdoc([_param_doc(m, p)], "  ")
        lines.append(f"  {p.name}: {_field_ts_type(p.field, f'{m.name}({p.name})')};")
    lines.append("}")
    return lines


def _member_doc(tm: TsMethod) -> list[str]:
    m = tm.method
    out: list[str] = []
    for i, para in enumerate(paragraphs(m.doc) or [f"The `{m.name}` method."]):
        if i:
            out.append("")
        out.append(_doc_text(para))
    out.append("")
    if m.is_stream:
        header = f", headed by a `{m.header_type.__name__}`" if m.header_type is not None else ""
        out.append(f"Wire method `{m.name}` (stream{header}). Default: answers `UNIMPLEMENTED`.")
    else:
        out.append(f"Wire method `{m.name}` (unary). Default: answers `UNIMPLEMENTED`.")
    return out


def _render_member(tm: TsMethod) -> list[str]:
    lines = _jsdoc(_member_doc(tm), "  ")
    if tm.method.is_stream:
        lines.append(f"  readonly {tm.key}: VgiStream;")
    else:
        lines.append(f"  {tm.key}(params: {tm.params_type}, ctx: CallContext): VgiReply<{tm.result_type}>;")
    return lines


def _render_spec(tm: TsMethod) -> list[str]:
    m = tm.method
    row = BACKEND.row(m)
    origin = m.name
    lines = [
        "  {",
        f'    name: "{m.name}",',
        f'    key: "{tm.key}",',
        f'    type: "{"stream" if m.is_stream else "unary"}",',
        f"    params: {_schema_expr(row.params, f'{origin}(params)', '    ')},",
    ]
    if m.is_stream:
        if row.header is None:
            lines.append("    header: null,")
        else:
            lines.append(f"    header: {_schema_expr(row.header, f'{origin}(header)', '    ')},")
    else:
        assert row.result is not None  # VoidResult.EMPTY_SCHEMA: a unary always has a result schema
        lines.append(f"    result: {_schema_expr(row.result, f'{origin}(result)', '    ')},")
    lines.append(f"    doc: {json.dumps(summary(m.doc) or m.name, ensure_ascii=False)},")
    lines.append("  },")
    return lines


def _render_default(tm: TsMethod) -> str:
    if tm.method.is_stream:
        return f'  {tm.key}: unimplementedStream("{tm.method.name}"),'
    return f'  {tm.key}: async () => {{\n    throw notImplemented("{tm.method.name}");\n  }},'


_PRELUDE = """\
/** The protocol's wire name: the `vgi_rpc.protocol` routing key and the HTTP path segment. */
export const VGI_PROTOCOL_NAME = "{name}";

/** The protocol's semver, stamped on every request by a client of this protocol. */
export const VGI_PROTOCOL_VERSION = "{version}";

/** What a unary handler returns: the result row, now or later. */
export type VgiReply<T> = T | Promise<T>;

/** The result of a unary method with no `result` column. */
export type NoResult = Record<string, never>;

/** A `list` column's value as the Arrow backend decodes it: a vector on arrow-js, an array on flechette. */
export type WireList<T> = Iterable<T>;

/** A `map` column's value: vgi-rpc passes it through opaque, in the Arrow backend's own representation. */
export type WireMap = unknown;

/** The exchange (bidirectional) callbacks of a stream method. */
export interface VgiExchangeStream<S = unknown> {{
  readonly kind: "exchange";
  readonly inputSchema: SchemaLike;
  readonly outputSchema: SchemaLike;
  readonly init: ExchangeInit<S>;
  readonly exchange: ExchangeFn<S>;
  readonly onCancel?: (state: S) => Promise<void> | void;
  readonly headerInit?: HeaderInit;
}}

/** The producer (server-streaming) callbacks of a stream method. */
export interface VgiProducerStream<S = unknown> {{
  readonly kind: "producer";
  readonly outputSchema: SchemaLike;
  readonly init: ProducerInit<S>;
  readonly produce: ProducerFn<S>;
  readonly onCancel?: (state: S) => Promise<void> | void;
  readonly headerInit?: HeaderInit;
}}

/**
 * A stream method's implementation. The params and header schemas come from
 * the registration table, never from here: only the callbacks are the
 * implementation's to choose. Build one with {{@link exchangeStream}} or
 * {{@link producerStream}}, which infer the state type from `init`.
 */
// biome-ignore lint/suspicious/noExplicitAny: each implementation's stream state is its own type.
export type VgiStream = VgiExchangeStream<any> | VgiProducerStream<any>;

/** An exchange {{@link VgiStream}}, its state type inferred from `init`. */
export function exchangeStream<S>(callbacks: Omit<VgiExchangeStream<S>, "kind">): VgiStream {{
  return {{ kind: "exchange", ...callbacks }};
}}

/** A producer {{@link VgiStream}}, its state type inferred from `init`. */
export function producerStream<S>(callbacks: Omit<VgiProducerStream<S>, "kind">): VgiStream {{
  return {{ kind: "producer", ...callbacks }};
}}
"""

_SERVICE_DOC = [
    "The `vgi.v2` protocol: every method a VGI worker serves, generated from the",
    "reference (`vgi.protocol.VgiProtocol` in vgi-python).",
    "",
    "The protocol is the unit of optionality: every method is registered, whether",
    "or not this worker implements it, so `vgi_rpc.Reflection.v1` reports one",
    "`vgi.v2` hash in every SDK. Build an implementation by spreading handlers",
    "over {@link UNIMPLEMENTED_VGI_SERVICE}; register it with",
    "{@link registerVgiService}. Never change a signature here by hand:",
    "regenerate it, so the change comes from the protocol.",
]

_POSTLUDE = """\
/** The message every unimplemented method answers with. */
export function unimplementedMessage(method: string): string {
  return `${method} is not implemented by this worker`;
}

function notImplemented(method: string): MethodNotImplementedError {
  return new MethodNotImplementedError(unimplementedMessage(method));
}

function unimplementedStream(method: string): VgiStream {
  return producerStream({
    outputSchema: schema([]),
    init: () => {
      throw notImplemented(method);
    },
    produce: () => {},
  });
}

/**
 * Register every `vgi.v2` method on *protocol*, with the reference's schemas
 * from {@link VGI_V2_METHODS} and the handlers from *service*.
 */
export function registerVgiService(protocol: Protocol, service: VgiService): Protocol {
  for (const m of VGI_V2_METHODS) {
    if (m.type === "unary") {
      const handler = service[m.key] as (params: object, ctx: CallContext) => ReturnType<UnaryHandler>;
      protocol.unary(m.name, {
        params: m.params,
        result: m.result,
        doc: m.doc,
        handler: (params, ctx) => handler.call(service, params, ctx as CallContext),
      });
      continue;
    }
    const stream = service[m.key] as VgiStream;
    const common = {
      params: m.params,
      outputSchema: stream.outputSchema,
      headerSchema: m.header ?? undefined,
      headerInit: stream.headerInit,
      onCancel: stream.onCancel,
      doc: m.doc,
    };
    if (stream.kind === "exchange") {
      protocol.exchange(m.name, {
        ...common,
        inputSchema: stream.inputSchema,
        init: stream.init,
        exchange: stream.exchange,
      });
    } else {
      protocol.producer(m.name, { ...common, init: stream.init, produce: stream.produce });
    }
  }
  return protocol;
}

/** A `Protocol` named {@link VGI_PROTOCOL_NAME} at {@link VGI_PROTOCOL_VERSION}, serving *service*. */
export function createVgiProtocol(service: VgiService): Protocol {
  return registerVgiService(new Protocol(VGI_PROTOCOL_NAME, { protocolVersion: VGI_PROTOCOL_VERSION }), service);
}
"""


def _render_body(model: Sequence[RegistryMethod]) -> str:
    methods = ts_methods(model)

    ts_schemas._IMPORTS_IN_USE.clear()
    specs: list[str] = []
    for tm in methods:
        specs += _render_spec(tm)
    ts_schemas._use("schema")  # unimplementedStream's empty output schema
    arrow_imports = sorted(ts_schemas._IMPORTS_IN_USE)

    lines = ["import {"]
    lines += [
        "  type CallContext,",
        "  type ExchangeFn,",
        "  type ExchangeInit,",
        "  type HeaderInit,",
        "  MethodNotImplementedError,",
        "  type ProducerFn,",
        "  type ProducerInit,",
        "  Protocol,",
        "  type SchemaLike,",
        "  type UnaryHandler,",
    ]
    lines.append('} from "@query-farm/vgi-rpc";')
    lines.append("import {")
    lines += [f"  {sym}," for sym in arrow_imports]
    lines.append("  type VgiSchema,")
    lines.append('} from "../arrow/index.js";')
    lines.append("")
    lines += _PRELUDE.format(name=protocol_name(), version=protocol_version()).splitlines()

    lines.append("")
    lines.append("// " + "-" * 76)
    lines.append("// Params")
    lines.append("// " + "-" * 76)
    for tm in methods:
        lines.append("")
        lines += _render_params_interface(tm)

    lines.append("")
    lines.append("// " + "-" * 76)
    lines.append("// The service")
    lines.append("// " + "-" * 76)
    lines.append("")
    lines += _jsdoc(_SERVICE_DOC, "")
    lines.append("export interface VgiService {")
    for i, tm in enumerate(methods):
        if i:
            lines.append("")
        lines += _render_member(tm)
    lines.append("}")

    lines.append("")
    lines.append("// " + "-" * 76)
    lines.append("// The registration table")
    lines.append("// " + "-" * 76)
    lines.append("")
    lines += _jsdoc(["One `vgi.v2` method: its wire name, its `VgiService` member, and its schemas."], "")
    lines += [
        "export type VgiMethodSpec =",
        "  | {",
        "      readonly name: string;",
        "      readonly key: keyof VgiService;",
        '      readonly type: "unary";',
        "      readonly params: VgiSchema;",
        "      /** The `result` column, or no fields for a method that returns nothing. */",
        "      readonly result: VgiSchema;",
        "      readonly doc: string;",
        "    }",
        "  | {",
        "      readonly name: string;",
        "      readonly key: keyof VgiService;",
        '      readonly type: "stream";',
        "      readonly params: VgiSchema;",
        "      readonly header: VgiSchema | null;",
        "      readonly doc: string;",
        "    };",
        "",
    ]
    lines += _jsdoc(
        [
            "Every `vgi.v2` method, sorted by wire name, with the reference's",
            "params / result / header schemas. This table is what",
            "{@link registerVgiService} registers, so it is what reflection hashes.",
        ],
        "",
    )
    lines.append("export const VGI_V2_METHODS: readonly VgiMethodSpec[] = [")
    lines += specs
    lines.append("];")

    lines.append("")
    lines += _jsdoc(
        [
            "A `VgiService` in which every method answers `UNIMPLEMENTED` /",
            '`method_not_implemented` with `"<method> is not implemented by this worker"`.',
        ],
        "",
    )
    lines.append("export const UNIMPLEMENTED_VGI_SERVICE: VgiService = Object.freeze({")
    lines += [_render_default(tm) for tm in methods]
    lines.append("});")
    lines.append("")
    lines += _POSTLUDE.splitlines()
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Derive-back: vgi-rpc-typescript registers the table's schema values as given
# ---------------------------------------------------------------------------

_SPEC = re.compile(
    r'  \{\n    name: "(\w+)",\n    key: "(\w+)",\n    type: "(unary|stream)",\n'
    r"    params: (schema\(.*?\)),\n    (result|header): (null|schema\(.*?\)),\n    doc: [^\n]+,\n  \},",
    re.DOTALL,
)
_DEFAULT = re.compile(
    r'  (\w+): (?:async \(\) => \{\n    throw notImplemented\("(\w+)"\);\n  \}|unimplementedStream\("(\w+)"\)),'
)


class _TimeUnit:
    def __getattr__(self, name: str) -> str:
        return {"SECOND": "s", "MILLISECOND": "ms", "MICROSECOND": "us", "NANOSECOND": "ns"}[name]


#: The vgi-typescript Arrow facade's factories, as pyarrow constructors. Note the
#: facade's ``dictionary(valueType, indexType, ordered?)`` argument order.
FACADE = ArrowDialect(
    rewrites=(),
    names={
        "schema": pa.schema,
        "field": lambda name, dtype, nullable: pa.field(name, dtype, nullable=nullable),
        "binary": pa.binary,
        "largeBinary": pa.large_binary,
        "utf8": pa.string,
        "largeUtf8": pa.large_string,
        "bool": pa.bool_,
        **{t: getattr(pa, t) for t in ("int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64")},
        "float32": pa.float32,
        "float64": pa.float64,
        "list": pa.list_,
        "map": lambda key, value, keys_sorted: pa.map_(key, value, keys_sorted),
        "dictionary": lambda value, index, ordered=False: pa.dictionary(index, value, ordered=ordered),
        "struct": pa.struct,
        "timestamp": lambda unit, tz=None: pa.timestamp(unit, tz=tz),
        "TimeUnit": _TimeUnit(),
    },
)


class TsRegistry(RegistryBackend):
    """vgi-typescript's ``vgi-service.ts``: the service type and the registration table."""

    key = "typescript"
    language = "TypeScript"
    module = "vgi.codegen.ts_registry"
    target = TARGET
    repo = "vgi-typescript"
    root_env = "VGI_TYPESCRIPT_ROOT"
    void_result = VoidResult.EMPTY_SCHEMA
    banner_head = "// Copyright 2025, 2026 Query Farm LLC - https://query.farm\n"
    tamper = Tamper(
        start='    name: "catalog_schemas",',
        end="doc:",
        old='field("transaction_opaque_data", binary(), true)',
        new='field("transaction_opaque_data", binary(), false)',
    )

    def render_body(self, methods: Sequence[RegistryMethod]) -> str:  # noqa: D102
        return _render_body(methods)

    def derive(self, text: str) -> list[DerivedMethod]:  # noqa: D102
        defaults = {m.group(1): m.group(2) or m.group(3) for m in _DEFAULT.finditer(text)}
        out: list[DerivedMethod] = []
        for match in _SPEC.finditer(text):
            name, key, kind, params_expr, slot, slot_expr = match.groups()
            expect(defaults.get(key) == name, f"{key}'s default answers for {defaults.get(key)}, not {name}")
            if kind == "unary":
                member = rf"\n  {key}\(params: \w+Params, ctx: CallContext\)"
            else:
                member = rf"\n  readonly {key}: VgiStream;"
            expect(re.search(member, text), f"VgiService has no {kind} member {key}")
            params = list(FACADE.schema(params_expr))
            if kind == "unary":
                expect(slot == "result", f"{name}: a unary spec carries a result")
                method_kind, result = self.unary_from_result(name, list(FACADE.schema(slot_expr)))
                out.append(DerivedMethod(name, method_kind, params, result))
            else:
                expect(slot == "header", f"{name}: a stream spec carries a header")
                header = None if slot_expr == "null" else FACADE.schema(slot_expr)
                out.append(DerivedMethod(name, MethodKind.STREAM, params, header=header))
        return out


BACKEND = TsRegistry()
emit, render, main = BACKEND.emit, BACKEND.render, BACKEND.main


if __name__ == "__main__":
    main()
