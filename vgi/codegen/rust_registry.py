# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit vgi-rust's ``vgi_service.rs``: the whole ``vgi.v2`` registry, for vgi-rust.

vgi-rpc-rust registers a protocol explicitly: one ``MethodInfo`` per method,
each carrying its params / result / header Arrow schemas and a handler closure
over the raw ``Request``. Nothing is derived by reflection, so the generated
file carries all three pieces of the registry (see :mod:`vgi.codegen._registry`
for the model and the reasons):

- ``trait VgiService`` -- one method per ``vgi.v2`` method, each with a default
  body answering vgi-rpc-rust's ``RpcError::method_not_implemented``
  (``UNIMPLEMENTED`` / ``method_not_implemented`` / ``"<method> is not
  implemented by this worker"``). vgi-rust's ``Dispatcher`` implements it and
  overrides what it serves. A stream method also gets a
  ``decode_<method>_state`` hook, the HTTP-continuation state decoder, with the
  same default;
- ``fn register`` -- the registration table: every method's ``MethodInfo``
  with the reference's params, result and header schemas spelled out field by
  field (through :func:`vgi.codegen.rust_schemas._emit_field`, the same Arrow ->
  arrow-rs mapping as ``protocol_schemas.rs``), its handler delegating to the
  trait;
- ``fn not_implemented`` -- the error the defaults answer.

Every handler takes the raw ``&Request`` and the ``&CallContext``, which is
vgi-rpc-rust's own handler shape: vgi-rust decodes params itself (packed
requests through ``wire::from_batch``, flat columns by name), after opening
sealed opaque values in the batch. So, unlike Java and C#, there is no
hand-maintained type table here: nothing in a Rust signature can change a
schema, because the schemas are in the table, not derived from the signature.

The one Rust-only choice is the stream kind: every ``vgi.v2`` stream registers
as ``MethodType::Dynamic`` (the handler's ``StreamResult`` picks producer or
exchange per call). The protocol hash records only ``"stream"``, so this is
wire-identical.

.. code-block:: bash

   uv run --project ~/Development/vgi-python python scripts/regen_generated.py

:class:`RustRegistry` is this language's backend for the one registry
generator (:mod:`vgi.codegen._registry_backend`); its ``derive`` evaluates the
rendered ``register`` table's arrow-rs schema expressions, which vgi-rpc-rust
registers as given.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

import pyarrow as pa

from vgi.codegen import rust_schemas
from vgi.codegen._registry import MethodKind, RegistryMethod, protocol_name, protocol_version
from vgi.codegen._registry_backend import (
    ArrowDialect,
    DerivedMethod,
    RegistryBackend,
    Tamper,
    VoidResult,
    call_args,
    expect,
)

#: The generated module, relative to the vgi-rust checkout.
TARGET = "vgi/src/protocol/vgi_service.rs"

#: The ``MethodType`` every ``vgi.v2`` stream registers with (see module doc).
RUST_STREAM_KIND = "MethodType::Dynamic"


def _type_text(dtype: pa.DataType) -> str:
    """A short, human-readable Arrow type for docs (``list<utf8>``, ``map<utf8, utf8>``)."""
    if pa.types.is_list(dtype):
        return f"list<{_type_text(dtype.value_type)}>"
    if pa.types.is_map(dtype):
        return f"map<{_type_text(dtype.key_type)}, {_type_text(dtype.item_type)}>"
    if pa.types.is_dictionary(dtype):
        return f"dictionary<{_type_text(dtype.index_type)}, {_type_text(dtype.value_type)}>"
    if dtype.equals(pa.string()):
        return "utf8"
    return str(dtype)


def _field_text(field: pa.Field[Any]) -> str:
    return f"{field.name}: {_type_text(field.type)}{'?' if field.nullable else ''}"


def _doc_text(text: str) -> str:
    """Make docstring prose safe for rustdoc under ``-D warnings``.

    RST ``literal`` becomes Markdown `literal`; outside code spans, brackets
    (intra-doc links) and angle brackets (HTML tags) are escaped.
    """
    text = text.replace("``", "`")
    out: list[str] = []
    in_code = False
    for ch in text:
        if ch == "`":
            in_code = not in_code
        elif not in_code and ch in "[]<>":
            out.append("\\")
        out.append(ch)
    return "".join(out)


def _wrap(text: str, prefix: str, width: int = 100) -> list[str]:
    """Wrap *text* into ``prefix``-led lines no wider than *width*."""
    # Split on spaces outside `code spans`, so a span never breaks across lines.
    words: list[str] = []
    in_code = False
    current = ""
    for ch in text:
        if ch == "`":
            in_code = not in_code
        if ch == " " and not in_code:
            if current:
                words.append(current)
            current = ""
        else:
            current += ch
    if current:
        words.append(current)
    lines: list[str] = []
    line = ""
    for word in words:
        candidate = f"{line} {word}" if line else word
        if line and len(prefix) + len(candidate) > width:
            lines.append(prefix + line)
            line = word
        else:
            line = candidate
    if line:
        lines.append(prefix + line)
    return lines


def _schema_expr(fields: list[pa.Field[Any]], *, origin: str, indent: str) -> list[str]:
    """``schema(vec![...])`` over *fields*, one ``Field::new`` per line."""
    if not fields:
        return [f"{indent}schema(vec![]),"]
    lines = [f"{indent}schema(vec!["]
    for f in fields:
        lines.append(f"{indent}    {rust_schemas._emit_field(f, origin=f'{origin}.{f.name}')},")
    lines.append(f"{indent}]),")
    return lines


# ---------------------------------------------------------------------------
# The trait
# ---------------------------------------------------------------------------


def _trait_method(m: RegistryMethod) -> list[str]:
    doc = m.summary or f"The `{m.name}` method."
    lines = _wrap(_doc_text(doc), "    /// ")
    lines.append("    ///")
    kind = "stream" if m.is_stream else "unary"
    params = ", ".join(_field_text(p.field) for p in m.params)
    shape = f"Wire method `{m.name}` ({kind}): params `({params})`"
    if m.kind is MethodKind.STREAM:
        if m.header_type is not None:
            shape += f", headed by a `{m.header_type.__name__}`"
    elif m.result_field is not None:
        shape += f", result `{_field_text(m.result_field)}`"
    else:
        shape += ", no result"
    shape += ". Default: answers `UNIMPLEMENTED`."
    lines += _wrap(shape, "    /// ")
    ret = "Result<StreamResult>" if m.is_stream else "Result<Option<RecordBatch>>"
    lines += [
        f"    fn {m.name}(&self, _req: &Request, _ctx: &CallContext) -> {ret} {{",
        f'        Err(not_implemented("{m.name}"))',
        "    }",
    ]
    if m.needs_state_decoder:
        lines += [
            "",
            f"    /// Rebuild a `{m.name}` stream's state from an HTTP continuation token (the bytes its",
            "    /// state serialized to). Default: answers `UNIMPLEMENTED`.",
            f"    fn decode_{m.name}_state(&self, _state: &[u8]) -> Result<StreamStateKind> {{",
            f'        Err(not_implemented("{m.name}"))',
            "    }",
        ]
    return lines


_TRAIT_DOC = """\
/// The `vgi.v2` protocol: every method a VGI worker serves, generated from the
/// reference (`vgi.protocol.VgiProtocol` in vgi-python).
///
/// Every method has a default body answering `UNIMPLEMENTED` /
/// `method_not_implemented` with `"<method> is not implemented by this worker"`;
/// an implementation overrides the methods it serves. [`register`] hosts every
/// method, implemented or not, with the reference's schemas: the protocol is the
/// unit of optionality, and `vgi_rpc.Reflection.v1` reports one `vgi.v2` hash in
/// every SDK.
///
/// Each method receives the raw request, whose batch matches the method's params
/// schema (documented on the method), and returns the raw result batch. Never
/// change a signature or a schema here by hand: regenerate, so the change comes
/// from the protocol."""


# ---------------------------------------------------------------------------
# The registration table
# ---------------------------------------------------------------------------


def _register_block(m: RegistryMethod) -> list[str]:
    row = BACKEND.row(m)
    params = row.params
    lines = ["    {", "        let s = svc.clone();"]
    if not m.is_stream:
        assert row.result is not None  # VoidResult.EMPTY_SCHEMA: a unary always has a result schema
        lines.append("        srv.register(MethodInfo::unary(")
        lines.append(f'            "{m.name}",')
        lines += _schema_expr(params, origin=f"{m.name}.params", indent="            ")
        lines += _schema_expr(row.result, origin=f"{m.name}.result", indent="            ")
        lines.append(f"            move |req, ctx| s.{m.name}(req, ctx),")
        lines.append("        ));")
    else:
        lines.append("        let d = svc.clone();")
        lines.append("        let info = MethodInfo::stream(")
        lines.append(f'            "{m.name}",')
        lines.append(f"            {RUST_STREAM_KIND},")
        lines += _schema_expr(params, origin=f"{m.name}.params", indent="            ")
        lines.append(f"            move |req, ctx| s.{m.name}(req, ctx),")
        lines.append("        )")
        lines.append(f"        .with_state_decoder(Arc::new(move |state: &[u8]| d.decode_{m.name}_state(state)));")
        header = row.header
        if header is not None:
            lines.append("        let info = info.header_schema(schema(vec![")
            for f in header:
                field = rust_schemas._emit_field(f, origin=f"{m.name}.header.{f.name}")
                lines.append(f"            {field},")
            lines.append("        ]));")
        lines.append("        srv.register(info);")
    lines.append("    }")
    return lines


def _render_body(methods: Sequence[RegistryMethod]) -> str:
    name = protocol_name()
    version = protocol_version()
    has_stream = any(m.is_stream for m in methods)

    lines = [
        "// Copyright 2025, 2026 Query Farm LLC - https://query.farm",
        "",
        f"//! The `{name}` registry (protocol version {version}): the [`VgiService`] trait,",
        "//! its `UNIMPLEMENTED` defaults, and [`register`], the table that hosts every",
        "//! method on an [`RpcServer`] with the reference's params, result and header",
        "//! schemas. Generated from vgi-python's `VgiProtocol`; the schemas are what",
        "//! `vgi_rpc.Reflection.v1` hashes.",
        "",
        "#![allow(clippy::too_many_lines)]",
        "",
        "use std::sync::Arc;",
        "",
        "use arrow_array::RecordBatch;",
        "use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};",
    ]
    if has_stream:
        lines += [
            "use vgi_rpc::stream::StreamStateKind;",
            "use vgi_rpc::{",
            "    CallContext, MethodInfo, MethodType, Request, Result, RpcError, RpcServer, StreamResult,",
            "};",
        ]
    else:
        lines.append("use vgi_rpc::{CallContext, MethodInfo, Request, Result, RpcError, RpcServer};")
    lines.append("")
    lines += [
        "/// Every `vgi.v2` method's wire name, sorted.",
        "pub const METHOD_NAMES: &[&str] = &[",
        *[f'    "{m.name}",' for m in methods],
        "];",
        "",
        "/// The error every [`VgiService`] default answers: `UNIMPLEMENTED` /",
        '/// `method_not_implemented` with `"<method> is not implemented by this worker"`.',
        "pub fn not_implemented(method: &str) -> RpcError {",
        '    RpcError::method_not_implemented(format!("{method} is not implemented by this worker"))',
        "}",
        "",
    ]
    lines += _TRAIT_DOC.splitlines()
    lines.append("#[rustfmt::skip]")
    lines.append("pub trait VgiService: Send + Sync + 'static {")
    for i, m in enumerate(methods):
        if i:
            lines.append("")
        lines += _trait_method(m)
    lines.append("}")
    lines.append("")
    lines += [
        "fn schema(fields: Vec<Field>) -> SchemaRef {",
        "    Arc::new(Schema::new(fields))",
        "}",
        "",
        "/// Register every `vgi.v2` method on `srv`, each delegating to `svc`.",
        "///",
        "/// Methods `svc` does not override answer `UNIMPLEMENTED`, but are still",
        "/// registered with their full schemas, so the protocol hash is the reference's.",
        "#[rustfmt::skip]",
        "pub fn register<S: VgiService>(srv: &mut RpcServer, svc: Arc<S>) {",
    ]
    for m in methods:
        lines += _register_block(m)
    lines.append("}")
    text = "\n".join(lines) + "\n"
    if "Fields::" not in text:
        text = text.replace("Field, Fields, Schema", "Field, Schema", 1)
    return text


# ---------------------------------------------------------------------------
# Derive-back: vgi-rpc-rust registers the table's schemas as given
# ---------------------------------------------------------------------------

_SCALARS = {
    "Boolean": pa.bool_(),
    **{t.capitalize(): getattr(pa, t)() for t in ("int8", "int16", "int32", "int64", "float32", "float64")},
    **{"U" + t[1:].capitalize(): getattr(pa, t)() for t in ("uint8", "uint16", "uint32", "uint64")},
    "Utf8": pa.string(),
    "LargeUtf8": pa.large_string(),
    "Binary": pa.binary(),
    "LargeBinary": pa.large_binary(),
}

#: arrow-rs, as pyarrow: ``Field::new("x", DataType::Binary, true)`` becomes
#: ``Field("x", Binary, true)``; ``Arc::new`` / ``Box::new`` / ``Fields::from`` /
#: ``Some`` are the identity, ``vec![..]`` a list.
ARROW_RS = ArrowDialect(
    rewrites=(
        (r"Field::new\(", "Field("),
        (r"DataType::", ""),
        (r"(?:Arc::new|Box::new|Fields::from|Some)\(", "I("),
        (r"vec!\[", "["),
        (r"\.into\(\)", ""),
        (r"TimeUnit::(\w+)", r"TimeUnit_\1"),
    ),
    names={
        "Field": lambda name, dtype, nullable: pa.field(name, dtype, nullable=nullable),
        "I": lambda value: value,
        "None": None,
        **_SCALARS,
        "List": pa.list_,
        "LargeList": pa.large_list,
        "Map": lambda entries, keys_sorted: pa.map_(entries.type.field(0), entries.type.field(1), keys_sorted),
        "Dictionary": pa.dictionary,
        "Struct": pa.struct,
        "Timestamp": lambda unit, tz: pa.timestamp(unit, tz=tz),
        **{f"TimeUnit_{k}": v for k, v in (("Second", "s"), ("Millisecond", "ms"))},
        **{f"TimeUnit_{k}": v for k, v in (("Microsecond", "us"), ("Nanosecond", "ns"))},
    },
)

_TRAIT_METHOD = re.compile(
    r"    fn (\w+)\(&self, _req: &Request, _ctx: &CallContext\) -> "
    r"(Result<StreamResult>|Result<Option<RecordBatch>>) \{\n"
    r'        Err\(not_implemented\("(\w+)"\)\)\n    \}'
)
_REGISTER_BLOCK = re.compile(r"^    \{\n(.*?)\n    \}$", re.S | re.M)


class RustRegistry(RegistryBackend):
    """vgi-rust's ``vgi_service.rs``: the trait, its defaults and the ``register`` table."""

    key = "rust"
    language = "Rust"
    module = "vgi.codegen.rust_registry"
    target = TARGET
    repo = "vgi-rust"
    root_env = "VGI_RUST_ROOT"
    void_result = VoidResult.EMPTY_SCHEMA
    tamper = Tamper(
        start='            "catalog_schemas",',
        end="move |req, ctx|",
        old='Field::new("transaction_opaque_data", DataType::Binary, true)',
        new='Field::new("transaction_opaque_data", DataType::Binary, false)',
    )

    def render_body(self, methods: Sequence[RegistryMethod]) -> str:  # noqa: D102
        return _render_body(methods)

    def derive(self, text: str) -> list[DerivedMethod]:  # noqa: D102
        start = text.index("pub trait VgiService")
        defaults = {}
        for name, ret, stub_name in _TRAIT_METHOD.findall(text[start : text.index("\n}\n", start)]):
            expect(stub_name == name, f"{name}'s default body reports {stub_name}")
            defaults[name] = ret
        out: list[DerivedMethod] = []
        for block in _REGISTER_BLOCK.findall(text[text.index("pub fn register<") :]):
            km = re.search(r'MethodInfo::(unary|stream)\(\n\s+"(\w+)",', block)
            expect(km is not None, f"unparsed registration block {block[:80]!r}")
            assert km is not None
            kind, name = km.groups()
            expect(f"move |req, ctx| s.{name}(req, ctx)" in block, f"{name}'s handler calls another method")
            schemas = [list(ARROW_RS.schema(args)) for args in call_args(block, "schema")]
            if kind == "unary":
                expect(defaults.get(name) == "Result<Option<RecordBatch>>", f"{name}: trait default is not unary")
                expect(len(schemas) == 2, f"{name}: a unary registers params and result")
                method_kind, result = self.unary_from_result(name, schemas[1])
                out.append(DerivedMethod(name, method_kind, schemas[0], result))
            else:
                expect(defaults.get(name) == "Result<StreamResult>", f"{name}: trait default is not a stream")
                expect(re.search(r"MethodType::(Dynamic|Producer|Exchange),", block), f"{name}: no stream kind")
                expect(f"d.decode_{name}_state(state)" in block, f"{name} has no state decoder")
                has_header = ".header_schema(" in block
                expect(len(schemas) == 1 + has_header, f"{name}: unexpected schemas")
                header = pa.schema(schemas[1]) if has_header else None
                out.append(DerivedMethod(name, MethodKind.STREAM, schemas[0], header=header))
        expect(sorted(defaults) == sorted(d.name for d in out), "trait and registration table differ")
        return out


BACKEND = RustRegistry()
emit, render, main = BACKEND.emit, BACKEND.render, BACKEND.main


if __name__ == "__main__":
    main()
