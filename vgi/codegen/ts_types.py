# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit typed builders and codecs for VGI protocol records, for vgi-typescript.

`vgi.codegen.ts_client` emits the *shape* of every request and response the
protocol's method signatures reach, and `vgi.codegen.ts_schemas` the Arrow
schemas. What neither gives a worker is a way to *construct* a record the way
Python does: a dataclass has defaults, so a Python catalog writes
``CatalogAttachResult(attach_opaque_data=..., ...)`` and every appended column
(``supports_catalog_contents``, say) is filled in for it, while a TypeScript
worker used to list every field by hand and restate each default — and a
forgotten one is a wire-protocol bug the extension catches only at runtime (it
pins the full result schema).

So for each record in `TS_RECORD_TYPES` this module emits, from the record's
``ARROW_SCHEMA`` (the authority, as in `vgi.codegen.csharp_types`) and its
dataclass defaults:

- the record's ``interface`` — or, when `ts_client` already emits one of that
  name, a re-export of it (checked here to have the same fields, in order, with
  the same nullability, so there is one definition of each shape);
- ``<Name>Init``: the interface with every defaulted field optional;
- ``build<Name>(init)``: fills the Python defaults, in schema order;
- ``encode<Name>`` / ``decode<Name>``: single-row Arrow IPC via the SDK's
  ``encodeASD`` / ``decodeASD``, against the record's generated schema const.

A record nested as a struct column (``SchemaContents``, the row type of
``CatalogContentsResponse.schemas``) is typed by its own record's interface.
Records that ``ts_client`` does not reach are emitted here in full.

Add a record by appending its class to `TS_RECORD_TYPES`. Anything the mapping
cannot express raises `GeneratorError` rather than emitting a type that silently
disagrees with the schema.

.. code-block:: bash

   uv run --project ~/Development/vgi-python python scripts/regen_generated.py

``tests/test_generated_ts_types.py`` fails when the checked-in file and the
generator disagree.
"""

from __future__ import annotations

import dataclasses
import enum
import io
import json
import re
import sys
import typing
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from vgi.catalog.catalog_interface import CatalogAttachResult
from vgi.codegen import ts_client
from vgi.codegen._common import (
    EXTRA_RESPONSE_TYPES,
    REQUEST_TYPES,
    GeneratorError,
    collect_schemas,
    provenance_comment,
)
from vgi.codegen.csharp_types import _parse_docstring
from vgi.protocol import CatalogContentsResponse, SchemaContents

if TYPE_CHECKING:
    from typing import TextIO


GENERATOR_VERSION = "1"

#: The records this module emits builders and codecs for. Append to extend.
TS_RECORD_TYPES: tuple[type, ...] = (
    CatalogAttachResult,
    SchemaContents,
    CatalogContentsResponse,
)


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------


@dataclass
class _Field:
    name: str
    ts_type: str
    nullable: bool
    #: TS expression for the Python default, or ``None`` when the field has none.
    default: str | None
    doc: str | None


@dataclass
class _Record:
    name: str
    schema_const: str
    fields: list[_Field]
    doc: str | None
    #: True when `ts_client` already emits the interface; it is re-exported.
    reexported: bool


def _ts_type(dtype: pa.DataType, origin: str) -> str:
    """The TypeScript type of one Arrow type, matching what ``decodeASD`` yields."""
    if pa.types.is_boolean(dtype):
        return "boolean"
    if pa.types.is_integer(dtype) or pa.types.is_floating(dtype):
        # ts_client types every protocol integer as `number`; encodeASD accepts it.
        return "number"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "string"
    if pa.types.is_dictionary(dtype) and pa.types.is_string(dtype.value_type):
        return "string"
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype) or pa.types.is_fixed_size_binary(dtype):
        return "Uint8Array"
    if pa.types.is_timestamp(dtype):
        return "bigint"
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        item = dtype.value_field
        inner = _ts_type(item.type, f"{origin}[item]")
        return f"{inner}[]" if " " not in inner else f"({inner})[]"
    if pa.types.is_map(dtype):
        if not (pa.types.is_string(dtype.key_type) and pa.types.is_string(dtype.item_type)):
            raise GeneratorError(f"{origin}: only map<utf8, utf8> is supported, got {dtype}.")
        return "Record<string, string>"
    if pa.types.is_struct(dtype):
        # A struct column is the record whose own schema is this struct.
        for record in TS_RECORD_TYPES:
            if pa.struct(list(record.ARROW_SCHEMA)).equals(dtype):  # type: ignore[attr-defined]
                return record.__name__
        raise GeneratorError(f"{origin}: no record in TS_RECORD_TYPES has the struct schema {dtype}.")
    raise GeneratorError(
        f"vgi.codegen.ts_types: unsupported Arrow type {dtype} at {origin}.\n"
        "Add a case to _ts_type() in vgi/codegen/ts_types.py.",
    )


def _literal(value: object, origin: str) -> str:
    """Render a Python default as a TypeScript expression."""
    if value is None:
        return "null"
    if isinstance(value, enum.Enum):
        return json.dumps(value.name)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list | tuple) and not value:
        return "[]"
    if isinstance(value, dict) and not value:
        return "{}"
    raise GeneratorError(f"{origin}: cannot render the default {value!r} in TypeScript.")


def _default(dc_field: dataclasses.Field[Any] | None, origin: str) -> str | None:
    if dc_field is None:
        return None
    if dc_field.default is not dataclasses.MISSING:
        return _literal(dc_field.default, origin)
    if dc_field.default_factory is not dataclasses.MISSING:
        return _literal(dc_field.default_factory(), origin)
    return None


def _schema_consts() -> dict[str, pa.Schema]:
    return {
        f"{es.name}Schema": es.schema
        for es in collect_schemas(extra_response_types=(*EXTRA_RESPONSE_TYPES, *REQUEST_TYPES))
    }


def _schema_const_for(cls: type, consts: dict[str, pa.Schema]) -> str:
    """The generated ``…Schema`` const carrying *cls*'s ARROW_SCHEMA.

    The record's own name wins; otherwise the (unique) method-result const with
    the same schema — ``CatalogContentsResponse`` is ``CatalogContentsResultSchema``.
    """
    schema: pa.Schema = cls.ARROW_SCHEMA  # type: ignore[attr-defined]
    own = f"{cls.__name__}Schema"
    if own in consts and consts[own].equals(schema, check_metadata=False):
        return own
    matches = sorted(
        n for n, s in consts.items() if n.endswith("ResultSchema") and s.equals(schema, check_metadata=False)
    )
    if len(matches) != 1:
        raise GeneratorError(f"{cls.__name__}: expected exactly one generated schema const for it, found {matches}.")
    return matches[0]


def _ts_client_interfaces() -> dict[str, list[tuple[str, bool]]]:
    """``{interface: [(field, optional), ...]}`` for every record `ts_client` emits."""
    ctx = ts_client._Ctx()
    ts_client._collect(ctx)
    out: dict[str, list[tuple[str, bool]]] = {}
    for cls, name in ctx.dataclasses_seen.items():
        hints = typing.get_type_hints(cls, include_extras=True)
        fields = []
        for f in dataclasses.fields(cls):
            unwrapped, _ = ts_client._unwrap(hints.get(f.name, f.type))
            fields.append((f.name, ts_client._is_optional(unwrapped)))
        out[name] = fields
    return out


def build_model() -> list[_Record]:
    """Lay out every record in `TS_RECORD_TYPES`."""
    consts = _schema_consts()
    client = _ts_client_interfaces()
    records: list[_Record] = []
    for cls in TS_RECORD_TYPES:
        name = cls.__name__
        schema: pa.Schema = cls.ARROW_SCHEMA  # type: ignore[attr-defined]
        dc_fields = {f.name: f for f in dataclasses.fields(cls)}
        summary, attr_docs = _parse_docstring(cls)
        fields: list[_Field] = []
        for af in schema:
            origin = f"{name}.{af.name}"
            if af.name not in dc_fields:
                raise GeneratorError(f"{origin}: in ARROW_SCHEMA but not a dataclass field.")
            fields.append(
                _Field(
                    name=af.name,
                    ts_type=_ts_type(af.type, origin),
                    nullable=af.nullable,
                    default=_default(dc_fields[af.name], origin),
                    doc=attr_docs.get(af.name),
                )
            )
        reexported = name in client
        if reexported:
            ours = [(f.name, f.nullable) for f in fields]
            if client[name] != ours:
                raise GeneratorError(
                    f"{name}: ts_client's interface {client[name]} disagrees with ARROW_SCHEMA {ours}.",
                )
        records.append(_Record(name, _schema_const_for(cls, consts), fields, summary, reexported))
    return records


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _jsdoc(text: str | None, indent: str) -> list[str]:
    if not text:
        return []
    flat = " ".join(text.split()).replace("*/", "*\\/")
    # Python doc markup -> plain TSDoc code spans: [`X`][] and ``x`` -> `X`, `x`.
    flat = re.sub(r"\[`([^`]+)`\]\[[^\]]*\]", r"`\1`", flat)
    flat = re.sub(r"``([^`]+)``", r"`\1`", flat)
    return [f"{indent}/** {flat} */"]


def _render_interface(r: _Record) -> list[str]:
    lines = _jsdoc(r.doc, "")
    lines.append(f"export interface {r.name} {{")
    for f in r.fields:
        lines += _jsdoc(f.doc, "  ")
        if f.nullable:
            lines.append(f"  {f.name}?: {f.ts_type} | null;")
        else:
            lines.append(f"  {f.name}: {f.ts_type};")
    lines.append("}")
    return lines


def _render_record(r: _Record) -> str:
    lines: list[str] = [f"// {'-' * 76}", f"// {r.name}  (schema: {r.schema_const})", f"// {'-' * 76}", ""]
    if not r.reexported:
        lines += _render_interface(r)
        lines.append("")

    defaulted = [f.name for f in r.fields if f.default is not None]
    if defaulted:
        keys = f"{r.name}DefaultedField"
        lines.append(f"/** Fields of `{r.name}` that have a protocol default. */")
        lines.append(f"export type {keys} =")
        lines += [f"  | {json.dumps(n)}" for n in defaulted]
        lines[-1] += ";"
        lines.append(f"/** `{r.name}` with every field that has a protocol default optional. */")
        lines.append(f"export type {r.name}Init = Omit<{r.name}, {keys}> & Partial<Pick<{r.name}, {keys}>>;")
    else:
        lines.append(f"/** `{r.name}` has no defaulted fields, so its init is the record itself. */")
        lines.append(f"export type {r.name}Init = {r.name};")
    lines.append("")

    lines.append(f"/** Build a complete `{r.name}`, filling the protocol defaults, in wire order. */")
    lines.append(f"export function build{r.name}(init: {r.name}Init): {r.name} {{")
    lines.append("  return {")
    for f in r.fields:
        if f.default is not None:
            lines.append(f"    {f.name}: init.{f.name} ?? {f.default},")
        elif f.nullable:
            lines.append(f"    {f.name}: init.{f.name} ?? null,")
        else:
            lines.append(f"    {f.name}: init.{f.name},")
    lines.append("  };")
    lines.append("}")
    lines.append("")
    lines.append(
        f"export const encode{r.name} = (v: {r.name}): Uint8Array => encodeASD({r.schema_const}, v);",
    )
    lines.append(
        f"export const decode{r.name} = (b: Uint8Array): {r.name} => decodeASD<{r.name}>({r.schema_const}, b);",
    )
    return "\n".join(lines) + "\n"


def emit(out: TextIO) -> None:
    """Write the generated TypeScript record types to *out*."""
    records = build_model()
    body = io.StringIO()
    body.write('import { encodeASD, decodeASD } from "../codec/asd.js";\n')
    body.write("import {\n")
    for const in sorted({r.schema_const for r in records}):
        body.write(f"  {const},\n")
    body.write('} from "./vgi-protocol-schemas.js";\n')
    reexported = sorted(r.name for r in records if r.reexported)
    if reexported:
        body.write(f'import type {{ {", ".join(reexported)} }} from "./vgi-client.js";\n')
        body.write(f"export type {{ {', '.join(reexported)} }};\n")
    for r in records:
        body.write("\n")
        body.write(_render_record(r))

    out.write("// ============================================================================\n")
    out.write(
        provenance_comment(
            generator_module="vgi.codegen.ts_types",
            generator_command="python -m vgi.codegen.ts_types",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=[
                "uv run --project ~/Development/vgi-python python scripts/regen_generated.py",
            ],
            body=body.getvalue(),
        )
    )
    out.write("// ============================================================================\n")
    out.write("\n")
    out.write(body.getvalue())


def main() -> None:
    """CLI entrypoint: write the generated TS record types to stdout."""
    try:
        emit(sys.stdout)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
