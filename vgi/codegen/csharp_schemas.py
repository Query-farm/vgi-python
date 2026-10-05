# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit the VGI protocol's Arrow schemas as C#, for vgi-csharp's test project.

The companion of ``vgi.codegen.csharp_types``. That module emits the C# record
types vgi-csharp derives its wire schemas from; this one emits the schemas the
derivation must produce, taken from the protocol rather than from C#, so a test
can compare the two field for field — the same "second description" vgi-java's
``WireRecordSchemaConformanceTest`` relies on.

Generating the types makes them agree with the protocol only as far as the
generator's CLR mapping agrees with vgi-rpc-csharp's ``SchemaDerivation``; this
is what checks that. It also covers what is still written by hand: the flat
parameters of ``IVgiService``'s methods, and which record each method returns.

Three maps, all keyed by protocol names:

- ``Records`` — every record ``csharp_types`` emits, by C# type name.
- ``MethodResults`` — each unary method's result record schema (the record
  inside the ``result`` binary), by wire method name; methods returning nothing
  or raw IPC are absent.
- ``MethodParams`` — each method's params batch schema, by wire method name.

.. code-block:: bash

   uv run --project ~/Development/vgi-python python scripts/regen_generated.py
"""

from __future__ import annotations

import argparse
import io
import sys
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from vgi_rpc.rpc._types import MethodType, rpc_methods  # type: ignore[attr-defined]

from vgi.codegen._common import GeneratorError, _resolve_inner_schema, provenance_comment
from vgi.codegen.csharp_types import build_model
from vgi.protocol import VgiProtocol

if TYPE_CHECKING:
    from typing import TextIO


GENERATOR_VERSION = "1"

DEFAULT_NAMESPACE = "QueryFarm.Vgi.Tests.Generated"

_SCALARS: list[tuple[pa.DataType, str]] = [
    (pa.bool_(), "BooleanType.Default"),
    (pa.int8(), "Int8Type.Default"),
    (pa.int16(), "Int16Type.Default"),
    (pa.int32(), "Int32Type.Default"),
    (pa.int64(), "Int64Type.Default"),
    (pa.uint8(), "UInt8Type.Default"),
    (pa.uint16(), "UInt16Type.Default"),
    (pa.uint32(), "UInt32Type.Default"),
    (pa.uint64(), "UInt64Type.Default"),
    (pa.float32(), "FloatType.Default"),
    (pa.float64(), "DoubleType.Default"),
    (pa.string(), "StringType.Default"),
    (pa.large_string(), "LargeStringType.Default"),
    (pa.binary(), "BinaryType.Default"),
    (pa.large_binary(), "LargeBinaryType.Default"),
]

_UNITS = {"s": "Second", "ms": "Millisecond", "us": "Microsecond", "ns": "Nanosecond"}


def _type(dtype: pa.DataType, origin: str, indent: str) -> str:
    for proto, expr in _SCALARS:
        if dtype.equals(proto):
            return expr
    if pa.types.is_dictionary(dtype):
        index = _type(dtype.index_type, f"{origin}[dict index]", indent)
        value = _type(dtype.value_type, f"{origin}[dict value]", indent)
        return f"new DictionaryType({index}, {value}, ordered: {str(dtype.ordered).lower()})"
    if pa.types.is_fixed_size_binary(dtype):
        return f"new FixedSizeBinaryType({dtype.byte_width})"
    if pa.types.is_timestamp(dtype):
        tz = "(string?)null" if dtype.tz is None else f'"{dtype.tz}"'
        return f"new TimestampType(TimeUnit.{_UNITS[dtype.unit]}, {tz})"
    if pa.types.is_list(dtype):
        return f"new ListType({_field(dtype.value_field, origin, indent)})"
    if pa.types.is_map(dtype):
        key = _field(dtype.key_field.with_name("key"), origin, indent)
        value = _field(dtype.item_field.with_name("value"), origin, indent)
        return f"new MapType({key}, {value})"
    if pa.types.is_struct(dtype):
        inner = indent + "    "
        children = ",\n".join(inner + _field(dtype.field(i), origin, inner) for i in range(dtype.num_fields))
        return f"new StructType(\n{indent}[\n{children},\n{indent}])"
    raise GeneratorError(f"vgi.codegen.csharp_schemas: unsupported Arrow type {dtype} at {origin}.")


def _field(f: pa.Field[Any], origin: str, indent: str) -> str:
    return f'F("{f.name}", {_type(f.type, f"{origin}.{f.name}", indent)}, {str(f.nullable).lower()})'


def _schema(schema: pa.Schema, origin: str) -> str:
    if len(schema) == 0:
        return "S()"
    indent = "            "
    fields = ",\n".join(indent + _field(f, origin, indent) for f in schema)
    return f"S(\n{fields})"


def _map(name: str, doc: str, entries: list[tuple[str, pa.Schema]]) -> str:
    lines = [
        f"    /// <summary>{doc}</summary>",
        f"    public static IReadOnlyDictionary<string, Schema> {name} {{ get; }} = new Dictionary<string, Schema>",
        "    {",
    ]
    for key, schema in entries:
        lines.append(f'        ["{key}"] = {_schema(schema, key)},')
    lines.append("    };")
    return "\n".join(lines) + "\n"


def emit(out: TextIO, *, namespace: str = DEFAULT_NAMESPACE) -> None:
    """Emit the generated C# schema class to *out*."""
    model = build_model()
    records = [(name, model.records[name].schema) for name in sorted(model.records)]

    methods = rpc_methods(VgiProtocol)
    results: list[tuple[str, pa.Schema]] = []
    params: list[tuple[str, pa.Schema]] = []
    for method_name in sorted(methods):
        info = methods[method_name]
        params.append((method_name, info.params_schema))
        if info.method_type == MethodType.UNARY and info.has_return:
            inner = _resolve_inner_schema(info.result_type, method_name)
            if inner is not None:
                results.append((method_name, inner))

    body = io.StringIO()
    body.write("// Copyright 2025, 2026 Query Farm LLC - https://query.farm\n")
    body.write("// <auto-generated/>\n")
    body.write("\n")
    body.write("#nullable enable\n")
    body.write("\n")
    body.write("using Apache.Arrow;\n")
    body.write("using Apache.Arrow.Types;\n")
    body.write("\n")
    body.write(f"namespace {namespace};\n")
    body.write("\n")
    body.write(
        "/// <summary>\n"
        "/// The VGI protocol's Arrow schemas, generated from the protocol itself (vgi-python) rather\n"
        "/// than derived from this port's CLR types, so a test can compare the two. Dictionary ids and\n"
        "/// metadata are not part of the comparison.\n"
        "/// </summary>\n"
    )
    body.write("public static class VgiProtocolSchemas\n{\n")
    body.write(
        "    private static Schema S(params Field[] fields) => new(fields, metadata: null);\n\n"
        "    private static Field F(string name, IArrowType type, bool nullable) => new(name, type, nullable);\n\n"
    )
    body.write(_map("Records", "Every protocol record's schema, by its C# type name.", records))
    body.write("\n")
    body.write(
        _map(
            "MethodResults",
            "Each unary method's result record schema, by wire method name. Methods that return "
            "nothing or raw IPC are absent.",
            results,
        )
    )
    body.write("\n")
    body.write(_map("MethodParams", "Each method's params batch schema, by wire method name.", params))
    body.write("}\n")

    out.write(
        provenance_comment(
            generator_module="vgi.codegen.csharp_schemas",
            generator_command="python -m vgi.codegen.csharp_schemas",
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
    """Console-script entrypoint — write the C# schema class to stdout."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE, help="C# namespace of the emitted class")
    args = parser.parse_args()
    try:
        emit(sys.stdout, namespace=args.namespace)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
