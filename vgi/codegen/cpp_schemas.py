# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Emit Arrow schema factories for the VGI C++ DuckDB extension.

See module docstring in `vgi.codegen._common` for the shared machinery.

### Multirepo workflow

`vgi-python` and `vgi` are separate git repos. When a Protocol change lands:

1. Modify the dataclass in `vgi-python`.
2. Run `uv run --project ~/Development/vgi-python vgi-gen-cpp-schemas \
       > ~/Development/vgi/src/generated/vgi_protocol_schemas.hpp`.
3. Commit the regenerated file in the `vgi` repo on the same branch.

`tests/test_generated_cpp_schemas.py` in vgi-python enforces that the
checked-in `.hpp` matches what the generator would emit right now.
"""

from __future__ import annotations

import argparse
import io
import sys
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa

from vgi.codegen._common import (
    DEFAULT_CPP_NAMESPACE,
    EXTRA_RESPONSE_TYPES,
    REQUEST_TYPES,
    EmittedSchema,
    GeneratorError,
    close_namespace,
    collect_schemas,
    open_namespace,
    parse_cpp_namespace,
    provenance_comment,
)

if TYPE_CHECKING:
    from typing import TextIO


# --------------------------------------------------------------------------- #
# Type emitter: pyarrow DataType -> C++ expression (arrow::...)
# --------------------------------------------------------------------------- #

_SCALAR_MAP: dict[Any, str] = {
    pa.null(): "arrow::null()",
    pa.bool_(): "arrow::boolean()",
    pa.int8(): "arrow::int8()",
    pa.int16(): "arrow::int16()",
    pa.int32(): "arrow::int32()",
    pa.int64(): "arrow::int64()",
    pa.uint8(): "arrow::uint8()",
    pa.uint16(): "arrow::uint16()",
    pa.uint32(): "arrow::uint32()",
    pa.uint64(): "arrow::uint64()",
    pa.float32(): "arrow::float32()",
    pa.float64(): "arrow::float64()",
    pa.string(): "arrow::utf8()",
    pa.binary(): "arrow::binary()",
    # The large_ variants are on the wire (InitRequest.pushdown_filters and the
    # join_keys / split_tokens list items are large_binary — a filter payload or
    # a set of join keys can exceed the 2 GiB that 32-bit offsets address).
    # Their absence here was invisible for as long as nothing generated a schema
    # containing one; adding the request records to the emitter surfaced it
    # immediately, as a hard GeneratorError rather than a silent narrowing.
    pa.large_string(): "arrow::large_utf8()",
    pa.large_binary(): "arrow::large_binary()",
}


def _emit_type(dtype: pa.DataType, *, origin: str) -> str:
    """Render a pyarrow DataType as the C++ expression that constructs an equivalent arrow::DataType."""
    for proto, expr in _SCALAR_MAP.items():
        if dtype.equals(proto):
            return expr

    if pa.types.is_large_list(dtype):
        value_field = dtype.value_field
        inner_type = _emit_type(value_field.type, origin=f"{origin}[large list item]")
        if value_field.name == "item" and value_field.nullable:
            return f"arrow::large_list({inner_type})"
        return (
            "arrow::large_list("
            f'arrow::field("{value_field.name}", {inner_type}, '
            f"/*nullable=*/{str(value_field.nullable).lower()}))"
        )

    if pa.types.is_list(dtype):
        value_field = dtype.value_field
        inner_type = _emit_type(value_field.type, origin=f"{origin}[list item]")
        if value_field.name == "item" and value_field.nullable:
            return f"arrow::list({inner_type})"
        return (
            "arrow::list("
            f'arrow::field("{value_field.name}", {inner_type}, '
            f"/*nullable=*/{'true' if value_field.nullable else 'false'}))"
        )

    if pa.types.is_map(dtype):
        key_field = dtype.key_field
        item_field = dtype.item_field
        if (
            key_field.name != "key"
            or item_field.name != "value"
            or key_field.nullable is not False
            or not _uses_default_map_field_name(dtype)
        ):
            raise GeneratorError(
                f"Map at {origin} uses non-default child field names "
                f"(key='{key_field.name}' nullable={key_field.nullable}, "
                f"item='{item_field.name}'). "
                "arrow::map(k, v) only produces entries/key/value with key non-null; "
                "add explicit MapType construction to _emit_type() if this is needed.",
            )
        key_type = _emit_type(dtype.key_type, origin=f"{origin}[map key]")
        item_type = _emit_type(dtype.item_type, origin=f"{origin}[map value]")
        if item_field.nullable is not True:
            raise GeneratorError(
                f"Map at {origin} has a non-nullable value field; arrow::map default is nullable. "
                "Explicit MapType construction would be needed.",
            )
        return f"arrow::map({key_type}, {item_type})"

    if pa.types.is_dictionary(dtype):
        index_type = _emit_type(dtype.index_type, origin=f"{origin}[dict index]")
        value_type = _emit_type(dtype.value_type, origin=f"{origin}[dict value]")
        ordered = "true" if dtype.ordered else "false"
        if dtype.ordered:
            return f"arrow::dictionary({index_type}, {value_type}, /*ordered=*/{ordered})"
        return f"arrow::dictionary({index_type}, {value_type})"

    if pa.types.is_struct(dtype):
        child_exprs = [
            _emit_field(dtype.field(i), origin=f"{origin}[struct child {i}]") for i in range(dtype.num_fields)
        ]
        return "arrow::struct_({" + ", ".join(child_exprs) + "})"

    if pa.types.is_timestamp(dtype):
        unit_map = {
            "s": "arrow::TimeUnit::SECOND",
            "ms": "arrow::TimeUnit::MILLI",
            "us": "arrow::TimeUnit::MICRO",
            "ns": "arrow::TimeUnit::NANO",
        }
        unit_expr = unit_map.get(dtype.unit)
        if unit_expr is None:
            raise GeneratorError(
                f"timestamp at {origin} has unknown unit {dtype.unit!r}; expected one of {sorted(unit_map)}.",
            )
        if dtype.tz is None:
            return f"arrow::timestamp({unit_expr})"
        # arrow::timestamp(unit, timezone) — timezone string is verbatim per the Arrow spec.
        return f'arrow::timestamp({unit_expr}, "{dtype.tz}")'

    raise GeneratorError(
        f"vgi.codegen.cpp_schemas: unsupported Arrow type {type(dtype).__name__!r} at {origin} "
        f"(type={dtype!r}).\n"
        "To support this type, add a case to _emit_type() in vgi/codegen/cpp_schemas.py.",
    )


def _uses_default_map_field_name(dtype: pa.MapType[Any, Any, Any]) -> bool:
    canonical = pa.map_(dtype.key_type, dtype.item_type)
    return canonical.equals(dtype)


def _emit_field(field: pa.Field[Any], *, origin: str) -> str:
    type_expr = _emit_type(field.type, origin=f"{origin}[{field.name}]")
    nullable = "true" if field.nullable else "false"
    return f'arrow::field("{field.name}", {type_expr}, /*nullable=*/{nullable})'


# --------------------------------------------------------------------------- #
# nanoarrow backend: pyarrow DataType -> vgi::type DSL (vgi_arrow.hpp)
# --------------------------------------------------------------------------- #

_NA_SCALAR_MAP: dict[Any, str] = {
    pa.null(): "type::Null()",
    pa.bool_(): "type::Bool()",
    pa.int8(): "type::Int8()",
    pa.int16(): "type::Int16()",
    pa.int32(): "type::Int32()",
    pa.int64(): "type::Int64()",
    pa.uint8(): "type::UInt8()",
    pa.uint16(): "type::UInt16()",
    pa.uint32(): "type::UInt32()",
    pa.uint64(): "type::UInt64()",
    pa.float32(): "type::Float32()",
    pa.float64(): "type::Float64()",
    pa.string(): "type::Utf8()",
    pa.binary(): "type::Binary()",
    pa.large_string(): "type::LargeUtf8()",
    pa.large_binary(): "type::LargeBinary()",
}


def _na_emit_type(dtype: pa.DataType, *, origin: str) -> str:
    """Render a pyarrow DataType as a ``vgi::type`` expression (the nanoarrow backend)."""
    for proto, expr in _NA_SCALAR_MAP.items():
        if dtype.equals(proto):
            return expr
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        value_field = dtype.value_field
        if value_field.name != "item":
            raise GeneratorError(f"list at {origin} names its item {value_field.name!r}; only 'item' is supported")
        inner = _na_emit_type(value_field.type, origin=f"{origin}[list item]")
        ctor = "type::LargeList" if pa.types.is_large_list(dtype) else "type::List"
        nullable = "true" if value_field.nullable else "false"
        return f"{ctor}({inner}, /*item_nullable=*/{nullable})"
    if pa.types.is_map(dtype):
        key_field = dtype.key_field
        item_field = dtype.item_field
        if key_field.name != "key" or item_field.name != "value" or key_field.nullable is not False:
            raise GeneratorError(f"Map at {origin} uses non-default child fields")
        if not _uses_default_map_field_name(dtype):
            raise GeneratorError(f"Map at {origin} uses a non-default entries field")
        key = _na_emit_type(dtype.key_type, origin=f"{origin}[map key]")
        item = _na_emit_type(dtype.item_type, origin=f"{origin}[map value]")
        nullable = "true" if item_field.nullable else "false"
        return f"type::Map({key}, {item}, /*value_nullable=*/{nullable})"
    if pa.types.is_dictionary(dtype):
        if dtype.ordered:
            raise GeneratorError(f"ordered dictionary at {origin} is not supported")
        index = _na_emit_type(dtype.index_type, origin=f"{origin}[dict index]")
        value = _na_emit_type(dtype.value_type, origin=f"{origin}[dict value]")
        return f"type::Dictionary({index}, {value})"
    if pa.types.is_struct(dtype):
        children = [
            _na_emit_field(dtype.field(i), origin=f"{origin}[struct child {i}]") for i in range(dtype.num_fields)
        ]
        return "type::Struct({" + ", ".join(children) + "})"
    if pa.types.is_timestamp(dtype):
        units = {"s": "s", "ms": "m", "us": "u", "ns": "n"}
        if dtype.unit not in units:
            raise GeneratorError(f"timestamp at {origin} has unknown unit {dtype.unit!r}")
        tz = dtype.tz or ""
        return f"type::Timestamp('{units[dtype.unit]}', \"{tz}\")"
    raise GeneratorError(f"vgi.codegen.cpp_schemas (nanoarrow): unsupported Arrow type {dtype!r} at {origin}")


def _na_emit_field(field: pa.Field[Any], *, origin: str) -> str:
    """Render a pyarrow Field as a named ``vgi::VgiField`` expression."""
    if field.metadata:
        raise GeneratorError(f"field metadata at {origin} is not supported by the nanoarrow backend")
    type_expr = _na_emit_type(field.type, origin=f"{origin}[{field.name}]")
    nullable = "true" if field.nullable else "false"
    return f'{type_expr}.Named("{field.name}", /*nullable=*/{nullable})'


def _na_emit_factory(es: EmittedSchema) -> str:
    if es.schema.metadata:
        raise GeneratorError(f"schema metadata on {es.name} is not supported by the nanoarrow backend")
    body = f"// Origin: {es.origin}\ninline const VgiSchemaPtr &{es.name}Schema() {{\n"
    if len(es.schema) == 0:
        body += "\tstatic const auto schema = MakeSchema({});\n"
    else:
        body += "\tstatic const auto schema = MakeSchema({\n"
        lines = ["\t    " + _na_emit_field(f, origin=f"{es.name}.{f.name}") for f in es.schema]
        body += ",\n".join(lines) + ",\n"
        body += "\t});\n"
    body += "\treturn schema;\n}\n"
    return body


def _emit_factory(es: EmittedSchema) -> str:
    body = f"// Origin: {es.origin}\ninline const std::shared_ptr<arrow::Schema> &{es.name}Schema() {{\n"
    if len(es.schema) == 0:
        body += "\tstatic const auto schema = arrow::schema({});\n"
    else:
        body += "\tstatic const auto schema = arrow::schema({\n"
        lines = ["\t    " + _emit_field(f, origin=f"{es.name}.{f.name}") for f in es.schema]
        body += ",\n".join(lines) + ",\n"
        body += "\t});\n"
    body += "\treturn schema;\n"
    body += "}\n"
    return body


# Re-export for existing tests that referenced these directly.
_collect_schemas = collect_schemas
_GeneratorError = GeneratorError
_ = cast  # keep cast import used for older test compatibility


GENERATOR_VERSION = "1"


def emit_schemas(
    out: TextIO,
    schemas: list[EmittedSchema],
    *,
    generator_module: str,
    generator_command: str,
    regen_command_lines: list[str],
    namespace: list[str] | None = None,
    backend: str = "arrow",
) -> None:
    """Render a list of EmittedSchema records as a C++ schema-factory header.

    ``backend`` is ``"arrow"`` (Arrow C++ factories; vgi-c++, vgi-sqlite) or
    ``"nanoarrow"`` (``VgiSchemaPtr`` factories over ``vgi_arrow.hpp``; the DuckDB
    extension).

    Shared by the main protocol generator and the secret protocol generator;
    only the schema set and the provenance banner differ.
    """
    if namespace is None:
        namespace = parse_cpp_namespace(DEFAULT_CPP_NAMESPACE)
    body = io.StringIO()
    if backend not in ("arrow", "nanoarrow"):
        raise GeneratorError(f"unknown C++ schema backend {backend!r}")
    body.write("#pragma once\n\n")
    if backend == "arrow":
        body.write("#include <arrow/api.h>\n")
    else:
        body.write('#include "vgi_arrow.hpp"\n')
    body.write("#include <memory>\n\n")
    body.write(open_namespace(namespace))
    body.write("\n")
    if backend == "nanoarrow":
        body.write("using ::duckdb::vgi::MakeSchema;\n")
        body.write("using ::duckdb::vgi::VgiSchemaPtr;\n")
        body.write("namespace type = ::duckdb::vgi::type;\n\n")

    factory = _emit_factory if backend == "arrow" else _na_emit_factory
    for es in schemas:
        body.write(factory(es))
        body.write("\n")

    body.write(close_namespace(namespace))

    out.write("// ============================================================================\n")
    out.write(
        provenance_comment(
            generator_module=generator_module,
            generator_command=generator_command,
            generator_version=GENERATOR_VERSION,
            regen_command_lines=regen_command_lines,
            body=body.getvalue(),
        )
    )
    out.write("// ============================================================================\n")
    out.write("\n")
    out.write(body.getvalue())


def emit(out: TextIO, namespace: list[str] | None = None, record: str | None = None) -> None:
    """Emit the generated C++ schemas header to *out* (``record="nanoarrow"``: the nanoarrow backend)."""
    emit_schemas(
        out,
        # The request records are included so the C++ client can be CHECKED
        # against them: it hand-builds BindRequest / InitRequest /
        # CatalogAttachRequest in vgi_rpc_types.cpp, and until now nothing
        # compared those to the protocol. Two real defects reached production
        # that way.
        collect_schemas(extra_response_types=(*EXTRA_RESPONSE_TYPES, *REQUEST_TYPES)),
        generator_module="vgi.codegen.cpp_schemas",
        generator_command="vgi-gen-cpp-schemas",
        regen_command_lines=[
            "uv run --project ~/Development/vgi-python vgi-gen-cpp-schemas \\",
            "  > ~/Development/vgi/src/generated/vgi_protocol_schemas.hpp",
        ],
        namespace=namespace,
        backend=record or "arrow",
    )


def main() -> None:
    """Console-script entrypoint — write the C++ schemas header to stdout."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--namespace",
        default=DEFAULT_CPP_NAMESPACE,
        help=(
            "C++ namespace to emit into, `::`-separated "
            f"(default: {DEFAULT_CPP_NAMESPACE}). VGI is not DuckDB-only: a "
            "standalone worker SDK wants something like `vgi::generated`."
        ),
    )
    args = parser.parse_args()
    try:
        namespace = parse_cpp_namespace(args.namespace)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)
    try:
        emit(sys.stdout, namespace)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
