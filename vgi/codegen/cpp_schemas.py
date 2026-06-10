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

import io
import sys
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa

from vgi.codegen._common import (
    EmittedSchema,
    GeneratorError,
    collect_schemas,
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
}


def _emit_type(dtype: pa.DataType, *, origin: str) -> str:
    """Render a pyarrow DataType as the C++ expression that constructs an equivalent arrow::DataType."""
    for proto, expr in _SCALAR_MAP.items():
        if dtype.equals(proto):
            return expr

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
) -> None:
    """Render a list of EmittedSchema records as a C++ schema-factory header.

    Shared by the main protocol generator and the secret protocol generator;
    only the schema set and the provenance banner differ.
    """
    body = io.StringIO()
    body.write("#pragma once\n\n")
    body.write("#include <arrow/api.h>\n")
    body.write("#include <memory>\n\n")
    body.write("namespace duckdb {\n")
    body.write("namespace vgi {\n")
    body.write("namespace generated {\n\n")

    for es in schemas:
        body.write(_emit_factory(es))
        body.write("\n")

    body.write("} // namespace generated\n")
    body.write("} // namespace vgi\n")
    body.write("} // namespace duckdb\n")

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


def emit(out: TextIO) -> None:
    """Emit the generated C++ schemas header to *out*."""
    emit_schemas(
        out,
        collect_schemas(),
        generator_module="vgi.codegen.cpp_schemas",
        generator_command="vgi-gen-cpp-schemas",
        regen_command_lines=[
            "uv run --project ~/Development/vgi-python vgi-gen-cpp-schemas \\",
            "  > ~/Development/vgi/src/generated/vgi_protocol_schemas.hpp",
        ],
    )


def main() -> None:
    """Console-script entrypoint — write the C++ schemas header to stdout."""
    try:
        emit(sys.stdout)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
