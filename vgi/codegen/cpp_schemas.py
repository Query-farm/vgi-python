"""Emit Arrow schema factories for the VGI C++ DuckDB extension.

The C++ extension validates every worker RPC response against a registry of
Arrow schemas. Those schemas must match the vgi-python `ArrowSerializableDataclass`
definitions bit-for-bit. Hand-porting them is error-prone (the `data_version_spec`
nullability bug caught in vgi commit `11f9f42` is one example).

This generator emits a single `.hpp` file with `inline` Meyers-singleton
factories, one per unique schema. It reads:

- `vgi_rpc.rpc_methods(VgiProtocol)` for per-method unary response schemas.
- An explicit list of info-object dataclasses for the inner `List<Binary>`
  item schemas (those types are erased on the method signature).

### Multirepo workflow

`vgi-python` and `vgi` are separate git repos. When a Protocol change lands:

1. Modify the dataclass in `vgi-python`.
2. Run `uv run --project ~/Development/vgi-python vgi-gen-cpp-schemas \
       > ~/Development/vgi/src/generated/vgi_protocol_schemas.hpp`.
3. Commit the regenerated file in the `vgi` repo on the same branch.

`tests/test_generated_cpp_schemas.py` in vgi-python enforces that the
checked-in `.hpp` matches what the generator would emit fresh.

### Adding a new info type

Append the dataclass to `INFO_TYPES`. A safety-net assertion fails if any
transitive subclass of `CatalogObject` is missing from the list.
"""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa

if TYPE_CHECKING:
    from typing import TextIO

from vgi.catalog.catalog_interface import (
    CatalogInfo,
    CatalogObject,
    FunctionInfo,
    IndexInfo,
    MacroInfo,
    ScanFunctionResult,
    SchemaInfo,
    TableInfo,
    ViewInfo,
)
from vgi.protocol import VgiProtocol
from vgi_rpc.rpc._types import MethodType, rpc_methods  # type: ignore[attr-defined]

# Info-object dataclasses that appear as inner items inside `{items: List<Binary>}`
# responses. The wire layer erases them to `binary`, so the method signature
# doesn't reference them — we have to enumerate explicitly.
INFO_TYPES: tuple[type, ...] = (
    CatalogInfo,
    SchemaInfo,
    TableInfo,
    ViewInfo,
    FunctionInfo,
    MacroInfo,
    IndexInfo,
)

# Extra dataclasses whose ARROW_SCHEMA is referenced by methods whose return
# type is `bytes` (raw IPC). The generator can't infer these from method
# signatures alone; enumerate explicitly so the C++ registry can validate the
# deserialized inner batch even when the method's `result_type` is `bytes`.
EXTRA_RESPONSE_TYPES: tuple[type, ...] = (
    ScanFunctionResult,  # catalog_table_{scan,insert,update,delete}_function_get
)


@dataclass(frozen=True)
class EmittedSchema:
    """A named Arrow schema emission target."""

    name: str  # C++ factory function name stem (without trailing "Schema")
    schema: pa.Schema
    origin: str  # e.g. "method 'catalog_catalogs' result" (for diagnostics)


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


class _GeneratorError(RuntimeError):
    """Raised when an Arrow feature used by vgi-python isn't supported by the emitter."""


def _emit_type(dtype: pa.DataType, *, origin: str) -> str:
    """Render a pyarrow DataType as the C++ expression that constructs an equivalent arrow::DataType.

    Args:
        dtype: The pyarrow type to emit.
        origin: A short breadcrumb (e.g. ``"CatalogInfo.tags"``) used in error messages.

    Raises:
        _GeneratorError: if the type isn't in the whitelisted set. The message names the offending
            type, its `repr`, and the exact fix to apply in this file.
    """
    # Scalar types by identity lookup.
    for proto, expr in _SCALAR_MAP.items():
        if dtype.equals(proto):
            return expr

    # List<T>: pa.list_(T) or pa.list_(pa.field("name", T, nullable)).
    if pa.types.is_list(dtype):
        value_field = dtype.value_field
        inner_type = _emit_type(value_field.type, origin=f"{origin}[list item]")
        if value_field.name == "item" and value_field.nullable:
            # Arrow C++ default matches exactly.
            return f"arrow::list({inner_type})"
        return (
            "arrow::list("
            f'arrow::field("{value_field.name}", {inner_type}, '
            f"/*nullable=*/{'true' if value_field.nullable else 'false'}))"
        )

    # Map<K, V>: pa.map_(K, V) uses the canonical entries/key/value child names.
    if pa.types.is_map(dtype):
        key_field = dtype.key_field
        item_field = dtype.item_field
        # Arrow C++ arrow::map(k, v) produces field name "entries", child fields "key" (non-null),
        # "value" (nullable by default). If pyarrow diverges, we'd emit the wrong shape.
        if (
            key_field.name != "key"
            or item_field.name != "value"
            or key_field.nullable is not False
            or not _uses_default_map_field_name(dtype)
        ):
            raise _GeneratorError(
                f"Map at {origin} uses non-default child field names "
                f"(key='{key_field.name}' nullable={key_field.nullable}, "
                f"item='{item_field.name}'). "
                "arrow::map(k, v) only produces entries/key/value with key non-null; "
                "add explicit MapType construction to _emit_type() if this is needed."
            )
        key_type = _emit_type(dtype.key_type, origin=f"{origin}[map key]")
        item_type = _emit_type(dtype.item_type, origin=f"{origin}[map value]")
        # Note: arrow::map() defaults item nullable; confirm pyarrow matches.
        if item_field.nullable is not True:
            raise _GeneratorError(
                f"Map at {origin} has a non-nullable value field; arrow::map default is nullable. "
                "Explicit MapType construction would be needed."
            )
        return f"arrow::map({key_type}, {item_type})"

    # Dictionary<index, value, ordered>.
    if pa.types.is_dictionary(dtype):
        index_type = _emit_type(dtype.index_type, origin=f"{origin}[dict index]")
        value_type = _emit_type(dtype.value_type, origin=f"{origin}[dict value]")
        ordered = "true" if dtype.ordered else "false"
        # Match the convention in the hand-written registry (ordered defaults false,
        # emit explicitly only when true).
        if dtype.ordered:
            return f"arrow::dictionary({index_type}, {value_type}, /*ordered=*/{ordered})"
        return f"arrow::dictionary({index_type}, {value_type})"

    # Struct<field, ...>.
    if pa.types.is_struct(dtype):
        child_exprs = [
            _emit_field(dtype.field(i), origin=f"{origin}[struct child {i}]")
            for i in range(dtype.num_fields)
        ]
        return "arrow::struct_({" + ", ".join(child_exprs) + "})"

    raise _GeneratorError(
        f"vgi.codegen.cpp_schemas: unsupported Arrow type {type(dtype).__name__!r} at {origin} "
        f"(type={dtype!r}).\n"
        "To support this type, add a case to _emit_type() in vgi/codegen/cpp_schemas.py."
    )


def _uses_default_map_field_name(dtype: pa.MapType[Any, Any, Any]) -> bool:
    """pyarrow's Map stores its child list-field name ('entries' by default) accessible via .field.

    We can't trivially introspect it without constructing a schema, so check by equality
    against a freshly-built default map of the same key/item types.
    """
    canonical = pa.map_(dtype.key_type, dtype.item_type)
    return canonical.equals(dtype)


def _emit_field(field: pa.Field[Any], *, origin: str) -> str:
    """Render a pyarrow Field as an ``arrow::field(name, type, nullable)`` expression."""
    type_expr = _emit_type(field.type, origin=f"{origin}[{field.name}]")
    nullable = "true" if field.nullable else "false"
    return f'arrow::field("{field.name}", {type_expr}, /*nullable=*/{nullable})'


# --------------------------------------------------------------------------- #
# Schema / factory emitter
# --------------------------------------------------------------------------- #


def _sanitize_cpp_name(name: str) -> str:
    """Convert a method/class name into a valid, capitalized C++ factory stem."""
    # Preserve existing CamelCase (from class names); CamelCase snake_case method names.
    if "_" in name:
        return "".join(part.capitalize() for part in name.split("_"))
    # Already CamelCase-ish.
    return name[:1].upper() + name[1:]


def _emit_factory(es: EmittedSchema) -> str:
    """Render a single factory function definition."""
    if len(es.schema) == 0:
        fields_block = "    // empty struct ack — zero fields"
    else:
        lines: list[str] = []
        for f in es.schema:
            lines.append("    " + _emit_field(f, origin=f"{es.name}.{f.name}"))
        fields_block = ",\n".join(lines)
    body = (
        f"// Origin: {es.origin}\n"
        f"inline const std::shared_ptr<arrow::Schema> &{es.name}Schema() {{\n"
    )
    if len(es.schema) == 0:
        body += "\tstatic const auto schema = arrow::schema({});\n"
    else:
        body += "\tstatic const auto schema = arrow::schema({\n"
        body += "\t" + fields_block.replace("\n", "\n\t") + ",\n"
        body += "\t});\n"
    body += "\treturn schema;\n"
    body += "}\n"
    return body


# --------------------------------------------------------------------------- #
# Collection: walk VgiProtocol + INFO_TYPES into EmittedSchema records
# --------------------------------------------------------------------------- #


def _collect_schemas() -> list[EmittedSchema]:
    """Walk the Protocol + info types and return schemas to emit, deduped by name."""
    out: list[EmittedSchema] = []
    seen_names: set[str] = set()

    # Safety net: every subclass of CatalogObject that has its own ARROW_SCHEMA
    # (i.e., a serializable leaf type) must be in INFO_TYPES. Intermediate ABCs
    # like CatalogSchemaObject are skipped automatically.
    declared = set(INFO_TYPES)
    missing = {
        c for c in _all_info_subclasses()
        if hasattr(c, "ARROW_SCHEMA") and isinstance(getattr(c, "ARROW_SCHEMA", None), pa.Schema)
    } - declared
    if missing:
        names = sorted(c.__name__ for c in missing)
        raise _GeneratorError(
            f"Info dataclasses {names} inherit from CatalogObject and have ARROW_SCHEMA "
            "but are not in INFO_TYPES in vgi/codegen/cpp_schemas.py. Add them to "
            "keep inner-item validation in sync."
        )

    # 1) Inner info-type schemas and extra standalone dataclasses.
    for cls in (*INFO_TYPES, *EXTRA_RESPONSE_TYPES):
        name = _sanitize_cpp_name(cls.__name__)  # already CamelCase
        if name in seen_names:
            continue
        seen_names.add(name)
        schema = getattr(cls, "ARROW_SCHEMA")  # noqa: B009 — classvar access
        out.append(EmittedSchema(name=name, schema=schema, origin=f"{cls.__name__}"))

    # 2) Per-method unary response schemas. `result_schema` on RpcMethodInfo is
    # the OUTER envelope ({result: binary?}) — what we actually want is the
    # INNER dataclass schema that the binary decodes to, because that's what
    # the C++ extension validates after ExtractAndDeserializeResult.
    methods = rpc_methods(VgiProtocol)
    for method_name in sorted(methods.keys()):
        info = methods[method_name]
        if info.method_type != MethodType.UNARY:
            continue
        if not info.has_return:
            continue  # void responses aren't generated as schemas
        inner_schema = _resolve_inner_schema(info.result_type, method_name)
        if inner_schema is None:
            continue  # raw-bytes or opt-out return types (dynamic in the registry)
        name = _sanitize_cpp_name(method_name) + "Result"
        if name in seen_names:
            continue
        seen_names.add(name)
        out.append(
            EmittedSchema(
                name=name,
                schema=inner_schema,
                origin=f"method '{method_name}' result",
            )
        )

    return out


def _resolve_inner_schema(result_type: object, method_name: str) -> pa.Schema | None:
    """Translate a method's return-type annotation to its inner Arrow schema.

    Returns None when the method's return carries raw IPC bytes or ``None``
    (both map to ``dynamic`` in the C++ registry and don't need a factory).
    """
    import types
    import typing

    # Direct class reference: most common case.
    if isinstance(result_type, type):
        arrow_schema = getattr(result_type, "ARROW_SCHEMA", None)
        if isinstance(arrow_schema, pa.Schema):
            return arrow_schema
        # Return types that don't have a dataclass schema (void / raw bytes)
        # are registered as dynamic in the C++ registry; no factory needed.
        if result_type is type(None) or result_type is bytes:
            return None
        raise _GeneratorError(
            f"Method '{method_name}' returns {result_type!r}, which isn't an "
            "ArrowSerializableDataclass or raw bytes. Extend _resolve_inner_schema().",
        )

    # Unions / Optional: e.g. `bytes | None`, `SchemaInfo | None`.
    origin = typing.get_origin(result_type)
    if origin is typing.Union or origin is types.UnionType:
        args = [a for a in typing.get_args(result_type) if a is not type(None)]
        if len(args) == 1:
            return _resolve_inner_schema(args[0], method_name)
        raise _GeneratorError(
            f"Method '{method_name}' returns a non-Optional union {result_type!r}; unsupported."
        )

    raise _GeneratorError(
        f"Method '{method_name}' has unrecognized result_type {result_type!r}. "
        "Extend _resolve_inner_schema() in vgi/codegen/cpp_schemas.py."
    )


def _all_info_subclasses() -> set[type]:
    """Transitive subclasses of CatalogObject, excluding CatalogObject itself."""
    result: set[type] = set()
    stack = [CatalogObject]
    while stack:
        cls = stack.pop()
        for sub in cls.__subclasses__():
            if sub not in result:
                result.add(sub)
                stack.append(sub)
    return result


# --------------------------------------------------------------------------- #
# File emission
# --------------------------------------------------------------------------- #


def _vgi_python_sha() -> str:
    """Best-effort git SHA for the vgi-python checkout, or 'unknown' off-tree."""
    root = Path(__file__).resolve().parents[2]
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, stderr=subprocess.DEVNULL, text=True,
        ).strip()
        return sha or "unknown"
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


GENERATOR_VERSION = "1"


def emit(out: "TextIO") -> None:
    """Write the full generated .hpp to `out`."""
    schemas = _collect_schemas()
    sha = _vgi_python_sha()

    out.write("// ============================================================================\n")
    out.write("// GENERATED by vgi.codegen.cpp_schemas. DO NOT EDIT BY HAND.\n")
    out.write("//\n")
    out.write(f"// Generator: vgi-gen-cpp-schemas v{GENERATOR_VERSION}\n")
    out.write(f"// vgi-python SHA: {sha}\n")
    out.write("//\n")
    out.write("// To regenerate:\n")
    out.write("//   uv run --project ~/Development/vgi-python vgi-gen-cpp-schemas \\\n")
    out.write("//     > ~/Development/vgi/src/generated/vgi_protocol_schemas.hpp\n")
    out.write("// ============================================================================\n")
    out.write("\n")
    out.write("#pragma once\n\n")
    out.write("#include <arrow/api.h>\n")
    out.write("#include <memory>\n\n")
    out.write("namespace duckdb {\n")
    out.write("namespace vgi {\n")
    out.write("namespace generated {\n\n")

    for es in schemas:
        out.write(_emit_factory(es))
        out.write("\n")

    out.write("} // namespace generated\n")
    out.write("} // namespace vgi\n")
    out.write("} // namespace duckdb\n")


def main() -> None:
    """Entry point for the `vgi-gen-cpp-schemas` console script."""
    try:
        emit(sys.stdout)
    except _GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
