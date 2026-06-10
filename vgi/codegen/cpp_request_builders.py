# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit RecordBatch-builders for VGI RPC request schemas as a C++ header.

Companion to ``cpp_schemas`` (which emits the schema-factory header). This
generator emits one ``Build<Name>Params(...)`` function per ``params`` schema
collected from VgiProtocol — the Arrow record-batch builders the C++ extension
uses to construct outgoing RPC request batches.

Why a generator: hand-coded ``Build*Params`` functions in
``vgi/src/vgi_rpc_types.cpp`` and the schema declarations in
``vgi_protocol_schemas.hpp`` drifted three times in one bug-hunt session
(``streaming_partitioned``, ``tags`` map missing on schema_create, swapped
field order on column_add/column_drop). The runtime validator caught each,
but only after a user query exercised the RPC. Generating the builders
from the same protocol source the schemas come from collapses that drift
class — both sides regenerate together or the build breaks.

The generated header is a single TU's worth of ``inline`` functions in
``duckdb::vgi::generated`` — only ``vgi_rpc_types.cpp`` includes it. The
hand-coded Complex bucket (``BuildTableCreateRequest`` etc., which carry
``list<list<int32>>``, ``list<struct<...>>``, or hand-rolled invariants)
stays in the .cpp — the generator emits a comment for those methods and
the caller of ``Build<Name>Params`` for the Complex method picks up the
hand-coded definition from the same header chain.

### Multirepo workflow

Same as ``cpp_schemas``: regenerate after a Protocol change::

    uv run --project ~/Development/vgi-python vgi-gen-cpp-request-builders \\
        > ~/Development/vgi/src/generated/vgi_request_builders.hpp

``tests/test_generated_cpp_request_builders.py`` enforces drift.
"""

from __future__ import annotations

import io
import sys
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from vgi.codegen._common import (
    EmittedSchema,
    GeneratorError,
    collect_schemas,
    provenance_comment,
)

if TYPE_CHECKING:
    from typing import TextIO


GENERATOR_VERSION = "1"


# Methods whose ``params`` field set is hand-coded in vgi_rpc_types.cpp because
# the generator can't (yet) emit list-of-list / list-of-struct / cross-field
# invariants. The generator emits a comment for these so the file documents
# what's still hand-coded; the hand-coded definition lives in vgi_rpc_types.cpp.
#
# Currently empty — earlier the set held ``catalog_table_create``, but its
# outer params schema is just ``{request: binary}`` (the inner
# TableCreateRequest with list<list<int32>> / list<binary> shapes is
# IPC-serialized into the binary field by the caller), so there's nothing
# Complex about the outer wrapper. The inner request body remains hand-coded
# in BuildTableCreateRequest.
COMPLEX_METHODS: frozenset[str] = frozenset()


# --------------------------------------------------------------------------- #
# Field-type classification + C++ rendering
# --------------------------------------------------------------------------- #


def _is_simple_scalar(dtype: pa.DataType) -> bool:
    """Scalars the generator can render as a single ``Build*Scalar`` call."""
    return (
        dtype.equals(pa.binary())
        or dtype.equals(pa.string())
        or dtype.equals(pa.bool_())
        or dtype.equals(pa.int8())
        or dtype.equals(pa.int16())
        or dtype.equals(pa.int32())
        or dtype.equals(pa.int64())
        or pa.types.is_dictionary(dtype)
    )


def _is_simple_list(dtype: pa.DataType) -> bool:
    """list<simple-scalar> — generator emits a small inline loop."""
    if not pa.types.is_list(dtype):
        return False
    inner = dtype.value_type
    return bool(
        inner.equals(pa.binary()) or inner.equals(pa.string()) or inner.equals(pa.int32()) or inner.equals(pa.int64())
    )


def _is_simple_map(dtype: pa.DataType) -> bool:
    """map<utf8, utf8> — generator emits a MapBuilder loop."""
    return pa.types.is_map(dtype) and dtype.key_type.equals(pa.utf8()) and dtype.item_type.equals(pa.utf8())


def _is_supported_field(field: pa.Field[Any]) -> bool:
    return _is_simple_scalar(field.type) or _is_simple_list(field.type) or _is_simple_map(field.type)


def _cpp_param_type(field: pa.Field[Any]) -> str:
    """C++ parameter type for a schema field (with std::optional<T> for nullable)."""
    base: str
    t = field.type
    if t.equals(pa.binary()):
        base = "std::vector<uint8_t>"
    elif t.equals(pa.string()):
        base = "std::string"
    elif t.equals(pa.bool_()):
        base = "bool"
    elif t.equals(pa.int8()):
        base = "int8_t"
    elif t.equals(pa.int16()):
        base = "int16_t"
    elif t.equals(pa.int32()):
        base = "int32_t"
    elif t.equals(pa.int64()):
        base = "int64_t"
    elif pa.types.is_dictionary(t):
        # Dictionary<int16, utf8> on the wire; caller passes the string label.
        base = "std::string"
    elif _is_simple_list(t):
        inner = t.value_type
        if inner.equals(pa.binary()):
            base = "std::vector<std::vector<uint8_t>>"
        elif inner.equals(pa.string()):
            base = "std::vector<std::string>"
        elif inner.equals(pa.int32()):
            base = "std::vector<int32_t>"
        elif inner.equals(pa.int64()):
            base = "std::vector<int64_t>"
        else:
            raise GeneratorError(f"unhandled list<{inner}> for field '{field.name}'")
    elif _is_simple_map(t):
        base = "std::vector<std::pair<std::string, std::string>>"
    else:
        raise GeneratorError(f"unsupported field type {t!r} for '{field.name}'")

    if field.nullable:
        # bool/int scalars use std::optional<T> by value; everything else by const-ref.
        if base in {"bool", "int8_t", "int16_t", "int32_t", "int64_t"}:
            return f"std::optional<{base}>"
        return f"const std::optional<{base}>&"
    if base in {"bool", "int8_t", "int16_t", "int32_t", "int64_t"}:
        return base
    return f"const {base}&"


def _build_enum_table() -> dict[tuple[str, str], list[str]]:
    """Map (method_name, field_name) → enum member-name list.

    Walks ``VgiProtocol.<method>`` signatures looking for parameters whose
    annotation is an ``enum.Enum`` subclass; the wire convention encodes the
    enum's ``__members__`` keys (uppercase) into the dictionary array. Pulled
    from inspection so the generator stays in sync if a new enum-typed field
    is added on either side.
    """
    import enum
    import inspect

    from vgi.protocol import VgiProtocol

    table: dict[tuple[str, str], list[str]] = {}
    for name, member in inspect.getmembers(VgiProtocol, predicate=inspect.isfunction):
        if name.startswith("_"):
            continue
        try:
            sig = inspect.signature(member, eval_str=True)
        except (TypeError, NameError):
            continue
        for pname, p in sig.parameters.items():
            ann = p.annotation
            if isinstance(ann, type) and issubclass(ann, enum.Enum):
                table[(name, pname)] = [m.name for m in ann]
    return table


# Lazy-evaluate so import remains side-effect-free.
_ENUM_VALUES_CACHE: dict[tuple[str, str], list[str]] | None = None


def _enum_values_for(method_name: str, field_name: str) -> list[str] | None:
    global _ENUM_VALUES_CACHE
    if _ENUM_VALUES_CACHE is None:
        _ENUM_VALUES_CACHE = _build_enum_table()
    return _ENUM_VALUES_CACHE.get((method_name, field_name))


def _build_array_call(field: pa.Field[Any], param: str, *, method_name: str) -> str:
    """Emit a single statement producing ``arrays.push_back(<expr>)`` for one field."""
    t = field.type
    nullable = field.nullable

    if t.equals(pa.binary()):
        if nullable:
            return f"arrays.push_back(BuildOptionalBinaryScalar({param}));"
        return f"arrays.push_back(BuildBinaryScalarRequired({param}));"

    if t.equals(pa.string()):
        if nullable:
            return f"arrays.push_back(BuildOptionalStringScalar({param}));"
        return f"arrays.push_back(BuildStringScalar({param}));"

    if t.equals(pa.bool_()):
        if nullable:
            return f"arrays.push_back(BuildOptionalBoolScalar({param}));"
        return f"arrays.push_back(BuildBoolScalar({param}));"

    if t.equals(pa.int32()):
        if nullable:
            return f"arrays.push_back(BuildOptionalInt32Scalar({param}));"
        return f"arrays.push_back(BuildInt32Scalar({param}));"

    if t.equals(pa.int64()):
        if nullable:
            return f"arrays.push_back(BuildOptionalInt64Scalar({param}));"
        return f"arrays.push_back(BuildInt64Scalar({param}));"

    if pa.types.is_dictionary(t):
        values = _enum_values_for(method_name, field.name)
        if values is None:
            raise GeneratorError(
                f"dictionary field '{field.name}' on method '{method_name}' has no "
                f"matching Enum-typed parameter on VgiProtocol.{method_name}. The "
                f"generator pulls dict values from method signatures via "
                f"inspect.signature; add an enum-typed annotation, or extend "
                f"_build_enum_table() if there's a different mapping."
            )
        # Inline the dictionary values literally — same shape every send.
        values_init = ", ".join(f'"{v}"' for v in values)
        if nullable:
            return f"arrays.push_back(BuildOptionalEnumArray({param}, {{{values_init}}}));"
        return f"arrays.push_back(BuildEnumArray({param}, {{{values_init}}}));"

    if _is_simple_list(t):
        inner = t.value_type
        if inner.equals(pa.string()):
            return f"arrays.push_back(BuildStringListScalar({param}));"
        if inner.equals(pa.binary()):
            return f"arrays.push_back(BuildBinaryListScalar({param}));"
        if inner.equals(pa.int32()):
            return f"arrays.push_back(BuildInt32ListScalar({param}));"
        if inner.equals(pa.int64()):
            return f"arrays.push_back(BuildInt64ListScalar({param}));"
        raise GeneratorError(f"unhandled list<{inner}> for '{field.name}'")

    if _is_simple_map(t):
        if nullable:
            return f"arrays.push_back(BuildOptionalStringMapScalar({param}));"
        return f"arrays.push_back(BuildStringMapScalar({param}));"

    raise GeneratorError(f"unhandled field type {t!r} for '{field.name}'")


def _emit_builder(es: EmittedSchema, *, method_name: str) -> str:
    """Render one ``Build<Name>Params(...)`` function as an inline C++ definition."""
    schema = es.schema
    name = es.name  # e.g. ``CatalogSchemaCreateParams``
    func_name = f"Build{name}"

    # All fields supported?
    unsupported = [f for f in schema if not _is_supported_field(f)]
    if unsupported:
        kinds = ", ".join(f"{f.name}: {f.type}" for f in unsupported)
        return (
            f"// SKIPPED: {func_name} — fields not in generator's supported set: {kinds}\n"
            f"// Hand-coded in vgi_rpc_types.cpp.\n\n"
        )

    # Param signatures — one per field, in schema order.
    params_decl: list[str] = []
    for f in schema:
        cpp_type = _cpp_param_type(f)
        params_decl.append(f"\t    {cpp_type} {f.name}")
    params_str = ",\n".join(params_decl)

    # Body — emit one push_back per field.
    pushes: list[str] = []
    for f in schema:
        pushes.append(f"\t{_build_array_call(f, f.name, method_name=method_name)}")
    body = "\n".join(pushes)

    out = io.StringIO()
    out.write(f"// Origin: {es.origin}\n")
    out.write(f"inline std::shared_ptr<arrow::RecordBatch> {func_name}(\n")
    out.write(params_str)
    out.write(") {\n")
    out.write(f"\tconst auto &schema = {name}Schema();\n")
    out.write('\tstatic_assert(true, "compile-time anchor — runtime field-count assert below");\n')
    out.write("\tstd::vector<std::shared_ptr<arrow::Array>> arrays;\n")
    out.write(f"\tarrays.reserve({len(schema)});\n")
    out.write(body + "\n")
    out.write("\tif (arrays.size() != static_cast<size_t>(schema->num_fields())) {\n")
    out.write(
        f'\t\tthrow IOException("vgi codegen drift: {func_name} produced " '
        f'+ std::to_string(arrays.size()) + " arrays but schema has " '
        f'+ std::to_string(schema->num_fields()) + " fields");\n'
    )
    out.write("\t}\n")
    out.write("\treturn arrow::RecordBatch::Make(schema, 1, arrays);\n")
    out.write("}\n")
    return out.getvalue()


# --------------------------------------------------------------------------- #
# Top-level emit
# --------------------------------------------------------------------------- #


def _params_method_name(es: EmittedSchema) -> str | None:
    """Recover the protocol method name from an EmittedSchema's origin string.

    ``origin`` for params is ``"method '<name>' params"``.
    """
    prefix = "method '"
    if not es.origin.startswith(prefix):
        return None
    rest = es.origin[len(prefix) :]
    if not rest.endswith("' params"):
        return None
    return rest[: -len("' params")]


def emit_builders(
    out: TextIO,
    schemas: list[EmittedSchema],
    *,
    generator_module: str,
    generator_command: str,
    regen_command_lines: list[str],
    schemas_include: str = "vgi_protocol_schemas.hpp",
) -> None:
    """Render request-builder functions for a set of params schemas as a C++ header.

    Shared by the main protocol generator and the secret protocol generator;
    only the schema set, the schema-factory include, and the provenance banner
    differ.
    """
    body = io.StringIO()
    body.write("#pragma once\n\n")
    body.write("// Generated builders depend on helpers + the schema factories.\n")
    body.write("// Both live behind the same public header.\n")
    body.write("// vgi_rpc_types.hpp goes through src/include; vgi_protocol_schemas.hpp is\n")
    body.write("// a sibling in src/generated/ and resolves via this quoted include's\n")
    body.write("// relative-path search.\n")
    body.write('#include "vgi_rpc_types.hpp"\n')
    body.write(f'#include "{schemas_include}"\n\n')
    body.write("#include <cstdint>\n")
    body.write("#include <optional>\n")
    body.write("#include <string>\n")
    body.write("#include <utility>\n")
    body.write("#include <vector>\n\n")
    body.write("#include <arrow/api.h>\n\n")
    body.write('#include "duckdb/common/exception.hpp"\n\n')
    body.write("namespace duckdb {\n")
    body.write("namespace vgi {\n")
    body.write("namespace generated {\n\n")

    skipped: list[str] = []
    emitted: list[str] = []

    for es in schemas:
        method_name = _params_method_name(es)
        if method_name is None:
            continue  # not a params schema (it's an info type or response)
        if method_name in COMPLEX_METHODS:
            body.write(
                f"// SKIPPED: Build{es.name} — method '{method_name}' is in COMPLEX_METHODS\n"
                f"//          (list<list<...>>, list<struct<...>>, or cross-field invariants).\n"
                f"//          Hand-coded in vgi_rpc_types.cpp.\n\n"
            )
            skipped.append(method_name)
            continue
        chunk = _emit_builder(es, method_name=method_name)
        body.write(chunk + "\n")
        if chunk.lstrip().startswith("// SKIPPED"):
            skipped.append(method_name)
        else:
            emitted.append(method_name)

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
    out.write("//\n")
    out.write(f"// Emitted: {len(emitted)} builders\n")
    out.write(f"// Skipped: {len(skipped)} (Complex bucket — see vgi_rpc_types.cpp)\n")
    if skipped:
        out.write("// Skipped methods:\n")
        for m in sorted(skipped):
            out.write(f"//   - {m}\n")
    out.write("//\n")
    out.write("// ============================================================================\n")
    out.write("\n")
    out.write(body.getvalue())


def emit(out: TextIO) -> None:
    """Emit the generated C++ request-builder header to *out*."""
    emit_builders(
        out,
        collect_schemas(),
        generator_module="vgi.codegen.cpp_request_builders",
        generator_command="vgi-gen-cpp-request-builders",
        regen_command_lines=[
            "uv run --project ~/Development/vgi-python vgi-gen-cpp-request-builders \\",
            "  > ~/Development/vgi/src/generated/vgi_request_builders.hpp",
        ],
        schemas_include="vgi_protocol_schemas.hpp",
    )


def main() -> None:
    """Console-script entrypoint — write the generated header to stdout."""
    try:
        emit(sys.stdout)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
