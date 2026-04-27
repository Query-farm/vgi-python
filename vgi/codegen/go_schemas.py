"""Emit arrow-go Schema factories for the VGI Go worker.

Sister module to `vgi.codegen.cpp_schemas` and `vgi.codegen.ts_schemas`.
Same inputs (Protocol walk + explicit info-type list), different rendering —
emits a single `.go` file with one `var XxxSchema = arrow.NewSchema(...)`
per unique dataclass or per-method.

### Multirepo workflow

`vgi-python` and `vgi-go` are separate repos. Protocol changes flow:

1. Modify the dataclass in `vgi-python`.
2. Run:
   ```
   uv run --project ~/Development/vgi-python vgi-gen-go-schemas \
       > ~/Development/vgi-go/vgi/generated/protocol_schemas.go
   ```
3. Commit the regenerated file in the `vgi-go` repo on the same branch.

`tests/test_generated_go_schemas.py` in vgi-python enforces that the
checked-in `.go` matches what the generator would emit right now.

### arrow-go quirks

- `arrow.MapOf(keyType, valueType)` produces the canonical
  `map<entries: struct<key, value>>` form with `keys_sorted=false`,
  `key` non-null, `value` nullable. Maps that diverge from this raise
  `GeneratorError`.
- `arrow.ListOf(elemType)` produces a list with `item` nullable. For
  non-null items, `arrow.ListOfField(arrow.Field{Name: "item", Type: t})`.
- Booleans: `&arrow.BooleanType{}` (no module-level singleton in arrow-go v18).
- Dictionary: `&arrow.DictionaryType{IndexType: ..., ValueType: ..., Ordered: ...}`.
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


_SCALAR_MAP: dict[Any, str] = {
    pa.bool_(): "&arrow.BooleanType{}",
    pa.int8(): "arrow.PrimitiveTypes.Int8",
    pa.int16(): "arrow.PrimitiveTypes.Int16",
    pa.int32(): "arrow.PrimitiveTypes.Int32",
    pa.int64(): "arrow.PrimitiveTypes.Int64",
    pa.uint8(): "arrow.PrimitiveTypes.Uint8",
    pa.uint16(): "arrow.PrimitiveTypes.Uint16",
    pa.uint32(): "arrow.PrimitiveTypes.Uint32",
    pa.uint64(): "arrow.PrimitiveTypes.Uint64",
    pa.float32(): "arrow.PrimitiveTypes.Float32",
    pa.float64(): "arrow.PrimitiveTypes.Float64",
    pa.string(): "arrow.BinaryTypes.String",
    pa.binary(): "arrow.BinaryTypes.Binary",
}


def _emit_type(dtype: pa.DataType, *, origin: str) -> str:
    for proto, expr in _SCALAR_MAP.items():
        if dtype.equals(proto):
            return expr

    if pa.types.is_list(dtype):
        value_field = dtype.value_field
        inner_type = _emit_type(value_field.type, origin=f"{origin}[list item]")
        if value_field.nullable and value_field.name == "item":
            return f"arrow.ListOf({inner_type})"
        # Non-default item nullability or name — use ListOfField.
        return (
            f'arrow.ListOfField(arrow.Field{{Name: "{value_field.name}", '
            f"Type: {inner_type}, Nullable: {str(value_field.nullable).lower()}}})"
        )

    if pa.types.is_map(dtype):
        key_field = dtype.key_field
        item_field = dtype.item_field
        if (
            key_field.name != "key"
            or item_field.name != "value"
            or key_field.nullable is not False
            or item_field.nullable is not True
            or not _uses_default_map_field_name(dtype)
        ):
            raise GeneratorError(
                f"Map at {origin} uses non-default child field names or nullability "
                f"(key='{key_field.name}' nullable={key_field.nullable}, "
                f"item='{item_field.name}' nullable={item_field.nullable}). "
                "Add explicit MapType construction to go_schemas._emit_type() if needed.",
            )
        key_type = _emit_type(dtype.key_type, origin=f"{origin}[map key]")
        item_type = _emit_type(dtype.item_type, origin=f"{origin}[map value]")
        return f"arrow.MapOf({key_type}, {item_type})"

    if pa.types.is_dictionary(dtype):
        value_type = _emit_type(dtype.value_type, origin=f"{origin}[dict value]")
        index_type = _emit_type(dtype.index_type, origin=f"{origin}[dict index]")
        ordered = "true" if dtype.ordered else "false"
        return f"&arrow.DictionaryType{{IndexType: {index_type}, ValueType: {value_type}, Ordered: {ordered}}}"

    if pa.types.is_struct(dtype):
        child_exprs = [
            _emit_field_literal(dtype.field(i), origin=f"{origin}[struct child {i}]") for i in range(dtype.num_fields)
        ]
        return "arrow.StructOf(\n" + ",\n".join("\t\t" + c for c in child_exprs) + ",\n\t)"

    raise GeneratorError(
        f"vgi.codegen.go_schemas: unsupported Arrow type {type(dtype).__name__!r} at {origin} "
        f"(type={dtype!r}).\n"
        "To support this type, add a case to _emit_type() in vgi/codegen/go_schemas.py.",
    )


def _uses_default_map_field_name(dtype: pa.MapType[Any, Any, Any]) -> bool:
    canonical = pa.map_(dtype.key_type, dtype.item_type)
    return canonical.equals(dtype)


def _emit_field_literal(field: pa.Field[Any], *, origin: str) -> str:
    type_expr = _emit_type(field.type, origin=f"{origin}[{field.name}]")
    nullable_part = ", Nullable: true" if field.nullable else ""
    return f'arrow.Field{{Name: "{field.name}", Type: {type_expr}{nullable_part}}}'


def _emit_var(es: EmittedSchema) -> str:
    body = f"// Origin: {es.origin}\n"
    body += f"var {es.name}Schema = arrow.NewSchema([]arrow.Field{{\n"
    for f in es.schema:
        body += "\t" + _emit_field_literal(f, origin=f"{es.name}.{f.name}") + ",\n"
    body += "}, nil)\n"
    return body


GENERATOR_VERSION = "1"


def emit(out: TextIO) -> None:
    schemas = collect_schemas()

    body = io.StringIO()
    body.write("// © Copyright 2025-2026, Query.Farm LLC - https://query.farm\n")
    body.write("// SPDX-License-Identifier: Apache-2.0\n")
    body.write("\n")
    body.write("package generated\n")
    body.write("\n")
    body.write('import (\n\t"github.com/apache/arrow-go/v18/arrow"\n)\n')
    body.write("\n")
    body.write("var _ = arrow.BinaryTypes.String // keep import live when no schemas reference it\n")
    body.write("\n")
    for block in (_emit_var(es) for es in schemas):
        body.write(block)
        body.write("\n")

    out.write(
        provenance_comment(
            generator_module="vgi.codegen.go_schemas",
            generator_command="vgi-gen-go-schemas",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=[
                "uv run --project ~/Development/vgi-python vgi-gen-go-schemas \\",
                "  > ~/Development/vgi-go/vgi/generated/protocol_schemas.go",
            ],
            body=body.getvalue(),
        )
    )
    out.write("\n")
    out.write(body.getvalue())


def main() -> None:
    try:
        emit(sys.stdout)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
