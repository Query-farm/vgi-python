"""Emit arrow-js Schema literals for the VGI TypeScript worker.

Sister module to `vgi.codegen.cpp_schemas`. Same inputs (Protocol walk +
explicit info-type list), different rendering — emits human-readable
TypeScript that `import`s from `@query-farm/apache-arrow` and exports one
`Schema` constant per unique dataclass or per-method.

### Multirepo workflow

`vgi-python` and `vgi-typescript` are separate repos. Protocol changes flow:

1. Modify the dataclass in `vgi-python`.
2. Run:
   ```
   uv run --project ~/Development/vgi-python vgi-gen-ts-schemas \
       > ~/Development/vgi-typescript/src/generated/vgi-protocol-schemas.ts
   ```
3. Commit the regenerated file in the `vgi-typescript` repo on the same branch.

`tests/test_generated_ts_schemas.py` in vgi-python enforces that the
checked-in `.ts` matches what the generator would emit right now.

### Arrow-JS quirks

- `new Dictionary(valueType, indexType, ordered, ...)` — value first, opposite
  of pyarrow's `pa.dictionary(index_type, value_type)`. Emitter flips.
- `new Map_(new Field("entries", new Struct([key, value]), false), keysSorted)`
  — structural form; arrow-js doesn't expose a convenience overload.
- `new Field(name, type, nullable, metadata?)` — 3rd positional is nullable.
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


# --------------------------------------------------------------------------- #
# Type emitter: pyarrow DataType -> TS expression (new Utf8() / new Field(...))
# --------------------------------------------------------------------------- #

_SCALAR_MAP: dict[Any, str] = {
    pa.null(): "new Null()",
    pa.bool_(): "new Bool()",
    pa.int8(): "new Int8()",
    pa.int16(): "new Int16()",
    pa.int32(): "new Int32()",
    pa.int64(): "new Int64()",
    pa.uint8(): "new Uint8()",
    pa.uint16(): "new Uint16()",
    pa.uint32(): "new Uint32()",
    pa.uint64(): "new Uint64()",
    pa.float32(): "new Float32()",
    pa.float64(): "new Float64()",
    pa.string(): "new Utf8()",
    pa.binary(): "new Binary()",
}

# Track which arrow-js symbols the emitter used, so the output's import
# statement only pulls in what's actually referenced.
_IMPORTS_IN_USE: set[str] = set()


def _use(name: str) -> None:
    _IMPORTS_IN_USE.add(name)


_SCALAR_IMPORT: dict[str, str] = {
    "new Null()": "Null",
    "new Bool()": "Bool",
    "new Int8()": "Int8",
    "new Int16()": "Int16",
    "new Int32()": "Int32",
    "new Int64()": "Int64",
    "new Uint8()": "Uint8",
    "new Uint16()": "Uint16",
    "new Uint32()": "Uint32",
    "new Uint64()": "Uint64",
    "new Float32()": "Float32",
    "new Float64()": "Float64",
    "new Utf8()": "Utf8",
    "new Binary()": "Binary",
}


def _emit_type(dtype: pa.DataType, *, origin: str) -> str:
    for proto, expr in _SCALAR_MAP.items():
        if dtype.equals(proto):
            _use(_SCALAR_IMPORT[expr])
            return expr

    if pa.types.is_list(dtype):
        _use("List")
        _use("Field")
        value_field = dtype.value_field
        inner_type = _emit_type(value_field.type, origin=f"{origin}[list item]")
        nullable = "true" if value_field.nullable else "false"
        return f'new List(new Field("{value_field.name}", {inner_type}, {nullable}))'

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
                "Add explicit MapType construction to ts_schemas._emit_type() if needed.",
            )
        _use("Map_")
        _use("Field")
        _use("Struct")
        key_type = _emit_type(dtype.key_type, origin=f"{origin}[map key]")
        item_type = _emit_type(dtype.item_type, origin=f"{origin}[map value]")
        # pyarrow's pa.map_ defaults keys_sorted=False.
        keys_sorted = "false"
        # Cast children to `any` to bypass arrow-js's Field<T> generic
        # inference, matching the pattern already used in dispatch.ts:354-357.
        return (
            'new Map_(new Field("entries", new Struct([\n'
            f'      new Field("key", {key_type} as any, false),\n'
            f'      new Field("value", {item_type} as any, true),\n'
            f"    ] as any), false), {keys_sorted})"
        )

    if pa.types.is_dictionary(dtype):
        _use("Dictionary")
        value_type = _emit_type(dtype.value_type, origin=f"{origin}[dict value]")
        index_type = _emit_type(dtype.index_type, origin=f"{origin}[dict index]")
        # arrow-js: `new Dictionary(valueType, indexType, id?, isOrdered?)`.
        # Value comes first — opposite of pyarrow's (index, value). Omit `id` so
        # arrow-js auto-assigns; pass `null` positional when `ordered` is true.
        if dtype.ordered:
            return f"new Dictionary({value_type}, {index_type}, null, true)"
        return f"new Dictionary({value_type}, {index_type})"

    if pa.types.is_struct(dtype):
        _use("Struct")
        _use("Field")
        child_exprs = [
            _emit_field(dtype.field(i), origin=f"{origin}[struct child {i}]") for i in range(dtype.num_fields)
        ]
        return "new Struct([" + ", ".join(child_exprs) + "])"

    raise GeneratorError(
        f"vgi.codegen.ts_schemas: unsupported Arrow type {type(dtype).__name__!r} at {origin} "
        f"(type={dtype!r}).\n"
        "To support this type, add a case to _emit_type() in vgi/codegen/ts_schemas.py.",
    )


def _uses_default_map_field_name(dtype: pa.MapType[Any, Any, Any]) -> bool:
    canonical = pa.map_(dtype.key_type, dtype.item_type)
    return canonical.equals(dtype)


def _emit_field(field: pa.Field[Any], *, origin: str) -> str:
    _use("Field")
    type_expr = _emit_type(field.type, origin=f"{origin}[{field.name}]")
    nullable = "true" if field.nullable else "false"
    return f'new Field("{field.name}", {type_expr}, {nullable})'


def _emit_const(es: EmittedSchema) -> str:
    _use("Schema")
    body = f"// Origin: {es.origin}\n"
    body += f"export const {es.name}Schema = new Schema("
    if len(es.schema) == 0:
        body += "[]);\n"
    else:
        body += "[\n"
        for f in es.schema:
            body += "  " + _emit_field(f, origin=f"{es.name}.{f.name}") + ",\n"
        body += "]);\n"
    return body


GENERATOR_VERSION = "1"


def emit(out: TextIO) -> None:
    schemas = collect_schemas()

    # Render all schemas FIRST to capture which arrow-js symbols were used.
    _IMPORTS_IN_USE.clear()
    body_blocks = [_emit_const(es) for es in schemas]
    # Schema is always used in the import because every factory returns one.
    _use("Schema")
    imports = sorted(_IMPORTS_IN_USE)

    body = io.StringIO()
    body.write("import {\n")
    for sym in imports:
        body.write(f"  {sym},\n")
    body.write('} from "@query-farm/apache-arrow";\n\n')

    for block in body_blocks:
        body.write(block)
        body.write("\n")

    out.write("// ============================================================================\n")
    out.write(
        provenance_comment(
            generator_module="vgi.codegen.ts_schemas",
            generator_command="vgi-gen-ts-schemas",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=[
                "uv run --project ~/Development/vgi-python vgi-gen-ts-schemas \\",
                "  > ~/Development/vgi-typescript/src/generated/vgi-protocol-schemas.ts",
            ],
            body=body.getvalue(),
        )
    )
    out.write("// ============================================================================\n")
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
