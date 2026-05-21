# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Emit facade-style Schema literals for the VGI TypeScript worker.

Sister module to `vgi.codegen.cpp_schemas`. Same inputs (Protocol walk +
explicit info-type list), different rendering — emits human-readable
TypeScript that imports from the vgi-typescript Arrow facade
(`../arrow/index.js`) and exports one `VgiSchema` constant per unique
dataclass or per-method.

The facade re-exports either arrow-js or flechette per build target, so
the generated file is backend-agnostic — the same source compiles for the
Bun subprocess worker AND the Cloudflare Workers HTTP entrypoint.

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

### Facade quirks

- `dictionary(valueType, indexType, ordered?, id?)` — value first, opposite
  of pyarrow's `pa.dictionary(index_type, value_type)`. Emitter flips.
- `map(keyField, valueField, keysSorted)` — flat signature; the facade
  builds the Struct{key,value} entries Field internally.
- `field(name, type, nullable, metadata?)` — 3rd positional is nullable.
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
# Type emitter: pyarrow DataType -> facade factory call (utf8() / field(...))
# --------------------------------------------------------------------------- #

# pyarrow proto -> (factory expression, facade symbol to import)
_SCALAR_MAP: dict[Any, tuple[str, str]] = {
    pa.null(): ("nullType()", "nullType"),
    pa.bool_(): ("bool()", "bool"),
    pa.int8(): ("int8()", "int8"),
    pa.int16(): ("int16()", "int16"),
    pa.int32(): ("int32()", "int32"),
    pa.int64(): ("int64()", "int64"),
    pa.uint8(): ("uint8()", "uint8"),
    pa.uint16(): ("uint16()", "uint16"),
    pa.uint32(): ("uint32()", "uint32"),
    pa.uint64(): ("uint64()", "uint64"),
    pa.float32(): ("float32()", "float32"),
    pa.float64(): ("float64()", "float64"),
    pa.string(): ("utf8()", "utf8"),
    pa.binary(): ("binary()", "binary"),
}

# Track which facade symbols the emitter used, so the output's import
# statement only pulls in what's actually referenced.
_IMPORTS_IN_USE: set[str] = set()


def _use(name: str) -> None:
    _IMPORTS_IN_USE.add(name)


def _emit_type(dtype: pa.DataType, *, origin: str) -> str:
    for proto, (expr, sym) in _SCALAR_MAP.items():
        if dtype.equals(proto):
            _use(sym)
            return expr

    if pa.types.is_list(dtype):
        _use("list")
        _use("field")
        value_field = dtype.value_field
        inner_type = _emit_type(value_field.type, origin=f"{origin}[list item]")
        nullable = "true" if value_field.nullable else "false"
        return f'list(field("{value_field.name}", {inner_type}, {nullable}))'

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
        _use("map")
        _use("field")
        key_type = _emit_type(dtype.key_type, origin=f"{origin}[map key]")
        item_type = _emit_type(dtype.item_type, origin=f"{origin}[map value]")
        # pyarrow's pa.map_ defaults keys_sorted=False.
        return f'map(field("key", {key_type}, false), field("value", {item_type}, true), false)'

    if pa.types.is_dictionary(dtype):
        _use("dictionary")
        value_type = _emit_type(dtype.value_type, origin=f"{origin}[dict value]")
        index_type = _emit_type(dtype.index_type, origin=f"{origin}[dict index]")
        # facade: `dictionary(valueType, indexType, ordered?, id?)`.
        # Value comes first — opposite of pyarrow's (index, value).
        if dtype.ordered:
            return f"dictionary({value_type}, {index_type}, true)"
        return f"dictionary({value_type}, {index_type})"

    if pa.types.is_struct(dtype):
        _use("struct")
        _use("field")
        child_exprs = [
            _emit_field(dtype.field(i), origin=f"{origin}[struct child {i}]") for i in range(dtype.num_fields)
        ]
        return "struct([" + ", ".join(child_exprs) + "])"

    raise GeneratorError(
        f"vgi.codegen.ts_schemas: unsupported Arrow type {type(dtype).__name__!r} at {origin} "
        f"(type={dtype!r}).\n"
        "To support this type, add a case to _emit_type() in vgi/codegen/ts_schemas.py.",
    )


def _uses_default_map_field_name(dtype: pa.MapType[Any, Any, Any]) -> bool:
    canonical = pa.map_(dtype.key_type, dtype.item_type)
    return canonical.equals(dtype)


def _emit_field(field_obj: pa.Field[Any], *, origin: str) -> str:
    _use("field")
    type_expr = _emit_type(field_obj.type, origin=f"{origin}[{field_obj.name}]")
    nullable = "true" if field_obj.nullable else "false"
    return f'field("{field_obj.name}", {type_expr}, {nullable})'


def _emit_const(es: EmittedSchema) -> str:
    _use("schema")
    body = f"// Origin: {es.origin}\n"
    body += f"export const {es.name}Schema = schema("
    if len(es.schema) == 0:
        body += "[]);\n"
    else:
        body += "[\n"
        for f in es.schema:
            body += "  " + _emit_field(f, origin=f"{es.name}.{f.name}") + ",\n"
        body += "]);\n"
    return body


# Bumped to v2 to mark the arrow-js -> facade transition. The generated
# file's `Generator: vgi-gen-ts-schemas v2` header lets the conformance test
# detect stale checkouts that need a regen.
GENERATOR_VERSION = "2"


def emit(out: TextIO) -> None:
    """Emit the generated TypeScript schemas module to *out*."""
    schemas = collect_schemas()

    # Render all schemas FIRST to capture which facade symbols were used.
    _IMPORTS_IN_USE.clear()
    body_blocks = [_emit_const(es) for es in schemas]
    # `schema` is always used in the import because every factory returns one.
    _use("schema")
    imports = sorted(_IMPORTS_IN_USE)

    body = io.StringIO()
    body.write("import {\n")
    for sym in imports:
        body.write(f"  {sym},\n")
    body.write('} from "../arrow/index.js";\n\n')

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
    """Console-script entrypoint — write the TypeScript schemas module to stdout."""
    try:
        emit(sys.stdout)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
