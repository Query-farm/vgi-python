# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Emit arrow-rs Schema factories for the VGI Rust protocol crate.

Sister module to `vgi.codegen.cpp_schemas`, `vgi.codegen.go_schemas` and
`vgi.codegen.ts_schemas`. Same inputs (Protocol walk + explicit info-type list),
different rendering — emits a single `.rs` file with one
`pub fn xxx_schema() -> SchemaRef` per unique dataclass or per-method, plus a
total `params_schema_for()` dispatch over every method.

### Why this exists

`vgi-protocol`'s hand-written `wire::params_schema_for` covers 14 methods. The
protocol has 70, of which 45 take flat params rather than the wrapped
`request` envelope — so 31 methods currently advertise the envelope schema
through `__describe__` when they should advertise their real field list. A
client that builds its request from the advertised schema (the TypeScript
client does) then sends a metadata-only batch and every handler reports a
missing column. Generating the full table removes that whole class of bug and
keeps it removed.

### Multirepo workflow

`vgi-python` and `vgi-rust` are separate repos. Protocol changes flow:

1. Modify the dataclass in `vgi-python`.
2. Run:
   ```
   uv run --project ~/Development/vgi-python vgi-gen-rust-schemas \
       > ~/Development/vgi-rust/vgi-protocol/src/generated/protocol_schemas.rs
   ```
3. Commit the regenerated file in the `vgi-rust` repo on the same branch.

`tests/test_generated_rust_schemas.py` in vgi-python enforces that the
checked-in `.rs` matches what the generator would emit right now.

### arrow-rs quirks

- `DataType::Dictionary(Box<DataType>, Box<DataType>)` carries **no `ordered`
  flag** — arrow-rs cannot represent an ordered dictionary in its type system.
  An ordered dictionary therefore raises `GeneratorError` rather than silently
  losing the flag. (No VGI schema uses one today.)
- `DataType::Map(Arc<Field>, bool)` takes the *entries struct field*, not the
  key/value pair: `Field::new("entries", DataType::Struct([key, value]), false)`.
  The trailing bool is `keys_sorted`.
- `DataType::List(Arc<Field>)` likewise carries the item field, so item name and
  nullability round-trip exactly.
- Unsigned ints are `UInt8`/`UInt16`/… (capital `I`), unlike pyarrow's `uint8`.
- Schemas are heap types, so each factory caches in a `OnceLock` and hands back
  a cheap `Arc` clone — `params_schema_for` is on the per-request path.
"""

from __future__ import annotations

import io
import sys
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from vgi_rpc.rpc._types import rpc_methods

from vgi.codegen._common import (
    EXTRA_RESPONSE_TYPES,
    REQUEST_TYPES,
    EmittedSchema,
    GeneratorError,
    collect_schemas,
    provenance_comment,
)
from vgi.protocol import VgiProtocol

if TYPE_CHECKING:
    from typing import TextIO


_SCALAR_MAP: dict[Any, str] = {
    pa.bool_(): "DataType::Boolean",
    pa.int8(): "DataType::Int8",
    pa.int16(): "DataType::Int16",
    pa.int32(): "DataType::Int32",
    pa.int64(): "DataType::Int64",
    pa.uint8(): "DataType::UInt8",
    pa.uint16(): "DataType::UInt16",
    pa.uint32(): "DataType::UInt32",
    pa.uint64(): "DataType::UInt64",
    pa.float32(): "DataType::Float32",
    pa.float64(): "DataType::Float64",
    pa.string(): "DataType::Utf8",
    pa.large_string(): "DataType::LargeUtf8",
    pa.binary(): "DataType::Binary",
    pa.large_binary(): "DataType::LargeBinary",
}

_TIME_UNIT_MAP = {
    "s": "TimeUnit::Second",
    "ms": "TimeUnit::Millisecond",
    "us": "TimeUnit::Microsecond",
    "ns": "TimeUnit::Nanosecond",
}


def snake_case(camel: str) -> str:
    """Convert an EmittedSchema CamelCase stem to a Rust snake_case fn stem."""
    out: list[str] = []
    for i, ch in enumerate(camel):
        if ch.isupper() and i > 0 and not camel[i - 1].isupper():
            out.append("_")
        out.append(ch.lower())
    return "".join(out)


def _rust_str(s: str) -> str:
    """Render a Python string as a Rust string literal."""
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _emit_type(dtype: pa.DataType, *, origin: str) -> str:
    for proto, expr in _SCALAR_MAP.items():
        if dtype.equals(proto):
            return expr

    if pa.types.is_list(dtype):
        item = _emit_field(dtype.value_field, origin=f"{origin}[list item]")
        return f"DataType::List(Arc::new({item}))"

    if pa.types.is_large_list(dtype):
        item = _emit_field(dtype.value_field, origin=f"{origin}[large_list item]")
        return f"DataType::LargeList(Arc::new({item}))"

    if pa.types.is_map(dtype):
        key_field = dtype.key_field
        item_field = dtype.item_field
        if key_field.name != "key" or item_field.name != "value" or key_field.nullable:
            raise GeneratorError(
                f"Map at {origin} uses non-canonical child fields "
                f"(key='{key_field.name}' nullable={key_field.nullable}, "
                f"item='{item_field.name}' nullable={item_field.nullable}). "
                "arrow-rs needs an explicit entries struct — extend "
                "rust_schemas._emit_type() if this shape is intentional.",
            )
        key = _emit_field(key_field, origin=f"{origin}[map key]")
        value = _emit_field(item_field, origin=f"{origin}[map value]")
        keys_sorted = "true" if dtype.keys_sorted else "false"
        return (
            'DataType::Map(Arc::new(Field::new("entries", '
            f"DataType::Struct(Fields::from(vec![{key}, {value}])), false)), {keys_sorted})"
        )

    if pa.types.is_dictionary(dtype):
        if dtype.ordered:
            raise GeneratorError(
                f"Ordered dictionary at {origin}: arrow-rs's DataType::Dictionary has no "
                "`ordered` flag, so the ordering would be silently dropped on the Rust side "
                "while pyarrow and the C++ extension keep it. Either drop the ordering in "
                "vgi-python or teach the Rust worker to carry it out of band.",
            )
        index = _emit_type(dtype.index_type, origin=f"{origin}[dict index]")
        value = _emit_type(dtype.value_type, origin=f"{origin}[dict value]")
        return f"DataType::Dictionary(Box::new({index}), Box::new({value}))"

    if pa.types.is_struct(dtype):
        children = ", ".join(
            _emit_field(dtype.field(i), origin=f"{origin}[struct child {i}]") for i in range(dtype.num_fields)
        )
        return f"DataType::Struct(Fields::from(vec![{children}]))"

    if pa.types.is_timestamp(dtype):
        unit = _TIME_UNIT_MAP.get(dtype.unit)
        if unit is None:
            raise GeneratorError(
                f"timestamp at {origin} has unknown unit {dtype.unit!r}; expected one of {sorted(_TIME_UNIT_MAP)}.",
            )
        tz = f"Some({_rust_str(dtype.tz)}.into())" if dtype.tz else "None"
        return f"DataType::Timestamp({unit}, {tz})"

    raise GeneratorError(
        f"vgi.codegen.rust_schemas: unsupported Arrow type {type(dtype).__name__!r} at {origin} "
        f"(type={dtype!r}).\n"
        "To support this type, add a case to _emit_type() in vgi/codegen/rust_schemas.py.",
    )


def _emit_field(field: pa.Field[Any], *, origin: str) -> str:
    type_expr = _emit_type(field.type, origin=f"{origin}[{field.name}]")
    nullable = "true" if field.nullable else "false"
    return f"Field::new({_rust_str(field.name)}, {type_expr}, {nullable})"


def _emit_fn(es: EmittedSchema) -> str:
    fn = snake_case(es.name) + "_schema"
    lines = [
        f"/// Origin: {es.origin}",
        # Nested Arrow types emit as long single-line expressions that rustfmt
        # would rewrap, breaking the byte-exact drift test in vgi-python (whose
        # CI has no Rust toolchain). The per-item outer `#[rustfmt::skip]` is
        # stable; the file-level `#![rustfmt::skip]` form is NOT (rustc rejects
        # custom inner attributes) and rustfmt.toml's `ignore` is nightly-only.
        "#[rustfmt::skip]",
        f"pub fn {fn}() -> SchemaRef {{",
        "    static S: OnceLock<SchemaRef> = OnceLock::new();",
        "    S.get_or_init(|| {",
    ]
    fields = list(es.schema)
    if not fields:
        lines.append("        Arc::new(Schema::new(Vec::<Field>::new()))")
    else:
        lines.append("        Arc::new(Schema::new(vec![")
        for f in fields:
            lines.append("            " + _emit_field(f, origin=f"{es.name}.{f.name}") + ",")
        lines.append("        ]))")
    lines.append("    })")
    lines.append("    .clone()")
    lines.append("}")
    return "\n".join(lines) + "\n"


def _emit_params_dispatch(schemas: list[EmittedSchema]) -> str:
    """Emit the total `params_schema_for` over every RPC method."""
    from vgi.codegen._common import sanitize_name

    by_name = {es.name: es for es in schemas}
    methods = rpc_methods(VgiProtocol)

    lines = [
        "/// Params schema for an RPC method, or `None` when the method is unknown to",
        "/// this protocol version.",
        "///",
        "/// Callers that need a total function should fall back to the wrapped",
        "/// `request` envelope, which is what an unrecognised method carries.",
        "#[rustfmt::skip]",
        "pub fn params_schema_for(method: &str) -> Option<SchemaRef> {",
        "    match method {",
    ]
    for method_name in sorted(methods.keys()):
        stem = sanitize_name(method_name) + "Params"
        if stem not in by_name:
            raise GeneratorError(
                f"Method '{method_name}' has no collected params schema named '{stem}'. "
                "collect_schemas() and rpc_methods() have diverged.",
            )
        lines.append(f"        {_rust_str(method_name)} => Some({snake_case(stem)}_schema()),")
    lines.append("        _ => None,")
    lines.append("    }")
    lines.append("}")
    return "\n".join(lines) + "\n"


GENERATOR_VERSION = "1"


def emit(out: TextIO) -> None:
    """Emit the generated Rust schemas module to *out*."""
    # The request records ride inside their method's params envelope as an
    # opaque blob, so nothing reaches them from a method signature. Emitting
    # them here gives vgi-rust the same schema set every other SDK carries —
    # and keeps the cross-language name check total.
    schemas = collect_schemas(extra_response_types=(*EXTRA_RESPONSE_TYPES, *REQUEST_TYPES))

    body = io.StringIO()
    body.write("// Copyright 2025, 2026 Query Farm LLC - https://query.farm\n")
    body.write("\n")
    body.write("//! Arrow schemas for the VGI wire protocol, generated from the canonical\n")
    body.write("//! Python `VgiProtocol`. Every factory caches its schema in a `OnceLock` and\n")
    body.write("//! returns a cheap `Arc` clone.\n")
    body.write("\n")
    body.write("#![allow(clippy::too_many_lines)]\n")
    body.write("\n")
    body.write("use std::sync::{Arc, OnceLock};\n")
    body.write("\n")
    body.write("use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};\n")
    body.write("\n")
    body.write("\n".join(_emit_fn(es) for es in schemas))
    body.write("\n")
    body.write(_emit_params_dispatch(schemas))

    out.write(
        provenance_comment(
            generator_module="vgi.codegen.rust_schemas",
            generator_command="vgi-gen-rust-schemas",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=[
                "uv run --project ~/Development/vgi-python vgi-gen-rust-schemas \\",
                "  > ~/Development/vgi-rust/vgi-protocol/src/generated/protocol_schemas.rs",
            ],
            body=body.getvalue(),
        )
    )
    out.write("\n")
    out.write(body.getvalue())


def main() -> None:
    """Console-script entrypoint — write the Rust schemas module to stdout."""
    try:
        emit(sys.stdout)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
