# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Emit `#[derive(VgiArrow)]` params structs for every VGI RPC method.

Sister module to `vgi.codegen.cpp_request_builders`. The C++ side needs
hand-rolled `BuildXxxParams(...)` functions because it has no derive macro; Rust
does, so the idiomatic equivalent is a **struct per method** that
`vgi_protocol::wire::to_batch` turns into the request batch:

```rust
use vgi_protocol::generated::request_params::CatalogTableGetParams;
use vgi_protocol::wire::to_batch;

let batch = to_batch(CatalogTableGetParams {
    attach_opaque_data: handle.into(),
    schema_name: "main".into(),
    name: "orders".into(),
    at_unit: None,
    at_value: None,
    transaction_opaque_data: None,
})?;
```

That is strictly better than emitting builder functions: the compiler enforces
that every required column is supplied, and `Option<T>` makes nullability part
of the type rather than a runtime convention.

### The correctness anchor

A struct is only useful if its derived schema *equals* the advertised params
schema. The generator emits a `schema_parity` test module into
`request_params.rs` itself asserting exactly that for every struct — derive
output versus `wire::params_schema_for`. If the
derive's type mapping ever disagrees with the canonical protocol, that test
fails rather than a client silently sending a malformed batch.

### Multirepo workflow

```
uv run --project ~/Development/vgi-python python -m vgi.codegen.rust_request_builders \
    > ~/Development/vgi-rust/vgi-protocol/src/generated/request_params.rs
```

`tests/test_generated_rust_request_builders.py` in vgi-python enforces that the
checked-in `.rs` matches what the generator would emit right now.
"""

from __future__ import annotations

import io
import sys
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from vgi_rpc.rpc._types import rpc_methods

from vgi.codegen._common import (
    GeneratorError,
    collect_schemas,
    provenance_comment,
    sanitize_name,
)
from vgi.protocol import VgiProtocol

if TYPE_CHECKING:
    from typing import TextIO


# Reserved words that would be a syntax error as a bare field name. `type` is
# the one that actually occurs (the kind selector on the schema-contents
# methods), but escaping the whole set keeps a future protocol field from
# breaking the build.
_RUST_KEYWORDS = frozenset(
    [
        "as",
        "break",
        "const",
        "continue",
        "crate",
        "dyn",
        "else",
        "enum",
        "extern",
        "false",
        "fn",
        "for",
        "if",
        "impl",
        "in",
        "let",
        "loop",
        "match",
        "mod",
        "move",
        "mut",
        "pub",
        "ref",
        "return",
        "self",
        "Self",
        "static",
        "struct",
        "super",
        "trait",
        "true",
        "type",
        "unsafe",
        "use",
        "where",
        "while",
        "async",
        "await",
        "box",
        "do",
        "final",
        "macro",
        "override",
        "priv",
        "try",
        "typeof",
        "unsized",
        "virtual",
        "yield",
    ]
)


def _rust_ident(name: str) -> str:
    """Escape a wire field name into a usable Rust field identifier."""
    return f"r#{name}" if name in _RUST_KEYWORDS else name


_SCALAR_RUST: dict[Any, str] = {
    pa.bool_(): "bool",
    pa.int8(): "i8",
    pa.int16(): "i16",
    pa.int32(): "i32",
    pa.int64(): "i64",
    pa.uint8(): "u8",
    pa.uint16(): "u16",
    pa.uint32(): "u32",
    pa.uint64(): "u64",
    pa.float32(): "f32",
    pa.float64(): "f64",
    pa.string(): "String",
    pa.binary(): "Bytes",
    pa.large_binary(): "LargeBytes",
}

_DICT_STRING = pa.dictionary(pa.int16(), pa.string())
_STR_MAP = pa.map_(pa.string(), pa.string())


def _rust_type(dtype: pa.DataType, *, origin: str) -> str:
    for proto, rust in _SCALAR_RUST.items():
        if dtype.equals(proto):
            return rust
    if dtype.equals(_DICT_STRING):
        return "DictString"
    if dtype.equals(_STR_MAP):
        return "StrMap"
    raise GeneratorError(
        f"vgi.codegen.rust_request_builders: no Rust type for Arrow type {dtype!r} at {origin}.\n"
        "Params schemas have historically used only binary / string / bool / dictionary<string> / "
        "map<string,string>. Add a mapping to _rust_type() in "
        "vgi/codegen/rust_request_builders.py, and make sure the VgiArrow derive round-trips it.",
    )


def _emit_struct(method_name: str, schema: pa.Schema) -> str:
    struct = sanitize_name(method_name) + "Params"
    lines = [
        f"/// Params for the `{method_name}` RPC.",
        "///",
        f'/// Derived schema must equal `wire::params_schema_for("{method_name}")`.',
        "#[derive(Debug, Clone, VgiArrow)]",
        f"pub struct {struct} {{",
    ]
    for field in schema:
        ty = _rust_type(field.type, origin=f"{struct}.{field.name}")
        if field.nullable:
            ty = f"Option<{ty}>"
        lines.append(f"    pub {_rust_ident(field.name)}: {ty},")
    lines.append("}")
    return "\n".join(lines) + "\n"


GENERATOR_VERSION = "1"


def emit(out: TextIO) -> None:
    """Emit the generated Rust params structs to *out*."""
    by_method: dict[str, pa.Schema] = {}
    for es in collect_schemas():
        if not es.name.endswith("Params"):
            continue
        # origin is "method '<name>' params"
        method = es.origin.split("'")[1]
        by_method[method] = es.schema

    methods = sorted(rpc_methods(VgiProtocol))
    missing = [m for m in methods if m not in by_method]
    if missing:
        raise GeneratorError(f"No params schema collected for methods: {missing}")

    body = io.StringIO()
    body.write("// Copyright 2025, 2026 Query Farm LLC - https://query.farm\n")
    body.write("\n")
    body.write("//! Typed request-params structs, generated from the canonical Python\n")
    body.write("//! `VgiProtocol`. Hand one to [`crate::wire::to_batch`] to build a request\n")
    body.write("//! batch whose schema matches [`crate::wire::params_schema_for`] exactly.\n")
    body.write("//!\n")
    body.write("//! Nullability is carried by `Option<T>`, so the compiler enforces which\n")
    body.write("//! columns are required. Field names that collide with Rust keywords are\n")
    body.write("//! escaped with `r#` and keep their wire spelling.\n")
    body.write("\n")
    # `VgiArrow` cannot derive on a field-less struct, and a method whose params
    # carry no columns needs no builder anyway — the caller sends a metadata-only
    # batch. Name them in the module docs rather than emitting a type that
    # wouldn't compile.
    empty = [m for m in methods if not len(by_method[m])]
    non_empty = [m for m in methods if len(by_method[m])]

    if empty:
        body.write("//!\n")
        body.write("//! These methods take no params columns and so have no struct here; send a\n")
        body.write("//! metadata-only batch:\n")
        for m in empty:
            body.write(f"//! - `{m}`\n")
    body.write("\n")

    # Import only what the emitted structs actually name — an unused import is a
    # warning, and vgi-rust builds warning-free.
    rendered = {_rust_type(f.type, origin=m) for m in non_empty for f in by_method[m]}
    vgi_rpc_names = sorted({n for n in rendered if n in {"Bytes", "LargeBytes", "DictString"}} | {"VgiArrow"})
    body.write(f"use vgi_rpc::{{{', '.join(vgi_rpc_names)}}};\n")
    if "StrMap" in rendered:
        body.write("\n")
        body.write("use crate::protocol::dtos::StrMap;\n")
    body.write("\n")
    body.write("\n".join(_emit_struct(m, by_method[m]) for m in non_empty))

    # The whole point of these structs is that `to_batch` produces a batch whose
    # schema matches what the peer advertises. Emit the proof alongside them so
    # the list can never drift from the structs it checks.
    body.write("\n")
    body.write("#[cfg(test)]\n")
    body.write("mod schema_parity {\n")
    body.write("    //! Every generated struct's derived schema must equal the advertised\n")
    body.write("    //! params schema for its method. If the `VgiArrow` derive's type mapping\n")
    body.write("    //! ever disagrees with the canonical protocol, this fails here rather\n")
    body.write("    //! than as a malformed batch on the wire.\n")
    body.write("\n")
    body.write("    use super::*;\n")
    body.write("    use crate::wire::{flat_schema, params_schema_for};\n")
    body.write("\n")
    body.write("    #[test]\n")
    body.write("    fn derived_schemas_match_the_advertised_params() {\n")
    for m in non_empty:
        struct = sanitize_name(m) + "Params"
        body.write("        assert_eq!(\n")
        body.write(f"            flat_schema::<{struct}>(),\n")
        body.write(f'            params_schema_for("{m}"),\n')
        body.write(f'            "{struct} derives a schema that does not match params_schema_for(\\"{m}\\")",\n')
        body.write("        );\n")
    body.write("    }\n")
    body.write("}\n")

    out.write(
        provenance_comment(
            generator_module="vgi.codegen.rust_request_builders",
            generator_command="vgi-gen-rust-request-builders",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=[
                "uv run --project ~/Development/vgi-python \\",
                "  python -m vgi.codegen.rust_request_builders \\",
                "  > ~/Development/vgi-rust/vgi-protocol/src/generated/request_params.rs",
            ],
            body=body.getvalue(),
        )
    )
    out.write("\n")
    out.write(body.getvalue())


def main() -> None:
    """Console-script entrypoint — write the Rust params structs to stdout."""
    try:
        emit(sys.stdout)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
