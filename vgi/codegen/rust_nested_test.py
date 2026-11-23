# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit Rust's nested-binary conformance test (vgi docs/large-values-3.4.md §3.4).

The emitted file is a `vgi-protocol` integration test that reads the corpus
`conformance/nested-binary/` (`vgi.codegen.nested_binary_fixtures`, copied into
vgi-rust by ``scripts/regen_generated.py``) and checks the generated
`Nested<T>` wrapper (`vgi.codegen.rust_types`) against it:

1. **Byte identity:** each embedding fixture decodes; its `bind_call` re-encodes
   as exactly `bind_call.bin`, both without and after accessing the value; the
   accessed value's fields equal `expected.json`; a decode error names the field
   path and does not fail the embedding record's decode.
2. **Typed construction:** a `BindRequest` built from fields, wrapped, embedded
   in an `InitRequest`, encoded and decoded, reads back the same fields; encoding
   the same wrapper twice gives identical bytes.
3. **Null arguments:** `bind-arguments-null.arrow` decodes with `arguments ==
   None` and the location set; `bind-both-null` / `bind-both-set` fail the
   generated `BindRequest::validate_arguments` with `invalid_bind_request`.

Spec test 4 (re-encoding after opening sealed opaque data, rule 11) exercises
vgi-rust's `OpaqueSealer::open_batch`, which is SDK runtime, not generated code;
it belongs to vgi-rust's own suite (Phase 4).

The embedding records, their nested fields and the struct literals are derived
from `rust_types.build_model()`, so a field added to a record in Python reaches
this test on regeneration.

.. code-block:: bash

   uv run --project ~/Development/vgi-python python scripts/regen_generated.py
"""

from __future__ import annotations

import io
import re
import sys
from typing import TYPE_CHECKING

from vgi.codegen import rust_types
from vgi.codegen._common import GeneratorError, provenance_comment
from vgi.codegen.rust_schemas import snake_case

if TYPE_CHECKING:
    from typing import TextIO

GENERATOR_VERSION = "1"

#: The repository this test is checked into, and its path there.
REPO = "vgi-rust"
TARGET = "vgi-protocol/tests/nested_binary.rs"

_SIMPLE_DEFAULTS = {
    "String": "String::new()",
    "Bytes": "Bytes(Vec::new())",
    "LargeBytes": "LargeBytes(Vec::new())",
    "DictString": "DictString(String::new())",
    "bool": "false",
    "StrMap": "Default::default()",
    "IntMap": "Default::default()",
}


def _default(rust_type: str, origin: str) -> str:
    """A Rust expression for an empty value of *rust_type*."""
    if rust_type.startswith("Option<"):
        return "None"
    if rust_type.startswith("Vec<"):
        return "Vec::new()"
    if rust_type in _SIMPLE_DEFAULTS:
        return _SIMPLE_DEFAULTS[rust_type]
    if re.fullmatch(r"[iu](8|16|32|64)", rust_type):
        return "0"
    if rust_type in {"f32", "f64"}:
        return "0.0"
    raise GeneratorError(f"{origin}: no default expression for Rust type {rust_type}; extend _default().")


def _literal(struct: rust_types._Struct, overrides: dict[str, str], indent: str) -> str:
    """A struct literal: every field, *overrides* where given, empty values elsewhere."""
    unknown = set(overrides) - {f.wire_name for f in struct.fields}
    if unknown:
        raise GeneratorError(f"{struct.name}: no fields {sorted(unknown)}")
    lines = [f"{struct.name} {{"]
    for f in struct.fields:
        value = overrides.get(f.wire_name) or _default(f.rust_type, f"{struct.name}.{f.wire_name}")
        lines.append(f"{indent}    {f.ident}: {value},")
    lines.append(f"{indent}}}")
    return "\n".join(lines)


_PRELUDE = r"""
//! Nested-binary conformance (vgi `docs/large-values-3.4.md` §3.4) for the
//! generated raw-preserving `Nested<T>` wrapper, over the shared corpus in
//! `conformance/nested-binary/`. Spec test 4 (re-encode after opening sealed
//! opaque data) is vgi-rust runtime and lives in its own suite.

use std::path::PathBuf;

use arrow_array::types::Int16Type;
use arrow_array::{
    Array, BinaryArray, BooleanArray, DictionaryArray, Int32Array, Int64Array, LargeBinaryArray,
    ListArray, StringArray, StructArray,
};
use arrow_schema::DataType;
use serde_json::{Map, Value};
use vgi_protocol::generated::protocol_types::*;
use vgi_protocol::{ipc, wire};
use vgi_rpc::{Bytes, Code, DictString, VgiArrow};

fn corpus(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("conformance")
        .join("nested-binary")
        .join(name)
}

fn read(name: &str) -> Vec<u8> {
    std::fs::read(corpus(name)).unwrap_or_else(|e| panic!("reading {name}: {e}"))
}

fn read_json(name: &str) -> Value {
    serde_json::from_slice(&read(name)).unwrap_or_else(|e| panic!("parsing {name}: {e}"))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_value(bytes: &[u8]) -> Value {
    Value::Object(Map::from_iter([(
        "hex".to_string(),
        Value::String(hex(bytes)),
    )]))
}

/// One Arrow value in the corpus's language-neutral JSON form (`expected.json`
/// `value_encoding`).
fn wire_json(arr: &dyn Array, idx: usize) -> Value {
    if arr.is_null(idx) {
        return Value::Null;
    }
    let any = arr.as_any();
    match arr.data_type() {
        DataType::Binary => hex_value(any.downcast_ref::<BinaryArray>().unwrap().value(idx)),
        DataType::LargeBinary => {
            hex_value(any.downcast_ref::<LargeBinaryArray>().unwrap().value(idx))
        }
        DataType::Utf8 => {
            Value::String(any.downcast_ref::<StringArray>().unwrap().value(idx).into())
        }
        DataType::Boolean => Value::Bool(any.downcast_ref::<BooleanArray>().unwrap().value(idx)),
        DataType::Int64 => Value::from(any.downcast_ref::<Int64Array>().unwrap().value(idx)),
        DataType::Int32 => Value::from(any.downcast_ref::<Int32Array>().unwrap().value(idx)),
        DataType::Dictionary(_, _) => {
            let dict = any.downcast_ref::<DictionaryArray<Int16Type>>().unwrap();
            wire_json(dict.values().as_ref(), dict.keys().value(idx) as usize)
        }
        DataType::List(_) => {
            let items = any.downcast_ref::<ListArray>().unwrap().value(idx);
            Value::Array(
                (0..items.len())
                    .map(|i| wire_json(items.as_ref(), i))
                    .collect(),
            )
        }
        DataType::Struct(fields) => {
            let s = any.downcast_ref::<StructArray>().unwrap();
            Value::Object(
                fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| (f.name().clone(), wire_json(s.column(i).as_ref(), idx)))
                    .collect(),
            )
        }
        other => panic!("wire_json: unexpected Arrow type {other:?}"),
    }
}

/// A record's columns in the corpus's JSON form, by encoding its typed fields.
fn record_json<R: VgiArrow>(record: R) -> Map<String, Value> {
    let batch = wire::to_batch(record).expect("encode");
    batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().clone(), wire_json(batch.column(i).as_ref(), 0)))
        .collect()
}

fn assert_fields(bind: &BindRequest, expected: &Value, context: &str) {
    let got = record_json(bind.clone());
    let want = expected.as_object().expect("expected fields object");
    let got_names: Vec<&String> = got.keys().collect();
    let want_names: Vec<&String> = want.keys().collect();
    assert_eq!(got_names, want_names, "{context}: BindRequest columns");
    for (name, value) in want {
        assert_eq!(&got[name], value, "{context}: BindRequest.{name}");
    }
}

fn decode<R: VgiArrow>(bytes: &[u8]) -> R {
    wire::from_batch(&ipc::read_batch(bytes).expect("ipc")).expect("decode")
}

fn encode<R: VgiArrow>(record: R) -> Vec<u8> {
    ipc::write_batch(&wire::to_batch(record).expect("encode")).expect("ipc")
}

/// The `field` column of `record` re-encoded.
fn reencoded<R: VgiArrow>(record: R, field: &str) -> Vec<u8> {
    let batch = wire::to_batch(record).expect("encode");
    let col = batch.column_by_name(field).expect("column");
    col.as_any()
        .downcast_ref::<BinaryArray>()
        .expect("binary")
        .value(0)
        .to_vec()
}

fn check_embedding<R: VgiArrow + Clone>(
    file: &str,
    field: &str,
    nested: for<'a> fn(&'a R) -> &'a Nested<BindRequest>,
    access: for<'a> fn(&'a R) -> vgi_rpc::Result<&'a BindRequest>,
) {
    let data = read(file);
    let held = read("bind_call.bin");
    let expected = read_json("expected.json");

    // Re-encode without accessing: the held bytes, and still not decoded.
    let record: R = decode(&data);
    assert!(
        !nested(&record).is_decoded(),
        "{file}: decoding the request decoded {field}"
    );
    assert_eq!(nested(&record).bytes(), &held[..], "{file}: held bytes");
    assert_eq!(
        reencoded(record.clone(), field),
        held,
        "{file}: re-encoded {field} without access"
    );
    assert!(
        !nested(&record).is_decoded(),
        "{file}: encoding decoded {field}"
    );

    // Access, compare with expected.json, re-encode: still the held bytes.
    let bind = access(&record).unwrap_or_else(|e| panic!("{file}: {e}"));
    assert_fields(bind, &expected["fields"], file);
    assert!(nested(&record).is_decoded());
    assert_eq!(
        reencoded(record.clone(), field),
        held,
        "{file}: re-encoded {field} after access"
    );

    // A whole IPC round trip keeps the bytes too.
    let again: R = decode(&encode(record));
    assert_eq!(nested(&again).bytes(), &held[..], "{file}: IPC round trip");
}
"""


#: rustfmt's default `max_width` (vgi-rust CI: `cargo fmt --all -- --check`).
_WIDTH = 100
#: rustfmt's default `fn_call_width`: longer argument lists go one per line.
_FN_CALL_WIDTH = 60


def _call(head: str, args: list[str], indent: int, end: str = "") -> str:
    """A call as rustfmt lays it out: one line if it fits, else one argument per line.

    Macros (``head`` ending in ``!``) get no trailing comma, as rustfmt leaves them.
    """
    pad = " " * indent
    joined = ", ".join(args)
    one = f"{pad}{head}({joined}){end}"
    if len(joined) <= _FN_CALL_WIDTH and len(one) <= _WIDTH:
        return one + "\n"
    comma = "" if head.endswith("!") else ","
    body = ",\n".join(f"{pad}    {a}" for a in args)
    return f"{pad}{head}(\n{body}{comma}\n{pad}){end}\n"


def _embedding_structs() -> list[tuple[rust_types._Struct, rust_types._Field]]:
    out = []
    for s in rust_types.build_model():
        nested = [f for f in s.fields if f.nested == "BindRequest"]
        if len(nested) > 1:
            raise GeneratorError(f"{s.name}: more than one nested BindRequest field")
        if nested:
            if nested[0].nullable:
                raise GeneratorError(f"{s.name}.{nested[0].wire_name}: a nullable bind_call is not expected")
            out.append((s, nested[0]))
    if not out:
        raise GeneratorError("no generated record embeds a nested BindRequest")
    return out


def _body() -> str:
    structs = {s.name: s for s in rust_types.build_model()}
    embedding = _embedding_structs()
    w = io.StringIO()
    w.write("// Copyright 2025, 2026 Query Farm LLC - https://query.farm\n")
    w.write(_PRELUDE)

    # Field accessors as fn items (closures cannot return borrows of their argument).
    for s, f in embedding:
        fn = f"{snake_case(s.name)}_{f.wire_name}"
        sig = f"fn {fn}(r: &{s.name}) -> &Nested<BindRequest> {{"
        if len(sig) > _WIDTH:
            sig = f"fn {fn}(\n    r: &{s.name},\n) -> &Nested<BindRequest> {{"
        w.write(f"\n{sig}\n    &r.{f.ident}\n}}\n")

    # Test 1: every manifest embedding entry, dispatched by record name.
    w.write("\n#[test]\nfn byte_identity_round_trip() {\n")
    w.write('    let manifest = read_json("manifest.json");\n')
    w.write('    let entries = manifest["embedding"].as_array().expect("embedding");\n')
    w.write(f'    assert_eq!(entries.len(), {len(embedding)}, "one fixture per embedding record");\n')
    w.write("    for entry in entries {\n")
    w.write('        let file = entry["file"].as_str().unwrap();\n')
    w.write('        let field = entry["field"].as_str().unwrap();\n')
    w.write('        match entry["record"].as_str().unwrap() {\n')
    for s, f in embedding:
        fn = f"{snake_case(s.name)}_{f.wire_name}"
        w.write(f'            "{s.name}" => {{\n')
        w.write(f'                assert_eq!(field, "{f.wire_name}");\n')
        w.write(_call(f"check_embedding::<{s.name}>", ["file", "field", fn, f"{s.name}::{f.wire_name}_value"], 16))
        w.write("            }\n")
    w.write("            other => {\n")
    w.write('                panic!("manifest names an embedding record this SDK does not generate: {other}")\n')
    w.write("            }\n")
    w.write("        }\n    }\n}\n")

    init = next(s for s, _ in embedding if s.name == "InitRequest")
    init_field = next(f for s, f in embedding if s.name == "InitRequest")
    bind_struct = structs["BindRequest"]
    typed_bind = _literal(
        bind_struct,
        {
            "function_name": '"typed_construction".to_string()',
            "arguments": "Some(Bytes(vec![1, 2, 3]))",
            "function_type": 'DictString("TABLE".to_string())',
            "attach_opaque_data": 'Some(Bytes(b"attach".to_vec()))',
            "resolved_secrets_provided": "true",
            "at_unit": 'Some("VERSION".to_string())',
            "schema_path": 'Some(vec!["main".to_string(), "s".to_string()])',
            "argument_names": 'Some(vec![Some("a".to_string()), None])',
        },
        "    ",
    )
    init_literal = _literal(
        init,
        {
            init_field.wire_name: "wrapped.clone()",
            "output_schema": 'Bytes(b"schema".to_vec())',
            "row_limit": "Some(10)",
        },
        "    ",
    )

    # Rule 3: a malformed nested value fails at access, naming the path.
    bad_init = _literal(init, {init_field.wire_name: 'Nested::from_bytes(&b"not arrow"[..])'}, "    ")
    w.write("\n#[test]\nfn decode_errors_surface_at_access_with_the_field_path() {\n")
    w.write(f"    let record = {bad_init};\n")
    w.write("    let decoded: InitRequest = decode(&encode(record));\n")
    w.write("    let err = decoded\n")
    w.write(f"        .{init_field.wire_name}_value()\n")
    w.write('        .expect_err("garbage must not decode");\n')
    w.write("    assert!(\n")
    w.write(f'        err.message.contains("InitRequest.{init_field.wire_name}"),\n')
    w.write('        "{}",\n        err.message\n    );\n')
    w.write("    let empty: Nested<BindRequest> = Nested::from_bytes(Vec::new());\n")
    w.write("    assert!(\n        empty.get().is_err(),\n")
    w.write('        "an empty nested value is a decode error at access"\n    );\n')
    w.write("}\n")

    # Test 2: typed construction.
    w.write("\n#[test]\nfn typed_construction_round_trips() {\n")
    w.write(f"    let bind = {typed_bind};\n")
    w.write('    let wrapped = Nested::from_record(bind.clone()).expect("serialize");\n')
    w.write(f"    let init = {init_literal};\n")
    w.write("    let first = encode(init.clone());\n")
    w.write("    let second = encode(init);\n")
    w.write('    assert_eq!(first, second, "encoding the same wrapper twice");\n')
    w.write("    let decoded: InitRequest = decode(&first);\n")
    held_args = [f"decoded.{init_field.ident}.bytes()", "wrapped.bytes()", '"held bytes survive"']
    w.write(_call("assert_eq!", held_args, 4, ";"))
    w.write(f'    let got = decoded.{init_field.wire_name}_value().expect("access");\n')
    w.write(_call("assert_eq!", ["record_json(got.clone())", "record_json(bind.clone())", '"fields equal"'], 4, ";"))
    w.write('    assert_eq!(got.function_name, "typed_construction");\n')
    w.write("    assert_eq!(\n")
    w.write('        Nested::from_record(bind).expect("serialize").bytes(),\n')
    w.write("        wrapped.bytes(),\n")
    w.write('        "serializing the same record is deterministic",\n')
    w.write("    );\n")
    w.write("}\n")

    # Test 3: null arguments and the exactly-one rule.
    w.write(r"""
#[test]
fn null_arguments_and_exactly_one() {
    let manifest = read_json("manifest.json");
    for entry in manifest["binds"].as_array().expect("binds") {
        let file = entry["file"].as_str().unwrap();
        let bind: BindRequest = decode(&read(file));
        if entry["valid"].as_bool().unwrap() {
            assert!(
                bind.arguments.is_none(),
                "{file}: arguments must decode as None, not empty"
            );
            let location = bind
                .arguments_location
                .as_ref()
                .expect("arguments_location");
            let want = &entry["arguments_location"];
            assert_eq!(
                hex(&location.r#ref.0),
                want["ref"]["hex"].as_str().unwrap(),
                "{file}: ref"
            );
            assert_eq!(
                location.size,
                want["size"].as_i64().unwrap(),
                "{file}: size"
            );
            assert_eq!(
                location.sha256,
                want["sha256"].as_str().unwrap(),
                "{file}: sha256"
            );
            bind.validate_arguments()
                .unwrap_or_else(|e| panic!("{file}: {e}"));
            assert_fields(&bind, &entry["fields"], file);
        } else {
            let err = bind.validate_arguments().expect_err(file);
            assert_eq!(
                err.error_kind(),
                entry["error_kind"].as_str().unwrap(),
                "{file}: kind"
            );
            assert_eq!(err.code(), Code::InvalidArgument, "{file}: code");
            assert_eq!(
                err.message,
                manifest["error_message"].as_str().unwrap(),
                "{file}: message"
            );
        }
    }
}
""")
    return w.getvalue()


def emit(out: TextIO) -> None:
    """Emit the Rust nested-binary conformance test to *out*."""
    body = _body()
    out.write(
        provenance_comment(
            generator_module="vgi.codegen.rust_nested_test",
            generator_command="python -m vgi.codegen.rust_nested_test",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=[
                "uv run --project ~/Development/vgi-python python scripts/regen_generated.py",
            ],
            body=body,
        )
    )
    out.write("\n")
    out.write(body)


def main() -> None:
    """Console-script entrypoint — write the test to stdout."""
    try:
        emit(sys.stdout)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
