# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Drift + determinism + round-trip tests for `vgi.codegen.rust_schemas`.

Three layers, mirroring `test_generated_go_schemas.py`:

1. **Determinism** — `emit()` twice must be byte-identical.
2. **Round-trip** — parse the emitted arrow-rs expressions back into pyarrow
   types and compare against `collect_schemas()`. This is the layer that
   catches emitter bugs a byte-compare cannot: a wrong nullability flag, a
   `uint8` rendered as `Int8`, a map whose key/value got swapped.
3. **Drift** — the checked-in `vgi-rust/vgi-protocol/src/generated/protocol_schemas.rs`
   must match what the generator would emit right now. Skipped when the
   sibling `vgi-rust` repo isn't present.

The drift check is a pure byte comparison and needs no Rust toolchain — the
generated file carries `#[rustfmt::skip]` on every item precisely so `cargo fmt`
never rewrites what the generator produced.
"""

from __future__ import annotations

import io
import os
import re
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from vgi.codegen._common import EXTRA_RESPONSE_TYPES, REQUEST_TYPES, collect_schemas
from vgi.codegen.rust_schemas import emit, snake_case


def _vgi_rust_generated_path() -> Path:
    override = os.environ.get("VGI_RUST_GENERATED_RS")
    if override:
        return Path(override)
    return (
        Path(__file__).resolve().parents[2] / "vgi-rust" / "vgi-protocol" / "src" / "generated" / "protocol_schemas.rs"
    )


_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python python -m vgi.codegen.rust_schemas \\\n"
    "    > ~/Development/vgi-rust/vgi-protocol/src/generated/protocol_schemas.rs"
)


def test_generator_is_deterministic() -> None:
    """Calling emit() twice produces byte-identical output."""
    out1 = io.StringIO()
    emit(out1)
    out2 = io.StringIO()
    emit(out2)
    assert out1.getvalue() == out2.getvalue(), "rust_schemas generator is non-deterministic"


# --------------------------------------------------------------------------
# A tiny parser for the arrow-rs expressions the generator emits.
# --------------------------------------------------------------------------


def _balanced(src: str, open_at: int) -> tuple[str, int]:
    """Return the substring inside the bracket at *open_at*, and the index past its close.

    Handles nesting and string literals so a `,` or `)` inside `"..."` doesn't
    terminate the scan early.
    """
    pairs = {"(": ")", "[": "]", "{": "}"}
    opener = src[open_at]
    closer = pairs[opener]
    depth = 0
    i = open_at
    in_str = False
    while i < len(src):
        ch = src[i]
        if in_str:
            if ch == "\\":
                i += 2
                continue
            if ch == '"':
                in_str = False
        elif ch == '"':
            in_str = True
        elif ch == opener:
            depth += 1
        elif ch == closer:
            depth -= 1
            if depth == 0:
                return src[open_at + 1 : i], i + 1
        i += 1
    raise AssertionError(f"unbalanced {opener!r} starting at {open_at}")


def _split_top_level_comma(expr: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    start = 0
    in_str = False
    i = 0
    while i < len(expr):
        ch = expr[i]
        if in_str:
            if ch == "\\":
                i += 2
                continue
            if ch == '"':
                in_str = False
        elif ch == '"':
            in_str = True
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(expr[start:i])
            start = i + 1
        i += 1
    parts.append(expr[start:])
    return [p.strip() for p in parts if p.strip()]


_SIMPLE_TYPE: dict[str, pa.DataType] = {
    "DataType::Boolean": pa.bool_(),
    "DataType::Int8": pa.int8(),
    "DataType::Int16": pa.int16(),
    "DataType::Int32": pa.int32(),
    "DataType::Int64": pa.int64(),
    "DataType::UInt8": pa.uint8(),
    "DataType::UInt16": pa.uint16(),
    "DataType::UInt32": pa.uint32(),
    "DataType::UInt64": pa.uint64(),
    "DataType::Float32": pa.float32(),
    "DataType::Float64": pa.float64(),
    "DataType::Utf8": pa.string(),
    "DataType::LargeUtf8": pa.large_string(),
    "DataType::Binary": pa.binary(),
    "DataType::LargeBinary": pa.large_binary(),
}

_TIME_UNIT = {
    "TimeUnit::Second": "s",
    "TimeUnit::Millisecond": "ms",
    "TimeUnit::Microsecond": "us",
    "TimeUnit::Nanosecond": "ns",
}


def _strip_wrapper(expr: str, prefix: str) -> str | None:
    """If *expr* is `prefix(...)`, return the inside; else None."""
    expr = expr.strip()
    if not expr.startswith(prefix):
        return None
    inner, end = _balanced(expr, len(prefix) - 1)
    assert end == len(expr), f"trailing text after {prefix}: {expr!r}"
    return inner


def _parse_type(expr: str) -> pa.DataType:
    expr = expr.strip()
    if expr in _SIMPLE_TYPE:
        return _SIMPLE_TYPE[expr]

    inner = _strip_wrapper(expr, "DataType::List(")
    if inner is not None:
        arc = _strip_wrapper(inner, "Arc::new(")
        assert arc is not None, inner
        return cast(pa.DataType, pa.list_(_parse_field(arc)))

    inner = _strip_wrapper(expr, "DataType::LargeList(")
    if inner is not None:
        arc = _strip_wrapper(inner, "Arc::new(")
        assert arc is not None, inner
        return cast(pa.DataType, pa.large_list(_parse_field(arc)))

    inner = _strip_wrapper(expr, "DataType::Struct(")
    if inner is not None:
        fields_vec = _strip_wrapper(inner, "Fields::from(")
        assert fields_vec is not None, inner
        items = _strip_wrapper(fields_vec.strip(), "vec![")
        assert items is not None, fields_vec
        return pa.struct([_parse_field(p) for p in _split_top_level_comma(items)])

    inner = _strip_wrapper(expr, "DataType::Dictionary(")
    if inner is not None:
        parts = _split_top_level_comma(inner)
        index = _strip_wrapper(parts[0], "Box::new(")
        value = _strip_wrapper(parts[1], "Box::new(")
        assert index is not None and value is not None, inner
        # arrow-rs cannot express an ordered dictionary; the emitter rejects one.
        return cast(pa.DataType, pa.dictionary(_parse_type(index), _parse_type(value), ordered=False))

    inner = _strip_wrapper(expr, "DataType::Map(")
    if inner is not None:
        parts = _split_top_level_comma(inner)
        entries = _strip_wrapper(parts[0], "Arc::new(")
        assert entries is not None, inner
        entries_field = _parse_field(entries)
        assert entries_field.name == "entries", entries_field
        struct_type = entries_field.type
        keys_sorted = parts[1].strip() == "true"
        return cast(
            pa.DataType,
            # pyarrow-stubs names this parameter ``key_sorted`` and types it as a
            # literal; the runtime keyword is ``keys_sorted`` and takes a plain bool.
            pa.map_(struct_type.field(0).type, struct_type.field(1), keys_sorted=keys_sorted),  # type: ignore[call-overload]
        )

    inner = _strip_wrapper(expr, "DataType::Timestamp(")
    if inner is not None:
        parts = _split_top_level_comma(inner)
        unit = _TIME_UNIT[parts[0].strip()]
        tz_expr = parts[1].strip()
        if tz_expr == "None":
            return cast(pa.DataType, pa.timestamp(unit))
        tz_inner = _strip_wrapper(tz_expr, "Some(")
        assert tz_inner is not None, tz_expr
        tz = tz_inner.strip().removesuffix(".into()").strip().strip('"')
        return cast(pa.DataType, pa.timestamp(unit, tz=tz))

    raise AssertionError(f"test parser doesn't understand Rust type expression: {expr!r}")


def _parse_field(expr: str) -> pa.Field[Any]:
    inner = _strip_wrapper(expr, "Field::new(")
    assert inner is not None, f"not a Field::new(...): {expr!r}"
    parts = _split_top_level_comma(inner)
    assert len(parts) == 3, parts
    name = parts[0].strip().strip('"')
    dtype = _parse_type(parts[1])
    nullable = parts[2].strip() == "true"
    return pa.field(name, dtype, nullable=nullable)


_FN_RE = re.compile(r"pub fn (\w+)_schema\(\) -> SchemaRef \{")


def _parse_generated(src: str) -> dict[str, pa.Schema]:
    """Extract every emitted schema factory as {fn_stem: pyarrow schema}."""
    out: dict[str, pa.Schema] = {}
    for m in _FN_RE.finditer(src):
        stem = m.group(1)
        tail = src[m.end() :]
        marker = "Schema::new("
        idx = tail.index(marker)
        body, _ = _balanced(tail, idx + len(marker) - 1)
        body = body.strip()
        if body == "Vec::<Field>::new()":
            out[stem] = pa.schema([])
            continue
        items = _strip_wrapper(body, "vec![")
        assert items is not None, body
        out[stem] = pa.schema([_parse_field(p) for p in _split_top_level_comma(items)])
    return out


def test_emitted_schemas_round_trip_to_pyarrow() -> None:
    """Every emitted Rust schema parses back to exactly the pyarrow schema it came from."""
    buf = io.StringIO()
    emit(buf)
    parsed = _parse_generated(buf.getvalue())

    expected = {
        snake_case(es.name): es
        for es in collect_schemas(extra_response_types=(*EXTRA_RESPONSE_TYPES, *REQUEST_TYPES))
    }
    assert set(parsed) == set(expected), (
        f"emitted factories differ from collect_schemas(): "
        f"only-in-rust={sorted(set(parsed) - set(expected))}, "
        f"only-in-python={sorted(set(expected) - set(parsed))}"
    )

    for stem, es in expected.items():
        assert parsed[stem].equals(es.schema), (
            f"schema '{es.name}' ({es.origin}) does not round-trip.\n  python: {es.schema}\n  rust:   {parsed[stem]}"
        )


def test_every_rpc_method_has_a_params_entry() -> None:
    """The generated `params_schema_for` dispatch names every method exactly once."""
    from vgi_rpc.rpc._types import rpc_methods

    from vgi.protocol import VgiProtocol

    buf = io.StringIO()
    emit(buf)
    src = buf.getvalue()
    dispatch_start = src.index("pub fn params_schema_for(")
    arms = re.findall(r'^\s+"([a-z0-9_]+)" => Some\(', src[dispatch_start:], re.MULTILINE)

    assert len(arms) == len(set(arms)), "duplicate match arm in params_schema_for"
    assert set(arms) == set(rpc_methods(VgiProtocol)), (
        "params_schema_for dispatch is out of sync with the protocol's method list"
    )


def test_checked_in_rust_matches_generator() -> None:
    """The file committed in vgi-rust must equal what the generator emits now."""
    path = _vgi_rust_generated_path()
    if not path.exists():
        pytest.skip(f"vgi-rust checkout not found at {path}")

    buf = io.StringIO()
    emit(buf)
    expected = buf.getvalue()
    actual = path.read_text(encoding="utf-8")

    assert actual == expected, f"{path} is stale.\n\n{_REGEN_HINT}"
