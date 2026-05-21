# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Drift + determinism tests for `vgi.codegen.ts_schemas`.

Enforces that `vgi-typescript/src/generated/vgi-protocol-schemas.ts` in the
sibling repo matches what the generator would emit right now. When they
fail, the error message prints the regeneration command.

If the `vgi-typescript` repo isn't present next to `vgi-python`, the drift
test is skipped.
"""

from __future__ import annotations

import io
import os
import re
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from vgi.codegen._common import collect_schemas
from vgi.codegen.ts_schemas import emit


def _vgi_ts_generated_path() -> Path:
    override = os.environ.get("VGI_TS_GENERATED_TS")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "vgi-typescript" / "src" / "generated" / "vgi-protocol-schemas.ts"


_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python vgi-gen-ts-schemas \\\n"
    "    > ~/Development/vgi-typescript/src/generated/vgi-protocol-schemas.ts"
)


def test_generator_is_deterministic() -> None:
    """Calling emit() twice produces byte-identical output."""
    out1 = io.StringIO()
    emit(out1)
    out2 = io.StringIO()
    emit(out2)
    assert out1.getvalue() == out2.getvalue(), "ts_schemas generator is non-deterministic"


# Match one exported Schema const. Body (group 2) is empty for `new Schema([])`
# or the inner Field list for non-empty schemas.
_SCHEMA_RE = re.compile(
    r"export const (\w+)Schema = new Schema\("
    r"(?:\[\]"
    r"|\[\n(.*?)\n\]"
    r")\);",
    re.DOTALL,
)


def _strip_any_casts(expr: str) -> str:
    """Remove `as any` fragments the emitter adds for arrow-js generic typing."""
    return re.sub(r"\s+as any", "", expr)


def _split_top_level_comma(expr: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    start = 0
    for i, ch in enumerate(expr):
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(expr[start:i])
            start = i + 1
    parts.append(expr[start:])
    return [p.strip() for p in parts if p.strip()]


def _parse_type(expr: str) -> pa.DataType:
    expr = _strip_any_casts(expr).strip()

    simple: dict[str, pa.DataType] = {
        "new Null()": pa.null(),
        "new Bool()": pa.bool_(),
        "new Int8()": pa.int8(),
        "new Int16()": pa.int16(),
        "new Int32()": pa.int32(),
        "new Int64()": pa.int64(),
        "new Uint8()": pa.uint8(),
        "new Uint16()": pa.uint16(),
        "new Uint32()": pa.uint32(),
        "new Uint64()": pa.uint64(),
        "new Float32()": pa.float32(),
        "new Float64()": pa.float64(),
        "new Utf8()": pa.string(),
        "new Binary()": pa.binary(),
    }
    if expr in simple:
        return simple[expr]

    # new List(new Field("item", T, nullable))
    if expr.startswith("new List(") and expr.endswith(")"):
        inner = expr[len("new List(") : -1]
        field = _parse_field(inner)
        return pa.list_(field)

    # new Map_(new Field("entries", new Struct([key,value]), false), <keys_sorted>)
    if expr.startswith("new Map_(") and expr.endswith(")"):
        inside = expr[len("new Map_(") : -1]
        parts = _split_top_level_comma(inside)
        entries_field = _parse_field(parts[0])
        keys_sorted = False
        if len(parts) >= 2:
            keys_sorted = parts[1].strip() == "true"
        struct_type = entries_field.type
        assert isinstance(struct_type, pa.StructType)
        key_field = struct_type.field(0)
        value_field = struct_type.field(1)
        if keys_sorted:
            return cast(pa.DataType, pa.map_(key_field.type, value_field.type, True))
        return cast(pa.DataType, pa.map_(key_field.type, value_field.type))

    # new Dictionary(valueType, indexType[, id, ordered])
    if expr.startswith("new Dictionary(") and expr.endswith(")"):
        inside = expr[len("new Dictionary(") : -1]
        parts = _split_top_level_comma(inside)
        value = _parse_type(parts[0])
        index = _parse_type(parts[1])
        ordered = False
        if len(parts) >= 4:
            ordered = parts[3].strip() == "true"
        return cast(
            pa.DataType,
            pa.dictionary(index, value, ordered=ordered),
        )

    # new Struct([...fields])
    if expr.startswith("new Struct(") and expr.endswith(")"):
        inside = expr[len("new Struct(") : -1]
        if inside.startswith("[") and inside.endswith("]"):
            inside = inside[1:-1]
        children = [_parse_field(p) for p in _split_top_level_comma(inside)]
        return pa.struct(children)

    raise AssertionError(f"cannot parse generated type expression: {expr!r}")


_FIELD_RE = re.compile(
    r'new Field\(\s*"(?P<name>[^"]+)"\s*,\s*(?P<type>.+)\s*,\s*(?P<nullable>true|false)\s*\)',
    re.DOTALL,
)


def _parse_field(expr: str) -> pa.Field[Any]:
    expr = _strip_any_casts(expr).strip()
    if expr.endswith(","):
        expr = expr[:-1].strip()
    m = _FIELD_RE.fullmatch(expr)
    if not m:
        raise AssertionError(f"cannot parse generated field expression: {expr!r}")
    return pa.field(
        m.group("name"),
        _parse_type(m.group("type")),
        nullable=m.group("nullable") == "true",
    )


def _parse_generated_ts(text: str) -> dict[str, pa.Schema]:
    result: dict[str, pa.Schema] = {}
    for match in _SCHEMA_RE.finditer(text):
        name = match.group(1)
        body = match.group(2)
        if body is None:
            result[name] = pa.schema([])
            continue
        field_exprs = _split_top_level_comma(body)
        result[name] = pa.schema([_parse_field(e) for e in field_exprs])
    return result


def test_checked_in_generated_ts_matches_generator() -> None:
    """Drift check: checked-in generated TS matches what the generator produces."""
    path = _vgi_ts_generated_path()
    if not path.exists():
        pytest.skip(f"{path} not found; set VGI_TS_GENERATED_TS or check out vgi-typescript next to vgi-python")

    actual = _parse_generated_ts(path.read_text())
    expected = {es.name: es.schema for es in collect_schemas()}

    missing = set(expected) - set(actual)
    extra = set(actual) - set(expected)
    assert not missing, f"checked-in .ts is missing schemas: {sorted(missing)}\n{_REGEN_HINT}"
    assert not extra, f"checked-in .ts has stale schemas no longer in the Protocol: {sorted(extra)}\n{_REGEN_HINT}"

    for name, expected_schema in expected.items():
        if not expected_schema.equals(actual[name], check_metadata=False):
            raise AssertionError(
                f"schema '{name}' in checked-in .ts differs from generator output.\n"
                f"  expected: {expected_schema}\n"
                f"  actual:   {actual[name]}\n"
                f"{_REGEN_HINT}",
            )


def test_parser_roundtrip_self_test() -> None:
    """Self-test: the local parser round-trips the generator's own output."""
    buf = io.StringIO()
    emit(buf)
    parsed = _parse_generated_ts(buf.getvalue())
    expected = {es.name: es.schema for es in collect_schemas()}
    assert set(parsed) == set(expected), "parser missed a factory the generator emitted"
    for name, schema in expected.items():
        assert schema.equals(parsed[name], check_metadata=False), (
            f"parser round-trip broke schema '{name}': expected {schema}, got {parsed[name]}"
        )
