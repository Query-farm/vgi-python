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
    r"export const (\w+)Schema = schema\("
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
        "null()": pa.null(),
        "bool()": pa.bool_(),
        "int8()": pa.int8(),
        "int16()": pa.int16(),
        "int32()": pa.int32(),
        "int64()": pa.int64(),
        "uint8()": pa.uint8(),
        "uint16()": pa.uint16(),
        "uint32()": pa.uint32(),
        "uint64()": pa.uint64(),
        "float16()": pa.float16(),
        "float32()": pa.float32(),
        "float64()": pa.float64(),
        "utf8()": pa.string(),
        "binary()": pa.binary(),
    }
    if expr in simple:
        return simple[expr]

    # list(field("item", T, nullable))
    if expr.startswith("list(") and expr.endswith(")"):
        return pa.list_(_parse_field(expr[len("list(") : -1]))

    # map(field("key", K, n), field("value", V, n), keysSorted)
    if expr.startswith("map(") and expr.endswith(")"):
        parts = _split_top_level_comma(expr[len("map(") : -1])
        key_field = _parse_field(parts[0])
        value_field = _parse_field(parts[1])
        keys_sorted = len(parts) >= 3 and parts[2].strip() == "true"
        return cast(pa.DataType, pa.map_(key_field.type, value_field.type, keys_sorted))

    # dictionary(valueType, indexType)
    if expr.startswith("dictionary(") and expr.endswith(")"):
        parts = _split_top_level_comma(expr[len("dictionary(") : -1])
        value = _parse_type(parts[0])
        index = _parse_type(parts[1])
        return cast(pa.DataType, pa.dictionary(index, value))

    # struct([field(...), ...])
    if expr.startswith("struct(") and expr.endswith(")"):
        inside = expr[len("struct(") : -1]
        if inside.startswith("[") and inside.endswith("]"):
            inside = inside[1:-1]
        children = [_parse_field(p) for p in _split_top_level_comma(inside)]
        return pa.struct(children)

    # timestamp(TimeUnit.MICROSECOND[, "tz"])
    if expr.startswith("timestamp(") and expr.endswith(")"):
        parts = _split_top_level_comma(expr[len("timestamp(") : -1])
        unit = {
            "TimeUnit.SECOND": "s",
            "TimeUnit.MILLISECOND": "ms",
            "TimeUnit.MICROSECOND": "us",
            "TimeUnit.NANOSECOND": "ns",
        }[parts[0].strip()]
        tz = parts[1].strip() if len(parts) >= 2 else ""
        if tz.startswith('"') and tz.endswith('"'):
            tz = tz[1:-1]
        return cast(pa.DataType, pa.timestamp(unit, tz=tz or None))

    raise AssertionError(f"cannot parse generated type expression: {expr!r}")


_FIELD_RE = re.compile(
    r'field\(\s*"(?P<name>[^"]+)"\s*,\s*(?P<type>.+)\s*,\s*(?P<nullable>true|false)\s*\)',
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
