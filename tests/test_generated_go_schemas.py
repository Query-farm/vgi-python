# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Drift + determinism tests for `vgi.codegen.go_schemas`.

Enforces that `vgi-go/vgi/generated/protocol_schemas.go` in the sibling
repo matches what the generator would emit right now. When they fail,
the error message prints the regeneration command.

If the `vgi-go` repo isn't present next to `vgi-python`, the drift test
is skipped.
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
from vgi.codegen.go_schemas import emit


def _vgi_go_generated_path() -> Path:
    override = os.environ.get("VGI_GO_GENERATED_GO")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "vgi-go" / "vgi" / "generated" / "protocol_schemas.go"


_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python vgi-gen-go-schemas \\\n"
    "    > ~/Development/vgi-go/vgi/generated/protocol_schemas.go"
)


def test_generator_is_deterministic() -> None:
    """Calling emit() twice produces byte-identical output."""
    out1 = io.StringIO()
    emit(out1)
    out2 = io.StringIO()
    emit(out2)
    assert out1.getvalue() == out2.getvalue(), "go_schemas generator is non-deterministic"


# Match one `var XxxSchema = arrow.NewSchema(...)` declaration. Body (group 2)
# is the inner Field list; an empty schema is `arrow.NewSchema([]arrow.Field{}, nil)`.
_SCHEMA_RE = re.compile(
    r"var (\w+)Schema = arrow\.NewSchema\(\[\]arrow\.Field\{"
    r"(.*?)"
    r"\}, nil\)",
    re.DOTALL,
)


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


_SIMPLE_TYPE: dict[str, pa.DataType] = {
    "&arrow.BooleanType{}": pa.bool_(),
    "arrow.PrimitiveTypes.Int8": pa.int8(),
    "arrow.PrimitiveTypes.Int16": pa.int16(),
    "arrow.PrimitiveTypes.Int32": pa.int32(),
    "arrow.PrimitiveTypes.Int64": pa.int64(),
    "arrow.PrimitiveTypes.Uint8": pa.uint8(),
    "arrow.PrimitiveTypes.Uint16": pa.uint16(),
    "arrow.PrimitiveTypes.Uint32": pa.uint32(),
    "arrow.PrimitiveTypes.Uint64": pa.uint64(),
    "arrow.PrimitiveTypes.Float32": pa.float32(),
    "arrow.PrimitiveTypes.Float64": pa.float64(),
    "arrow.BinaryTypes.String": pa.string(),
    "arrow.BinaryTypes.Binary": pa.binary(),
}


def _parse_type(expr: str) -> pa.DataType:
    expr = expr.strip()
    if expr in _SIMPLE_TYPE:
        return _SIMPLE_TYPE[expr]

    if expr.startswith("arrow.ListOf(") and expr.endswith(")"):
        inner = expr[len("arrow.ListOf(") : -1]
        return pa.list_(_parse_type(inner))

    if expr.startswith("arrow.ListOfField(") and expr.endswith(")"):
        inner = expr[len("arrow.ListOfField(") : -1]
        return pa.list_(_parse_field(inner))

    if expr.startswith("arrow.MapOf(") and expr.endswith(")"):
        inside = expr[len("arrow.MapOf(") : -1]
        parts = _split_top_level_comma(inside)
        return cast(pa.DataType, pa.map_(_parse_type(parts[0]), _parse_type(parts[1])))

    if expr.startswith("&arrow.DictionaryType{") and expr.endswith("}"):
        body = expr[len("&arrow.DictionaryType{") : -1]
        kv = {}
        for part in _split_top_level_comma(body):
            k, _, v = part.partition(":")
            kv[k.strip()] = v.strip()
        index = _parse_type(kv["IndexType"])
        value = _parse_type(kv["ValueType"])
        ordered = kv.get("Ordered", "false") == "true"
        return cast(pa.DataType, pa.dictionary(index, value, ordered=ordered))

    if expr.startswith("arrow.StructOf(") and expr.endswith(")"):
        inside = expr[len("arrow.StructOf(") : -1]
        children = [_parse_field(p) for p in _split_top_level_comma(inside)]
        return pa.struct(children)

    raise AssertionError(f"cannot parse generated Go type expression: {expr!r}")


def _parse_field(expr: str) -> pa.Field[Any]:
    expr = expr.strip()
    if not (expr.startswith("arrow.Field{") and expr.endswith("}")):
        raise AssertionError(f"cannot parse generated Go field expression: {expr!r}")
    body = expr[len("arrow.Field{") : -1]
    parts = _split_top_level_comma(body)
    kv: dict[str, str] = {}
    for part in parts:
        k, _, v = part.partition(":")
        kv[k.strip()] = v.strip()
    name = kv["Name"]
    if name.startswith('"') and name.endswith('"'):
        name = name[1:-1]
    nullable = kv.get("Nullable", "false") == "true"
    return pa.field(name, _parse_type(kv["Type"]), nullable=nullable)


def _parse_generated_go(text: str) -> dict[str, pa.Schema]:
    result: dict[str, pa.Schema] = {}
    for match in _SCHEMA_RE.finditer(text):
        name = match.group(1)
        body = match.group(2).strip()
        if not body:
            result[name] = pa.schema([])
            continue
        field_exprs = _split_top_level_comma(body)
        result[name] = pa.schema([_parse_field(e) for e in field_exprs])
    return result


def test_checked_in_generated_go_matches_generator() -> None:
    """Drift check: checked-in generated Go matches what the generator produces."""
    path = _vgi_go_generated_path()
    if not path.exists():
        pytest.skip(
            f"{path} not found; set VGI_GO_GENERATED_GO or check out vgi-go next to vgi-python",
        )

    actual = _parse_generated_go(path.read_text())
    expected = {es.name: es.schema for es in collect_schemas()}

    missing = set(expected) - set(actual)
    extra = set(actual) - set(expected)
    assert not missing, f"checked-in .go is missing schemas: {sorted(missing)}\n{_REGEN_HINT}"
    assert not extra, f"checked-in .go has stale schemas no longer in the Protocol: {sorted(extra)}\n{_REGEN_HINT}"

    for name, expected_schema in expected.items():
        if not expected_schema.equals(actual[name], check_metadata=False):
            raise AssertionError(
                f"schema '{name}' in checked-in .go differs from generator output.\n"
                f"  expected: {expected_schema}\n"
                f"  actual:   {actual[name]}\n"
                f"{_REGEN_HINT}",
            )


def test_parser_roundtrip_self_test() -> None:
    """Self-test: the local parser round-trips the generator's own output."""
    buf = io.StringIO()
    emit(buf)
    parsed = _parse_generated_go(buf.getvalue())
    expected = {es.name: es.schema for es in collect_schemas()}
    assert set(parsed) == set(expected), "parser missed a factory the generator emitted"
    for name, schema in expected.items():
        assert schema.equals(parsed[name], check_metadata=False), (
            f"parser round-trip broke schema '{name}': expected {schema}, got {parsed[name]}"
        )
