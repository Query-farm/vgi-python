"""Drift + determinism tests for `vgi.codegen.cpp_schemas`.

These tests enforce that `vgi/src/generated/vgi_protocol_schemas.hpp` in the
sibling `vgi` repo matches what the generator would emit right now. When they
fail, the error message prints the exact regeneration command.

If the `vgi` repo isn't present next to `vgi-python`, the drift test is
skipped (the determinism test still runs — it needs no external repo).
"""

from __future__ import annotations

import io
import os
import re
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from vgi.codegen.cpp_schemas import _collect_schemas, emit


def _vgi_generated_path() -> Path:
    """Locate `vgi/src/generated/vgi_protocol_schemas.hpp` relative to this repo."""
    override = os.environ.get("VGI_GENERATED_HPP")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "vgi" / "src" / "generated" / "vgi_protocol_schemas.hpp"


_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python vgi-gen-cpp-schemas \\\n"
    "    > ~/Development/vgi/src/generated/vgi_protocol_schemas.hpp"
)


def test_generator_is_deterministic() -> None:
    """Running the generator twice produces byte-identical output."""
    out1 = io.StringIO()
    emit(out1)
    out2 = io.StringIO()
    emit(out2)
    assert out1.getvalue() == out2.getvalue(), (
        "generator is non-deterministic — sorting or collection order is unstable"
    )


# Regex matches each generated factory body. The format is highly regular
# (see `_emit_factory` in cpp_schemas.py), so we avoid pulling in a real C++
# parser.
_FACTORY_RE = re.compile(
    r"inline const std::shared_ptr<arrow::Schema> &(\w+)Schema\(\) \{\n"
    r"(?:\tstatic const auto schema = arrow::schema\(\{\}\);\n"
    r"|\tstatic const auto schema = arrow::schema\(\{\n(.*?)\n\t\}\);\n)"
    r"\treturn schema;\n\}",
    re.DOTALL,
)

_FIELD_RE = re.compile(
    r'arrow::field\("(?P<name>[^"]+)", (?P<type>.+?), /\*nullable=\*/(?P<nullable>true|false)\)',
)


def _parse_type(expr: str) -> pa.DataType:
    """Parse a subset of C++ arrow::... expressions back into pa.DataType.

    Supports the whitelisted subset the generator emits. Raises on anything
    unrecognized so the test fails loudly rather than silently pass a broken
    schema.
    """
    expr = expr.strip()
    simple: dict[str, pa.DataType] = {
        "arrow::boolean()": pa.bool_(),
        "arrow::int8()": pa.int8(),
        "arrow::int16()": pa.int16(),
        "arrow::int32()": pa.int32(),
        "arrow::int64()": pa.int64(),
        "arrow::uint8()": pa.uint8(),
        "arrow::uint16()": pa.uint16(),
        "arrow::uint32()": pa.uint32(),
        "arrow::uint64()": pa.uint64(),
        "arrow::float32()": pa.float32(),
        "arrow::float64()": pa.float64(),
        "arrow::utf8()": pa.string(),
        "arrow::binary()": pa.binary(),
        "arrow::null()": pa.null(),
    }
    if expr in simple:
        return simple[expr]

    if expr.startswith("arrow::list(") and expr.endswith(")"):
        inner = expr[len("arrow::list(") : -1]
        return pa.list_(_parse_type(inner))

    if expr.startswith("arrow::map(") and expr.endswith(")"):
        # arrow::map(K, V) — split on the top-level comma.
        inside = expr[len("arrow::map(") : -1]
        k, v = _split_top_level_comma(inside)
        return pa.map_(_parse_type(k), _parse_type(v))

    if expr.startswith("arrow::dictionary(") and expr.endswith(")"):
        # arrow::dictionary(I, V) or arrow::dictionary(I, V, /*ordered=*/true|false).
        # pyarrow's `dictionary` stubs constrain index type narrowly; cast away.
        inside = expr[len("arrow::dictionary(") : -1]
        parts = _split_top_level_comma(inside)
        if len(parts) == 2:
            idx, val = parts
            return cast(pa.DataType, pa.dictionary(_parse_type(idx), _parse_type(val)))
        if len(parts) == 3:
            idx, val, ordered_expr = parts
            ordered = "true" in ordered_expr
            return cast(
                pa.DataType,
                pa.dictionary(_parse_type(idx), _parse_type(val), ordered=ordered),
            )

    if expr.startswith("arrow::struct_({") and expr.endswith("})"):
        inner = expr[len("arrow::struct_({") : -len("})")]
        children = [_parse_field(part) for part in _split_top_level_comma(inner)]
        return pa.struct(children)

    if expr.startswith("arrow::timestamp(") and expr.endswith(")"):
        # arrow::timestamp(arrow::TimeUnit::UNIT) or arrow::timestamp(unit, "tz").
        inside = expr[len("arrow::timestamp(") : -1]
        parts = _split_top_level_comma(inside)
        unit_map = {
            "arrow::TimeUnit::SECOND": "s",
            "arrow::TimeUnit::MILLI": "ms",
            "arrow::TimeUnit::MICRO": "us",
            "arrow::TimeUnit::NANO": "ns",
        }
        unit = unit_map[parts[0].strip()]
        if len(parts) == 1:
            return pa.timestamp(unit)
        if len(parts) == 2:
            tz = parts[1].strip()
            assert tz.startswith('"') and tz.endswith('"'), f"unexpected timezone literal: {tz!r}"
            return pa.timestamp(unit, tz=tz[1:-1])

    raise AssertionError(f"cannot parse generated type expression: {expr!r}")


def _parse_field(expr: str) -> pa.Field[Any]:
    m = _FIELD_RE.fullmatch(expr.strip())
    if not m:
        raise AssertionError(f"cannot parse generated field expression: {expr!r}")
    return pa.field(m.group("name"), _parse_type(m.group("type")), nullable=m.group("nullable") == "true")


def _split_top_level_comma(expr: str) -> list[str]:
    """Split `expr` on commas outside of parentheses/braces."""
    parts: list[str] = []
    depth = 0
    start = 0
    for i, ch in enumerate(expr):
        if ch in "({":
            depth += 1
        elif ch in ")}":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(expr[start:i])
            start = i + 1
    parts.append(expr[start:])
    return [p.strip() for p in parts if p.strip()]


def _parse_generated_hpp(text: str) -> dict[str, pa.Schema]:
    """Extract name → Schema from a generated .hpp file."""
    result: dict[str, pa.Schema] = {}
    for match in _FACTORY_RE.finditer(text):
        name = match.group(1)
        body = match.group(2)
        if body is None:
            result[name] = pa.schema([])
            continue
        # Body is one or more `\t    arrow::field(...)` lines joined by `,\n`.
        field_exprs = _split_top_level_comma(body)
        result[name] = pa.schema([_parse_field(e) for e in field_exprs])
    return result


def test_checked_in_generated_hpp_matches_generator() -> None:
    """The .hpp checked into the vgi repo must match the current generator output."""
    path = _vgi_generated_path()
    if not path.exists():
        pytest.skip(f"{path} not found; set VGI_GENERATED_HPP or check out vgi repo next to vgi-python")

    actual = _parse_generated_hpp(path.read_text())
    expected = {es.name: es.schema for es in _collect_schemas()}

    # Extra or missing factories.
    missing = set(expected) - set(actual)
    extra = set(actual) - set(expected)
    assert not missing, f"checked-in .hpp is missing schemas: {sorted(missing)}\n{_REGEN_HINT}"
    assert not extra, f"checked-in .hpp has stale schemas no longer in the Protocol: {sorted(extra)}\n{_REGEN_HINT}"

    for name, expected_schema in expected.items():
        if not expected_schema.equals(actual[name], check_metadata=False):
            raise AssertionError(
                f"schema '{name}' in checked-in .hpp differs from generator output.\n"
                f"  expected: {expected_schema}\n"
                f"  actual:   {actual[name]}\n"
                f"{_REGEN_HINT}",
            )


def test_parser_roundtrip_self_test() -> None:
    """Sanity: the generator output round-trips through our own parser."""
    buf = io.StringIO()
    emit(buf)
    parsed = _parse_generated_hpp(buf.getvalue())
    expected = {es.name: es.schema for es in _collect_schemas()}
    assert set(parsed) == set(expected), "parser missed a factory the generator emitted"
    for name, schema in expected.items():
        assert schema.equals(parsed[name], check_metadata=False), (
            f"parser round-trip broke schema '{name}': expected {schema}, got {parsed[name]}"
        )
