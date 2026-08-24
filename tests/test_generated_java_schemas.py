# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Drift + determinism tests for `vgi.codegen.java_schemas`.

Enforces that
``vgi-java/vgi/src/test/java/farm/query/vgi/generated/VgiProtocolSchemas.java``
matches what the generator would emit right now. When they fail, the error
message prints the regeneration command.

The Java file is a TEST artifact, unlike the C++ / Go / Rust / TypeScript ones —
vgi-java derives its wire schemas from its record declarations and has no
runtime use for a generated schema. Its job is to be the second description
those declarations are checked against, which only works while it stays in step
with the protocol; hence this test.

Skipped when the `vgi-java` checkout isn't present. It lives at
``~/vgi-java`` rather than beside the other SDKs, so the default search covers
both locations and ``VGI_JAVA_GENERATED_JAVA`` overrides either.
"""

from __future__ import annotations

import io
import os
import re
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from vgi.codegen._common import EXTRA_RESPONSE_TYPES, collect_schemas
from vgi.codegen.java_schemas import JAVA_EXTRA_TYPES, emit

_RELATIVE = Path("vgi") / "src" / "test" / "java" / "farm" / "query" / "vgi" / "generated" / "VgiProtocolSchemas.java"


def _vgi_java_generated_path() -> Path:
    override = os.environ.get("VGI_JAVA_GENERATED_JAVA")
    if override:
        return Path(override)
    candidates = [
        Path.home() / "vgi-java" / _RELATIVE,
        Path(__file__).resolve().parents[2] / "vgi-java" / _RELATIVE,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python python -m vgi.codegen.java_schemas \\\n"
    "    > ~/vgi-java/vgi/src/test/java/farm/query/vgi/generated/VgiProtocolSchemas.java"
)


def _expected() -> dict[str, pa.Schema]:
    return {
        es.name: es.schema for es in collect_schemas(extra_response_types=(*EXTRA_RESPONSE_TYPES, *JAVA_EXTRA_TYPES))
    }


def test_generator_is_deterministic() -> None:
    """Calling emit() twice produces byte-identical output."""
    out1 = io.StringIO()
    emit(out1)
    out2 = io.StringIO()
    emit(out2)
    assert out1.getvalue() == out2.getvalue(), "java_schemas generator is non-deterministic"


# One `private static Schema xxx() { return new Schema(List.of(...)); }` method.
_SCHEMA_RE = re.compile(
    r"private static Schema (\w+)\(\) \{\s*return new Schema\(List\.of\((.*?)\)\);\s*\}",
    re.DOTALL,
)

# The name -> method map at the bottom, which is what Java actually reads.
_ENTRY_RE = re.compile(r'Map\.entry\("([^"]+)", (\w+)\(\)\)')


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
    "new ArrowType.Bool()": pa.bool_(),
    "new ArrowType.Int(8, true)": pa.int8(),
    "new ArrowType.Int(16, true)": pa.int16(),
    "new ArrowType.Int(32, true)": pa.int32(),
    "new ArrowType.Int(64, true)": pa.int64(),
    "new ArrowType.Int(8, false)": pa.uint8(),
    "new ArrowType.Int(16, false)": pa.uint16(),
    "new ArrowType.Int(32, false)": pa.uint32(),
    "new ArrowType.Int(64, false)": pa.uint64(),
    "new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)": pa.float32(),
    "new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)": pa.float64(),
    "new ArrowType.Utf8()": pa.string(),
    "new ArrowType.LargeUtf8()": pa.large_string(),
    "new ArrowType.Binary()": pa.binary(),
    "new ArrowType.LargeBinary()": pa.large_binary(),
}

_TIMESTAMP_RE = re.compile(r"new ArrowType\.Timestamp\(TimeUnit\.(\w+), (null|\"[^\"]*\")\)")
_UNITS = {"SECOND": "s", "MILLISECOND": "ms", "MICROSECOND": "us", "NANOSECOND": "ns"}


def _parse_type(expr: str, children: list[pa.Field[Any]]) -> pa.DataType:
    expr = expr.strip()
    if expr in _SIMPLE_TYPE:
        return _SIMPLE_TYPE[expr]
    ts = _TIMESTAMP_RE.fullmatch(expr)
    if ts:
        tz = None if ts.group(2) == "null" else ts.group(2)[1:-1]
        return cast(pa.DataType, pa.timestamp(_UNITS[ts.group(1)], tz=tz))
    if expr == "new ArrowType.List()":
        return pa.list_(children[0])
    if expr == "new ArrowType.LargeList()":
        return pa.large_list(children[0])
    if expr == "new ArrowType.Struct()":
        return pa.struct(children)
    if expr == "new ArrowType.Map(false)":
        # Arrow-Java models a map as one `entries` struct child; pyarrow as
        # key/item. Re-fold so the comparison is against the pyarrow shape.
        entries = children[0]
        key = entries.type.field(0)
        value = entries.type.field(1)
        return cast(pa.DataType, pa.map_(key.type, value.type))
    raise AssertionError(f"cannot parse generated Java type expression: {expr!r}")


_DICT_RE = re.compile(r"new DictionaryEncoding\(0L, (true|false), (new ArrowType\.Int\(\d+, \w+\))\)")


def _parse_field(expr: str) -> pa.Field[Any]:
    expr = expr.strip()
    is_dict = expr.startswith("dict(")
    prefix = "dict(" if is_dict else "f("
    if not (expr.startswith(prefix) and expr.endswith(")")):
        raise AssertionError(f"cannot parse generated Java field expression: {expr!r}")
    parts = _split_top_level_comma(expr[len(prefix) : -1])

    name = parts[0].strip()[1:-1]
    nullable = parts[1].strip() == "true"
    type_expr = parts[2]
    if is_dict:
        enc = _DICT_RE.fullmatch(parts[3].strip())
        assert enc, f"cannot parse dictionary encoding: {parts[3]!r}"
        value_type = _parse_type(type_expr, [])
        index_type = _SIMPLE_TYPE[enc.group(2)]
        dtype: pa.DataType = cast(
            pa.DataType,
            pa.dictionary(index_type, value_type, ordered=enc.group(1) == "true"),
        )
        return pa.field(name, dtype, nullable=nullable)

    children = [_parse_field(p) for p in parts[3:]]
    return pa.field(name, _parse_type(type_expr, children), nullable=nullable)


def _parse_generated_java(text: str) -> dict[str, pa.Schema]:
    by_method: dict[str, pa.Schema] = {}
    for match in _SCHEMA_RE.finditer(text):
        body = match.group(2).strip()
        fields = [_parse_field(e) for e in _split_top_level_comma(body)] if body else []
        by_method[match.group(1)] = pa.schema(fields)

    # An empty schema is emitted as `new Schema(List.of());`, which the regex
    # above does not match — pick those up so a schema losing every field reads
    # as an empty schema rather than as a schema that vanished.
    for empty in re.finditer(r"private static Schema (\w+)\(\) \{\s*return new Schema\(List\.of\(\)\);", text):
        by_method.setdefault(empty.group(1), pa.schema([]))

    result: dict[str, pa.Schema] = {}
    for name, method in _ENTRY_RE.findall(text):
        assert method in by_method, f"map entry '{name}' names missing method {method}()"
        result[name] = by_method[method]
    return result


def test_checked_in_generated_java_matches_generator() -> None:
    """Drift check: the checked-in Java matches what the generator produces."""
    path = _vgi_java_generated_path()
    if not path.exists():
        pytest.skip(f"{path} not found; set VGI_JAVA_GENERATED_JAVA or check out vgi-java")

    actual = _parse_generated_java(path.read_text())
    expected = _expected()

    missing = set(expected) - set(actual)
    extra = set(actual) - set(expected)
    assert not missing, f"checked-in .java is missing schemas: {sorted(missing)}\n{_REGEN_HINT}"
    assert not extra, f"checked-in .java has stale schemas no longer in the Protocol: {sorted(extra)}\n{_REGEN_HINT}"

    for name, expected_schema in expected.items():
        if not expected_schema.equals(actual[name], check_metadata=False):
            raise AssertionError(
                f"schema '{name}' in checked-in .java differs from generator output.\n"
                f"  expected: {expected_schema}\n"
                f"  actual:   {actual[name]}\n"
                f"{_REGEN_HINT}",
            )


def test_parser_roundtrip_self_test() -> None:
    """Self-test: the local parser round-trips the generator's own output."""
    buf = io.StringIO()
    emit(buf)
    parsed = _parse_generated_java(buf.getvalue())
    expected = _expected()
    assert set(parsed) == set(expected), "parser missed a schema the generator emitted"
    for name, schema in expected.items():
        assert schema.equals(parsed[name], check_metadata=False), (
            f"parser round-trip broke schema '{name}': expected {schema}, got {parsed[name]}"
        )
