# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Drift + determinism tests for `vgi.codegen.java_types`.

vgi-java checks in one generated record per entry of
``java_types.JAVA_RECORDS`` under ``vgi/src/main/java/farm/query/vgi/protocol/``;
each must match what the generator emits right now. Like the C# types, the
comparison is textual: the file is what vgi-java compiles, and vgi-rpc-java
derives the wire schema from it.

A second test closes the loop the textual one cannot: it reads the generated
component declarations back into an Arrow schema using vgi-rpc-java's
``SchemaDerivation`` rules and compares that with the record's ``ARROW_SCHEMA``,
so a mapping bug in the generator fails here rather than at the C++ client.

Skipped when the `vgi-java` checkout isn't present. It lives at ``~/vgi-java``
for some checkouts and beside the other SDKs for others; ``VGI_JAVA_ROOT``
overrides both.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from vgi.codegen import java_types
from vgi.codegen._common import GeneratorError

_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python python scripts/regen_generated.py\n"
    "\n"
    "Do not redirect a generator into its destination with `>`: the shell\n"
    "truncates the file before the generator runs, so any failure destroys\n"
    "the checked-in artifact. The script renders to memory first."
)


def _vgi_java_root() -> Path:
    override = os.environ.get("VGI_JAVA_ROOT")
    if override:
        return Path(override)
    # Same order as scripts/regen_generated.py: beside vgi-python first, then ~.
    candidates = [Path(__file__).resolve().parents[2] / "vgi-java", Path.home() / "vgi-java"]
    for candidate in candidates:
        if (candidate / java_types.JAVA_PROTOCOL_DIR).is_dir():
            return candidate
    return candidates[0]


_RECORDS = java_types.record_names()


@pytest.mark.parametrize("record", _RECORDS)
def test_generator_is_deterministic(record: str) -> None:
    """Rendering a record twice produces byte-identical output."""
    assert java_types.render(record) == java_types.render(record)


@pytest.mark.parametrize(("record", "relative"), java_types.targets())
def test_checked_in_java_matches_generator(record: str, relative: str) -> None:
    """Drift check: the checked-in Java record matches what the generator produces."""
    path = _vgi_java_root() / relative
    if not path.parent.is_dir():
        pytest.skip(f"{path.parent} not found; set VGI_JAVA_ROOT or check out vgi-java")
    assert path.exists(), f"{path} is missing.\n{_REGEN_HINT}"
    assert path.read_text() == java_types.render(record), f"{path} is stale.\n{_REGEN_HINT}"


# ---------------------------------------------------------------------------
# Generated declaration -> Arrow schema, by vgi-rpc-java's SchemaDerivation rules
# ---------------------------------------------------------------------------

_INFERRED: dict[str, pa.DataType] = {
    "boolean": pa.bool_(),
    "Boolean": pa.bool_(),
    "long": pa.int64(),
    "Long": pa.int64(),
    "double": pa.float64(),
    "Double": pa.float64(),
    "String": pa.string(),
    "byte[]": pa.binary(),
}

_OVERRIDES: dict[str, pa.DataType] = {
    "DICT_INT16_UTF8": pa.dictionary(pa.int16(), pa.string()),
    "LARGE_BINARY": pa.large_binary(),
}

_COMPONENT_RE = re.compile(
    r"^\s+((?:@\w+(?:\([^)]*\))? )*)([\w\[\]<>, ]+?) (\w+)(?:,|\) implements ArrowSerializableRecord \{)$"
)


def _derive(java_type: str, annotations: str) -> pa.DataType:
    override = re.search(r"@ArrowField\(ArrowFieldType\.(\w+)\)", annotations)
    if override:
        return _OVERRIDES[override.group(1)]
    if java_type.startswith("List<"):
        element = java_type[5:-1]
        if element in _INFERRED:
            return pa.list_(pa.field("item", _INFERRED[element], nullable=True))
        # A nested generated record derives as an inline struct of its components.
        nested = _derived_schema(java_types.render(element))
        return pa.list_(pa.field("item", pa.struct(list(nested)), nullable=True))
    if java_type.startswith("Map<"):
        key, value = (t.strip() for t in java_type[4:-1].split(","))
        return pa.map_(_INFERRED[key], _INFERRED[value])
    return _INFERRED[java_type]


def _derived_schema(text: str) -> pa.Schema:
    fields: list[pa.Field[Any]] = []
    in_record = False
    for line in text.splitlines():
        if line.startswith("public record "):
            in_record = True
            continue
        if not in_record:
            continue
        match = _COMPONENT_RE.match(line)
        if match is None:
            break
        annotations, java_type, name = match.groups()
        fields.append(pa.field(name, _derive(java_type, annotations), nullable="@Nullable" in annotations))
    return pa.schema(fields)


@pytest.mark.parametrize("record", _RECORDS)
def test_generated_declaration_derives_the_protocol_schema(record: str) -> None:
    """vgi-rpc-java's derivation of the generated record is the protocol's ARROW_SCHEMA."""
    cls = next(c for c in java_types.JAVA_RECORDS if java_types.java_name(c) == record)
    derived = _derived_schema(java_types.render(record))
    expected: pa.Schema = cls.ARROW_SCHEMA  # type: ignore[attr-defined]
    assert derived.equals(expected, check_metadata=False), f"{record}:\n  derived {derived}\n  expected {expected}"


def test_unsupported_arrow_type_is_refused() -> None:
    """A shape vgi-rpc-java cannot derive raises instead of emitting a lookalike."""
    with pytest.raises(GeneratorError):
        java_types._component(pa.field("t", pa.timestamp("us")), "X")
    with pytest.raises(GeneratorError):
        java_types._component(pa.field("l", pa.list_(pa.field("item", pa.binary(), nullable=False))), "X")
