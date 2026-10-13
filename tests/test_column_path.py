# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Structural identities, required-filter semantics and explicit legacy migration."""

from typing import Any

import pyarrow as pa
import pytest

from vgi.column_path import RequiredFilterPaths, column_path_key, resolve_column_path


def _schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("a.b", pa.string()),
            pa.field("a", pa.struct([pa.field("b", pa.int64()), pa.field('c."d', pa.bool_())])),
            pa.field("other", pa.int32()),
        ]
    )


def test_literal_dotted_names_and_nested_names_are_distinct() -> None:
    assert column_path_key(["a.b"]) != column_path_key(["a", "b"])
    assert column_path_key(["A"]) != column_path_key(["a"])
    assert column_path_key(["İ"]) != column_path_key(["i\u0307"])
    assert resolve_column_path(_schema(), ["a.b"]).type == pa.string()
    assert resolve_column_path(_schema(), ["a", "b"]).type == pa.int64()
    assert resolve_column_path(_schema(), ["a", 'c."d']).type == pa.bool_()


@pytest.mark.parametrize("path", ["a.b", b"a", [1], None, {"a"}])
def test_invalid_component_types_reject(path: Any) -> None:
    with pytest.raises(TypeError, match="column path"):
        column_path_key(path)


@pytest.mark.parametrize("path", [[], [""], ["a", ""], ["a\x00b"]])
def test_invalid_components_reject(path: list[str]) -> None:
    with pytest.raises(ValueError, match="column path"):
        column_path_key(path)


@pytest.mark.parametrize("path", [["missing"], ["a", "missing"], ["other", "x"], ["A"]])
def test_resolution_rejects_missing_and_nonstruct_parents(path: list[str]) -> None:
    with pytest.raises(ValueError, match="missing or ambiguous|non-struct"):
        resolve_column_path(_schema(), path)


def test_duplicate_arrow_names_are_ambiguous() -> None:
    schema = pa.schema([pa.field("x", pa.int32()), pa.field("x", pa.int64())])
    with pytest.raises(ValueError, match="ambiguous"):
        resolve_column_path(schema, ["x"])


def test_and_of_or_groups_and_component_prefixes() -> None:
    required = RequiredFilterPaths(groups=[[["a", "b"], ["other"]], [["a.b"]]])
    required.validate_schema(_schema())
    assert required.is_satisfied_by([["other"], ["a.b"]])
    assert required.is_satisfied_by([["a"], ["a.b"]])
    assert not required.is_satisfied_by([["a.b"]])
    assert not required.is_satisfied_by([["a", "b"]])
    assert not required.is_satisfied_by([["a", "c"], ["a.b"]])
    assert not RequiredFilterPaths(groups=[[["a"]]]).is_satisfied_by([["a", "b"]])
    assert RequiredFilterPaths().is_satisfied_by([])


def test_wire_round_trip_retains_groups_and_exact_components() -> None:
    required = RequiredFilterPaths(groups=[[["a", 'c."d'], ["a.b"]], [["other"]]])
    restored = RequiredFilterPaths.deserialize_from_bytes(required.serialize_to_bytes())
    assert restored == required
    assert RequiredFilterPaths.ARROW_SCHEMA.field("groups").type == pa.list_(pa.list_(pa.list_(pa.string())))


@pytest.mark.parametrize("groups", [[[]], [[["a"], ["a"]]], [[[]]], [[[""]]]])
def test_malformed_groups_reject(groups: Any) -> None:
    with pytest.raises(ValueError):
        RequiredFilterPaths(groups=groups)


def test_requirements_must_resolve_before_using_them() -> None:
    required = RequiredFilterPaths(groups=[[["a", "missing"]]])
    with pytest.raises(ValueError, match="missing or ambiguous"):
        required.validate_schema(_schema())


def test_legacy_migration_requires_unambiguous_schema_evidence() -> None:
    with pytest.raises(ValueError, match="ambiguous; use raw components"):
        RequiredFilterPaths.from_legacy([["a.b"]], schema=_schema())
    with pytest.raises(ValueError, match="missing or ambiguous"):
        RequiredFilterPaths.from_legacy([["missing"]], schema=_schema())
    nested = pa.schema([_schema().field("a")])
    assert RequiredFilterPaths.from_legacy([["a.b"]], schema=nested).groups == [[["a", "b"]]]
    literal = pa.schema([_schema().field("a.b")])
    assert RequiredFilterPaths.from_legacy([["a.b"]], schema=literal).groups == [[["a.b"]]]


def test_mutation_cannot_bypass_validation() -> None:
    required = RequiredFilterPaths()
    required.groups.append([])
    with pytest.raises(ValueError, match="must not be empty"):
        required.is_satisfied_by([["a"]])
