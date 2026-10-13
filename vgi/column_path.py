# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Raw, resolved column identities shared by catalog operations and filters.

Components are canonical field names supplied by the catalog. Dots and quotes
are literal characters. SQL identifier resolution belongs to the engine adapter;
wire helpers must not split, unquote, or case-fold identifiers.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeAlias

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass

# vgi-rpc's runtime Arrow inference resolves TypeAlias but not TypeAliasType.
ColumnPath: TypeAlias = list[str]  # noqa: UP040
ColumnKey: TypeAlias = tuple[str, ...]  # noqa: UP040


def column_path_key(path: Sequence[str]) -> ColumnKey:
    """Validate components and return an immutable, case-preserving identity."""
    if isinstance(path, (str, bytes)) or not isinstance(path, Sequence):
        raise TypeError("column path must be a sequence of raw identifier components")
    if not path:
        raise ValueError("column path must contain at least one component")
    if any(not isinstance(component, str) for component in path):
        raise TypeError("column path components must be strings")
    if any(not component or "\x00" in component for component in path):
        raise ValueError("column path components must be nonempty and contain no NUL")
    return tuple(path)


def resolve_column_path(schema: pa.Schema, path: Sequence[str]) -> pa.Field[Any]:
    """Look up exact field names, rejecting missing, ambiguous or non-struct parents.

    A list element, map key, or union arm is not an implicit field traversal.
    Supporting those operations requires an explicitly defined access operation.
    """
    key = column_path_key(path)
    fields: pa.Schema | pa.StructType = schema
    for index, component in enumerate(key):
        matches = [definition for definition in fields if definition.name == component]
        if len(matches) != 1:
            raise ValueError(f"column path {list(key)!r} is missing or ambiguous at {component!r}")
        definition = matches[0]
        if index < len(key) - 1:
            if not pa.types.is_struct(definition.type):
                raise ValueError(f"column path {list(key)!r} has a non-struct parent {component!r}")
            fields = definition.type
    return definition


@dataclass(frozen=True, kw_only=True)
class RequiredFilterPaths(ArrowSerializableDataclass):
    """AND-of-OR groups of raw column paths required by a table scan.

    A group is satisfied when a present filter path is a component prefix of
    at least one member. Filtering ``["a"]`` satisfies ``["a", "b"]``;
    filtering ``["a.b"]`` does not. An empty outer list means no requirements;
    an empty group is invalid. These are column-presence requirements, not a
    proof that a predicate was enforced remotely or that it restricts any rows.

    Attributes:
        groups: Ordered AND groups, each containing one or more OR alternatives.
    """

    groups: list[list[ColumnPath]] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Reject malformed paths and empty or duplicate alternatives."""
        self._keys()

    def _keys(self) -> list[list[ColumnKey]]:
        if not isinstance(self.groups, list) or any(not isinstance(group, list) for group in self.groups):
            raise TypeError("required filter groups must be lists of path alternatives")
        keys = []
        for group in self.groups:
            if not group:
                raise ValueError("required filter groups must not be empty")
            alternatives = [column_path_key(path) for path in group]
            if len(set(alternatives)) != len(alternatives):
                raise ValueError("required filter group contains duplicate paths")
            keys.append(alternatives)
        return keys

    def validate_schema(self, schema: pa.Schema) -> None:
        """Require every alternative to identify a field in the table schema."""
        for group in self._keys():
            for path in group:
                resolve_column_path(schema, path)

    def is_satisfied_by(self, present: Iterable[Sequence[str]]) -> bool:
        """Check structural prefixes without conflating literal dotted names."""
        requirements = self._keys()
        keys = {column_path_key(path) for path in present}
        return all(
            any(required[:length] in keys for required in group for length in range(1, len(required) + 1))
            for group in requirements
        )

    @classmethod
    def from_legacy(cls, groups: list[list[str]], *, schema: pa.Schema) -> RequiredFilterPaths:
        """Resolve legacy dotted strings only when the schema proves one identity.

        This is an explicit migration helper, never a wire-decoding fallback.
        A legacy name matching both a literal dotted column and nested fields
        is rejected so the catalog author can choose the intended components.
        """
        paths: dict[str, list[ColumnPath]] = {}

        def collect(fields: pa.Schema | pa.StructType, prefix: ColumnPath) -> None:
            for definition in fields:
                path = [*prefix, definition.name]
                column_path_key(path)
                paths.setdefault(".".join(path), []).append(path)
                if pa.types.is_struct(definition.type):
                    collect(definition.type, path)

        collect(schema, [])
        if not isinstance(groups, list) or any(not isinstance(group, list) for group in groups):
            raise TypeError("legacy required filter groups must be lists")
        result = []
        for group in groups:
            alternatives = []
            for member in group:
                if not isinstance(member, str):
                    raise TypeError("legacy required filter members must be strings")
                matches = paths.get(member, [])
                if len(matches) != 1:
                    raise ValueError(f"legacy required filter {member!r} is missing or ambiguous; use raw components")
                alternatives.append(matches[0])
            result.append(alternatives)
        return cls(groups=result)
