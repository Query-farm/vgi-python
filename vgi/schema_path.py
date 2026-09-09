# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Qualified schema-path types and helpers."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TypeAlias

# vgi-rpc's runtime Arrow inference resolves TypeAlias but not TypeAliasType.
SchemaPath: TypeAlias = list[str]  # noqa: UP040
SchemaKey: TypeAlias = tuple[str, ...]  # noqa: UP040


def schema_path_key(path: Sequence[str]) -> SchemaKey:
    """Return a case-insensitive structural lookup key for a schema path.

    Args:
        path: Raw identifier components, from outermost to innermost.

    Returns:
        A normalized immutable key that preserves component boundaries.

    Raises:
        ValueError: If the path is empty or contains an empty component.
        TypeError: If any component is not a string.
    """
    if isinstance(path, (str, bytes)):
        raise TypeError("schema_path must be a sequence of identifier components, not a string")
    if not path:
        raise ValueError("schema_path must contain at least one identifier component")
    if any(not isinstance(component, str) for component in path):
        raise TypeError("schema_path components must be strings")
    if any(not component for component in path):
        raise ValueError("schema_path components must not be empty")
    return tuple(component.lower() for component in path)


def schema_path_display(path: Sequence[str]) -> str:
    """Return an unambiguous display form that retains component boundaries."""
    schema_path_key(path)
    return repr(list(path))


def sql_qualified_name(path: Sequence[str], name: str) -> str:
    """Quote a schema path and object name as a DuckDB qualified name."""
    if isinstance(path, (str, bytes)):
        raise TypeError("schema path must be a sequence of identifier components, not a string")
    if not isinstance(name, str):
        raise TypeError("object name must be a string")
    if not name:
        raise ValueError("object name must not be empty")
    components = [*path, name]
    if any(not isinstance(component, str) for component in components):
        raise TypeError("qualified name components must be strings")
    if any(not component for component in components):
        raise ValueError("qualified name components must not be empty")
    return ".".join('"' + component.replace('"', '""') + '"' for component in components)
