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


def single_column_name(path: Sequence[str]) -> str:
    """Adapt one raw component to a flat provider; never join nested components."""
    key = column_path_key(path)
    if len(key) != 1:
        raise NotImplementedError("provider does not support nested column paths")
    return key[0]


def column_definition(payload: bytes, path: Sequence[str], *, type_only: bool = False) -> pa.Field[Any]:
    """Decode exactly one field whose name agrees with the explicit target path.

    Type changes use a plain nullable field as a type carrier. Nullability and
    metadata must be changed through their own operations, never discarded.
    """
    from vgi.catalog.column_metadata import validate_column_metadata

    key = column_path_key(path)
    reader = pa.BufferReader(payload)
    # The Arrow stubs omit NativeFile, which read_schema accepts at runtime.
    schema = pa.ipc.read_schema(reader)  # type: ignore[arg-type]
    validate_column_metadata(schema)
    if reader.tell() != len(payload) or len(schema) != 1 or schema.metadata:
        raise ValueError("column definition must contain exactly one field and no schema metadata or trailing bytes")
    result = schema.field(0)
    if result.name != key[-1]:
        raise ValueError("column definition name must match the last column path component")
    attributes = {
        b"default",
        b"generated_expression",
        b"comment",
        b"tags",
        b"compression",
        b"vgi.column.default.v1",
        b"vgi.column.generated.v1",
        b"vgi.column.generation.v1",
    }
    if type_only:
        if not result.nullable:
            raise ValueError("type change field must contain only the target type and name")
        pending = [result]
        while pending:
            field = pending.pop()
            metadata = field.metadata or {}
            if attributes.intersection(metadata) or any(
                key.startswith((b"vgi.column.default.", b"vgi.column.generated.", b"vgi.column.generation."))
                for key in metadata
            ):
                raise ValueError("type change field must contain only the target type and name")
            dtype = field.type
            while isinstance(dtype, pa.BaseExtensionType) or pa.types.is_dictionary(dtype):
                dtype = dtype.storage_type if isinstance(dtype, pa.BaseExtensionType) else dtype.value_type
            pending.extend(dtype.field(index) for index in range(dtype.num_fields))
    return result


def replace_column_field(
    schema: pa.Schema,
    path: Sequence[str],
    replacement: pa.Field[Any] | None,
    *,
    add: bool = False,
) -> pa.Schema:
    """Build a new schema after a field edit, preserving unrelated metadata.

    Adding appends to the target's immediate parent. Replacing retains its
    position, including a rename. Removing the last field of a table or struct
    is rejected. The caller owns data rewrites, constraints and dependencies.
    """
    key = column_path_key(path)
    if replacement is not None:
        column_path_key([replacement.name])
    if add and (replacement is None or replacement.name != key[-1]):
        raise ValueError("added field must match the last column path component")

    def edit(fields: list[pa.Field[Any]], depth: int) -> list[pa.Field[Any]]:
        matches = [i for i, definition in enumerate(fields) if definition.name == key[depth]]
        if len(matches) > 1:
            raise ValueError("column path is ambiguous")
        if depth == len(key) - 1 and add:
            if matches:
                raise ValueError("column already exists")
            assert replacement is not None
            return [*fields, replacement]
        if not matches:
            raise ValueError("column path does not exist")
        index = matches[0]
        definition = fields[index]
        if depth != len(key) - 1:
            if not pa.types.is_struct(definition.type):
                raise ValueError("column path has a non-struct parent")
            updated = definition.with_type(pa.struct(edit(list(definition.type), depth + 1)))
        elif replacement is None:
            if len(fields) == 1:
                raise ValueError("cannot drop the last column or struct field")
            return fields[:index] + fields[index + 1 :]
        else:
            if any(i != index and field.name == replacement.name for i, field in enumerate(fields)):
                raise ValueError("column already exists")
            updated = replacement
        return fields[:index] + [updated] + fields[index + 1 :]

    metadata: dict[bytes | str, bytes | str] | None = (
        {key: value for key, value in schema.metadata.items()} if schema.metadata else None
    )
    return pa.schema(edit(list(schema), 0), metadata=metadata)
