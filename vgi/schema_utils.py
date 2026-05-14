"""Schema building utilities for VGI functions.

This module provides helpers for creating and modifying Arrow schemas with
minimal boilerplate, making output_schema definitions more concise.

FUNCTIONS
---------
schema(**fields)
    Build a schema from keyword arguments mapping names to types.

schema_like(source, add, remove, rename, replace)
    Derive a new schema from an existing one with modifications.

"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pyarrow as pa

# A field spec is either a bare DataType or a (DataType, metadata) tuple.
FieldSpec = pa.DataType | tuple[pa.DataType, dict[bytes | str, bytes | str]]

__all__ = [
    "FieldSpec",
    "VGI_PARTITION_COLUMN_KEY",
    "partition_field",
    "schema",
    "schema_like",
]

#: KeyValueMetadata key that marks an Arrow field as a partition column
#: for VGI's Hive-style partitioning (PartitionColumns mode). Workers
#: opt in by setting ``Meta.partition_kind`` to a non-default
#: :class:`vgi.metadata.PartitionKind` AND annotating at least one
#: field of their bind schema with this key.
VGI_PARTITION_COLUMN_KEY: bytes = b"vgi.partition_column"


def partition_field(
    name: str,
    type: pa.DataType,
    *,
    nullable: bool = True,
    metadata: dict[bytes | str, bytes | str] | None = None,
) -> pa.Field:
    """Build a ``pa.Field`` marked as a VGI partition column.

    Equivalent to::

        pa.field(name, type, nullable=nullable,
                 metadata={VGI_PARTITION_COLUMN_KEY: b"true",
                           **(metadata or {})})

    Use in a bind schema when the function opts into PartitionColumns
    mode by setting ``Meta.partition_kind`` to a non-default
    :class:`vgi.metadata.PartitionKind`. Per-field metadata round-trips
    through Arrow IPC, so the C++ extension can identify partition
    columns from ``bind_result.output_schema`` without a parallel
    list-of-names.

    Args:
        name: Column name.
        type: Arrow data type.
        nullable: Whether the column can contain nulls.
        metadata: Extra field-level metadata to merge with the
            partition-column marker. Useful for extension types
            (e.g. geoarrow.wkb's ``ARROW:extension:name`` key).

    Returns:
        A ``pa.Field`` carrying ``{VGI_PARTITION_COLUMN_KEY: b"true"}``
        in its metadata.
    """
    merged: dict[bytes, bytes] = {VGI_PARTITION_COLUMN_KEY: b"true"}
    if metadata:
        for k, v in metadata.items():
            key = k if isinstance(k, bytes) else k.encode()
            val = v if isinstance(v, bytes) else v.encode()
            merged[key] = val
    return pa.field(name, type, nullable=nullable, metadata=merged)


def schema(
    __fields: Mapping[str, FieldSpec] | None = None,
    /,
    **kwargs: FieldSpec,
) -> pa.Schema:
    """Build an Arrow schema from field definitions.

    Creates a schema with fields in the order specified. Field names are
    the keys and values are either Arrow data types or ``(type, metadata)``
    tuples for attaching field-level metadata.

    Args:
        __fields: Optional mapping of field names to specs (for programmatic use).
        **kwargs: Field names mapped to Arrow data types or ``(type, metadata)`` tuples.

    Returns:
        Arrow schema with the specified fields.

    Raises:
        TypeError: If a value is not a valid Arrow data type or field spec.

    Examples::

        schema(id=pa.int64(), name=pa.string())
        schema(row_id=(pa.int64(), {b"is_row_id": b""}), id=pa.int64())

    """
    # Combine __fields dict with kwargs
    all_fields: dict[str, FieldSpec] = {}
    if __fields is not None:
        all_fields.update(__fields)
    all_fields.update(kwargs)

    # Validate and build schema
    pa_fields: list[pa.Field[Any]] = []
    for name, spec in all_fields.items():
        if isinstance(spec, tuple):
            dtype, metadata = spec
            if not isinstance(dtype, pa.DataType):
                raise TypeError(
                    f"Field '{name}': expected pa.DataType as first tuple element, "
                    f"got {type(dtype).__name__}. Use pa.int64(), pa.string(), etc."
                )
            pa_fields.append(pa.field(name, dtype, metadata=metadata))
        elif isinstance(spec, pa.DataType):
            pa_fields.append(pa.field(name, spec))
        else:
            raise TypeError(
                f"Field '{name}': expected pa.DataType or (pa.DataType, metadata) tuple, "
                f"got {type(spec).__name__}. Use pa.int64(), pa.string(), etc."
            )

    return pa.schema(pa_fields)


def schema_like(
    source: pa.Schema,
    *,
    add: Mapping[str, pa.DataType] | None = None,
    remove: list[str] | None = None,
    rename: Mapping[str, str] | None = None,
    replace: Mapping[str, pa.DataType] | None = None,
) -> pa.Schema:
    """Derive a new schema from an existing one with modifications.

    Creates a modified copy of the source schema. Operations are applied
    in this order: remove -> rename -> replace -> add.

    Args:
        source: The source schema to derive from.
        add: Fields to add at the end. Dict mapping names to types.
        remove: Field names to remove from the schema.
        rename: Field name mappings (old_name -> new_name).
        replace: Fields to replace with new types (keeps position).

    Returns:
        New schema with the specified modifications.

    Raises:
        KeyError: If a field to remove, rename, or replace doesn't exist.
        ValueError: If trying to add a field that already exists.

    """
    # Start with source field names for tracking
    field_names = set(source.names)

    # Validate remove fields exist
    if remove:
        for name in remove:
            if name not in field_names:
                raise KeyError(f"Cannot remove field '{name}': not found in schema. Available fields: {source.names}")

    # Validate rename fields exist
    if rename:
        for old_name in rename:
            if old_name not in field_names:
                raise KeyError(
                    f"Cannot rename field '{old_name}': not found in schema. Available fields: {source.names}"
                )

    # Validate replace fields exist
    if replace:
        for name in replace:
            if name not in field_names:
                raise KeyError(f"Cannot replace field '{name}': not found in schema. Available fields: {source.names}")

    # Build the new schema
    # Step 1: Remove fields
    remove_set = set(remove) if remove else set()

    # Step 2 & 3: Process remaining fields (rename and replace)
    rename_map = rename or {}
    replace_map = replace or {}

    new_fields: list[pa.Field[Any]] = []
    final_names: set[str] = set()

    for field in source:
        # Skip removed fields
        if field.name in remove_set:
            continue

        # Get the (possibly renamed) name
        new_name = rename_map.get(field.name, field.name)

        # Get the (possibly replaced) type
        new_type = replace_map.get(field.name, field.type)

        new_fields.append(pa.field(new_name, new_type, metadata=field.metadata))
        final_names.add(new_name)

    # Step 4: Add new fields
    if add:
        for name, dtype in add.items():
            if name in final_names:
                raise ValueError(
                    f"Cannot add field '{name}': already exists in schema. "
                    f"Use 'replace' to change an existing field's type."
                )
            if not isinstance(dtype, pa.DataType):
                raise TypeError(
                    f"Field '{name}': expected pa.DataType, "
                    f"got {type(dtype).__name__}. Use pa.int64(), pa.string(), etc."
                )
            new_fields.append(pa.field(name, dtype))

    return pa.schema(new_fields)
