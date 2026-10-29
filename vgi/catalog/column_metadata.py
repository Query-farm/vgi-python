# Copyright 2026 Query Farm LLC - https://query.farm

"""Strict comment and tag metadata on existing Arrow column definitions."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, cast

import pyarrow as pa

COMMENT_KEY = b"comment"
TAGS_KEY = b"tags"


def _text(value: str, label: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"column {label} must be a string")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError(f"column {label} must contain valid Unicode") from error


@dataclass(frozen=True, kw_only=True)
class ColumnMetadata:
    """Absent values are None; empty comments and empty tag maps remain present."""

    comment: str | None = None
    tags: dict[str, str] | None = None

    def __post_init__(self) -> None:
        """Require Unicode comments and string maps without normalizing values."""
        if self.comment is not None:
            _text(self.comment, "comment")
        if self.tags is not None:
            if not isinstance(self.tags, dict):
                raise TypeError("column tags must be a string map")
            for key, value in self.tags.items():
                _text(key, "tag key")
                _text(value, "tag value")


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("column tags contain duplicate JSON keys")
        result[key] = value
    return result


def _constant(value: str) -> Any:
    raise ValueError(f"column tags contain invalid JSON constant {value}")


def column_metadata(field: pa.Field[Any]) -> ColumnMetadata:
    """Decode reserved keys strictly while leaving unrelated metadata opaque."""
    # Catalog column metadata already requires unique Arrow keys. This is
    # separate from duplicate decoded keys inside the tags JSON object.
    if not field.equals(field.with_metadata(cast(Any, field.metadata)), check_metadata=True):
        raise ValueError("column metadata keys must be unique")
    metadata = field.metadata or {}
    try:
        comment = metadata[COMMENT_KEY].decode("utf-8") if COMMENT_KEY in metadata else None
        tags = (
            json.loads(metadata[TAGS_KEY].decode("utf-8"), object_pairs_hook=_object, parse_constant=_constant)
            if TAGS_KEY in metadata
            else None
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as error:
        raise ValueError("column comment/tags metadata requires valid UTF-8 and JSON") from error
    if TAGS_KEY in metadata and not isinstance(tags, dict):
        raise ValueError("column tags JSON must be an object of strings")
    if tags is not None and any(not isinstance(value, str) for value in tags.values()):
        raise ValueError("column tags JSON must be an object of strings")
    return ColumnMetadata(comment=comment, tags=tags)


def with_column_metadata(field: pa.Field[Any], value: ColumnMetadata) -> pa.Field[Any]:
    """Replace the two reserved values, preserving all unrelated field metadata."""
    original = column_metadata(field)
    value.__post_init__()
    metadata = dict(field.metadata or {})
    metadata.pop(COMMENT_KEY, None)
    metadata.pop(TAGS_KEY, None)
    if value.comment is not None:
        metadata[COMMENT_KEY] = value.comment.encode("utf-8")
    if value.tags is not None:
        tags = (
            (field.metadata or {})[TAGS_KEY]
            if value.tags == original.tags
            else json.dumps(value.tags, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        )
        metadata[TAGS_KEY] = tags
    return field.with_metadata(cast(Any, metadata or None))


def validate_column_metadata(schema: pa.Schema) -> None:
    """Validate each field, including container, dictionary and extension children."""
    pending = list(schema)
    while pending:
        field = pending.pop()
        column_metadata(field)
        dtype = field.type
        while isinstance(dtype, pa.BaseExtensionType) or pa.types.is_dictionary(dtype):
            dtype = dtype.storage_type if isinstance(dtype, pa.BaseExtensionType) else dtype.value_type
        pending.extend(dtype.field(index) for index in range(dtype.num_fields))
