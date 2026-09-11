# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Writable-table result modes shared by catalogs and write functions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal

import pyarrow as pa

WriteOperation = Literal["insert", "update", "delete"]
WriteResultMode = Literal["count", "rows", "changes"]

WRITE_OPERATIONS: tuple[WriteOperation, ...] = ("insert", "update", "delete")
WRITE_RESULT_MODES: tuple[WriteResultMode, ...] = ("count", "rows", "changes")
_MODE_RANK = {mode: rank for rank, mode in enumerate(WRITE_RESULT_MODES)}


def validate_write_result_modes(modes: Mapping[str, str]) -> dict[str, str]:
    """Return a validated copy of an operation-to-maximum-mode mapping."""
    result = dict(modes)
    unknown_operations = sorted(set(result).difference(WRITE_OPERATIONS))
    if unknown_operations:
        raise ValueError(f"Unknown write operation(s): {', '.join(unknown_operations)}")
    for operation, mode in result.items():
        if mode not in _MODE_RANK:
            raise ValueError(f"Unknown write result mode {mode!r} for {operation!r}")
    return result


def supports_write_result_mode(maximum: str, requested: str) -> bool:
    """Whether ``maximum`` promises support for ``requested``."""
    if maximum not in _MODE_RANK or requested not in _MODE_RANK:
        return False
    return _MODE_RANK[maximum] >= _MODE_RANK[requested]


def write_result_schema(mode: WriteResultMode, table_schema: pa.Schema) -> pa.Schema:
    """Build the exact output schema for a writable-table result mode."""
    if mode == "count":
        return pa.schema([pa.field("count", pa.int64(), nullable=False)])
    if mode == "rows":
        return table_schema
    row_type = pa.struct(list(table_schema))
    return pa.schema(
        [
            pa.field("old", row_type, nullable=True),
            pa.field("new", row_type, nullable=True),
        ]
    )


def write_changes_batch(
    table_schema: pa.Schema,
    old_rows: Sequence[Mapping[str, Any] | None],
    new_rows: Sequence[Mapping[str, Any] | None],
) -> pa.RecordBatch:
    """Build a ``changes`` batch while enforcing one OLD/NEW pair per row."""
    if len(old_rows) != len(new_rows):
        raise ValueError("old_rows and new_rows must have equal length")
    rows = [
        {"old": dict(old) if old is not None else None, "new": dict(new) if new is not None else None}
        for old, new in zip(old_rows, new_rows, strict=True)
    ]
    return pa.RecordBatch.from_pylist(rows, schema=write_result_schema("changes", table_schema))


def coerce_write_result_mode(value: str) -> WriteResultMode:
    """Validate a wire value and narrow it to ``WriteResultMode``."""
    if value not in _MODE_RANK:
        raise ValueError(f"Unknown write result mode: {value!r}")
    return value
