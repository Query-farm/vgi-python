# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Explicit facts for fixed fixture tables; never an inference from Arrow flags.

Callers own the proof: immutable rows are checked before publication, or every
fixture storage write validates values. Keys are fixed provider-owned identifiers
for tables whose definitions cannot be dropped/recreated through their API.
"""

from typing import Any

from vgi.catalog.constraints import (
    ConstraintDefinition,
    ConstraintEnforcement,
    ConstraintInfo,
    ConstraintKind,
    ConstraintValidation,
)
from vgi.catalog.dependencies import CatalogObjectRef


def fixed_fact(
    table: str, path: list[str], key: str, definition: ConstraintDefinition, **kwargs: Any
) -> ConstraintInfo:
    """Attach an explicit fixture definition to its stable provider-owned key."""
    return ConstraintInfo(
        ref=CatalogObjectRef(kind="constraint", schema_path=path, owner_table=table, name=key),
        definition=definition,
        enforcement=ConstraintEnforcement.ENFORCED,
        validation=ConstraintValidation.ALL_ROWS,
        **kwargs,
    )


def fixed_not_null(table: str, path: list[str], columns: tuple[tuple[str, ...], ...]) -> tuple[ConstraintInfo, ...]:
    """Encode explicit NOT_NULL facts that the fixed fixture actually enforces."""
    return tuple(
        fixed_fact(
            table,
            path,
            f"not-null-{index}",
            ConstraintDefinition(kind=ConstraintKind.NOT_NULL, column_paths=[list(column)]),
        )
        for index, column in enumerate(columns)
    )
