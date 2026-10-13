# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Portable table-layout definitions; they never promise scan output ordering."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Annotated

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi.schema_path import SchemaPath, schema_path_key


@dataclass(frozen=True, kw_only=True)
class SqlExpression(ArrowSerializableDataclass):
    """Unbound scalar SQL with an explicit semantic profile and owner schema.

    The receiving provider binds and evaluates the expression. A profile must
    define its supported grammar, functions and type/NULL behavior; unknown
    profiles are rejected before effects. No engine AST or bound plan is sent.
    ``schema_path`` is attachment-relative, never a caller's catalog alias.
    """

    sql: str
    profile: str
    schema_path: SchemaPath

    def __post_init__(self) -> None:
        """Reject empty SQL, malformed profiles and invalid owner paths."""
        if not isinstance(self.sql, str) or not self.sql.strip() or "\x00" in self.sql:
            raise ValueError("SQL expression must be nonempty and contain no NUL")
        if not re.fullmatch(r"[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*\.v[1-9][0-9]*", self.profile):
            raise ValueError("expression profile must be a versioned semantic name")
        schema_path_key(self.schema_path)


class SortDirection(StrEnum):
    """Explicit sort direction, independent of the caller's session defaults."""

    ASC = "asc"
    DESC = "desc"


class NullOrder(StrEnum):
    """Explicit NULL position for a sort key."""

    FIRST = "first"
    LAST = "last"


@dataclass(frozen=True, kw_only=True)
class SortKey(ArrowSerializableDataclass):
    """Scalar key, direction and NULL order for a table layout."""

    expression: Annotated[SqlExpression, ArrowType(pa.binary())]
    direction: SortDirection
    null_order: NullOrder

    def __post_init__(self) -> None:
        """Validate the expression and reject session-dependent order values."""
        self.expression.__post_init__()
        SortDirection(self.direction)
        NullOrder(self.null_order)


class LayoutCoverage(StrEnum):
    """Whether existing data has been rewritten for the current definition."""

    ALL_ROWS = "all_rows"
    FUTURE_WRITES = "future_writes"


@dataclass(frozen=True, kw_only=True)
class TableLayout(ArrowSerializableDataclass):
    """Stored layout policy, with ordered lists of IPC expression/key records.

    Empty lists mean no partitioning/sorting policy. ``coverage`` describes
    physical layout applicability, not relational constraints or scan order.
    A create request uses ALL_ROWS because the new table initially has no rows.
    """

    partition_by: Annotated[list[bytes], ArrowType(pa.list_(pa.binary()))] = field(default_factory=list)
    sorted_by: Annotated[list[bytes], ArrowType(pa.list_(pa.binary()))] = field(default_factory=list)
    coverage: LayoutCoverage = LayoutCoverage.ALL_ROWS

    def __post_init__(self) -> None:
        """Validate every nested record before provider execution."""
        self.partition_expressions()
        self.sort_keys()
        LayoutCoverage(self.coverage)

    def partition_expressions(self) -> list[SqlExpression]:
        """Decode ordered partition expressions."""
        return [SqlExpression.deserialize_from_bytes(value) for value in self.partition_by]

    def sort_keys(self) -> list[SortKey]:
        """Decode ordered sort keys without discarding direction or NULL order."""
        return [SortKey.deserialize_from_bytes(value) for value in self.sorted_by]

    @classmethod
    def from_expressions(cls, *, partition_by: list[SqlExpression], sorted_by: list[SortKey]) -> TableLayout:
        """Build a creation layout from typed expression/key objects."""
        return cls(
            partition_by=[expr.serialize_to_bytes() for expr in partition_by],
            sorted_by=[key.serialize_to_bytes() for key in sorted_by],
        )


class LayoutChangeMode(StrEnum):
    """Explicit existing-data policy for an atomic layout alteration."""

    FUTURE_WRITES = "future_writes"
    REWRITE = "rewrite"


@dataclass(frozen=True, kw_only=True)
class TableLayoutChange(ArrowSerializableDataclass):
    """Replace either layout component, atomically under the requested mode.

    NULL leaves a component unchanged; an empty list clears it. REWRITE succeeds
    only after all existing data conforms to the replacement layout. A provider
    that cannot honor the mode must reject it before mutation. No implicit retry.
    """

    mode: LayoutChangeMode
    partition_by: Annotated[list[bytes] | None, ArrowType(pa.list_(pa.binary()))] = None
    sorted_by: Annotated[list[bytes] | None, ArrowType(pa.list_(pa.binary()))] = None

    def __post_init__(self) -> None:
        """Reject empty changes and validate all supplied definitions."""
        LayoutChangeMode(self.mode)
        if self.partition_by is None and self.sorted_by is None:
            raise ValueError("layout change must replace at least one component")
        TableLayout(partition_by=self.partition_by or [], sorted_by=self.sorted_by or [])

    def apply_to(self, current: TableLayout) -> TableLayout:
        """Build a candidate; the provider must implement the requested rewrite."""
        self.__post_init__()
        current.__post_init__()
        return TableLayout(
            partition_by=current.partition_by if self.partition_by is None else self.partition_by,
            sorted_by=current.sorted_by if self.sorted_by is None else self.sorted_by,
            coverage=LayoutCoverage.ALL_ROWS if self.mode == LayoutChangeMode.REWRITE else LayoutCoverage.FUTURE_WRITES,
        )
