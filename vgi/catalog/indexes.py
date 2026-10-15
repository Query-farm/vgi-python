# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Typed index definitions; providers own binding, collation and physical indexes."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Annotated

import pyarrow as pa
from vgi_rpc import ArrowType

from vgi.catalog.constraints import _ConstraintRecord, _identifier, _paths, _records, _reference
from vgi.catalog.dependencies import CatalogObjectRef
from vgi.catalog.sql import SqlExpression
from vgi.column_path import ColumnPath
from vgi.typed_values import TypedValueBatch


class IndexConstraintType(Enum):
    """Declared index semantics; UNIQUE does not imply a logical PRIMARY key."""

    NONE = "none"
    UNIQUE = "unique"
    PRIMARY = "primary"


@dataclass(frozen=True, kw_only=True)
class IndexKey(_ConstraintRecord):
    """Exactly one raw column path or explicitly contextual SQL expression.

    Raw keys require provider-established binary column collation in the initial
    profile. Direction and physical ordering require an algorithm profile.
    """

    column_path: ColumnPath | None = None
    expression: Annotated[SqlExpression | None, ArrowType(pa.binary())] = None

    def __post_init__(self) -> None:
        """Reject absent or ambiguous keys without interpreting SQL strings."""
        if (self.column_path is None) == (self.expression is None):
            raise ValueError("index key requires exactly one column path or expression")
        if self.column_path is not None:
            _paths([self.column_path])
        if self.expression is not None:
            self.expression.__post_init__()


@dataclass(frozen=True, kw_only=True)
class IndexDefinition(_ConstraintRecord):
    """Complete typed index creation and resolved discovery definition.

    Options are typed literals. Discovery returns effective option values here
    and in IndexInfo.options, whose explicit_names preserve stored overrides.
    No algorithm, ordering, collation or constraint association is inferred.
    """

    table: Annotated[CatalogObjectRef, ArrowType(pa.binary())]
    algorithm: str
    keys: list[bytes]
    options: Annotated[TypedValueBatch, ArrowType(pa.binary())]
    constraint_type: IndexConstraintType
    associated_constraint: Annotated[CatalogObjectRef | None, ArrowType(pa.binary())] = None

    def __post_init__(self) -> None:
        """Validate the complete record before provider binding or publication."""
        _reference(self.table, "table")
        _identifier(self.algorithm, "index algorithm")
        _records(self.keys, nonempty=True)
        for key in self.key_definitions():
            if key.expression is not None and key.expression.schema_path != self.table.schema_path:
                raise ValueError("index expression must bind in its table schema")
        if not isinstance(self.options, TypedValueBatch):
            raise TypeError("index options require TypedValueBatch")
        self.options.__post_init__()
        kind = IndexConstraintType(self.constraint_type)
        if kind == IndexConstraintType.PRIMARY and self.associated_constraint is None:
            raise ValueError("PRIMARY index requires an authoritative constraint association")
        if self.associated_constraint is not None:
            _reference(self.associated_constraint, "constraint")
            if (
                self.associated_constraint.schema_path != self.table.schema_path
                or self.associated_constraint.owner_table != self.table.name
            ):
                raise ValueError("associated constraint belongs to another table")
            if kind == IndexConstraintType.NONE:
                raise ValueError("regular index cannot claim a key constraint association")

    def key_definitions(self) -> list[IndexKey]:
        """Decode keys in their declared order without splitting raw names."""
        return [IndexKey.deserialize_from_bytes(value) for value in self.keys]
