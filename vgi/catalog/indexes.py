# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Typed index definitions; providers own binding, collation and physical indexes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Any

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


def index_definition(
    schema_path: Sequence[str],
    table: str,
    columns: Sequence[Sequence[str]],
    *,
    algorithm: str,
    options: Mapping[str, Any] | None = None,
    constraint_type: IndexConstraintType | str = IndexConstraintType.NONE,
    associated_constraint_id: str | None = None,
) -> IndexDefinition:
    """Build an [`IndexDefinition`][] over raw column paths without hand-encoding records.

    Args:
        schema_path: Schema path of the indexed table.
        table: Indexed table name.
        columns: Raw component paths of the keys in order, e.g. ``[["id"]]``.
        algorithm: Provider index algorithm identifier, e.g. ``"art"``.
        options: Effective option values (Python or ``pa.Scalar`` values).
        constraint_type: NONE, UNIQUE or PRIMARY.
        associated_constraint_id: ``id`` of the table's PRIMARY_KEY/UNIQUE
            record this index backs; required for PRIMARY.

    Returns:
        The validated definition.

    Raises:
        TypeError: ``columns`` holds names instead of component paths.
        ValueError: The arguments do not describe one valid definition.
    """
    if isinstance(columns, (str, bytes)) or any(isinstance(path, (str, bytes)) for path in columns):
        raise TypeError("index columns are raw component paths such as [['id']], not names")
    owner = list(schema_path)
    return IndexDefinition(
        table=CatalogObjectRef(kind="table", schema_path=owner, name=table),
        algorithm=algorithm,
        keys=[IndexKey(column_path=list(path)).serialize_to_bytes() for path in columns],
        options=TypedValueBatch.from_values(options or {}),
        constraint_type=constraint_type
        if isinstance(constraint_type, IndexConstraintType)
        else IndexConstraintType(constraint_type.lower()),
        associated_constraint=None
        if associated_constraint_id is None
        else CatalogObjectRef(kind="constraint", schema_path=owner, name=associated_constraint_id, owner_table=table),
    )
