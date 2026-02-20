"""Declarative descriptor classes for catalog definition.

This module provides classes for declaratively defining catalog structure:
- Catalog: Top-level container for schemas
- Schema: Groups tables, views, and functions
- Table: Table definition with columns and constraints
- View: View definition with SQL

"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from vgi.arguments import Arguments
from vgi.catalog.catalog_interface import (
    AttachId,
    SchemaInfo,
    SerializedSchema,
    TableInfo,
    ViewInfo,
)
from vgi.invocation import FunctionType

if TYPE_CHECKING:
    from vgi.function import Function
    from vgi.table_function import TableFunctionGenerator

__all__ = [
    "Catalog",
    "Schema",
    "Table",
    "View",
]


@dataclass(frozen=True, slots=True, kw_only=True)
class Table:
    """Declarative table definition.

    Immutable. Can be defined in two ways:

    1. **Explicit columns**: Provide ``columns`` schema directly.
    2. **Function-backed**: Provide ``function`` reference — the schema is
       derived by calling ``bind()`` on the function class. If the function
       requires arguments, supply them via ``arguments``.

    Attributes:
        name: Table name.
        columns: Explicit PyArrow schema (mutually exclusive with function).
        function: TableFunctionGenerator class to derive schema from
            (mutually exclusive with columns).
        arguments: Arguments to pass when calling ``bind()`` on a
            function-backed table. Required when the function has
            mandatory parameters.
        not_null: Tuple of column names with NOT NULL constraints.
        unique: Tuple of column name tuples for UNIQUE constraints.
        check: Tuple of SQL expressions for CHECK constraints.
        comment: Optional table comment.
        tags: Optional metadata tags.

    """

    name: str
    columns: pa.Schema | None = None
    function: type[TableFunctionGenerator[Any, Any]] | None = None
    arguments: Arguments | None = None
    not_null: tuple[str, ...] = ()
    unique: tuple[tuple[str, ...], ...] = ()
    check: tuple[str, ...] = ()
    comment: str | None = None
    tags: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate configuration and constraint column names."""
        # Validate mutually exclusive options
        if self.columns is None and self.function is None:
            raise ValueError(f"Table '{self.name}': must specify either 'columns' or 'function'")
        if self.columns is not None and self.function is not None:
            raise ValueError(f"Table '{self.name}': cannot specify both 'columns' and 'function'")

        # Resolve columns to validate constraints
        resolved = self._get_resolved_columns()
        column_names = {f.name for f in resolved}

        # Validate not_null column names
        for col in self.not_null:
            if col not in column_names:
                raise ValueError(
                    f"Table '{self.name}': not_null column '{col}' not found "
                    f"in schema. Available columns: {sorted(column_names)}"
                )

        # Validate unique column names
        for group in self.unique:
            for col in group:
                if col not in column_names:
                    raise ValueError(
                        f"Table '{self.name}': unique column '{col}' not found "
                        f"in schema. Available columns: {sorted(column_names)}"
                    )

    def _get_resolved_columns(self) -> pa.Schema:
        """Get the resolved columns schema (explicit or derived from function).

        For function-backed tables, calls ``bind()`` on the function class
        to obtain the output schema.  If the function requires arguments,
        they must be supplied via the ``arguments`` field.
        """
        if self.columns is not None:
            return self.columns

        assert self.function is not None
        arguments = self.arguments if self.arguments is not None else Arguments()
        from vgi.protocol import BindRequest

        bind_call = BindRequest(
            function_name=self.function.Meta.name,  # type: ignore[attr-defined]
            arguments=arguments,
            function_type=FunctionType.TABLE,
        )
        try:
            result = self.function.bind(bind_call)
            return result.output_schema
        except Exception as e:
            raise ValueError(
                f"Table '{self.name}': failed to derive schema from function "
                f"'{self.function.__name__}' via bind(). If the function requires "
                f"arguments, pass them via arguments=Arguments(...). Error: {e}"
            ) from e

    @property
    def resolved_columns(self) -> pa.Schema:
        """The resolved column schema (explicit or derived from function)."""
        return self._get_resolved_columns()

    def _resolve_not_null_indices(self) -> list[int]:
        """Convert column names to indices for not_null constraints."""
        cols = self.resolved_columns
        return [cols.get_field_index(col) for col in self.not_null]

    def _resolve_unique_indices(self) -> list[list[int]]:
        """Convert column names to indices for unique constraints."""
        cols = self.resolved_columns
        return [[cols.get_field_index(col) for col in group] for group in self.unique]

    def to_table_info(self, schema_name: str) -> TableInfo:
        """Convert to TableInfo for catalog response."""
        cols = self.resolved_columns
        return TableInfo(
            name=self.name,
            schema_name=schema_name,
            columns=SerializedSchema(cols.serialize().to_pybytes()),
            not_null_constraints=self._resolve_not_null_indices(),
            unique_constraints=self._resolve_unique_indices(),
            check_constraints=list(self.check),
            comment=self.comment,
            tags=dict(self.tags),
        )


@dataclass(frozen=True)
class View:
    """Declarative view definition.

    Immutable.

    Attributes:
        name: View name.
        definition: SQL definition of the view.
        comment: Optional view comment.
        tags: Optional metadata tags.

    """

    name: str
    definition: str
    comment: str | None = None
    tags: dict[str, str] = field(default_factory=dict)

    def to_view_info(self, schema_name: str) -> ViewInfo:
        """Convert to ViewInfo for catalog response."""
        return ViewInfo(
            name=self.name,
            schema_name=schema_name,
            definition=self.definition,
            comment=self.comment,
            tags=dict(self.tags),
        )


@dataclass
class Schema:
    """Declarative schema definition grouping tables, views, and functions.

    Attributes:
        name: Schema name.
        comment: Optional schema comment.
        tags: Optional metadata tags.
        tables: Sequence of Table definitions.
        views: Sequence of View definitions.
        functions: Sequence of Function classes (scalar, table, or aggregate).

    """

    name: str
    comment: str | None = None
    tags: dict[str, str] = field(default_factory=dict)
    tables: Sequence[Table] = ()
    views: Sequence[View] = ()
    functions: Sequence[type[Function]] = ()

    def to_schema_info(self, attach_id: AttachId) -> SchemaInfo:
        """Convert to SchemaInfo for catalog response."""
        return SchemaInfo(
            attach_id=attach_id,
            name=self.name,
            comment=self.comment,
            tags=dict(self.tags),
        )


@dataclass
class Catalog:
    """Declarative catalog definition containing schemas.

    The single entry point for defining all catalog metadata on a Worker.

    Attributes:
        name: The catalog name (used in SQL as the database name).
        default_schema: Schema to use for unqualified table/view/function names.
        schemas: Sequence of Schema objects defining the catalog contents.

    """

    name: str
    default_schema: str = "main"
    schemas: Sequence[Schema] = ()

    def __post_init__(self) -> None:
        """Validate catalog configuration."""
        schema_names = {s.name.lower() for s in self.schemas}

        # Validate default_schema exists
        if self.default_schema.lower() not in schema_names:
            available = sorted(s.name for s in self.schemas) or ["(none)"]
            raise ValueError(
                f"Catalog '{self.name}': default_schema '{self.default_schema}' "
                f"not found in schemas. Available schemas: {available}"
            )

        # Check for duplicate schema names (case-insensitive)
        seen: set[str] = set()
        for schema in self.schemas:
            key = schema.name.lower()
            if key in seen:
                raise ValueError(f"Catalog '{self.name}': duplicate schema name '{schema.name}'")
            seen.add(key)
