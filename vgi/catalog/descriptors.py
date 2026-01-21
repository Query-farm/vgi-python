"""Declarative descriptor classes for catalog definition.

This module provides classes for declaratively defining catalog structure:
- Catalog: Top-level container for schemas
- Schema: Groups tables, views, and functions
- Table: Table definition with columns and constraints
- View: View definition with SQL

Example:
    from vgi import Worker, TableFunctionGenerator, Output
    from vgi.catalog import Catalog, Schema, Table, View
    import pyarrow as pa

    class UsersFunction(TableFunctionGenerator):
        @property
        def output_schema(self) -> pa.Schema:
            return pa.schema([("id", pa.int64()), ("name", pa.string())])

        def process(self):
            yield Output(...)

    # Table with function-backed schema (recommended - no duplication)
    users_table = Table(
        name="users",
        function=UsersFunction,
        not_null=["id"],
        unique=[["id"]],
    )

    # View definition
    active_users = View(
        name="active_users",
        definition="SELECT * FROM users WHERE active = true",
    )

    class MyWorker(Worker):
        catalog = Catalog(
            name="myapp",
            default_schema="main",
            schemas=[
                Schema(
                    name="main",
                    tables=[users_table],
                    views=[active_users],
                    functions=[UsersFunction],
                ),
            ],
        )

"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from vgi.catalog.catalog_interface import (
    AttachId,
    SchemaInfo,
    SerializedSchema,
    TableInfo,
    ViewInfo,
)

if TYPE_CHECKING:
    from vgi.function import Function
    from vgi.table_function import TableFunctionGenerator

__all__ = [
    "Catalog",
    "Schema",
    "Table",
    "View",
]


@dataclass(frozen=True)
class Table:
    """Declarative table definition.

    Immutable. Can be defined in two ways:

    1. **Explicit columns**: Provide `columns` schema directly
    2. **Function-backed**: Provide `function` reference - schema auto-derived

    For function-backed tables, the function must:
    - Be a TableFunctionGenerator subclass
    - Have no required arguments (or all with defaults)
    - Have a static `output_schema` (not dependent on runtime state)

    Attributes:
        name: Table name.
        columns: Explicit PyArrow schema (mutually exclusive with function).
        function: TableFunctionGenerator class to derive schema from
            (mutually exclusive with columns).
        not_null: Tuple of column names with NOT NULL constraints.
        unique: Tuple of column name tuples for UNIQUE constraints.
        check: Tuple of SQL expressions for CHECK constraints.
        comment: Optional table comment.
        tags: Optional metadata tags.

    Example (explicit columns):
        users = Table(
            name="users",
            columns=pa.schema([("id", pa.int64()), ("name", pa.string())]),
            not_null=["id"],
            unique=[["id"]],
        )

    Example (function-backed - schema derived automatically):
        class UsersFunction(TableFunctionGenerator):
            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([("id", pa.int64()), ("name", pa.string())])
            def process(self):
                yield Output(...)

        users = Table(
            name="users",
            function=UsersFunction,  # columns auto-derived from output_schema
            not_null=["id"],
            unique=[["id"]],
        )

    """

    name: str
    columns: pa.Schema | None = None
    function: type[TableFunctionGenerator] | None = None
    not_null: tuple[str, ...] = ()
    unique: tuple[tuple[str, ...], ...] = ()
    check: tuple[str, ...] = ()
    comment: str | None = None
    tags: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate configuration and constraint column names."""
        # Validate mutually exclusive options
        if self.columns is None and self.function is None:
            raise ValueError(
                f"Table '{self.name}': must specify either 'columns' or 'function'"
            )
        if self.columns is not None and self.function is not None:
            raise ValueError(
                f"Table '{self.name}': cannot specify both 'columns' and 'function'"
            )

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
        """Get the resolved columns schema (explicit or derived from function)."""
        if self.columns is not None:
            return self.columns

        # Derive from function's output_schema
        assert self.function is not None
        try:
            # Create instance with empty invocation to get output_schema
            instance = self.function.__new__(self.function)
            # Call property directly to get schema
            schema = instance.output_schema
            return schema
        except Exception as e:
            raise ValueError(
                f"Table '{self.name}': failed to derive schema from function "
                f"'{self.function.__name__}'. Ensure the function has no required "
                f"arguments and output_schema doesn't depend on runtime state. "
                f"Error: {e}"
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

    Example:
        active_users = View(
            name="active_users",
            definition="SELECT * FROM users WHERE active = true",
            comment="Active user accounts",
        )

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

    Example:
        Schema(
            name="analytics",
            comment="Analytics data",
            tables=[users_table, events_table],
            views=[daily_summary],
            functions=[AggregateFunction],
        )

    """

    name: str
    comment: str | None = None
    tags: dict[str, str] = field(default_factory=dict)
    tables: Sequence[Table] = ()
    views: Sequence[View] = ()
    functions: Sequence[type[Function[Any]]] = ()

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

    Example:
        class MyWorker(Worker):
            catalog = Catalog(
                name="myapp",
                default_schema="main",
                schemas=[
                    Schema(
                        name="main",
                        tables=[users_table, orders_table],
                        views=[active_users_view],
                        functions=[MyFunction],
                    ),
                    Schema(
                        name="analytics",
                        tables=[events_table],
                        functions=[AggregateFunc],
                    ),
                ],
            )

            def table_scan_function_get(self, *, schema_name, name, **kwargs):
                # Handle table scanning (not needed for function-backed tables)
                ...

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
                raise ValueError(
                    f"Catalog '{self.name}': duplicate schema name '{schema.name}'"
                )
            seen.add(key)
