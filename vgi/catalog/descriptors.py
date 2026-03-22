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
    MacroInfo,
    MacroType,
    SchemaInfo,
    SerializedSchema,
    TableInfo,
    ViewInfo,
)
from vgi.invocation import BindResponse, FunctionType

if TYPE_CHECKING:
    from vgi.function import Function
    from vgi.table_function import TableFunctionGenerator
    from vgi.table_in_out_function import TableInOutGenerator


class Sql(str):
    """A raw SQL expression, passed through verbatim as a default value.

    Use this when the default is a SQL expression rather than a Python literal::

        defaults={"created_at": Sql("current_timestamp")}
    """


# A default value can be a Python literal (str, int, float, bool, None)
# or Sql() for raw SQL expressions. Plain str values are treated as string
# literals and automatically quoted.
DefaultValue = str | int | float | bool | None

__all__ = [
    "Catalog",
    "DefaultValue",
    "ForeignKeyDef",
    "Macro",
    "Schema",
    "Sql",
    "Table",
    "View",
]


def _default_to_sql(value: DefaultValue) -> str:
    """Convert a Python default value to a SQL expression string.

    - ``Sql``: passed through verbatim (raw SQL)
    - ``str``: quoted as a SQL string literal (``'hello'``)
    - ``int`` / ``float``: unquoted numeric literal
    - ``bool``: ``true`` / ``false``
    - ``None``: ``NULL``
    """
    if isinstance(value, Sql):
        return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if value is None:
        return "NULL"
    # str — quote as SQL string literal, escaping single quotes
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


@dataclass(frozen=True, slots=True)
class ForeignKeyDef:
    """A foreign key constraint definition.

    Attributes:
        columns: Column names in THIS table that form the FK.
        referenced_table: Name of the referenced table.
        referenced_columns: Column names in the referenced table.
        referenced_schema: Schema of the referenced table.
            Defaults to None meaning same schema as this table.

    """

    columns: tuple[str, ...]
    referenced_table: str
    referenced_columns: tuple[str, ...]
    referenced_schema: str | None = None


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
        defaults: Dict mapping column names to default values. Accepts
            Python literals (str, int, float, bool, None) which are
            auto-converted, or SqlExpression for raw SQL.
        comment: Optional table comment.
        tags: Optional metadata tags.

    """

    name: str
    columns: pa.Schema | None = None
    function: type[TableFunctionGenerator[Any, Any]] | None = None
    arguments: Arguments | None = None
    supports_time_travel: bool = False
    insert_function: type[TableInOutGenerator[Any, Any]] | None = None
    update_function: type[TableInOutGenerator[Any, Any]] | None = None
    delete_function: type[TableInOutGenerator[Any, Any]] | None = None
    not_null: tuple[str, ...] = ()
    unique: tuple[tuple[str, ...], ...] = ()
    check: tuple[str, ...] = ()
    primary_key: tuple[tuple[str, ...], ...] = ()
    foreign_key: tuple[ForeignKeyDef, ...] = ()
    defaults: dict[str, DefaultValue] = field(default_factory=dict)
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

        # Validate primary_key column names
        for group in self.primary_key:
            for col in group:
                if col not in column_names:
                    raise ValueError(
                        f"Table '{self.name}': primary_key column '{col}' not found "
                        f"in schema. Available columns: {sorted(column_names)}"
                    )

        # Validate foreign_key column names (only FK side, not referenced table)
        for fk in self.foreign_key:
            for col in fk.columns:
                if col not in column_names:
                    raise ValueError(
                        f"Table '{self.name}': foreign_key column '{col}' not found "
                        f"in schema. Available columns: {sorted(column_names)}"
                    )

        # Validate at most one primary key
        if len(self.primary_key) > 1:
            raise ValueError(
                f"Table '{self.name}': at most one primary_key constraint allowed, got {len(self.primary_key)}"
            )

        # Validate foreign_key column count parity
        for fk in self.foreign_key:
            if len(fk.columns) != len(fk.referenced_columns):
                raise ValueError(
                    f"Table '{self.name}': foreign_key referencing '{fk.referenced_table}' "
                    f"has {len(fk.columns)} FK columns but {len(fk.referenced_columns)} "
                    f"referenced columns — counts must match"
                )

        # Validate defaults column names
        for col in self.defaults:
            if col not in column_names:
                raise ValueError(
                    f"Table '{self.name}': defaults column '{col}' not found "
                    f"in schema. Available columns: {sorted(column_names)}"
                )

        # Validate write functions: UPDATE/DELETE require a scan function for row IDs
        if (self.update_function is not None or self.delete_function is not None) and self.function is None:
            raise ValueError(
                f"Table '{self.name}': update_function and delete_function require "
                f"a scan function (set 'function') to provide row IDs"
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
            if not isinstance(result, BindResponse):
                raise ValueError(
                    f"Table '{self.name}': function '{self.function.__name__}' returned "
                    f"unexpected bind result type: {type(result).__name__}"
                )
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

    def _resolve_primary_key_indices(self) -> list[list[int]]:
        """Convert column names to indices for primary_key constraints."""
        cols = self.resolved_columns
        return [[cols.get_field_index(col) for col in group] for group in self.primary_key]

    def _serialize_foreign_keys(self, schema_name: str) -> list[bytes]:
        """Serialize foreign key constraints as IPC bytes."""
        from vgi_rpc.utils import serialize_record_batch_bytes

        result = []
        for fk in self.foreign_key:
            batch = pa.RecordBatch.from_pydict(
                {
                    "fk_columns": [list(fk.columns)],
                    "pk_columns": [list(fk.referenced_columns)],
                    "referenced_table": [fk.referenced_table],
                    "referenced_schema": [fk.referenced_schema or schema_name],
                },
                schema=pa.schema(
                    [
                        ("fk_columns", pa.list_(pa.utf8())),
                        ("pk_columns", pa.list_(pa.utf8())),
                        ("referenced_table", pa.utf8()),
                        ("referenced_schema", pa.utf8()),
                    ]
                ),
            )
            result.append(serialize_record_batch_bytes(batch))
        return result

    def _apply_defaults_to_schema(self, schema: pa.Schema) -> pa.Schema:
        """Return schema with default value metadata applied to fields."""
        if not self.defaults:
            return schema
        for col_name, value in self.defaults.items():
            sql_expr = _default_to_sql(value)
            idx = schema.get_field_index(col_name)
            f = schema.field(idx)
            existing = dict(f.metadata) if f.metadata else {}
            existing[b"default"] = sql_expr.encode("utf-8")
            schema = schema.set(idx, f.with_metadata(existing))
        return schema

    def to_table_info(self, schema_name: str) -> TableInfo:
        """Convert to TableInfo for catalog response."""
        cols = self._apply_defaults_to_schema(self.resolved_columns)
        return TableInfo(
            name=self.name,
            schema_name=schema_name,
            columns=SerializedSchema(cols.serialize().to_pybytes()),
            not_null_constraints=self._resolve_not_null_indices(),
            unique_constraints=self._resolve_unique_indices(),
            check_constraints=list(self.check),
            primary_key_constraints=self._resolve_primary_key_indices(),
            foreign_key_constraints=self._serialize_foreign_keys(schema_name),
            supports_insert=self.insert_function is not None,
            supports_update=self.update_function is not None,
            supports_delete=self.delete_function is not None,
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


@dataclass(frozen=True)
class Macro:
    """Declarative macro definition.

    Attributes:
        name: Macro name.
        macro_type: Whether this is a scalar or table macro.
        parameters: Ordered list of parameter names.
        parameter_default_values: One-row RecordBatch where columns are parameter
            names and values are typed defaults. None if no defaults.
            Example: pa.RecordBatch.from_pydict({"b": [5]}) for b := 5.
        definition: SQL expression (scalar) or query (table).
        comment: Optional macro comment.
        tags: Optional metadata tags.

    """

    name: str
    macro_type: MacroType
    parameters: list[str] = field(default_factory=list)
    parameter_default_values: pa.RecordBatch | None = None
    definition: str = ""
    comment: str | None = None
    tags: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate macro configuration."""
        if self.parameter_default_values is not None:
            if self.parameter_default_values.num_rows != 1:
                raise ValueError(
                    f"Macro '{self.name}': parameter_default_values must have exactly 1 row, "
                    f"got {self.parameter_default_values.num_rows}"
                )
            # Validate that default param column names exist in parameters list
            param_set = set(self.parameters)
            for col_name in self.parameter_default_values.schema.names:
                if col_name not in param_set:
                    raise ValueError(
                        f"Macro '{self.name}': default parameter '{col_name}' not found "
                        f"in parameters list {self.parameters}"
                    )

    def to_macro_info(self, schema_name: str) -> MacroInfo:
        """Convert to MacroInfo for catalog response."""
        return MacroInfo(
            name=self.name,
            schema_name=schema_name,
            macro_type=self.macro_type,
            parameters=list(self.parameters),
            parameter_default_values=self.parameter_default_values,
            definition=self.definition,
            comment=self.comment,
            tags=dict(self.tags),
        )


@dataclass
class Schema:
    """Declarative schema definition grouping tables, views, functions, and macros.

    Attributes:
        name: Schema name.
        comment: Optional schema comment.
        tags: Optional metadata tags.
        tables: Sequence of Table definitions.
        views: Sequence of View definitions.
        functions: Sequence of Function classes (scalar, table, or aggregate).
        macros: Sequence of Macro definitions.

    """

    name: str
    comment: str | None = None
    tags: dict[str, str] = field(default_factory=dict)
    tables: Sequence[Table] = ()
    views: Sequence[View] = ()
    functions: Sequence[type[Function]] = ()
    macros: Sequence[Macro] = ()

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
