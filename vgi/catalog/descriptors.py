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
from typing import TYPE_CHECKING, Any, Union

import pyarrow as pa

from vgi.arguments import Arguments
from vgi.catalog.catalog_interface import (
    AttachId,
    ColumnStatistics,
    IndexConstraintType,
    IndexInfo,
    MacroInfo,
    MacroType,
    ScanFunctionResult,
    SchemaInfo,
    SerializedSchema,
    TableColumnStatisticsResult,
    TableInfo,
    ViewInfo,
    serialize_column_statistics,
)
from vgi.invocation import BindResponse, FunctionType
from vgi.metadata import CatalogFunctionType

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

# A stat value is either a plain Python value (auto-converted using the column's
# Arrow type) or an explicit PyArrow scalar (used as-is).
StatValue = Union[None, bool, int, float, str, bytes, "pa.Scalar"]  # type: ignore[type-arg]

__all__ = [
    "Catalog",
    "ColumnStatisticsInput",
    "DefaultValue",
    "ForeignKeyDef",
    "Index",
    "Macro",
    "Schema",
    "Sql",
    "Table",
    "View",
]


def _to_scalar(
    value: StatValue,
    arrow_type: pa.DataType,
) -> pa.Scalar | None:  # type: ignore[type-arg]
    """Convert a stat value to a PyArrow scalar, inferring type from the column schema."""
    if value is None:
        return None
    if isinstance(value, pa.Scalar):
        return value  # Already a scalar — use as-is
    # Unwrap dictionary type — stats should use the value type so min/max
    # serialize as the actual value, not the dictionary index.
    if pa.types.is_dictionary(arrow_type):
        arrow_type = arrow_type.value_type
    return pa.scalar(value, type=arrow_type)


@dataclass(frozen=True, slots=True)
class ColumnStatisticsInput:
    """Column statistics specified on a Table descriptor.

    Values for ``min`` and ``max`` can be plain Python literals (int, float, str, etc.)
    which are auto-converted to PyArrow scalars using the column's Arrow type from
    the table schema, or explicit ``pa.scalar(...)`` values used as-is.

    Example::

        # Plain Python values — types inferred from schema
        ColumnStatisticsInput(min=1, max=100, has_null=False, distinct_count=100)

        # Explicit PyArrow scalars
        ColumnStatisticsInput(min=pa.scalar(1, pa.int32()), max=pa.scalar(100, pa.int32()))

    """

    min: StatValue = None
    max: StatValue = None
    has_null: bool = True
    has_not_null: bool = True
    distinct_count: int | None = None
    contains_unicode: bool | None = None
    max_string_length: int | None = None

    def resolve(self, column_name: str, arrow_type: pa.DataType) -> ColumnStatistics:
        """Convert to a :class:`ColumnStatistics` with properly typed PyArrow scalars."""
        return ColumnStatistics(
            column_name=column_name,
            min=_to_scalar(self.min, arrow_type),
            max=_to_scalar(self.max, arrow_type),
            has_null=self.has_null,
            has_not_null=self.has_not_null,
            distinct_count=self.distinct_count,
            contains_unicode=self.contains_unicode,
            max_string_length=self.max_string_length,
        )


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


def _inline_function_result(
    func: type[Function] | None,
) -> bytes | None:
    """Build inlined ``ScanFunctionResult`` IPC bytes for a function-backed table.

    Returns ``None`` when the table is not function-backed for that operation.
    Mirrors ``ReadOnlyCatalogInterface._write_function_get`` /
    ``table_scan_function_get`` auto-impl: empty positional/named arguments,
    no required extensions. The C++ extension uses these bytes verbatim and
    skips the corresponding ``catalog_table_*_function_get`` RPC.
    """
    if func is None:
        return None
    func_meta = func.get_metadata()
    return ScanFunctionResult(
        function_name=func_meta.name,
        positional_arguments=[],
        named_arguments={},
        required_extensions=[],
    ).serialize()


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
        generated_columns: Dict mapping column names to SQL expressions
            for generated (virtual) columns. Generated columns are
            computed on read by DuckDB and are mutually exclusive with
            defaults.
        column_comments: Dict mapping column names to comment strings.
            Comments are transported as Arrow field metadata and visible
            via ``duckdb_columns()`` in DuckDB.
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
    generated_columns: dict[str, str] = field(default_factory=dict)
    column_comments: dict[str, str] = field(default_factory=dict)
    statistics: dict[str, ColumnStatisticsInput] = field(default_factory=dict)
    statistics_cache_max_age_seconds: int | None = None
    # Optional inlined cardinality. When set, the C++ extension uses these
    # values directly and skips the per-bind ``table_function_cardinality``
    # RPC. Use for read-only or slow-changing tables. Leave both as ``None``
    # to keep the existing per-bind RPC behavior.
    cardinality_estimate: int | None = None
    cardinality_max: int | None = None
    # Opt into pre-binding the function during ``schema_contents`` and
    # inlining the result on ``TableInfo.bind_result``. The C++ extension
    # then skips the per-scan ``bind`` RPC.
    #
    # Only valid when ``function`` is a ``@bind_fixed_schema``-decorated
    # ``TableFunctionGenerator`` subclass — the decorator's contract (output
    # is exactly ``cls.FIXED_SCHEMA``, no per-call inputs) matches what's
    # safe to freeze for the catalog cache lifetime. Setting this on a
    # descriptor whose function is not decorated raises at descriptor build.
    inline_bind: bool = False
    comment: str | None = None
    tags: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate configuration and constraint column names."""
        # Validate mutually exclusive options
        if self.columns is None and self.function is None:
            raise ValueError(f"Table '{self.name}': must specify either 'columns' or 'function'")
        if self.columns is not None and self.function is not None:
            raise ValueError(f"Table '{self.name}': cannot specify both 'columns' and 'function'")

        # Validate inline_bind contract: only @bind_fixed_schema-decorated
        # functions qualify for the catalog framework's pre-bind path. The
        # decorator marks both the class (_inline_bind_safe=True) and the
        # installed on_bind function (_is_bind_fixed_schema=True). The
        # function-level marker lets us reject subclasses that overrode
        # on_bind even though they inherit _inline_bind_safe via MRO.
        if self.inline_bind:
            if self.function is None:
                raise ValueError(
                    f"Table '{self.name}': inline_bind=True requires function= to be set"
                )
            if not getattr(self.function, "_inline_bind_safe", False):
                raise ValueError(
                    f"Table '{self.name}': inline_bind=True requires the function class "
                    f"to be decorated with @bind_fixed_schema. Got {self.function.__name__}, "
                    f"which has a custom on_bind. Either decorate it (deleting the manual "
                    f"on_bind) or leave inline_bind=False."
                )
            on_bind_attr = self.function.__dict__.get("on_bind")
            if on_bind_attr is not None:
                # The class has its own on_bind in __dict__. Either the
                # decorator installed it (good — has _is_bind_fixed_schema
                # marker on the underlying function) or a subclass overrode
                # it (bad — escapes the decorator's contract).
                underlying = getattr(on_bind_attr, "__func__", on_bind_attr)
                if not getattr(underlying, "_is_bind_fixed_schema", False):
                    raise ValueError(
                        f"Table '{self.name}': inline_bind=True is not safe for "
                        f"{self.function.__name__} because it overrides on_bind, "
                        f"escaping @bind_fixed_schema's contract. Either remove the "
                        f"override or leave inline_bind=False."
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

        # Validate generated_columns column names and no overlap with defaults
        for col in self.generated_columns:
            if col not in column_names:
                raise ValueError(
                    f"Table '{self.name}': generated_columns column '{col}' not found "
                    f"in schema. Available columns: {sorted(column_names)}"
                )
            if col in self.defaults:
                raise ValueError(
                    f"Table '{self.name}': column '{col}' cannot have both a default value and a generated expression"
                )

        # Validate column_comments column names
        for col in self.column_comments:
            if col not in column_names:
                raise ValueError(
                    f"Table '{self.name}': column_comments column '{col}' not found "
                    f"in schema. Available columns: {sorted(column_names)}"
                )

        # Validate statistics column names
        for col in self.statistics:
            if col not in column_names:
                raise ValueError(
                    f"Table '{self.name}': statistics column '{col}' not found "
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
            schema = schema.set(idx, f.with_metadata(existing))  # type: ignore[arg-type]
        return schema

    def _apply_generated_columns_to_schema(self, schema: pa.Schema) -> pa.Schema:
        """Return schema with generated expression metadata applied to fields."""
        if not self.generated_columns:
            return schema
        for col_name, expression in self.generated_columns.items():
            idx = schema.get_field_index(col_name)
            f = schema.field(idx)
            existing = dict(f.metadata) if f.metadata else {}
            existing[b"generated_expression"] = expression.encode("utf-8")
            schema = schema.set(idx, f.with_metadata(existing))  # type: ignore[arg-type]
        return schema

    def _apply_column_comments_to_schema(self, schema: pa.Schema) -> pa.Schema:
        """Return schema with column comment metadata applied to fields."""
        if not self.column_comments:
            return schema
        for col_name, comment in self.column_comments.items():
            if not comment:
                continue
            idx = schema.get_field_index(col_name)
            f = schema.field(idx)
            existing = dict(f.metadata) if f.metadata else {}
            existing[b"comment"] = comment.encode("utf-8")
            schema = schema.set(idx, f.with_metadata(existing))  # type: ignore[arg-type]
        return schema

    def to_table_info(self, schema_name: str) -> TableInfo:
        """Convert to TableInfo for catalog response."""
        cols = self._apply_defaults_to_schema(self.resolved_columns)
        cols = self._apply_generated_columns_to_schema(cols)
        cols = self._apply_column_comments_to_schema(cols)
        # Inline the resolved stats blob so the C++ extension can short-circuit
        # the per-scan ``table_function_statistics`` and per-table
        # ``catalog_table_column_statistics_get`` RPCs entirely. This freezes
        # the resolved stats for the lifetime of the catalog cache; workers
        # whose stats change faster than catalog_version must override
        # ``to_table_info`` and leave column_statistics null.
        resolved_stats = self.resolve_column_statistics()
        column_statistics_blob = (
            serialize_column_statistics(
                resolved_stats.statistics,
                resolved_stats.cache_max_age_seconds,
            )
            if resolved_stats is not None
            else None
        )
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
            supports_column_statistics=bool(self.statistics),
            comment=self.comment,
            tags=dict(self.tags),
            scan_function=_inline_function_result(self.function),
            insert_function=_inline_function_result(self.insert_function),
            update_function=_inline_function_result(self.update_function),
            delete_function=_inline_function_result(self.delete_function),
            cardinality_estimate=self.cardinality_estimate,
            cardinality_max=self.cardinality_max,
            column_statistics=column_statistics_blob,
        )

    def resolve_column_statistics(self) -> TableColumnStatisticsResult | None:
        """Resolve the ``statistics`` dict into a :class:`TableColumnStatisticsResult`.

        Returns ``None`` if no statistics are defined. Otherwise, converts
        each entry to a :class:`ColumnStatistics` with properly typed PyArrow
        scalars inferred from the table's column schema.
        """
        if not self.statistics:
            return None
        resolved_cols = self.resolved_columns
        stats = []
        for col_name, stat_input in self.statistics.items():
            col_field = resolved_cols.field(col_name)
            stats.append(stat_input.resolve(col_name, col_field.type))
        return TableColumnStatisticsResult(
            statistics=stats,
            cache_max_age_seconds=self.statistics_cache_max_age_seconds,
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


@dataclass(frozen=True, slots=True, kw_only=True)
class Index:
    """Declarative index definition.

    Immutable.

    Attributes:
        name: Index name.
        table_name: Name of the table this index is on.
        expressions: SQL expression strings or column names defining the index.
            For column-based indexes: ("col_a", "col_b")
            For expression indexes: ("lower(col_a)", "col_b + 1")
        index_type: The index type (e.g., "" for default).
        constraint_type: NONE for regular, UNIQUE for unique indexes.
        options: Key-value index options.
        comment: Optional index comment.
        tags: Optional metadata tags.

    """

    name: str
    table_name: str
    expressions: tuple[str, ...] = ()
    index_type: str = ""
    constraint_type: IndexConstraintType = IndexConstraintType.NONE
    options: dict[str, str] = field(default_factory=dict)
    comment: str | None = None
    tags: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate index configuration."""
        if not self.expressions:
            raise ValueError(f"Index '{self.name}': must specify at least one expression")
        if not self.table_name:
            raise ValueError(f"Index '{self.name}': must specify a table_name")

    def to_index_info(self, schema_name: str) -> IndexInfo:
        """Convert to IndexInfo for catalog response."""
        return IndexInfo(
            name=self.name,
            schema_name=schema_name,
            table_name=self.table_name,
            index_type=self.index_type,
            constraint_type=self.constraint_type,
            expressions=list(self.expressions),
            options=dict(self.options),
            comment=self.comment,
            tags=dict(self.tags),
        )


@dataclass
class Schema:
    """Declarative schema definition grouping tables, views, functions, macros, and indexes.

    Attributes:
        name: Schema name.
        comment: Optional schema comment.
        tags: Optional metadata tags.
        tables: Sequence of Table definitions.
        views: Sequence of View definitions.
        functions: Sequence of Function classes (scalar, table, or aggregate).
        macros: Sequence of Macro definitions.
        indexes: Sequence of Index definitions.

    """

    name: str
    comment: str | None = None
    tags: dict[str, str] = field(default_factory=dict)
    tables: Sequence[Table] = ()
    views: Sequence[View] = ()
    functions: Sequence[type[Function]] = ()
    macros: Sequence[Macro] = ()
    indexes: Sequence[Index] = ()

    def to_schema_info(self, attach_id: AttachId) -> SchemaInfo:
        """Convert to SchemaInfo for catalog response.

        Populates ``estimated_object_count`` from the declared population so
        the C++ extension's eager-load gate can choose between bulk
        ``LoadEntries`` and per-name single-entry RPCs without an extra round
        trip. Functions are partitioned by ``get_metadata().function_type``
        into the three keys (``scalar_function``, ``aggregate_function``,
        ``table_function``) so DuckDB's per-type catalog probes (a name
        lookup walks scalar → aggregate → table) skip the bulk RPC for any
        category the schema doesn't populate.

        **Zero counts are load-bearing.** Empty declarative collections
        (e.g. ``views=()``) emit ``0`` here, which the C++ client treats as
        a hard guarantee and uses to skip the corresponding bulk + per-name
        RPCs entirely. Do not "optimize" this into omitting empty keys —
        absence reads as count=1 (unknown), suppressing the RPC bypass.
        """
        function_counts = {
            CatalogFunctionType.SCALAR: 0,
            CatalogFunctionType.AGGREGATE: 0,
            CatalogFunctionType.TABLE: 0,
        }
        for func in self.functions:
            function_counts[func.get_metadata().function_type] += 1
        return SchemaInfo(
            attach_id=attach_id,
            name=self.name,
            comment=self.comment,
            tags=dict(self.tags),
            estimated_object_count={
                "table": len(self.tables),
                "view": len(self.views),
                "scalar_function": function_counts[CatalogFunctionType.SCALAR],
                "aggregate_function": function_counts[CatalogFunctionType.AGGREGATE],
                "table_function": function_counts[CatalogFunctionType.TABLE],
                "macro": len(self.macros),
                "index": len(self.indexes),
            },
        )


@dataclass
class Catalog:
    """Declarative catalog definition containing schemas.

    The single entry point for defining all catalog metadata on a Worker.

    Attributes:
        name: The catalog name (used in SQL as the database name).
        default_schema: Schema to use for unqualified table/view/function names.
        schemas: Sequence of Schema objects defining the catalog contents.
        comment: Optional comment describing the catalog.
        tags: Optional key-value tags associated with the catalog.

    """

    name: str
    default_schema: str = "main"
    schemas: Sequence[Schema] = ()
    comment: str | None = None
    tags: dict[str, str] = field(default_factory=dict)

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
