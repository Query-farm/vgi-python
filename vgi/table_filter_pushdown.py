# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Filter pushdown AST classes for table functions.

This module provides:
- Filter AST classes for representing pushdown filter predicates
- ColumnBounds for extracting numeric bounds from filters
- PushdownFilters container with evaluation and helper methods
- Deserialization from Arrow IPC format

Filter Types:
    ConstantFilter: Comparison with a constant value (=, !=, >, >=, <, <=)
    IsNullFilter: IS NULL check
    IsNotNullFilter: IS NOT NULL check
    InFilter: Set membership (IN clause)
    AndFilter: Conjunction of child filters
    OrFilter: Disjunction of child filters
    StructFilter: Nested struct field filter
"""

from __future__ import annotations

import dataclasses
import json
import logging
import os
import threading
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
import pyarrow.compute as pc

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

# =============================================================================
# Debug Logging
# =============================================================================

# Enable with VGI_FILTER_DEBUG=1 for detailed filter pushdown diagnostics
_FILTER_DEBUG = os.environ.get("VGI_FILTER_DEBUG", "").lower() in ("1", "true", "yes")
_filter_logger = logging.getLogger("vgi.filter_pushdown")


def _log_debug(event: str, **kwargs: Any) -> None:
    """Log a debug message if VGI_FILTER_DEBUG is enabled."""
    if _FILTER_DEBUG:
        extra = " ".join(f"{k}={v}" for k, v in kwargs.items())
        _filter_logger.debug("%s %s", event, extra) if extra else _filter_logger.debug("%s", event)


# Supported filter protocol version
_SUPPORTED_VERSION = "1"


def _strip_extension(x: Any) -> Any:
    """Unwrap a canonical Arrow extension wrapper, returning the storage form.

    Filter literals serialised by the DuckDB VGI extension carry their
    canonical Arrow extension type (e.g. ``arrow.bool8`` for BOOLEAN, see
    ``vgi/duckdb/src/common/arrow/arrow_type_extension.cpp``). PyArrow's
    binary compute kernels are type-pair-keyed and have no entry for
    ``equal(bool, extension<arrow.bool8>)`` — so we must strip the wrapper
    before comparing. Pass the result of this helper to ``pc.equal`` and
    friends.

    For Arrays / ChunkedArrays, returns the ``.storage`` array. For
    Scalars, returns the ``.value`` storage scalar. Plain (non-extension)
    inputs are returned unchanged.

    Note: the type-check uses ``pa.BaseExtensionType`` rather than
    ``pa.ExtensionType`` because canonical Arrow extension types like
    ``Bool8Type`` and ``UuidType`` inherit from ``BaseExtensionType``
    directly without going through the user-extension class.
    """
    if isinstance(x.type, pa.BaseExtensionType):
        if isinstance(x, (pa.Array, pa.ChunkedArray)):
            # Narrowed to Array/ChunkedArray, but an extension-typed one —
            # ``.storage`` exists at runtime; re-widen so mypy allows it.
            return cast(Any, x).storage
        return x.value
    return x


def _normalize_for_compare(col: Any, val: Any) -> tuple[Any, Any]:
    """Bring a (column, scalar) pair into a shape pyarrow.compute can compare.

    Three transforms in order:
      1. Strip canonical Arrow extension wrappers from both sides.
      2. Decode a dictionary-encoded column to its value type. The
         compute kernels (``is_in``, ``equal``, …) accept a dictionary
         column paired with a plain literal, but throw if the literal is
         *also* dictionary-encoded — so decoding the column (rather than
         casting the literal up to a dictionary) is the path that
         resolves. See ``ArrowTypeError: Array type doesn't match type
         of values set``.
      3. If the resulting types still differ (e.g. column is plain
         ``bool_`` while the literal came over the wire as
         ``arrow.bool8`` and stripped to ``int8``), cast the literal to
         the column's type so the kernel resolves.
    """
    col = _strip_extension(col)
    val = _strip_extension(val)
    if pa.types.is_dictionary(col.type):
        col = col.cast(col.type.value_type)
    if val.type != col.type:
        val = val.cast(col.type)
    return col, val


def _make_bool_array(value: bool, length: int) -> pa.BooleanArray:
    """Create a boolean array of constant value.

    Used for empty AND (all True) and empty OR (all False) filter results.
    """
    return pa.repeat(pa.scalar(value), length)


__all__ = [
    # Exceptions
    "FilterError",
    "FilterDeserializationError",
    "FilterVersionError",
    # Enums
    "FilterType",
    "ComparisonOp",
    "ExpressionNodeType",
    # Filter classes
    "Filter",
    "ConstantFilter",
    "IsNullFilter",
    "IsNotNullFilter",
    "InFilter",
    "AndFilter",
    "OrFilter",
    "StructFilter",
    "ExpressionFilter",
    # Expression node classes
    "ExpressionNode",
    "ColumnRefNode",
    "ConstantNode",
    "FunctionNode",
    "ComparisonNode",
    "ConjunctionNode",
    # Helpers
    "ColumnBounds",
    "PushdownFilters",
    # Functions
    "deserialize_filters",
]


# =============================================================================
# Exceptions
# =============================================================================


class FilterError(Exception):
    """Base exception for filter pushdown errors."""


class FilterDeserializationError(FilterError):
    """Failed to parse filter IPC bytes."""


class FilterVersionError(FilterError):
    """Unsupported filter protocol version."""


# =============================================================================
# Enums
# =============================================================================


class FilterType(Enum):
    """Filter type identifiers matching the JSON protocol."""

    CONSTANT = "constant"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    IN = "in"
    JOIN_KEYS = "join_keys"
    AND = "and"
    OR = "or"
    STRUCT = "struct"
    EXPRESSION = "expression"


class ExpressionNodeType(Enum):
    """Expression node type identifiers matching the JSON protocol."""

    COLUMN_REF = "column_ref"
    CONSTANT = "constant"
    FUNCTION = "function"
    COMPARISON = "comparison"
    CONJUNCTION = "conjunction"


class ComparisonOp(Enum):
    """Comparison operators for constant filters."""

    EQ = "eq"  # =
    NE = "ne"  # !=
    GT = "gt"  # >
    GE = "ge"  # >=
    LT = "lt"  # <
    LE = "le"  # <=

    @property
    def symbol(self) -> str:
        """Return the SQL symbol for this operator."""
        symbols = {"eq": "=", "ne": "!=", "gt": ">", "ge": ">=", "lt": "<", "le": "<="}
        return symbols[self.value]


# =============================================================================
# Filter Base Class
# =============================================================================


@dataclass(frozen=True, slots=True)
class Filter:
    """Base class for all filter types.

    Attributes:
        column_name: Name of the column this filter applies to.
        column_index: Index of the column in the output schema.

    """

    column_name: str
    column_index: int

    def evaluate(self, batch: pa.RecordBatch) -> pa.BooleanArray:
        """Evaluate filter against batch using PyArrow compute.

        Args:
            batch: RecordBatch to evaluate filter against.

        Returns:
            Boolean array with True for rows that pass the filter.

        Raises:
            NotImplementedError: Base class does not implement evaluation.

        """
        raise NotImplementedError


# =============================================================================
# Filter Type Classes
# =============================================================================


@dataclass(frozen=True, slots=True)
class ConstantFilter(Filter):
    """Comparison filter: column <op> value.

    Examples:
        age >= 18
        status = 'active'
        price < 100.0

    """

    op: ComparisonOp
    value: pa.Scalar[Any]

    def evaluate(self, batch: pa.RecordBatch) -> pa.BooleanArray:
        """Evaluate comparison against batch column."""
        col, val = _normalize_for_compare(batch.column(self.column_index), self.value)
        # _normalize_for_compare returns (Any, Any) — the compute kernels
        # below resolve to BooleanArray at runtime; cast once on the way out.
        result: Any
        match self.op:
            case ComparisonOp.EQ:
                result = pc.equal(col, val)
            case ComparisonOp.NE:
                result = pc.not_equal(col, val)
            case ComparisonOp.GT:
                result = pc.greater(col, val)
            case ComparisonOp.GE:
                result = pc.greater_equal(col, val)
            case ComparisonOp.LT:
                result = pc.less(col, val)
            case ComparisonOp.LE:
                result = pc.less_equal(col, val)
            case _:
                raise ValueError(f"Unknown comparison operator: {self.op}")
        return cast("pa.BooleanArray", result)

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return f"ConstantFilter({self.column_name} {self.op.symbol} {self.value})"


@dataclass(frozen=True, slots=True)
class IsNullFilter(Filter):
    """IS NULL check filter."""

    def evaluate(self, batch: pa.RecordBatch) -> pa.BooleanArray:
        """Evaluate IS NULL check against batch column."""
        return pc.is_null(batch.column(self.column_index))

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return f"IsNullFilter({self.column_name} IS NULL)"


@dataclass(frozen=True, slots=True)
class IsNotNullFilter(Filter):
    """IS NOT NULL check filter."""

    def evaluate(self, batch: pa.RecordBatch) -> pa.BooleanArray:
        """Evaluate IS NOT NULL check against batch column."""
        return pc.is_valid(batch.column(self.column_index))

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return f"IsNotNullFilter({self.column_name} IS NOT NULL)"


@dataclass(frozen=True, slots=True)
class InFilter(Filter):
    """IN (v1, v2, ...) set membership filter.

    The values are stored as an Arrow array (the contents of the list column).
    """

    values: pa.Array[Any]

    def evaluate(self, batch: pa.RecordBatch) -> pa.BooleanArray:
        """Evaluate IN membership against batch column."""
        col, vals = _normalize_for_compare(batch.column(self.column_index), self.values)
        return cast("pa.BooleanArray", pc.is_in(col, vals))

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        values = self.values.to_pylist()
        preview = f"{values[:3]!r}...({len(values)} total)" if len(values) > 5 else repr(values)
        return f"InFilter({self.column_name} IN {preview})"


@dataclass(frozen=True, slots=True)
class AndFilter(Filter):
    """Conjunction of child filters.

    All child filters must pass for a row to pass.
    """

    children: tuple[Filter, ...]

    def evaluate(self, batch: pa.RecordBatch) -> pa.BooleanArray:
        """Evaluate AND of all child filters."""
        if not self.children:
            return _make_bool_array(True, batch.num_rows)
        result = self.children[0].evaluate(batch)
        for child in self.children[1:]:
            result = pc.and_(result, child.evaluate(batch))
        return result

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        children_repr = " AND ".join(repr(c) for c in self.children)
        return f"AndFilter({children_repr})"


@dataclass(frozen=True, slots=True)
class OrFilter(Filter):
    """Disjunction of child filters.

    At least one child filter must pass for a row to pass.
    """

    children: tuple[Filter, ...]

    def evaluate(self, batch: pa.RecordBatch) -> pa.BooleanArray:
        """Evaluate OR of all child filters."""
        if not self.children:
            return _make_bool_array(False, batch.num_rows)
        result = self.children[0].evaluate(batch)
        for child in self.children[1:]:
            result = pc.or_(result, child.evaluate(batch))
        return result

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        children_repr = " OR ".join(repr(c) for c in self.children)
        return f"OrFilter({children_repr})"


class _SingleColumnBatch:
    """Lightweight wrapper providing batch-like interface for a single array.

    Used by StructFilter to avoid creating a full RecordBatch when evaluating
    child filters on nested struct fields.
    """

    __slots__ = ("_array",)

    def __init__(self, array: pa.Array[Any]) -> None:
        self._array = array

    def column(self, _index: int) -> pa.Array[Any]:
        """Return the wrapped array (index is ignored)."""
        return self._array

    @property
    def num_rows(self) -> int:
        """Return the number of rows in the array."""
        return len(self._array)


@dataclass(frozen=True, slots=True)
class StructFilter(Filter):
    """Nested struct field filter.

    Filters on a nested field within a struct column.
    Example: address.city = 'Seattle'
    """

    child_index: int
    child_name: str
    child_filter: Filter

    def evaluate(self, batch: pa.RecordBatch) -> pa.BooleanArray:
        """Evaluate filter on nested struct field."""
        struct_col = batch.column(self.column_index)
        nested = pc.struct_field(struct_col, self.child_name)
        # Use lightweight wrapper instead of creating a full RecordBatch
        wrapper = _SingleColumnBatch(nested)
        # Adjust child filter to use column_index=0 for the wrapper
        adjusted_child = dataclasses.replace(self.child_filter, column_index=0)
        return adjusted_child.evaluate(wrapper)  # type: ignore[arg-type]

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        nested = f"{self.column_name}.{self.child_name}"
        return f"StructFilter({nested}: {self.child_filter!r})"


# =============================================================================
# Expression Filter (recursive expression tree from DuckDB)
# =============================================================================


@dataclass(frozen=True, slots=True)
class ExpressionNode:
    """Base class for expression tree nodes.

    Subclasses must set ``expr_type`` to match their class. This field
    is used for serialization round-tripping (JSON ``expr_type`` key).
    """

    expr_type: ExpressionNodeType

    def to_sql(self, column_name: str) -> str:
        """Convert node to SQL string. Override in subclasses."""
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class ColumnRefNode(ExpressionNode):
    """Column reference node.

    Note: In v1, all column refs in an expression filter refer to the same
    column (the filter column). The index is stored for future multi-column
    support but to_sql() always uses the filter's column_name.
    """

    index: int

    def to_sql(self, column_name: str) -> str:
        """Return quoted column name with double-quote escaping."""
        escaped = column_name.replace('"', '""')
        return f'"{escaped}"'


@dataclass(frozen=True, slots=True)
class ConstantNode(ExpressionNode):
    """Constant value node."""

    value: pa.Scalar[Any]
    field: pa.Field[Any] | None = None  # Arrow field with extension metadata (if available)

    def to_sql(self, column_name: str) -> str:
        """Format Arrow scalar as SQL literal, using field metadata for extension types."""
        return _arrow_scalar_to_sql(self.value, self.field)


def _is_operator_name(name: str) -> bool:
    """Check if a function name is an infix operator (all non-alphanumeric/underscore chars)."""
    return len(name) > 0 and all(not (c.isalnum() or c == "_") for c in name)


@dataclass(frozen=True, slots=True)
class FunctionNode(ExpressionNode):
    """Function call node."""

    function_name: str
    children: tuple[ExpressionNode, ...]

    def to_sql(self, column_name: str) -> str:
        """Format as function_name(args...) or infix for operators like &&."""
        if _is_operator_name(self.function_name) and len(self.children) == 2:
            left = self.children[0].to_sql(column_name)
            right = self.children[1].to_sql(column_name)
            return f"({left} {self.function_name} {right})"
        args = ", ".join(c.to_sql(column_name) for c in self.children)
        return f"{self.function_name}({args})"


@dataclass(frozen=True, slots=True)
class ComparisonNode(ExpressionNode):
    """Comparison node (left op right)."""

    op: ComparisonOp
    left: ExpressionNode
    right: ExpressionNode

    def to_sql(self, column_name: str) -> str:
        """Format as (left op right)."""
        return f"({self.left.to_sql(column_name)} {self.op.symbol} {self.right.to_sql(column_name)})"


@dataclass(frozen=True, slots=True)
class ConjunctionNode(ExpressionNode):
    """AND/OR conjunction node."""

    conjunction_type: str  # "and" or "or"
    children: tuple[ExpressionNode, ...]

    def to_sql(self, column_name: str) -> str:
        """Format as (child1 AND/OR child2 AND/OR ...)."""
        joiner = " AND " if self.conjunction_type == "and" else " OR "
        parts = [c.to_sql(column_name) for c in self.children]
        return f"({joiner.join(parts)})"


# Arrow extension type name → SQL wrapper for binary values.
# Maps ARROW:extension:name metadata to a function that converts hex to SQL.
_ARROW_EXTENSION_SQL: dict[str, Callable[[str], str]] = {
    "geoarrow.wkb": lambda hex_str: f"ST_GeomFromHEXWKB('{hex_str}')",
}


def _arrow_scalar_to_sql(scalar: pa.Scalar[Any], field: pa.Field[Any] | None = None) -> str:
    """Convert an Arrow scalar to a SQL literal string.

    Uses Arrow field metadata to handle extension types (e.g., geoarrow.wkb
    for geometry). Falls back to generic representations for standard types.

    Args:
        scalar: Arrow scalar value.
        field: Arrow field with extension metadata (optional). Used to detect
               extension types like geoarrow.wkb for proper SQL rendering.

    """
    if scalar.is_valid is False:
        return "NULL"

    val = scalar.as_py()
    typ = scalar.type

    if pa.types.is_boolean(typ):
        return "TRUE" if val else "FALSE"
    elif pa.types.is_integer(typ) or pa.types.is_floating(typ):
        return str(val)
    elif pa.types.is_string(typ) or pa.types.is_large_string(typ):
        escaped = str(val).replace("'", "''")
        return f"'{escaped}'"
    elif pa.types.is_binary(typ) or pa.types.is_large_binary(typ):
        hex_str = val.hex()
        # Check Arrow extension metadata for type-specific rendering
        if field is not None and field.metadata is not None:
            ext_name = field.metadata.get(b"ARROW:extension:name", b"").decode()
            if ext_name in _ARROW_EXTENSION_SQL:
                return _ARROW_EXTENSION_SQL[ext_name](hex_str)
        return f"'\\x{hex_str}'::BLOB"
    else:
        # Fallback: use Python repr as string literal
        escaped = str(val).replace("'", "''")
        return f"'{escaped}'"


_expression_eval_local = threading.local()


def _get_expression_eval_connection() -> Any:
    """Get or create a thread-local DuckDB connection for expression filter evaluation.

    Each thread gets its own connection (DuckDB connections are not thread-safe).
    Spatial extension is loaded if available (needed for geometry functions like &&).
    """
    conn = getattr(_expression_eval_local, "conn", None)
    if conn is not None:
        return conn

    import duckdb

    conn = duckdb.connect()
    try:
        conn.load_extension("spatial")
    except Exception:
        try:
            conn.install_extension("spatial")
            conn.load_extension("spatial")
        except Exception:
            pass  # spatial not available — non-spatial expressions still work
    _expression_eval_local.conn = conn
    return conn


@dataclass(frozen=True, slots=True)
class ExpressionFilter(Filter):
    """Expression tree filter pushed from DuckDB.

    Contains a recursive expression tree that the worker evaluates
    using DuckDB. Typical use: spatial predicates like ``geom && box``.
    """

    expr: ExpressionNode

    def evaluate(self, batch: pa.RecordBatch) -> pa.BooleanArray:
        """Evaluate expression tree against batch using DuckDB.

        Uses a cached per-process DuckDB connection with spatial extension
        pre-loaded (if available). DuckDB is imported lazily — workers that
        don't use expression filters don't need it as a dependency.
        """
        conn = _get_expression_eval_connection()
        tbl = conn.from_arrow(batch)  # noqa: F841 (used in SQL below)
        sql_expr = self.expr.to_sql(self.column_name)
        query_result = conn.sql(f"SELECT ({sql_expr})::BOOLEAN AS _r FROM tbl")
        # Use to_arrow_table if available (duckdb >= 1.5.1), fall back to fetch_arrow_table
        fetch = getattr(query_result, "to_arrow_table", None) or query_result.fetch_arrow_table
        result = fetch()
        return result.column("_r").combine_chunks()  # type: ignore[no-any-return]

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return f"ExpressionFilter({self.column_name}: {self.expr.to_sql(self.column_name)})"


# =============================================================================
# Column Bounds Helper
# =============================================================================


@dataclass(frozen=True, slots=True)
class ColumnBounds:
    """Numeric/comparable bounds for a column extracted from filters.

    Use case: Partition pruning, index range scans, bounded data fetches.

    Attributes:
        min_value: Minimum bound value, or None if unbounded below.
        min_inclusive: True if min_value is inclusive (>=), False if exclusive (>).
        max_value: Maximum bound value, or None if unbounded above.
        max_inclusive: True if max_value is inclusive (<=), False if exclusive (<).

    """

    min_value: pa.Scalar[Any] | None = None
    min_inclusive: bool = True
    max_value: pa.Scalar[Any] | None = None
    max_inclusive: bool = True

    def contains(self, value: Any) -> bool:
        """Check if a value satisfies these bounds.

        Args:
            value: Value to check against bounds.

        Returns:
            True if value is within bounds, False otherwise.

        """
        if self.min_value is not None:
            min_val = self.min_value.as_py()
            below_min = value < min_val if self.min_inclusive else value <= min_val
            if below_min:
                return False

        if self.max_value is not None:
            max_val = self.max_value.as_py()
            above_max = value > max_val if self.max_inclusive else value >= max_val
            if above_max:
                return False

        return True


# =============================================================================
# PushdownFilters Container
# =============================================================================


@dataclass(frozen=True, slots=True)
class PushdownFilters:
    """Container for pushdown filters with evaluation and query helpers.

    The top-level filters array represents a conjunction (AND). Each filter in
    the array must be satisfied for a row to pass. Individual filters may
    themselves be AND/OR compound filters for more complex expressions.

    Provides:
    - evaluate(batch) / apply(batch) - Apply filters using PyArrow compute
    - get_column_bounds(name) - Extract numeric bounds for partition pruning
    - get_column_constant(name) - Get equality constant for a column
    - get_column_in_values(name) - Get IN list values
    - get_column_filters(name) - Get all filters for a column
    - to_sql() - Generate SQL WHERE clause

    """

    filters: tuple[Filter, ...]
    version: str = _SUPPORTED_VERSION
    join_keys_batches: list[pa.RecordBatch] | None = None

    def get_join_keys_batch(self) -> pa.RecordBatch | None:
        """Return a merged join keys batch for temp table registration.

        When all join key batches have the same row count (the semi-join
        case), returns a single RecordBatch with all columns merged.
        When batches have different row counts (independent IN filters),
        they cannot be merged, so this returns ``None``.

        For individual column access, use :meth:`get_join_keys_batches`
        or :meth:`get_column_in_values`.

        Example::

            keys = params.current_pushdown_filters.get_join_keys_batch()
            if keys is not None:
                conn.register("join_keys", keys)
                result = conn.sql(
                    "SELECT d.* FROM my_data d JOIN join_keys USING (id)"
                )

        Returns:
            Merged RecordBatch when all batches have equal row counts,
            or None.

        """
        if not self.join_keys_batches:
            return None
        row_counts = {b.num_rows for b in self.join_keys_batches}
        if len(row_counts) != 1:
            return None
        # All same cardinality — merge into one multi-column batch
        columns: list[pa.Array[Any]] = []
        fields: list[pa.Field[Any]] = []
        for b in self.join_keys_batches:
            for i in range(b.num_columns):
                columns.append(b.column(i))
                fields.append(b.schema.field(i))
        return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))

    def get_join_keys_batches(self) -> list[pa.RecordBatch] | None:
        """Return all join key batches (one per IN filter column).

        Each batch is a single-column RecordBatch. Different batches may have
        different row counts. Returns ``None`` if no join keys were pushed.

        """
        return self.join_keys_batches if self.join_keys_batches else None

    def evaluate(self, batch: pa.RecordBatch) -> pa.BooleanArray:
        """Evaluate all filters, returning boolean mask.

        Filters are combined with AND at the top level - a row passes only
        if ALL filters evaluate to true for that row.

        Args:
            batch: RecordBatch to evaluate filters against.

        Returns:
            Boolean array with True for rows that pass all filters.

        """
        _log_debug(
            "evaluate_start",
            num_filters=len(self.filters),
            input_rows=batch.num_rows,
            columns=[f.column_name for f in self.filters],
        )

        if not self.filters:
            _log_debug("evaluate_no_filters", input_rows=batch.num_rows)
            return _make_bool_array(True, batch.num_rows)

        result = self.filters[0].evaluate(batch)
        # pc.sum works on BooleanArray (counts True values) but stubs don't reflect this
        true_count: int | None = pc.sum(result).as_py()  # type: ignore[type-var]
        _log_debug(
            "evaluate_filter",
            filter_index=0,
            filter_type=type(self.filters[0]).__name__,
            filter_repr=repr(self.filters[0]),
            rows_passing=true_count,
        )

        for i, f in enumerate(self.filters[1:], start=1):
            result = pc.and_(result, f.evaluate(batch))
            true_count = pc.sum(result).as_py()  # type: ignore[type-var]
            _log_debug(
                "evaluate_filter",
                filter_index=i,
                filter_type=type(f).__name__,
                filter_repr=repr(f),
                rows_passing=true_count,
            )

        final_count: int | None = pc.sum(result).as_py()  # type: ignore[type-var]
        _log_debug(
            "evaluate_complete",
            input_rows=batch.num_rows,
            rows_passing=final_count,
            rows_filtered=batch.num_rows - (final_count or 0),
        )
        return result

    def apply(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Apply all filters to batch, returning filtered batch.

        Args:
            batch: RecordBatch to filter.

        Returns:
            Filtered RecordBatch containing only rows that pass all filters.

        """
        _log_debug("apply_start", input_rows=batch.num_rows)
        mask = self.evaluate(batch)
        # pc.filter supports RecordBatch but pyarrow-stubs don't have the overload
        filtered: pa.RecordBatch = pc.filter(batch, mask)  # type: ignore[call-overload]
        _log_debug(
            "apply_complete",
            input_rows=batch.num_rows,
            output_rows=filtered.num_rows,
            rows_removed=batch.num_rows - filtered.num_rows,
        )
        return filtered

    # =========================================================================
    # Column Query Helpers
    # =========================================================================

    @property
    def filtered_columns(self) -> frozenset[str]:
        """Set of column names that have filters applied.

        Use case: Quick check of which columns are constrained.
        """
        return frozenset(f.column_name for f in self.filters)

    def get_column_filters(self, column_name: str) -> list[Filter]:
        """Get all top-level filters for a specific column.

        Use case: Inspect what constraints apply to a column.

        Args:
            column_name: Name of the column to get filters for.

        Returns:
            List of filters that apply to the column.

        """
        return [f for f in self.filters if f.column_name == column_name]

    def has_filter_for_column(self, column_name: str) -> bool:
        """Check if any filter constrains the given column.

        Args:
            column_name: Name of the column to check.

        Returns:
            True if at least one filter applies to the column.

        """
        return any(f.column_name == column_name for f in self.filters)

    def get_column_constant(self, column_name: str) -> pa.Scalar[Any] | None:
        """Get constant value if column has an equality filter.

        Use case: Partition key lookup, exact match optimization.

        Args:
            column_name: Name of the column to check.

        Returns:
            The constant value if an equality filter exists, None otherwise.

        """
        # Descend one level into AndFilter children (consistent with
        # get_column_bounds): DuckDB commonly pushes `col = v` / `col IN (...)`
        # conjoined with derived range bounds as a single AndFilter.
        for f in self._collect_column_filters(column_name):
            if isinstance(f, ConstantFilter) and f.op == ComparisonOp.EQ:
                return f.value
        return None

    def get_column_in_values(self, column_name: str) -> pa.Array[Any] | None:
        """Get IN list values if column has an IN filter.

        Use case: Multi-key lookup, batch fetching.

        Args:
            column_name: Name of the column to check.

        Returns:
            Arrow array of IN values if an IN filter exists, None otherwise.

        """
        # Descend one level into AndFilter children (consistent with
        # get_column_bounds): an `IN (...)` predicate is frequently pushed as
        # AndFilter(InFilter, >=min, <=max).
        for f in self._collect_column_filters(column_name):
            if isinstance(f, InFilter):
                return f.values
        return None

    def get_column_values(self, column_name: str) -> pa.Array[Any] | None:
        """Get all distinct values a column could have based on filters.

        Returns values from equality (=) or IN filters as an Arrow array.
        Useful for partition pruning when partitions are keyed by specific values.

        Use case: Partition key lookup, directory-based partitioning.

        Args:
            column_name: Name of the column to check.

        Returns:
            Arrow array of discrete values if available, None otherwise.

        """
        # Descend one level into AndFilter children (consistent with
        # get_column_bounds). DuckDB pushes `col = v` / `col IN (...)` conjoined
        # with derived range bounds as a single AndFilter; without this descent
        # the discrete-value fast path silently misses those and callers fall
        # back to scanning every partition.
        for f in self._collect_column_filters(column_name):
            if isinstance(f, ConstantFilter) and f.op == ComparisonOp.EQ:
                # Wrap single value in array for consistent return type
                arr: pa.Array[Any] = pa.array([f.value.as_py()], type=f.value.type)
                return arr
            elif isinstance(f, InFilter):
                return f.values
            elif isinstance(f, OrFilter):
                # `col = a OR col = b`, `col IN (...) OR col = c`, etc. The
                # column's possible values are the UNION of the branches — but
                # only when *every* branch pins this column to discrete values.
                # If any branch is a range/IS NULL, or constrains a different
                # column (so this column is unbounded in that branch), the set
                # is not enumerable and we must fall through to None. Unlike the
                # AND case, returning one branch's values would be an unsafe
                # subset (a pruning caller would skip the other branches' rows).
                union = self._or_discrete_values(f, column_name)
                if union is not None:
                    return union
        return None

    def _or_discrete_values(self, or_filter: OrFilter, column_name: str) -> pa.Array[Any] | None:
        """Union of discrete values for ``column_name`` across all OR branches.

        Returns the deduplicated union iff every child constrains ``column_name``
        to discrete values (``=`` or ``IN``); otherwise None (the column could
        take any value through a non-discrete branch, so it cannot be enumerated).
        Descends one level into ``OrFilter`` children, consistent with the
        single-level descent used elsewhere; deeper nesting yields None.
        """
        values: list[Any] = []
        for child in or_filter.children:
            if child.column_name != column_name:
                # A branch that constrains some other column (or none) leaves
                # this column unbounded within that branch -> not enumerable.
                return None
            if isinstance(child, ConstantFilter) and child.op == ComparisonOp.EQ:
                values.append(child.value.as_py())
            elif isinstance(child, InFilter):
                values.extend(child.values.to_pylist())
            else:
                return None
        if not values:
            return None
        # Deduplicate, preserving first-seen order for stable output.
        seen: set[Any] = set()
        deduped: list[Any] = []
        for v in values:
            if v not in seen:
                seen.add(v)
                deduped.append(v)
        result: pa.Array[Any] = pa.array(deduped)
        return result

    def get_column_bounds(self, column_name: str) -> ColumnBounds | None:
        """Extract numeric bounds from comparison filters.

        Analyzes gt/ge/lt/le filters to determine value range.

        Use case: Range scans, partition pruning, bounded iteration.

        Args:
            column_name: Name of the column to extract bounds for.

        Returns:
            ColumnBounds with min/max values if bounds exist, None otherwise.

        """
        min_val: pa.Scalar[Any] | None = None
        min_inc = True
        max_val: pa.Scalar[Any] | None = None
        max_inc = True

        for f in self._collect_column_filters(column_name):
            if isinstance(f, ConstantFilter):
                if f.op == ComparisonOp.GT:
                    if min_val is None or f.value.as_py() > min_val.as_py():
                        min_val, min_inc = f.value, False
                elif f.op == ComparisonOp.GE:
                    if min_val is None or f.value.as_py() >= min_val.as_py():
                        min_val, min_inc = f.value, True
                elif f.op == ComparisonOp.LT:
                    if max_val is None or f.value.as_py() < max_val.as_py():
                        max_val, max_inc = f.value, False
                elif f.op == ComparisonOp.LE:
                    if max_val is None or f.value.as_py() <= max_val.as_py():
                        max_val, max_inc = f.value, True
                elif f.op == ComparisonOp.EQ:
                    # Equality implies exact bounds
                    return ColumnBounds(f.value, True, f.value, True)

        if min_val is None and max_val is None:
            return None
        return ColumnBounds(min_val, min_inc, max_val, max_inc)

    def _collect_column_filters(self, column_name: str) -> list[Filter]:
        """Collect filters for a column from top-level and direct AND children.

        Note: Only descends one level into AndFilter children. Deeply nested
        AND filters (AND within AND) are not traversed. This is sufficient
        for most query patterns where bounds filters are either at top level
        or grouped in a single AND.
        """
        result: list[Filter] = []
        for f in self.filters:
            if f.column_name == column_name:
                if isinstance(f, AndFilter):
                    result.extend(c for c in f.children if c.column_name == column_name)
                else:
                    result.append(f)
        return result

    # =========================================================================
    # SQL Generation
    # =========================================================================

    def to_sql(
        self,
        quote_identifier: Callable[[str], str] | None = None,
        placeholder: str = "?",
    ) -> tuple[str, list[Any]]:
        """Convert filters to SQL WHERE clause with parameters.

        Args:
            quote_identifier: Function to quote column names (default: double quotes)
            placeholder: Parameter placeholder style ("?", "%s", ":name")

        Returns:
            Tuple of (where_clause, params) - clause excludes "WHERE" keyword.

        """
        if not self.filters:
            return "", []

        quote = quote_identifier or (lambda s: f'"{s}"')
        conditions: list[str] = []
        params: list[Any] = []

        for f in self.filters:
            sql, ps = _filter_to_sql(f, quote, placeholder, len(params))
            conditions.append(sql)
            params.extend(ps)

        return " AND ".join(conditions), params

    # =========================================================================
    # Dunder Methods
    # =========================================================================

    @classmethod
    def empty(cls) -> PushdownFilters:
        """Create an empty PushdownFilters instance (no filters)."""
        return cls(filters=())

    def __bool__(self) -> bool:
        """Return True if there are any filters."""
        return len(self.filters) > 0

    def __len__(self) -> int:
        """Return the number of top-level filters."""
        return len(self.filters)

    def __iter__(self) -> Iterator[Filter]:
        """Iterate over top-level filters."""
        return iter(self.filters)

    def __contains__(self, column_name: str) -> bool:
        """Check if any filter constrains the given column.

        Allows 'column_name in filters' syntax.
        """
        return any(f.column_name == column_name for f in self.filters)

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        if not self.filters:
            return "PushdownFilters([])"
        filters_repr = ", ".join(repr(f) for f in self.filters)
        return f"PushdownFilters([{filters_repr}])"


# =============================================================================
# SQL Generation Helper
# =============================================================================


def _filter_to_sql(
    f: Filter,
    quote: Callable[[str], str],
    placeholder: str,
    param_offset: int,
) -> tuple[str, list[Any]]:
    """Convert a single filter to SQL fragment.

    Args:
        f: Filter to convert.
        quote: Function to quote identifiers.
        placeholder: Parameter placeholder style.
        param_offset: Current parameter offset (for recursive calls).

    Returns:
        Tuple of (sql_fragment, params).

    """
    col = quote(f.column_name)

    match f:
        case ConstantFilter(op=op, value=value):
            return f"{col} {op.symbol} {placeholder}", [value.as_py()]

        case IsNullFilter():
            return f"{col} IS NULL", []

        case IsNotNullFilter():
            return f"{col} IS NOT NULL", []

        case InFilter(values=values):
            placeholders = ", ".join([placeholder] * len(values))
            return f"{col} IN ({placeholders})", values.to_pylist()

        case AndFilter(children=children):
            parts: list[str] = []
            params: list[Any] = []
            for child in children:
                offset = param_offset + len(params)
                sql, ps = _filter_to_sql(child, quote, placeholder, offset)
                parts.append(sql)
                params.extend(ps)
            return f"({' AND '.join(parts)})", params

        case OrFilter(children=children):
            parts = []
            params = []
            for child in children:
                offset = param_offset + len(params)
                sql, ps = _filter_to_sql(child, quote, placeholder, offset)
                parts.append(sql)
                params.extend(ps)
            return f"({' OR '.join(parts)})", params

        case StructFilter(child_name=child_name, child_filter=child_filter):
            # Struct access varies by database - use dot notation as default
            nested_col = f"{f.column_name}.{child_name}"
            return _filter_to_sql(child_filter, lambda _: quote(nested_col), placeholder, param_offset)

        case ExpressionFilter(expr=expr):
            # Expression filters use inline constants in to_sql() because
            # they may contain geometry literals that can't be parameterized.
            # Constants are embedded directly in the SQL string.
            return expr.to_sql(f.column_name), []

        case _:
            raise ValueError(f"Unknown filter type: {type(f)}")


# =============================================================================
# Deserialization
# =============================================================================


def deserialize_filters(
    batch: pa.RecordBatch,
    join_keys: list[pa.RecordBatch] | None = None,
) -> PushdownFilters:
    """Deserialize Arrow IPC bytes to typed AST.

    Args:
        batch: Arrow RecordBatch containing the serialized filters.
        join_keys: Optional list of single-column Arrow RecordBatches, one per
            IN filter column. Each batch may have a different row count.
            Referenced by ``join_keys`` filter type entries in the filter spec.

    Returns:
        PushdownFilters container with parsed filter AST.

    Raises:
        FilterDeserializationError: If parsing fails.
        FilterVersionError: If version is unsupported.

    """
    # Validate version
    metadata = batch.schema.field(0).metadata
    if metadata is None:
        raise FilterVersionError("Missing vgi_filter_version metadata")
    version = metadata.get(b"vgi_filter_version", b"").decode()
    if version != _SUPPORTED_VERSION:
        raise FilterVersionError(f"Unsupported filter version: {version!r}")

    _log_debug("deserialize_version", version=version)

    # Parse JSON spec
    try:
        filter_specs = json.loads(batch.column(0)[0].as_py())
    except Exception as e:
        _log_debug("deserialize_json_error", error=str(e))
        raise FilterDeserializationError(f"Failed to parse filter JSON: {e}") from e

    _log_debug("deserialize_specs", num_filters=len(filter_specs), specs=filter_specs)

    # Value resolver - returns scalar for value_ref N from column N+1
    def get_value(ref: int) -> pa.Scalar[Any]:
        value = batch.column(ref + 1)[0]
        _log_debug(
            "deserialize_value_ref",
            ref=ref,
            column_index=ref + 1,
            value_type=str(value.type),
            value=str(value),
        )
        return value  # type: ignore[no-any-return]

    def get_field(ref: int) -> pa.Field[Any]:
        """Get the Arrow field for a value_ref (column ref+1 in the batch)."""
        return batch.schema.field(ref + 1)

    def get_join_keys_column(column_name: str) -> pa.Array[Any] | None:
        """Resolve a column from the join keys batches by name."""
        if not join_keys:
            return None
        for keys_batch in join_keys:
            try:
                return keys_batch.column(column_name)
            except KeyError:
                continue
        return None

    # Parse filters
    try:
        parsed: list[Filter] = []
        for spec in filter_specs:
            f = _parse_filter(spec, get_value, get_field, get_join_keys_column)
            if f is not None:
                parsed.append(f)
        filters = tuple(parsed)
    except Exception as e:
        _log_debug("deserialize_parse_error", error=str(e))
        raise FilterDeserializationError(f"Failed to parse filters: {e}") from e

    _log_debug(
        "deserialize_complete",
        num_filters=len(filters),
        filter_types=[type(f).__name__ for f in filters],
        columns=[f.column_name for f in filters],
    )

    return PushdownFilters(filters=filters, version=version, join_keys_batches=join_keys)


def _parse_filter(
    spec: dict[str, Any],
    get_value: Callable[[int], pa.Scalar[Any]],
    get_field: Callable[[int], pa.Field[Any]],
    get_join_keys_column: Callable[[str], pa.Array[Any] | None] | None = None,
) -> Filter | None:
    """Parse a single filter spec into a typed Filter object.

    Args:
        spec: Filter specification dict from JSON.
        get_value: Function to get Arrow scalar by value_ref index.
        get_field: Function to get Arrow field by value_ref index (for extension metadata).
        get_join_keys_column: Function to resolve a column from the join keys batch by name.
            Returns None if no join keys batch or column not found.

    Returns:
        Typed Filter object, or None if the filter references missing join keys.

    Raises:
        FilterDeserializationError: If filter type is unknown.

    """
    column_name = spec["column_name"]
    column_index = spec["column_index"]
    filter_type = spec["type"]

    _log_debug(
        "parse_filter_start",
        filter_type=filter_type,
        column_name=column_name,
        column_index=column_index,
    )

    if filter_type == FilterType.CONSTANT.value:
        op = ComparisonOp(spec["op"])
        value = get_value(spec["value_ref"])
        result = ConstantFilter(
            column_name=column_name,
            column_index=column_index,
            op=op,
            value=value,
        )
        _log_debug(
            "parse_filter_constant",
            column=column_name,
            op=op.value,
            value=str(value),
            value_type=str(value.type),
        )
        return result

    elif filter_type == FilterType.IS_NULL.value:
        _log_debug("parse_filter_is_null", column=column_name)
        return IsNullFilter(column_name=column_name, column_index=column_index)

    elif filter_type == FilterType.IS_NOT_NULL.value:
        _log_debug("parse_filter_is_not_null", column=column_name)
        return IsNotNullFilter(column_name=column_name, column_index=column_index)

    elif filter_type == FilterType.IN.value:
        # value_ref points to a list column; extract the list's values as an array
        list_scalar = get_value(spec["value_ref"])
        # ListScalar.values gives us the underlying array
        # pyarrow-stubs doesn't type ListScalar.values correctly
        values_array: pa.Array[Any] = list_scalar.values  # type: ignore[attr-defined]
        _log_debug(
            "parse_filter_in",
            column=column_name,
            num_values=len(values_array),
            values=values_array.to_pylist(),
            value_type=str(values_array.type),
        )
        return InFilter(
            column_name=column_name,
            column_index=column_index,
            values=values_array,
        )

    elif filter_type == FilterType.JOIN_KEYS.value:
        keys_column_name = spec["keys_column"]
        join_values: pa.Array[Any] | None = get_join_keys_column(keys_column_name) if get_join_keys_column else None
        if join_values is None:
            _log_debug(
                "parse_filter_join_keys_missing",
                column=column_name,
                keys_column=keys_column_name,
            )
            return None  # graceful degradation — DuckDB filters client-side
        _log_debug(
            "parse_filter_join_keys",
            column=column_name,
            keys_column=keys_column_name,
            num_values=len(join_values),
            value_type=str(join_values.type),
        )
        return InFilter(
            column_name=column_name,
            column_index=column_index,
            values=join_values,
        )

    elif filter_type == FilterType.AND.value:
        _log_debug(
            "parse_filter_and_start",
            column=column_name,
            num_children=len(spec["children"]),
        )
        children = tuple(
            f
            for c in spec["children"]
            if (f := _parse_filter(c, get_value, get_field, get_join_keys_column)) is not None
        )
        _log_debug("parse_filter_and_complete", column=column_name)
        return AndFilter(
            column_name=column_name,
            column_index=column_index,
            children=children,
        )

    elif filter_type == FilterType.OR.value:
        _log_debug(
            "parse_filter_or_start",
            column=column_name,
            num_children=len(spec["children"]),
        )
        parsed_children: list[Filter] = []
        for c in spec["children"]:
            child = _parse_filter(c, get_value, get_field, get_join_keys_column)
            if child is None:
                # Dropping a child from OR would strengthen the filter (fewer rows pass),
                # which is wrong for graceful degradation. Drop the entire OR instead.
                _log_debug("parse_filter_or_child_missing", column=column_name)
                return None
            parsed_children.append(child)
        children = tuple(parsed_children)
        _log_debug("parse_filter_or_complete", column=column_name)
        return OrFilter(
            column_name=column_name,
            column_index=column_index,
            children=children,
        )

    elif filter_type == FilterType.STRUCT.value:
        child_name = spec["child_name"]
        _log_debug(
            "parse_filter_struct_start",
            column=column_name,
            child_name=child_name,
        )
        child_filter = _parse_filter(spec["child_filter"], get_value, get_field, get_join_keys_column)
        if child_filter is None:
            return None
        _log_debug("parse_filter_struct_complete", column=column_name)
        return StructFilter(
            column_name=column_name,
            column_index=column_index,
            child_index=spec["child_index"],
            child_name=child_name,
            child_filter=child_filter,
        )

    elif filter_type == FilterType.EXPRESSION.value:
        _log_debug("parse_filter_expression_start", column=column_name)
        expr = _parse_expression_node(spec["expr"], get_value, get_field)
        _log_debug("parse_filter_expression_complete", column=column_name)
        return ExpressionFilter(
            column_name=column_name,
            column_index=column_index,
            expr=expr,
        )

    else:
        _log_debug("parse_filter_unknown", filter_type=filter_type)
        raise FilterDeserializationError(f"Unknown filter type: {filter_type}")


def _parse_expression_node(
    spec: dict[str, Any],
    get_value: Callable[[int], pa.Scalar[Any]],
    get_field: Callable[[int], pa.Field[Any]],
) -> ExpressionNode:
    """Parse a single expression node from a JSON spec.

    Args:
        spec: Expression node specification dict from JSON.
        get_value: Function to get Arrow scalar by value_ref index.
        get_field: Function to get Arrow field by value_ref index (for extension metadata).

    Returns:
        Typed ExpressionNode.

    Raises:
        FilterDeserializationError: If expression node type is unknown.

    """
    expr_type = spec["expr_type"]

    if expr_type == ExpressionNodeType.COLUMN_REF.value:
        return ColumnRefNode(expr_type=ExpressionNodeType.COLUMN_REF, index=spec["index"])

    elif expr_type == ExpressionNodeType.CONSTANT.value:
        ref = spec["value_ref"]
        return ConstantNode(
            expr_type=ExpressionNodeType.CONSTANT,
            value=get_value(ref),
            field=get_field(ref),
        )

    elif expr_type == ExpressionNodeType.FUNCTION.value:
        children = tuple(_parse_expression_node(c, get_value, get_field) for c in spec["children"])
        return FunctionNode(
            expr_type=ExpressionNodeType.FUNCTION,
            function_name=spec["function_name"],
            children=children,
        )

    elif expr_type == ExpressionNodeType.COMPARISON.value:
        return ComparisonNode(
            expr_type=ExpressionNodeType.COMPARISON,
            op=ComparisonOp(spec["op"]),
            left=_parse_expression_node(spec["left"], get_value, get_field),
            right=_parse_expression_node(spec["right"], get_value, get_field),
        )

    elif expr_type == ExpressionNodeType.CONJUNCTION.value:
        children = tuple(_parse_expression_node(c, get_value, get_field) for c in spec["children"])
        return ConjunctionNode(
            expr_type=ExpressionNodeType.CONJUNCTION,
            conjunction_type=spec["conjunction_type"],
            children=children,
        )

    else:
        raise FilterDeserializationError(f"Unknown expression node type: {expr_type}")
