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
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.compute as pc

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

__all__ = [
    "AndFilter",
    "ColumnBounds",
    "ComparisonOp",
    "ConstantFilter",
    "deserialize_filters",
    "Filter",
    "FilterDeserializationError",
    "FilterError",
    "FilterType",
    "FilterVersionError",
    "InFilter",
    "IsNotNullFilter",
    "IsNullFilter",
    "OrFilter",
    "PushdownFilters",
    "StructFilter",
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
    AND = "and"
    OR = "or"
    STRUCT = "struct"


class ComparisonOp(Enum):
    """Comparison operators for constant filters."""

    EQ = "eq"  # =
    NE = "ne"  # !=
    GT = "gt"  # >
    GE = "ge"  # >=
    LT = "lt"  # <
    LE = "le"  # <=


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
        col = batch.column(self.column_index)
        match self.op:
            case ComparisonOp.EQ:
                return pc.equal(col, self.value)
            case ComparisonOp.NE:
                return pc.not_equal(col, self.value)
            case ComparisonOp.GT:
                return pc.greater(col, self.value)
            case ComparisonOp.GE:
                return pc.greater_equal(col, self.value)
            case ComparisonOp.LT:
                return pc.less(col, self.value)
            case ComparisonOp.LE:
                return pc.less_equal(col, self.value)

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        op_symbols = {
            "eq": "=",
            "ne": "!=",
            "gt": ">",
            "ge": ">=",
            "lt": "<",
            "le": "<=",
        }
        op_sym = op_symbols[self.op.value]
        return f"ConstantFilter({self.column_name} {op_sym} {self.value})"


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
        col = batch.column(self.column_index)
        return pc.is_in(col, self.values)

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return f"InFilter({self.column_name} IN ({self.values.to_pylist()}))"


@dataclass(frozen=True, slots=True)
class AndFilter(Filter):
    """Conjunction of child filters.

    All child filters must pass for a row to pass.
    """

    children: tuple[Filter, ...]

    def evaluate(self, batch: pa.RecordBatch) -> pa.BooleanArray:
        """Evaluate AND of all child filters."""
        if not self.children:
            return pa.array([True] * batch.num_rows, type=pa.bool_())
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
            return pa.array([False] * batch.num_rows, type=pa.bool_())
        result = self.children[0].evaluate(batch)
        for child in self.children[1:]:
            result = pc.or_(result, child.evaluate(batch))
        return result

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        children_repr = " OR ".join(repr(c) for c in self.children)
        return f"OrFilter({children_repr})"


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
        # Create temp batch with nested field at index 0
        temp = pa.RecordBatch.from_arrays([nested], names=[self.column_name])
        # Adjust child filter to use column_index=0 for temp batch
        adjusted_child = dataclasses.replace(self.child_filter, column_index=0)
        return adjusted_child.evaluate(temp)

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        nested = f"{self.column_name}.{self.child_name}"
        return f"StructFilter({nested}: {self.child_filter!r})"


# =============================================================================
# Column Bounds Helper
# =============================================================================


@dataclass(frozen=True, slots=True)
class ColumnBounds:
    """Numeric/comparable bounds for a column extracted from filters.

    Use case: Partition pruning, index range scans, bounded data fetches.

    Example:
        # WHERE age >= 18 AND age < 65
        bounds = filters.get_column_bounds("age")
        # bounds.min_value = 18, bounds.min_inclusive = True
        # bounds.max_value = 65, bounds.max_inclusive = False

        # Use for data source optimization
        if bounds.min_value is not None:
            start_from = bounds.min_value.as_py()

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
            if self.min_inclusive:
                if value < min_val:
                    return False
            else:
                if value <= min_val:
                    return False

        if self.max_value is not None:
            max_val = self.max_value.as_py()
            if self.max_inclusive:
                if value > max_val:
                    return False
            else:
                if value >= max_val:
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
    version: str = "1"

    def evaluate(self, batch: pa.RecordBatch) -> pa.BooleanArray:
        """Evaluate all filters, returning boolean mask.

        Filters are combined with AND at the top level - a row passes only
        if ALL filters evaluate to true for that row.

        Args:
            batch: RecordBatch to evaluate filters against.

        Returns:
            Boolean array with True for rows that pass all filters.

        """
        if not self.filters:
            return pa.array([True] * batch.num_rows, type=pa.bool_())
        result = self.filters[0].evaluate(batch)
        for f in self.filters[1:]:
            result = pc.and_(result, f.evaluate(batch))
        return result

    def apply(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Apply all filters to batch, returning filtered batch.

        Args:
            batch: RecordBatch to filter.

        Returns:
            Filtered RecordBatch containing only rows that pass all filters.

        """
        mask = self.evaluate(batch)
        # pc.filter supports RecordBatch but pyarrow-stubs don't have the overload
        return pc.filter(batch, mask)  # type: ignore[call-overload,no-any-return]

    # =========================================================================
    # Column Query Helpers
    # =========================================================================

    @property
    def filtered_columns(self) -> frozenset[str]:
        """Set of column names that have filters applied.

        Use case: Quick check of which columns are constrained.

        Example:
            if "tenant_id" in filters.filtered_columns:
                # Optimize for tenant-filtered query
                ...

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

        Example:
            # WHERE tenant_id = 'abc123'
            tenant = filters.get_column_constant("tenant_id")
            if tenant:
                # Fetch only from tenant's partition
                return fetch_partition(tenant.as_py())

        Args:
            column_name: Name of the column to check.

        Returns:
            The constant value if an equality filter exists, None otherwise.

        """
        for f in self.filters:
            if (
                f.column_name == column_name
                and isinstance(f, ConstantFilter)
                and f.op == ComparisonOp.EQ
            ):
                return f.value
        return None

    def get_column_in_values(self, column_name: str) -> pa.Array[Any] | None:
        """Get IN list values if column has an IN filter.

        Use case: Multi-key lookup, batch fetching.

        Example:
            # WHERE id IN (1, 2, 3)
            ids = filters.get_column_in_values("id")
            if ids:
                return batch_fetch(ids.to_pylist())

        Args:
            column_name: Name of the column to check.

        Returns:
            Arrow array of IN values if an IN filter exists, None otherwise.

        """
        for f in self.filters:
            if f.column_name == column_name and isinstance(f, InFilter):
                return f.values
        return None

    def get_column_values(self, column_name: str) -> pa.Array[Any] | None:
        """Get all distinct values a column could have based on filters.

        Returns values from equality (=) or IN filters as an Arrow array.
        Useful for partition pruning when partitions are keyed by specific values.

        Use case: Partition key lookup, directory-based partitioning.

        Example:
            # WHERE tenant_id = 'abc123'  -> returns array with single value
            # WHERE tenant_id IN ('a', 'b')  -> returns array ['a', 'b']
            # WHERE tenant_id > 5  -> returns None (not discrete values)

            values = filters.get_column_values("tenant_id")
            if values:
                # Scan only relevant partitions
                for val in values.to_pylist():
                    yield from scan_partition(f"tenant={val}")
            else:
                # Scan all partitions (filter not discrete)
                yield from scan_all_partitions()

        Args:
            column_name: Name of the column to check.

        Returns:
            Arrow array of discrete values if available, None otherwise.

        """
        for f in self.filters:
            if f.column_name == column_name:
                if isinstance(f, ConstantFilter) and f.op == ComparisonOp.EQ:
                    # Wrap single value in array for consistent return type
                    arr: pa.Array[Any] = pa.array([f.value.as_py()], type=f.value.type)
                    return arr
                elif isinstance(f, InFilter):
                    return f.values
        return None

    def get_column_bounds(self, column_name: str) -> ColumnBounds | None:
        """Extract numeric bounds from comparison filters.

        Analyzes gt/ge/lt/le filters to determine value range.

        Use case: Range scans, partition pruning, bounded iteration.

        Example:
            # WHERE timestamp >= '2024-01-01' AND timestamp < '2024-02-01'
            bounds = filters.get_column_bounds("timestamp")
            if bounds:
                for partition in get_partitions_in_range(
                    bounds.min_value.as_py(),
                    bounds.max_value.as_py()
                ):
                    yield from scan_partition(partition)

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
        """Recursively collect all filters for a column (including in AND)."""
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
            Tuple of (where_clause, params) - clause excludes "WHERE" keyword

        Example:
            clause, params = filters.to_sql()
            # clause = '"age" >= ? AND "status" = ?'
            # params = [18, 'active']
            cursor.execute(f"SELECT * FROM t WHERE {clause}", params)

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

    def __bool__(self) -> bool:
        """Return True if there are any filters."""
        return len(self.filters) > 0

    def __len__(self) -> int:
        """Return the number of top-level filters."""
        return len(self.filters)

    def __iter__(self) -> Iterator[Filter]:
        """Iterate over top-level filters."""
        return iter(self.filters)

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
        param_offset: Current parameter offset (unused, for future :name style).

    Returns:
        Tuple of (sql_fragment, params).

    """
    col = quote(f.column_name)

    if isinstance(f, ConstantFilter):
        op_map = {
            ComparisonOp.EQ: "=",
            ComparisonOp.NE: "!=",
            ComparisonOp.GT: ">",
            ComparisonOp.GE: ">=",
            ComparisonOp.LT: "<",
            ComparisonOp.LE: "<=",
        }
        return f"{col} {op_map[f.op]} {placeholder}", [f.value.as_py()]

    elif isinstance(f, IsNullFilter):
        return f"{col} IS NULL", []

    elif isinstance(f, IsNotNullFilter):
        return f"{col} IS NOT NULL", []

    elif isinstance(f, InFilter):
        placeholders = ", ".join([placeholder] * len(f.values))
        return f"{col} IN ({placeholders})", f.values.to_pylist()

    elif isinstance(f, AndFilter):
        parts: list[str] = []
        params: list[Any] = []
        for child in f.children:
            offset = param_offset + len(params)
            sql, ps = _filter_to_sql(child, quote, placeholder, offset)
            parts.append(sql)
            params.extend(ps)
        return f"({' AND '.join(parts)})", params

    elif isinstance(f, OrFilter):
        parts = []
        params = []
        for child in f.children:
            offset = param_offset + len(params)
            sql, ps = _filter_to_sql(child, quote, placeholder, offset)
            parts.append(sql)
            params.extend(ps)
        return f"({' OR '.join(parts)})", params

    elif isinstance(f, StructFilter):
        # Struct access varies by database - use dot notation as default
        nested_col = f"{f.column_name}.{f.child_name}"
        # Replace column name in child filter for SQL generation
        return _filter_to_sql(
            f.child_filter, lambda _: quote(nested_col), placeholder, param_offset
        )

    else:
        raise ValueError(f"Unknown filter type: {type(f)}")


# =============================================================================
# Deserialization
# =============================================================================


def deserialize_filters(ipc_bytes: bytes) -> PushdownFilters:
    """Deserialize Arrow IPC bytes to typed AST.

    Args:
        ipc_bytes: Arrow IPC stream bytes from pushdown_filters field.

    Returns:
        PushdownFilters container with parsed filter AST.

    Raises:
        FilterDeserializationError: If parsing fails.
        FilterVersionError: If version is unsupported.

    """
    try:
        reader = pa.ipc.open_stream(ipc_bytes)
        batch = reader.read_next_batch()
    except Exception as e:
        raise FilterDeserializationError(f"Failed to read IPC stream: {e}") from e

    # Validate version
    metadata = batch.schema.field(0).metadata
    if metadata is None:
        raise FilterVersionError("Missing vgi_filter_version metadata")
    version = metadata.get(b"vgi_filter_version", b"").decode()
    if version != "1":
        raise FilterVersionError(f"Unsupported filter version: {version!r}")

    # Parse JSON spec
    try:
        filter_specs = json.loads(batch.column(0)[0].as_py())
    except Exception as e:
        raise FilterDeserializationError(f"Failed to parse filter JSON: {e}") from e

    # Value resolver - returns scalar for value_ref N from column N+1
    def get_value(ref: int) -> pa.Scalar[Any]:
        return batch.column(ref + 1)[0]  # type: ignore[no-any-return]

    # Parse filters
    try:
        filters = tuple(_parse_filter(spec, get_value) for spec in filter_specs)
    except Exception as e:
        raise FilterDeserializationError(f"Failed to parse filters: {e}") from e

    return PushdownFilters(filters=filters, version=version)


def _parse_filter(
    spec: dict[str, Any], get_value: Callable[[int], pa.Scalar[Any]]
) -> Filter:
    """Parse a single filter spec into a typed Filter object.

    Args:
        spec: Filter specification dict from JSON.
        get_value: Function to get Arrow scalar by value_ref index.

    Returns:
        Typed Filter object.

    Raises:
        FilterDeserializationError: If filter type is unknown.

    """
    column_name = spec["column_name"]
    column_index = spec["column_index"]
    filter_type = spec["type"]

    if filter_type == "constant":
        return ConstantFilter(
            column_name=column_name,
            column_index=column_index,
            op=ComparisonOp(spec["op"]),
            value=get_value(spec["value_ref"]),
        )

    elif filter_type == "is_null":
        return IsNullFilter(column_name=column_name, column_index=column_index)

    elif filter_type == "is_not_null":
        return IsNotNullFilter(column_name=column_name, column_index=column_index)

    elif filter_type == "in":
        # value_ref points to a list column; extract the list's values as an array
        list_scalar = get_value(spec["value_ref"])
        # ListScalar.values gives us the underlying array
        # pyarrow-stubs doesn't type ListScalar.values correctly
        values_array: pa.Array[Any] = list_scalar.values  # type: ignore[attr-defined]
        return InFilter(
            column_name=column_name,
            column_index=column_index,
            values=values_array,
        )

    elif filter_type == "and":
        children = tuple(_parse_filter(c, get_value) for c in spec["children"])
        return AndFilter(
            column_name=column_name,
            column_index=column_index,
            children=children,
        )

    elif filter_type == "or":
        children = tuple(_parse_filter(c, get_value) for c in spec["children"])
        return OrFilter(
            column_name=column_name,
            column_index=column_index,
            children=children,
        )

    elif filter_type == "struct":
        child_filter = _parse_filter(spec["child_filter"], get_value)
        return StructFilter(
            column_name=column_name,
            column_index=column_index,
            child_index=spec["child_index"],
            child_name=spec["child_name"],
            child_filter=child_filter,
        )

    else:
        raise FilterDeserializationError(f"Unknown filter type: {filter_type}")
