"""Unit tests for table_filter_pushdown.py convenience APIs and edge cases."""

from __future__ import annotations

import json

import pyarrow as pa
import pytest

from vgi.table_filter_pushdown import (
    AndFilter,
    ColumnBounds,
    ColumnRefNode,
    ComparisonNode,
    ComparisonOp,
    ConjunctionNode,
    ConstantFilter,
    ConstantNode,
    ExpressionFilter,
    ExpressionNodeType,
    Filter,
    FilterDeserializationError,
    FilterVersionError,
    FunctionNode,
    InFilter,
    IsNotNullFilter,
    IsNullFilter,
    OrFilter,
    PushdownFilters,
    StructFilter,
    _arrow_scalar_to_sql,
    _filter_to_sql,
    deserialize_filters,
)

# =============================================================================
# Helpers
# =============================================================================


def _batch(*columns: tuple[str, list[object]]) -> pa.RecordBatch:
    """Create a RecordBatch from (name, values) pairs."""
    return pa.RecordBatch.from_pydict({name: vals for name, vals in columns})


def _const(col: str, idx: int, op: ComparisonOp, value: int) -> ConstantFilter:
    """Build a ConstantFilter with the given comparison operator."""
    return ConstantFilter(column_name=col, column_index=idx, op=op, value=pa.scalar(value))


def _eq(col: str, idx: int, value: int) -> ConstantFilter:
    """Build an equality ConstantFilter."""
    return _const(col, idx, ComparisonOp.EQ, value)


def _filters(*fs: Filter) -> PushdownFilters:
    """Build a PushdownFilters from filter instances."""
    return PushdownFilters(filters=fs)


# =============================================================================
# TestColumnBounds
# =============================================================================


class TestColumnBounds:
    """Tests for ColumnBounds.contains()."""

    def test_contains_both_inclusive(self) -> None:
        """Inclusive bounds include endpoints."""
        b = ColumnBounds(pa.scalar(2), True, pa.scalar(8), True)
        assert b.contains(2)
        assert b.contains(5)
        assert b.contains(8)
        assert not b.contains(1)
        assert not b.contains(9)

    def test_contains_both_exclusive(self) -> None:
        """Exclusive bounds exclude endpoints."""
        b = ColumnBounds(pa.scalar(2), False, pa.scalar(8), False)
        assert not b.contains(2)
        assert b.contains(3)
        assert b.contains(7)
        assert not b.contains(8)

    def test_contains_min_only(self) -> None:
        """Min-only bound is unbounded above."""
        b = ColumnBounds(pa.scalar(5), True, None, True)
        assert b.contains(5)
        assert b.contains(100)
        assert not b.contains(4)

    def test_contains_max_only(self) -> None:
        """Max-only bound is unbounded below."""
        b = ColumnBounds(None, True, pa.scalar(5), True)
        assert b.contains(5)
        assert b.contains(-100)
        assert not b.contains(6)

    def test_contains_unbounded(self) -> None:
        """Unbounded accepts everything."""
        b = ColumnBounds()
        assert b.contains(0)
        assert b.contains(999)

    def test_contains_min_exclusive(self) -> None:
        """Exclusive min excludes the boundary value."""
        b = ColumnBounds(pa.scalar(5), False, None, True)
        assert not b.contains(5)
        assert b.contains(6)

    def test_contains_max_exclusive(self) -> None:
        """Exclusive max excludes the boundary value."""
        b = ColumnBounds(None, True, pa.scalar(5), False)
        assert not b.contains(5)
        assert b.contains(4)


# =============================================================================
# TestPushdownFiltersQuery
# =============================================================================


class TestPushdownFiltersQuery:
    """Tests for PushdownFilters query helper methods."""

    def test_filtered_columns(self) -> None:
        """filtered_columns returns set of column names."""
        pf = _filters(_eq("a", 0, 1), _eq("b", 1, 2))
        assert pf.filtered_columns == frozenset({"a", "b"})

    def test_filtered_columns_empty(self) -> None:
        """Empty filters have no filtered columns."""
        assert PushdownFilters.empty().filtered_columns == frozenset()

    def test_get_column_filters(self) -> None:
        """get_column_filters returns filters for a specific column."""
        f1 = _eq("a", 0, 1)
        f2 = _const("a", 0, ComparisonOp.LT, 10)
        f3 = _eq("b", 1, 5)
        pf = _filters(f1, f2, f3)
        assert pf.get_column_filters("a") == [f1, f2]
        assert pf.get_column_filters("b") == [f3]
        assert pf.get_column_filters("c") == []

    def test_has_filter_for_column(self) -> None:
        """has_filter_for_column checks column presence."""
        pf = _filters(_eq("a", 0, 1))
        assert pf.has_filter_for_column("a")
        assert not pf.has_filter_for_column("b")

    def test_get_column_constant(self) -> None:
        """get_column_constant returns equality value."""
        pf = _filters(_eq("a", 0, 42))
        result = pf.get_column_constant("a")
        assert result is not None
        assert result.as_py() == 42
        assert pf.get_column_constant("b") is None

    def test_get_column_constant_ignores_non_eq(self) -> None:
        """get_column_constant ignores non-equality filters."""
        pf = _filters(_const("a", 0, ComparisonOp.GT, 5))
        assert pf.get_column_constant("a") is None

    def test_get_column_in_values(self) -> None:
        """get_column_in_values returns IN filter values."""
        inf = InFilter(column_name="a", column_index=0, values=pa.array([1, 2, 3]))
        pf = _filters(inf)
        result = pf.get_column_in_values("a")
        assert result is not None
        assert result.to_pylist() == [1, 2, 3]
        assert pf.get_column_in_values("b") is None

    def test_get_column_values_eq(self) -> None:
        """get_column_values wraps equality value in array."""
        pf = _filters(_eq("a", 0, 7))
        result = pf.get_column_values("a")
        assert result is not None
        assert result.to_pylist() == [7]

    def test_get_column_values_in(self) -> None:
        """get_column_values returns IN values directly."""
        inf = InFilter(column_name="a", column_index=0, values=pa.array([10, 20]))
        pf = _filters(inf)
        result = pf.get_column_values("a")
        assert result is not None
        assert result.to_pylist() == [10, 20]

    def test_get_column_values_none(self) -> None:
        """get_column_values returns None for non-discrete filters."""
        pf = _filters(_const("a", 0, ComparisonOp.GT, 5))
        assert pf.get_column_values("a") is None

    def test_get_column_bounds_range(self) -> None:
        """get_column_bounds extracts range from GE/LT filters."""
        pf = _filters(
            _const("a", 0, ComparisonOp.GE, 3),
            _const("a", 0, ComparisonOp.LT, 10),
        )
        bounds = pf.get_column_bounds("a")
        assert bounds is not None
        assert bounds.min_value is not None
        assert bounds.max_value is not None
        assert bounds.min_value.as_py() == 3
        assert bounds.min_inclusive is True
        assert bounds.max_value.as_py() == 10
        assert bounds.max_inclusive is False

    def test_get_column_bounds_eq(self) -> None:
        """Equality filter produces exact bounds."""
        pf = _filters(_eq("a", 0, 5))
        bounds = pf.get_column_bounds("a")
        assert bounds is not None
        assert bounds.min_value is not None
        assert bounds.max_value is not None
        assert bounds.min_value.as_py() == 5
        assert bounds.max_value.as_py() == 5
        assert bounds.min_inclusive is True
        assert bounds.max_inclusive is True

    def test_get_column_bounds_none(self) -> None:
        """No filters for column returns None."""
        pf = _filters(_eq("b", 1, 5))
        assert pf.get_column_bounds("a") is None

    def test_get_column_bounds_gt(self) -> None:
        """GT filter produces exclusive lower bound."""
        pf = _filters(_const("a", 0, ComparisonOp.GT, 5))
        bounds = pf.get_column_bounds("a")
        assert bounds is not None
        assert bounds.min_value is not None
        assert bounds.min_value.as_py() == 5
        assert bounds.min_inclusive is False
        assert bounds.max_value is None

    def test_get_column_bounds_le(self) -> None:
        """LE filter produces inclusive upper bound."""
        pf = _filters(_const("a", 0, ComparisonOp.LE, 10))
        bounds = pf.get_column_bounds("a")
        assert bounds is not None
        assert bounds.min_value is None
        assert bounds.max_value is not None
        assert bounds.max_value.as_py() == 10
        assert bounds.max_inclusive is True

    def test_collect_column_filters_with_and(self) -> None:
        """AND children are unwrapped for bounds collection."""
        child1 = _const("a", 0, ComparisonOp.GE, 3)
        child2 = _const("a", 0, ComparisonOp.LT, 10)
        and_f = AndFilter(column_name="a", column_index=0, children=(child1, child2))
        pf = _filters(and_f)
        bounds = pf.get_column_bounds("a")
        assert bounds is not None
        assert bounds.min_value is not None
        assert bounds.max_value is not None
        assert bounds.min_value.as_py() == 3
        assert bounds.max_value.as_py() == 10

    def test_get_column_bounds_tighter_wins(self) -> None:
        """When multiple bounds exist, tighter bound wins."""
        pf = _filters(
            _const("a", 0, ComparisonOp.GE, 3),
            _const("a", 0, ComparisonOp.GE, 5),  # tighter
            _const("a", 0, ComparisonOp.LE, 10),
            _const("a", 0, ComparisonOp.LE, 8),  # tighter
        )
        bounds = pf.get_column_bounds("a")
        assert bounds is not None
        assert bounds.min_value is not None
        assert bounds.max_value is not None
        assert bounds.min_value.as_py() == 5
        assert bounds.max_value.as_py() == 8


# =============================================================================
# TestPushdownFiltersDunder
# =============================================================================


class TestPushdownFiltersDunder:
    """Tests for PushdownFilters dunder methods."""

    def test_bool_true(self) -> None:
        """Non-empty filters are truthy."""
        assert bool(_filters(_eq("a", 0, 1))) is True

    def test_bool_false(self) -> None:
        """Empty filters are falsy."""
        assert bool(PushdownFilters.empty()) is False

    def test_len(self) -> None:
        """__len__ returns number of top-level filters."""
        assert len(_filters(_eq("a", 0, 1), _eq("b", 1, 2))) == 2
        assert len(PushdownFilters.empty()) == 0

    def test_iter(self) -> None:
        """__iter__ yields top-level filters."""
        f1 = _eq("a", 0, 1)
        f2 = _eq("b", 1, 2)
        assert list(_filters(f1, f2)) == [f1, f2]

    def test_contains(self) -> None:
        """__contains__ checks column name."""
        pf = _filters(_eq("a", 0, 1))
        assert "a" in pf
        assert "b" not in pf

    def test_repr_empty(self) -> None:
        """Empty repr shows empty list."""
        assert repr(PushdownFilters.empty()) == "PushdownFilters([])"

    def test_repr_with_filters(self) -> None:
        """Non-empty repr includes filter type."""
        pf = _filters(_eq("a", 0, 1))
        r = repr(pf)
        assert "PushdownFilters" in r
        assert "ConstantFilter" in r

    def test_empty_factory(self) -> None:
        """empty() creates an empty instance."""
        pf = PushdownFilters.empty()
        assert len(pf) == 0
        assert pf.filters == ()


# =============================================================================
# TestFilterRepr
# =============================================================================


class TestFilterRepr:
    """Tests for __repr__ of all filter types."""

    def test_constant(self) -> None:
        """ConstantFilter repr shows column, op, and value."""
        r = repr(_eq("n", 0, 5))
        assert r == "ConstantFilter(n = 5)"

    def test_is_null(self) -> None:
        """IsNullFilter repr shows IS NULL."""
        r = repr(IsNullFilter(column_name="n", column_index=0))
        assert r == "IsNullFilter(n IS NULL)"

    def test_is_not_null(self) -> None:
        """IsNotNullFilter repr shows IS NOT NULL."""
        r = repr(IsNotNullFilter(column_name="n", column_index=0))
        assert r == "IsNotNullFilter(n IS NOT NULL)"

    def test_in_filter(self) -> None:
        """InFilter repr shows IN and values."""
        f = InFilter(column_name="n", column_index=0, values=pa.array([1, 2, 3]))
        r = repr(f)
        assert "InFilter" in r
        assert "IN" in r

    def test_in_filter_long(self) -> None:
        """InFilter repr truncates long value lists."""
        f = InFilter(column_name="n", column_index=0, values=pa.array(list(range(10))))
        r = repr(f)
        assert "10 total" in r

    def test_and_filter(self) -> None:
        """AndFilter repr shows AND between children."""
        f = AndFilter(
            column_name="n",
            column_index=0,
            children=(_eq("n", 0, 1), _eq("n", 0, 2)),
        )
        assert "AND" in repr(f)

    def test_or_filter(self) -> None:
        """OrFilter repr shows OR between children."""
        f = OrFilter(
            column_name="n",
            column_index=0,
            children=(_eq("n", 0, 1), _eq("n", 0, 2)),
        )
        assert "OR" in repr(f)

    def test_struct_filter(self) -> None:
        """StructFilter repr shows dotted column path."""
        child = _eq("index", 0, 5)
        f = StructFilter(
            column_name="metadata",
            column_index=0,
            child_index=0,
            child_name="index",
            child_filter=child,
        )
        r = repr(f)
        assert "StructFilter" in r
        assert "metadata.index" in r


# =============================================================================
# TestToSql
# =============================================================================


class TestToSql:
    """Tests for PushdownFilters.to_sql() and _filter_to_sql()."""

    def test_empty(self) -> None:
        """Empty filters produce empty SQL."""
        sql, params = PushdownFilters.empty().to_sql()
        assert sql == ""
        assert params == []

    def test_constant(self) -> None:
        """Constant filter generates comparison SQL."""
        pf = _filters(_eq("n", 0, 5))
        sql, params = pf.to_sql()
        assert sql == '"n" = ?'
        assert params == [5]

    def test_is_null(self) -> None:
        """IsNull filter generates IS NULL SQL."""
        pf = _filters(IsNullFilter(column_name="n", column_index=0))
        sql, params = pf.to_sql()
        assert sql == '"n" IS NULL'
        assert params == []

    def test_is_not_null(self) -> None:
        """IsNotNull filter generates IS NOT NULL SQL."""
        pf = _filters(IsNotNullFilter(column_name="n", column_index=0))
        sql, params = pf.to_sql()
        assert sql == '"n" IS NOT NULL'
        assert params == []

    def test_in_filter(self) -> None:
        """IN filter generates IN clause with placeholders."""
        inf = InFilter(column_name="n", column_index=0, values=pa.array([1, 2, 3]))
        pf = _filters(inf)
        sql, params = pf.to_sql()
        assert sql == '"n" IN (?, ?, ?)'
        assert params == [1, 2, 3]

    def test_and_filter(self) -> None:
        """AND filter generates parenthesized conjunction."""
        and_f = AndFilter(
            column_name="n",
            column_index=0,
            children=(
                _const("n", 0, ComparisonOp.GE, 3),
                _const("n", 0, ComparisonOp.LT, 10),
            ),
        )
        pf = _filters(and_f)
        sql, params = pf.to_sql()
        assert sql == '("n" >= ? AND "n" < ?)'
        assert params == [3, 10]

    def test_or_filter(self) -> None:
        """OR filter generates parenthesized disjunction."""
        or_f = OrFilter(
            column_name="n",
            column_index=0,
            children=(_eq("n", 0, 1), _eq("n", 0, 9)),
        )
        pf = _filters(or_f)
        sql, params = pf.to_sql()
        assert sql == '("n" = ? OR "n" = ?)'
        assert params == [1, 9]

    def test_struct_filter(self) -> None:
        """Struct filter generates dotted column name."""
        child = _eq("index", 0, 5)
        sf = StructFilter(
            column_name="metadata",
            column_index=0,
            child_index=0,
            child_name="index",
            child_filter=child,
        )
        pf = _filters(sf)
        sql, params = pf.to_sql()
        assert "metadata.index" in sql
        assert params == [5]

    def test_multiple_filters_and_joined(self) -> None:
        """Multiple top-level filters joined with AND."""
        pf = _filters(_eq("a", 0, 1), _const("b", 1, ComparisonOp.GT, 5))
        sql, params = pf.to_sql()
        assert " AND " in sql
        assert params == [1, 5]

    def test_custom_placeholder(self) -> None:
        """Custom placeholder style is used."""
        pf = _filters(_eq("n", 0, 5))
        sql, params = pf.to_sql(placeholder="%s")
        assert sql == '"n" = %s'

    def test_custom_quote(self) -> None:
        """Custom quote function is used."""
        pf = _filters(_eq("n", 0, 5))
        sql, params = pf.to_sql(quote_identifier=lambda s: f"`{s}`")
        assert sql == "`n` = ?"

    def test_unknown_filter_raises(self) -> None:
        """_filter_to_sql raises on unknown filter types."""
        f = Filter(column_name="x", column_index=0)
        with pytest.raises(ValueError, match="Unknown filter type"):
            _filter_to_sql(f, lambda s: f'"{s}"', "?", 0)


# =============================================================================
# TestIsNullIsNotNullEvaluate
# =============================================================================


class TestIsNullIsNotNullEvaluate:
    """Tests for IsNullFilter and IsNotNullFilter evaluation."""

    def test_is_null_with_nulls(self) -> None:
        """IS NULL returns True for null values."""
        batch = _batch(("n", [1, None, 3, None, 5]))
        f = IsNullFilter(column_name="n", column_index=0)
        result = f.evaluate(batch).to_pylist()
        assert result == [False, True, False, True, False]

    def test_is_null_no_nulls(self) -> None:
        """IS NULL returns all False when no nulls."""
        batch = _batch(("n", [1, 2, 3]))
        f = IsNullFilter(column_name="n", column_index=0)
        result = f.evaluate(batch).to_pylist()
        assert result == [False, False, False]

    def test_is_not_null_with_nulls(self) -> None:
        """IS NOT NULL returns False for null values."""
        batch = _batch(("n", [1, None, 3, None, 5]))
        f = IsNotNullFilter(column_name="n", column_index=0)
        result = f.evaluate(batch).to_pylist()
        assert result == [True, False, True, False, True]

    def test_is_not_null_no_nulls(self) -> None:
        """IS NOT NULL returns all True when no nulls."""
        batch = _batch(("n", [1, 2, 3]))
        f = IsNotNullFilter(column_name="n", column_index=0)
        result = f.evaluate(batch).to_pylist()
        assert result == [True, True, True]


# =============================================================================
# TestOrFilterEvaluate
# =============================================================================


class TestOrFilterEvaluate:
    """Tests for OrFilter evaluation."""

    def test_or_basic(self) -> None:
        """OR of two equality filters."""
        batch = _batch(("n", [0, 1, 2, 3, 4]))
        f = OrFilter(
            column_name="n",
            column_index=0,
            children=(_eq("n", 0, 1), _eq("n", 0, 3)),
        )
        result = f.evaluate(batch).to_pylist()
        assert result == [False, True, False, True, False]

    def test_or_three_children(self) -> None:
        """OR with three children."""
        batch = _batch(("n", [0, 1, 2, 3, 4]))
        f = OrFilter(
            column_name="n",
            column_index=0,
            children=(_eq("n", 0, 0), _eq("n", 0, 2), _eq("n", 0, 4)),
        )
        result = f.evaluate(batch).to_pylist()
        assert result == [True, False, True, False, True]

    def test_or_all_false(self) -> None:
        """OR with no matching values."""
        batch = _batch(("n", [0, 1, 2]))
        f = OrFilter(
            column_name="n",
            column_index=0,
            children=(_eq("n", 0, 99),),
        )
        result = f.evaluate(batch).to_pylist()
        assert result == [False, False, False]


# =============================================================================
# TestEmptyAndOrEvaluate
# =============================================================================


class TestEmptyAndOrEvaluate:
    """Tests for empty AND/OR filter edge cases."""

    def test_empty_and_all_true(self) -> None:
        """Empty AND produces all True (identity for conjunction)."""
        batch = _batch(("n", [1, 2, 3]))
        f = AndFilter(column_name="n", column_index=0, children=())
        result = f.evaluate(batch).to_pylist()
        assert result == [True, True, True]

    def test_empty_or_all_false(self) -> None:
        """Empty OR produces all False (identity for disjunction)."""
        batch = _batch(("n", [1, 2, 3]))
        f = OrFilter(column_name="n", column_index=0, children=())
        result = f.evaluate(batch).to_pylist()
        assert result == [False, False, False]


# =============================================================================
# TestPushdownFiltersEvaluateAndApply
# =============================================================================


class TestPushdownFiltersEvaluateAndApply:
    """Tests for PushdownFilters.evaluate() and apply()."""

    def test_evaluate_empty_filters(self) -> None:
        """Empty filters pass all rows."""
        batch = _batch(("n", [1, 2, 3]))
        pf = PushdownFilters.empty()
        result = pf.evaluate(batch).to_pylist()
        assert result == [True, True, True]

    def test_apply(self) -> None:
        """apply() returns filtered batch."""
        batch = _batch(("n", [0, 1, 2, 3, 4]))
        pf = _filters(_const("n", 0, ComparisonOp.GE, 3))
        result = pf.apply(batch)
        assert result.column("n").to_pylist() == [3, 4]


# =============================================================================
# TestDeserializationErrors
# =============================================================================


class TestDeserializationErrors:
    """Tests for deserialization error paths."""

    def _make_filter_batch(self, *, metadata: dict[bytes, bytes] | None = None, json_str: str = "[]") -> pa.RecordBatch:
        """Create a minimal batch for deserialization testing."""
        field = pa.field("filter_spec", pa.string(), metadata=metadata)
        schema = pa.schema([field])
        return pa.RecordBatch.from_pydict({"filter_spec": [json_str]}, schema=schema)

    def test_missing_metadata(self) -> None:
        """Missing metadata raises FilterVersionError."""
        batch = self._make_filter_batch(metadata=None)
        with pytest.raises(FilterVersionError, match="Missing"):
            deserialize_filters(batch)

    def test_unsupported_version(self) -> None:
        """Unsupported version raises FilterVersionError."""
        batch = self._make_filter_batch(metadata={b"vgi_filter_version": b"99"})
        with pytest.raises(FilterVersionError, match="Unsupported"):
            deserialize_filters(batch)

    def test_bad_json(self) -> None:
        """Bad JSON raises FilterDeserializationError."""
        batch = self._make_filter_batch(
            metadata={b"vgi_filter_version": b"1"},
            json_str="not-valid-json",
        )
        with pytest.raises(FilterDeserializationError, match="Failed to parse filter JSON"):
            deserialize_filters(batch)

    def test_unknown_filter_type(self) -> None:
        """Unknown filter type raises FilterDeserializationError."""
        specs = json.dumps([{"column_name": "x", "column_index": 0, "type": "unknown_type"}])
        batch = self._make_filter_batch(
            metadata={b"vgi_filter_version": b"1"},
            json_str=specs,
        )
        with pytest.raises(FilterDeserializationError, match="Unknown filter type"):
            deserialize_filters(batch)


# =============================================================================
# Expression Filter Tests
# =============================================================================


class TestExpressionNodeToSql:
    """Tests for ExpressionNode.to_sql() rendering."""

    def test_column_ref(self) -> None:
        """Column ref renders as quoted column name."""
        node = ColumnRefNode(expr_type=ExpressionNodeType.COLUMN_REF, index=0)
        assert node.to_sql("geom") == '"geom"'

    def test_constant_int(self) -> None:
        """Integer renders as number."""
        node = ConstantNode(expr_type=ExpressionNodeType.CONSTANT, value=pa.scalar(42))
        assert node.to_sql("x") == "42"

    def test_constant_string(self) -> None:
        """String renders as quoted literal."""
        node = ConstantNode(expr_type=ExpressionNodeType.CONSTANT, value=pa.scalar("hello"))
        assert node.to_sql("x") == "'hello'"

    def test_constant_string_with_quote(self) -> None:
        """Single quote in string is escaped."""
        node = ConstantNode(expr_type=ExpressionNodeType.CONSTANT, value=pa.scalar("it's"))
        assert node.to_sql("x") == "'it''s'"

    def test_constant_float(self) -> None:
        """Float renders as number."""
        node = ConstantNode(expr_type=ExpressionNodeType.CONSTANT, value=pa.scalar(3.14))
        assert node.to_sql("x") == "3.14"

    def test_constant_bool(self) -> None:
        """Boolean renders as TRUE/FALSE."""
        node = ConstantNode(expr_type=ExpressionNodeType.CONSTANT, value=pa.scalar(True))
        assert node.to_sql("x") == "TRUE"

    def test_function_node(self) -> None:
        """Function renders as name(args)."""
        col = ColumnRefNode(expr_type=ExpressionNodeType.COLUMN_REF, index=0)
        const = ConstantNode(expr_type=ExpressionNodeType.CONSTANT, value=pa.scalar(5))
        func = FunctionNode(
            expr_type=ExpressionNodeType.FUNCTION,
            function_name="my_func",
            children=(col, const),
        )
        assert func.to_sql("x") == 'my_func("x", 5)'

    def test_operator_function_infix(self) -> None:
        """Operator function renders as infix: (left op right)."""
        col = ColumnRefNode(expr_type=ExpressionNodeType.COLUMN_REF, index=0)
        const = ConstantNode(expr_type=ExpressionNodeType.CONSTANT, value=pa.scalar(5))
        func = FunctionNode(
            expr_type=ExpressionNodeType.FUNCTION,
            function_name="&&",
            children=(col, const),
        )
        assert func.to_sql("geom") == '("geom" && 5)'

    def test_comparison_node(self) -> None:
        """Comparison renders as (left op right)."""
        col = ColumnRefNode(expr_type=ExpressionNodeType.COLUMN_REF, index=0)
        const = ConstantNode(expr_type=ExpressionNodeType.CONSTANT, value=pa.scalar(10))
        comp = ComparisonNode(
            expr_type=ExpressionNodeType.COMPARISON,
            op=ComparisonOp.GT,
            left=col,
            right=const,
        )
        assert comp.to_sql("n") == '("n" > 10)'

    def test_conjunction_and(self) -> None:
        """AND conjunction joins children with AND."""
        c1 = ConstantNode(expr_type=ExpressionNodeType.CONSTANT, value=pa.scalar(True))
        c2 = ConstantNode(expr_type=ExpressionNodeType.CONSTANT, value=pa.scalar(False))
        conj = ConjunctionNode(
            expr_type=ExpressionNodeType.CONJUNCTION,
            conjunction_type="and",
            children=(c1, c2),
        )
        assert conj.to_sql("x") == "(TRUE AND FALSE)"

    def test_nested_function(self) -> None:
        """Nested function: outer(inner(col, 100), const)."""
        col = ColumnRefNode(expr_type=ExpressionNodeType.COLUMN_REF, index=0)
        c100 = ConstantNode(expr_type=ExpressionNodeType.CONSTANT, value=pa.scalar(100))
        inner = FunctionNode(
            expr_type=ExpressionNodeType.FUNCTION,
            function_name="st_buffer",
            children=(col, c100),
        )
        c_geom = ConstantNode(expr_type=ExpressionNodeType.CONSTANT, value=pa.scalar("POINT(0 0)"))
        outer = FunctionNode(
            expr_type=ExpressionNodeType.FUNCTION,
            function_name="st_intersects",
            children=(inner, c_geom),
        )
        assert outer.to_sql("geom") == "st_intersects(st_buffer(\"geom\", 100), 'POINT(0 0)')"


class TestExpressionFilterDeserialization:
    """Tests for ExpressionFilter deserialization from Arrow IPC."""

    def _make_expression_batch(self, expr_spec: object, value: int = 42) -> pa.RecordBatch:
        """Build a filter batch with a single expression filter and one value column."""
        spec = json.dumps(
            [
                {
                    "column_name": "n",
                    "column_index": 0,
                    "type": "expression",
                    "expr": expr_spec,
                }
            ]
        )
        fields: list[pa.Field] = [  # type: ignore[type-arg]
            pa.field("filter_spec", pa.string(), metadata={b"vgi_filter_version": b"1"}),
            pa.field("_val_0", pa.int64()),
        ]
        s = pa.schema(fields)
        return pa.RecordBatch.from_pydict({"filter_spec": [spec], "_val_0": [value]}, schema=s)

    def test_deserialize_simple_function(self) -> None:
        """Deserialize a function(column_ref, constant) expression."""
        expr_spec = {
            "expr_type": "function",
            "function_name": "my_func",
            "children": [
                {"expr_type": "column_ref", "index": 0},
                {"expr_type": "constant", "value_ref": 0},
            ],
        }
        batch = self._make_expression_batch(expr_spec)
        pf = deserialize_filters(batch)

        assert len(pf) == 1
        f = pf.filters[0]
        assert isinstance(f, ExpressionFilter)
        assert f.column_name == "n"
        assert isinstance(f.expr, FunctionNode)
        assert f.expr.function_name == "my_func"
        assert len(f.expr.children) == 2
        assert isinstance(f.expr.children[0], ColumnRefNode)
        assert isinstance(f.expr.children[1], ConstantNode)
        assert f.expr.children[1].value.as_py() == 42

    def test_deserialize_comparison(self) -> None:
        """Deserialize a comparison(column_ref, constant) expression."""
        expr_spec = {
            "expr_type": "comparison",
            "op": "gt",
            "left": {"expr_type": "column_ref", "index": 0},
            "right": {"expr_type": "constant", "value_ref": 0},
        }
        batch = self._make_expression_batch(expr_spec, value=10)
        pf = deserialize_filters(batch)

        f = pf.filters[0]
        assert isinstance(f, ExpressionFilter)
        assert isinstance(f.expr, ComparisonNode)
        assert f.expr.op == ComparisonOp.GT

    def test_deserialize_conjunction(self) -> None:
        """Deserialize a conjunction of two constant nodes."""
        expr_spec = {
            "expr_type": "conjunction",
            "conjunction_type": "or",
            "children": [
                {"expr_type": "constant", "value_ref": 0},
                {"expr_type": "constant", "value_ref": 0},
            ],
        }
        batch = self._make_expression_batch(expr_spec)
        pf = deserialize_filters(batch)

        f = pf.filters[0]
        assert isinstance(f, ExpressionFilter)
        assert isinstance(f.expr, ConjunctionNode)
        assert f.expr.conjunction_type == "or"
        assert len(f.expr.children) == 2


class TestExpressionFilterEvaluate:
    """Tests for ExpressionFilter.evaluate() using DuckDB."""

    def test_evaluate_comparison(self) -> None:
        """Evaluate a comparison expression against a batch."""
        batch = _batch(("n", [1, 2, 3, 4, 5]))
        # Build: n > 3
        expr = ComparisonNode(
            expr_type=ExpressionNodeType.COMPARISON,
            op=ComparisonOp.GT,
            left=ColumnRefNode(expr_type=ExpressionNodeType.COLUMN_REF, index=0),
            right=ConstantNode(expr_type=ExpressionNodeType.CONSTANT, value=pa.scalar(3)),
        )
        ef = ExpressionFilter(column_name="n", column_index=0, expr=expr)
        result = ef.evaluate(batch)
        assert result.to_pylist() == [False, False, False, True, True]

    def test_evaluate_function(self) -> None:
        """Evaluate a function expression (list_contains) against a batch."""
        batch = _batch(("vals", [[1, 2, 3], [4, 5, 6], [7, 8, 9]]))
        # Build: list_contains(vals, 5)
        expr = FunctionNode(
            expr_type=ExpressionNodeType.FUNCTION,
            function_name="list_contains",
            children=(
                ColumnRefNode(expr_type=ExpressionNodeType.COLUMN_REF, index=0),
                ConstantNode(expr_type=ExpressionNodeType.CONSTANT, value=pa.scalar(5)),
            ),
        )
        ef = ExpressionFilter(column_name="vals", column_index=0, expr=expr)
        result = ef.evaluate(batch)
        assert result.to_pylist() == [False, True, False]

    def test_evaluate_with_nulls(self) -> None:
        """NULL values in column produce NULL in boolean mask (treated as not passing)."""
        batch = pa.RecordBatch.from_pydict(
            {"n": [1, None, 3, None, 5]},
            schema=pa.schema([("n", pa.int64())]),
        )
        # Build: n > 2
        expr = ComparisonNode(
            expr_type=ExpressionNodeType.COMPARISON,
            op=ComparisonOp.GT,
            left=ColumnRefNode(expr_type=ExpressionNodeType.COLUMN_REF, index=0),
            right=ConstantNode(expr_type=ExpressionNodeType.CONSTANT, value=pa.scalar(2)),
        )
        ef = ExpressionFilter(column_name="n", column_index=0, expr=expr)
        result = ef.evaluate(batch)
        # NULL > 2 = NULL, which is treated as False by pc.filter
        assert result.to_pylist() == [False, None, True, None, True]


class TestArrowScalarToSql:
    """Tests for _arrow_scalar_to_sql helper."""

    def test_int(self) -> None:
        """Integer scalar to SQL."""
        assert _arrow_scalar_to_sql(pa.scalar(42)) == "42"

    def test_float(self) -> None:
        """Float scalar to SQL."""
        assert _arrow_scalar_to_sql(pa.scalar(3.14)) == "3.14"

    def test_string(self) -> None:
        """String scalar to SQL."""
        assert _arrow_scalar_to_sql(pa.scalar("hello")) == "'hello'"

    def test_bool_true(self) -> None:
        """True renders as TRUE."""
        assert _arrow_scalar_to_sql(pa.scalar(True)) == "TRUE"

    def test_bool_false(self) -> None:
        """False renders as FALSE."""
        assert _arrow_scalar_to_sql(pa.scalar(False)) == "FALSE"

    def test_null(self) -> None:
        """Null renders as NULL."""
        assert _arrow_scalar_to_sql(pa.scalar(None, type=pa.int64())) == "NULL"

    def test_binary_blob(self) -> None:
        """Non-geometry binary renders as hex BLOB literal."""
        result = _arrow_scalar_to_sql(pa.scalar(b"\x01\x02\x03"))
        assert result == "'\\x010203'::BLOB"

    def test_binary_blob_with_plain_field(self) -> None:
        """Binary with plain field (no extension metadata) renders as BLOB."""
        field = pa.field("data", pa.binary())
        result = _arrow_scalar_to_sql(pa.scalar(b"\x01\x02\x03"), field)
        assert result == "'\\x010203'::BLOB"

    def test_binary_geometry(self) -> None:
        """Binary with geoarrow.wkb extension renders as ST_GeomFromHEXWKB."""
        field = pa.field(
            "geom",
            pa.binary(),
            metadata={
                b"ARROW:extension:name": b"geoarrow.wkb",
                b"ARROW:extension:metadata": b"{}",
            },
        )
        result = _arrow_scalar_to_sql(pa.scalar(b"\x01\x02\x03"), field)
        assert result == "ST_GeomFromHEXWKB('010203')"


# =============================================================================
# Join Keys Batch Tests
# =============================================================================


class TestJoinKeysBatch:
    """Tests for the join_keys filter type and get_join_keys_batch()."""

    @staticmethod
    def _make_filter_batch_with_join_keys() -> pa.RecordBatch:
        """Build a filter batch containing a join_keys filter spec (no value columns)."""
        spec = json.dumps(
            [
                {
                    "column_name": "id",
                    "column_index": 0,
                    "type": "join_keys",
                    "keys_column": "id",
                }
            ]
        )
        fields = [
            pa.field("filter_spec", pa.string(), metadata={b"vgi_filter_version": b"1"}),
        ]
        return pa.RecordBatch.from_pydict({"filter_spec": [spec]}, schema=pa.schema(fields))

    @staticmethod
    def _make_join_keys_batch(values: list[int]) -> pa.RecordBatch:
        """Build a flat join keys RecordBatch with an 'id' column."""
        return pa.RecordBatch.from_pydict({"id": values})

    def test_deserialize_join_keys(self) -> None:
        """join_keys filter type deserializes into InFilter with correct values."""
        filter_batch = self._make_filter_batch_with_join_keys()
        keys_batch = self._make_join_keys_batch([10, 20, 30])

        pf = deserialize_filters(filter_batch, join_keys=keys_batch)

        assert len(pf) == 1
        f = pf.filters[0]
        assert isinstance(f, InFilter)
        assert f.column_name == "id"
        assert f.values.to_pylist() == [10, 20, 30]

    def test_evaluate_join_keys(self) -> None:
        """InFilter from join_keys correctly filters a data batch."""
        filter_batch = self._make_filter_batch_with_join_keys()
        keys_batch = self._make_join_keys_batch([1, 3, 5])

        pf = deserialize_filters(filter_batch, join_keys=keys_batch)
        data = pa.RecordBatch.from_pydict({"id": [0, 1, 2, 3, 4, 5, 6]})
        result = pf.apply(data)

        assert result.column("id").to_pylist() == [1, 3, 5]

    def test_get_join_keys_batch(self) -> None:
        """get_join_keys_batch() returns the original batch (zero-copy)."""
        filter_batch = self._make_filter_batch_with_join_keys()
        keys_batch = self._make_join_keys_batch([100, 200])

        pf = deserialize_filters(filter_batch, join_keys=keys_batch)

        returned = pf.get_join_keys_batch()
        assert returned is keys_batch  # same object, zero-copy

    def test_get_join_keys_batch_none(self) -> None:
        """get_join_keys_batch() returns None when no join keys were provided."""
        spec = json.dumps(
            [
                {
                    "column_name": "id",
                    "column_index": 0,
                    "type": "constant",
                    "op": "eq",
                    "value_ref": 0,
                }
            ]
        )
        fields = [
            pa.field("filter_spec", pa.string(), metadata={b"vgi_filter_version": b"1"}),
            pa.field("_val_0", pa.int64()),
        ]
        batch = pa.RecordBatch.from_pydict({"filter_spec": [spec], "_val_0": [42]}, schema=pa.schema(fields))
        pf = deserialize_filters(batch)

        assert pf.get_join_keys_batch() is None

    def test_join_keys_missing_batch_skips_filter(self) -> None:
        """join_keys filter with no keys batch is silently skipped."""
        filter_batch = self._make_filter_batch_with_join_keys()

        # No join_keys argument — the filter should be skipped
        pf = deserialize_filters(filter_batch, join_keys=None)

        assert len(pf) == 0  # filter was dropped

    def test_join_keys_missing_column_skips_filter(self) -> None:
        """join_keys filter referencing a non-existent column is skipped."""
        filter_batch = self._make_filter_batch_with_join_keys()
        # Keys batch has 'other_col' not 'id'
        keys_batch = pa.RecordBatch.from_pydict({"other_col": [1, 2, 3]})

        pf = deserialize_filters(filter_batch, join_keys=keys_batch)

        assert len(pf) == 0  # filter was dropped

    def test_join_keys_large_value_set(self) -> None:
        """Large join keys batch (10K values) deserializes and evaluates correctly."""
        filter_batch = self._make_filter_batch_with_join_keys()
        keys = list(range(0, 100000, 10))  # 10K values: 0, 10, 20, ...
        keys_batch = self._make_join_keys_batch(keys)

        pf = deserialize_filters(filter_batch, join_keys=keys_batch)

        assert len(pf) == 1
        f = pf.filters[0]
        assert isinstance(f, InFilter)
        assert len(f.values) == 10000

        # Evaluate against a small data batch
        data = pa.RecordBatch.from_pydict({"id": [0, 5, 10, 15, 20]})
        result = pf.apply(data)
        assert result.column("id").to_pylist() == [0, 10, 20]

    def test_join_keys_string_type(self) -> None:
        """Join keys work with string columns."""
        spec = json.dumps(
            [
                {
                    "column_name": "name",
                    "column_index": 0,
                    "type": "join_keys",
                    "keys_column": "name",
                }
            ]
        )
        fields = [
            pa.field("filter_spec", pa.string(), metadata={b"vgi_filter_version": b"1"}),
        ]
        filter_batch = pa.RecordBatch.from_pydict({"filter_spec": [spec]}, schema=pa.schema(fields))
        keys_batch = pa.RecordBatch.from_pydict({"name": ["alice", "bob"]})

        pf = deserialize_filters(filter_batch, join_keys=keys_batch)

        assert len(pf) == 1
        f = pf.filters[0]
        assert isinstance(f, InFilter)
        assert f.values.to_pylist() == ["alice", "bob"]

    def test_join_keys_mixed_with_other_filters(self) -> None:
        """join_keys filter alongside a constant filter both deserialize."""
        spec = json.dumps(
            [
                {
                    "column_name": "id",
                    "column_index": 0,
                    "type": "join_keys",
                    "keys_column": "id",
                },
                {
                    "column_name": "status",
                    "column_index": 1,
                    "type": "constant",
                    "op": "eq",
                    "value_ref": 0,
                },
            ]
        )
        fields = [
            pa.field("filter_spec", pa.string(), metadata={b"vgi_filter_version": b"1"}),
            pa.field("_val_0", pa.utf8()),
        ]
        filter_batch = pa.RecordBatch.from_pydict(
            {"filter_spec": [spec], "_val_0": ["active"]},
            schema=pa.schema(fields),
        )
        keys_batch = pa.RecordBatch.from_pydict({"id": [1, 2, 3]})

        pf = deserialize_filters(filter_batch, join_keys=keys_batch)

        assert len(pf) == 2
        assert isinstance(pf.filters[0], InFilter)
        assert isinstance(pf.filters[1], ConstantFilter)

    def test_join_keys_inside_and_filter(self) -> None:
        """join_keys nested inside an AND filter — AND drops the None child safely."""
        spec = json.dumps(
            [
                {
                    "column_name": "id",
                    "column_index": 0,
                    "type": "and",
                    "children": [
                        {
                            "column_name": "id",
                            "column_index": 0,
                            "type": "join_keys",
                            "keys_column": "id",
                        },
                        {
                            "column_name": "id",
                            "column_index": 0,
                            "type": "constant",
                            "op": "ge",
                            "value_ref": 0,
                        },
                    ],
                }
            ]
        )
        fields = [
            pa.field("filter_spec", pa.string(), metadata={b"vgi_filter_version": b"1"}),
            pa.field("_val_0", pa.int64()),
        ]
        batch = pa.RecordBatch.from_pydict({"filter_spec": [spec], "_val_0": [0]}, schema=pa.schema(fields))
        keys_batch = pa.RecordBatch.from_pydict({"id": [10, 20, 30]})

        pf = deserialize_filters(batch, join_keys=keys_batch)
        assert len(pf) == 1
        f = pf.filters[0]
        assert isinstance(f, AndFilter)
        assert len(f.children) == 2
        assert isinstance(f.children[0], InFilter)
        assert isinstance(f.children[1], ConstantFilter)

    def test_join_keys_inside_or_filter_drops_entire_or(self) -> None:
        """join_keys inside OR with missing batch drops the entire OR (not just the child)."""
        spec = json.dumps(
            [
                {
                    "column_name": "id",
                    "column_index": 0,
                    "type": "or",
                    "children": [
                        {
                            "column_name": "id",
                            "column_index": 0,
                            "type": "join_keys",
                            "keys_column": "id",
                        },
                        {
                            "column_name": "id",
                            "column_index": 0,
                            "type": "constant",
                            "op": "eq",
                            "value_ref": 0,
                        },
                    ],
                }
            ]
        )
        fields = [
            pa.field("filter_spec", pa.string(), metadata={b"vgi_filter_version": b"1"}),
            pa.field("_val_0", pa.int64()),
        ]
        batch = pa.RecordBatch.from_pydict({"filter_spec": [spec], "_val_0": [42]}, schema=pa.schema(fields))

        # No join_keys batch — the OR child resolves to None, so entire OR is dropped
        pf = deserialize_filters(batch, join_keys=None)
        assert len(pf) == 0

    def test_join_keys_empty_batch(self) -> None:
        """Empty join keys batch (0 rows) produces InFilter that filters everything out."""
        filter_batch = self._make_filter_batch_with_join_keys()
        keys_batch = pa.RecordBatch.from_pydict({"id": pa.array([], type=pa.int64())})

        pf = deserialize_filters(filter_batch, join_keys=keys_batch)
        assert len(pf) == 1
        f = pf.filters[0]
        assert isinstance(f, InFilter)
        assert len(f.values) == 0

        # Evaluating against data should filter everything out
        data = pa.RecordBatch.from_pydict({"id": [1, 2, 3]})
        result = pf.apply(data)
        assert result.num_rows == 0
