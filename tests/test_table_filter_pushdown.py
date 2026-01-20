"""Tests for filter pushdown AST classes and deserialization."""

from typing import Annotated, cast

import pyarrow as pa
import pytest

from vgi import Arg, Arguments, Output
from vgi.table_filter_pushdown import (
    AndFilter,
    ColumnBounds,
    ComparisonOp,
    ConstantFilter,
    FilterVersionError,
    InFilter,
    IsNotNullFilter,
    IsNullFilter,
    OrFilter,
    PushdownFilters,
    StructFilter,
    deserialize_filters,
)
from vgi.table_function import OutputGenerator, TableFunctionGenerator
from vgi.testing import (
    TableFunctionTestClient,
    create_pushdown_filters,
)


class TestCreatePushdownFilters:
    """Tests for the create_pushdown_filters test utility."""

    def test_constant_filter(self) -> None:
        """Test creating a constant comparison filter."""
        ipc_bytes = create_pushdown_filters(
            filters=[
                {
                    "column_name": "age",
                    "column_index": 0,
                    "type": "constant",
                    "op": "ge",
                    "value_ref": 0,
                }
            ],
            values={0: 18},
        )

        filters = deserialize_filters(ipc_bytes)
        assert len(filters) == 1
        assert isinstance(filters.filters[0], ConstantFilter)
        f = filters.filters[0]
        assert f.column_name == "age"
        assert f.column_index == 0
        assert f.op == ComparisonOp.GE
        assert f.value.as_py() == 18

    def test_in_filter(self) -> None:
        """Test creating an IN filter with list values."""
        ipc_bytes = create_pushdown_filters(
            filters=[
                {
                    "column_name": "status",
                    "column_index": 1,
                    "type": "in",
                    "value_ref": 0,
                }
            ],
            values={0: ["active", "pending", "review"]},
        )

        filters = deserialize_filters(ipc_bytes)
        assert len(filters) == 1
        assert isinstance(filters.filters[0], InFilter)
        f = filters.filters[0]
        assert f.column_name == "status"
        assert f.values.to_pylist() == ["active", "pending", "review"]

    def test_is_null_filter(self) -> None:
        """Test creating an IS NULL filter."""
        ipc_bytes = create_pushdown_filters(
            filters=[
                {
                    "column_name": "email",
                    "column_index": 2,
                    "type": "is_null",
                }
            ],
        )

        filters = deserialize_filters(ipc_bytes)
        assert len(filters) == 1
        assert isinstance(filters.filters[0], IsNullFilter)
        assert filters.filters[0].column_name == "email"

    def test_is_not_null_filter(self) -> None:
        """Test creating an IS NOT NULL filter."""
        ipc_bytes = create_pushdown_filters(
            filters=[
                {
                    "column_name": "email",
                    "column_index": 2,
                    "type": "is_not_null",
                }
            ],
        )

        filters = deserialize_filters(ipc_bytes)
        assert len(filters) == 1
        assert isinstance(filters.filters[0], IsNotNullFilter)

    def test_and_filter(self) -> None:
        """Test creating an AND compound filter."""
        ipc_bytes = create_pushdown_filters(
            filters=[
                {
                    "column_name": "age",
                    "column_index": 0,
                    "type": "and",
                    "children": [
                        {
                            "column_name": "age",
                            "column_index": 0,
                            "type": "constant",
                            "op": "ge",
                            "value_ref": 0,
                        },
                        {
                            "column_name": "age",
                            "column_index": 0,
                            "type": "constant",
                            "op": "lt",
                            "value_ref": 1,
                        },
                    ],
                }
            ],
            values={0: 18, 1: 65},
        )

        filters = deserialize_filters(ipc_bytes)
        assert len(filters) == 1
        assert isinstance(filters.filters[0], AndFilter)
        f = filters.filters[0]
        assert len(f.children) == 2

    def test_or_filter(self) -> None:
        """Test creating an OR compound filter."""
        ipc_bytes = create_pushdown_filters(
            filters=[
                {
                    "column_name": "status",
                    "column_index": 0,
                    "type": "or",
                    "children": [
                        {
                            "column_name": "status",
                            "column_index": 0,
                            "type": "constant",
                            "op": "eq",
                            "value_ref": 0,
                        },
                        {
                            "column_name": "status",
                            "column_index": 0,
                            "type": "constant",
                            "op": "eq",
                            "value_ref": 1,
                        },
                    ],
                }
            ],
            values={0: "active", 1: "pending"},
        )

        filters = deserialize_filters(ipc_bytes)
        assert len(filters) == 1
        assert isinstance(filters.filters[0], OrFilter)

    def test_struct_filter(self) -> None:
        """Test creating a struct field filter."""
        ipc_bytes = create_pushdown_filters(
            filters=[
                {
                    "column_name": "address",
                    "column_index": 0,
                    "type": "struct",
                    "child_index": 1,
                    "child_name": "city",
                    "child_filter": {
                        "column_name": "address",
                        "column_index": 0,
                        "type": "constant",
                        "op": "eq",
                        "value_ref": 0,
                    },
                }
            ],
            values={0: "Seattle"},
        )

        filters = deserialize_filters(ipc_bytes)
        assert len(filters) == 1
        assert isinstance(filters.filters[0], StructFilter)
        f = filters.filters[0]
        assert f.child_name == "city"
        assert isinstance(f.child_filter, ConstantFilter)


class TestFilterEvaluation:
    """Tests for filter evaluation using PyArrow compute."""

    def test_constant_eq(self) -> None:
        """Test equality filter evaluation."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3, 2, 1]})
        f = ConstantFilter("x", 0, ComparisonOp.EQ, pa.scalar(2))
        result = f.evaluate(batch)
        assert result.to_pylist() == [False, True, False, True, False]

    def test_constant_gt(self) -> None:
        """Test greater than filter evaluation."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3, 4, 5]})
        f = ConstantFilter("x", 0, ComparisonOp.GT, pa.scalar(3))
        result = f.evaluate(batch)
        assert result.to_pylist() == [False, False, False, True, True]

    def test_constant_ge(self) -> None:
        """Test greater or equal filter evaluation."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3, 4, 5]})
        f = ConstantFilter("x", 0, ComparisonOp.GE, pa.scalar(3))
        result = f.evaluate(batch)
        assert result.to_pylist() == [False, False, True, True, True]

    def test_constant_lt(self) -> None:
        """Test less than filter evaluation."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3, 4, 5]})
        f = ConstantFilter("x", 0, ComparisonOp.LT, pa.scalar(3))
        result = f.evaluate(batch)
        assert result.to_pylist() == [True, True, False, False, False]

    def test_constant_le(self) -> None:
        """Test less or equal filter evaluation."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3, 4, 5]})
        f = ConstantFilter("x", 0, ComparisonOp.LE, pa.scalar(3))
        result = f.evaluate(batch)
        assert result.to_pylist() == [True, True, True, False, False]

    def test_constant_ne(self) -> None:
        """Test not equal filter evaluation."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3, 2, 1]})
        f = ConstantFilter("x", 0, ComparisonOp.NE, pa.scalar(2))
        result = f.evaluate(batch)
        assert result.to_pylist() == [True, False, True, False, True]

    def test_is_null(self) -> None:
        """Test IS NULL filter evaluation."""
        batch = pa.RecordBatch.from_pydict({"x": [1, None, 3, None, 5]})
        f = IsNullFilter("x", 0)
        result = f.evaluate(batch)
        assert result.to_pylist() == [False, True, False, True, False]

    def test_is_not_null(self) -> None:
        """Test IS NOT NULL filter evaluation."""
        batch = pa.RecordBatch.from_pydict({"x": [1, None, 3, None, 5]})
        f = IsNotNullFilter("x", 0)
        result = f.evaluate(batch)
        assert result.to_pylist() == [True, False, True, False, True]

    def test_in_filter(self) -> None:
        """Test IN filter evaluation."""
        batch = pa.RecordBatch.from_pydict({"x": ["a", "b", "c", "d", "e"]})
        f = InFilter("x", 0, pa.array(["a", "c", "e"]))
        result = f.evaluate(batch)
        assert result.to_pylist() == [True, False, True, False, True]

    def test_and_filter(self) -> None:
        """Test AND filter evaluation."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3, 4, 5]})
        f = AndFilter(
            "x",
            0,
            (
                ConstantFilter("x", 0, ComparisonOp.GE, pa.scalar(2)),
                ConstantFilter("x", 0, ComparisonOp.LE, pa.scalar(4)),
            ),
        )
        result = f.evaluate(batch)
        assert result.to_pylist() == [False, True, True, True, False]

    def test_or_filter(self) -> None:
        """Test OR filter evaluation."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3, 4, 5]})
        f = OrFilter(
            "x",
            0,
            (
                ConstantFilter("x", 0, ComparisonOp.EQ, pa.scalar(1)),
                ConstantFilter("x", 0, ComparisonOp.EQ, pa.scalar(5)),
            ),
        )
        result = f.evaluate(batch)
        assert result.to_pylist() == [True, False, False, False, True]

    def test_struct_filter(self) -> None:
        """Test struct field filter evaluation."""
        batch = pa.RecordBatch.from_pydict(
            {
                "address": [
                    {"city": "Seattle", "state": "WA"},
                    {"city": "Portland", "state": "OR"},
                    {"city": "Seattle", "state": "WA"},
                ]
            }
        )
        child = ConstantFilter("address", 0, ComparisonOp.EQ, pa.scalar("Seattle"))
        f = StructFilter(
            "address",
            0,
            child_index=0,
            child_name="city",
            child_filter=child,
        )
        result = f.evaluate(batch)
        assert result.to_pylist() == [True, False, True]


class TestPushdownFiltersHelpers:
    """Tests for PushdownFilters helper methods."""

    def test_filtered_columns(self) -> None:
        """Test filtered_columns property."""
        filters = PushdownFilters(
            (
                ConstantFilter("age", 0, ComparisonOp.GE, pa.scalar(18)),
                ConstantFilter("status", 1, ComparisonOp.EQ, pa.scalar("active")),
            )
        )
        assert filters.filtered_columns == frozenset({"age", "status"})

    def test_has_filter_for_column(self) -> None:
        """Test has_filter_for_column method."""
        filters = PushdownFilters(
            (ConstantFilter("age", 0, ComparisonOp.GE, pa.scalar(18)),)
        )
        assert filters.has_filter_for_column("age") is True
        assert filters.has_filter_for_column("name") is False

    def test_get_column_constant(self) -> None:
        """Test get_column_constant for equality filters."""
        filters = PushdownFilters(
            (ConstantFilter("tenant_id", 0, ComparisonOp.EQ, pa.scalar("abc123")),)
        )
        result = filters.get_column_constant("tenant_id")
        assert result is not None
        assert result.as_py() == "abc123"

        # Non-equality filter should return None
        filters2 = PushdownFilters(
            (ConstantFilter("age", 0, ComparisonOp.GE, pa.scalar(18)),)
        )
        assert filters2.get_column_constant("age") is None

    def test_get_column_in_values(self) -> None:
        """Test get_column_in_values for IN filters."""
        filters = PushdownFilters(
            (InFilter("status", 0, pa.array(["active", "pending"])),)
        )
        result = filters.get_column_in_values("status")
        assert result is not None
        assert result.to_pylist() == ["active", "pending"]

    def test_get_column_values_equality(self) -> None:
        """Test get_column_values with equality filter."""
        filters = PushdownFilters(
            (ConstantFilter("tenant_id", 0, ComparisonOp.EQ, pa.scalar("abc")),)
        )
        result = filters.get_column_values("tenant_id")
        assert result is not None
        assert result.to_pylist() == ["abc"]

    def test_get_column_values_in(self) -> None:
        """Test get_column_values with IN filter."""
        filters = PushdownFilters((InFilter("status", 0, pa.array(["a", "b", "c"])),))
        result = filters.get_column_values("status")
        assert result is not None
        assert result.to_pylist() == ["a", "b", "c"]

    def test_get_column_bounds_range(self) -> None:
        """Test get_column_bounds with range filters."""
        filters = PushdownFilters(
            (
                ConstantFilter("age", 0, ComparisonOp.GE, pa.scalar(18)),
                ConstantFilter("age", 0, ComparisonOp.LT, pa.scalar(65)),
            )
        )
        bounds = filters.get_column_bounds("age")
        assert bounds is not None
        assert bounds.min_value is not None
        assert bounds.min_value.as_py() == 18
        assert bounds.min_inclusive is True
        assert bounds.max_value is not None
        assert bounds.max_value.as_py() == 65
        assert bounds.max_inclusive is False

    def test_get_column_bounds_equality(self) -> None:
        """Test get_column_bounds with equality filter."""
        filters = PushdownFilters(
            (ConstantFilter("id", 0, ComparisonOp.EQ, pa.scalar(42)),)
        )
        bounds = filters.get_column_bounds("id")
        assert bounds is not None
        assert bounds.min_value is not None
        assert bounds.min_value.as_py() == 42
        assert bounds.min_inclusive is True
        assert bounds.max_value is not None
        assert bounds.max_value.as_py() == 42
        assert bounds.max_inclusive is True

    def test_apply(self) -> None:
        """Test PushdownFilters.apply method."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3, 4, 5]})
        filters = PushdownFilters(
            (ConstantFilter("x", 0, ComparisonOp.GE, pa.scalar(3)),)
        )
        result = filters.apply(batch)
        assert result.column("x").to_pylist() == [3, 4, 5]


class TestColumnBounds:
    """Tests for ColumnBounds helper class."""

    def test_contains_inclusive(self) -> None:
        """Test contains with inclusive bounds."""
        bounds = ColumnBounds(
            min_value=pa.scalar(10),
            min_inclusive=True,
            max_value=pa.scalar(20),
            max_inclusive=True,
        )
        assert bounds.contains(10) is True
        assert bounds.contains(15) is True
        assert bounds.contains(20) is True
        assert bounds.contains(9) is False
        assert bounds.contains(21) is False

    def test_contains_exclusive(self) -> None:
        """Test contains with exclusive bounds."""
        bounds = ColumnBounds(
            min_value=pa.scalar(10),
            min_inclusive=False,
            max_value=pa.scalar(20),
            max_inclusive=False,
        )
        assert bounds.contains(10) is False
        assert bounds.contains(11) is True
        assert bounds.contains(19) is True
        assert bounds.contains(20) is False

    def test_contains_unbounded(self) -> None:
        """Test contains with unbounded side."""
        # No min bound
        bounds = ColumnBounds(max_value=pa.scalar(20), max_inclusive=True)
        assert bounds.contains(-1000) is True
        assert bounds.contains(20) is True
        assert bounds.contains(21) is False

        # No max bound
        bounds2 = ColumnBounds(min_value=pa.scalar(10), min_inclusive=True)
        assert bounds2.contains(10) is True
        assert bounds2.contains(1000) is True
        assert bounds2.contains(9) is False


class TestSQLGeneration:
    """Tests for SQL WHERE clause generation."""

    def test_constant_filter_sql(self) -> None:
        """Test SQL generation for constant filters."""
        filters = PushdownFilters(
            (ConstantFilter("age", 0, ComparisonOp.GE, pa.scalar(18)),)
        )
        sql, params = filters.to_sql()
        assert sql == '"age" >= ?'
        assert params == [18]

    def test_in_filter_sql(self) -> None:
        """Test SQL generation for IN filters."""
        filters = PushdownFilters((InFilter("status", 0, pa.array(["a", "b", "c"])),))
        sql, params = filters.to_sql()
        assert sql == '"status" IN (?, ?, ?)'
        assert params == ["a", "b", "c"]

    def test_is_null_sql(self) -> None:
        """Test SQL generation for IS NULL filters."""
        filters = PushdownFilters((IsNullFilter("email", 0),))
        sql, params = filters.to_sql()
        assert sql == '"email" IS NULL'
        assert params == []

    def test_multiple_filters_sql(self) -> None:
        """Test SQL generation with multiple top-level filters."""
        filters = PushdownFilters(
            (
                ConstantFilter("age", 0, ComparisonOp.GE, pa.scalar(18)),
                ConstantFilter("status", 1, ComparisonOp.EQ, pa.scalar("active")),
            )
        )
        sql, params = filters.to_sql()
        assert sql == '"age" >= ? AND "status" = ?'
        assert params == [18, "active"]

    def test_custom_placeholder(self) -> None:
        """Test SQL generation with custom placeholder."""
        filters = PushdownFilters(
            (ConstantFilter("x", 0, ComparisonOp.EQ, pa.scalar(1)),)
        )
        sql, params = filters.to_sql(placeholder="%s")
        assert sql == '"x" = %s'
        assert params == [1]


class TestVersionValidation:
    """Tests for filter version validation."""

    def test_missing_version(self) -> None:
        """Test error on missing version metadata."""
        # Create IPC bytes without version metadata
        import io

        schema = pa.schema([pa.field("filter_spec", pa.string())])
        batch = pa.RecordBatch.from_arrays(
            [pa.array(["[]"])],
            schema=schema,
        )
        sink = io.BytesIO()
        with pa.ipc.new_stream(sink, schema) as writer:
            writer.write_batch(batch)

        with pytest.raises(FilterVersionError, match="Missing"):
            deserialize_filters(sink.getvalue())

    def test_unsupported_version(self) -> None:
        """Test error on unsupported version."""
        import io

        field = pa.field(
            "filter_spec", pa.string(), metadata={b"vgi_filter_version": b"99"}
        )
        schema = pa.schema([field])
        batch = pa.RecordBatch.from_arrays([pa.array(["[]"])], schema=schema)
        sink = io.BytesIO()
        with pa.ipc.new_stream(sink, schema) as writer:
            writer.write_batch(batch)

        with pytest.raises(FilterVersionError, match="99"):
            deserialize_filters(sink.getvalue())


class TestFilterRepr:
    """Tests for filter __repr__ methods."""

    def test_constant_filter_repr(self) -> None:
        """Test ConstantFilter repr."""
        f = ConstantFilter("age", 0, ComparisonOp.GE, pa.scalar(18))
        assert "age" in repr(f)
        assert ">=" in repr(f)
        assert "18" in repr(f)

    def test_in_filter_repr(self) -> None:
        """Test InFilter repr."""
        f = InFilter("status", 0, pa.array(["a", "b"]))
        assert "status" in repr(f)
        assert "IN" in repr(f)

    def test_and_filter_repr(self) -> None:
        """Test AndFilter repr."""
        f = AndFilter(
            "x",
            0,
            (
                ConstantFilter("x", 0, ComparisonOp.GE, pa.scalar(1)),
                ConstantFilter("x", 0, ComparisonOp.LE, pa.scalar(10)),
            ),
        )
        assert "AndFilter" in repr(f)
        assert "AND" in repr(f)

    def test_pushdown_filters_repr(self) -> None:
        """Test PushdownFilters repr."""
        filters = PushdownFilters(())
        assert repr(filters) == "PushdownFilters([])"

        filters2 = PushdownFilters(
            (ConstantFilter("x", 0, ComparisonOp.EQ, pa.scalar(1)),)
        )
        assert "PushdownFilters" in repr(filters2)


class TestAutoApplyFilters:
    """Tests for auto_apply_filters Meta flag integration."""

    def test_auto_apply_filters_enabled(self) -> None:
        """Test that auto_apply_filters=True filters output batches."""

        class FilteredGenerator(TableFunctionGenerator):
            class Meta:
                filter_pushdown = True
                auto_apply_filters = True
                max_workers = 1

            count: Annotated[int, Arg(0)]

            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([("n", pa.int64())])

            def process(self) -> OutputGenerator:
                # Generate values 0-9
                for i in range(self.count):
                    yield Output(
                        pa.RecordBatch.from_pydict(
                            {"n": [i]}, schema=self.output_schema
                        )
                    )

        # Filter: n >= 5
        pushdown = create_pushdown_filters(
            filters=[
                {
                    "column_name": "n",
                    "column_index": 0,
                    "type": "constant",
                    "op": "ge",
                    "value_ref": 0,
                }
            ],
            values={0: 5},
        )

        with TableFunctionTestClient(FilteredGenerator) as client:
            outputs = list(
                client.table_function(
                    arguments=Arguments(positional=(pa.scalar(10),)),
                    pushdown_filters=pushdown,
                )
            )

        # Should only get values 5-9
        all_values: list[int] = []
        for batch in outputs:
            all_values.extend(cast(list[int], batch.column("n").to_pylist()))
        assert sorted(all_values) == [5, 6, 7, 8, 9]

    def test_auto_apply_filters_disabled(self) -> None:
        """Test that without auto_apply_filters, all rows are returned."""

        class UnfilteredGenerator(TableFunctionGenerator):
            class Meta:
                filter_pushdown = True
                # auto_apply_filters NOT set
                max_workers = 1

            count: Annotated[int, Arg(0)]

            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([("n", pa.int64())])

            def process(self) -> OutputGenerator:
                for i in range(self.count):
                    yield Output(
                        pa.RecordBatch.from_pydict(
                            {"n": [i]}, schema=self.output_schema
                        )
                    )

        # Filter would match n >= 5, but not auto-applied
        pushdown = create_pushdown_filters(
            filters=[
                {
                    "column_name": "n",
                    "column_index": 0,
                    "type": "constant",
                    "op": "ge",
                    "value_ref": 0,
                }
            ],
            values={0: 5},
        )

        with TableFunctionTestClient(UnfilteredGenerator) as client:
            outputs = list(
                client.table_function(
                    arguments=Arguments(positional=(pa.scalar(10),)),
                    pushdown_filters=pushdown,
                )
            )

        # All values should be present (filter not applied)
        all_values: list[int] = []
        for batch in outputs:
            all_values.extend(cast(list[int], batch.column("n").to_pylist()))
        assert sorted(all_values) == list(range(10))

    def test_pushdown_filters_property_access(self) -> None:
        """Test that functions can access pushdown_filters property."""
        accessed_filters: list[PushdownFilters | None] = []

        class InspectingGenerator(TableFunctionGenerator):
            class Meta:
                filter_pushdown = True
                max_workers = 1

            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([("n", pa.int64())])

            def process(self) -> OutputGenerator:
                # Access pushdown_filters for inspection
                accessed_filters.append(self.pushdown_filters)
                batch = pa.RecordBatch.from_pydict(
                    {"n": [1, 2, 3]}, schema=self.output_schema
                )
                yield Output(batch)

        pushdown = create_pushdown_filters(
            filters=[
                {
                    "column_name": "n",
                    "column_index": 0,
                    "type": "constant",
                    "op": "ge",
                    "value_ref": 0,
                }
            ],
            values={0: 2},
        )

        with TableFunctionTestClient(InspectingGenerator) as client:
            list(client.table_function(pushdown_filters=pushdown))

        assert len(accessed_filters) == 1
        assert accessed_filters[0] is not None
        assert len(accessed_filters[0]) == 1
        assert isinstance(accessed_filters[0].filters[0], ConstantFilter)
