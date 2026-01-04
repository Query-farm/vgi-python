"""Tests for vgi.table_in_out_function_patterns base classes."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import structlog

from vgi.arguments import Arg, Arguments
from vgi.invocation import Invocation
from vgi.log import Level
from vgi.table_in_out_function_patterns import (
    AggregationFunction,
    FilterFunction,
    MapFunction,
)
from vgi.testing import FunctionTestClient

# =============================================================================
# AggregationFunction Tests
# =============================================================================


class SumAggregation(AggregationFunction):
    """Test aggregation: sum all numeric columns."""

    def __init__(
        self, invocation: Invocation, logger: structlog.stdlib.BoundLogger
    ) -> None:
        """Initialize with empty sums dict."""
        super().__init__(invocation, logger)
        self._sums: dict[str, pa.Scalar[Any]] = {}

    @property
    def output_schema(self) -> pa.Schema:
        """Build schema with numeric columns promoted to int64/float64."""
        fields: list[pa.Field[Any]] = []
        for field in self.input_schema:
            if pa.types.is_integer(field.type):
                fields.append(pa.field(field.name, pa.int64()))
            elif pa.types.is_floating(field.type):
                fields.append(pa.field(field.name, pa.float64()))
        return pa.schema(fields)

    @property
    def state_schema(self) -> pa.Schema:
        """State schema matches output schema."""
        return self.output_schema

    def accumulate(self, batch: pa.RecordBatch) -> None:
        """Sum columns from this batch."""
        for field in self.output_schema:
            col_sum = pc.sum(batch.column(field.name))
            if field.name in self._sums:
                self._sums[field.name] = pc.add(self._sums[field.name], col_sum)
            else:
                self._sums[field.name] = pa.scalar(
                    col_sum.as_py() if col_sum.is_valid else 0, type=field.type
                )

    def get_accumulated_state(self) -> pa.RecordBatch:
        """Return current sums as a RecordBatch."""
        if not self._sums:
            return pa.RecordBatch.from_pydict(
                {f.name: [] for f in self.state_schema}, schema=self.state_schema
            )
        return pa.RecordBatch.from_pydict(
            {name: [scalar.as_py()] for name, scalar in self._sums.items()},
            schema=self.state_schema,
        )

    def merge_accumulated_states(self, states: pa.Table) -> None:
        """Merge partial sums from all workers."""
        self._sums = {}
        for field in self.output_schema:
            total = pc.sum(states.column(field.name))
            self._sums[field.name] = pa.scalar(
                total.as_py() if total.is_valid else 0, type=field.type
            )

    def compute_result(self) -> pa.RecordBatch:
        """Return final sums as output."""
        if not self._sums:
            return pa.RecordBatch.from_pydict(
                {f.name: [0] for f in self.output_schema}, schema=self.output_schema
            )
        return pa.RecordBatch.from_pydict(
            {name: [scalar.as_py()] for name, scalar in self._sums.items()},
            schema=self.output_schema,
        )


class TestAggregationFunction:
    """Tests for AggregationFunction base class."""

    def test_single_batch_sum(self) -> None:
        """Sum should work with a single input batch."""
        batch = pa.RecordBatch.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]})

        with FunctionTestClient(SumAggregation) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        assert len(outputs) == 1
        result = outputs[0].to_pydict()
        assert result["a"] == [6]  # 1+2+3
        assert result["b"] == [60]  # 10+20+30

    def test_multiple_batch_sum(self) -> None:
        """Sum should accumulate across multiple batches."""
        batch1 = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})
        batch2 = pa.RecordBatch.from_pydict({"x": [4, 5]})
        batch3 = pa.RecordBatch.from_pydict({"x": [6]})

        with FunctionTestClient(SumAggregation) as client:
            outputs = list(
                client.table_in_out_function(input=iter([batch1, batch2, batch3]))
            )

        assert len(outputs) == 1
        result = outputs[0].to_pydict()
        assert result["x"] == [21]  # 1+2+3+4+5+6

    def test_float_sum(self) -> None:
        """Sum should work with float columns."""
        batch = pa.RecordBatch.from_pydict({"x": [1.5, 2.5, 3.0]})

        with FunctionTestClient(SumAggregation) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        assert len(outputs) == 1
        result = outputs[0].to_pydict()
        assert result["x"] == [7.0]

    def test_excludes_non_numeric(self) -> None:
        """Sum should exclude non-numeric columns from output."""
        batch = pa.RecordBatch.from_pydict(
            {
                "num": [1, 2, 3],
                "name": ["a", "b", "c"],
            }
        )

        with FunctionTestClient(SumAggregation) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        assert len(outputs) == 1
        result = outputs[0].to_pydict()
        assert "num" in result
        assert "name" not in result

    def test_logs_accumulation(self) -> None:
        """AggregationFunction should log accumulation at DEBUG level."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})

        with FunctionTestClient(SumAggregation) as client:
            list(client.table_in_out_function(input=iter([batch])))

            debug_logs = [log for log in client.logs if log.level == Level.DEBUG]
            assert any("Accumulating" in log.message for log in debug_logs)
            assert any("Aggregation complete" in log.message for log in debug_logs)


# =============================================================================
# FilterFunction Tests
# =============================================================================


class PositiveFilter(FilterFunction):
    """Test filter: keep rows where 'value' column is positive."""

    def predicate(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Return True for positive values."""
        return pc.greater(batch.column("value"), pa.scalar(0))


class RangeFilter(FilterFunction):
    """Test filter: keep rows where 'value' is in [min_val, max_val]."""

    min_val = Arg[int](0)
    max_val = Arg[int](1)

    def predicate(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Return True for values in the specified range."""
        col = batch.column("value")
        above_min = pc.greater_equal(col, pa.scalar(self.min_val))
        below_max = pc.less_equal(col, pa.scalar(self.max_val))
        return pc.and_(above_min, below_max)


class TestFilterFunction:
    """Tests for FilterFunction base class."""

    def test_filter_keeps_matching_rows(self) -> None:
        """Filter should keep rows that match predicate."""
        batch = pa.RecordBatch.from_pydict({"value": [-1, 0, 1, 2, -3, 4]})

        with FunctionTestClient(PositiveFilter) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        assert len(outputs) == 1
        result = outputs[0].to_pydict()
        assert result["value"] == [1, 2, 4]

    def test_filter_all_pass(self) -> None:
        """Filter should pass through all rows when all match."""
        batch = pa.RecordBatch.from_pydict({"value": [1, 2, 3, 4, 5]})

        with FunctionTestClient(PositiveFilter) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        assert len(outputs) == 1
        result = outputs[0].to_pydict()
        assert result["value"] == [1, 2, 3, 4, 5]

    def test_filter_none_pass(self) -> None:
        """Filter should return empty batch when none match."""
        batch = pa.RecordBatch.from_pydict({"value": [-1, -2, -3]})

        with FunctionTestClient(PositiveFilter) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        # Empty batches may be filtered out
        total_rows = sum(o.num_rows for o in outputs)
        assert total_rows == 0

    def test_filter_multiple_batches(self) -> None:
        """Filter should work across multiple batches."""
        batch1 = pa.RecordBatch.from_pydict({"value": [-1, 1, -2, 2]})
        batch2 = pa.RecordBatch.from_pydict({"value": [3, -3, 4]})

        with FunctionTestClient(PositiveFilter) as client:
            outputs = list(client.table_in_out_function(input=iter([batch1, batch2])))

        # Combine all outputs
        all_values: list[int] = []
        for output in outputs:
            all_values.extend(output.to_pydict()["value"])

        assert sorted(all_values) == [1, 2, 3, 4]

    def test_filter_preserves_other_columns(self) -> None:
        """Filter should preserve all columns, not just the predicate column."""
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2, 3, 4],
                "value": [-1, 1, -2, 2],
                "name": ["a", "b", "c", "d"],
            }
        )

        with FunctionTestClient(PositiveFilter) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        assert len(outputs) == 1
        result = outputs[0].to_pydict()
        assert result["id"] == [2, 4]
        assert result["value"] == [1, 2]
        assert result["name"] == ["b", "d"]

    def test_filter_with_arguments(self) -> None:
        """Filter should work with Arg descriptors."""
        batch = pa.RecordBatch.from_pydict({"value": [1, 5, 10, 15, 20]})

        with FunctionTestClient(RangeFilter) as client:
            outputs = list(
                client.table_in_out_function(
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(5), pa.scalar(15))),
                )
            )

        assert len(outputs) == 1
        result = outputs[0].to_pydict()
        assert result["value"] == [5, 10, 15]

    def test_filter_logs_stats(self) -> None:
        """FilterFunction should log filtering statistics."""
        batch = pa.RecordBatch.from_pydict({"value": [-1, 1, -2, 2]})

        with FunctionTestClient(PositiveFilter) as client:
            list(client.table_in_out_function(input=iter([batch])))

            debug_logs = [log for log in client.logs if log.level == Level.DEBUG]
            # Should log "kept X, dropped Y"
            assert any(
                "kept" in log.message and "dropped" in log.message for log in debug_logs
            )


# =============================================================================
# MapFunction Tests
# =============================================================================


class DoubleValues(MapFunction):
    """Test map: double the 'value' column."""

    def map_columns(self, batch: pa.RecordBatch) -> dict[str, pa.Array[Any]]:
        """Double the value column."""
        return {"value": pc.multiply(batch.column("value"), 2)}


class MultiColumnMap(MapFunction):
    """Test map: transform multiple columns."""

    def map_columns(self, batch: pa.RecordBatch) -> dict[str, pa.Array[Any]]:
        """Multiply a by 10, add 100 to b."""
        return {
            "a": pc.multiply(batch.column("a"), 10),
            "b": pc.add(batch.column("b"), 100),
        }


class UpperCaseMap(MapFunction):
    """Test map: convert string column to uppercase."""

    def map_columns(self, batch: pa.RecordBatch) -> dict[str, pa.Array[Any]]:
        """Convert name to uppercase."""
        return {"name": pc.utf8_upper(batch.column("name"))}


class CastToFloat(MapFunction):
    """Test map with output schema change: cast int to float."""

    @property
    def output_schema(self) -> pa.Schema:
        """Output schema with value as float64."""
        fields: list[pa.Field[Any]] = [
            pa.field("id", pa.int64()),
            pa.field("value", pa.float64()),
        ]
        return pa.schema(fields)

    def map_columns(self, batch: pa.RecordBatch) -> dict[str, pa.Array[Any]]:
        """Cast value to float64."""
        return {"value": batch.column("value").cast(pa.float64())}


class TestMapFunction:
    """Tests for MapFunction base class."""

    def test_map_transforms_column(self) -> None:
        """Map should transform specified column."""
        batch = pa.RecordBatch.from_pydict({"value": [1, 2, 3, 4, 5]})

        with FunctionTestClient(DoubleValues) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        assert len(outputs) == 1
        result = outputs[0].to_pydict()
        assert result["value"] == [2, 4, 6, 8, 10]

    def test_map_multiple_columns(self) -> None:
        """Map should transform multiple columns."""
        batch = pa.RecordBatch.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]})

        with FunctionTestClient(MultiColumnMap) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        assert len(outputs) == 1
        result = outputs[0].to_pydict()
        assert result["a"] == [10, 20, 30]  # multiplied by 10
        assert result["b"] == [110, 120, 130]  # added 100

    def test_map_preserves_other_columns(self) -> None:
        """Map should preserve columns not in map_columns result."""
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2, 3],
                "value": [10, 20, 30],
                "name": ["a", "b", "c"],
            }
        )

        with FunctionTestClient(DoubleValues) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        assert len(outputs) == 1
        result = outputs[0].to_pydict()
        assert result["id"] == [1, 2, 3]  # unchanged
        assert result["value"] == [20, 40, 60]  # doubled
        assert result["name"] == ["a", "b", "c"]  # unchanged

    def test_map_string_column(self) -> None:
        """Map should work with string transformations."""
        batch = pa.RecordBatch.from_pydict({"name": ["alice", "bob", "charlie"]})

        with FunctionTestClient(UpperCaseMap) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        assert len(outputs) == 1
        result = outputs[0].to_pydict()
        assert result["name"] == ["ALICE", "BOB", "CHARLIE"]

    def test_map_multiple_batches(self) -> None:
        """Map should work across multiple batches."""
        batch1 = pa.RecordBatch.from_pydict({"value": [1, 2]})
        batch2 = pa.RecordBatch.from_pydict({"value": [3, 4, 5]})

        with FunctionTestClient(DoubleValues) as client:
            outputs = list(client.table_in_out_function(input=iter([batch1, batch2])))

        assert len(outputs) == 2
        assert outputs[0].to_pydict()["value"] == [2, 4]
        assert outputs[1].to_pydict()["value"] == [6, 8, 10]

    def test_map_with_type_change(self) -> None:
        """Map with output_schema override should change column types."""
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2, 3],
                "value": [10, 20, 30],
            }
        )

        with FunctionTestClient(CastToFloat) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        assert len(outputs) == 1
        result = outputs[0]
        assert result.schema.field("value").type == pa.float64()
        assert result.to_pydict()["value"] == [10.0, 20.0, 30.0]


# =============================================================================
# Edge Cases and Integration Tests
# =============================================================================


class TestPatternEdgeCases:
    """Edge case tests for pattern base classes."""

    def test_aggregation_empty_input(self) -> None:
        """Aggregation with no input should not crash."""
        with FunctionTestClient(SumAggregation) as client:
            outputs = list(client.table_in_out_function(input=iter([])))

        # No input = no output
        assert len(outputs) == 0

    def test_filter_empty_batch(self) -> None:
        """Filter with empty batch should work."""
        batch = pa.RecordBatch.from_pydict(
            {"value": []},
            schema=pa.schema([pa.field("value", pa.int64())]),
        )

        with FunctionTestClient(PositiveFilter) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        # Empty input = empty or no output
        total_rows = sum(o.num_rows for o in outputs)
        assert total_rows == 0

    def test_map_empty_batch(self) -> None:
        """Map with empty batch should work."""
        batch = pa.RecordBatch.from_pydict(
            {"value": []},
            schema=pa.schema([pa.field("value", pa.int64())]),
        )

        with FunctionTestClient(DoubleValues) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        # Empty input = empty or no output
        total_rows = sum(o.num_rows for o in outputs)
        assert total_rows == 0

    def test_aggregation_single_row(self) -> None:
        """Aggregation with single row should work."""
        batch = pa.RecordBatch.from_pydict({"x": [42]})

        with FunctionTestClient(SumAggregation) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        assert len(outputs) == 1
        assert outputs[0].to_pydict()["x"] == [42]

    def test_filter_single_row_kept(self) -> None:
        """Filter with single matching row should keep it."""
        batch = pa.RecordBatch.from_pydict({"value": [5]})

        with FunctionTestClient(PositiveFilter) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        assert len(outputs) == 1
        assert outputs[0].to_pydict()["value"] == [5]

    def test_filter_single_row_dropped(self) -> None:
        """Filter with single non-matching row should drop it."""
        batch = pa.RecordBatch.from_pydict({"value": [-5]})

        with FunctionTestClient(PositiveFilter) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        total_rows = sum(o.num_rows for o in outputs)
        assert total_rows == 0
