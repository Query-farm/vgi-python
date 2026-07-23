# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for the SumAllColumnsFunction (aggregation)."""

from __future__ import annotations

import pyarrow as pa
import pytest

from tests.conftest import assert_single_result, filter_non_empty, make_schema
from vgi.arguments import Arguments
from vgi.client import Client


class TestSumAllColumnsFunction:
    """Tests for the sum_all_columns function (aggregation)."""

    def test_sum_numeric_columns(self, fixture_worker: str, numeric_batches: list[pa.RecordBatch]) -> None:
        """Should sum all numeric columns across all batches."""
        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_buffering_function(
                    function_name="sum_all_columns",
                    schema_name="main",
                    input=iter(numeric_batches),
                )
            )

        # a: 1+2+3+4+5 = 15, b: 1.5+2.5+3.0+4.0+5.0 = 16.0
        assert_single_result(output_batches, {"a": [15], "b": [16.0]})

    def test_sum_excludes_non_numeric(self, fixture_worker: str, simple_batches: list[pa.RecordBatch]) -> None:
        """Should exclude non-numeric columns from output."""
        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_buffering_function(
                    function_name="sum_all_columns",
                    schema_name="main",
                    input=iter(simple_batches),
                )
            )

        non_empty = filter_non_empty(output_batches)
        assert len(non_empty) == 1
        # Should only have numeric columns (id, value), not string (name)
        assert "id" in non_empty[0].schema.names
        assert "value" in non_empty[0].schema.names
        assert "name" not in non_empty[0].schema.names

    def test_sum_promotes_types(self, fixture_worker: str, numeric_batches: list[pa.RecordBatch]) -> None:
        """Should promote int32 to int64 and float32 to float64."""
        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_buffering_function(
                    function_name="sum_all_columns",
                    schema_name="main",
                    input=iter(numeric_batches),
                )
            )

        result_schema = filter_non_empty(output_batches)[0].schema
        # int32 input -> int64 output
        assert result_schema.field("a").type == pa.int64()
        # float64 stays float64
        assert result_schema.field("b").type == pa.float64()


class TestSumAllColumnsFunctionWithLogging:
    """Tests for sum_all_columns with logging=True (aggregation with in-band logging)."""

    @staticmethod
    def _logging_args() -> Arguments:
        return Arguments(named={"logging": pa.scalar(True)})

    def test_sum_with_logging_produces_correct_results(
        self, fixture_worker: str, numeric_batches: list[pa.RecordBatch]
    ) -> None:
        """Should produce the same sums as the non-logging version."""
        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_buffering_function(
                    function_name="sum_all_columns",
                    schema_name="main",
                    arguments=self._logging_args(),
                    input=iter(numeric_batches),
                )
            )

        # a: 1+2+3+4+5 = 15, b: 1.5+2.5+3.0+4.0+5.0 = 16.0
        assert_single_result(output_batches, {"a": [15], "b": [16.0]})

    def test_sum_with_logging_emits_log_messages(
        self, fixture_worker: str, numeric_batches: list[pa.RecordBatch]
    ) -> None:
        """Should not crash when logging is enabled."""
        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_buffering_function(
                    function_name="sum_all_columns",
                    schema_name="main",
                    arguments=self._logging_args(),
                    input=iter(numeric_batches),
                )
            )

        # Should still produce valid output
        assert len(filter_non_empty(output_batches)) == 1

    def test_sum_with_logging_handles_single_batch(
        self, fixture_worker: str, numeric_batches: list[pa.RecordBatch]
    ) -> None:
        """Should work correctly with a single input batch."""
        single_batch = [numeric_batches[0]]
        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_buffering_function(
                    function_name="sum_all_columns",
                    schema_name="main",
                    arguments=self._logging_args(),
                    input=iter(single_batch),
                )
            )

        # a: 1+2+3 = 6, b: 1.5+2.5+3.0 = 7.0
        assert_single_result(output_batches, {"a": [6], "b": [7.0]})


class TestSumAllColumnsFunctionDistributed:
    """Tests for sum_all_columns_distributed function (distributed aggregation)."""

    def test_sum_distributed_many_batches(self, fixture_worker: str) -> None:
        """Should correctly sum across multiple batches."""
        schema = make_schema([pa.field("a", pa.int64()), pa.field("b", pa.float64())])

        # Create 20 batches, each with 100 rows (enough to exercise distributed path)
        num_batches = 20
        rows_per_batch = 100
        batches = []

        expected_a_sum = 0
        expected_b_sum = 0.0

        for batch_idx in range(num_batches):
            start = batch_idx * rows_per_batch
            end = (batch_idx + 1) * rows_per_batch
            a_values = list(range(start, end))
            b_values = [float(v) * 0.5 for v in a_values]

            expected_a_sum += sum(a_values)
            expected_b_sum += sum(b_values)

            batch = pa.RecordBatch.from_pydict({"a": a_values, "b": b_values}, schema=schema)
            batches.append(batch)

        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_buffering_function(
                    function_name="sum_all_columns",
                    schema_name="main",
                    input=iter(batches),
                )
            )

        non_empty = filter_non_empty(output_batches)
        assert len(non_empty) == 1
        result = non_empty[0].to_pydict()
        assert result["a"] == [expected_a_sum]
        assert result["b"][0] == pytest.approx(expected_b_sum)

    def test_sum_distributed_excludes_non_numeric(
        self, fixture_worker: str, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Should exclude non-numeric columns from output."""
        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_buffering_function(
                    function_name="sum_all_columns",
                    schema_name="main",
                    input=iter(simple_batches),
                )
            )

        non_empty = filter_non_empty(output_batches)
        assert len(non_empty) == 1
        # Should only have numeric columns (id, value), not string (name)
        assert "id" in non_empty[0].schema.names
        assert "value" in non_empty[0].schema.names
        assert "name" not in non_empty[0].schema.names

    def test_sum_distributed_empty_batch(self, fixture_worker: str) -> None:
        """Should handle empty batch correctly."""
        schema = make_schema([pa.field("a", pa.int64()), pa.field("b", pa.float64())])
        empty_batch = pa.RecordBatch.from_pydict({"a": [], "b": []}, schema=schema)

        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_buffering_function(
                    function_name="sum_all_columns",
                    schema_name="main",
                    input=iter([empty_batch]),
                )
            )

        # Sum of empty column should be 0
        assert_single_result(output_batches, {"a": [0], "b": [0.0]})


class TestSumAllColumnsSimpleDistributed:
    """Tests for sum_all_columns_simple_distributed (buffered global reduction)."""

    def test_sum_simple_distributed_basic(self, fixture_worker: str, numeric_batches: list[pa.RecordBatch]) -> None:
        """Should sum all numeric columns across all batches."""
        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_buffering_function(
                    function_name="sum_all_columns_simple_distributed",
                    schema_name="main",
                    input=iter(numeric_batches),
                )
            )

        # a: 1+2+3+4+5 = 15, b: 1.5+2.5+3.0+4.0+5.0 = 16.0
        assert_single_result(output_batches, {"a": [15], "b": [16.0]})

    def test_sum_simple_distributed_many_batches(self, fixture_worker: str) -> None:
        """Should correctly sum across multiple batches."""
        schema = make_schema([pa.field("a", pa.int64()), pa.field("b", pa.float64())])

        # Create 20 batches, each with 100 rows (enough to exercise distributed path)
        num_batches = 20
        rows_per_batch = 100
        batches = []

        expected_a_sum = 0
        expected_b_sum = 0.0

        for batch_idx in range(num_batches):
            start = batch_idx * rows_per_batch
            end = (batch_idx + 1) * rows_per_batch
            a_values = list(range(start, end))
            b_values = [float(v) * 0.5 for v in a_values]

            expected_a_sum += sum(a_values)
            expected_b_sum += sum(b_values)

            batch = pa.RecordBatch.from_pydict({"a": a_values, "b": b_values}, schema=schema)
            batches.append(batch)

        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_buffering_function(
                    function_name="sum_all_columns_simple_distributed",
                    schema_name="main",
                    input=iter(batches),
                )
            )

        non_empty = filter_non_empty(output_batches)
        assert len(non_empty) == 1
        result = non_empty[0].to_pydict()
        assert result["a"] == [expected_a_sum]
        assert result["b"][0] == pytest.approx(expected_b_sum)

    def test_sum_simple_distributed_excludes_non_numeric(
        self, fixture_worker: str, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Should exclude non-numeric columns from output."""
        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_buffering_function(
                    function_name="sum_all_columns_simple_distributed",
                    schema_name="main",
                    input=iter(simple_batches),
                )
            )

        non_empty = filter_non_empty(output_batches)
        assert len(non_empty) == 1
        # Should only have numeric columns (id, value), not string (name)
        assert "id" in non_empty[0].schema.names
        assert "value" in non_empty[0].schema.names
        assert "name" not in non_empty[0].schema.names

    def test_sum_simple_distributed_empty_batch(self, fixture_worker: str) -> None:
        """Should handle empty batch correctly."""
        schema = make_schema([pa.field("a", pa.int64()), pa.field("b", pa.float64())])
        empty_batch = pa.RecordBatch.from_pydict({"a": [], "b": []}, schema=schema)

        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_buffering_function(
                    function_name="sum_all_columns_simple_distributed",
                    schema_name="main",
                    input=iter([empty_batch]),
                )
            )

        # Sum of empty column should be 0
        assert_single_result(output_batches, {"a": [0], "b": [0.0]})
