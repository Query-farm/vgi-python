"""End-to-end tests for scalar functions via Client subprocess."""

from __future__ import annotations

from typing import cast

import pyarrow as pa
import pytest

from tests.conftest import assert_total_rows
from vgi import schema
from vgi.arguments import Arguments
from vgi.client import Client
from vgi.client.client import ClientError


class TestScalarFunctionClient:
    """Tests for scalar functions via Client subprocess."""

    def test_double_column_basic(self, example_worker: str) -> None:
        """Test basic scalar function via Client."""
        s = schema(x=pa.int64())
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=s)

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="double_column",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar("x"),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [2, 4, 6]}

    def test_add_columns(self, example_worker: str) -> None:
        """Test add_columns scalar function."""
        s = schema(a=pa.int64(), b=pa.int64())
        batch = pa.RecordBatch.from_pydict(
            {"a": [1, 2, 3], "b": [10, 20, 30]}, schema=s
        )

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="add_columns",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar("a"), pa.scalar("b"))),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [11, 22, 33]}

    def test_upper_case(self, example_worker: str) -> None:
        """Test upper_case scalar function."""
        s = schema(name=pa.string())
        batch = pa.RecordBatch.from_pydict(
            {"name": ["alice", "bob", "charlie"]}, schema=s
        )

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="upper_case",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar("name"),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": ["ALICE", "BOB", "CHARLIE"]}

    def test_multiple_batches(self, example_worker: str) -> None:
        """Test scalar function with multiple input batches."""
        s = schema(x=pa.int64())
        batch1 = pa.RecordBatch.from_pydict({"x": [1, 2]}, schema=s)
        batch2 = pa.RecordBatch.from_pydict({"x": [3, 4, 5]}, schema=s)
        batch3 = pa.RecordBatch.from_pydict({"x": [6]}, schema=s)

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="double_column",
                    input=iter([batch1, batch2, batch3]),
                    arguments=Arguments(positional=(pa.scalar("x"),)),
                )
            )

        # Should get 3 output batches (one per input)
        assert len(outputs) == 3
        assert_total_rows(outputs, 6)

        # Verify the values (order may vary in parallel mode, but we're single-worker)
        all_values: list[int] = []
        for batch in outputs:
            all_values.extend(cast(list[int], batch.column("result").to_pylist()))
        assert sorted(all_values) == [2, 4, 6, 8, 10, 12]

    def test_empty_batch(self, example_worker: str) -> None:
        """Test scalar function with empty batch."""
        s = schema(x=pa.int64())
        empty_batch = pa.RecordBatch.from_pydict({"x": []}, schema=s)

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="double_column",
                    input=iter([empty_batch]),
                    arguments=Arguments(positional=(pa.scalar("x"),)),
                )
            )

        # Should get one output batch with zero rows
        assert len(outputs) == 1
        assert outputs[0].num_rows == 0

    def test_empty_iterator_raises(self, example_worker: str) -> None:
        """Test scalar function with no input batches raises error."""
        with (
            Client(example_worker) as client,
            pytest.raises(ClientError, match="requires at least one input batch"),
        ):
            list(
                client.scalar_function(
                    function_name="double_column",
                    input=iter([]),
                    arguments=Arguments(positional=(pa.scalar("x"),)),
                )
            )

    def test_scalar_function_not_started_raises(self, example_worker: str) -> None:
        """Calling scalar_function before start should raise ClientError."""
        client = Client(example_worker)
        s = schema(x=pa.int64())
        batch = pa.RecordBatch.from_pydict({"x": [1]}, schema=s)

        with pytest.raises(ClientError, match="not started"):
            list(
                client.scalar_function(
                    function_name="double_column",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar("x"),)),
                )
            )

    def test_large_batch(self, example_worker: str) -> None:
        """Test scalar function with a large batch."""
        s = schema(x=pa.int64())
        large_data = list(range(10000))
        batch = pa.RecordBatch.from_pydict({"x": large_data}, schema=s)

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="double_column",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar("x"),)),
                )
            )

        assert_total_rows(outputs, 10000)

        # Verify first and last values
        all_values = []
        for b in outputs:
            all_values.extend(b.column("result").to_pylist())
        assert all_values[0] == 0  # 0 * 2 = 0
        assert all_values[-1] == 19998  # 9999 * 2 = 19998

    def test_bind_result_callback(self, example_worker: str) -> None:
        """Test that bind_result_callback is invoked."""
        s = schema(x=pa.int64())
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=s)

        bind_results: list[pa.RecordBatch] = []

        def capture_bind_result(result: pa.RecordBatch) -> None:
            bind_results.append(result)

        with Client(example_worker) as client:
            list(
                client.scalar_function(
                    function_name="double_column",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar("x"),)),
                    bind_result_callback=capture_bind_result,
                )
            )

        # Should have received bind result
        assert len(bind_results) == 1
        bind_result = bind_results[0]

        # Verify bind result contains expected fields
        assert "output_schema" in bind_result.schema.names
        assert "max_processes" in bind_result.schema.names

    def test_add_columns_accepts_float_columns(self, example_worker: str) -> None:
        """Test that add_columns accepts float columns."""
        s = schema(a=pa.float64(), b=pa.float64())
        batch = pa.RecordBatch.from_pydict({"a": [1.5, 2.5], "b": [0.5, 0.5]}, schema=s)

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="add_columns",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar("a"), pa.scalar("b"))),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [2.0, 3.0]}

    def test_add_columns_accepts_mixed_int_types(self, example_worker: str) -> None:
        """Test that add_columns accepts mixed integer types and promotes correctly."""
        s = schema(a=pa.int32(), b=pa.int64())
        batch = pa.RecordBatch.from_pydict({"a": [1, 2], "b": [10, 20]}, schema=s)

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="add_columns",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar("a"), pa.scalar("b"))),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [11, 22]}
        # Output should be int64 (promoted from int64 common type)
        assert outputs[0].schema.field("result").type == pa.int64()


class TestSumColumns:
    """Tests for SumColumnsFunction via Client."""

    def test_sum_two_columns(self, example_worker: str) -> None:
        """Sum of two columns."""
        s = schema(a=pa.int64(), b=pa.int64())
        batch = pa.RecordBatch.from_pydict(
            {"a": [1, 2, 3], "b": [10, 20, 30]}, schema=s
        )

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="sum_columns",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar("a"), pa.scalar("b"))),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [11, 22, 33]}

    def test_sum_three_columns(self, example_worker: str) -> None:
        """Sum of three columns using varargs."""
        s = schema(a=pa.int64(), b=pa.int64(), c=pa.int64())
        batch = pa.RecordBatch.from_pydict(
            {"a": [1, 2], "b": [10, 20], "c": [100, 200]}, schema=s
        )

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="sum_columns",
                    input=iter([batch]),
                    arguments=Arguments(
                        positional=(pa.scalar("a"), pa.scalar("b"), pa.scalar("c"))
                    ),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [111, 222]}

    def test_sum_with_type_promotion(self, example_worker: str) -> None:
        """Different int types promote correctly."""
        s = schema(a=pa.int32(), b=pa.int64())
        batch = pa.RecordBatch.from_pydict({"a": [1, 2], "b": [10, 20]}, schema=s)

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="sum_columns",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar("a"), pa.scalar("b"))),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [11, 22]}
        # Output should be int64 (promoted from int32)
        assert outputs[0].schema.field("result").type == pa.int64()

    def test_sum_rejects_string_column(self, example_worker: str) -> None:
        """Type bound rejects non-numeric columns."""
        s = schema(a=pa.int64(), b=pa.string())
        batch = pa.RecordBatch.from_pydict({"a": [1, 2], "b": ["x", "y"]}, schema=s)

        with (
            Client(example_worker) as client,
            pytest.raises(Exception, match="does not match any of"),
        ):
            list(
                client.scalar_function(
                    function_name="sum_columns",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar("a"), pa.scalar("b"))),
                )
            )

    def test_sum_multiple_batches(self, example_worker: str) -> None:
        """Multiple input batches processed correctly."""
        s = schema(a=pa.int64(), b=pa.int64())
        batch1 = pa.RecordBatch.from_pydict({"a": [1, 2], "b": [10, 20]}, schema=s)
        batch2 = pa.RecordBatch.from_pydict({"a": [3, 4], "b": [30, 40]}, schema=s)

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="sum_columns",
                    input=iter([batch1, batch2]),
                    arguments=Arguments(positional=(pa.scalar("a"), pa.scalar("b"))),
                )
            )

        assert_total_rows(outputs, 4)
        all_values: list[int] = []
        for batch in outputs:
            all_values.extend(cast(list[int], batch.column("result").to_pylist()))
        assert sorted(all_values) == [11, 22, 33, 44]

    def test_sum_empty_batch(self, example_worker: str) -> None:
        """Empty batch returns empty output."""
        s = schema(a=pa.int64(), b=pa.int64())
        empty_batch = pa.RecordBatch.from_pydict({"a": [], "b": []}, schema=s)

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="sum_columns",
                    input=iter([empty_batch]),
                    arguments=Arguments(positional=(pa.scalar("a"), pa.scalar("b"))),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].num_rows == 0

    def test_sum_float_columns(self, example_worker: str) -> None:
        """Sum of float columns."""
        s = schema(a=pa.float64(), b=pa.float64())
        batch = pa.RecordBatch.from_pydict({"a": [1.5, 2.5], "b": [0.5, 0.5]}, schema=s)

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="sum_columns",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar("a"), pa.scalar("b"))),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [2.0, 3.0]}


class TestScalarFunctionParallel:
    """Tests for scalar functions with parallel processing."""

    def test_parallel_double_column(self, example_worker: str) -> None:
        """Test scalar function with multiple workers."""
        s = schema(x=pa.int64())
        batches = [
            pa.RecordBatch.from_pydict(
                {"x": list(range(i * 100, (i + 1) * 100))}, schema=s
            )
            for i in range(10)
        ]

        with Client(example_worker, max_workers=4) as client:
            outputs = list(
                client.scalar_function(
                    function_name="double_column",
                    input=iter(batches),
                    arguments=Arguments(positional=(pa.scalar("x"),)),
                )
            )

        # Should get all 1000 rows back
        assert_total_rows(outputs, 1000)

        # Verify all values are correctly doubled
        all_values = set()
        for batch in outputs:
            all_values.update(batch.column("result").to_pylist())

        expected = {i * 2 for i in range(1000)}
        assert all_values == expected

    def test_parallel_add_columns(self, example_worker: str) -> None:
        """Test add_columns with multiple workers."""
        s = schema(a=pa.int64(), b=pa.int64())
        batches = [
            pa.RecordBatch.from_pydict(
                {"a": [i, i + 1, i + 2], "b": [100, 200, 300]}, schema=s
            )
            for i in range(20)
        ]

        with Client(example_worker, max_workers=3) as client:
            outputs = list(
                client.scalar_function(
                    function_name="add_columns",
                    input=iter(batches),
                    arguments=Arguments(positional=(pa.scalar("a"), pa.scalar("b"))),
                )
            )

        # Should get 60 rows total (20 batches * 3 rows)
        assert_total_rows(outputs, 60)

    def test_parallel_empty_batches_mixed(self, example_worker: str) -> None:
        """Test parallel processing with mix of empty and non-empty batches."""
        s = schema(x=pa.int64())
        batches = [
            pa.RecordBatch.from_pydict({"x": [1, 2]}, schema=s),
            pa.RecordBatch.from_pydict({"x": []}, schema=s),  # Empty
            pa.RecordBatch.from_pydict({"x": [3]}, schema=s),
            pa.RecordBatch.from_pydict({"x": []}, schema=s),  # Empty
            pa.RecordBatch.from_pydict({"x": [4, 5, 6]}, schema=s),
        ]

        with Client(example_worker, max_workers=2) as client:
            outputs = list(
                client.scalar_function(
                    function_name="double_column",
                    input=iter(batches),
                    arguments=Arguments(positional=(pa.scalar("x"),)),
                )
            )

        # Should get 6 rows total (2 + 0 + 1 + 0 + 3)
        assert_total_rows(outputs, 6)

        # Verify values
        all_values = set()
        for batch in outputs:
            all_values.update(batch.column("result").to_pylist())
        assert all_values == {2, 4, 6, 8, 10, 12}

    def test_parallel_single_batch(self, example_worker: str) -> None:
        """Test parallel mode with just one batch (should still work)."""
        s = schema(x=pa.int64())
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=s)

        with Client(example_worker, max_workers=4) as client:
            outputs = list(
                client.scalar_function(
                    function_name="double_column",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar("x"),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [2, 4, 6]}


class TestScalarMultiWorkerEdgeCases:
    """Tests for edge cases with multiple workers for scalar functions.

    These tests expose timeout/hang bugs when:
    - Processing parquet with one batch of zero rows
    - Additional workers spawned but don't receive batches
    """

    def test_zero_row_batch_single_worker(self, example_worker: str) -> None:
        """Baseline: zero-row batch with max_workers=1 should complete quickly."""
        s = schema(x=pa.int64())
        zero_row_batch = pa.RecordBatch.from_pydict({"x": []}, schema=s)

        with Client(example_worker, max_workers=1) as client:
            outputs = list(
                client.scalar_function(
                    function_name="double_column",
                    input=iter([zero_row_batch]),
                    arguments=Arguments(positional=(pa.scalar("x"),)),
                )
            )

        # Should complete without hanging
        assert len(outputs) == 1
        assert outputs[0].num_rows == 0

    def test_zero_row_batch_forced_multiple_workers(self, example_worker: str) -> None:
        """Zero-row batch with max_workers=4 should complete without hanging."""
        s = schema(x=pa.int64())
        zero_row_batch = pa.RecordBatch.from_pydict({"x": []}, schema=s)

        # Force 4 workers even though there's only one batch with zero rows
        with Client(example_worker, max_workers=4) as client:
            outputs = list(
                client.scalar_function(
                    function_name="double_column",
                    input=iter([zero_row_batch]),
                    arguments=Arguments(positional=(pa.scalar("x"),)),
                )
            )

        # Should complete without hanging
        assert len(outputs) == 1
        assert outputs[0].num_rows == 0

    def test_single_batch_multiple_workers(self, example_worker: str) -> None:
        """Single normal batch with max_workers=4 should complete without hanging."""
        s = schema(x=pa.int64())
        single_batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=s)

        # Force 4 workers even though there's only 1 batch
        with Client(example_worker, max_workers=4) as client:
            outputs = list(
                client.scalar_function(
                    function_name="double_column",
                    input=iter([single_batch]),
                    arguments=Arguments(positional=(pa.scalar("x"),)),
                )
            )

        # Should complete without hanging and return correct data
        assert_total_rows(outputs, 3)

    def test_fewer_batches_than_workers(self, example_worker: str) -> None:
        """2 batches with max_workers=4 should complete without hanging."""
        s = schema(x=pa.int64())
        batch1 = pa.RecordBatch.from_pydict({"x": [1, 2]}, schema=s)
        batch2 = pa.RecordBatch.from_pydict({"x": [3, 4, 5]}, schema=s)

        # Force 4 workers even though there are only 2 batches
        with Client(example_worker, max_workers=4) as client:
            outputs = list(
                client.scalar_function(
                    function_name="double_column",
                    input=iter([batch1, batch2]),
                    arguments=Arguments(positional=(pa.scalar("x"),)),
                )
            )

        # Should complete without hanging and return correct data (5 rows total)
        assert_total_rows(outputs, 5)
