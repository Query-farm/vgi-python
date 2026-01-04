"""End-to-end tests for scalar functions via Client subprocess."""

from __future__ import annotations

from typing import cast

import pyarrow as pa
import pytest

from vgi.client import Client
from vgi.client.client import ClientError
from vgi.function import Arguments


class TestScalarFunctionClient:
    """Tests for scalar functions via Client subprocess."""

    def test_double_column_basic(self, example_worker: str) -> None:
        """Test basic scalar function via Client."""
        schema = pa.schema([("x", pa.int64())])
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=schema)

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
        schema = pa.schema([("a", pa.int64()), ("b", pa.int64())])
        batch = pa.RecordBatch.from_pydict(
            {"a": [1, 2, 3], "b": [10, 20, 30]}, schema=schema
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
        schema = pa.schema([("name", pa.string())])
        batch = pa.RecordBatch.from_pydict(
            {"name": ["alice", "bob", "charlie"]}, schema=schema
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
        schema = pa.schema([("x", pa.int64())])
        batch1 = pa.RecordBatch.from_pydict({"x": [1, 2]}, schema=schema)
        batch2 = pa.RecordBatch.from_pydict({"x": [3, 4, 5]}, schema=schema)
        batch3 = pa.RecordBatch.from_pydict({"x": [6]}, schema=schema)

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
        total_rows = sum(b.num_rows for b in outputs)
        assert total_rows == 6

        # Verify the values (order may vary in parallel mode, but we're single-worker)
        all_values: list[int] = []
        for batch in outputs:
            all_values.extend(cast(list[int], batch.column("result").to_pylist()))
        assert sorted(all_values) == [2, 4, 6, 8, 10, 12]

    def test_empty_batch(self, example_worker: str) -> None:
        """Test scalar function with empty batch."""
        schema = pa.schema([("x", pa.int64())])
        empty_batch = pa.RecordBatch.from_pydict({"x": []}, schema=schema)

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

    def test_empty_iterator(self, example_worker: str) -> None:
        """Test scalar function with no input batches."""
        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="double_column",
                    input=iter([]),
                    arguments=Arguments(positional=(pa.scalar("x"),)),
                )
            )

        # No input means no output
        assert len(outputs) == 0

    def test_scalar_function_not_started_raises(self, example_worker: str) -> None:
        """Calling scalar_function before start should raise ClientError."""
        client = Client(example_worker)
        schema = pa.schema([("x", pa.int64())])
        batch = pa.RecordBatch.from_pydict({"x": [1]}, schema=schema)

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
        schema = pa.schema([("x", pa.int64())])
        large_data = list(range(10000))
        batch = pa.RecordBatch.from_pydict({"x": large_data}, schema=schema)

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="double_column",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar("x"),)),
                )
            )

        total_rows = sum(b.num_rows for b in outputs)
        assert total_rows == 10000

        # Verify first and last values
        all_values = []
        for b in outputs:
            all_values.extend(b.column("result").to_pylist())
        assert all_values[0] == 0  # 0 * 2 = 0
        assert all_values[-1] == 19998  # 9999 * 2 = 19998

    def test_bind_result_callback(self, example_worker: str) -> None:
        """Test that bind_result_callback is invoked."""
        schema = pa.schema([("x", pa.int64())])
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=schema)

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


class TestScalarFunctionParallel:
    """Tests for scalar functions with parallel processing."""

    def test_parallel_double_column(self, example_worker: str) -> None:
        """Test scalar function with multiple workers."""
        schema = pa.schema([("x", pa.int64())])
        batches = [
            pa.RecordBatch.from_pydict(
                {"x": list(range(i * 100, (i + 1) * 100))}, schema=schema
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
        total_rows = sum(b.num_rows for b in outputs)
        assert total_rows == 1000

        # Verify all values are correctly doubled
        all_values = set()
        for batch in outputs:
            all_values.update(batch.column("result").to_pylist())

        expected = {i * 2 for i in range(1000)}
        assert all_values == expected

    def test_parallel_add_columns(self, example_worker: str) -> None:
        """Test add_columns with multiple workers."""
        schema = pa.schema([("a", pa.int64()), ("b", pa.int64())])
        batches = [
            pa.RecordBatch.from_pydict(
                {"a": [i, i + 1, i + 2], "b": [100, 200, 300]}, schema=schema
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
        total_rows = sum(b.num_rows for b in outputs)
        assert total_rows == 60

    def test_parallel_empty_batches_mixed(self, example_worker: str) -> None:
        """Test parallel processing with mix of empty and non-empty batches."""
        schema = pa.schema([("x", pa.int64())])
        batches = [
            pa.RecordBatch.from_pydict({"x": [1, 2]}, schema=schema),
            pa.RecordBatch.from_pydict({"x": []}, schema=schema),  # Empty
            pa.RecordBatch.from_pydict({"x": [3]}, schema=schema),
            pa.RecordBatch.from_pydict({"x": []}, schema=schema),  # Empty
            pa.RecordBatch.from_pydict({"x": [4, 5, 6]}, schema=schema),
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
        total_rows = sum(b.num_rows for b in outputs)
        assert total_rows == 6

        # Verify values
        all_values = set()
        for batch in outputs:
            all_values.update(batch.column("result").to_pylist())
        assert all_values == {2, 4, 6, 8, 10, 12}

    def test_parallel_single_batch(self, example_worker: str) -> None:
        """Test parallel mode with just one batch (should still work)."""
        schema = pa.schema([("x", pa.int64())])
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=schema)

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
