"""Tests for the SumAllColumnsFunction (aggregation)."""

import pyarrow as pa
import pytest

from vgi.client import Client


class TestSumAllColumnsFunction:
    """Tests for the sum_all_columns function (aggregation)."""

    def test_sum_numeric_columns(
        self, example_worker: str, numeric_batches: list[pa.RecordBatch]
    ) -> None:
        """Should sum all numeric columns across all batches."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="sum_all_columns",
                    input=iter(numeric_batches),
                )
            )

        # Should get empty batches during data phase, then single row on finalize
        non_empty = [b for b in output_batches if b.num_rows > 0]
        assert len(non_empty) == 1

        result = non_empty[0].to_pydict()
        # a: 1+2+3+4+5 = 15
        assert result["a"] == [15]
        # b: 1.5+2.5+3.0+4.0+5.0 = 16.0
        assert result["b"] == [16.0]

    def test_sum_excludes_non_numeric(
        self, example_worker: str, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Should exclude non-numeric columns from output."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="sum_all_columns",
                    input=iter(simple_batches),
                )
            )

        non_empty = [b for b in output_batches if b.num_rows > 0]
        assert len(non_empty) == 1

        result = non_empty[0]
        # Should only have numeric columns (id, value), not string (name)
        assert "id" in result.schema.names
        assert "value" in result.schema.names
        assert "name" not in result.schema.names

    def test_sum_promotes_types(
        self, example_worker: str, numeric_batches: list[pa.RecordBatch]
    ) -> None:
        """Should promote int32 to int64 and float32 to float64."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="sum_all_columns",
                    input=iter(numeric_batches),
                )
            )

        non_empty = [b for b in output_batches if b.num_rows > 0]
        result_schema = non_empty[0].schema

        # int32 input -> int64 output
        assert result_schema.field("a").type == pa.int64()
        # float64 stays float64
        assert result_schema.field("b").type == pa.float64()


class TestSumAllColumnsFunctionWithLogging:
    """Tests for sum_all_columns_with_logging function (aggregation with logging)."""

    def test_sum_with_logging_produces_correct_results(
        self, example_worker: str, numeric_batches: list[pa.RecordBatch]
    ) -> None:
        """Should produce the same sums as the non-logging version."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="sum_all_columns_with_logging",
                    input=iter(numeric_batches),
                )
            )

        non_empty = [b for b in output_batches if b.num_rows > 0]
        assert len(non_empty) == 1

        result = non_empty[0].to_pydict()
        # a: 1+2+3+4+5 = 15
        assert result["a"] == [15]
        # b: 1.5+2.5+3.0+4.0+5.0 = 16.0
        assert result["b"] == [16.0]

    def test_sum_with_logging_emits_log_messages(
        self, example_worker: str, numeric_batches: list[pa.RecordBatch]
    ) -> None:
        """Should emit log messages for each batch processed."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="sum_all_columns_with_logging",
                    input=iter(numeric_batches),
                )
            )
            stderr = client.get_worker_stderr()

        # Should still produce valid output
        non_empty = [b for b in output_batches if b.num_rows > 0]
        assert len(non_empty) == 1

        # Log messages should appear in worker stderr (via client logging)
        # The function logs "Processing batch with N rows" for each batch
        assert "Processing batch" in stderr or "rows" in stderr

    def test_sum_with_logging_handles_single_batch(
        self, example_worker: str, numeric_batches: list[pa.RecordBatch]
    ) -> None:
        """Should work correctly with a single input batch."""
        single_batch = [numeric_batches[0]]
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="sum_all_columns_with_logging",
                    input=iter(single_batch),
                )
            )

        non_empty = [b for b in output_batches if b.num_rows > 0]
        assert len(non_empty) == 1

        result = non_empty[0].to_pydict()
        # a: 1+2+3 = 6
        assert result["a"] == [6]
        # b: 1.5+2.5+3.0 = 7.0
        assert result["b"] == [7.0]


class TestSumAllColumnsFunctionDistributed:
    """Tests for sum_all_columns_distributed function (distributed aggregation)."""

    def test_sum_distributed_many_batches(self, example_worker: str) -> None:
        """Should correctly sum across 200 batches with many rows each."""
        schema = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.float64())])

        # Create 200 batches, each with 100 rows
        num_batches = 200
        rows_per_batch = 100
        batches = []

        expected_a_sum = 0
        expected_b_sum = 0.0

        for batch_idx in range(num_batches):
            # Each batch has values based on batch index to make sums predictable
            start = batch_idx * rows_per_batch
            end = (batch_idx + 1) * rows_per_batch
            a_values = list(range(start, end))
            b_values = [float(v) * 0.5 for v in a_values]

            expected_a_sum += sum(a_values)
            expected_b_sum += sum(b_values)

            batch = pa.RecordBatch.from_pydict(
                {"a": a_values, "b": b_values}, schema=schema
            )
            batches.append(batch)

        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="sum_all_columns_distributed",
                    input=iter(batches),
                )
            )

        non_empty = [b for b in output_batches if b.num_rows > 0]
        assert len(non_empty) == 1

        result = non_empty[0].to_pydict()
        # Total rows: 200 * 100 = 20,000
        # a: sum of 0..19999 = 19999 * 20000 / 2 = 199,990,000
        assert result["a"] == [expected_a_sum]
        assert result["b"][0] == pytest.approx(expected_b_sum)

    def test_sum_distributed_excludes_non_numeric(
        self, example_worker: str, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Should exclude non-numeric columns from output."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="sum_all_columns_distributed",
                    input=iter(simple_batches),
                )
            )

        non_empty = [b for b in output_batches if b.num_rows > 0]
        assert len(non_empty) == 1

        result = non_empty[0]
        # Should only have numeric columns (id, value), not string (name)
        assert "id" in result.schema.names
        assert "value" in result.schema.names
        assert "name" not in result.schema.names

    def test_sum_distributed_empty_batch(self, example_worker: str) -> None:
        """Should handle empty batch correctly."""
        schema = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.float64())])
        empty_batch = pa.RecordBatch.from_pydict({"a": [], "b": []}, schema=schema)

        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="sum_all_columns_distributed",
                    input=iter([empty_batch]),
                )
            )

        non_empty = [b for b in output_batches if b.num_rows > 0]
        assert len(non_empty) == 1

        result = non_empty[0].to_pydict()
        # Sum of empty column should be 0
        assert result["a"] == [0]
        assert result["b"] == [0.0]
