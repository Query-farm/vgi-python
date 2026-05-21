# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for the RepeatInputsFunction (explosion)."""

from __future__ import annotations

import pyarrow as pa

from tests.conftest import make_schema, total_rows
from vgi.arguments import Arguments
from vgi.client import Client


class TestRepeatInputsFunction:
    """Tests for the repeat_inputs function (explosion)."""

    def test_repeat_custom_count(self, fixture_worker: str, simple_batches: list[pa.RecordBatch]) -> None:
        """Should respect custom repeat count argument."""
        repeat_count = 3
        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="repeat_inputs",
                    arguments=Arguments(positional=(pa.scalar(repeat_count),)),
                    input=iter(simple_batches),
                )
            )

        assert total_rows(output_batches) == total_rows(simple_batches) * repeat_count

    def test_repeat_single_time(self, fixture_worker: str, simple_batches: list[pa.RecordBatch]) -> None:
        """Repeat count of 1 should act like echo."""
        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="repeat_inputs",
                    arguments=Arguments(positional=(pa.scalar(1),), named={}),
                    input=iter(simple_batches),
                )
            )

        assert total_rows(output_batches) == total_rows(simple_batches)

    def test_repeat_distributed_many_batches(self, fixture_worker: str) -> None:
        """Should correctly repeat across many batches with multiple workers."""
        schema = make_schema([pa.field("a", pa.int64()), pa.field("b", pa.float64())])

        # Create 100 batches, each with 50 rows
        num_batches = 100
        rows_per_batch = 50
        repeat_count = 3
        batches = []

        for batch_idx in range(num_batches):
            start = batch_idx * rows_per_batch
            end = (batch_idx + 1) * rows_per_batch
            a_values = list(range(start, end))
            b_values = [float(v) * 0.5 for v in a_values]

            batch = pa.RecordBatch.from_pydict({"a": a_values, "b": b_values}, schema=schema)
            batches.append(batch)

        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="repeat_inputs",
                    arguments=Arguments(positional=(pa.scalar(repeat_count),)),
                    input=iter(batches),
                )
            )

        assert total_rows(output_batches) == total_rows(batches) * repeat_count

    def test_repeat_distributed_preserves_data(self, fixture_worker: str) -> None:
        """Should preserve data correctly when repeated across workers."""
        schema = make_schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])

        # Create batches with distinct values to verify data integrity
        batches = [
            pa.RecordBatch.from_pydict({"id": [1, 2, 3], "value": ["a", "b", "c"]}, schema=schema),
            pa.RecordBatch.from_pydict({"id": [4, 5, 6], "value": ["d", "e", "f"]}, schema=schema),
            pa.RecordBatch.from_pydict({"id": [7, 8, 9], "value": ["g", "h", "i"]}, schema=schema),
        ]

        repeat_count = 2

        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="repeat_inputs",
                    arguments=Arguments(positional=(pa.scalar(repeat_count),)),
                    input=iter(batches),
                )
            )

        # Combine all outputs into a table
        table = pa.Table.from_batches(output_batches)
        result = table.to_pydict()

        # Each id should appear exactly repeat_count times
        for expected_id in range(1, 10):
            count = result["id"].count(expected_id)
            assert count == repeat_count, f"id {expected_id} appeared {count} times"

        # Each value should appear exactly repeat_count times
        for expected_value in ["a", "b", "c", "d", "e", "f", "g", "h", "i"]:
            count = result["value"].count(expected_value)
            assert count == repeat_count, f"value {expected_value} appeared {count} times"
