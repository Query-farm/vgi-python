# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for the BufferInputFunction (collect then emit)."""

import pyarrow as pa

from tests.conftest import filter_non_empty, total_rows
from vgi.client import Client


class TestBufferInputFunction:
    """Tests for the buffer_input function (collect then emit)."""

    def test_buffer_emits_on_finalize(self, fixture_worker: str, simple_batches: list[pa.RecordBatch]) -> None:
        """Buffer should emit all batches during finalization."""
        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="buffer_input",
                    input=iter(simple_batches),
                )
            )

        # During data phase, buffer returns empty batches
        # During finalize, it returns all buffered batches
        assert total_rows(output_batches) == total_rows(simple_batches)

    def test_buffer_preserves_order(self, fixture_worker: str, simple_batches: list[pa.RecordBatch]) -> None:
        """Buffer should emit batches in the order they were received."""
        with Client(fixture_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="buffer_input",
                    input=iter(simple_batches),
                )
            )

        non_empty = filter_non_empty(output_batches)
        assert len(non_empty) == len(simple_batches)

        for input_batch, output_batch in zip(simple_batches, non_empty, strict=True):
            assert output_batch.to_pydict() == input_batch.to_pydict()
