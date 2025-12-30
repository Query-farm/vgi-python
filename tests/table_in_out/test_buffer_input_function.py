"""Tests for the BufferInputFunction (collect then emit)."""

import pyarrow as pa

from vgi.client import Client


class TestBufferInputFunction:
    """Tests for the buffer_input function (collect then emit)."""

    def test_buffer_emits_on_finalize(
        self, example_worker: str, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Buffer should emit all batches during finalization."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="buffer_input",
                    input=iter(simple_batches),
                )
            )

        # During data phase, buffer returns empty batches
        # During finalize, it returns all buffered batches
        # So we expect: empty, empty, batch1, batch2
        total_input_rows = sum(b.num_rows for b in simple_batches)
        total_output_rows = sum(b.num_rows for b in output_batches)
        assert total_output_rows == total_input_rows

    def test_buffer_preserves_order(
        self, example_worker: str, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Buffer should emit batches in the order they were received."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="buffer_input",
                    input=iter(simple_batches),
                )
            )

        # Filter to non-empty batches (the actual buffered data)
        non_empty = [b for b in output_batches if b.num_rows > 0]
        assert len(non_empty) == len(simple_batches)

        for input_batch, output_batch in zip(simple_batches, non_empty, strict=True):
            assert output_batch.to_pydict() == input_batch.to_pydict()
