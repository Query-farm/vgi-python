"""Tests for the EchoFunction (passthrough)."""

import pyarrow as pa

from vgi.client import Client


class TestEchoFunction:
    """Tests for the echo function (passthrough)."""

    def test_echo_preserves_data(
        self, example_worker: str, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Echo should return the same data it receives."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="echo",
                    input=iter(simple_batches),
                )
            )

        # Should have same number of data batches plus finalize batch
        assert len(output_batches) == len(simple_batches) + 1

        # Data batches should match input
        for _i, (input_batch, output_batch) in enumerate(
            zip(simple_batches, output_batches[:-1], strict=False)
        ):
            assert output_batch.schema == input_batch.schema
            assert output_batch.num_rows == input_batch.num_rows
            assert output_batch.to_pydict() == input_batch.to_pydict()

        # Finalize batch should be empty
        assert output_batches[-1].num_rows == 0

    def test_echo_preserves_schema(
        self, example_worker: str, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Echo should preserve the input schema exactly."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="echo",
                    input=iter(simple_batches),
                )
            )

        assert output_batches[0].schema == simple_batches[0].schema
