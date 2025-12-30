"""Tests for the RepeatInputsFunction (explosion)."""

import pyarrow as pa

from vgi.client import Client
from vgi.function import Arguments


class TestRepeatInputsFunction:
    """Tests for the repeat_inputs function (explosion)."""

    def test_repeat_custom_count(
        self, example_worker: str, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Should respect custom repeat count argument."""
        repeat_count = 3
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="repeat_inputs",
                    arguments=Arguments(positional=tuple([repeat_count]), named={}),
                    input=iter(simple_batches),
                )
            )

        total_input_rows = sum(b.num_rows for b in simple_batches)
        total_output_rows = sum(b.num_rows for b in output_batches)
        assert total_output_rows == total_input_rows * repeat_count

    def test_repeat_single_time(
        self, example_worker: str, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Repeat count of 1 should act like echo."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="repeat_inputs",
                    arguments=Arguments(positional=tuple([1]), named={}),
                    input=iter(simple_batches),
                )
            )

        total_input_rows = sum(b.num_rows for b in simple_batches)
        total_output_rows = sum(b.num_rows for b in output_batches)
        assert total_output_rows == total_input_rows
