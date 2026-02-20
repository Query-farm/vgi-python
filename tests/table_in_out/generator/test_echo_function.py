"""Tests for the EchoFunction (passthrough)."""

import pyarrow as pa

from tests.conftest import filter_non_empty
from vgi.client import Client


class TestEchoFunction:
    """Tests for the echo function (passthrough)."""

    def test_echo_preserves_data(self, example_worker: str, simple_batches: list[pa.RecordBatch]) -> None:
        """Echo should return the same data it receives.

        Note: With parallel processing, batch order may not be preserved,
        so we compare by combining all data and checking totals match.
        """
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="echo",
                    input=iter(simple_batches),
                )
            )

        # Combine input and output (excluding empty finalize batch) into tables
        input_table = pa.Table.from_batches(simple_batches)
        output_table = pa.Table.from_batches(filter_non_empty(output_batches))

        # Total rows should match
        assert output_table.num_rows == input_table.num_rows

        # Schemas should match
        assert output_table.schema == input_table.schema

        # All data should be present (compare sorted by id column)
        input_sorted = input_table.sort_by("id").to_pydict()
        output_sorted = output_table.sort_by("id").to_pydict()
        assert output_sorted == input_sorted

    def test_echo_preserves_schema(self, example_worker: str, simple_batches: list[pa.RecordBatch]) -> None:
        """Echo should preserve the input schema exactly."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="echo",
                    input=iter(simple_batches),
                )
            )

        assert filter_non_empty(output_batches)[0].schema == simple_batches[0].schema
