"""Tests for the LoggingGeneratorFunction."""

import pyarrow as pa

from vgi.arguments import Arguments
from vgi.client import Client


class TestLoggingGeneratorFunctionViaClient:
    """Tests that run via Client subprocess (logs go to stderr, not captured)."""

    def test_generates_correct_output(self) -> None:
        """Function should generate correct output via Client."""
        with Client("vgi-example-worker") as client:
            outputs = list(
                client.table_function(
                    function_name="logging_generator",
                    arguments=Arguments(positional=(pa.scalar(5),)),
                )
            )

        # Combine outputs
        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 5

        values = table.column("n").to_pylist()
        assert values == [0, 1, 2, 3, 4]

    def test_zero_count_produces_no_output(self) -> None:
        """Function with count=0 should produce no output via Client."""
        with Client("vgi-example-worker") as client:
            outputs = list(
                client.table_function(
                    function_name="logging_generator",
                    arguments=Arguments(positional=(pa.scalar(0),)),
                )
            )

        assert len(outputs) == 0

    def test_large_output(self) -> None:
        """Function should handle larger outputs via Client."""
        with Client("vgi-example-worker") as client:
            outputs = list(
                client.table_function(
                    function_name="logging_generator",
                    arguments=Arguments(positional=(pa.scalar(100),)),
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 100
        values = table.column("n").to_pylist()
        assert values == list(range(100))
