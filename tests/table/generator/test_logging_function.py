"""Tests for the LoggingGeneratorFunction."""

import pyarrow as pa

from vgi.client import Client
from vgi.examples.table import LoggingGeneratorFunction
from vgi.function import Arguments
from vgi.log import Level
from vgi.testing import TableFunctionTestClient


class TestLoggingGeneratorFunctionInProcess:
    """In-process tests for the logging_generator function (captures logs)."""

    def test_emits_start_and_end_logs(self) -> None:
        """Function should emit start and end log messages."""
        with TableFunctionTestClient(LoggingGeneratorFunction) as client:
            list(client.table_function(arguments=Arguments(positional=(pa.scalar(5),))))
            logs = client.logs

        assert len(logs) == 2
        assert logs[0].level == Level.INFO
        assert "Starting generation of 5 values" in logs[0].message
        assert logs[1].level == Level.INFO
        assert "Generation complete" in logs[1].message

    def test_generates_correct_output(self) -> None:
        """Function should generate correct output alongside logs."""
        with TableFunctionTestClient(LoggingGeneratorFunction) as client:
            outputs = list(
                client.table_function(arguments=Arguments(positional=(pa.scalar(3),)))
            )

        # Combine outputs
        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 3

        values = table.column("n").to_pylist()
        assert values == [0, 1, 2]

    def test_zero_count_still_logs(self) -> None:
        """Function with count=0 should still emit start/end logs."""
        with TableFunctionTestClient(LoggingGeneratorFunction) as client:
            outputs = list(
                client.table_function(arguments=Arguments(positional=(pa.scalar(0),)))
            )
            logs = client.logs

        assert len(outputs) == 0
        assert len(logs) == 2
        assert "Starting generation of 0 values" in logs[0].message
        assert "Generation complete" in logs[1].message


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
