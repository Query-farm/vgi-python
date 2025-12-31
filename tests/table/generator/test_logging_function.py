"""Tests for the LoggingGeneratorFunction."""

import pyarrow as pa

from vgi.examples.table import LoggingGeneratorFunction
from vgi.log import Level
from vgi.testing import run_table_function


class TestLoggingGeneratorFunction:
    """Tests for the logging_generator function."""

    def test_emits_start_and_end_logs(self) -> None:
        """Function should emit start and end log messages."""
        outputs, logs = run_table_function(LoggingGeneratorFunction, args=(5,))

        assert len(logs) == 2
        assert logs[0].level == Level.INFO
        assert "Starting generation of 5 values" in logs[0].message
        assert logs[1].level == Level.INFO
        assert "Generation complete" in logs[1].message

    def test_generates_correct_output(self) -> None:
        """Function should generate correct output alongside logs."""
        outputs, logs = run_table_function(LoggingGeneratorFunction, args=(3,))

        # Combine outputs
        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 3

        values = table.column("n").to_pylist()
        assert values == [0, 1, 2]

    def test_zero_count_still_logs(self) -> None:
        """Function with count=0 should still emit start/end logs."""
        outputs, logs = run_table_function(LoggingGeneratorFunction, args=(0,))

        assert len(outputs) == 0
        assert len(logs) == 2
        assert "Starting generation of 0 values" in logs[0].message
        assert "Generation complete" in logs[1].message
