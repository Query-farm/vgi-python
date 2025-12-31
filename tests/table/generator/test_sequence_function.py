"""Tests for the SequenceFunction."""

import pyarrow as pa

from vgi.examples.table import SequenceFunction
from vgi.testing import assert_table_function_output, batch

from .conftest import RunnerWithMode


class TestSequenceFunctionInProcess:
    """In-process tests for the sequence function (using TableFunctionTestClient)."""

    def test_generates_sequence(self) -> None:
        """Sequence should generate integers from 0 to n-1."""
        assert_table_function_output(
            SequenceFunction,
            args=(5,),
            expected=[batch(n=[0, 1, 2, 3, 4])],
        )

    def test_metadata(self) -> None:
        """Sequence function should have correct metadata."""
        meta = SequenceFunction.get_metadata()
        assert meta.name == "sequence"
        assert meta.max_workers == 1
        assert "generator" in meta.categories

    def test_cardinality(self) -> None:
        """Cardinality should match requested count."""
        import structlog

        from vgi.function import Arguments, Invocation

        invocation = Invocation(
            function_name="sequence",
            arguments=Arguments(positional=(pa.scalar(100),)),
            in_out_function_input_schema=None,
            correlation_id="test",
            invocation_id=b"test",
        )
        func = SequenceFunction(
            invocation=invocation,
            logger=structlog.get_logger(),
        )
        cardinality = func.cardinality()
        assert cardinality is not None
        assert cardinality.estimate == 100
        assert cardinality.max == 100


class TestSequenceFunctionBothModes:
    """Tests that run both in-process and via Client subprocess."""

    def test_generates_sequence(self, run_table_function_mode: RunnerWithMode) -> None:
        """Sequence should generate integers from 0 to n-1."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(SequenceFunction, (5,))

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 5
        values = table.column("n").to_pylist()
        assert values == [0, 1, 2, 3, 4]

    def test_zero_count(self, run_table_function_mode: RunnerWithMode) -> None:
        """Sequence with count=0 should produce no output."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(SequenceFunction, (0,))
        assert len(outputs) == 0

    def test_large_sequence_batches(
        self, run_table_function_mode: RunnerWithMode
    ) -> None:
        """Large sequences should be split into batches."""
        runner, mode = run_table_function_mode
        # Generate 2500 numbers (should produce 3 batches of 1000, 1000, 500)
        outputs, logs = runner(SequenceFunction, (2500,))

        # Combine all batches
        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 2500

        # Check all values are present
        values = table.column("n").to_pylist()
        assert values == list(range(2500))

    def test_single_value(self, run_table_function_mode: RunnerWithMode) -> None:
        """Sequence with count=1 should produce single value."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(SequenceFunction, (1,))

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 1
        assert table.column("n").to_pylist() == [0]
