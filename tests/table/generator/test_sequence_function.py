"""Tests for the SequenceFunction."""

import pyarrow as pa
import pytest
import structlog

from tests.conftest import make_invocation
from vgi.examples.table import SequenceFunction
from vgi.function import Arguments
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
        invocation = make_invocation(
            function_name="sequence",
            arguments=Arguments(positional=(pa.scalar(100),)),
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

    @pytest.mark.parametrize("count,expected", [(5, [0, 1, 2, 3, 4]), (1, [0])])
    def test_generates_sequence(
        self,
        run_table_function_mode: RunnerWithMode,
        count: int,
        expected: list[int],
    ) -> None:
        """Sequence should generate integers from 0 to n-1."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(SequenceFunction, (count,))

        table = pa.Table.from_batches(outputs)
        assert table.column("n").to_pylist() == expected

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
        outputs, logs = runner(SequenceFunction, (2500,))

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 2500
        assert table.column("n").to_pylist() == list(range(2500))
