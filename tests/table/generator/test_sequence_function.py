"""Tests for the SequenceFunction."""

import pyarrow as pa
import pytest
import structlog

from tests.conftest import make_invocation
from vgi.arguments import Arguments
from vgi.client import Client
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
        invocation = make_invocation(
            function_name="sequence",
            arguments=Arguments(positional=(pa.scalar(100),)),
        )
        func = SequenceFunction(
            invocation=invocation,
            logger=structlog.get_logger(),
        )
        cardinality = func.cardinality
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

    def test_custom_batch_size(self, run_table_function_mode: RunnerWithMode) -> None:
        """Custom batch size should control output batch sizes."""
        runner, mode = run_table_function_mode
        # Generate 250 values with batch size of 100
        outputs, logs = runner(SequenceFunction, (250, 100))

        # Should produce 3 batches: 100, 100, 50
        assert len(outputs) == 3
        assert outputs[0].num_rows == 100
        assert outputs[1].num_rows == 100
        assert outputs[2].num_rows == 50

        table = pa.Table.from_batches(outputs)
        assert table.column("n").to_pylist() == list(range(250))

    def test_batch_size_larger_than_count(
        self, run_table_function_mode: RunnerWithMode
    ) -> None:
        """Batch size larger than count should produce single batch."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(SequenceFunction, (50, 1000))

        assert len(outputs) == 1
        assert outputs[0].num_rows == 50
        assert outputs[0].column("n").to_pylist() == list(range(50))


class TestSequenceFunctionClient:
    """Tests for SequenceFunction via Client (wire protocol)."""

    def test_cardinality_returned_in_bind_result(self) -> None:
        """Cardinality should be returned in bind_result via Client."""
        bind_results: list[pa.RecordBatch] = []

        def capture_bind_result(result: pa.RecordBatch) -> None:
            bind_results.append(result)

        with Client("vgi-example-worker") as client:
            list(
                client.table_function(
                    function_name="sequence",
                    arguments=Arguments(positional=(pa.scalar(100),)),
                    bind_result_callback=capture_bind_result,
                )
            )

        assert len(bind_results) == 1
        bind_result = bind_results[0]

        # Verify cardinality fields are present and correct
        assert "cardinality_estimated" in bind_result.schema.names
        assert "cardinality_max" in bind_result.schema.names
        assert bind_result.column("cardinality_estimated")[0].as_py() == 100
        assert bind_result.column("cardinality_max")[0].as_py() == 100
