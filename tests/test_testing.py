"""Tests for vgi.testing.FunctionTestClient using example functions."""

from __future__ import annotations

import pyarrow as pa
import pytest

from tests.conftest import make_schema
from vgi.examples.table_in_out import (
    BufferInputFunction,
    EchoFunction,
    ExceptionFinalizeFunction,
    ExceptionProcessFunction,
    RepeatInputsFunction,
    SumAllColumnsFunction,
    SumAllColumnsFunctionWithLogging,
    SumAllColumnsSimpleDistributed,
)
from vgi.function import Arguments
from vgi.log import Level
from vgi.testing import (
    FunctionTestClient,
    FunctionTestClientError,
    assert_function_logs,
    assert_function_output,
    batch,
    run_function,
)


class TestEchoFunction:
    """Tests for EchoFunction - basic passthrough."""

    def test_single_batch(self) -> None:
        """Echo function should pass through a single batch unchanged."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"]})

        with FunctionTestClient(EchoFunction) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        assert len(outputs) == 1
        assert outputs[0].equals(batch)

    def test_multiple_batches(self) -> None:
        """Echo function should pass through multiple batches unchanged."""
        batch1 = pa.RecordBatch.from_pydict({"x": [1, 2]})
        batch2 = pa.RecordBatch.from_pydict({"x": [3, 4]})
        batch3 = pa.RecordBatch.from_pydict({"x": [5, 6]})

        with FunctionTestClient(EchoFunction) as client:
            outputs = list(
                client.table_in_out_function(input=iter([batch1, batch2, batch3]))
            )

        assert len(outputs) == 3
        assert outputs[0].equals(batch1)
        assert outputs[1].equals(batch2)
        assert outputs[2].equals(batch3)

    def test_empty_input(self) -> None:
        """Echo function with no input batches should produce no output."""
        with FunctionTestClient(EchoFunction) as client:
            outputs = list(client.table_in_out_function(input=iter([])))

        assert len(outputs) == 0


class TestRepeatInputsFunction:
    """Tests for RepeatInputsFunction - multiple outputs per input."""

    def test_repeat_twice(self) -> None:
        """RepeatInputsFunction should duplicate each batch N times."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})

        with FunctionTestClient(RepeatInputsFunction) as client:
            outputs = list(
                client.table_in_out_function(
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(2),)),
                )
            )

        assert len(outputs) == 2
        assert outputs[0].equals(batch)
        assert outputs[1].equals(batch)

    def test_repeat_three_times_multiple_batches(self) -> None:
        """RepeatInputsFunction should work with multiple input batches."""
        batch1 = pa.RecordBatch.from_pydict({"x": [1]})
        batch2 = pa.RecordBatch.from_pydict({"x": [2]})

        with FunctionTestClient(RepeatInputsFunction) as client:
            outputs = list(
                client.table_in_out_function(
                    input=iter([batch1, batch2]),
                    arguments=Arguments(positional=(pa.scalar(3),)),
                )
            )

        # 2 input batches * 3 repeats = 6 output batches
        assert len(outputs) == 6
        assert outputs[0].equals(batch1)
        assert outputs[1].equals(batch1)
        assert outputs[2].equals(batch1)
        assert outputs[3].equals(batch2)
        assert outputs[4].equals(batch2)
        assert outputs[5].equals(batch2)


class TestBufferInputFunction:
    """Tests for BufferInputFunction - buffering with finalize output."""

    def test_buffers_and_emits_on_finalize(self) -> None:
        """BufferInputFunction should collect all input and emit during finalize."""
        batch1 = pa.RecordBatch.from_pydict({"x": [1, 2]})
        batch2 = pa.RecordBatch.from_pydict({"x": [3, 4]})
        batch3 = pa.RecordBatch.from_pydict({"x": [5, 6]})

        with FunctionTestClient(BufferInputFunction) as client:
            outputs = list(
                client.table_in_out_function(input=iter([batch1, batch2, batch3]))
            )

        # All batches should be emitted during finalize in order
        assert len(outputs) == 3
        assert outputs[0].equals(batch1)
        assert outputs[1].equals(batch2)
        assert outputs[2].equals(batch3)


class TestSumAllColumnsFunction:
    """Tests for SumAllColumnsFunction - aggregation."""

    def test_sum_integer_columns(self) -> None:
        """SumAllColumnsFunction should sum integer columns."""
        batch1 = pa.RecordBatch.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]})
        batch2 = pa.RecordBatch.from_pydict({"a": [4, 5], "b": [40, 50]})

        with FunctionTestClient(SumAllColumnsFunction) as client:
            outputs = list(client.table_in_out_function(input=iter([batch1, batch2])))

        assert len(outputs) == 1
        result = outputs[0].to_pydict()
        assert result["a"] == [15]  # 1+2+3+4+5
        assert result["b"] == [150]  # 10+20+30+40+50

    def test_sum_float_columns(self) -> None:
        """SumAllColumnsFunction should sum float columns."""
        batch = pa.RecordBatch.from_pydict({"x": [1.5, 2.5, 3.0]})

        with FunctionTestClient(SumAllColumnsFunction) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        assert len(outputs) == 1
        result = outputs[0].to_pydict()
        assert result["x"] == [7.0]

    def test_excludes_non_numeric_columns(self) -> None:
        """SumAllColumnsFunction should exclude non-numeric columns from output."""
        batch = pa.RecordBatch.from_pydict(
            {
                "num": [1, 2, 3],
                "name": ["a", "b", "c"],
            }
        )

        with FunctionTestClient(SumAllColumnsFunction) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

        assert len(outputs) == 1
        result = outputs[0].to_pydict()
        assert "num" in result
        assert "name" not in result
        assert result["num"] == [6]


class TestSumAllColumnsFunctionWithLogging:
    """Tests for log message capture."""

    def test_captures_log_messages(self) -> None:
        """FunctionTestClient should capture log messages emitted by the function."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})

        with FunctionTestClient(SumAllColumnsFunctionWithLogging) as client:
            outputs = list(client.table_in_out_function(input=iter([batch])))

            # Check that logs were captured
            assert len(client.logs) >= 2
            assert any("Processing batch" in log.message for log in client.logs)
            assert any("Finalizing" in log.message for log in client.logs)

        # Verify output is still correct
        assert len(outputs) == 1
        result = outputs[0].to_pydict()
        assert result["x"] == [6]


class TestExceptionHandling:
    """Tests for exception handling."""

    def test_process_exception_raises_error(self) -> None:
        """Exception during process() should raise FunctionTestClientError."""
        batch1 = pa.RecordBatch.from_pydict({"x": [1]})
        batch2 = pa.RecordBatch.from_pydict({"x": [2]})

        with (
            FunctionTestClient(ExceptionProcessFunction) as client,
            pytest.raises(FunctionTestClientError, match="Intentional exception"),
        ):
            # Exception occurs on second batch
            list(client.table_in_out_function(input=iter([batch1, batch2])))

    def test_finalize_exception_raises_error(self) -> None:
        """Exception during finalize() should raise FunctionTestClientError."""
        batch = pa.RecordBatch.from_pydict({"x": [1]})

        with (
            FunctionTestClient(ExceptionFinalizeFunction) as client,
            pytest.raises(FunctionTestClientError, match="Intentional exception"),
        ):
            list(client.table_in_out_function(input=iter([batch])))


class TestProjectionIds:
    """Tests for projection_ids support."""

    def test_projection_ids_filters_output_columns(self) -> None:
        """projection_ids should filter output to specified columns."""
        batch = pa.RecordBatch.from_pydict(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
                "c": [7, 8, 9],
            }
        )

        with FunctionTestClient(SumAllColumnsFunction) as client:
            # Only project column 0 (a) and column 2 (c)
            outputs = list(
                client.table_in_out_function(
                    input=iter([batch]),
                    projection_ids=[0, 2],
                )
            )

        assert len(outputs) == 1
        result = outputs[0].to_pydict()
        # Only projected columns should be in output
        assert "a" in result
        assert "c" in result
        assert "b" not in result
        assert result["a"] == [6]  # 1+2+3
        assert result["c"] == [24]  # 7+8+9


class TestBindResultCallback:
    """Tests for bind_result_callback."""

    def test_bind_result_callback_invoked(self) -> None:
        """bind_result_callback should be called with bind result batch."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})
        bind_results: list[pa.RecordBatch] = []

        def callback(bind_batch: pa.RecordBatch) -> None:
            bind_results.append(bind_batch)

        with FunctionTestClient(EchoFunction) as client:
            list(
                client.table_in_out_function(
                    input=iter([batch]),
                    bind_result_callback=callback,
                )
            )

        assert len(bind_results) == 1
        bind_batch = bind_results[0]
        assert "output_schema" in bind_batch.schema.names
        assert "max_processes" in bind_batch.schema.names
        assert "invocation_id" in bind_batch.schema.names


class TestDistributedStateSupport:
    """Tests for distributed state support (save_state/load_states)."""

    def test_simple_distributed_function(self) -> None:
        """SumAllColumnsSimpleDistributed should work with FunctionTestClient.

        This function uses save_state() and load_states() internally,
        testing that the distributed state framework works in single-process mode.
        """
        batch1 = pa.RecordBatch.from_pydict({"a": [1, 2], "b": [10, 20]})
        batch2 = pa.RecordBatch.from_pydict({"a": [3, 4], "b": [30, 40]})

        with FunctionTestClient(SumAllColumnsSimpleDistributed) as client:
            outputs = list(client.table_in_out_function(input=iter([batch1, batch2])))

        assert len(outputs) == 1
        result = outputs[0].to_pydict()
        assert result["a"] == [10]  # 1+2+3+4
        assert result["b"] == [100]  # 10+20+30+40


class TestArgumentsPassing:
    """Tests for passing arguments to functions."""

    def test_positional_arguments(self) -> None:
        """Functions should receive positional arguments correctly."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})

        with FunctionTestClient(RepeatInputsFunction) as client:
            outputs = list(
                client.table_in_out_function(
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(4),)),
                )
            )

        # Repeat 4 times
        assert len(outputs) == 4

    def test_default_arguments(self) -> None:
        """Functions should use default Arguments when none provided."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})

        with FunctionTestClient(EchoFunction) as client:
            # No arguments parameter - should use empty Arguments()
            outputs = list(client.table_in_out_function(input=iter([batch])))

        assert len(outputs) == 1


class TestLogLevelCapture:
    """Tests for capturing log levels."""

    def test_captures_info_level(self) -> None:
        """Should capture INFO level logs."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})

        with FunctionTestClient(SumAllColumnsFunctionWithLogging) as client:
            list(client.table_in_out_function(input=iter([batch])))

            info_logs = [log for log in client.logs if log.level == Level.INFO]
            assert len(info_logs) >= 1


class TestLogsClearing:
    """Tests for logs being cleared between invocations."""

    def test_logs_cleared_between_calls(self) -> None:
        """Logs should be cleared at start of each table_in_out_function call."""
        test_batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})

        with FunctionTestClient(SumAllColumnsFunctionWithLogging) as client:
            # First call
            list(client.table_in_out_function(input=iter([test_batch])))
            first_call_log_count = len(client.logs)
            assert first_call_log_count > 0

            # Second call - logs should be reset
            list(client.table_in_out_function(input=iter([test_batch])))
            second_call_log_count = len(client.logs)

            # Log counts should be the same (logs were cleared)
            assert first_call_log_count == second_call_log_count


# =============================================================================
# Tests for Declarative Test Helpers
# =============================================================================


class TestBatchHelper:
    """Tests for the batch() helper function."""

    def test_batch_creates_record_batch(self) -> None:
        """batch() should create a RecordBatch from column data."""
        b = batch(x=[1, 2, 3], y=["a", "b", "c"])

        assert isinstance(b, pa.RecordBatch)
        assert b.num_rows == 3
        assert b.num_columns == 2
        assert b.column("x").to_pylist() == [1, 2, 3]
        assert b.column("y").to_pylist() == ["a", "b", "c"]

    def test_batch_with_explicit_schema(self) -> None:
        """batch() should respect explicit schema."""
        schema = make_schema([("x", pa.int64()), ("y", pa.string())])
        b = batch(schema, x=[1, 2, 3], y=["a", "b", "c"])

        assert b.schema == schema
        assert b.column("x").type == pa.int64()

    def test_batch_empty(self) -> None:
        """batch() should handle empty columns."""
        schema = make_schema([("x", pa.int64())])
        b = batch(schema, x=[])

        assert b.num_rows == 0
        assert b.num_columns == 1


class TestRunFunctionHelper:
    """Tests for the run_function() helper."""

    def test_run_function_basic(self) -> None:
        """run_function() should run a function and return outputs."""
        outputs, logs = run_function(
            EchoFunction,
            input_batches=[batch(x=[1, 2, 3])],
        )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"x": [1, 2, 3]}
        assert isinstance(logs, list)

    def test_run_function_with_args(self) -> None:
        """run_function() should pass positional arguments."""
        outputs, logs = run_function(
            RepeatInputsFunction,
            input_batches=[batch(x=[1])],
            args=(3,),
        )

        assert len(outputs) == 3

    def test_run_function_captures_logs(self) -> None:
        """run_function() should capture log messages."""
        outputs, logs = run_function(
            SumAllColumnsFunctionWithLogging,
            input_batches=[batch(x=[1, 2, 3])],
        )

        assert len(logs) >= 2
        assert any("Processing batch" in log.message for log in logs)


class TestAssertFunctionOutput:
    """Tests for assert_function_output()."""

    def test_assert_function_output_pass(self) -> None:
        """assert_function_output() should pass when output matches."""
        logs = assert_function_output(
            function=EchoFunction,
            input=[batch(x=[1, 2, 3])],
            expected=[batch(x=[1, 2, 3])],
        )
        assert isinstance(logs, list)

    def test_assert_function_output_fail_count(self) -> None:
        """assert_function_output() should fail when batch count differs."""
        with pytest.raises(AssertionError, match="Expected 2 output batches"):
            assert_function_output(
                function=EchoFunction,
                input=[batch(x=[1, 2, 3])],
                expected=[batch(x=[1]), batch(x=[2, 3])],
            )

    def test_assert_function_output_fail_content(self) -> None:
        """assert_function_output() should fail when content differs."""
        with pytest.raises(AssertionError, match="Batch 0 mismatch"):
            assert_function_output(
                function=EchoFunction,
                input=[batch(x=[1, 2, 3])],
                expected=[batch(x=[4, 5, 6])],
            )

    def test_assert_function_output_with_args(self) -> None:
        """assert_function_output() should handle arguments."""
        assert_function_output(
            function=RepeatInputsFunction,
            input=[batch(x=[1])],
            expected=[batch(x=[1]), batch(x=[1]), batch(x=[1])],
            args=(3,),
        )

    def test_assert_function_output_aggregation(self) -> None:
        """assert_function_output() should work with aggregation functions."""
        assert_function_output(
            function=SumAllColumnsFunction,
            input=[batch(a=[1, 2], b=[10, 20]), batch(a=[3, 4], b=[30, 40])],
            expected=[batch(a=[10], b=[100])],
        )

    def test_assert_function_output_custom_message(self) -> None:
        """assert_function_output() should include custom message in error."""
        with pytest.raises(AssertionError, match="Custom message:"):
            assert_function_output(
                function=EchoFunction,
                input=[batch(x=[1])],
                expected=[batch(x=[999])],
                msg="Custom message",
            )


class TestAssertFunctionLogs:
    """Tests for assert_function_logs()."""

    def test_assert_function_logs_pass(self) -> None:
        """assert_function_logs() should pass when logs match expectations."""
        outputs = assert_function_logs(
            function=SumAllColumnsFunctionWithLogging,
            input=[batch(x=[1, 2, 3])],
            expected_logs=[
                {"level": Level.INFO, "message_contains": "Processing batch"},
            ],
        )
        assert isinstance(outputs, list)

    def test_assert_function_logs_fail(self) -> None:
        """assert_function_logs() should fail when logs don't match."""
        with pytest.raises(AssertionError, match="Expected log pattern"):
            assert_function_logs(
                function=EchoFunction,  # EchoFunction doesn't log
                input=[batch(x=[1, 2, 3])],
                expected_logs=[
                    {"level": Level.INFO, "message_contains": "Processing"},
                ],
            )

    def test_assert_function_logs_level_check(self) -> None:
        """assert_function_logs() should check log level."""
        _ = assert_function_logs(
            function=SumAllColumnsFunctionWithLogging,
            input=[batch(x=[1, 2, 3])],
            expected_logs=[
                {"level": Level.INFO},
            ],
        )
