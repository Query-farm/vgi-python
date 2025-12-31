"""Tests for the RangeFunction."""

import pyarrow as pa

from vgi.examples.table import RangeFunction
from vgi.testing import assert_table_function_output, batch

from .conftest import RunnerWithMode


class TestRangeFunctionInProcess:
    """In-process tests for the range function (using TableFunctionTestClient)."""

    def test_basic_range(self) -> None:
        """Range should generate integers from start to end."""
        assert_table_function_output(
            RangeFunction,
            args=(0, 5),
            expected=[batch(value=[0, 1, 2, 3, 4])],
        )

    def test_range_with_step(self) -> None:
        """Range with step should skip values."""
        assert_table_function_output(
            RangeFunction,
            args=(0, 10, 2),
            expected=[batch(value=[0, 2, 4, 6, 8])],
        )

    def test_metadata(self) -> None:
        """Range function should have correct metadata."""
        meta = RangeFunction.get_metadata()
        assert meta.name == "range"
        assert meta.max_workers == 1


class TestRangeFunctionBothModes:
    """Tests that run both in-process and via Client subprocess."""

    def test_basic_range(self, run_table_function_mode: RunnerWithMode) -> None:
        """Range should generate integers from start to end."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(RangeFunction, (0, 5))

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 5
        values = table.column("value").to_pylist()
        assert values == [0, 1, 2, 3, 4]

    def test_range_with_step(self, run_table_function_mode: RunnerWithMode) -> None:
        """Range with step should skip values."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(RangeFunction, (0, 10, 2))

        table = pa.Table.from_batches(outputs)
        values = table.column("value").to_pylist()
        assert values == [0, 2, 4, 6, 8]

    def test_range_non_zero_start(
        self, run_table_function_mode: RunnerWithMode
    ) -> None:
        """Range with non-zero start."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(RangeFunction, (5, 10))

        table = pa.Table.from_batches(outputs)
        values = table.column("value").to_pylist()
        assert values == [5, 6, 7, 8, 9]

    def test_empty_range(self, run_table_function_mode: RunnerWithMode) -> None:
        """Range where end <= start should produce no output."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(RangeFunction, (10, 5))
        assert len(outputs) == 0

    def test_single_value_range(self, run_table_function_mode: RunnerWithMode) -> None:
        """Range of length 1."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(RangeFunction, (42, 43))

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 1
        assert table.column("value").to_pylist() == [42]

    def test_large_range_batches(
        self, run_table_function_mode: RunnerWithMode
    ) -> None:
        """Large ranges should be split into batches."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(RangeFunction, (0, 2500))

        # Combine all batches
        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 2500

        # Check all values are present
        values = table.column("value").to_pylist()
        assert values == list(range(2500))
