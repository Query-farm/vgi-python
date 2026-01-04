"""Tests for the RangeFunction."""

import pyarrow as pa
import pytest

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

    @pytest.mark.parametrize(
        "args,expected",
        [
            ((0, 5), [0, 1, 2, 3, 4]),
            ((0, 10, 2), [0, 2, 4, 6, 8]),
            ((5, 10), [5, 6, 7, 8, 9]),
            ((42, 43), [42]),
        ],
    )
    def test_range_values(
        self,
        run_table_function_mode: RunnerWithMode,
        args: tuple[int, ...],
        expected: list[int],
    ) -> None:
        """Range should generate expected integer values."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(RangeFunction, args)

        table = pa.Table.from_batches(outputs)
        assert table.column("value").to_pylist() == expected

    def test_empty_range(self, run_table_function_mode: RunnerWithMode) -> None:
        """Range where end <= start should produce no output."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(RangeFunction, (10, 5))
        assert len(outputs) == 0

    def test_large_range_batches(self, run_table_function_mode: RunnerWithMode) -> None:
        """Large ranges should be split into batches."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(RangeFunction, (0, 2500))

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 2500
        assert table.column("value").to_pylist() == list(range(2500))
