"""Tests for the ConstantTableFunction."""

import pyarrow as pa
import pytest
import structlog

from tests.conftest import make_invocation
from vgi.arguments import Arguments
from vgi.examples.table import ConstantTableFunction
from vgi.testing import assert_table_function_output, batch

from .conftest import RunnerWithMode


class TestConstantTableFunctionInProcess:
    """In-process tests for the constant_table function."""

    def test_returns_constant_value(self) -> None:
        """Constant table should return rows with the given value."""
        assert_table_function_output(
            ConstantTableFunction,
            args=(3, 42),  # count=3, value=42
            expected=[batch(value=[42, 42, 42])],
        )

    def test_returns_single_row(self) -> None:
        """Constant table with count=1 should return one row."""
        assert_table_function_output(
            ConstantTableFunction,
            args=(1, 100),
            expected=[batch(value=[100])],
        )

    def test_metadata(self) -> None:
        """Constant table function should have correct metadata."""
        meta = ConstantTableFunction.get_metadata()
        assert meta.name == "constant_table"
        assert meta.max_workers == 1

    def test_cardinality(self) -> None:
        """Cardinality should equal the count parameter."""
        invocation = make_invocation(
            function_name="constant_table",
            arguments=Arguments(positional=(pa.scalar(5), pa.scalar(42))),
        )
        func = ConstantTableFunction(
            invocation=invocation,
            logger=structlog.get_logger(),
        )
        cardinality = func.cardinality
        assert cardinality is not None
        assert cardinality.estimate == 5
        assert cardinality.max == 5


class TestConstantTableFunctionBothModes:
    """Tests that run both in-process and via Client subprocess."""

    @pytest.mark.parametrize("value", [42, -100, 0])
    def test_returns_value(
        self, run_table_function_mode: RunnerWithMode, value: int
    ) -> None:
        """Constant table should return rows with the given value."""
        runner, mode = run_table_function_mode
        count = 3
        outputs, logs = runner(ConstantTableFunction, (count, value))

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == count
        assert table.column("value").to_pylist() == [value] * count

    def test_large_count(self, run_table_function_mode: RunnerWithMode) -> None:
        """Large counts should work correctly with batching."""
        runner, mode = run_table_function_mode
        count = 2500  # Larger than default BATCH_SIZE of 1000
        value = 7
        outputs, logs = runner(ConstantTableFunction, (count, value))

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == count
        assert all(v == value for v in table.column("value").to_pylist())
