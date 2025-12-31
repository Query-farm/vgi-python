"""Tests for the ConstantTableFunction."""

import pyarrow as pa

from vgi.examples.table import ConstantTableFunction
from vgi.testing import assert_table_function_output, batch

from .conftest import RunnerWithMode


class TestConstantTableFunctionInProcess:
    """In-process tests for the constant_table function."""

    def test_returns_constant_value(self) -> None:
        """Constant table should return a single row with the given value."""
        assert_table_function_output(
            ConstantTableFunction,
            args=(42,),
            expected=[batch(value=[42])],
        )

    def test_metadata(self) -> None:
        """Constant table function should have correct metadata."""
        meta = ConstantTableFunction.get_metadata()
        assert meta.name == "constant_table"
        assert meta.max_workers == 1

    def test_cardinality(self) -> None:
        """Cardinality should always be 1."""
        import structlog

        from vgi.function import Arguments, Invocation

        invocation = Invocation(
            function_name="constant_table",
            arguments=Arguments(positional=(pa.scalar(42),)),
            in_out_function_input_schema=None,
            correlation_id="test",
            invocation_id=b"test",
        )
        func = ConstantTableFunction(
            invocation=invocation,
            logger=structlog.get_logger(),
        )
        cardinality = func.cardinality()
        assert cardinality is not None
        assert cardinality.estimate == 1
        assert cardinality.max == 1


class TestConstantTableFunctionBothModes:
    """Tests that run both in-process and via Client subprocess."""

    def test_returns_constant_value(
        self, run_table_function_mode: RunnerWithMode
    ) -> None:
        """Constant table should return a single row with the given value."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(ConstantTableFunction, (42,))

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 1
        assert table.column("value").to_pylist() == [42]

    def test_negative_value(self, run_table_function_mode: RunnerWithMode) -> None:
        """Constant table should handle negative values."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(ConstantTableFunction, (-100,))

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 1
        assert table.column("value").to_pylist() == [-100]

    def test_zero_value(self, run_table_function_mode: RunnerWithMode) -> None:
        """Constant table should handle zero."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(ConstantTableFunction, (0,))

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 1
        assert table.column("value").to_pylist() == [0]
