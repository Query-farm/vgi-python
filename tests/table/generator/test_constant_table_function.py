"""Tests for the ConstantTableFunction."""

import pyarrow as pa
import pytest
import structlog

from tests.conftest import make_invocation
from vgi.examples.table import ConstantTableFunction
from vgi.function import Arguments
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
        invocation = make_invocation(
            function_name="constant_table",
            arguments=Arguments(positional=(pa.scalar(42),)),
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

    @pytest.mark.parametrize("value", [42, -100, 0])
    def test_returns_value(
        self, run_table_function_mode: RunnerWithMode, value: int
    ) -> None:
        """Constant table should return a single row with the given value."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(ConstantTableFunction, (value,))

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 1
        assert table.column("value").to_pylist() == [value]
