"""Tests for the ConstantTableFunction."""

from vgi.examples.table import ConstantTableFunction
from vgi.testing import assert_table_function_output, batch


class TestConstantTableFunction:
    """Tests for the constant_table function."""

    def test_returns_constant_value(self) -> None:
        """Constant table should return a single row with the given value."""
        assert_table_function_output(
            ConstantTableFunction,
            args=(42,),
            expected=[batch(value=[42])],
        )

    def test_negative_value(self) -> None:
        """Constant table should handle negative values."""
        assert_table_function_output(
            ConstantTableFunction,
            args=(-100,),
            expected=[batch(value=[-100])],
        )

    def test_zero_value(self) -> None:
        """Constant table should handle zero."""
        assert_table_function_output(
            ConstantTableFunction,
            args=(0,),
            expected=[batch(value=[0])],
        )

    def test_metadata(self) -> None:
        """Constant table function should have correct metadata."""
        meta = ConstantTableFunction.get_metadata()
        assert meta.name == "constant_table"
        assert meta.max_workers == 1

    def test_cardinality(self) -> None:
        """Cardinality should always be 1."""
        import pyarrow as pa
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
