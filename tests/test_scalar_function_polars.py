"""Tests for PolarsScalarFunction base class.

These tests verify the functionality of the PolarsScalarFunction base class
independently of the example functions.
"""

# ruff: noqa: E402
# mypy: ignore-errors

from __future__ import annotations

import pytest

# Skip all tests if polars is not installed
polars = pytest.importorskip("polars")
import polars as pl  # noqa: E402
import pyarrow as pa

from vgi import Arg, Arguments, schema
from vgi.scalar_function_polars import AnyPolars, PolarsScalarFunction
from vgi.testing import ScalarFunctionTestClient


def batch(**kwargs: list) -> pa.RecordBatch:
    """Create a RecordBatch from keyword arguments."""
    return pa.RecordBatch.from_pydict(kwargs)


class TestPolarsScalarFunctionBasic:
    """Basic functionality tests for PolarsScalarFunction."""

    def test_simple_expression(self) -> None:
        """Test basic Polars expression in compute_polars."""

        class DoubleColumn(PolarsScalarFunction):
            class Meta:
                output_type = pl.Int64

            column = Arg[str](0, doc="Column to double")

            def compute_polars(self, df: pl.DataFrame) -> pl.Series:
                return df[self.column] * 2

        input_batch = batch(x=[1, 2, 3, 4, 5])

        with ScalarFunctionTestClient(DoubleColumn) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar("x"),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0]["result"].to_pylist() == [2, 4, 6, 8, 10]

    def test_string_expression(self) -> None:
        """Test Polars string operations in compute_polars."""

        class ReverseString(PolarsScalarFunction):
            class Meta:
                output_type = pl.Utf8

            column = Arg[str](0, doc="Column to reverse")

            def compute_polars(self, df: pl.DataFrame) -> pl.Series:
                return df[self.column].str.reverse()

        input_batch = batch(text=["hello", "world", "abc"])

        with ScalarFunctionTestClient(ReverseString) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar("text"),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0]["result"].to_pylist() == ["olleh", "dlrow", "cba"]


class TestPolarsScalarFunctionRowCount:
    """Tests for row count preservation."""

    def test_preserves_row_count_empty_batch(self) -> None:
        """Empty batch should return empty output or no output.

        Note: Empty batches may produce either an empty output batch or no output
        at all, depending on the test client implementation. We verify that the
        total row count is 0.
        """

        class IdentityFunction(PolarsScalarFunction):
            class Meta:
                output_type = pl.Int64

            column = Arg[str](0, doc="Column")

            def compute_polars(self, df: pl.DataFrame) -> pl.Series:
                return df[self.column]

        s = schema(value=pa.int64())
        input_batch = pa.RecordBatch.from_pydict({"value": []}, schema=s)

        with ScalarFunctionTestClient(IdentityFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar("value"),)),
                )
            )

        # Total row count should be 0 (either from 1 empty batch or 0 batches)
        total_rows = sum(o.num_rows for o in outputs)
        assert total_rows == 0

    def test_preserves_row_count_single_row(self) -> None:
        """Single row batch should return single row output."""

        class AddOne(PolarsScalarFunction):
            class Meta:
                output_type = pl.Int64

            column = Arg[str](0, doc="Column")

            def compute_polars(self, df: pl.DataFrame) -> pl.Series:
                return df[self.column] + 1

        input_batch = batch(value=[42])

        with ScalarFunctionTestClient(AddOne) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar("value"),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].num_rows == 1
        assert outputs[0]["result"].to_pylist() == [43]

    def test_preserves_row_count_multiple_batches(self) -> None:
        """Multiple batches should preserve row counts."""

        class AddOne(PolarsScalarFunction):
            class Meta:
                output_type = pl.Int64

            column = Arg[str](0, doc="Column")

            def compute_polars(self, df: pl.DataFrame) -> pl.Series:
                return df[self.column] + 1

        batch1 = batch(value=[1, 2])
        batch2 = batch(value=[3, 4, 5])
        batch3 = batch(value=[6])

        with ScalarFunctionTestClient(AddOne) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([batch1, batch2, batch3]),
                    arguments=Arguments(positional=(pa.scalar("value"),)),
                )
            )

        assert len(outputs) == 3
        assert outputs[0].num_rows == 2
        assert outputs[1].num_rows == 3
        assert outputs[2].num_rows == 1


class TestPolarsScalarFunctionOutputType:
    """Tests for output type handling."""

    def test_static_output_type(self) -> None:
        """Static output type should match Meta.output_type."""

        class Float64Output(PolarsScalarFunction):
            class Meta:
                output_type = pl.Float64

            column = Arg[str](0, doc="Column")

            def compute_polars(self, df: pl.DataFrame) -> pl.Series:
                return df[self.column].cast(pl.Float64)

        input_batch = batch(x=[1, 2, 3])

        with ScalarFunctionTestClient(Float64Output) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar("x"),)),
                )
            )

        assert outputs[0].schema.field("result").type == pa.float64()

    def test_dynamic_output_type(self) -> None:
        """Dynamic output type using AnyPolars and polars_schema."""

        class PreserveType(PolarsScalarFunction):
            class Meta:
                output_type = AnyPolars

            column = Arg[str](0, doc="Column")

            @property
            def output_polars_type(self) -> pl.DataType:
                return self.polars_schema[self.column]

            def compute_polars(self, df: pl.DataFrame) -> pl.Series:
                return df[self.column]

        # Test with int64 input
        input_batch = batch(value=[1, 2, 3])

        with ScalarFunctionTestClient(PreserveType) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar("value"),)),
                )
            )

        assert outputs[0].schema.field("result").type == pa.int64()


class TestPolarsScalarFunctionPolarsSchema:
    """Tests for polars_schema property."""

    def test_polars_schema_available(self) -> None:
        """polars_schema should be available during bind."""

        class UsePolarsSchema(PolarsScalarFunction):
            class Meta:
                output_type = AnyPolars

            column = Arg[str](0, doc="Column")
            _detected_type: pl.DataType

            def bind(self) -> None:
                # Store the detected type for verification
                self._detected_type = self.polars_schema[self.column]

            @property
            def output_polars_type(self) -> pl.DataType:
                return self._detected_type

            def compute_polars(self, df: pl.DataFrame) -> pl.Series:
                return df[self.column]

        input_batch = batch(value=[1.5, 2.5, 3.5])

        with ScalarFunctionTestClient(UsePolarsSchema) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar("value"),)),
                )
            )

        # Should have detected float64 type
        assert outputs[0].schema.field("result").type == pa.float64()


class TestPolarsScalarFunctionComplexOperations:
    """Tests for complex Polars operations."""

    def test_conditional_expression(self) -> None:
        """Test Polars when/then/otherwise in compute_polars."""

        class ConditionalSign(PolarsScalarFunction):
            class Meta:
                output_type = pl.Int64

            column = Arg[str](0, doc="Column")

            def compute_polars(self, df: pl.DataFrame) -> pl.Series:
                col = pl.col(self.column)
                expr = pl.when(col > 0).then(1).when(col < 0).then(-1).otherwise(0)
                return df.select(expr.alias("result"))["result"]

        input_batch = batch(value=[-5, 0, 5, -1, 1])

        with ScalarFunctionTestClient(ConditionalSign) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar("value"),)),
                )
            )

        assert outputs[0]["result"].to_pylist() == [-1, 0, 1, -1, 1]

    def test_aggregate_within_row(self) -> None:
        """Test per-row operations that reference multiple columns."""

        class RowSum(PolarsScalarFunction):
            class Meta:
                output_type = pl.Int64

            col1 = Arg[str](0, doc="First column")
            col2 = Arg[str](1, doc="Second column")

            def compute_polars(self, df: pl.DataFrame) -> pl.Series:
                return df[self.col1] + df[self.col2]

        input_batch = batch(a=[1, 2, 3], b=[10, 20, 30])

        with ScalarFunctionTestClient(RowSum) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar("a"), pa.scalar("b"))),
                )
            )

        assert outputs[0]["result"].to_pylist() == [11, 22, 33]
