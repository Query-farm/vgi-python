"""Tests for PolarsScalarFunction base class.

These tests verify the functionality of the PolarsScalarFunction base class
using the expression-based API where compute_polars() returns a pl.Expr.
"""

# ruff: noqa: E402
# mypy: ignore-errors

from __future__ import annotations

from typing import Annotated, Any

import pytest

# Skip all tests if polars is not installed
polars = pytest.importorskip("polars")
import polars as pl  # noqa: E402
import pyarrow as pa
import pyarrow.types as pat

from vgi import Arguments, schema
from vgi.arguments import Param
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
            value: Annotated[pl.Int64, Param(position=0, doc="Column to double")]

            class Meta:
                output_type = pl.Int64

            def compute_polars(self) -> pl.Expr:
                return pl.col("value") * 2

        input_batch = batch(x=[1, 2, 3, 4, 5])

        with ScalarFunctionTestClient(DoubleColumn) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(),
                )
            )

        assert len(outputs) == 1
        assert outputs[0]["result"].to_pylist() == [2, 4, 6, 8, 10]

    def test_string_expression(self) -> None:
        """Test Polars string operations in compute_polars."""

        class ReverseString(PolarsScalarFunction):
            text: Annotated[pl.Utf8, Param(position=0, doc="Column to reverse")]

            class Meta:
                output_type = pl.Utf8

            def compute_polars(self) -> pl.Expr:
                return pl.col("text").str.reverse()

        input_batch = batch(text=["hello", "world", "abc"])

        with ScalarFunctionTestClient(ReverseString) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(),
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
            value: Annotated[pl.Int64, Param(position=0, doc="Column")]

            class Meta:
                output_type = pl.Int64

            def compute_polars(self) -> pl.Expr:
                return pl.col("value")

        s = schema(value=pa.int64())
        input_batch = pa.RecordBatch.from_pydict({"value": []}, schema=s)

        with ScalarFunctionTestClient(IdentityFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(),
                )
            )

        # Total row count should be 0 (either from 1 empty batch or 0 batches)
        total_rows = sum(o.num_rows for o in outputs)
        assert total_rows == 0

    def test_preserves_row_count_single_row(self) -> None:
        """Single row batch should return single row output."""

        class AddOne(PolarsScalarFunction):
            value: Annotated[pl.Int64, Param(position=0, doc="Column")]

            class Meta:
                output_type = pl.Int64

            def compute_polars(self) -> pl.Expr:
                return pl.col("value") + 1

        input_batch = batch(value=[42])

        with ScalarFunctionTestClient(AddOne) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].num_rows == 1
        assert outputs[0]["result"].to_pylist() == [43]

    def test_preserves_row_count_multiple_batches(self) -> None:
        """Multiple batches should preserve row counts."""

        class AddOne(PolarsScalarFunction):
            value: Annotated[pl.Int64, Param(position=0, doc="Column")]

            class Meta:
                output_type = pl.Int64

            def compute_polars(self) -> pl.Expr:
                return pl.col("value") + 1

        batch1 = batch(value=[1, 2])
        batch2 = batch(value=[3, 4, 5])
        batch3 = batch(value=[6])

        with ScalarFunctionTestClient(AddOne) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([batch1, batch2, batch3]),
                    arguments=Arguments(),
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
            value: Annotated[pl.Int64, Param(position=0, doc="Column")]

            class Meta:
                output_type = pl.Float64

            def compute_polars(self) -> pl.Expr:
                return pl.col("value").cast(pl.Float64)

        input_batch = batch(x=[1, 2, 3])

        with ScalarFunctionTestClient(Float64Output) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(),
                )
            )

        assert outputs[0].schema.field("result").type == pa.float64()

    def test_dynamic_output_type(self) -> None:
        """Dynamic output type using AnyPolars and polars_schema."""

        class PreserveType(PolarsScalarFunction):
            value: Annotated[
                Any,
                Param(
                    position=0,
                    doc="Column",
                    type_bound=[pat.is_integer, pat.is_floating],
                ),
            ]

            class Meta:
                output_type = AnyPolars

            @property
            def output_polars_type(self) -> pl.DataType:
                return self.polars_schema[self.input_schema.field(0).name]

            def compute_polars(self) -> pl.Expr:
                return pl.col("value")

        # Test with int64 input
        input_batch = batch(value=[1, 2, 3])

        with ScalarFunctionTestClient(PreserveType) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(),
                )
            )

        assert outputs[0].schema.field("result").type == pa.int64()


class TestPolarsScalarFunctionPolarsSchema:
    """Tests for polars_schema property."""

    def test_polars_schema_available(self) -> None:
        """polars_schema should be available during bind."""

        class UsePolarsSchema(PolarsScalarFunction):
            value: Annotated[
                Any,
                Param(
                    position=0,
                    doc="Column",
                    type_bound=[pat.is_integer, pat.is_floating],
                ),
            ]
            _detected_type: pl.DataType

            class Meta:
                output_type = AnyPolars

            def bind(self) -> None:
                super().bind()
                # Store the detected type for verification
                col_name = self.input_schema.field(0).name
                self._detected_type = self.polars_schema[col_name]

            @property
            def output_polars_type(self) -> pl.DataType:
                return self._detected_type

            def compute_polars(self) -> pl.Expr:
                return pl.col("value")

        input_batch = batch(value=[1.5, 2.5, 3.5])

        with ScalarFunctionTestClient(UsePolarsSchema) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(),
                )
            )

        # Should have detected float64 type
        assert outputs[0].schema.field("result").type == pa.float64()


class TestPolarsScalarFunctionComplexOperations:
    """Tests for complex Polars operations."""

    def test_conditional_expression(self) -> None:
        """Test Polars when/then/otherwise in compute_polars."""

        class ConditionalSign(PolarsScalarFunction):
            value: Annotated[pl.Int64, Param(position=0, doc="Column")]

            class Meta:
                output_type = pl.Int64

            def compute_polars(self) -> pl.Expr:
                col = pl.col("value")
                return pl.when(col > 0).then(1).when(col < 0).then(-1).otherwise(0)

        input_batch = batch(value=[-5, 0, 5, -1, 1])

        with ScalarFunctionTestClient(ConditionalSign) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(),
                )
            )

        assert outputs[0]["result"].to_pylist() == [-1, 0, 1, -1, 1]

    def test_aggregate_within_row(self) -> None:
        """Test per-row operations that reference multiple columns."""

        class RowSum(PolarsScalarFunction):
            col1: Annotated[pl.Int64, Param(position=0, doc="First column")]
            col2: Annotated[pl.Int64, Param(position=1, doc="Second column")]

            class Meta:
                output_type = pl.Int64

            def compute_polars(self) -> pl.Expr:
                return pl.col("col1") + pl.col("col2")

        input_batch = batch(a=[1, 2, 3], b=[10, 20, 30])

        with ScalarFunctionTestClient(RowSum) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(),
                )
            )

        assert outputs[0]["result"].to_pylist() == [11, 22, 33]
