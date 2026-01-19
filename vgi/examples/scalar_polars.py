"""Example Polars scalar function implementations.

This module provides example scalar functions that use Polars for data
processing, demonstrating the PolarsScalarFunction base class which
handles zero-copy Arrow-Polars conversion automatically.

POLARS SCALAR FUNCTIONS
-----------------------
PolarsScalarFunction handles the Arrow <-> Polars conversion automatically.
The compute_polars() method receives a DataFrame containing the input values
(columns accessed by position, not name):

    class MyFunction(PolarsScalarFunction):
        class Meta:
            output_type = pl.Utf8  # Polars type, not Arrow

        def compute_polars(self, df: pl.DataFrame) -> pl.Series:
            # Access first input value by position
            return df.get_columns()[0].str.to_uppercase()

STATIC OUTPUT TYPE
------------------
PolarsUpperCaseFunction     - Converts string values to uppercase
PolarsStringLengthFunction  - Computes string lengths
PolarsNormalizeFunction     - Z-score normalization

MULTIPLE ARGUMENTS
------------------
PolarsAddValuesFunction     - Adds two numeric values together
PolarsMultiplyFunction      - Multiplies a value by a constant factor

VARARGS
-------
PolarsSumValuesFunction     - Sums multiple numeric values

DYNAMIC OUTPUT TYPE (AnyPolars)
-------------------------------
PolarsDoubleFunction        - Doubles numeric values, preserving input type
"""

from __future__ import annotations

from typing import Annotated

import polars as pl

from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.scalar_function_polars import AnyPolars, PolarsScalarFunction

__all__ = [
    "PolarsAddValuesFunction",
    "PolarsDoubleFunction",
    "PolarsMultiplyFunction",
    "PolarsNormalizeFunction",
    "PolarsStringLengthFunction",
    "PolarsSumValuesFunction",
    "PolarsUpperCaseFunction",
]


class PolarsUpperCaseFunction(PolarsScalarFunction):
    """Converts string values to uppercase using Polars.

    Demonstrates PolarsScalarFunction which handles zero-copy Arrow-to-Polars
    conversion automatically. The input values are accessed by position in
    the DataFrame.

    Example:
        SQL:    SELECT polars_upper_case(name) FROM users
        Input:  ["alice", "bob", "charlie"]
        Output: ["ALICE", "BOB", "CHARLIE"]

    """

    class Meta:
        """Function metadata."""

        name = "polars_upper_case"
        description = "Converts string values to uppercase using Polars"
        output_type = pl.Utf8
        examples = [
            FunctionExample(
                sql="SELECT polars_upper_case(name) FROM users",
                description="Convert user names to uppercase using Polars",
            ),
        ]

    def compute_polars(self, df: pl.DataFrame) -> pl.Series:
        """Convert the string values to uppercase using Polars."""
        return df.get_columns()[0].str.to_uppercase()


class PolarsStringLengthFunction(PolarsScalarFunction):
    """Computes string lengths using Polars.

    Demonstrates using Polars string length computation with
    PolarsScalarFunction for automatic Arrow integration.

    Example:
        SQL:    SELECT polars_string_length(text) FROM documents
        Input:  ["hello", "hi", "goodbye"]
        Output: [5, 2, 7]

    """

    class Meta:
        """Function metadata."""

        name = "polars_string_length"
        description = "Computes string lengths using Polars"
        output_type = pl.UInt32
        examples = [
            FunctionExample(
                sql="SELECT polars_string_length(description) FROM products",
                description="Get length of product descriptions",
            ),
        ]

    def compute_polars(self, df: pl.DataFrame) -> pl.Series:
        """Compute string lengths using Polars."""
        return df.get_columns()[0].str.len_chars()


class PolarsNormalizeFunction(PolarsScalarFunction):
    """Z-score normalization using Polars.

    Computes (value - mean) / std for numeric values, demonstrating
    Polars aggregation and arithmetic operations with PolarsScalarFunction.

    Example:
        SQL:    SELECT polars_normalize(score) FROM exam_results
        Input:  [10, 20, 30, 40, 50]
        Output: [-1.41, -0.71, 0.0, 0.71, 1.41] (approximately)

    """

    class Meta:
        """Function metadata."""

        name = "polars_normalize"
        description = "Z-score normalization using Polars"
        output_type = pl.Float64
        examples = [
            FunctionExample(
                sql="SELECT polars_normalize(score) FROM exam_results",
                description="Normalize exam scores to z-scores",
            ),
        ]

    def compute_polars(self, df: pl.DataFrame) -> pl.Series:
        """Normalize the values using z-score (value - mean) / std."""
        col = df.get_columns()[0]
        return (col - col.mean()) / col.std()


# =============================================================================
# Multiple Arguments Examples
# =============================================================================


class PolarsAddValuesFunction(PolarsScalarFunction):
    """Adds two numeric values together using Polars.

    Demonstrates multiple positional arguments with PolarsScalarFunction.
    Input values are accessed by position in the DataFrame.

    Example:
        SQL:    SELECT polars_add_values(price, tax) FROM orders
        Input:  price=[10, 20, 30], tax=[1, 2, 3]
        Output: [11, 22, 33]

    """

    class Meta:
        """Function metadata."""

        name = "polars_add_values"
        description = "Adds two numeric values together using Polars"
        output_type = pl.Float64
        examples = [
            FunctionExample(
                sql="SELECT polars_add_values(price, tax) FROM orders",
                description="Calculate total by adding price and tax",
            ),
        ]

    def compute_polars(self, df: pl.DataFrame) -> pl.Series:
        """Add the two values together."""
        cols = df.get_columns()
        return cols[0] + cols[1]


class PolarsMultiplyFunction(PolarsScalarFunction):
    """Multiplies a numeric value by a constant factor using Polars.

    Demonstrates mixing a value with a constant argument. The first
    argument is the numeric value, the second is the constant factor.

    Example:
        SQL:    SELECT polars_multiply(price, 2) FROM products
        Input:  price=[10, 20, 30], factor=2
        Output: [20, 40, 60]

    """

    class Meta:
        """Function metadata."""

        name = "polars_multiply"
        description = "Multiplies a numeric value by a constant factor"
        output_type = pl.Float64
        examples = [
            FunctionExample(
                sql="SELECT polars_multiply(price, 1.1) FROM products",
                description="Apply 10% markup to prices",
            ),
        ]

    factor: Annotated[float, Arg(1, doc="Multiplication factor")]

    def compute_polars(self, df: pl.DataFrame) -> pl.Series:
        """Multiply the values by the constant factor."""
        return df.get_columns()[0] * self.factor


# =============================================================================
# Varargs Example
# =============================================================================


class PolarsSumValuesFunction(PolarsScalarFunction):
    """Sums multiple numeric values using Polars.

    Demonstrates varargs with PolarsScalarFunction - accepts any number
    of numeric values and sums them row-wise. All input values are
    accessed by position in the DataFrame.

    Example:
        SQL:    SELECT polars_sum_values(a, b, c) FROM data
        Input:  a=[1, 2], b=[10, 20], c=[100, 200]
        Output: [111, 222]

    """

    class Meta:
        """Function metadata."""

        name = "polars_sum_values"
        description = "Sums multiple numeric values using Polars"
        output_type = pl.Float64
        examples = [
            FunctionExample(
                sql="SELECT polars_sum_values(price, tax, shipping) FROM orders",
                description="Calculate total from multiple cost values",
            ),
        ]

    def compute_polars(self, df: pl.DataFrame) -> pl.Series:
        """Sum all input values."""
        cols = df.get_columns()
        result = cols[0]
        for col in cols[1:]:
            result = result + col
        return result


# =============================================================================
# Dynamic Output Type (AnyPolars) Example
# =============================================================================


class PolarsDoubleFunction(PolarsScalarFunction):
    """Doubles numeric values, preserving the input type.

    Demonstrates AnyPolars for dynamic output type - the output type
    matches the input type (int64 in -> int64 out, float64 in -> float64 out).

    Example:
        SQL:    SELECT polars_double(count) FROM inventory
        Input:  [1, 2, 3] (Int64)
        Output: [2, 4, 6] (Int64, same type preserved)

    """

    class Meta:
        """Function metadata."""

        name = "polars_double"
        description = "Doubles numeric values, preserving input type"
        output_type = AnyPolars
        examples = [
            FunctionExample(
                sql="SELECT polars_double(quantity) FROM inventory",
                description="Double inventory quantities",
            ),
        ]

    @property
    def output_polars_type(self) -> pl.DataType:
        """Return the input value's type as the output type."""
        # Get the type of the first input column
        return self.polars_schema[self.input_schema.field(0).name]

    def compute_polars(self, df: pl.DataFrame) -> pl.Series:
        """Double the values, preserving the input type."""
        return df.get_columns()[0] * 2
