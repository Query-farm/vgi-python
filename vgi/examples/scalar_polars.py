"""Example Polars scalar function implementations.

This module provides example scalar functions that use Polars for data
processing, demonstrating the PolarsScalarFunction base class which
handles zero-copy Arrow-Polars conversion automatically.

EXPRESSION-BASED API
--------------------
PolarsScalarFunction uses an expression-based API where compute_polars()
returns a pl.Expr instead of a pl.Series. Columns are referenced by their
declared param names using pl.col("param_name"):

    class MyFunction(PolarsScalarFunction):
        text: Annotated[pl.Utf8, Param(position=0, doc="Input text")]

        class Meta:
            output_type = pl.Utf8

        def compute_polars(self) -> pl.Expr:
            return pl.col("text").str.to_uppercase()

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

DYNAMIC OUTPUT TYPE
-------------------
PolarsDoubleFunction        - Doubles numeric values, preserving input type
"""

from __future__ import annotations

from typing import Annotated, Any

import polars as pl
import pyarrow.types as pat

from vgi.arguments import Param
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

    Demonstrates the expression-based API where compute_polars() returns
    a pl.Expr and columns are referenced by their declared param names.

    Example:
        SQL:    SELECT polars_upper_case(name) FROM users
        Input:  ["alice", "bob", "charlie"]
        Output: ["ALICE", "BOB", "CHARLIE"]

    """

    text: Annotated[pl.Utf8, Param(position=0, doc="String value to uppercase")]

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

    def compute_polars(self) -> pl.Expr:
        """Convert the string values to uppercase using Polars."""
        return pl.col("text").str.to_uppercase()


class PolarsStringLengthFunction(PolarsScalarFunction):
    """Computes string lengths using Polars.

    Demonstrates using Polars string length computation with the
    expression-based API.

    Example:
        SQL:    SELECT polars_string_length(text) FROM documents
        Input:  ["hello", "hi", "goodbye"]
        Output: [5, 2, 7]

    """

    text: Annotated[pl.Utf8, Param(position=0, doc="String value")]

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

    def compute_polars(self) -> pl.Expr:
        """Compute string lengths using Polars."""
        return pl.col("text").str.len_chars()


class PolarsNormalizeFunction(PolarsScalarFunction):
    """Z-score normalization using Polars.

    Computes (value - mean) / std for numeric values, demonstrating
    Polars aggregation and arithmetic operations with the expression API.

    Example:
        SQL:    SELECT polars_normalize(score) FROM exam_results
        Input:  [10, 20, 30, 40, 50]
        Output: [-1.41, -0.71, 0.0, 0.71, 1.41] (approximately)

    """

    value: Annotated[pl.Float64, Param(position=0, doc="Numeric value to normalize")]

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

    def compute_polars(self) -> pl.Expr:
        """Normalize the values using z-score (value - mean) / std."""
        col = pl.col("value")
        return (col - col.mean()) / col.std()


# =============================================================================
# Multiple Arguments Examples
# =============================================================================


class PolarsAddValuesFunction(PolarsScalarFunction):
    """Adds two numeric values together using Polars.

    Demonstrates multiple positional arguments with named param references.

    Example:
        SQL:    SELECT polars_add_values(price, tax) FROM orders
        Input:  price=[10, 20, 30], tax=[1, 2, 3]
        Output: [11, 22, 33]

    """

    left: Annotated[pl.Float64, Param(position=0, doc="First value")]
    right: Annotated[pl.Float64, Param(position=1, doc="Second value")]

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

    def compute_polars(self) -> pl.Expr:
        """Add the two values together."""
        return pl.col("left") + pl.col("right")


class PolarsMultiplyFunction(PolarsScalarFunction):
    """Multiplies a numeric value by a constant factor using Polars.

    Demonstrates mixing a column value with instance state. The factor
    is resolved from the constant argument during initialization.

    Example:
        SQL:    SELECT polars_multiply(price, 2) FROM products
        Input:  price=[10, 20, 30], factor=2
        Output: [20, 40, 60]

    """

    value: Annotated[pl.Float64, Param(position=0, doc="Value to multiply")]

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

    @property
    def factor(self) -> float:
        """Get the multiplication factor from arguments."""
        return self.invocation.arguments.positional[0].as_py()  # type: ignore

    def compute_polars(self) -> pl.Expr:
        """Multiply the values by the constant factor."""
        return pl.col("value") * self.factor


# =============================================================================
# Varargs Example
# =============================================================================


class PolarsSumValuesFunction(PolarsScalarFunction):
    """Sums multiple numeric values using Polars.

    Demonstrates varargs with the expression API - accepts any number
    of numeric values and sums them row-wise using pl.sum_horizontal().

    Example:
        SQL:    SELECT polars_sum_values(a, b, c) FROM data
        Input:  a=[1, 2], b=[10, 20], c=[100, 200]
        Output: [111, 222]

    """

    values: Annotated[pl.Float64, Param(position=0, doc="Values to sum", varargs=True)]

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

    def compute_polars(self) -> pl.Expr:
        """Sum all input values using sum_horizontal."""
        # With varargs, columns are renamed to values_0, values_1, etc.
        # Use regex to match all vararg columns
        return pl.sum_horizontal(pl.col("^values_.*$"))


# =============================================================================
# Dynamic Output Type Example
# =============================================================================


class PolarsDoubleFunction(PolarsScalarFunction):
    """Doubles numeric values, preserving the input type.

    Demonstrates dynamic output type with type_bound - the output type
    matches the input type (int64 in -> int64 out, float64 in -> float64 out).
    Uses type_bound to ensure only numeric inputs are accepted.

    Example:
        SQL:    SELECT polars_double(count) FROM inventory
        Input:  [1, 2, 3] (Int64)
        Output: [2, 4, 6] (Int64, same type preserved)

    """

    value: Annotated[
        Any,
        Param(
            position=0,
            doc="Numeric value to double",
            type_bound=[pat.is_integer, pat.is_floating],
        ),
    ]

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

    def compute_polars(self) -> pl.Expr:
        """Double the values, preserving the input type."""
        return pl.col("value") * 2
