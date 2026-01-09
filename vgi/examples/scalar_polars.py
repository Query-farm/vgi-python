"""Example Polars scalar function implementations.

This module provides example scalar functions that use Polars for data
processing, demonstrating the PolarsScalarFunction base class which
handles zero-copy Arrow-Polars conversion automatically.

POLARS SCALAR FUNCTIONS
-----------------------
PolarsScalarFunction handles the Arrow <-> Polars conversion automatically:

    class MyFunction(PolarsScalarFunction):
        class Meta:
            output_type = pl.Utf8  # Polars type, not Arrow

        column = Arg[str](0, doc="Column name")

        def compute_polars(self, df: pl.DataFrame) -> pl.Series:
            return df[self.column].str.to_uppercase()

AVAILABLE FUNCTIONS
-------------------
PolarsUpperCaseFunction     - Converts string column to uppercase using Polars
PolarsStringLengthFunction  - Computes string lengths using Polars
PolarsNormalizeFunction     - Z-score normalization using Polars
"""

from __future__ import annotations

import polars as pl

from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.scalar_function_polars import PolarsScalarFunction

__all__ = [
    "PolarsNormalizeFunction",
    "PolarsStringLengthFunction",
    "PolarsUpperCaseFunction",
]


class PolarsUpperCaseFunction(PolarsScalarFunction):
    """Converts a string column to uppercase using Polars.

    Demonstrates PolarsScalarFunction which handles zero-copy Arrow-to-Polars
    conversion automatically.

    Example:
        Input:  name=["alice", "bob", "charlie"]
        Args:   column="name"
        Output: result=["ALICE", "BOB", "CHARLIE"]

    """

    class Meta:
        """Function metadata."""

        name = "polars_upper_case"
        description = "Converts string column to uppercase using Polars"
        output_type = pl.Utf8
        examples = [
            FunctionExample(
                sql="SELECT polars_upper_case(name) FROM users",
                description="Convert user names to uppercase using Polars",
            ),
        ]

    column = Arg[str](0, doc="Column name to uppercase")

    def compute_polars(self, df: pl.DataFrame) -> pl.Series:
        """Convert the column values to uppercase using Polars."""
        return df[self.column].str.to_uppercase()


class PolarsStringLengthFunction(PolarsScalarFunction):
    """Computes string lengths using Polars.

    Demonstrates using Polars string length computation with
    PolarsScalarFunction for automatic Arrow integration.

    Example:
        Input:  text=["hello", "hi", "goodbye"]
        Args:   column="text"
        Output: result=[5, 2, 7]

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

    column = Arg[str](0, doc="Column name to compute length of")

    def compute_polars(self, df: pl.DataFrame) -> pl.Series:
        """Compute string lengths using Polars."""
        return df[self.column].str.len_chars()


class PolarsNormalizeFunction(PolarsScalarFunction):
    """Z-score normalization using Polars.

    Computes (value - mean) / std for a numeric column, demonstrating
    Polars aggregation and arithmetic operations with PolarsScalarFunction.

    Example:
        Input:  value=[10, 20, 30, 40, 50]
        Args:   column="value"
        Output: result=[-1.41, -0.71, 0.0, 0.71, 1.41] (approximately)

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

    column = Arg[str](0, doc="Column name to normalize")

    def compute_polars(self, df: pl.DataFrame) -> pl.Series:
        """Normalize the column using z-score (value - mean) / std."""
        col = df[self.column]
        return (col - col.mean()) / col.std()
