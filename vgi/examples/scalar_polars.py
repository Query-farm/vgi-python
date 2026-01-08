"""Example Polars scalar function implementations.

This module provides example scalar functions that use Polars for data
processing, demonstrating the zero-copy integration pattern between
Arrow and Polars.

ZERO-COPY PATTERN
-----------------
Polars can work directly with Arrow data without copying:

    # Convert Arrow batch to Polars DataFrame (zero-copy)
    df = pl.from_arrow(batch)

    # Perform Polars operations
    result_df = df.select(pl.col("column").str.to_uppercase().alias("result"))

    # Convert back to Arrow array (zero-copy)
    result_array = result_df.to_arrow()["result"].combine_chunks()

AVAILABLE FUNCTIONS
-------------------
PolarsUpperCaseFunction     - Converts string column to uppercase using Polars
PolarsStringLengthFunction  - Computes string lengths using Polars
PolarsNormalizeFunction     - Z-score normalization using Polars
"""

from __future__ import annotations

from typing import Any

import polars as pl
import pyarrow as pa

from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction

__all__ = [
    "PolarsNormalizeFunction",
    "PolarsStringLengthFunction",
    "PolarsUpperCaseFunction",
]


class PolarsUpperCaseFunction(ScalarFunction):
    """Converts a string column to uppercase using Polars.

    Demonstrates zero-copy Arrow-to-Polars-to-Arrow conversion for string
    operations.

    Example:
        Input:  name=["alice", "bob", "charlie"]
        Args:   column="name"
        Output: result=["ALICE", "BOB", "CHARLIE"]

    """

    class Meta:
        """Function metadata."""

        name = "polars_upper_case"
        description = "Converts string column to uppercase using Polars"
        output_type = pa.string()
        examples = [
            FunctionExample(
                sql="SELECT polars_upper_case(name) FROM users",
                description="Convert user names to uppercase using Polars",
            ),
        ]

    column = Arg[str](0, doc="Column name to uppercase")

    def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Convert the column values to uppercase using Polars."""
        # Zero-copy conversion to Polars DataFrame
        df = pl.from_arrow(batch)

        # Polars string operation
        result_df = df.select(pl.col(self.column).str.to_uppercase().alias("result"))

        # Zero-copy conversion back to Arrow
        return result_df.to_arrow()["result"].combine_chunks()


class PolarsStringLengthFunction(ScalarFunction):
    """Computes string lengths using Polars.

    Demonstrates using Polars string length computation with zero-copy
    Arrow integration.

    Example:
        Input:  text=["hello", "hi", "goodbye"]
        Args:   column="text"
        Output: result=[5, 2, 7]

    """

    class Meta:
        """Function metadata."""

        name = "polars_string_length"
        description = "Computes string lengths using Polars"
        output_type = pa.uint32()
        examples = [
            FunctionExample(
                sql="SELECT polars_string_length(description) FROM products",
                description="Get length of product descriptions",
            ),
        ]

    column = Arg[str](0, doc="Column name to compute length of")

    def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Compute string lengths using Polars."""
        # Zero-copy conversion to Polars DataFrame
        df = pl.from_arrow(batch)

        # Polars string length operation
        result_df = df.select(pl.col(self.column).str.len_chars().alias("result"))

        # Zero-copy conversion back to Arrow
        return result_df.to_arrow()["result"].combine_chunks()


class PolarsNormalizeFunction(ScalarFunction):
    """Z-score normalization using Polars.

    Computes (value - mean) / std for a numeric column, demonstrating
    Polars aggregation and arithmetic operations with Arrow integration.

    Example:
        Input:  value=[10, 20, 30, 40, 50]
        Args:   column="value"
        Output: result=[-1.41, -0.71, 0.0, 0.71, 1.41] (approximately)

    """

    class Meta:
        """Function metadata."""

        name = "polars_normalize"
        description = "Z-score normalization using Polars"
        output_type = pa.float64()
        examples = [
            FunctionExample(
                sql="SELECT polars_normalize(score) FROM exam_results",
                description="Normalize exam scores to z-scores",
            ),
        ]

    column = Arg[str](0, doc="Column name to normalize")

    def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Normalize the column using z-score (value - mean) / std."""
        # Zero-copy conversion to Polars DataFrame
        df = pl.from_arrow(batch)

        col = pl.col(self.column)

        # Z-score normalization: (x - mean) / std
        result_df = df.select(
            ((col - col.mean()) / col.std()).alias("result"),
        )

        # Zero-copy conversion back to Arrow
        return result_df.to_arrow()["result"].combine_chunks()
