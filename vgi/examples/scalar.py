"""Example scalar function implementations.

This module provides example scalar functions that transform input batches
to single-column output with 1:1 row mapping.

AVAILABLE FUNCTIONS
-------------------
DoubleColumnFunction    - Doubles values in a numeric column
AddColumnsFunction      - Adds two numeric columns
UpperCaseFunction       - Converts string column to uppercase
"""

from __future__ import annotations

from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc

from vgi.arguments import Arg
from vgi.scalar_function import ScalarFunction

__all__ = [
    "DoubleColumnFunction",
    "AddColumnsFunction",
    "UpperCaseFunction",
]


class DoubleColumnFunction(ScalarFunction):
    """Doubles values in a numeric column.

    Example:
        Input:  x=[1, 2, 3]
        Args:   column="x"
        Output: result=[2, 4, 6]

    """

    class Meta:
        """Function metadata."""

        name = "double_column"
        description = "Doubles values in a numeric column"

    column = Arg[str](0, doc="Column name to double")

    @property
    def output_type(self) -> pa.DataType:
        """Return the type of the doubled column."""
        return cast(pa.DataType, self.input_schema.field(self.column).type)

    def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Double the values in the specified column."""
        return pc.multiply(batch.column(self.column), 2)  # type: ignore[no-matching-overload]


class AddColumnsFunction(ScalarFunction):
    """Adds two numeric columns together.

    Example:
        Input:  a=[1, 2, 3], b=[10, 20, 30]
        Args:   col1="a", col2="b"
        Output: result=[11, 22, 33]

    """

    class Meta:
        """Function metadata."""

        name = "add_columns"
        description = "Adds two numeric columns"

    col1 = Arg[str](0, doc="First column name")
    col2 = Arg[str](1, doc="Second column name")

    @property
    def output_type(self) -> pa.DataType:
        """Return the type of the first column."""
        return cast(pa.DataType, self.input_schema.field(self.col1).type)

    def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Add the two columns together."""
        return pc.add(batch.column(self.col1), batch.column(self.col2))


class UpperCaseFunction(ScalarFunction):
    """Converts a string column to uppercase.

    Example:
        Input:  name=["alice", "bob", "charlie"]
        Args:   column="name"
        Output: result=["ALICE", "BOB", "CHARLIE"]

    """

    class Meta:
        """Function metadata."""

        name = "upper_case"
        description = "Converts string column to uppercase"

    column = Arg[str](0, doc="Column name to uppercase")

    @property
    def output_type(self) -> pa.DataType:
        """Return string type."""
        return pa.string()

    def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Convert the column values to uppercase."""
        return pc.utf8_upper(batch.column(self.column))  # type: ignore[no-matching-overload]
