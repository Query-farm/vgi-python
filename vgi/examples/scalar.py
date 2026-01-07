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
import structlog

import vgi.invocation
from vgi.arguments import AnyArrow, Arg
from vgi.exceptions import SchemaValidationError
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction

__all__ = [
    "DoubleColumnFunction",
    "AddNumericColumnsFunction",
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
        examples = [
            FunctionExample(
                sql="SELECT double_column(price) FROM products",
                description="Double the price column",
            ),
            FunctionExample(
                sql="SELECT double_column(quantity) FROM inventory",
                description="Double inventory quantities",
            ),
        ]

    # Explicit arrow_type demonstrates type specification
    column = Arg[str](0, doc="Column name to double", arrow_type=pa.utf8())

    @classmethod
    def catalog_output_type(cls) -> pa.DataType | type[AnyArrow]:
        """Output type depends on input column type."""
        return AnyArrow

    @property
    def output_type(self) -> pa.DataType:
        """Return the type of the doubled column."""
        return cast(pa.DataType, self.input_schema.field(self.column).type)

    def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Double the values in the specified column."""
        return pc.multiply(batch.column(self.column), 2)  # type: ignore[no-matching-overload]


def _is_addable_type(dtype: pa.DataType) -> bool:
    """Check if a type can be passed to pyarrow.compute.add."""
    return (
        pa.types.is_integer(dtype)
        or pa.types.is_floating(dtype)
        or pa.types.is_decimal(dtype)
        or pa.types.is_temporal(dtype)
    )


def _promote_for_addition(dtype: pa.DataType) -> pa.DataType:
    """Return the appropriate output type for addition to reduce overflow risk.

    Adding two values of the same type can overflow, so we promote integers
    to the next larger size. For example, int32 + int32 -> int64.
    """
    if pa.types.is_floating(dtype) or pa.types.is_temporal(dtype):
        return dtype
    if pa.types.is_integer(dtype):
        # Promote to a larger integer type since a + b can overflow
        if dtype == pa.int8():
            return pa.int16()
        if dtype == pa.int16():
            return pa.int32()
        if dtype in (pa.int32(), pa.int64()):
            return pa.int64()
        # Unsigned integers
        if dtype == pa.uint8():
            return pa.uint16()
        if dtype == pa.uint16():
            return pa.uint32()
        if dtype in (pa.uint32(), pa.uint64()):
            return pa.uint64()
        return dtype
    if pa.types.is_decimal(dtype):
        return dtype
    raise SchemaValidationError(f"Unsupported numeric type for addition: {dtype}")


class AddNumericColumnsFunction(ScalarFunction):
    """Adds two numeric columns together.

    Validates that both columns are numeric types (integer, float, decimal, or
    temporal) at bind time, raising SchemaValidationError if not.

    Example:
        Input:  a=[1, 2, 3], b=[10, 20, 30]
        Args:   col1="a", col2="b"
        Output: result=[11, 22, 33]

    Raises:
        SchemaValidationError: If either column is not a numeric type.

    """

    class Meta:
        """Function metadata."""

        name = "add_columns"
        description = "Adds two numeric columns"
        examples = [
            FunctionExample(
                sql="SELECT add_columns(price, tax) FROM orders",
                description="Calculate total by adding price and tax",
            ),
            FunctionExample(
                sql="SELECT add_columns(quantity, reserved) FROM inventory",
                description="Sum quantity and reserved amounts",
            ),
        ]

    col1 = Arg[AnyArrow](0, doc="First column name")
    col2 = Arg[AnyArrow](1, doc="Second column name")

    def __init__(
        self,
        invocation: vgi.invocation.Invocation,
        logger: structlog.stdlib.BoundLogger,
    ):
        """Initialize and validate that input columns are numeric."""
        super().__init__(invocation, logger)
        assert invocation.input_schema is not None  # Required for scalar functions

        field1 = invocation.input_schema.field(self.col1.value)
        field2 = invocation.input_schema.field(self.col2.value)

        if not _is_addable_type(field1.type):
            col1_arg = type(self).col1
            raise SchemaValidationError(
                col1_arg.format_error(f"must be numeric, got {field1.type}")
            )
        if not _is_addable_type(field2.type):
            col2_arg = type(self).col2
            raise SchemaValidationError(
                col2_arg.format_error(f"must be numeric, got {field2.type}")
            )

        # Compute the output type by promoting to the wider of the two types,
        # then promoting again to reduce overflow risk.
        # Use pc.add with null values to determine the common type, as PyArrow's
        # compute functions handle type promotion correctly.
        common_type = pc.add(
            pa.nulls(1, type=field1.type), pa.nulls(1, type=field2.type)
        ).type
        self._output_type = _promote_for_addition(common_type)

    @classmethod
    def catalog_output_type(cls) -> pa.DataType | type[AnyArrow]:
        """Output type depends on input column types."""
        return AnyArrow

    @property
    def output_type(self) -> pa.DataType:
        """Return the computed output type based on input column types."""
        return self._output_type

    def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Add the two columns together."""
        return pc.add(batch.column(self.col1.value), batch.column(self.col2.value))


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
        examples = [
            FunctionExample(
                sql="SELECT upper_case(name) FROM users",
                description="Convert user names to uppercase",
            ),
            FunctionExample(
                sql="SELECT upper_case(status) FROM orders WHERE id = 1",
                description="Uppercase the status field",
            ),
        ]

    column = Arg[str](0, doc="Column name to uppercase")

    @classmethod
    def catalog_output_type(cls) -> pa.DataType | type[AnyArrow]:
        """Return string type (static output)."""
        return pa.string()

    # Note: No need to override output_type - default uses catalog_output_type()

    def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Convert the column values to uppercase."""
        return pc.utf8_upper(batch.column(self.column))  # type: ignore[no-matching-overload]
