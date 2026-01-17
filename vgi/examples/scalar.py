"""Example scalar function implementations.

This module provides example scalar functions that transform input batches
to single-column output with 1:1 row mapping.

AVAILABLE FUNCTIONS
-------------------
DoubleColumnFunction        - Doubles values in a numeric column
AddNumericColumnsFunction   - Adds two numeric columns
UpperCaseFunction           - Converts string column to uppercase
"""

from __future__ import annotations

from typing import Annotated, Any

import pyarrow as pa
import pyarrow.compute as pc

from vgi.arguments import AnyArrow, AnyArrowValue, Arg
from vgi.exceptions import SchemaValidationError
from vgi.metadata import FunctionExample, FunctionStability, NullHandling
from vgi.scalar_function import ScalarFunction

__all__ = [
    "AddNumericColumnsFunction",
    "DoubleColumnFunction",
    "NullHandlingFunction",
    "RandomIntFunction",
    "SumColumnsFunction",
    "UpperCaseFunction",
]


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
        output_type = AnyArrow  # Output type depends on input column type
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
    column: Annotated[
        AnyArrowValue, Arg(0, doc="Value to double", type_bound=_is_addable_type)
    ]

    def bind(self) -> None:
        """Compute output type from input column types."""
        field1 = self.input_schema.field(self.column.value)

        # Since we're going to be multiplying by 2, promote to a wider type
        self._output_type = _promote_for_addition(field1.type)

    @property
    def output_type(self) -> pa.DataType:
        """Return the type of the doubled column."""
        return self._output_type

    def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Double the values in the specified column."""
        return pc.multiply(batch.column(self.column.value), 2)


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
        output_type = AnyArrow  # Output type depends on input column types
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

    # type_bound validates column types at bind time (automatic via Function.__init__)
    col1: Annotated[
        AnyArrowValue, Arg(0, doc="First column name", type_bound=_is_addable_type)
    ]
    col2: Annotated[
        AnyArrowValue, Arg(1, doc="Second column name", type_bound=_is_addable_type)
    ]

    _output_type: pa.DataType

    def bind(self) -> None:
        """Compute output type from input column types."""
        field1 = self.input_schema.field(self.col1.value)
        field2 = self.input_schema.field(self.col2.value)

        # Compute the output type by promoting to the wider of the two types,
        # then promoting again to reduce overflow risk.
        # Use pc.add with null values to determine the common type, as PyArrow's
        # compute functions handle type promotion correctly.
        common_type = pc.add(
            pa.nulls(1, type=field1.type), pa.nulls(1, type=field2.type)
        ).type
        self._output_type = _promote_for_addition(common_type)

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
        output_type = pa.string()  # Static output type
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

    column: Annotated[str, Arg(0, doc="Value")]

    # Note: No need to override output_type - default uses Meta.output_type

    def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Convert the column values to uppercase."""
        return pc.utf8_upper(batch.column(self.column))  # type: ignore[no-matching-overload]


class SumColumnsFunction(ScalarFunction):
    """Sums values from multiple numeric columns.

    Uses varargs with type_bound to accept any number of numeric columns
    and validates that all columns are addable types at bind time.

    Example:
        Input:  a=[1, 2], b=[10, 20], c=[100, 200]
        Args:   columns=('a', 'b', 'c')
        Output: result=[111, 222]

    """

    class Meta:
        """Function metadata."""

        name = "sum_columns"
        description = "Sum values from multiple numeric columns"
        output_type = AnyArrow  # Output type depends on input column types
        examples = [
            FunctionExample(
                sql="SELECT sum_columns(price, tax, shipping) FROM orders",
                description="Calculate total cost from multiple columns",
            ),
        ]

    # Varargs with type_bound validates all columns are numeric
    # Note: varargs returns tuple[Any, ...], not AnyArrowValue
    columns: Annotated[
        tuple[Any, ...],
        Arg(
            0,
            varargs=True,
            type_bound=_is_addable_type,
            doc="Columns to sum (must be numeric)",
        ),
    ]

    _output_type: pa.DataType

    def bind(self) -> None:
        """Compute output type from first column, promoted for overflow safety."""
        # With varargs=True, self.columns is a tuple of column names
        first_col = self.columns[0]  # type: ignore[index]
        first_type = self.input_schema.field(first_col).type
        self._output_type = _promote_for_addition(first_type)

    @property
    def output_type(self) -> pa.DataType:
        """Return the computed output type based on first column."""
        return self._output_type

    def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Sum values from all specified columns."""
        # With varargs=True, self.columns is a tuple of column names at runtime
        columns: tuple[str, ...] = self.columns  # type: ignore[assignment]
        result = batch.column(columns[0])
        for col_name in columns[1:]:
            result = pc.add(result, batch.column(col_name))
        return result


class NullHandlingFunction(ScalarFunction):
    """Demonstrates special null handling in a scalar function.

    This function returns the input value if it's not null, or -5000 if it is null.
    It demonstrates how to use NullHandling.SPECIAL to receive null values
    instead of having them automatically converted to null output.

    Example:
        Input:  x=[1, None, 3]
        Args:   column="x"
        Output: result=[1, -5000, 3]

    """

    class Meta:
        """Function metadata."""

        name = "null_handling"
        description = "Returns value or -5000 if null"
        output_type = pa.int64()
        null_handling = NullHandling.SPECIAL
        examples = [
            FunctionExample(
                sql="SELECT null_handling(value) FROM data",
                description="Replace null values with -5000",
            ),
        ]

    column: Annotated[str, Arg(0, doc="Column name to process")]

    def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Return value if not null, otherwise -5000."""
        col = batch.column(self.column)
        # Use if_else: if value is null, return -5000, otherwise return the value
        result: pa.Array[Any] = pc.if_else(
            pc.is_null(col), pa.scalar(-5000, type=pa.int64()), col
        )
        return result


class RandomIntFunction(ScalarFunction):
    """Generates random integers for each row (demonstrates VOLATILE stability).

    This function demonstrates FunctionStability.VOLATILE - calling it twice
    with the same input will produce different results. The database optimizer
    cannot cache or reuse results from volatile functions.

    Other stability options:
    - CONSISTENT: Same input always produces same output (deterministic)
    - CONSISTENT_WITHIN_QUERY: Same within a query, may vary across queries

    Example:
        Input:  x=[1, 2, 3]  (any column, used only for row count)
        Args:   min_val=1, max_val=100
        Output: result=[42, 87, 13]  (random values, different each time)

    """

    class Meta:
        """Function metadata."""

        name = "random_int"
        description = "Generate random integers (demonstrates VOLATILE stability)"
        output_type = pa.int64()
        stability = FunctionStability.VOLATILE
        examples = [
            FunctionExample(
                sql="SELECT random_int(1, 100) FROM data",
                description="Generate random integers between 1 and 100",
            ),
        ]

    min_val: Annotated[int, Arg(0, doc="Minimum value (inclusive)", default=0)]
    max_val: Annotated[int, Arg(1, doc="Maximum value (inclusive)", default=100)]

    def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Generate random integers for each row."""
        import random

        num_rows = batch.num_rows
        # Generate random integers in the range [min_val, max_val]
        values = [random.randint(self.min_val, self.max_val) for _ in range(num_rows)]
        return pa.array(values, type=pa.int64())
