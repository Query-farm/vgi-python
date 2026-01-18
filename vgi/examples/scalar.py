"""Example scalar function implementations.

This module provides example scalar functions that transform input batches
to single-column output with 1:1 row mapping.

AVAILABLE FUNCTIONS
-------------------
DoubleColumnFunction        - Doubles values in a numeric column (AnyArrow example)
AddNumericColumnsFunction   - Adds two numeric columns
UpperCaseFunction           - Converts string column to uppercase
MultiplyFunction            - Multiplies column by constant (ConstParam example)
SumColumnsFunction          - Sums multiple columns (varargs example)
NullHandlingFunction        - Demonstrates special null handling
RandomIntFunction           - Generates random integers (VOLATILE stability)
"""

from __future__ import annotations

from typing import Annotated, Any

import pyarrow as pa
import pyarrow.compute as pc

from vgi.arguments import AnyArrow, AnyArrowValue, Arg, ConstParam, Param, Returns
from vgi.exceptions import SchemaValidationError
from vgi.metadata import FunctionExample, FunctionStability, NullHandling
from vgi.scalar_function import ScalarFunction

__all__ = [
    "AddNumericColumnsFunction",
    "DoubleColumnFunction",
    "MultiplyFunction",
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


# =============================================================================
# New Param/ConstParam/Returns API Examples
# =============================================================================


class MultiplyFunction(ScalarFunction):
    """Multiplies a column by a constant factor.

    This example demonstrates the new Param/ConstParam/Returns API:
    - Param() for columnar input (receives pa.Array at runtime)
    - ConstParam() for constant scalar input (receives Python value at runtime)
    - Returns() for declaring output type

    Example:
        SQL:    SELECT multiply(price, 2) FROM products
        Input:  price=[10, 20, 30]
        Args:   factor=2
        Output: result=[20, 40, 60]

    """

    class Meta:
        """Function metadata."""

        name = "multiply"
        description = "Multiplies a column by a constant factor"
        examples = [
            FunctionExample(
                sql="SELECT multiply(price, 2) FROM products",
                description="Double all prices",
            ),
            FunctionExample(
                sql="SELECT multiply(quantity, 10) FROM inventory",
                description="Scale quantities by 10",
            ),
        ]

    def compute(
        self,
        column: Param(pa.int64(), "Column to multiply"),  # type: ignore[valid-type]
        factor: ConstParam(int, "Multiplication factor"),  # type: ignore[valid-type]
    ) -> Returns(pa.int64()):  # type: ignore[valid-type]
        """Multiply column values by the constant factor."""
        return pc.multiply(column, factor)


# =============================================================================
# Legacy Arg Descriptor API Examples (still supported)
# =============================================================================


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

    def compute(self, *, column: pa.Array[Any]) -> pa.Array[Any]:
        """Double the values in the specified column."""
        return pc.multiply(column, 2)


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

    # type_bound validates value types at bind time (automatic via Function.__init__)
    col1: Annotated[
        AnyArrowValue, Arg(0, doc="First numeric value", type_bound=_is_addable_type)
    ]
    col2: Annotated[
        AnyArrowValue, Arg(1, doc="Second numeric value", type_bound=_is_addable_type)
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

    def compute(self, *, col1: pa.Array[Any], col2: pa.Array[Any]) -> pa.Array[Any]:
        """Add the two columns together."""
        return pc.add(col1, col2)


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

    column: Annotated[str, Arg(0, doc="String value to uppercase")]

    # Note: No need to override output_type - default uses Meta.output_type

    def compute(self, *, column: pa.Array[Any]) -> pa.Array[Any]:
        """Convert the string values to uppercase."""
        return pc.utf8_upper(column)  # type: ignore[no-matching-overload]


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

    # Varargs with type_bound validates all values are numeric
    # Note: varargs returns tuple[Any, ...], not AnyArrowValue
    columns: Annotated[
        tuple[Any, ...],
        Arg(
            0,
            varargs=True,
            type_bound=_is_addable_type,
            doc="Numeric values to sum",
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

    def compute(self, *, columns: list[pa.Array[Any]]) -> pa.Array[Any]:
        """Sum values from all specified columns."""
        result = columns[0]
        for col in columns[1:]:
            result = pc.add(result, col)
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

    column: Annotated[int, Arg(0, doc="Integer value to process")]

    def compute(self, *, column: pa.Array[Any]) -> pa.Array[pa.Int64Scalar]:
        """Return value if not null, otherwise -5000."""
        # Use if_else: if value is null, return -5000, otherwise return the value
        result: pa.Array[pa.Int64Scalar] = pc.if_else(
            pc.is_null(column), pa.scalar(-5000, type=pa.int64()), column
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
        SQL:    SELECT random_int(min_col, max_col) FROM data
        Input:  min_col=[1, 10, 100], max_col=[10, 100, 1000]
        Output: result=[7, 55, 823]  (random values per row, different each time)

    """

    class Meta:
        """Function metadata."""

        name = "random_int"
        description = "Generate random integers (demonstrates VOLATILE stability)"
        output_type = pa.int64()
        stability = FunctionStability.VOLATILE
        examples = [
            FunctionExample(
                sql="SELECT random_int(min_col, max_col) FROM data",
                description="Generate random integers between min and max values",
            ),
        ]

    min_val: Annotated[AnyArrowValue, Arg(0, doc="Minimum value (inclusive)")]
    max_val: Annotated[AnyArrowValue, Arg(1, doc="Maximum value (inclusive)")]

    def compute(
        self, *, min_val: pa.Array[Any], max_val: pa.Array[Any]
    ) -> pa.Array[Any]:
        """Generate random integers for each row."""
        import random

        # Get values from the arrays
        min_values: list[int] = min_val.to_pylist()  # type: ignore[assignment]
        max_values: list[int] = max_val.to_pylist()  # type: ignore[assignment]

        # Generate random integers using per-row min/max
        values = [
            random.randint(min_v, max_v)
            for min_v, max_v in zip(min_values, max_values, strict=True)
        ]
        return pa.array(values, type=pa.int64())
