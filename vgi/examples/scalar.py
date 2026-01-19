"""Example scalar function implementations.

This module provides example scalar functions that transform input batches
to single-column output with 1:1 row mapping.

All functions use the Annotated[T, Param/ConstParam/Returns] API with hybrid
type inference:

- Simple array types: Inferred from array class (pa.Int64Array -> pa.int64())
- Complex types: Require explicit arrow_type (pa.StructArray needs arrow_type=...)
- AnyArrow: Use pa.Array with no arrow_type for dynamic types

STATIC OUTPUT TYPE (inferred from array class)
----------------------------------------------
MultiplyFunction            - Multiplies value by constant (ConstParam example)
UpperCaseFunction           - Converts string value to uppercase
NullHandlingFunction        - Demonstrates special null handling (NullHandling.SPECIAL)
RandomIntFunction           - Generates random integers (VOLATILE stability)

DYNAMIC OUTPUT TYPE (with type_bound)
-------------------------------------
DoubleFunction              - Doubles numeric values (AnyArrow + type_bound)
AddValuesFunction           - Adds two numeric values (type promotion)
SumValuesFunction           - Sums multiple values (varargs example)
"""

from __future__ import annotations

from typing import Annotated, Any

import pyarrow as pa
import pyarrow.compute as pc

from vgi.arguments import ConstParam, Param, Returns
from vgi.exceptions import SchemaValidationError
from vgi.metadata import FunctionExample, FunctionStability, NullHandling
from vgi.scalar_function import ScalarFunction

__all__ = [
    "AddValuesFunction",
    "DoubleFunction",
    "MultiplyFunction",
    "NullHandlingFunction",
    "RandomIntFunction",
    "SumValuesFunction",
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
    """Multiplies a value by a constant factor.

    This example demonstrates type inference with array classes:
    - pa.Int64Array -> pa.int64() (inferred from Annotated type)
    - ConstParam() for constant scalar input (receives Python value at runtime)
    - Returns() output type is also inferred from pa.Int64Array

    Example:
        SQL:    SELECT multiply(price, 2) FROM products
        Input:  price=[10, 20, 30]
        Args:   factor=2
        Output: result=[20, 40, 60]

    """

    class Meta:
        """Function metadata."""

        name = "multiply"
        description = "Multiplies a value by a constant factor"
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
        value: Annotated[pa.Int64Array, Param(doc="Integer value to multiply")],
        factor: Annotated[int, ConstParam("Multiplication factor")],
    ) -> Annotated[pa.Int64Array, Returns()]:
        """Multiply values by the constant factor."""
        return pc.multiply(value, factor)


class DoubleFunction(ScalarFunction):
    """Doubles numeric values.

    This example demonstrates the Annotated API with:
    - AnyArrow type (arrow_type=None) with type_bound for flexible numeric input
    - Dynamic output type computed in bind()

    Example:
        SQL:    SELECT double(price) FROM products
        Input:  price=[1, 2, 3]
        Output: result=[2, 4, 6]

    """

    class Meta:
        """Function metadata."""

        name = "double"
        description = "Doubles numeric values"
        examples = [
            FunctionExample(
                sql="SELECT double(price) FROM products",
                description="Double the price values",
            ),
            FunctionExample(
                sql="SELECT double(quantity) FROM inventory",
                description="Double inventory quantities",
            ),
        ]

    _output_type: pa.DataType

    def bind(self) -> None:
        """Compute output type from input value type."""
        # Get the input type from the schema
        field = self.input_schema.field(0)
        # Promote to a wider type since we're multiplying by 2
        self._output_type = _promote_for_addition(field.type)

    @property
    def output_type(self) -> pa.DataType:
        """Return the type of the doubled value."""
        return self._output_type

    def compute(
        self,
        value: Annotated[
            pa.Array[Any],
            Param(doc="Numeric value to double", type_bound=_is_addable_type),
        ],
    ) -> Annotated[pa.Array[Any], Returns()]:
        """Double the input values."""
        result: pa.Array[Any] = pc.multiply(value, 2)
        return result


class AddValuesFunction(ScalarFunction):
    """Adds two numeric values together.

    This example demonstrates:
    - Multiple Param() annotations with type_bound validation
    - Dynamic output type with type promotion for overflow safety

    Validates that both values are numeric types (integer, float, decimal, or
    temporal) at compute time, raising SchemaValidationError if not.

    Example:
        SQL:    SELECT add_values(price, tax) FROM orders
        Input:  price=[1, 2, 3], tax=[10, 20, 30]
        Output: result=[11, 22, 33]

    Raises:
        SchemaValidationError: If either value is not a numeric type.

    """

    class Meta:
        """Function metadata."""

        name = "add_values"
        description = "Adds two numeric values"
        examples = [
            FunctionExample(
                sql="SELECT add_values(price, tax) FROM orders",
                description="Calculate total by adding price and tax",
            ),
            FunctionExample(
                sql="SELECT add_values(quantity, reserved) FROM inventory",
                description="Sum quantity and reserved amounts",
            ),
        ]

    _output_type: pa.DataType

    def bind(self) -> None:
        """Compute output type from input value types."""
        field1 = self.input_schema.field(0)
        field2 = self.input_schema.field(1)

        # Compute the output type by promoting to the wider of the two types,
        # then promoting again to reduce overflow risk.
        common_type = pc.add(
            pa.nulls(1, type=field1.type), pa.nulls(1, type=field2.type)
        ).type
        self._output_type = _promote_for_addition(common_type)

    @property
    def output_type(self) -> pa.DataType:
        """Return the computed output type based on input value types."""
        return self._output_type

    def compute(
        self,
        col1: Annotated[
            pa.Array[Any],
            Param(doc="First numeric value", type_bound=_is_addable_type),
        ],
        col2: Annotated[
            pa.Array[Any],
            Param(doc="Second numeric value", type_bound=_is_addable_type),
        ],
    ) -> Annotated[pa.Array[Any], Returns()]:
        """Add the two values together."""
        result: pa.Array[Any] = pc.add(col1, col2)
        return result


class UpperCaseFunction(ScalarFunction):
    """Converts string values to uppercase.

    This example demonstrates type inference with pa.StringArray:
    - pa.StringArray -> pa.string() (inferred from Annotated type)
    - Returns() output type is also inferred from pa.StringArray

    Example:
        SQL:    SELECT upper_case(name) FROM users
        Input:  name=["alice", "bob", "charlie"]
        Output: result=["ALICE", "BOB", "CHARLIE"]

    """

    class Meta:
        """Function metadata."""

        name = "upper_case"
        description = "Converts string values to uppercase"
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

    def compute(
        self,
        value: Annotated[pa.StringArray, Param(doc="String value to uppercase")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Convert the string values to uppercase."""
        return pc.utf8_upper(value)


class SumValuesFunction(ScalarFunction):
    """Sums multiple numeric values.

    This example demonstrates:
    - varargs=True to accept variable number of values
    - type_bound validation on all varargs values
    - Dynamic output type computed in bind()

    Example:
        SQL:    SELECT sum_values(price, tax, shipping) FROM orders
        Input:  price=[1, 2], tax=[10, 20], shipping=[100, 200]
        Output: result=[111, 222]

    """

    class Meta:
        """Function metadata."""

        name = "sum_values"
        description = "Sum multiple numeric values"
        examples = [
            FunctionExample(
                sql="SELECT sum_values(price, tax, shipping) FROM orders",
                description="Calculate total cost from multiple values",
            ),
        ]

    _output_type: pa.DataType

    def bind(self) -> None:
        """Compute output type from first value, promoted for overflow safety."""
        first_type = self.input_schema.field(0).type
        self._output_type = _promote_for_addition(first_type)

    @property
    def output_type(self) -> pa.DataType:
        """Return the computed output type based on first value."""
        return self._output_type

    def compute(
        self,
        values: Annotated[
            list[pa.Array[Any]],
            Param(
                doc="Numeric values to sum",
                type_bound=_is_addable_type,
                varargs=True,
            ),
        ],
    ) -> Annotated[pa.Array[Any], Returns()]:
        """Sum all specified values."""
        result: pa.Array[Any] = values[0]
        for val in values[1:]:
            result = pc.add(result, val)
        return result


class NullHandlingFunction(ScalarFunction):
    """Demonstrates special null handling in a scalar function.

    This function returns the input value if it's not null, or -5000 if it is null.
    It demonstrates how to use NullHandling.SPECIAL to receive null values
    instead of having them automatically converted to null output.

    This example uses type inference with pa.Int64Array and Meta.null_handling.

    Example:
        SQL:    SELECT null_handling(value) FROM data
        Input:  value=[1, None, 3]
        Output: result=[1, -5000, 3]

    """

    class Meta:
        """Function metadata."""

        name = "null_handling"
        description = "Returns value or -5000 if null"
        null_handling = NullHandling.SPECIAL
        examples = [
            FunctionExample(
                sql="SELECT null_handling(value) FROM data",
                description="Replace null values with -5000",
            ),
        ]

    def compute(
        self,
        value: Annotated[pa.Int64Array, Param(doc="Integer value to process")],
    ) -> Annotated[pa.Int64Array, Returns()]:
        """Return value if not null, otherwise -5000."""
        # Use if_else: if value is null, return -5000, otherwise return the value
        result: pa.Int64Array = pc.if_else(  # type: ignore[assignment]
            pc.is_null(value), pa.scalar(-5000, type=pa.int64()), value
        )
        return result


class RandomIntFunction(ScalarFunction):
    """Generates random integers for each row (demonstrates VOLATILE stability).

    This function demonstrates FunctionStability.VOLATILE - calling it twice
    with the same input will produce different results. The database optimizer
    cannot cache or reuse results from volatile functions.

    This example uses type inference with pa.Int64Array and Meta.stability.

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
        stability = FunctionStability.VOLATILE
        examples = [
            FunctionExample(
                sql="SELECT random_int(min_col, max_col) FROM data",
                description="Generate random integers between min and max values",
            ),
        ]

    def compute(
        self,
        min_val: Annotated[pa.Int64Array, Param(doc="Minimum value (inclusive)")],
        max_val: Annotated[pa.Int64Array, Param(doc="Maximum value (inclusive)")],
    ) -> Annotated[pa.Int64Array, Returns()]:
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
