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
ConditionalMessageFunction  - Multiple ConstParam parameters example
BinaryPacketFunction        - Complex ConstParam types (binary, struct)
UpperCaseFunction           - Converts string value to uppercase
NullHandlingFunction        - Demonstrates special null handling (NullHandling.SPECIAL)
RandomIntFunction           - Generates random integers (VOLATILE stability)
BernoulliFunction           - Generates random booleans (no-input VOLATILE example)
MultiplyBySettingFunction   - Multiplies value by a DuckDB setting
ReturnSecretValueFunction   - Returns a secret value as string

DYNAMIC OUTPUT TYPE (with type_bound)
-------------------------------------
DoubleFunction              - Doubles numeric values (AnyArrow + type_bound)
AddValuesFunction           - Adds two numeric values (type promotion)
SumValuesFunction           - Sums multiple values (varargs example)
"""

from __future__ import annotations

import json
from typing import Annotated, Any

import pyarrow as pa
import pyarrow.compute as pc

from vgi.arguments import ConstParam, OutputLength, Param, Returns, Secret, Setting
from vgi.exceptions import SchemaValidationError
from vgi.metadata import FunctionExample, FunctionStability, NullHandling
from vgi.scalar_function import BindParameters, BindResult, ScalarFunction

__all__ = [
    "AddValuesFunction",
    "BernoulliFunction",
    "BinaryPacketFunction",
    "ConditionalMessageFunction",
    "DoubleFunction",
    "MultiplyBySettingFunction",
    "MultiplyFunction",
    "NullHandlingFunction",
    "RandomIntFunction",
    "ReturnSecretValueFunction",
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

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.Int64Array, Param(doc="Integer value to multiply")],
        factor: Annotated[int, ConstParam("Multiplication factor")],
    ) -> Annotated[pa.Int64Array, Returns()]:
        """Multiply values by the constant factor."""
        return pc.multiply(value, factor)


class ConditionalMessageFunction(ScalarFunction):
    """Returns a repeated message when condition is true, empty string otherwise.

    This example demonstrates multiple ConstParam parameters:
    - repeat_count (int): How many times to repeat the message
    - message (string): The message to repeat
    - condition (boolean column): Whether to apply the message

    The constant parameters come first, followed by the column parameter.

    Example:
        SQL:    SELECT conditional_message(3, 'Hi! ', is_active) FROM users
        Input:  is_active=[true, false, true]
        Args:   repeat_count=3, message='Hi! '
        Output: result=['Hi! Hi! Hi! ', '', 'Hi! Hi! Hi! ']

    """

    class Meta:
        """Function metadata."""

        name = "conditional_message"
        description = "Returns repeated message when condition is true"
        examples = [
            FunctionExample(
                sql="SELECT conditional_message(3, 'Alert! ', flag) FROM items",
                description="Show alert message for flagged items",
            ),
            FunctionExample(
                sql="SELECT conditional_message(2, '⭐', is_featured) FROM products",
                description="Add stars to featured products",
            ),
        ]

    @classmethod
    def compute(
        cls,
        repeat_count: Annotated[int, ConstParam("Number of times to repeat")],
        message: Annotated[str, ConstParam("Message to repeat")],
        condition: Annotated[pa.BooleanArray, Param(doc="Apply message condition")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return repeated message when condition is true, empty string otherwise."""
        repeated_message = message * repeat_count
        result: pa.StringArray = pc.if_else(condition, repeated_message, "")  # type: ignore[assignment]
        return result


# Type for config struct: {label: string, version: int64}
_CONFIG_STRUCT_TYPE = pa.struct([("label", pa.string()), ("version", pa.int64())])


class BinaryPacketFunction(ScalarFunction):
    """Builds binary packets with header, payload, and config metadata.

    This example demonstrates complex ConstParam types:
    - header (binary): Constant prefix bytes at the start
    - payload (binary column): Variable binary data per row
    - config (struct): Constant metadata struct at the end

    The constant parameters bracket the column parameter (first and last).

    The function concatenates: header + payload + config.label encoded + version byte

    Example:
        SQL:    SELECT binary_packet(x'CAFE', data, {label: 'v1', version: 1}) FROM t
        Input:  data=[x'0102', x'0304']
        Args:   header=x'CAFE', config={label: 'v1', version: 1}
        Output: result=[x'CAFE0102763101', x'CAFE0304763101']

    """

    class Meta:
        """Function metadata."""

        name = "binary_packet"
        description = "Build binary packets with header, payload, and config"
        examples = [
            FunctionExample(
                sql="SELECT binary_packet(x'FF', payload, {'tag': 'msg', 1}) FROM t",
                description="Build packets with 0xFF header",
            ),
        ]

    @classmethod
    def compute(
        cls,
        header: Annotated[
            bytes,
            ConstParam("Header bytes to prepend", arrow_type=pa.binary()),
        ],
        payload: Annotated[pa.BinaryArray, Param(doc="Binary payload data")],
        config: Annotated[
            dict[str, Any],
            ConstParam("Config {label, version}", arrow_type=_CONFIG_STRUCT_TYPE),
        ],
    ) -> Annotated[pa.BinaryArray, Returns()]:
        """Build binary packets from header, payload, and config."""
        # Extract config fields
        label: str = config["label"]
        version: int = config["version"]

        # Build suffix from config: label bytes + version as single byte
        suffix = label.encode("utf-8") + bytes([version & 0xFF])

        # Concatenate header + payload + suffix for each row
        results: list[bytes] = []
        for i in range(len(payload)):
            if payload[i].is_valid:
                payload_bytes: bytes = payload[i].as_py()
                results.append(header + payload_bytes + suffix)
            else:
                results.append(header + suffix)  # Empty payload for nulls

        return pa.array(results, type=pa.binary())


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

    @classmethod
    def on_bind(cls, params: BindParameters) -> BindResult:
        """Compute output type from input value type."""
        field = params.arguments_schema.field(0)
        return BindResult(_promote_for_addition(field.type))

    @classmethod
    def compute(
        cls,
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

    @classmethod
    def on_bind(cls, params: BindParameters) -> BindResult:
        """Compute output type from input value types."""
        field1 = params.arguments_schema.field(0)
        field2 = params.arguments_schema.field(1)

        # Compute the output type by promoting to the wider of the two types,
        # then promoting again to reduce overflow risk.
        common_type = pc.add(pa.nulls(1, type=field1.type), pa.nulls(1, type=field2.type)).type
        return BindResult(_promote_for_addition(common_type))

    @classmethod
    def compute(
        cls,
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

    @classmethod
    def compute(
        cls,
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

    @classmethod
    def on_bind(
        cls,
        params: BindParameters,
    ) -> BindResult:
        """Compute output type from first value, promoted for overflow safety."""
        first_type = params.arguments_schema.field(0).type
        return BindResult(_promote_for_addition(first_type))

    @classmethod
    def compute(
        cls,
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

    @classmethod
    def compute(
        cls,
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

    @classmethod
    def compute(
        cls,
        min_val: Annotated[pa.Int64Array, Param(doc="Minimum value (inclusive)")],
        max_val: Annotated[pa.Int64Array, Param(doc="Maximum value (inclusive)")],
    ) -> Annotated[pa.Int64Array, Returns()]:
        """Generate random integers for each row."""
        import numpy as np

        result = np.random.randint(min_val.to_numpy(), max_val.to_numpy() + 1)
        return pa.array(result, type=pa.int64())


class BernoulliFunction(ScalarFunction):
    """Generates random booleans for each row (demonstrates VOLATILE stability).

    This function demonstrates how to generate output without any input parameters.
    It will produce a random 0 or 1 for each row in the output.

    Example:
        SQL:    SELECT bernoulli() FROM data

    """

    class Meta:
        """Function metadata."""

        name = "bernoulli"
        description = "Generate random booleans (demonstrates VOLATILE stability)"
        stability = FunctionStability.VOLATILE
        examples = [
            FunctionExample(
                sql="SELECT bernoulli() FROM data",
                description="Generate samples from the bernoulli distribution",
            ),
        ]

    @classmethod
    def compute(
        cls,
        _length: Annotated[int, OutputLength()],
    ) -> Annotated[pa.BooleanArray, Returns()]:
        """Generate random booleans for each row."""
        import random

        values = [bool(random.randint(0, 1)) for _ in range(_length)]
        return pa.array(values, type=pa.bool_())


class MultiplyBySettingFunction(ScalarFunction):
    """Generates the input value multiplied by a setting."""

    class Meta:
        """Function metadata."""

        name = "multiply_by_setting"
        description = "Multiply the input value by a setting value"
        examples = [
            FunctionExample(
                sql="SELECT multiply_by_setting(5)",
                description="Multiply the input value by a setting's value",
            ),
        ]

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.Int64Array, Param(doc="Integer value to multiply")],
        multiplier: Annotated[pa.Scalar[Any] | None, Setting()],
    ) -> Annotated[pa.Int64Array, Returns()]:
        """Generate the result for each row."""
        assert multiplier is not None
        return pc.multiply(multiplier, value)


class ReturnSecretValueFunction(ScalarFunction):
    """Return the value of a secret.

    Example:
        SQL:    SELECT return_secret_value()

    """

    class Meta:
        """Function metadata."""

        name = "return_secret_value"
        description = "Return a secret's value"
        examples = [
            FunctionExample(
                sql="SELECT return_secret_value()",
                description="Return a secret's value",
            ),
        ]

    @classmethod
    def compute(
        cls,
        vgi_example_secret: Annotated[dict[str, pa.Scalar[Any]], Secret()],
        _length: Annotated[int, OutputLength()],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Generate the result for each row."""
        # Convert pa.Scalar values to Python for JSON serialization
        secret_dict = {k: v.as_py() for k, v in vgi_example_secret.items()}
        return pa.array(
            [json.dumps(secret_dict) for _ in range(_length)],
            type=pa.string(),
        )
