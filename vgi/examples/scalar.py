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
HashSeedFunction            - Generates deterministic integers from constant seed (1 ConstParam, 0 Params)
BernoulliFunction           - Generates random booleans (no-input VOLATILE example)
MultiplyBySettingFunction   - Multiplies value by a DuckDB setting
ReturnSecretValueFunction   - Returns a secret value as string

DYNAMIC OUTPUT TYPE (with type_bound)
-------------------------------------
DoubleFunction              - Doubles numeric values (AnyArrow + type_bound)
AddValuesFunction           - Adds two numeric values (type promotion)
SumValuesFunction           - Sums multiple values (varargs example)

COMPLEX ARROW TYPES (struct, list, fixed-size list)
---------------------------------------------------
GeoDistanceStructFunction   - Euclidean distance between struct points
GeoDistanceListFunction     - Euclidean distance between list points
GeoDistanceFixedFunction    - Euclidean distance between fixed-size list points
GeoCentroidStructFunction   - Centroid of N struct points (varargs)
GeoCentroidListFunction     - Centroid of N list points (varargs)
GeoCentroidFixedFunction    - Centroid of N fixed-size list points (varargs)
"""

from __future__ import annotations

import json
from typing import Annotated, Any

import pyarrow as pa
import pyarrow.compute as pc

from vgi.arguments import Auth, ConstParam, OutputLength, Param, Returns, Secret, Setting
from vgi.auth import AuthContext
from vgi.exceptions import SchemaValidationError
from vgi.metadata import FunctionExample, FunctionStability, NullHandling
from vgi.scalar_function import BindParameters, BindResult, ScalarFunction

__all__ = [
    "AddValuesFunction",
    "AnyMixedIntFunction",
    "AnyMixedStrFunction",
    "BernoulliFunction",
    "ConcatValuesIntFunction",
    "ConcatValuesStrFunction",
    "FormatNumberDefaultFunction",
    "FormatNumberFullFunction",
    "FormatNumberPrecisionFunction",
    "GeoCentroidFixedFunction",
    "GeoCentroidListFunction",
    "GeoCentroidStructFunction",
    "GeoDistanceFixedFunction",
    "GeoDistanceListFunction",
    "GeoDistanceStructFunction",
    "HashSeedFunction",
    "BinaryPacketFunction",
    "ConditionalMessageFunction",
    "DoubleFunction",
    "MultiplyBySettingFunction",
    "MultiplyFunction",
    "NullHandlingFunction",
    "PairTypeIntIntFunction",
    "PairTypeIntStrFunction",
    "PairTypeStrStrFunction",
    "RandomIntFunction",
    "RandomBytesFunction",
    "ReturnSecretValueFunction",
    "SmartFormatPrefixFunction",
    "SmartFormatWidthFunction",
    "SumValuesFunction",
    "TypeInfoInt32Function",
    "TypeInfoInt64Function",
    "TypeInfoStringFunction",
    "TypeInfoUInt32Function",
    "TypeInfoUInt64Function",
    "UpperCaseFunction",
    "WhoAmIFunction",
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
    if pa.types.is_temporal(dtype):
        return dtype
    if pa.types.is_floating(dtype):
        # Promote float32 -> float64 to reduce overflow risk
        if dtype == pa.float16() or dtype == pa.float32():
            return pa.float64()
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
                sql="SELECT binary_packet(x'FF', payload, {label: 'msg', version: 1}) FROM t",
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


class HashSeedFunction(ScalarFunction):
    """Generates deterministic integers from a constant seed.

    Demonstrates the single-ConstParam pattern: one constant argument
    folded at plan time, no column parameters.

    Example:
        SQL:    SELECT hash_seed(42) FROM data
        Input:  (no column input)
        Args:   seed=42
        Output: result=[42, 43, 44, ...]  (seed + row_index)

    """

    class Meta:
        """Function metadata."""

        name = "hash_seed"
        description = "Generate deterministic integers from a constant seed"
        stability = FunctionStability.CONSISTENT
        examples = [
            FunctionExample(
                sql="SELECT hash_seed(42) FROM data",
                description="Generate deterministic integers seeded at 42",
            ),
        ]

    @classmethod
    def compute(
        cls,
        seed: Annotated[int, ConstParam("Seed value")],
        _length: Annotated[int, OutputLength()],
    ) -> Annotated[pa.Int64Array, Returns()]:
        """Generate deterministic integers: seed + row_index for each row."""
        return pa.array([seed + i for i in range(_length)], type=pa.int64())


class RandomBytesFunction(ScalarFunction):
    """Generates deterministic pseudo-random binary blobs from a seed."""

    class Meta:
        """Function metadata."""

        name = "random_bytes"
        description = "Generate pseudo-random binary blobs from seed and length"
        stability = FunctionStability.CONSISTENT
        examples = [
            FunctionExample(
                sql="SELECT random_bytes(42, 16) FROM data",
                description="Generate a deterministic 16-byte blob per input row",
            ),
        ]

    @classmethod
    def compute(
        cls,
        seed: Annotated[int, ConstParam("Seed for pseudo-random byte generation")],
        byte_length: Annotated[int, ConstParam("Output blob length in bytes")],
        _length: Annotated[int, OutputLength()],
    ) -> Annotated[pa.BinaryArray, Returns()]:
        """Generate pseudo-random binary blobs for each row."""
        import random

        if byte_length < 0:
            raise ValueError("byte_length must be >= 0")
        rng = random.Random(seed)
        return pa.array(
            [bytes(rng.getrandbits(8) for _ in range(byte_length)) for _ in range(_length)],
            type=pa.binary(),
        )


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
        vgi_example_secret: Annotated[dict[str, pa.Scalar[Any]], Secret("vgi_example")],
        _length: Annotated[int, OutputLength()],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Generate the result for each row."""
        # Convert pa.Scalar values to Python for JSON serialization
        secret_dict = {k: v.as_py() for k, v in vgi_example_secret.items()}
        return pa.array(
            [json.dumps(secret_dict) for _ in range(_length)],
            type=pa.string(),
        )


# ============================================================================
# format_number — overloaded scalar function (3 overloads by ConstParam count)
# ============================================================================


class FormatNumberDefaultFunction(ScalarFunction):
    """Format a number with default precision (0 decimal places).

    Overload with 0 ConstParams: just a column input.

    Example:
        SQL:    SELECT format_number(price) FROM products
        Input:  price=[3.14, 2.718, 100.5]
        Output: result=['3', '3', '100']

    """

    class Meta:
        """Function metadata."""

        name = "format_number"
        description = "Format number with default precision (0 decimals)"
        examples = [
            FunctionExample(
                sql="SELECT format_number(price) FROM products",
                description="Format prices with no decimal places",
            ),
        ]

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.DoubleArray, Param(doc="Number to format")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Format each value with 0 decimal places."""
        return pa.array(
            [f"{v:.0f}" if v is not None else None for v in value.to_pylist()],
            type=pa.string(),
        )


class FormatNumberPrecisionFunction(ScalarFunction):
    """Format a number with specified precision.

    Overload with 1 ConstParam: precision.

    Example:
        SQL:    SELECT format_number(2, price) FROM products
        Input:  price=[3.14159, 2.718, 100.5]
        Args:   precision=2
        Output: result=['3.14', '2.72', '100.50']

    """

    class Meta:
        """Function metadata."""

        name = "format_number"
        description = "Format number with specified precision"
        examples = [
            FunctionExample(
                sql="SELECT format_number(2, price) FROM products",
                description="Format prices with 2 decimal places",
            ),
        ]

    @classmethod
    def compute(
        cls,
        precision: Annotated[int, ConstParam("Number of decimal places")],
        value: Annotated[pa.DoubleArray, Param(doc="Number to format")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Format each value with the specified precision."""
        return pa.array(
            [f"{v:.{precision}f}" if v is not None else None for v in value.to_pylist()],
            type=pa.string(),
        )


class FormatNumberFullFunction(ScalarFunction):
    """Format a number with precision and prefix.

    Overload with 2 ConstParams: precision and prefix.

    Example:
        SQL:    SELECT format_number(2, '$', price) FROM products
        Input:  price=[3.14, 2.718, 100.5]
        Args:   precision=2, prefix='$'
        Output: result=['$3.14', '$2.72', '$100.50']

    """

    class Meta:
        """Function metadata."""

        name = "format_number"
        description = "Format number with precision and prefix"
        examples = [
            FunctionExample(
                sql="SELECT format_number(2, '$', price) FROM products",
                description="Format prices with dollar sign and 2 decimals",
            ),
        ]

    @classmethod
    def compute(
        cls,
        precision: Annotated[int, ConstParam("Number of decimal places")],
        prefix: Annotated[str, ConstParam("Prefix string")],
        value: Annotated[pa.DoubleArray, Param(doc="Number to format")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Format each value with prefix and specified precision."""
        return pa.array(
            [f"{prefix}{v:.{precision}f}" if v is not None else None for v in value.to_pylist()],
            type=pa.string(),
        )


# ============================================================================
# type_info — overloaded scalar function (5 overloads by column type)
#
# Each overload is a separate class because ScalarFunction.__init_subclass__
# introspects compute() annotations at class definition time to determine
# Arrow types. The shared logic is in _type_info_result().
# ============================================================================


def _type_info_result(label: str, v: pa.Array) -> pa.StringArray:  # type: ignore[type-arg]
    """Shared compute logic for all type_info overloads."""
    return pa.array([label if x is not None else None for x in v.to_pylist()], type=pa.string())


class TypeInfoInt32Function(ScalarFunction):
    """Return type name for int32 input."""

    class Meta:
        """Function metadata."""

        name = "type_info"
        description = "Return type name for int32 input"

    @classmethod
    def compute(
        cls,
        v: Annotated[pa.Int32Array, Param(doc="Input value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'int32' for each row."""
        return _type_info_result("int32", v)


class TypeInfoInt64Function(ScalarFunction):
    """Return type name for int64 input."""

    class Meta:
        """Function metadata."""

        name = "type_info"
        description = "Return type name for int64 input"

    @classmethod
    def compute(
        cls,
        v: Annotated[pa.Int64Array, Param(doc="Input value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'int64' for each row."""
        return _type_info_result("int64", v)


class TypeInfoUInt32Function(ScalarFunction):
    """Return type name for uint32 input."""

    class Meta:
        """Function metadata."""

        name = "type_info"
        description = "Return type name for uint32 input"

    @classmethod
    def compute(
        cls,
        v: Annotated[pa.UInt32Array, Param(doc="Input value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'uint32' for each row."""
        return _type_info_result("uint32", v)


class TypeInfoUInt64Function(ScalarFunction):
    """Return type name for uint64 input."""

    class Meta:
        """Function metadata."""

        name = "type_info"
        description = "Return type name for uint64 input"

    @classmethod
    def compute(
        cls,
        v: Annotated[pa.UInt64Array, Param(doc="Input value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'uint64' for each row."""
        return _type_info_result("uint64", v)


class TypeInfoStringFunction(ScalarFunction):
    """Return type name for string input."""

    class Meta:
        """Function metadata."""

        name = "type_info"
        description = "Return type name for string input"

    @classmethod
    def compute(
        cls,
        v: Annotated[pa.StringArray, Param(doc="Input value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'varchar' for each row."""
        return _type_info_result("varchar", v)


# ============================================================================
# smart_format — overloaded scalar function (2 overloads by ConstParam type)
# ============================================================================


class SmartFormatWidthFunction(ScalarFunction):
    """Right-align a double in a field of given width.

    Overload with int ConstParam.
    """

    class Meta:
        """Function metadata."""

        name = "smart_format"
        description = "Right-align value in field of given width"

    @classmethod
    def compute(
        cls,
        width: Annotated[int, ConstParam("Field width")],
        value: Annotated[pa.DoubleArray, Param(doc="Value to format")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Right-align value in field of given width."""
        return pa.array(
            [f"{v:>{width}}" if v is not None else None for v in value.to_pylist()],
            type=pa.string(),
        )


class SmartFormatPrefixFunction(ScalarFunction):
    """Prepend a prefix string to a formatted double.

    Overload with str ConstParam.
    """

    class Meta:
        """Function metadata."""

        name = "smart_format"
        description = "Prepend prefix to formatted value"

    @classmethod
    def compute(
        cls,
        prefix: Annotated[str, ConstParam("Prefix string")],
        value: Annotated[pa.DoubleArray, Param(doc="Value to format")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Prepend prefix to formatted value."""
        return pa.array(
            [f"{prefix}{v}" if v is not None else None for v in value.to_pylist()],
            type=pa.string(),
        )


# ============================================================================
# pair_type — overloaded scalar function (3 overloads by multi-column type)
#
# Each overload needs separate annotations for dispatch. Shared logic in
# _pair_type_result().
# ============================================================================


def _pair_type_result(label: str, a: pa.Array, b: pa.Array) -> pa.StringArray:  # type: ignore[type-arg]
    """Shared compute logic for all pair_type overloads."""
    return pa.array(
        [
            label if (x is not None and y is not None) else None
            for x, y in zip(a.to_pylist(), b.to_pylist(), strict=True)
        ],
        type=pa.string(),
    )


class PairTypeIntIntFunction(ScalarFunction):
    """Return 'int+int' for two int64 columns."""

    class Meta:
        """Function metadata."""

        name = "pair_type"
        description = "Return type pair name for int+int"

    @classmethod
    def compute(
        cls,
        a: Annotated[pa.Int64Array, Param(doc="First value")],
        b: Annotated[pa.Int64Array, Param(doc="Second value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'int+int' for each row."""
        return _pair_type_result("int+int", a, b)


class PairTypeStrStrFunction(ScalarFunction):
    """Return 'str+str' for two string columns."""

    class Meta:
        """Function metadata."""

        name = "pair_type"
        description = "Return type pair name for str+str"

    @classmethod
    def compute(
        cls,
        a: Annotated[pa.StringArray, Param(doc="First value")],
        b: Annotated[pa.StringArray, Param(doc="Second value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'str+str' for each row."""
        return _pair_type_result("str+str", a, b)


class PairTypeIntStrFunction(ScalarFunction):
    """Return 'int+str' for int64 + string columns."""

    class Meta:
        """Function metadata."""

        name = "pair_type"
        description = "Return type pair name for int+str"

    @classmethod
    def compute(
        cls,
        a: Annotated[pa.Int64Array, Param(doc="First value")],
        b: Annotated[pa.StringArray, Param(doc="Second value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'int+str' for each row."""
        return _pair_type_result("int+str", a, b)


# ============================================================================
# concat_values — overloaded scalar function (2 overloads by varargs column type)
# ============================================================================


class ConcatValuesIntFunction(ScalarFunction):
    """Concatenate integer column values as their string sum.

    Varargs overload for integer columns: sums all values per row and
    returns the result as a string.

    Example:
        SQL:    SELECT concat_values(a, b, c) FROM t
        Input:  a=[1, 2], b=[10, 20], c=[100, 200]
        Output: result=['111', '222']

    """

    class Meta:
        """Function metadata."""

        name = "concat_values"
        description = "Sum integer varargs and return as string"

    @classmethod
    def on_bind(cls, params: BindParameters) -> BindResult:
        """Output is always string."""
        return BindResult(pa.string())

    @classmethod
    def compute(
        cls,
        values: Annotated[
            list[pa.Int64Array],
            Param(doc="Integer values to sum", varargs=True),
        ],
    ) -> Annotated[pa.Array[Any], Returns()]:
        """Sum all integer columns and return as string."""
        result: pa.Array[Any] = values[0]
        for val in values[1:]:
            result = pc.add(result, val)
        return pc.cast(result, pa.string())


class ConcatValuesStrFunction(ScalarFunction):
    """Concatenate string column values.

    Varargs overload for string columns: concatenates all string values per row.

    Example:
        SQL:    SELECT concat_values(a, b) FROM t
        Input:  a=['hello', 'foo'], b=[' world', 'bar']
        Output: result=['hello world', 'foobar']

    """

    class Meta:
        """Function metadata."""

        name = "concat_values"
        description = "Concatenate string varargs"

    @classmethod
    def on_bind(cls, params: BindParameters) -> BindResult:
        """Output is always string."""
        return BindResult(pa.string())

    @classmethod
    def compute(
        cls,
        values: Annotated[
            list[pa.StringArray],
            Param(doc="String values to concatenate", varargs=True),
        ],
    ) -> Annotated[pa.Array[Any], Returns()]:
        """Concatenate all string columns."""
        result: pa.StringArray = values[0]
        for val in values[1:]:
            result = pc.binary_join_element_wise(result, val, "")  # type: ignore[call-overload]
        return result


# ============================================================================
# any_mixed — overloaded scalar function (2 overloads, AnyArrow + fixed type)
# ============================================================================


class AnyMixedIntFunction(ScalarFunction):
    """AnyArrow first param, Int64 second param."""

    class Meta:
        """Function metadata."""

        name = "any_mixed"
        description = "Any+int dispatch"

    @classmethod
    def compute(
        cls,
        a: Annotated[pa.Array, Param(doc="Any type value")],  # type: ignore[type-arg]
        b: Annotated[pa.Int64Array, Param(doc="Int value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'any+int: {b}' for each row."""
        return pa.array(
            [f"any+int: {y}" if y is not None else None for y in b.to_pylist()],
            type=pa.string(),
        )


class AnyMixedStrFunction(ScalarFunction):
    """AnyArrow first param, String second param."""

    class Meta:
        """Function metadata."""

        name = "any_mixed"
        description = "Any+str dispatch"

    @classmethod
    def compute(
        cls,
        a: Annotated[pa.Array, Param(doc="Any type value")],  # type: ignore[type-arg]
        b: Annotated[pa.StringArray, Param(doc="String value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'any+str: {b}' for each row."""
        return pa.array(
            [f"any+str: {y}" if y is not None else None for y in b.to_pylist()],
            type=pa.string(),
        )


# ============================================================================
# Geo functions — Complex Arrow types (struct, list, fixed-size list)
# ============================================================================

_POINT_STRUCT_TYPE = pa.struct([("lat", pa.float64()), ("lon", pa.float64())])


def _euclidean_distance(
    lat1: pa.Array[Any], lon1: pa.Array[Any], lat2: pa.Array[Any], lon2: pa.Array[Any]
) -> pa.DoubleArray:
    """Compute Euclidean distance: sqrt((lat2-lat1)^2 + (lon2-lon1)^2)."""
    dlat = pc.subtract(lat2, lat1)
    dlon = pc.subtract(lon2, lon1)
    return pc.sqrt(pc.add(pc.multiply(dlat, dlat), pc.multiply(dlon, dlon)))  # type: ignore[return-value]


def _compute_centroid(lat_arrays: list[pa.Array[Any]], lon_arrays: list[pa.Array[Any]]) -> pa.StructArray:
    """Compute centroid (average lat, average lon) from parallel lat/lon arrays."""
    n = len(lat_arrays)
    lat_sum: pa.Array[Any] = lat_arrays[0]
    lon_sum: pa.Array[Any] = lon_arrays[0]
    for i in range(1, n):
        lat_sum = pc.add(lat_sum, lat_arrays[i])
        lon_sum = pc.add(lon_sum, lon_arrays[i])
    divisor = pa.scalar(n, type=pa.float64())
    avg_lat = pc.divide(lat_sum, divisor)
    avg_lon = pc.divide(lon_sum, divisor)
    return pa.StructArray.from_arrays([avg_lat, avg_lon], names=["lat", "lon"])


class GeoDistanceStructFunction(ScalarFunction):
    """Euclidean distance between two struct points.

    Each point is a struct with lat and lon fields.

    Example:
        SQL:    SELECT geo_distance_struct(p1, p2) FROM points
        Input:  p1={lat: 0.0, lon: 0.0}, p2={lat: 3.0, lon: 4.0}
        Output: result=5.0

    """

    class Meta:
        """Function metadata."""

        name = "geo_distance_struct"
        description = "Euclidean distance between two struct points"
        examples = [
            FunctionExample(
                sql="SELECT geo_distance_struct({lat: 0, lon: 0}, {lat: 3, lon: 4})",
                description="Distance between origin and (3, 4)",
            ),
        ]

    @classmethod
    def compute(
        cls,
        p1: Annotated[
            pa.StructArray,
            Param(doc="First point {lat, lon}", arrow_type=_POINT_STRUCT_TYPE),
        ],
        p2: Annotated[
            pa.StructArray,
            Param(doc="Second point {lat, lon}", arrow_type=_POINT_STRUCT_TYPE),
        ],
    ) -> Annotated[pa.DoubleArray, Returns()]:
        """Compute Euclidean distance between two points."""
        return _euclidean_distance(p1.field("lat"), p1.field("lon"), p2.field("lat"), p2.field("lon"))


class GeoDistanceListFunction(ScalarFunction):
    """Euclidean distance between two list points.

    Each point is a list of two float64 values [lat, lon].

    Example:
        SQL:    SELECT geo_distance_list(p1, p2) FROM points
        Input:  p1=[0.0, 0.0], p2=[3.0, 4.0]
        Output: result=5.0

    """

    class Meta:
        """Function metadata."""

        name = "geo_distance_list"
        description = "Euclidean distance between two list points"
        examples = [
            FunctionExample(
                sql="SELECT geo_distance_list([0, 0], [3, 4])",
                description="Distance between origin and (3, 4)",
            ),
        ]

    @classmethod
    def compute(
        cls,
        p1: Annotated[  # type: ignore[type-arg]
            pa.ListArray,
            Param(doc="First point [lat, lon]", arrow_type=pa.list_(pa.float64())),
        ],
        p2: Annotated[  # type: ignore[type-arg]
            pa.ListArray,
            Param(doc="Second point [lat, lon]", arrow_type=pa.list_(pa.float64())),
        ],
    ) -> Annotated[pa.DoubleArray, Returns()]:
        """Compute Euclidean distance between two points."""
        return _euclidean_distance(
            pc.list_element(p1, 0),
            pc.list_element(p1, 1),
            pc.list_element(p2, 0),
            pc.list_element(p2, 1),
        )


class GeoDistanceFixedFunction(ScalarFunction):
    """Euclidean distance between two fixed-size list points.

    Each point is a fixed-size list of two float64 values [lat, lon].

    Example:
        SQL:    SELECT geo_distance_fixed(p1, p2) FROM points
        Input:  p1=[0.0, 0.0], p2=[3.0, 4.0]
        Output: result=5.0

    """

    class Meta:
        """Function metadata."""

        name = "geo_distance_fixed"
        description = "Euclidean distance between two fixed-size list points"
        examples = [
            FunctionExample(
                sql="SELECT geo_distance_fixed([0, 0], [3, 4])",
                description="Distance between origin and (3, 4)",
            ),
        ]

    @classmethod
    def compute(
        cls,
        p1: Annotated[  # type: ignore[type-arg]
            pa.FixedSizeListArray,
            Param(doc="First point [lat, lon]", arrow_type=pa.list_(pa.float64(), 2)),
        ],
        p2: Annotated[  # type: ignore[type-arg]
            pa.FixedSizeListArray,
            Param(doc="Second point [lat, lon]", arrow_type=pa.list_(pa.float64(), 2)),
        ],
    ) -> Annotated[pa.DoubleArray, Returns()]:
        """Compute Euclidean distance between two points."""
        return _euclidean_distance(
            pc.list_element(p1, 0),
            pc.list_element(p1, 1),
            pc.list_element(p2, 0),
            pc.list_element(p2, 1),
        )


class GeoCentroidStructFunction(ScalarFunction):
    """Centroid of N struct points (varargs).

    Computes the average lat and average lon across all input point columns.

    Example:
        SQL:    SELECT geo_centroid_struct(p1, p2) FROM points
        Input:  p1={lat: 0.0, lon: 0.0}, p2={lat: 4.0, lon: 6.0}
        Output: result={lat: 2.0, lon: 3.0}

    """

    class Meta:
        """Function metadata."""

        name = "geo_centroid_struct"
        description = "Centroid of N struct points"
        examples = [
            FunctionExample(
                sql="SELECT geo_centroid_struct(p1, p2) FROM points",
                description="Compute centroid of two struct points",
            ),
        ]

    @classmethod
    def compute(
        cls,
        points: Annotated[
            list[pa.StructArray],
            Param(
                doc="Point columns {lat, lon}",
                arrow_type=_POINT_STRUCT_TYPE,
                varargs=True,
            ),
        ],
    ) -> Annotated[pa.StructArray, Returns(arrow_type=_POINT_STRUCT_TYPE)]:
        """Compute centroid of all points."""
        return _compute_centroid(
            [p.field("lat") for p in points],
            [p.field("lon") for p in points],
        )


class GeoCentroidListFunction(ScalarFunction):
    """Centroid of N list points (varargs).

    Computes the average lat and average lon across all input point columns,
    where each point is a list of [lat, lon].

    Example:
        SQL:    SELECT geo_centroid_list(p1, p2) FROM points
        Input:  p1=[0.0, 0.0], p2=[4.0, 6.0]
        Output: result={lat: 2.0, lon: 3.0}

    """

    class Meta:
        """Function metadata."""

        name = "geo_centroid_list"
        description = "Centroid of N list points"
        examples = [
            FunctionExample(
                sql="SELECT geo_centroid_list(p1, p2) FROM points",
                description="Compute centroid of two list points",
            ),
        ]

    @classmethod
    def compute(
        cls,
        points: Annotated[  # type: ignore[type-arg]
            list[pa.ListArray],
            Param(
                doc="Point columns [lat, lon]",
                arrow_type=pa.list_(pa.float64()),
                varargs=True,
            ),
        ],
    ) -> Annotated[pa.StructArray, Returns(arrow_type=_POINT_STRUCT_TYPE)]:
        """Compute centroid of all points."""
        return _compute_centroid(
            [pc.list_element(p, 0) for p in points],
            [pc.list_element(p, 1) for p in points],
        )


class GeoCentroidFixedFunction(ScalarFunction):
    """Centroid of N fixed-size list points (varargs).

    Computes the average lat and average lon across all input point columns,
    where each point is a fixed-size list of [lat, lon].

    Example:
        SQL:    SELECT geo_centroid_fixed(p1, p2) FROM points
        Input:  p1=[0.0, 0.0], p2=[4.0, 6.0]
        Output: result={lat: 2.0, lon: 3.0}

    """

    class Meta:
        """Function metadata."""

        name = "geo_centroid_fixed"
        description = "Centroid of N fixed-size list points"
        examples = [
            FunctionExample(
                sql="SELECT geo_centroid_fixed(p1, p2) FROM points",
                description="Compute centroid of two fixed-size list points",
            ),
        ]

    @classmethod
    def compute(
        cls,
        points: Annotated[  # type: ignore[type-arg]
            list[pa.FixedSizeListArray],
            Param(
                doc="Point columns [lat, lon]",
                arrow_type=pa.list_(pa.float64(), 2),
                varargs=True,
            ),
        ],
    ) -> Annotated[pa.StructArray, Returns(arrow_type=_POINT_STRUCT_TYPE)]:
        """Compute centroid of all points."""
        return _compute_centroid(
            [pc.list_element(p, 0) for p in points],
            [pc.list_element(p, 1) for p in points],
        )


class WhoAmIFunction(ScalarFunction):
    """Return the authenticated principal name.

    Demonstrates the Auth annotation for accessing auth context in compute().
    Over stdio transport (or when no auth is configured), returns "anonymous".

    SQL: ``SELECT whoami(1)``
    """

    class Meta:
        """Metadata for the whoami function."""

        name = "whoami"

    @classmethod
    def compute(
        cls,
        x: Annotated[pa.Int64Array, Param(doc="dummy input")],
        auth: Annotated[AuthContext, Auth()],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return the authenticated principal name."""
        name = auth.principal or "anonymous"
        return pa.array([name] * len(x))
