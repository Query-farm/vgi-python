"""Arithmetic scalar fixtures (multiply, double, add, sum, concat)."""

from __future__ import annotations

from typing import Annotated, Any

import pyarrow as pa
import pyarrow.compute as pc

from vgi._test_fixtures.scalar._common import _is_addable_type, _is_multipliable_type, _promote_for_addition
from vgi.arguments import ConstParam, Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import BindParameters, BindResult, ScalarFunction


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
            Param(doc="Numeric value to double", type_bound=_is_multipliable_type),
        ],
    ) -> Annotated[pa.Array[Any], Returns()]:
        """Double the input values."""
        if pa.types.is_decimal(value.type):
            # pc.multiply on decimals follows the SQL rule
            # decimal(p1,s1) * decimal(p2,s2) -> decimal(p1+p2+1, s1+s2);
            # multiplying by the literal 2 (decimal128(19, 0)) blows past
            # decimal128's 38-digit cap for any input wider than ~18 digits.
            # Compute `value + value` (which only adds 1 to precision) and
            # cast the result to the declared output type. We do the add at
            # the input precision, then cast — for inputs at the 38-digit
            # cap we need decimal256 just to hold the +1 intermediate.
            in_p, in_s = value.type.precision, value.type.scale
            work_type = pa.decimal256(in_p, in_s) if in_p >= 38 else value.type
            casted = pc.cast(value, work_type) if work_type != value.type else value
            summed: pa.Array[Any] = pc.add(casted, casted)  # decimal(p+1, s)
            out_type = _promote_for_addition(value.type)
            if summed.type == out_type:
                return summed
            return pc.cast(summed, out_type)
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
