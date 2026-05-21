# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Null-handling and conditional-message scalar fixtures."""

from __future__ import annotations

from typing import Annotated

import pyarrow as pa
import pyarrow.compute as pc

from vgi.arguments import ConstParam, Param, Returns
from vgi.metadata import FunctionExample, NullHandling
from vgi.scalar_function import ScalarFunction


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
