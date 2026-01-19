"""Example Polars scalar function implementations.

This module provides example scalar functions that demonstrate the
PolarsScalarFunction expression-based API with zero-copy Arrow conversion.

Quick Start
-----------
Create a Polars scalar function in three steps:

1. Declare parameters as class attributes with ``Annotated[type, Param(...)]``
2. Specify output type in ``Meta.output_type``
3. Implement ``compute_polars()`` returning a ``pl.Expr``

Example::

    from typing import Annotated
    import polars as pl
    from vgi import PolarsScalarFunction, Param

    class MyFunction(PolarsScalarFunction):
        text: Annotated[pl.Utf8, Param(position=0, doc="Input text")]

        class Meta:
            output_type = pl.Utf8

        def compute_polars(self) -> pl.Expr:
            return pl.col("text").str.to_uppercase()

Key Concepts
------------
**Expression-based API**: ``compute_polars()`` returns a ``pl.Expr``, enabling
Polars' lazy evaluation and query optimization.

**Named column references**: Columns are referenced by their declared param
names using ``pl.col("param_name")``, regardless of actual input column names.

**Automatic type conversion**: Input/output conversion between Arrow and Polars
is handled automatically with zero-copy where possible.

Example Functions
-----------------
Static Output Type:
    - ``PolarsUpperCaseFunction`` - String to uppercase
    - ``PolarsStringLengthFunction`` - Character count

Multiple Arguments:
    - ``PolarsAddValuesFunction`` - Add two columns
    - ``PolarsMultiplyFunction`` - Multiply by constant

Variable Arguments:
    - ``PolarsSumValuesFunction`` - Sum any number of columns

Dynamic Output Type:
    - ``PolarsDoubleFunction`` - Double values, preserving input type
"""

from __future__ import annotations

from typing import Annotated, Any

import polars as pl
import pyarrow.types as pat

from vgi.arguments import ConstParam, Param
from vgi.metadata import FunctionExample
from vgi.scalar_function_polars import AnyPolars, PolarsScalarFunction

__all__ = [
    "PolarsAddValuesFunction",
    "PolarsDoubleFunction",
    "PolarsMultiplyFunction",
    "PolarsStringLengthFunction",
    "PolarsSumValuesFunction",
    "PolarsUpperCaseFunction",
]


class PolarsUpperCaseFunction(PolarsScalarFunction):
    """Convert string values to uppercase using Polars.

    This is the simplest example of a Polars scalar function, demonstrating:

    - Single parameter declaration with ``Annotated[pl.Utf8, Param(...)]``
    - Static output type in ``Meta.output_type``
    - Expression-based ``compute_polars()`` returning ``pl.Expr``

    The parameter name ``text`` becomes the column reference in the expression.

    Attributes:
        text: Input string column to convert.

    SQL Usage:
        ``SELECT polars_upper_case(name) FROM users``

    Example:
        >>> input: ["alice", "bob", "charlie"]
        >>> output: ["ALICE", "BOB", "CHARLIE"]

    """

    text: Annotated[pl.Utf8, Param(position=0, doc="String value to uppercase")]

    class Meta:
        """Function metadata."""

        name = "polars_upper_case"
        description = "Converts string values to uppercase using Polars"
        output_type = pl.Utf8
        examples = [
            FunctionExample(
                sql="SELECT polars_upper_case(name) FROM users",
                description="Convert user names to uppercase using Polars",
            ),
        ]

    def compute_polars(self) -> pl.Expr:
        """Convert the string values to uppercase using Polars."""
        return pl.col("text").str.to_uppercase()


class PolarsStringLengthFunction(PolarsScalarFunction):
    """Compute the character length of strings using Polars.

    Demonstrates using Polars string methods in expressions. The output type
    differs from the input type (Utf8 -> UInt32).

    Attributes:
        text: Input string column.

    SQL Usage:
        ``SELECT polars_string_length(description) FROM products``

    Example:
        >>> input: ["hello", "hi", "goodbye"]
        >>> output: [5, 2, 7]

    """

    text: Annotated[pl.Utf8, Param(position=0, doc="String value")]

    class Meta:
        """Function metadata."""

        name = "polars_string_length"
        description = "Computes string lengths using Polars"
        output_type = pl.UInt32
        examples = [
            FunctionExample(
                sql="SELECT polars_string_length(description) FROM products",
                description="Get length of product descriptions",
            ),
        ]

    def compute_polars(self) -> pl.Expr:
        """Compute string lengths using Polars."""
        return pl.col("text").str.len_chars()


# =============================================================================
# Multiple Arguments Examples
# =============================================================================


class PolarsAddValuesFunction(PolarsScalarFunction):
    """Add two numeric columns together using Polars.

    Demonstrates multiple parameters at different positions. Each parameter
    is declared with its own ``position`` index and can be referenced by
    name in the expression.

    Attributes:
        left: First numeric column (position 0).
        right: Second numeric column (position 1).

    SQL Usage:
        ``SELECT polars_add_values(price, tax) FROM orders``

    Example:
        >>> input: left=[10.0, 20.0, 30.0], right=[1.0, 2.0, 3.0]
        >>> output: [11.0, 22.0, 33.0]

    """

    left: Annotated[pl.Float64, Param(position=0, doc="First value")]
    right: Annotated[pl.Float64, Param(position=1, doc="Second value")]

    class Meta:
        """Function metadata."""

        name = "polars_add_values"
        description = "Adds two numeric values together using Polars"
        output_type = pl.Float64
        examples = [
            FunctionExample(
                sql="SELECT polars_add_values(price, tax) FROM orders",
                description="Calculate total by adding price and tax",
            ),
        ]

    def compute_polars(self) -> pl.Expr:
        """Add the two values together."""
        return pl.col("left") + pl.col("right")


class PolarsMultiplyFunction(PolarsScalarFunction):
    """Multiply a numeric column by a constant factor using Polars.

    Demonstrates combining a column parameter with a constant (scalar) argument.
    Use ``ConstParam`` to declare constant arguments that appear in function
    signatures and metadata.

    This pattern is useful when you need a user-specified value that doesn't
    come from a table column (e.g., scaling factor, threshold, limit).

    Attributes:
        value: Numeric column to multiply (position 0 in input batch).
        factor: Constant multiplication factor (position 0 in function arguments).

    SQL Usage:
        ``SELECT polars_multiply(price, 1.1) FROM products``

    Example:
        >>> input: value=[10.0, 20.0, 30.0], factor=2.0
        >>> output: [20.0, 40.0, 60.0]

    Note:
        The ``factor`` is a constant argument passed from SQL, not a table column.
        It's declared with ``ConstParam(position=0)`` to indicate it's the first
        positional argument in the function call.

    """

    # Column binding: maps input column at position 0 to "value" in expression
    value: Annotated[pl.Float64, Param(position=0, doc="Value to multiply")]

    # ConstParam declaration for metadata extraction (tells catalog about the argument).
    # The actual value is accessed via _factor property below since class-level
    # Annotated declarations are type hints only, not instance attributes.
    factor: Annotated[float, ConstParam("Multiplication factor", position=0)]

    class Meta:
        """Function metadata."""

        name = "polars_multiply"
        description = "Multiplies a numeric value by a constant factor"
        output_type = pl.Float64
        examples = [
            FunctionExample(
                sql="SELECT polars_multiply(price, 1.1) FROM products",
                description="Apply 10% markup to prices",
            ),
        ]

    @property
    def _factor(self) -> float:
        """Get the multiplication factor from arguments."""
        return self.invocation.arguments.positional[0].as_py()  # type: ignore

    def compute_polars(self) -> pl.Expr:
        """Multiply the values by the constant factor."""
        return pl.col("value") * self._factor


# =============================================================================
# Varargs Example
# =============================================================================


class PolarsSumValuesFunction(PolarsScalarFunction):
    """Sum any number of numeric columns row-wise using Polars.

    Demonstrates the varargs pattern with ``Param(varargs=True)``. When
    varargs is enabled, all columns from the specified position onward
    are collected and renamed to ``{name}_0``, ``{name}_1``, etc.

    Use ``pl.col("^values_.*$")`` regex pattern to match all vararg columns
    in the expression. The ``pl.sum_horizontal()`` function sums across
    columns for each row.

    Attributes:
        values: Variable number of numeric columns to sum.

    SQL Usage:
        ``SELECT polars_sum_values(price, tax, shipping) FROM orders``

    Example:
        >>> input: a=[1.0, 2.0], b=[10.0, 20.0], c=[100.0, 200.0]
        >>> output: [111.0, 222.0]

    Note:
        Column renaming: Input columns become ``values_0``, ``values_1``, etc.
        The regex ``^values_.*$`` matches all of them.

    """

    values: Annotated[pl.Float64, Param(position=0, doc="Values to sum", varargs=True)]

    class Meta:
        """Function metadata."""

        name = "polars_sum_values"
        description = "Sums multiple numeric values using Polars"
        output_type = pl.Float64
        examples = [
            FunctionExample(
                sql="SELECT polars_sum_values(price, tax, shipping) FROM orders",
                description="Calculate total from multiple cost values",
            ),
        ]

    def compute_polars(self) -> pl.Expr:
        """Sum all input values using sum_horizontal."""
        # With varargs, columns are renamed to values_0, values_1, etc.
        # Use regex to match all vararg columns
        return pl.sum_horizontal(pl.col("^values_.*$"))


# =============================================================================
# Dynamic Output Type Example
# =============================================================================


class PolarsDoubleFunction(PolarsScalarFunction):
    """Double numeric values while preserving the input type.

    Demonstrates dynamic output type using ``AnyPolars`` and ``type_bound``:

    - ``Meta.output_type = AnyPolars`` signals the output type is dynamic
    - ``type_bound=[pat.is_integer, pat.is_floating]`` constrains input
      to numeric types (validated at bind time with clear error messages)
    - ``output_polars_type`` property returns the actual output type
      based on input schema

    This pattern is essential when writing generic functions that work with
    multiple numeric types and should preserve the input precision.

    Attributes:
        value: Numeric column to double (accepts int or float types).

    Properties:
        output_polars_type: Returns input type to preserve it in output.

    SQL Usage:
        ``SELECT polars_double(quantity) FROM inventory``

    Example:
        >>> input: [1, 2, 3] (Int64)
        >>> output: [2, 4, 6] (Int64 - same type preserved)

        >>> input: [1.5, 2.5, 3.5] (Float64)
        >>> output: [3.0, 5.0, 7.0] (Float64 - same type preserved)

    Note:
        Type bound validation happens at bind time. If you pass a string
        column, you'll get a clear error: "Column 'value' has type string,
        but type_bound requires: is_integer, is_floating"

    """

    value: Annotated[
        Any,
        Param(
            position=0,
            doc="Numeric value to double",
            type_bound=[pat.is_integer, pat.is_floating],
        ),
    ]

    class Meta:
        """Function metadata."""

        name = "polars_double"
        description = "Doubles numeric values, preserving input type"
        output_type = AnyPolars
        examples = [
            FunctionExample(
                sql="SELECT polars_double(quantity) FROM inventory",
                description="Double inventory quantities",
            ),
        ]

    @property
    def output_polars_type(self) -> pl.DataType:
        """Return the input value's type as the output type."""
        # Get the type of the first input column
        return self.polars_schema[self.input_schema.field(0).name]

    def compute_polars(self) -> pl.Expr:
        """Double the values, preserving the input type."""
        return pl.col("value") * 2
