# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Number/string formatting scalar fixtures (format_number_*, smart_format_*)."""

from __future__ import annotations

from typing import Annotated

import pyarrow as pa

from vgi.arguments import ConstParam, Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction


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
        precision: Annotated[int, ConstParam("Number of decimal places", ge=0, le=10)],
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
        precision: Annotated[int, ConstParam("Number of decimal places", ge=0, le=10)],
        prefix: Annotated[str, ConstParam("Prefix string")],
        value: Annotated[pa.DoubleArray, Param(doc="Number to format")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Format each value with prefix and specified precision."""
        return pa.array(
            [f"{prefix}{v:.{precision}f}" if v is not None else None for v in value.to_pylist()],
            type=pa.string(),
        )


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
