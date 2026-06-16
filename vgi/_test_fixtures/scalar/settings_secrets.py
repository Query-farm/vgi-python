# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Setting/secret/auth-aware scalar fixtures (multiply_by_setting, return_secret_value, who_am_i)."""

from __future__ import annotations

import json
from typing import Annotated, Any

import pyarrow as pa
import pyarrow.compute as pc

from vgi.arguments import Auth, OutputLength, Param, Returns, Secret, Setting
from vgi.auth import AuthContext
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction


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


class ScaleBySettingFunction(ScalarFunction):
    """Scale the input value by the float (DOUBLE) setting ``scale_factor``.

    Companion to :class:`MultiplyBySettingFunction`, but reads a floating-point
    setting rather than an integer one.

    Example:
        SQL:    SELECT scale_by_setting(4.0)

    """

    class Meta:
        """Function metadata."""

        name = "scale_by_setting"
        description = "Scale the input value by the float setting `scale_factor`"
        examples = [
            FunctionExample(
                sql="SELECT scale_by_setting(4.0)",
                description="Scale the input value by the float setting's value",
            ),
        ]

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.DoubleArray, Param(doc="Value to scale")],
        scale_factor: Annotated[pa.Scalar[Any] | None, Setting()],
    ) -> Annotated[pa.DoubleArray, Returns()]:
        """Generate the result for each row."""
        factor = 1.0 if scale_factor is None or scale_factor.as_py() is None else scale_factor.as_py()
        return pc.multiply(pa.scalar(factor, type=pa.float64()), value)


class SecretFieldFunction(ScalarFunction):
    """Look up individual secret fields by name.

    ``port`` is read by named lookup on the ``vgi_example`` secret and
    ``secret_string`` by field name; the result mirrors the wire behaviour of
    the worker-side named/positional secret field accessors.

    Example:
        SQL:    SELECT secret_field()

    """

    class Meta:
        """Function metadata."""

        name = "secret_field"
        description = "Look up secret fields by name"
        examples = [
            FunctionExample(
                sql="SELECT secret_field()",
                description="Look up secret fields by name",
            ),
        ]

    @classmethod
    def compute(
        cls,
        vgi_example: Annotated[dict[str, pa.Scalar[Any]], Secret("vgi_example")],
        _length: Annotated[int, OutputLength()],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Generate the result for each row."""
        port = vgi_example.get("port")
        name = vgi_example.get("secret_string")
        port_s = "" if port is None else str(port.as_py())
        name_s = "" if name is None else str(name.as_py())
        result = f"port={port_s};name={name_s}"
        return pa.array([result for _ in range(_length)], type=pa.string())


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
