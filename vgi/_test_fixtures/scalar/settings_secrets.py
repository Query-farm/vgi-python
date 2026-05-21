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
