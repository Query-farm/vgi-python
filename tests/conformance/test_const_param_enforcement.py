# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance test: ConstParam value constraints are enforced at bind.

A ``ConstParam`` may declare ``choices``/``ge``/``le``/``gt``/``lt``/``pattern``.
Those are surfaced for agent discovery (via ``vgi_function_arguments()``) AND
must be enforced: a const value that violates a declared constraint fails at
bind with ``ArgumentValidationError``, rather than silently reaching
``compute()``. This mirrors the legacy ``Arg`` descriptor path.
"""

from __future__ import annotations

from typing import Annotated

import pyarrow as pa
import pytest

from vgi.arguments import Arguments, ArgumentValidationError, ConstParam, Param, Returns
from vgi.protocol import BindRequest, FunctionType
from vgi.scalar_function import BindResult, ScalarFunction


class _Measure(ScalarFunction):
    class Meta:
        name = "measure"

    @classmethod
    def on_bind(cls, params: object) -> BindResult:
        return BindResult(pa.string())

    @classmethod
    def compute(
        cls,
        unit: Annotated[str, ConstParam("Output unit", choices=["mm", "cm", "m"])],
        precision: Annotated[int, ConstParam("Decimals", ge=0, le=10)],
        value: Annotated[pa.DoubleArray, Param(doc="Value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        return pa.array([str(value)], type=pa.string())


def _bind(unit: str, precision: int) -> None:
    _Measure.bind(
        BindRequest(
            function_name="measure",
            arguments=Arguments(positional=(pa.scalar(unit), pa.scalar(precision))),
            function_type=FunctionType.SCALAR,
            input_schema=pa.schema([("value", pa.float64())]),
        )
    )


def test_valid_const_values_bind() -> None:
    """A const value inside every declared constraint binds cleanly."""
    _bind("cm", 2)


def test_choice_violation_raises_at_bind() -> None:
    """A const value outside the choice set fails at bind."""
    with pytest.raises(ArgumentValidationError) as exc:
        _bind("xx", 2)
    assert "unit" in str(exc.value)


def test_range_violation_raises_at_bind() -> None:
    """A const value outside the numeric range fails at bind."""
    with pytest.raises(ArgumentValidationError) as exc:
        _bind("cm", 99)
    assert "precision" in str(exc.value)
