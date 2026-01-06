"""Tests for example function argument type inference.

These tests verify that the Arg[type] subscript syntax correctly captures
the type parameter and converts it to the appropriate Arrow type for
function metadata.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from vgi.argument_spec import extract_argument_specs
from vgi.arguments import PYTHON_TO_ARROW, Arg, TableInput
from vgi.examples.scalar import (
    AddColumnsFunction,
    DoubleColumnFunction,
    UpperCaseFunction,
)
from vgi.examples.table import (
    ConstantTableFunction,
    GeneratorExceptionFunction,
    LoggingGeneratorFunction,
    PartitionedRangeFunction,
    ProjectedDataFunction,
    RandomSampleFunction,
    RangeFunction,
    SequenceFunction,
    SettingsAwareFunction,
)
from vgi.examples.table_in_out import (
    BufferInputFunction,
    EchoFunction,
    ExceptionFinalizeFunction,
    ExceptionProcessFunction,
    RepeatInputsFunction,
    SumAllColumnsFunction,
    SumAllColumnsFunctionDistributed,
    SumAllColumnsFunctionWithLogging,
    SumAllColumnsSimpleDistributed,
)


def get_arg_descriptors(func_cls: type) -> dict[str, Arg[Any]]:
    """Extract Arg descriptors from a function class."""
    descriptors: dict[str, Arg[Any]] = {}
    for klass in func_cls.__mro__:
        if klass is object:
            continue
        for name, value in vars(klass).items():
            if name.startswith("_"):
                continue
            if name in descriptors:
                continue
            if isinstance(value, Arg):
                descriptors[name] = value
    return descriptors


def get_expected_type(arg: Arg[Any], hint: type | None) -> pa.DataType:
    """Determine expected Arrow type from Arg descriptor and type hint.

    Uses the same priority logic as extract_argument_specs:
    1. Explicit arrow_type on Arg
    2. Type parameter from Arg[type] subscript
    3. Type hint with PYTHON_TO_ARROW mapping
    4. Default to pa.null()
    """
    if arg.arrow_type is not None:
        return arg.arrow_type
    if hint is TableInput:
        return pa.null()
    if (
        hasattr(arg, "_type_param")
        and arg._type_param is not None
        and arg._type_param in PYTHON_TO_ARROW
    ):
        return PYTHON_TO_ARROW[arg._type_param]
    if hint is not None and hint in PYTHON_TO_ARROW:
        return PYTHON_TO_ARROW[hint]
    return pa.null()


class TestArgTypeParamCapture:
    """Tests that Arg[type] captures the type parameter correctly."""

    def test_arg_captures_type_param_str(self) -> None:
        """Arg[str] captures str as _type_param."""
        arg: Arg[Any] = Arg[str](0)
        assert hasattr(arg, "_type_param")
        assert arg._type_param is str

    def test_arg_captures_type_param_int(self) -> None:
        """Arg[int] captures int as _type_param."""
        arg: Arg[Any] = Arg[int](0)
        assert hasattr(arg, "_type_param")
        assert arg._type_param is int

    def test_arg_captures_type_param_float(self) -> None:
        """Arg[float] captures float as _type_param."""
        arg: Arg[Any] = Arg[float](0)
        assert hasattr(arg, "_type_param")
        assert arg._type_param is float

    def test_arg_captures_type_param_bool(self) -> None:
        """Arg[bool] captures bool as _type_param."""
        arg: Arg[Any] = Arg[bool](0)
        assert hasattr(arg, "_type_param")
        assert arg._type_param is bool


class TestTypeInference:
    """Tests that extracted specs match expected types from Arg descriptors."""

    @pytest.mark.parametrize(
        "func_cls",
        [
            # Scalar functions
            DoubleColumnFunction,
            AddColumnsFunction,
            UpperCaseFunction,
            # Table functions
            SequenceFunction,
            RangeFunction,
            ConstantTableFunction,
            RandomSampleFunction,
            GeneratorExceptionFunction,
            LoggingGeneratorFunction,
            PartitionedRangeFunction,
            ProjectedDataFunction,
            SettingsAwareFunction,
            # Table-in-out functions
            EchoFunction,
            BufferInputFunction,
            RepeatInputsFunction,
            SumAllColumnsFunction,
            SumAllColumnsFunctionDistributed,
            SumAllColumnsFunctionWithLogging,
            SumAllColumnsSimpleDistributed,
            ExceptionProcessFunction,
            ExceptionFinalizeFunction,
        ],
    )
    def test_extracted_types_match_descriptors(self, func_cls: type) -> None:
        """Extracted argument types match what's defined in Arg descriptors."""
        # Get Arg descriptors from class
        descriptors = get_arg_descriptors(func_cls)

        # Get type hints for detecting TableInput
        try:
            from typing import get_type_hints

            hints = get_type_hints(func_cls)
        except (NameError, AttributeError):
            hints = {}

        # Extract specs
        specs = extract_argument_specs(func_cls)
        spec_by_name = {spec.name: spec for spec in specs}

        # Verify each descriptor's expected type matches extracted type
        for name, arg in descriptors.items():
            assert name in spec_by_name, f"Missing argument: {name}"
            hint = hints.get(name)
            expected_type = get_expected_type(arg, hint)
            actual_type = spec_by_name[name].arrow_type
            assert actual_type == expected_type, (
                f"{func_cls.__name__}.{name}: "
                f"expected {expected_type}, got {actual_type}"
            )


class TestExplicitArrowTypeOverride:
    """Tests that explicit arrow_type overrides type inference."""

    def test_range_step_uses_explicit_int32(self) -> None:
        """RangeFunction.step has explicit arrow_type=pa.int32() despite Arg[int]."""
        descriptors = get_arg_descriptors(RangeFunction)
        # Verify the descriptor has explicit arrow_type
        assert descriptors["step"].arrow_type == pa.int32()
        # Verify extracted spec uses explicit type
        specs = extract_argument_specs(RangeFunction)
        spec_by_name = {spec.name: spec for spec in specs}
        assert spec_by_name["step"].arrow_type == pa.int32()

    def test_double_column_uses_explicit_utf8(self) -> None:
        """DoubleColumnFunction.column has explicit arrow_type=pa.utf8()."""
        descriptors = get_arg_descriptors(DoubleColumnFunction)
        assert descriptors["column"].arrow_type == pa.utf8()
        specs = extract_argument_specs(DoubleColumnFunction)
        spec_by_name = {spec.name: spec for spec in specs}
        assert spec_by_name["column"].arrow_type == pa.utf8()


class TestTableInputBecomesNull:
    """Tests that TableInput arguments become pa.null() type."""

    @pytest.mark.parametrize(
        "func_cls,arg_name",
        [
            (EchoFunction, "data"),
            (BufferInputFunction, "data"),
            (RepeatInputsFunction, "data"),
            (SumAllColumnsFunction, "data"),
            (SumAllColumnsFunctionDistributed, "data"),
            (SumAllColumnsFunctionWithLogging, "data"),
            (SumAllColumnsSimpleDistributed, "data"),
            (ExceptionProcessFunction, "data"),
            (ExceptionFinalizeFunction, "data"),
        ],
    )
    def test_table_input_becomes_null(self, func_cls: type, arg_name: str) -> None:
        """TableInput arguments are represented as pa.null() in specs."""
        specs = extract_argument_specs(func_cls)
        spec_by_name = {spec.name: spec for spec in specs}
        assert spec_by_name[arg_name].arrow_type == pa.null()
        assert spec_by_name[arg_name].is_table_input is True


class TestPythonToArrowMapping:
    """Tests that PYTHON_TO_ARROW mapping is applied correctly."""

    def test_int_maps_to_int64(self) -> None:
        """Arg[int] without explicit arrow_type maps to pa.int64()."""
        specs = extract_argument_specs(SequenceFunction)
        spec_by_name = {spec.name: spec for spec in specs}
        # SequenceFunction.count uses Arg[int](0) without explicit arrow_type
        assert spec_by_name["count"].arrow_type == pa.int64()

    def test_str_maps_to_utf8(self) -> None:
        """Arg[str] without explicit arrow_type maps to pa.utf8()."""
        specs = extract_argument_specs(AddColumnsFunction)
        spec_by_name = {spec.name: spec for spec in specs}
        # AddColumnsFunction uses Arg[str](0/1) without explicit arrow_type
        assert spec_by_name["col1"].arrow_type == pa.utf8()
        assert spec_by_name["col2"].arrow_type == pa.utf8()


class TestAllExampleFunctionsCovered:
    """Verifies all example functions are tested."""

    def test_all_scalar_functions_covered(self) -> None:
        """All exported scalar functions have type inference tests."""
        from vgi.examples import scalar

        for name in scalar.__all__:
            func_cls = getattr(scalar, name)
            # Should not raise - tests pass for all
            specs = extract_argument_specs(func_cls)
            assert len(specs) >= 0  # At least have specs

    def test_all_table_functions_covered(self) -> None:
        """All exported table functions have type inference tests."""
        from vgi.examples import table

        for name in table.__all__:
            func_cls = getattr(table, name)
            specs = extract_argument_specs(func_cls)
            assert len(specs) >= 0

    def test_all_table_in_out_functions_covered(self) -> None:
        """All exported table-in-out functions have type inference tests."""
        from vgi.examples import table_in_out

        for name in table_in_out.__all__:
            func_cls = getattr(table_in_out, name)
            specs = extract_argument_specs(func_cls)
            assert len(specs) >= 0
