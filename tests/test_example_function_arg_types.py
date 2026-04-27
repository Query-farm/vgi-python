"""Tests for example function argument type inference.

These tests verify that the Arg[type] subscript syntax correctly captures
the type parameter and converts it to the appropriate Arrow type for
function metadata.
"""

from __future__ import annotations

from typing import Any, get_type_hints

import pyarrow as pa
import pytest

from vgi._test_fixtures.worker import ExampleWorker
from vgi.argument_spec import extract_argument_specs
from vgi.arguments import PYTHON_TO_ARROW, Arg, TableInput


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
    if hasattr(arg, "_type_param") and arg._type_param is not None and arg._type_param in PYTHON_TO_ARROW:
        return PYTHON_TO_ARROW[arg._type_param]
    if hint is not None and hint in PYTHON_TO_ARROW:
        return PYTHON_TO_ARROW[hint]
    return pa.null()


class TestArgTypeParamCapture:
    """Tests that Arg[type] captures the type parameter correctly."""

    @pytest.mark.parametrize("python_type", [str, int, float, bool], ids=["str", "int", "float", "bool"])
    def test_arg_captures_type_param(self, python_type: type) -> None:
        """Arg[type] captures the type as _type_param."""
        arg: Arg[Any] = Arg[python_type](0)  # type: ignore[valid-type]
        assert hasattr(arg, "_type_param")
        assert arg._type_param is python_type


class TestTypeInference:
    """Tests that extracted specs match expected types from Arg descriptors."""

    @pytest.mark.parametrize("func_cls", ExampleWorker.functions)
    def test_extracted_types_match_descriptors(self, func_cls: type) -> None:
        """Extracted argument types match what's defined in Arg descriptors."""
        # Get Arg descriptors from class
        descriptors = get_arg_descriptors(func_cls)

        # Get type hints for detecting TableInput
        try:
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
                f"{func_cls.__name__}.{name}: expected {expected_type}, got {actual_type}"
            )


class TestExplicitArrowTypeOverride:
    """Tests that explicit arrow_type overrides type inference."""

    def test_explicit_arrow_type_overrides_inference(self) -> None:
        """Explicit arrow_type on Arg takes precedence over type inference."""
        # Find functions with explicit arrow_type that differs from inferred
        for func_cls in ExampleWorker.functions:
            descriptors = get_arg_descriptors(func_cls)
            specs = extract_argument_specs(func_cls)
            spec_by_name = {spec.name: spec for spec in specs}

            for name, arg in descriptors.items():
                if arg.arrow_type is not None:
                    # Verify the explicit type is used
                    assert spec_by_name[name].arrow_type == arg.arrow_type


class TestTableInputBecomesNull:
    """Tests that TableInput arguments become pa.null() type."""

    def test_table_input_becomes_null(self) -> None:
        """TableInput arguments are represented as pa.null() in specs."""
        for func_cls in ExampleWorker.functions:
            try:
                hints = get_type_hints(func_cls)
            except (NameError, AttributeError):
                continue

            specs = extract_argument_specs(func_cls)
            spec_by_name = {spec.name: spec for spec in specs}

            for name, hint in hints.items():
                if hint is TableInput and name in spec_by_name:
                    spec = spec_by_name[name]
                    assert spec.arrow_type == pa.null(), (
                        f"{func_cls.__name__}.{name}: TableInput should be pa.null(), got {spec.arrow_type}"
                    )
                    assert spec.is_table_input is True


class TestPythonToArrowMapping:
    """Tests that PYTHON_TO_ARROW mapping is applied correctly."""

    def test_type_param_maps_correctly(self) -> None:
        """Arg[type] subscript types map to correct Arrow types."""
        for func_cls in ExampleWorker.functions:
            descriptors = get_arg_descriptors(func_cls)
            specs = extract_argument_specs(func_cls)
            spec_by_name = {spec.name: spec for spec in specs}

            try:
                hints = get_type_hints(func_cls)
            except (NameError, AttributeError):
                hints = {}

            for name, arg in descriptors.items():
                # Skip if has explicit arrow_type or is TableInput
                if arg.arrow_type is not None:
                    continue
                hint = hints.get(name)
                if hint is TableInput:
                    continue

                # Check if _type_param is in PYTHON_TO_ARROW
                if hasattr(arg, "_type_param") and arg._type_param is not None and arg._type_param in PYTHON_TO_ARROW:
                    expected = PYTHON_TO_ARROW[arg._type_param]
                    actual = spec_by_name[name].arrow_type
                    assert actual == expected, (
                        f"{func_cls.__name__}.{name}: Arg[{arg._type_param.__name__}] "
                        f"should map to {expected}, got {actual}"
                    )


class TestAllExampleFunctionsCovered:
    """Verifies all example worker functions are tested."""

    def test_all_worker_functions_have_extractable_specs(self) -> None:
        """All ExampleWorker functions can have specs extracted."""
        for func_cls in ExampleWorker.functions:
            # Should not raise
            specs = extract_argument_specs(func_cls)
            # Should have at least some attributes (may have 0 args)
            assert isinstance(specs, list)
