# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for vgi.metadata module and integration with Function classes."""

from __future__ import annotations

import pyarrow as pa

from vgi import (
    AnyArrow,
    Arg,
    CatalogFunctionType,
    FunctionExample,
    TableInOutFunction,
    TableInput,
    functions_to_arrow,
)
from vgi.metadata import (
    arrow_to_functions,
    extract_parameters,
    resolve_metadata,
)


class TestResolveMetadata:
    """Tests for metadata resolution from function classes."""

    def test_minimal_function(self) -> None:
        """Function with no Meta class uses defaults."""

        class MinimalFunction(TableInOutFunction):  # type: ignore[type-arg]
            """A minimal function."""

            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        meta = MinimalFunction.get_metadata()
        assert meta.name == "minimal"  # Auto-converted from MinimalFunction
        assert meta.class_name == "MinimalFunction"
        assert meta.description == "A minimal function."  # From docstring
        assert meta.function_type == CatalogFunctionType.TABLE
        assert meta.max_workers is None
        assert meta.categories == []

    def test_function_with_meta(self) -> None:
        """Function with Meta class uses defined values."""

        class CustomFunction(TableInOutFunction):  # type: ignore[type-arg]
            """Docstring description."""

            class Meta:
                name = "custom_func"
                description = "Custom description"
                categories = ["transform", "utility"]
                max_workers = 4

            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        meta = CustomFunction.get_metadata()
        assert meta.name == "custom_func"
        assert meta.description == "Custom description"
        assert meta.categories == ["transform", "utility"]
        assert meta.max_workers == 4

    def test_name_auto_generation(self) -> None:
        """Function name is auto-generated from class name."""

        class MyAwesomeTransformFunction(TableInOutFunction):  # type: ignore[type-arg]
            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        meta = MyAwesomeTransformFunction.get_metadata()
        # Converts CamelCase to snake_case, removes _function suffix
        assert meta.name == "my_awesome_transform"

    def test_docstring_fallback(self) -> None:
        """Description falls back to first line of docstring."""

        class DocstringFunction(TableInOutFunction):  # type: ignore[type-arg]
            """First line of docstring.

            More detailed description here.
            """

            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        meta = DocstringFunction.get_metadata()
        assert meta.description == "First line of docstring."

    def test_examples_normalization(self) -> None:
        """String examples are converted to FunctionExample objects."""

        class ExampleFunction(TableInOutFunction):  # type: ignore[type-arg]
            class Meta:
                examples = [
                    "SELECT * FROM example_function(data)",
                    FunctionExample(
                        sql="SELECT * FROM example_function(data, n=5)",
                        description="With parameter",
                    ),
                ]

            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        meta = ExampleFunction.get_metadata()
        assert len(meta.examples) == 2
        assert meta.examples[0].sql == "SELECT * FROM example_function(data)"
        assert meta.examples[0].description == ""
        assert meta.examples[1].description == "With parameter"


class TestMetaInheritance:
    """Tests for Meta class inheritance."""

    def test_inheritance_from_parent(self) -> None:
        """Child inherits Meta attributes from parent."""

        class BaseFunction(TableInOutFunction):  # type: ignore[type-arg]
            class Meta:
                categories = ["base"]
                max_workers = 2

            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        class ChildFunction(BaseFunction):
            class Meta:
                description = "Child function"

        meta = ChildFunction.get_metadata()
        assert meta.categories == ["base"]  # Inherited
        assert meta.max_workers == 2  # Inherited
        assert meta.description == "Child function"  # Defined

    def test_child_overrides_parent(self) -> None:
        """Child Meta attributes override parent."""

        class BaseFunction(TableInOutFunction):  # type: ignore[type-arg]
            class Meta:
                categories = ["base"]
                max_workers = 2

            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        class ChildFunction(BaseFunction):
            class Meta:
                categories = ["child"]  # Override

        meta = ChildFunction.get_metadata()
        assert meta.categories == ["child"]
        assert meta.max_workers == 2  # Still inherited


class TestExtractParameters:
    """Tests for extracting parameters from Arg descriptors."""

    def test_positional_arg(self) -> None:
        """Positional Arg descriptor is extracted."""

        class ArgFunction(TableInOutFunction):  # type: ignore[type-arg]
            count = Arg[int](0, doc="Number of items")

        # Skip TableInput validation since we're testing Arg extraction
        params = extract_parameters(ArgFunction, validate_table_input=False)
        assert len(params) == 1
        assert params[0].name == "count"
        assert params[0].position == 0
        assert params[0].description == "Number of items"
        assert params[0].required is True

    def test_named_arg_with_default(self) -> None:
        """Named Arg with default is optional."""

        class ArgFunction(TableInOutFunction):  # type: ignore[type-arg]
            sep = Arg[str]("separator", default=",", doc="Field separator")

        # Skip TableInput validation since we're testing Arg extraction
        params = extract_parameters(ArgFunction, validate_table_input=False)
        assert len(params) == 1
        assert params[0].name == "sep"
        assert params[0].position == "separator"
        assert params[0].required is False
        assert params[0].default == ","

    def test_arg_with_constraints(self) -> None:
        """Arg validation constraints are extracted."""

        class ArgFunction(TableInOutFunction):  # type: ignore[type-arg]
            count = Arg[int](0, ge=1, le=100)
            ratio = Arg[float](1, gt=0.0, lt=1.0)

        # Skip TableInput validation since we're testing Arg extraction
        params = extract_parameters(ArgFunction, validate_table_input=False)
        count_param = next(p for p in params if p.name == "count")
        ratio_param = next(p for p in params if p.name == "ratio")

        assert count_param.constraints == {"ge": 1, "le": 100}
        assert ratio_param.constraints == {"gt": 0.0, "lt": 1.0}

    def test_multiple_args_sorted(self) -> None:
        """Multiple Args are sorted by position."""

        class ArgFunction(TableInOutFunction):  # type: ignore[type-arg]
            third = Arg[str](2)
            first = Arg[int](0)
            named = Arg[str]("name", default="x")
            second = Arg[float](1)

        # Skip TableInput validation since we're testing Arg extraction
        params = extract_parameters(ArgFunction, validate_table_input=False)
        names = [p.name for p in params]
        # Positional first (by index), then named (alphabetically)
        assert names == ["first", "second", "third", "named"]


class TestMaxWorkersIntegration:
    """Tests for max_workers metadata extraction."""

    def test_max_workers_used(self) -> None:
        """max_workers is extracted from Meta when defined."""

        class LimitedFunction(TableInOutFunction):  # type: ignore[type-arg]
            class Meta:
                max_workers = 2

            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        meta = LimitedFunction.get_metadata()
        assert meta.max_workers == 2

    def test_default_max_workers(self) -> None:
        """max_workers defaults to None when not defined."""

        class UnlimitedFunction(TableInOutFunction):  # type: ignore[type-arg]
            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        meta = UnlimitedFunction.get_metadata()
        assert meta.max_workers is None


class TestHasFinalizeDetection:
    """Tests for the has_finalize auto-detection heuristic."""

    def test_no_override(self) -> None:
        """A subclass that defines neither finish nor finalize has_finalize=False."""
        from vgi.table_in_out_function import TableInOutFunction as TIOF

        class Plain(TIOF):  # type: ignore[type-arg]
            class Meta:
                name = "plain_fn"

            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

        assert resolve_metadata(Plain).has_finalize is False

    def test_finish_override_detected(self) -> None:
        """Overriding finish() on TableInOutFunction sets has_finalize=True."""
        from typing import Any

        from vgi.table_in_out_function import TableInOutFunction as TIOF

        class WithFinish(TIOF):  # type: ignore[type-arg]
            class Meta:
                name = "with_finish_fn"

            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

            @classmethod
            def finish(cls, params: Any, states: Any) -> list[Any]:
                return []

        assert resolve_metadata(WithFinish).has_finalize is True

    def test_generator_finalize_override_detected(self) -> None:
        """Overriding finalize() on TableInOutGenerator is detected."""
        from typing import Any

        from vgi.table_in_out_function import TableInOutGenerator

        class GenWithFinalize(TableInOutGenerator):  # type: ignore[type-arg]
            class Meta:
                name = "gen_with_finalize_fn"

            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

            @classmethod
            def finalize(cls, params: Any) -> list[Any]:
                return []

        assert resolve_metadata(GenWithFinalize).has_finalize is True

    def test_unrelated_mixin_with_finish_ignored(self) -> None:
        """A mixin contributing a finish attribute must not false-positive.

        The mixin is not a TableInOut ancestor.
        """
        from vgi.table_in_out_function import TableInOutFunction as TIOF

        class UnrelatedMixin:
            def finish(self) -> None:  # unrelated method — not a TableInOut override
                pass

        class WithMixin(UnrelatedMixin, TIOF):  # type: ignore[type-arg,misc]
            class Meta:
                name = "with_mixin_fn"

            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

        assert resolve_metadata(WithMixin).has_finalize is False

    def test_non_callable_attr_ignored(self) -> None:
        """A class-level attribute named finish that isn't callable doesn't count."""
        from vgi.table_in_out_function import TableInOutFunction as TIOF

        class WithBogusAttr(TIOF):  # type: ignore[type-arg]
            class Meta:
                name = "bogus_attr_fn"

            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

            finish = "not a method"  # type: ignore[assignment]

        assert resolve_metadata(WithBogusAttr).has_finalize is False

    def test_meta_override_forces_false(self) -> None:
        """Meta.has_finalize = False wins over a real finish() override."""
        from typing import Any

        from vgi.table_in_out_function import TableInOutFunction as TIOF

        class ForcedOff(TIOF):  # type: ignore[type-arg]
            class Meta:
                name = "forced_off"
                has_finalize = False  # user claims this is a no-op

            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

            @classmethod
            def finish(cls, params: Any, states: Any) -> list[Any]:
                return []

        assert resolve_metadata(ForcedOff).has_finalize is False

    def test_meta_override_forces_true(self) -> None:
        """Meta.has_finalize = True forces the bit even with no finish override."""
        from vgi.table_in_out_function import TableInOutFunction as TIOF

        class ForcedOn(TIOF):  # type: ignore[type-arg]
            class Meta:
                name = "forced_on"
                has_finalize = True

            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

        assert resolve_metadata(ForcedOn).has_finalize is True

    def test_non_tableinout_class(self) -> None:
        """Non-TableInOut function types always report has_finalize=False."""
        from typing import Annotated, Any

        from vgi.arguments import Param, Returns
        from vgi.scalar_function import ScalarFunction

        class ScalarWithFinalize(ScalarFunction):
            class Meta:
                name = "scalar_with_finalize_fn"

            @classmethod
            def finalize(cls, params: Any) -> list[Any]:
                # irrelevant for scalar, detection must ignore
                return []

            @classmethod
            def compute(
                cls,
                x: Annotated[pa.Int64Array, Param(doc="")],
            ) -> Annotated[pa.Int64Array, Returns(pa.int64())]:
                return x

        assert resolve_metadata(ScalarWithFinalize).has_finalize is False


class TestArrowSerialization:
    """Tests for Arrow serialization of metadata."""

    def test_single_function_roundtrip(self) -> None:
        """Single function metadata survives Arrow roundtrip."""

        class TestFunction(TableInOutFunction):  # type: ignore[type-arg]
            """Test function for serialization."""

            class Meta:
                name = "test_func"
                description = "A test function"
                categories = ["test"]
                max_workers = 3

            count = Arg[int](0, doc="Count parameter")
            data: TableInput = Arg[TableInput](1, doc="Input table")  # type: ignore[assignment]

        batch = functions_to_arrow([TestFunction])
        assert batch.num_rows == 1

        restored = arrow_to_functions(batch)
        assert len(restored) == 1

        meta = restored[0]
        assert meta.name == "test_func"
        assert meta.description == "A test function"
        assert meta.categories == ["test"]
        assert meta.max_workers == 3
        assert len(meta.parameters) == 2
        assert meta.parameters[0].name == "count"
        assert meta.parameters[1].name == "data"
        assert meta.parameters[1].is_table_input is True

    def test_multiple_functions_roundtrip(self) -> None:
        """Multiple functions survive Arrow roundtrip."""

        class Func1(TableInOutFunction):  # type: ignore[type-arg]
            class Meta:
                name = "func_one"

            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        class Func2(TableInOutFunction):  # type: ignore[type-arg]
            class Meta:
                name = "func_two"

            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        class Func3(TableInOutFunction):  # type: ignore[type-arg]
            class Meta:
                name = "func_three"

            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        batch = functions_to_arrow([Func1, Func2, Func3])
        assert batch.num_rows == 3

        restored = arrow_to_functions(batch)
        names = [m.name for m in restored]
        assert names == ["func_one", "func_two", "func_three"]

    def test_empty_functions_list(self) -> None:
        """Empty function list produces empty batch."""
        batch = functions_to_arrow([])
        assert batch.num_rows == 0

        restored = arrow_to_functions(batch)
        assert restored == []


class TestDescribeMethod:
    """Tests for the describe() classmethod."""

    def test_describe_returns_dict(self) -> None:
        """describe() returns a JSON-serializable dict."""
        import json

        class DescribeFunction(TableInOutFunction):  # type: ignore[type-arg]
            class Meta:
                name = "describe_test"
                description = "Test describe method"
                categories = ["test"]

            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        result = DescribeFunction.describe()

        # Should be JSON-serializable
        json_str = json.dumps(result)
        assert json_str

        # Should have expected keys
        assert result["name"] == "describe_test"
        assert result["description"] == "Test describe method"
        assert result["categories"] == ["test"]


class TestVarargsMetadata:
    """Tests for varargs parameter metadata extraction and validation."""

    def test_varargs_is_extracted(self) -> None:
        """Varargs parameter should have is_varargs=True in ParameterInfo."""

        class VarargsFunction(TableInOutFunction):  # type: ignore[type-arg]
            name = Arg[str](0, doc="Function name")
            values = Arg[int](1, varargs=True, doc="One or more values", arrow_type=pa.int64())

        params = extract_parameters(VarargsFunction, validate_table_input=False)
        name_param = next(p for p in params if p.name == "name")
        values_param = next(p for p in params if p.name == "values")

        assert name_param.is_varargs is False
        assert values_param.is_varargs is True

    def test_multiple_varargs_raises(self) -> None:
        """Multiple varargs parameters should raise VarargsValidationError."""
        import pytest

        from vgi.metadata import VarargsValidationError

        class BadFunction(TableInOutFunction):  # type: ignore[type-arg]
            values1 = Arg[int](0, varargs=True, arrow_type=pa.int64())
            values2 = Arg[str](1, varargs=True, arrow_type=pa.string())
            data: TableInput = Arg[TableInput](2, doc="Input")  # type: ignore[assignment]

        with pytest.raises(VarargsValidationError, match="at most one varargs"):
            extract_parameters(BadFunction)

    def test_varargs_not_last_raises(self) -> None:
        """Varargs not at last positional position should raise."""
        import pytest

        from vgi.metadata import VarargsValidationError

        class BadFunction(TableInOutFunction):  # type: ignore[type-arg]
            values = Arg[int](0, varargs=True, arrow_type=pa.int64())
            after = Arg[str](1)  # Regular arg after varargs
            data: TableInput = Arg[TableInput](2, doc="Input")  # type: ignore[assignment]

        with pytest.raises(VarargsValidationError, match="must be the last positional"):
            extract_parameters(BadFunction)

    def test_varargs_before_table_input_ok(self) -> None:
        """Varargs can be before TableInput (TableInput is always last)."""

        class GoodFunction(TableInOutFunction):  # type: ignore[type-arg]
            name = Arg[str](0, doc="Name")
            columns = Arg[str](1, varargs=True, doc="Column names", arrow_type=pa.string())
            data: TableInput = Arg[TableInput](2, doc="Input table")  # type: ignore[assignment]

        # Should not raise
        params = extract_parameters(GoodFunction)
        assert len(params) == 3

        columns_param = next(p for p in params if p.name == "columns")
        data_param = next(p for p in params if p.name == "data")

        assert columns_param.is_varargs is True
        assert data_param.is_table_input is True

    def test_varargs_arrow_roundtrip(self) -> None:
        """Varargs is_varargs survives Arrow serialization."""

        class VarargsFunction(TableInOutFunction):  # type: ignore[type-arg]
            """Test varargs serialization."""

            class Meta:
                name = "varargs_test"

            columns = Arg[str](0, varargs=True, doc="Column names", arrow_type=pa.string())
            data: TableInput = Arg[TableInput](1, doc="Input table")  # type: ignore[assignment]

        batch = functions_to_arrow([VarargsFunction])
        restored = arrow_to_functions(batch)

        assert len(restored) == 1
        meta = restored[0]

        columns_param = next(p for p in meta.parameters if p.name == "columns")
        data_param = next(p for p in meta.parameters if p.name == "data")

        assert columns_param.is_varargs is True
        assert data_param.is_varargs is False
        assert data_param.is_table_input is True


class TestAnyArrowMetadata:
    """Tests for AnyArrow parameter metadata extraction."""

    def test_any_arrow_type_extracted(self) -> None:
        """AnyArrow type should be extracted as 'AnyArrow' in metadata."""

        class AnyArrowFunction(TableInOutFunction):  # type: ignore[type-arg]
            value: AnyArrow = Arg[AnyArrow](0, doc="Any type value")  # type: ignore[assignment]
            data: TableInput = Arg[TableInput](1, doc="Input table")  # type: ignore[assignment]

        params = extract_parameters(AnyArrowFunction)
        value_param = next(p for p in params if p.name == "value")

        assert value_param.type_name == "AnyArrow"
        assert value_param.is_table_input is False

    def test_any_arrow_arrow_roundtrip(self) -> None:
        """AnyArrow type survives Arrow serialization."""

        class AnyArrowFunction(TableInOutFunction):  # type: ignore[type-arg]
            """Test AnyArrow serialization."""

            class Meta:
                name = "any_arrow_test"

            value: AnyArrow = Arg[AnyArrow](0, doc="Any type")  # type: ignore[assignment]
            data: TableInput = Arg[TableInput](1, doc="Input")  # type: ignore[assignment]

        batch = functions_to_arrow([AnyArrowFunction])
        restored = arrow_to_functions(batch)

        assert len(restored) == 1
        meta = restored[0]

        value_param = next(p for p in meta.parameters if p.name == "value")
        assert value_param.type_name == "AnyArrow"


class TestFunctionTypeInference:
    """Tests for function type inference from class hierarchy."""

    def test_table_in_out_function_type(self) -> None:
        """TableInOutFunction is detected as TABLE_IN_OUT."""

        class TestFunc(TableInOutFunction):  # type: ignore[type-arg]
            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        meta = resolve_metadata(TestFunc)
        assert meta.function_type == CatalogFunctionType.TABLE
