"""Tests for vgi.metadata module and integration with Function classes."""

from __future__ import annotations

import pyarrow as pa
import structlog

from vgi import (
    Arg,
    FunctionExample,
    FunctionType,
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

        class MinimalFunction(TableInOutFunction):
            """A minimal function."""

            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        meta = MinimalFunction.get_metadata()
        assert meta.name == "minimal"  # Auto-converted from MinimalFunction
        assert meta.class_name == "MinimalFunction"
        assert meta.description == "A minimal function."  # From docstring
        assert meta.function_type == FunctionType.TABLE
        assert meta.max_workers is None
        assert meta.categories == []

    def test_function_with_meta(self) -> None:
        """Function with Meta class uses defined values."""

        class CustomFunction(TableInOutFunction):
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

        class MyAwesomeTransformFunction(TableInOutFunction):
            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        meta = MyAwesomeTransformFunction.get_metadata()
        # Converts CamelCase to snake_case, removes _function suffix
        assert meta.name == "my_awesome_transform"

    def test_docstring_fallback(self) -> None:
        """Description falls back to first line of docstring."""

        class DocstringFunction(TableInOutFunction):
            """First line of docstring.

            More detailed description here.
            """

            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        meta = DocstringFunction.get_metadata()
        assert meta.description == "First line of docstring."

    def test_examples_normalization(self) -> None:
        """String examples are converted to FunctionExample objects."""

        class ExampleFunction(TableInOutFunction):
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

        class BaseFunction(TableInOutFunction):
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

        class BaseFunction(TableInOutFunction):
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

        class ArgFunction(TableInOutFunction):
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

        class ArgFunction(TableInOutFunction):
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

        class ArgFunction(TableInOutFunction):
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

        class ArgFunction(TableInOutFunction):
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
    """Tests for max_workers integration with max_processes property."""

    def test_max_workers_used(self, test_logger: structlog.stdlib.BoundLogger) -> None:
        """max_processes returns Meta.max_workers when defined."""
        from tests.conftest import make_invocation

        class LimitedFunction(TableInOutFunction):
            class Meta:
                max_workers = 2

            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        invocation = make_invocation(input_schema=pa.schema([]))
        func = LimitedFunction(invocation=invocation, logger=test_logger)
        assert func.max_processes == 2

    def test_default_max_workers(
        self, test_logger: structlog.stdlib.BoundLogger
    ) -> None:
        """max_processes returns default when max_workers not defined."""
        from tests.conftest import make_invocation

        class UnlimitedFunction(TableInOutFunction):
            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        invocation = make_invocation(input_schema=pa.schema([]))
        func = UnlimitedFunction(invocation=invocation, logger=test_logger)
        assert func.max_processes == 99999


class TestArrowSerialization:
    """Tests for Arrow serialization of metadata."""

    def test_single_function_roundtrip(self) -> None:
        """Single function metadata survives Arrow roundtrip."""

        class TestFunction(TableInOutFunction):
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

        class Func1(TableInOutFunction):
            class Meta:
                name = "func_one"

            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        class Func2(TableInOutFunction):
            class Meta:
                name = "func_two"

            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        class Func3(TableInOutFunction):
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

        class DescribeFunction(TableInOutFunction):
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


class TestFunctionTypeInference:
    """Tests for function type inference from class hierarchy."""

    def test_table_in_out_function_type(self) -> None:
        """TableInOutFunction is detected as TABLE_IN_OUT."""

        class TestFunc(TableInOutFunction):
            data: TableInput = Arg[TableInput](0, doc="Input table")  # type: ignore[assignment]

        meta = resolve_metadata(TestFunc)
        assert meta.function_type == FunctionType.TABLE
