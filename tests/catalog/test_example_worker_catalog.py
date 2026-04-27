"""Tests for ExampleWorker catalog interface.

These tests verify that the ExampleWorker exposes its functions via the catalog
interface, allowing clients to discover available functions.
"""

import pyarrow as pa

from vgi._test_fixtures.worker import ExampleWorker
from vgi.catalog import (
    AttachId,
    FunctionInfo,
    FunctionType,
    MacroInfo,
    MacroType,
    SchemaObjectType,
    TableInfo,
    ViewInfo,
)
from vgi.client import Client

# Worker command for catalog tests
EXAMPLE_WORKER = "vgi-fixture-worker"


def _get_expected_function_names() -> set[str]:
    """Get all function names from ExampleWorker dynamically."""
    names = set()
    # Support new declarative catalog pattern
    if hasattr(ExampleWorker, "catalog") and ExampleWorker.catalog is not None:
        for schema in ExampleWorker.catalog.schemas:
            for func_cls in schema.functions:
                meta = func_cls.get_metadata()
                names.add(meta.name)
    # Support legacy functions list pattern
    elif hasattr(ExampleWorker, "functions"):
        for func_cls in ExampleWorker.functions:
            meta = func_cls.get_metadata()
            names.add(meta.name)
    return names


def _get_functions(
    contents: list[TableInfo | ViewInfo | FunctionInfo],
) -> list[FunctionInfo]:
    """Filter schema contents to only FunctionInfo objects."""
    return [item for item in contents if isinstance(item, FunctionInfo)]


def _get_all_functions(client: Client, attach_id: AttachId) -> list[FunctionInfo]:
    """Get both table and scalar functions from the catalog."""
    table_funcs = list(
        client.schema_contents(
            attach_id=attach_id,
            name="main",
            type=SchemaObjectType.TABLE_FUNCTION,
        )
    )
    scalar_funcs = list(
        client.schema_contents(
            attach_id=attach_id,
            name="main",
            type=SchemaObjectType.SCALAR_FUNCTION,
        )
    )
    # Combine to list - the overloads guarantee FunctionInfo for function types
    return list(table_funcs) + list(scalar_funcs)


class TestExampleWorkerCatalog:
    """Test ExampleWorker's catalog interface."""

    def test_catalogs_returns_example(self) -> None:
        """ExampleWorker.catalogs() returns 'example' catalog."""
        client = Client(EXAMPLE_WORKER)
        catalogs = client.catalogs()
        assert "example" in [c.name for c in catalogs]

    def test_catalog_attach_works(self) -> None:
        """Can attach to the 'example' catalog."""
        client = Client(EXAMPLE_WORKER)
        result = client.catalog_attach(name="example", options={}, data_version_spec=None, implementation_version=None)

        assert result.attach_id is not None
        assert result.supports_transactions is False
        assert result.catalog_version_frozen is True

    def test_schema_contents_returns_functions(self) -> None:
        """schema_contents() returns FunctionInfo for table functions."""
        client = Client(EXAMPLE_WORKER)

        # Attach to catalog
        attach_result = client.catalog_attach(
            name="example", options={}, data_version_spec=None, implementation_version=None
        )
        attach_id = attach_result.attach_id

        # Get table functions
        contents = list(client.schema_contents(attach_id=attach_id, name="main", type=SchemaObjectType.TABLE_FUNCTION))

        # Should have functions
        assert len(contents) > 0

        # All contents should be FunctionInfo
        for item in contents:
            assert isinstance(item, FunctionInfo)

    def test_all_example_functions_listed(self) -> None:
        """All example functions are listed in the catalog."""
        client = Client(EXAMPLE_WORKER)

        # Attach and get both table and scalar functions
        attach_result = client.catalog_attach(
            name="example", options={}, data_version_spec=None, implementation_version=None
        )

        # Get table functions
        table_funcs = list(
            client.schema_contents(
                attach_id=attach_result.attach_id,
                name="main",
                type=SchemaObjectType.TABLE_FUNCTION,
            )
        )

        # Get scalar functions
        scalar_funcs = list(
            client.schema_contents(
                attach_id=attach_result.attach_id,
                name="main",
                type=SchemaObjectType.SCALAR_FUNCTION,
            )
        )

        # Get aggregate functions
        aggregate_funcs = list(
            client.schema_contents(
                attach_id=attach_result.attach_id,
                name="main",
                type=SchemaObjectType.AGGREGATE_FUNCTION,
            )
        )

        # Combine all functions
        contents = table_funcs + scalar_funcs + aggregate_funcs

        # Get function names
        function_names = {item.name for item in contents}

        # Get expected functions from ExampleWorker dynamically
        expected_functions = _get_expected_function_names()

        # All expected functions should be present
        missing = expected_functions - function_names
        assert not missing, f"Missing functions: {missing}"

        # No extra functions should be present
        extra = function_names - expected_functions
        assert not extra, f"Unexpected functions: {extra}"

    def test_function_info_has_correct_types(self) -> None:
        """FunctionInfo has correct function types."""
        client = Client(EXAMPLE_WORKER)

        attach_result = client.catalog_attach(
            name="example", options={}, data_version_spec=None, implementation_version=None
        )
        functions = _get_all_functions(client, attach_result.attach_id)

        # Create lookup by name
        by_name = {fn.name: fn for fn in functions}

        # Check scalar functions
        assert by_name["double"].function_type == FunctionType.SCALAR
        assert by_name["add_values"].function_type == FunctionType.SCALAR
        assert by_name["upper_case"].function_type == FunctionType.SCALAR

        # Check table functions (TableFunctionGenerator and TableInOutGenerator)
        assert by_name["echo"].function_type == FunctionType.TABLE
        assert by_name["sequence"].function_type == FunctionType.TABLE

    def test_function_info_has_arguments(self) -> None:
        """FunctionInfo has serialized argument schema."""
        client = Client(EXAMPLE_WORKER)

        attach_result = client.catalog_attach(
            name="example", options={}, data_version_spec=None, implementation_version=None
        )
        functions = _get_all_functions(client, attach_result.attach_id)

        # Create lookup by name
        by_name = {fn.name: fn for fn in functions}

        # Check sequence function has 'count' argument
        sequence_info = by_name["sequence"]
        args_schema = pa.ipc.read_schema(pa.py_buffer(sequence_info.arguments))
        field_names = [f.name for f in args_schema]
        assert "count" in field_names

        # Check double function has 'value' argument
        double_info = by_name["double"]
        args_schema = pa.ipc.read_schema(pa.py_buffer(double_info.arguments))
        field_names = [f.name for f in args_schema]
        assert "value" in field_names

    def test_function_info_has_description(self) -> None:
        """FunctionInfo has description from docstring or Meta."""
        client = Client(EXAMPLE_WORKER)

        attach_result = client.catalog_attach(
            name="example", options={}, data_version_spec=None, implementation_version=None
        )
        functions = list(
            client.schema_contents(
                attach_id=attach_result.attach_id,
                name="main",
                type=SchemaObjectType.TABLE_FUNCTION,
            )
        )

        # Create lookup by name
        by_name = {fn.name: fn for fn in functions}

        # Functions should have descriptions (in description field, not comment)
        echo_info = by_name["echo"]
        assert echo_info.description is not None
        assert len(echo_info.description) > 0

    def test_function_info_schema_name(self) -> None:
        """FunctionInfo has schema_name set to 'main'."""
        client = Client(EXAMPLE_WORKER)

        attach_result = client.catalog_attach(
            name="example", options={}, data_version_spec=None, implementation_version=None
        )
        functions = _get_all_functions(client, attach_result.attach_id)

        # All functions should be in 'main' schema
        for item in functions:
            assert item.schema_name == "main"

    def test_scalar_function_has_output_schema(self) -> None:
        """Scalar functions with static output types have output_schema populated."""
        client = Client(EXAMPLE_WORKER)

        attach_result = client.catalog_attach(
            name="example", options={}, data_version_spec=None, implementation_version=None
        )
        functions = list(
            client.schema_contents(
                attach_id=attach_result.attach_id,
                name="main",
                type=SchemaObjectType.SCALAR_FUNCTION,
            )
        )

        # Create lookup by name
        by_name = {fn.name: fn for fn in functions}

        # upper_case has static output type (string)
        upper_info = by_name["upper_case"]
        output_schema = pa.ipc.read_schema(pa.py_buffer(upper_info.output_schema))

        # Should have a single column named "result" with string type
        assert len(output_schema) == 1
        assert output_schema.field(0).name == "result"
        assert output_schema.field(0).type == pa.string()

    def test_scalar_function_with_dynamic_output_has_any_type(self) -> None:
        """Scalar functions with AnyArrow output type have 'any' output_schema."""
        client = Client(EXAMPLE_WORKER)

        attach_result = client.catalog_attach(
            name="example", options={}, data_version_spec=None, implementation_version=None
        )
        functions = list(
            client.schema_contents(
                attach_id=attach_result.attach_id,
                name="main",
                type=SchemaObjectType.SCALAR_FUNCTION,
            )
        )

        # Create lookup by name
        by_name = {fn.name: fn for fn in functions}

        # double returns AnyArrow (output depends on input)
        double_info = by_name["double"]
        output_schema = pa.ipc.read_schema(pa.py_buffer(double_info.output_schema))

        # Should have a single "result" field with null type and vgi:any metadata
        assert len(output_schema) == 1
        assert output_schema.field(0).name == "result"
        assert output_schema.field(0).type == pa.null()
        assert output_schema.field(0).metadata == {b"vgi:any": b"true"}

    def test_table_function_has_empty_output_schema(self) -> None:
        """Table functions have empty output_schema (can't determine without input)."""
        client = Client(EXAMPLE_WORKER)

        attach_result = client.catalog_attach(
            name="example", options={}, data_version_spec=None, implementation_version=None
        )
        functions = list(
            client.schema_contents(
                attach_id=attach_result.attach_id,
                name="main",
                type=SchemaObjectType.TABLE_FUNCTION,
            )
        )

        # Create lookup by name
        by_name = {fn.name: fn for fn in functions}

        # echo is a table function
        echo_info = by_name["echo"]
        output_schema = pa.ipc.read_schema(pa.py_buffer(echo_info.output_schema))

        # Table functions don't have catalog_output_schema, so it's empty
        assert len(output_schema) == 0


class TestExampleWorkerViews:
    """Test ExampleWorker's view catalog entries."""

    def test_schema_contents_returns_views_in_main(self) -> None:
        """Main schema has 2 views (first_ten, even_numbers)."""
        client = Client(EXAMPLE_WORKER)
        attach_result = client.catalog_attach(
            name="example", options={}, data_version_spec=None, implementation_version=None
        )

        views = list(
            client.schema_contents(
                attach_id=attach_result.attach_id,
                name="main",
                type=SchemaObjectType.VIEW,
            )
        )

        view_names = {v.name for v in views}
        assert "first_ten" in view_names
        assert "even_numbers" in view_names
        assert len(views) == 2

    def test_schema_contents_returns_views_in_data(self) -> None:
        """Data schema has 1 view (small_numbers)."""
        client = Client(EXAMPLE_WORKER)
        attach_result = client.catalog_attach(
            name="example", options={}, data_version_spec=None, implementation_version=None
        )

        views = list(
            client.schema_contents(
                attach_id=attach_result.attach_id,
                name="data",
                type=SchemaObjectType.VIEW,
            )
        )

        assert len(views) == 1
        assert views[0].name == "small_numbers"

    def test_view_get_returns_correct_info(self) -> None:
        """view_get returns correct definition, comment, and tags."""
        client = Client(EXAMPLE_WORKER)
        attach_result = client.catalog_attach(
            name="example", options={}, data_version_spec=None, implementation_version=None
        )

        view = client.view_get(
            attach_id=attach_result.attach_id,
            schema_name="main",
            name="first_ten",
        )

        assert view is not None
        assert isinstance(view, ViewInfo)
        assert view.name == "first_ten"
        assert "sequence(10)" in view.definition
        assert view.comment is not None


class TestExampleWorkerMacros:
    """Test ExampleWorker's macro catalog entries."""

    def test_schema_contents_returns_macros_in_main(self) -> None:
        """Main schema has 3 macros."""
        client = Client(EXAMPLE_WORKER)
        attach_result = client.catalog_attach(
            name="example", options={}, data_version_spec=None, implementation_version=None
        )

        scalar_macros = list(
            client.schema_contents(
                attach_id=attach_result.attach_id,
                name="main",
                type=SchemaObjectType.SCALAR_MACRO,
            )
        )
        table_macros = list(
            client.schema_contents(
                attach_id=attach_result.attach_id,
                name="main",
                type=SchemaObjectType.TABLE_MACRO,
            )
        )

        all_macros = scalar_macros + table_macros
        macro_names = {m.name for m in all_macros}
        assert macro_names == {"vgi_multiply", "vgi_clamp", "vgi_range_table"}

    def test_macro_get_returns_correct_info(self) -> None:
        """macro_get returns a MacroInfo with correct fields."""
        client = Client(EXAMPLE_WORKER)
        attach_result = client.catalog_attach(
            name="example", options={}, data_version_spec=None, implementation_version=None
        )

        info = client.macro_get(
            attach_id=attach_result.attach_id,
            schema_name="main",
            name="vgi_multiply",
        )

        assert info is not None
        assert isinstance(info, MacroInfo)
        assert info.name == "vgi_multiply"
        assert info.schema_name == "main"
        assert info.definition == "x * y"
        assert info.comment == "Multiply two values"

    def test_macro_info_has_correct_types(self) -> None:
        """Macro type correctly distinguishes scalar vs table."""
        client = Client(EXAMPLE_WORKER)
        attach_result = client.catalog_attach(
            name="example", options={}, data_version_spec=None, implementation_version=None
        )

        multiply = client.macro_get(
            attach_id=attach_result.attach_id,
            schema_name="main",
            name="vgi_multiply",
        )
        range_table = client.macro_get(
            attach_id=attach_result.attach_id,
            schema_name="main",
            name="vgi_range_table",
        )

        assert multiply is not None
        assert multiply.macro_type == MacroType.SCALAR

        assert range_table is not None
        assert range_table.macro_type == MacroType.TABLE

    def test_macro_info_has_parameters(self) -> None:
        """Parameter list survives round-trip."""
        client = Client(EXAMPLE_WORKER)
        attach_result = client.catalog_attach(
            name="example", options={}, data_version_spec=None, implementation_version=None
        )

        info = client.macro_get(
            attach_id=attach_result.attach_id,
            schema_name="main",
            name="vgi_multiply",
        )

        assert info is not None
        assert info.parameters == ["x", "y"]

    def test_macro_info_has_parameter_default_values(self) -> None:
        """RecordBatch parameter defaults survive round-trip with types."""
        client = Client(EXAMPLE_WORKER)
        attach_result = client.catalog_attach(
            name="example", options={}, data_version_spec=None, implementation_version=None
        )

        info = client.macro_get(
            attach_id=attach_result.attach_id,
            schema_name="main",
            name="vgi_clamp",
        )

        assert info is not None
        assert info.parameters == ["val", "lo", "hi"]
        assert info.parameter_default_values is not None
        defaults = info.parameter_default_values
        assert defaults.num_rows == 1
        assert set(defaults.schema.names) == {"lo", "hi"}
        assert defaults.column("lo")[0].as_py() == 0
        assert defaults.column("hi")[0].as_py() == 100
