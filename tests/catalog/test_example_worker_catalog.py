"""Tests for ExampleWorker catalog interface.

These tests verify that the ExampleWorker exposes its functions via the catalog
interface, allowing clients to discover available functions.
"""

import pyarrow as pa

from vgi.catalog import FunctionInfo, FunctionType, TableInfo, ViewInfo
from vgi.client import Client

# Worker command for catalog tests
EXAMPLE_WORKER = "vgi-example-worker"


def _get_functions(
    contents: list[TableInfo | ViewInfo | FunctionInfo],
) -> list[FunctionInfo]:
    """Filter schema contents to only FunctionInfo objects."""
    return [item for item in contents if isinstance(item, FunctionInfo)]


class TestExampleWorkerCatalog:
    """Test ExampleWorker's catalog interface."""

    def test_catalogs_returns_example(self) -> None:
        """ExampleWorker.catalogs() returns 'example' catalog."""
        client = Client(EXAMPLE_WORKER)
        catalogs = client.catalogs()
        assert "example" in catalogs

    def test_catalog_attach_works(self) -> None:
        """Can attach to the 'example' catalog."""
        client = Client(EXAMPLE_WORKER)
        result = client.catalog_attach(name="example", options={})

        assert result.attach_id is not None
        assert result.supports_transactions is False
        assert result.catalog_version_frozen is True

    def test_schema_contents_returns_functions(self) -> None:
        """schema_contents() returns FunctionInfo for all example functions."""
        client = Client(EXAMPLE_WORKER)

        # Attach to catalog
        attach_result = client.catalog_attach(name="example", options={})
        attach_id = attach_result.attach_id

        # Get schema contents
        contents = list(client.schema_contents(attach_id=attach_id, name="main"))

        # Should have functions
        assert len(contents) > 0

        # All contents should be FunctionInfo
        for item in contents:
            assert isinstance(item, FunctionInfo)

    def test_all_example_functions_listed(self) -> None:
        """All example functions are listed in the catalog."""
        client = Client(EXAMPLE_WORKER)

        # Attach and get contents
        attach_result = client.catalog_attach(name="example", options={})
        contents = list(
            client.schema_contents(attach_id=attach_result.attach_id, name="main")
        )

        # Get function names
        function_names = {item.name for item in contents}

        # Check for known example functions
        expected_functions = {
            # TableInOutGenerator functions
            "echo",
            "buffer_input",
            "repeat_inputs",
            "sum_all_columns",
            # TableFunctionGenerator functions
            "sequence",
            "range",
            "constant_table",
            "random_sample",
            # ScalarFunctionGenerator functions
            "double_column",
            "add_columns",
            "upper_case",
        }

        # All expected functions should be present
        missing = expected_functions - function_names
        assert not missing, f"Missing functions: {missing}"

    def test_function_info_has_correct_types(self) -> None:
        """FunctionInfo has correct function types."""
        client = Client(EXAMPLE_WORKER)

        attach_result = client.catalog_attach(name="example", options={})
        contents = list(
            client.schema_contents(attach_id=attach_result.attach_id, name="main")
        )
        functions = _get_functions(contents)

        # Create lookup by name
        by_name = {fn.name: fn for fn in functions}

        # Check scalar functions
        assert by_name["double_column"].function_type == FunctionType.SCALAR
        assert by_name["add_columns"].function_type == FunctionType.SCALAR
        assert by_name["upper_case"].function_type == FunctionType.SCALAR

        # Check table functions (TableFunctionGenerator and TableInOutGenerator)
        assert by_name["echo"].function_type == FunctionType.TABLE
        assert by_name["sequence"].function_type == FunctionType.TABLE
        assert by_name["range"].function_type == FunctionType.TABLE

    def test_function_info_has_arguments(self) -> None:
        """FunctionInfo has serialized argument schema."""
        client = Client(EXAMPLE_WORKER)

        attach_result = client.catalog_attach(name="example", options={})
        contents = list(
            client.schema_contents(attach_id=attach_result.attach_id, name="main")
        )
        functions = _get_functions(contents)

        # Create lookup by name
        by_name = {fn.name: fn for fn in functions}

        # Check sequence function has 'count' argument
        sequence_info = by_name["sequence"]
        args_schema = pa.ipc.read_schema(pa.py_buffer(sequence_info.arguments))
        field_names = [f.name for f in args_schema]
        assert "count" in field_names

        # Check double_column function has 'column' argument
        double_info = by_name["double_column"]
        args_schema = pa.ipc.read_schema(pa.py_buffer(double_info.arguments))
        field_names = [f.name for f in args_schema]
        assert "column" in field_names

    def test_function_info_has_description(self) -> None:
        """FunctionInfo has description from docstring or Meta."""
        client = Client(EXAMPLE_WORKER)

        attach_result = client.catalog_attach(name="example", options={})
        contents = list(
            client.schema_contents(attach_id=attach_result.attach_id, name="main")
        )
        functions = _get_functions(contents)

        # Create lookup by name
        by_name = {fn.name: fn for fn in functions}

        # Functions should have descriptions
        echo_info = by_name["echo"]
        assert echo_info.comment is not None
        assert len(echo_info.comment) > 0

    def test_function_info_schema_name(self) -> None:
        """FunctionInfo has schema_name set to 'main'."""
        client = Client(EXAMPLE_WORKER)

        attach_result = client.catalog_attach(name="example", options={})
        contents = list(
            client.schema_contents(attach_id=attach_result.attach_id, name="main")
        )

        # All functions should be in 'main' schema
        for item in contents:
            assert item.schema_name == "main"
