"""Tests for CLI catalog operations to list available functions.

These tests verify that the CLI can list all example functions via the
catalog interface (catalog list, catalog attach, schema contents).
"""

from __future__ import annotations

import json

from click.testing import CliRunner

from vgi.client.cli import cli
from vgi.examples.worker import ExampleWorker


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


class TestCLICatalogList:
    """Tests for listing catalogs via CLI."""

    def test_catalog_list_shows_example(self, example_worker: str) -> None:
        """Catalog list shows 'example' catalog."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["catalog", "list", "--worker", example_worker],
        )
        assert result.exit_code == 0
        catalogs = json.loads(result.output)
        assert "example" in catalogs


class TestCLICatalogAttach:
    """Tests for attaching to catalogs via CLI."""

    def test_catalog_attach_returns_attach_id(self, example_worker: str) -> None:
        """Catalog attach returns attach_id and capabilities."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["catalog", "attach", "example", "--worker", example_worker],
        )
        assert result.exit_code == 0
        attach_result = json.loads(result.output)
        assert "attach_id" in attach_result
        assert len(attach_result["attach_id"]) > 0
        assert attach_result["supports_transactions"] is False
        assert attach_result["catalog_version_frozen"] is True
        # ReadOnlyCatalogInterface returns attach_id_required=False
        assert attach_result["attach_id_required"] is False


class TestCLISchemaContents:
    """Tests for listing schema contents (functions) via CLI."""

    def test_schema_contents_lists_table_functions(self, example_worker: str) -> None:
        """Schema contents lists table functions in main schema."""
        runner = CliRunner()

        # First attach to get attach_id
        attach_result = runner.invoke(
            cli,
            ["catalog", "attach", "example", "--worker", example_worker],
        )
        assert attach_result.exit_code == 0
        attach_data = json.loads(attach_result.output)
        attach_id = attach_data["attach_id"]

        # List schema contents using --attach-id option with --type
        contents_result = runner.invoke(
            cli,
            [
                "catalog",
                "schema",
                "contents",
                "main",
                "--attach-id",
                attach_id,
                "--worker",
                example_worker,
                "--type",
                "table_function",
            ],
        )
        assert contents_result.exit_code == 0

        # Parse all output lines as JSON
        lines = contents_result.output.strip().split("\n")
        items = [json.loads(line) for line in lines if line.strip()]

        # Should have items
        assert len(items) > 0

        # All items should be functions
        for item in items:
            assert item["type"] == "function"

    def test_schema_contents_requires_type(self, example_worker: str) -> None:
        """Schema contents requires --type parameter."""
        runner = CliRunner()

        # Attach to get attach_id
        attach_result = runner.invoke(
            cli,
            ["catalog", "attach", "example", "--worker", example_worker],
        )
        attach_id = json.loads(attach_result.output)["attach_id"]

        # List schema contents without --type should fail
        contents_result = runner.invoke(
            cli,
            [
                "catalog",
                "schema",
                "contents",
                "main",
                "--attach-id",
                attach_id,
                "--worker",
                example_worker,
            ],
        )
        assert contents_result.exit_code != 0
        assert (
            "Missing option" in contents_result.output
            or "required" in contents_result.output.lower()
        )

    def test_schema_contents_with_catalog_option(self, example_worker: str) -> None:
        """Schema contents works with --catalog option for auto-attach."""
        runner = CliRunner()

        # List schema contents using --catalog option (auto-attach)
        contents_result = runner.invoke(
            cli,
            [
                "catalog",
                "schema",
                "contents",
                "main",
                "--catalog",
                "example",
                "--worker",
                example_worker,
                "--type",
                "table_function",
            ],
        )
        assert contents_result.exit_code == 0

        # Parse all output lines as JSON
        lines = contents_result.output.strip().split("\n")
        items = [json.loads(line) for line in lines if line.strip()]

        # Should have items
        assert len(items) > 0

        # All items should be functions
        for item in items:
            assert item["type"] == "function"

    def test_all_example_functions_present(self, example_worker: str) -> None:
        """All expected example functions are listed via CLI."""
        runner = CliRunner()

        # Attach to catalog
        attach_result = runner.invoke(
            cli,
            ["catalog", "attach", "example", "--worker", example_worker],
        )
        assert attach_result.exit_code == 0
        attach_id = json.loads(attach_result.output)["attach_id"]

        # Get table functions
        table_result = runner.invoke(
            cli,
            [
                "catalog",
                "schema",
                "contents",
                "main",
                "--attach-id",
                attach_id,
                "--worker",
                example_worker,
                "--type",
                "table_function",
            ],
        )
        assert table_result.exit_code == 0

        # Get scalar functions
        scalar_result = runner.invoke(
            cli,
            [
                "catalog",
                "schema",
                "contents",
                "main",
                "--attach-id",
                attach_id,
                "--worker",
                example_worker,
                "--type",
                "scalar_function",
            ],
        )
        assert scalar_result.exit_code == 0

        # Parse function names from both results
        function_names: set[str] = set()
        for result in [table_result, scalar_result]:
            lines = result.output.strip().split("\n")
            items = [json.loads(line) for line in lines if line.strip()]
            function_names.update(item["name"] for item in items)

        # Get expected functions from ExampleWorker
        expected_functions = _get_expected_function_names()

        # All expected functions should be present
        missing = expected_functions - function_names
        assert not missing, f"Missing functions: {missing}"

        # No extra functions should be present
        extra = function_names - expected_functions
        assert not extra, f"Unexpected functions: {extra}"

    def test_function_info_has_correct_structure(self, example_worker: str) -> None:
        """Function info from schema contents has expected fields."""
        runner = CliRunner()

        # Attach and get contents
        attach_result = runner.invoke(
            cli,
            ["catalog", "attach", "example", "--worker", example_worker],
        )
        attach_id = json.loads(attach_result.output)["attach_id"]

        contents_result = runner.invoke(
            cli,
            [
                "catalog",
                "schema",
                "contents",
                "main",
                "--attach-id",
                attach_id,
                "--worker",
                example_worker,
                "--type",
                "table_function",
            ],
        )
        assert contents_result.exit_code == 0

        # Parse first function
        lines = contents_result.output.strip().split("\n")
        first_func = json.loads(lines[0])

        # Check expected fields
        assert first_func["type"] == "function"
        assert "name" in first_func
        assert "schema_name" in first_func
        assert first_func["schema_name"] == "main"
        assert "function_type" in first_func
        assert first_func["function_type"] == "table"

    def test_scalar_functions_have_correct_type(self, example_worker: str) -> None:
        """Scalar functions are marked as function_type='scalar'."""
        runner = CliRunner()

        # Attach and get contents
        attach_result = runner.invoke(
            cli,
            ["catalog", "attach", "example", "--worker", example_worker],
        )
        attach_id = json.loads(attach_result.output)["attach_id"]

        contents_result = runner.invoke(
            cli,
            [
                "catalog",
                "schema",
                "contents",
                "main",
                "--attach-id",
                attach_id,
                "--worker",
                example_worker,
                "--type",
                "scalar_function",
            ],
        )
        assert contents_result.exit_code == 0

        # Parse scalar functions
        lines = contents_result.output.strip().split("\n")
        items = [json.loads(line) for line in lines if line.strip()]
        by_name = {item["name"]: item for item in items}

        # Check known scalar functions
        assert by_name["double"]["function_type"] == "scalar"
        assert by_name["add_values"]["function_type"] == "scalar"
        assert by_name["upper_case"]["function_type"] == "scalar"

    def test_table_functions_have_correct_type(self, example_worker: str) -> None:
        """Table functions are marked as function_type='table'."""
        runner = CliRunner()

        # Attach and get contents
        attach_result = runner.invoke(
            cli,
            ["catalog", "attach", "example", "--worker", example_worker],
        )
        attach_id = json.loads(attach_result.output)["attach_id"]

        contents_result = runner.invoke(
            cli,
            [
                "catalog",
                "schema",
                "contents",
                "main",
                "--attach-id",
                attach_id,
                "--worker",
                example_worker,
                "--type",
                "table_function",
            ],
        )
        assert contents_result.exit_code == 0

        # Parse table functions
        lines = contents_result.output.strip().split("\n")
        items = [json.loads(line) for line in lines if line.strip()]
        by_name = {item["name"]: item for item in items}

        # Check known table functions (generators and table-in-out)
        assert by_name["echo"]["function_type"] == "table"
        assert by_name["sequence"]["function_type"] == "table"
        assert by_name["sum_all_columns"]["function_type"] == "table"

    def test_varargs_function_shows_varargs_in_arguments(
        self, example_worker: str
    ) -> None:
        """Varargs functions show varargs indicator in arguments."""
        runner = CliRunner()

        # Attach and get contents
        attach_result = runner.invoke(
            cli,
            ["catalog", "attach", "example", "--worker", example_worker],
        )
        attach_id = json.loads(attach_result.output)["attach_id"]

        # sum_values is a scalar function
        contents_result = runner.invoke(
            cli,
            [
                "catalog",
                "schema",
                "contents",
                "main",
                "--attach-id",
                attach_id,
                "--worker",
                example_worker,
                "--type",
                "scalar_function",
            ],
        )
        assert contents_result.exit_code == 0

        # Parse and find sum_values function
        lines = contents_result.output.strip().split("\n")
        items = [json.loads(line) for line in lines if line.strip()]
        by_name = {item["name"]: item for item in items}

        # Verify sum_values function exists
        assert "sum_values" in by_name, "sum_values function not found"

        sum_func = by_name["sum_values"]

        # Verify arguments include varargs indicator
        arguments = sum_func.get("arguments", [])
        values_arg = next(
            (a for a in arguments if a.get("name") == "values"),
            None,
        )
        assert values_arg is not None, "values argument not found"
        assert values_arg.get("varargs") is True, "varargs indicator missing"


class TestCLISchemaList:
    """Tests for listing schemas via CLI."""

    def test_schema_list_shows_main(self, example_worker: str) -> None:
        """Schema list shows 'main' schema."""
        runner = CliRunner()

        # Attach first
        attach_result = runner.invoke(
            cli,
            ["catalog", "attach", "example", "--worker", example_worker],
        )
        attach_id = json.loads(attach_result.output)["attach_id"]

        # List schemas using --attach-id option
        list_result = runner.invoke(
            cli,
            [
                "catalog",
                "schema",
                "list",
                "--attach-id",
                attach_id,
                "--worker",
                example_worker,
            ],
        )
        assert list_result.exit_code == 0

        # Parse schema info (NDJSON - one JSON object per line)
        # Get the first schema which should be "main"
        lines = list_result.output.strip().split("\n")
        schema_info = json.loads(lines[0])
        assert schema_info["name"] == "main"

    def test_schema_list_with_catalog_option(self, example_worker: str) -> None:
        """Schema list works with --catalog option."""
        runner = CliRunner()

        # List schemas using --catalog option (auto-attach)
        list_result = runner.invoke(
            cli,
            [
                "catalog",
                "schema",
                "list",
                "--catalog",
                "example",
                "--worker",
                example_worker,
            ],
        )
        assert list_result.exit_code == 0

        # Parse schema info (NDJSON - one JSON object per line)
        # Get the first schema which should be "main"
        lines = list_result.output.strip().split("\n")
        schema_info = json.loads(lines[0])
        assert schema_info["name"] == "main"


class TestCLIAttachIdCatalogOptions:
    """Tests for --attach-id and --catalog option validation."""

    def test_mutual_exclusivity(self, example_worker: str) -> None:
        """Error when both --attach-id and --catalog are specified."""
        runner = CliRunner()

        # First get an attach_id
        attach_result = runner.invoke(
            cli,
            ["catalog", "attach", "example", "--worker", example_worker],
        )
        attach_id = json.loads(attach_result.output)["attach_id"]

        # Try to use both options
        result = runner.invoke(
            cli,
            [
                "catalog",
                "schema",
                "list",
                "--attach-id",
                attach_id,
                "--catalog",
                "example",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code != 0
        assert "Cannot specify both" in result.output

    def test_requires_attach_id_or_catalog(self, example_worker: str) -> None:
        """Error when neither --attach-id nor --catalog is specified."""
        runner = CliRunner()

        result = runner.invoke(
            cli,
            [
                "catalog",
                "schema",
                "list",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code != 0
        assert "Must specify either" in result.output
