"""Tests for VGI CLI client."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from vgi.client.cli import OutputWriter, cli


@pytest.fixture
def sample_batch() -> pa.RecordBatch:
    """Create a simple test RecordBatch."""
    return pa.RecordBatch.from_pydict(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
        }
    )


@pytest.fixture
def input_parquet(tmp_path: Path, sample_batch: pa.RecordBatch) -> Path:
    """Create a temporary parquet file with sample data."""
    input_file = tmp_path / "input.parquet"
    pq.write_table(pa.Table.from_batches([sample_batch]), str(input_file))
    return input_file


class TestOutputWriter:
    """Tests for OutputWriter class."""

    def test_write_batch_to_log_when_no_output_file(
        self, sample_batch: pa.RecordBatch
    ) -> None:
        """When output_file is None, batch is logged, not written."""
        writer = OutputWriter(None, "json")
        # Should not raise - just logs the batch
        writer.write_batch(sample_batch)
        writer.close()

    def test_write_batch_parquet_to_file(
        self, tmp_path: Path, sample_batch: pa.RecordBatch
    ) -> None:
        """Write parquet format to file path."""
        output_file = tmp_path / "output.parquet"
        writer = OutputWriter(str(output_file), "parquet")
        writer.write_batch(sample_batch)
        writer.close()

        # Verify file is valid parquet with correct data
        table = pq.read_table(str(output_file))
        assert table.num_rows == 3
        assert table.column_names == ["id", "name"]

    def test_write_batch_parquet_multiple_batches(
        self, tmp_path: Path, sample_batch: pa.RecordBatch
    ) -> None:
        """Write multiple parquet batches to same file."""
        output_file = tmp_path / "output.parquet"
        writer = OutputWriter(str(output_file), "parquet")
        writer.write_batch(sample_batch)
        writer.write_batch(sample_batch)
        writer.close()

        table = pq.read_table(str(output_file))
        assert table.num_rows == 6

    def test_write_batch_csv_to_file(
        self, tmp_path: Path, sample_batch: pa.RecordBatch
    ) -> None:
        """Write CSV format to file."""
        output_file = tmp_path / "output.csv"
        writer = OutputWriter(str(output_file), "csv")
        writer.write_batch(sample_batch)
        writer.close()

        content = output_file.read_text()
        # Check header is present
        assert "id" in content
        assert "name" in content
        # Check data is present
        assert "1" in content
        assert "a" in content

    def test_write_batch_csv_header_once(
        self, tmp_path: Path, sample_batch: pa.RecordBatch
    ) -> None:
        """CSV header should only be written on first batch."""
        output_file = tmp_path / "output.csv"
        writer = OutputWriter(str(output_file), "csv")
        writer.write_batch(sample_batch)
        writer.write_batch(sample_batch)
        writer.close()

        content = output_file.read_text()
        # Header should appear exactly once
        lines = content.strip().split("\n")
        header_count = sum(1 for line in lines if "id" in line and "name" in line)
        assert header_count == 1
        # Should have 6 data rows + 1 header = 7 lines
        assert len(lines) == 7

    def test_write_batch_json_to_file(
        self, tmp_path: Path, sample_batch: pa.RecordBatch
    ) -> None:
        """Write JSON format to file."""
        output_file = tmp_path / "output.jsonl"
        writer = OutputWriter(str(output_file), "json")
        writer.write_batch(sample_batch)
        writer.close()

        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == 3
        row1 = json.loads(lines[0])
        assert row1["id"] == 1
        assert row1["name"] == "a"

    def test_write_batch_json_multiple_batches(
        self, tmp_path: Path, sample_batch: pa.RecordBatch
    ) -> None:
        """Write multiple JSON batches to same file."""
        output_file = tmp_path / "output.jsonl"
        writer = OutputWriter(str(output_file), "json")
        writer.write_batch(sample_batch)
        writer.write_batch(sample_batch)
        writer.close()

        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == 6

    def test_close_without_writer(self) -> None:
        """close() is safe when no writer was created."""
        writer = OutputWriter(None, "json")
        writer.close()  # Should not raise


class TestCLIValidation:
    """Tests for CLI argument validation."""

    def test_invalid_json_args(self, example_worker: str) -> None:
        """Invalid JSON in --args should raise error."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--function",
                "echo",
                "--args",
                "not valid json",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code != 0
        assert "Invalid JSON" in result.output

    def test_args_not_array(self, example_worker: str) -> None:
        """--args must be a JSON array, not object."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--function",
                "echo",
                "--args",
                '{"key": "value"}',
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code != 0
        assert "must be a JSON array" in result.output

    def test_table_input_position_without_input(self, example_worker: str) -> None:
        """--table-input-position requires --input."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--function",
                "sequence",
                "--args",
                "[10]",
                "--table-input-position",
                "1",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code != 0
        assert "requires --input" in result.output

    def test_table_input_position_negative(
        self, example_worker: str, input_parquet: Path
    ) -> None:
        """--table-input-position must be non-negative."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(input_parquet),
                "--function",
                "echo",
                "--table-input-position",
                "-1",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code != 0
        assert "non-negative" in result.output

    def test_table_input_position_out_of_range(
        self, example_worker: str, input_parquet: Path
    ) -> None:
        """--table-input-position out of range for args."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(input_parquet),
                "--function",
                "echo",
                "--args",
                "[1, 2]",
                "--table-input-position",
                "5",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code != 0
        assert "out of range" in result.output

    def test_invalid_attach_id_hex(self, example_worker: str) -> None:
        """--attach-id must be valid hex."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--function",
                "sequence",
                "--args",
                "[5]",
                "--attach-id",
                "not_hex_string",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code != 0
        assert "valid hex string" in result.output

    def test_missing_function_shows_help(self) -> None:
        """Calling CLI with no arguments shows help (group behavior)."""
        runner = CliRunner()
        result = runner.invoke(cli, [])
        # With Click group, no subcommand and no --function shows help
        assert result.exit_code == 0
        assert "Usage:" in result.output


class TestCLITableFunction:
    """Tests for CLI table function invocation (no input)."""

    def test_table_function_invocation(self, example_worker: str) -> None:
        """Invoke a table function without input."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--function",
                "sequence",
                "--args",
                "[5]",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0

    def test_table_function_with_output_file(
        self, example_worker: str, tmp_path: Path
    ) -> None:
        """Table function with output to file."""
        output_file = tmp_path / "output.jsonl"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--function",
                "sequence",
                "--args",
                "[5]",
                "--output",
                str(output_file),
                "--format",
                "json",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0
        assert output_file.exists()
        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == 5


class TestCLITableInOutFunction:
    """Tests for CLI table-in-out function invocation (with input)."""

    def test_table_in_out_function_invocation(
        self, example_worker: str, input_parquet: Path
    ) -> None:
        """Invoke a table-in-out function with input."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(input_parquet),
                "--function",
                "echo",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0

    def test_table_in_out_with_output_file(
        self, example_worker: str, input_parquet: Path, tmp_path: Path
    ) -> None:
        """Table-in-out function with output to file."""
        output_file = tmp_path / "output.jsonl"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(input_parquet),
                "--output",
                str(output_file),
                "--format",
                "json",
                "--function",
                "echo",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0
        assert output_file.exists()
        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == 3


class TestCLIOutputFormats:
    """Tests for CLI output format options."""

    def test_output_format_json(
        self, example_worker: str, input_parquet: Path, tmp_path: Path
    ) -> None:
        """JSON output format."""
        output_file = tmp_path / "output.json"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(input_parquet),
                "--output",
                str(output_file),
                "--format",
                "json",
                "--function",
                "echo",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0
        # Verify it's valid JSON
        for line in output_file.read_text().strip().split("\n"):
            json.loads(line)

    def test_output_format_csv(
        self, example_worker: str, input_parquet: Path, tmp_path: Path
    ) -> None:
        """CSV output format."""
        output_file = tmp_path / "output.csv"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(input_parquet),
                "--output",
                str(output_file),
                "--format",
                "csv",
                "--function",
                "echo",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0
        content = output_file.read_text()
        assert "id" in content  # Header present

    def test_output_format_parquet(
        self, example_worker: str, input_parquet: Path, tmp_path: Path
    ) -> None:
        """Parquet output format."""
        output_file = tmp_path / "output.parquet"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(input_parquet),
                "--output",
                str(output_file),
                "--format",
                "parquet",
                "--function",
                "echo",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0
        # Verify it's valid parquet
        table = pq.read_table(str(output_file))
        assert table.num_rows == 3

    def test_output_format_arrow_ipc(
        self, example_worker: str, input_parquet: Path, tmp_path: Path
    ) -> None:
        """Arrow IPC streaming output format."""
        from pyarrow import ipc

        output_file = tmp_path / "output.arrow"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(input_parquet),
                "--output",
                str(output_file),
                "--format",
                "arrow-ipc",
                "--function",
                "echo",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0
        # Verify it's valid Arrow IPC
        with open(output_file, "rb") as f:
            reader = ipc.open_stream(f)
            table = reader.read_all()
        assert table.num_rows == 3
        assert "id" in table.schema.names


class TestCLIOptions:
    """Tests for various CLI options."""

    def test_max_workers_option(self, example_worker: str, input_parquet: Path) -> None:
        """--max-workers option is passed correctly."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(input_parquet),
                "--function",
                "echo",
                "--max-workers",
                "2",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0

    def test_worker_stderr_passthrough(
        self, example_worker: str, input_parquet: Path
    ) -> None:
        """--worker-stderr flag works."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(input_parquet),
                "--function",
                "echo",
                "--worker-stderr",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0

    def test_valid_attach_id(self, example_worker: str) -> None:
        """Valid hex attach-id is accepted."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--function",
                "sequence",
                "--args",
                "[5]",
                "--attach-id",
                "deadbeef",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0

    def test_projection_ids(self, example_worker: str, input_parquet: Path) -> None:
        """--projection-id option works."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(input_parquet),
                "--function",
                "echo",
                "--projection-id",
                "0",
                "--projection-id",
                "1",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0

    def test_table_input_position_valid(
        self, example_worker: str, input_parquet: Path
    ) -> None:
        """Valid --table-input-position is accepted."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(input_parquet),
                "--function",
                "echo",
                "--args",
                "[]",
                "--table-input-position",
                "0",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0


class TestCLIErrorHandling:
    """Tests for CLI error handling."""

    def test_nonexistent_function(self, example_worker: str) -> None:
        """Non-existent function returns error."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--function",
                "nonexistent_function_xyz",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code != 0

    def test_stdout_output_json(self, example_worker: str) -> None:
        """Output to stdout with - works."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--function",
                "sequence",
                "--args",
                "[3]",
                "--output",
                "-",
                "--format",
                "json",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0
        # Should have JSON output
        assert "n" in result.output


class TestCLIScalarFunction:
    """Tests for CLI scalar function invocation with --type scalar."""

    @pytest.fixture
    def scalar_input_parquet(self, tmp_path: Path) -> Path:
        """Create a parquet file suitable for scalar function tests."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3, 4, 5]})
        input_file = tmp_path / "scalar_input.parquet"
        pq.write_table(pa.Table.from_batches([batch]), str(input_file))
        return input_file

    def test_scalar_function_invocation(
        self, example_worker: str, scalar_input_parquet: Path
    ) -> None:
        """Invoke a scalar function with --type scalar."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(scalar_input_parquet),
                "--function",
                "double",
                "--args",
                '["x"]',
                "--type",
                "scalar",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0

    def test_scalar_function_with_output_file(
        self, example_worker: str, scalar_input_parquet: Path, tmp_path: Path
    ) -> None:
        """Scalar function with output to file."""
        output_file = tmp_path / "output.jsonl"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(scalar_input_parquet),
                "--output",
                str(output_file),
                "--format",
                "json",
                "--function",
                "double",
                "--args",
                '["x"]',
                "--type",
                "scalar",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0
        assert output_file.exists()
        lines = output_file.read_text().strip().split("\n")
        # Should have 5 rows
        assert len(lines) == 5
        # Verify first row is doubled
        first_row = json.loads(lines[0])
        assert first_row["result"] == 2

    def test_scalar_function_parquet_output(
        self, example_worker: str, scalar_input_parquet: Path, tmp_path: Path
    ) -> None:
        """Scalar function with parquet output."""
        output_file = tmp_path / "output.parquet"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(scalar_input_parquet),
                "--output",
                str(output_file),
                "--format",
                "parquet",
                "--function",
                "double",
                "--args",
                '["x"]',
                "--type",
                "scalar",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0
        # Verify parquet output
        table = pq.read_table(str(output_file))
        assert table.num_rows == 5
        assert table.column_names == ["result"]
        assert table.column("result").to_pylist() == [2, 4, 6, 8, 10]

    def test_scalar_type_requires_input(self, example_worker: str) -> None:
        """--type scalar requires --input."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--function",
                "double",
                "--args",
                '["x"]',
                "--type",
                "scalar",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code != 0
        assert "requires --input" in result.output

    def test_table_in_out_type_requires_input(self, example_worker: str) -> None:
        """--type table-in-out requires --input."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--function",
                "echo",
                "--type",
                "table-in-out",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code != 0
        assert "requires --input" in result.output

    def test_table_type_rejects_input(
        self, example_worker: str, scalar_input_parquet: Path
    ) -> None:
        """--type table does not accept --input."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(scalar_input_parquet),
                "--function",
                "sequence",
                "--args",
                "[5]",
                "--type",
                "table",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code != 0
        assert "does not accept --input" in result.output

    def test_auto_type_with_input_uses_table_in_out(
        self, example_worker: str, scalar_input_parquet: Path, tmp_path: Path
    ) -> None:
        """--type auto with --input uses table-in-out (echo function)."""
        output_file = tmp_path / "output.jsonl"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(scalar_input_parquet),
                "--output",
                str(output_file),
                "--format",
                "json",
                "--function",
                "echo",
                "--type",
                "auto",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0
        # Echo should preserve the original column name "x"
        content = output_file.read_text()
        assert '"x"' in content

    def test_auto_type_without_input_uses_table(
        self, example_worker: str, tmp_path: Path
    ) -> None:
        """--type auto without --input uses table function."""
        output_file = tmp_path / "output.jsonl"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--output",
                str(output_file),
                "--format",
                "json",
                "--function",
                "sequence",
                "--args",
                "[3]",
                "--type",
                "auto",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0
        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == 3

    def test_scalar_with_add_values(self, example_worker: str, tmp_path: Path) -> None:
        """Test add_values scalar function via CLI."""
        # Create input with two columns
        batch = pa.RecordBatch.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]})
        input_file = tmp_path / "input.parquet"
        pq.write_table(pa.Table.from_batches([batch]), str(input_file))

        output_file = tmp_path / "output.jsonl"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(input_file),
                "--output",
                str(output_file),
                "--format",
                "json",
                "--function",
                "add_values",
                "--args",
                '["a", "b"]',
                "--type",
                "scalar",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0
        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == 3
        # Verify sums
        results = [json.loads(line)["result"] for line in lines]
        assert results == [11, 22, 33]

    def test_scalar_with_upper_case(self, example_worker: str, tmp_path: Path) -> None:
        """Test upper_case scalar function via CLI."""
        batch = pa.RecordBatch.from_pydict({"name": ["alice", "bob"]})
        input_file = tmp_path / "input.parquet"
        pq.write_table(pa.Table.from_batches([batch]), str(input_file))

        output_file = tmp_path / "output.jsonl"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--input",
                str(input_file),
                "--output",
                str(output_file),
                "--format",
                "json",
                "--function",
                "upper_case",
                "--args",
                '["name"]',
                "--type",
                "scalar",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0
        lines = output_file.read_text().strip().split("\n")
        results = [json.loads(line)["result"] for line in lines]
        assert results == ["ALICE", "BOB"]


class TestCLISettings:
    """Tests for CLI --setting option."""

    def test_setting_passed_to_table_function(
        self, example_worker: str, tmp_path: Path
    ) -> None:
        """Settings should be passed to table functions."""
        output_file = tmp_path / "output.jsonl"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--function",
                "settings_aware",
                "--args",
                "[3]",
                "--setting",
                "vgi_verbose_mode=false",
                "--setting",
                "greeting=Hi",
                "--setting",
                "multiplier=2",
                "--output",
                str(output_file),
                "--format",
                "json",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0
        assert output_file.exists()

        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == 3

        # Verify greeting is passed through
        row0 = json.loads(lines[0])
        assert row0["greeting"] == "Hi"

        # Verify multiplier affects value (value = id * 2.5 * multiplier)
        # id=0: 0*2.5*2 = 0.0, id=1: 1*2.5*2 = 5.0, id=2: 2*2.5*2 = 10.0
        values = [json.loads(line)["value"] for line in lines]
        assert values == [0.0, 5.0, 10.0]

    def test_setting_short_option(self, example_worker: str, tmp_path: Path) -> None:
        """Short -s option should work for settings."""
        output_file = tmp_path / "output.jsonl"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--function",
                "settings_aware",
                "--args",
                "[2]",
                "-s",
                "vgi_verbose_mode=false",
                "-s",
                "greeting=Hello",
                "-s",
                "multiplier=1",
                "--output",
                str(output_file),
                "--format",
                "json",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0

        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == 2
        row0 = json.loads(lines[0])
        assert row0["greeting"] == "Hello"

    def test_setting_verbose_mode_adds_details(
        self, example_worker: str, tmp_path: Path
    ) -> None:
        """vgi_verbose_mode=true should add details column."""
        output_file = tmp_path / "output.jsonl"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--function",
                "settings_aware",
                "--args",
                "[2]",
                "-s",
                "vgi_verbose_mode=true",
                "-s",
                "greeting=Test",
                "-s",
                "multiplier=1",
                "--output",
                str(output_file),
                "--format",
                "json",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0

        lines = output_file.read_text().strip().split("\n")
        row0 = json.loads(lines[0])
        # verbose mode adds the details column
        assert "details" in row0
        assert row0["details"] == "row_0"

    def test_setting_invalid_format_raises_error(self, example_worker: str) -> None:
        """Invalid setting format (missing =) should raise error."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--function",
                "sequence",
                "--args",
                "[5]",
                "--setting",
                "invalid_setting_no_equals",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code != 0
        assert "Invalid --setting format" in result.output

    def test_multiple_settings_combined(
        self, example_worker: str, tmp_path: Path
    ) -> None:
        """Multiple settings should all be passed through."""
        output_file = tmp_path / "output.jsonl"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--function",
                "settings_aware",
                "--args",
                "[1]",
                "-s",
                "vgi_verbose_mode=true",
                "-s",
                "greeting=CustomGreeting",
                "-s",
                "multiplier=10",
                "--output",
                str(output_file),
                "--format",
                "json",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0

        lines = output_file.read_text().strip().split("\n")
        row0 = json.loads(lines[0])
        assert row0["greeting"] == "CustomGreeting"
        # value = 0 * 2.5 * 10 = 0.0
        assert row0["value"] == 0.0
        assert "details" in row0  # verbose mode on

    def test_setting_with_equals_in_value(
        self, example_worker: str, tmp_path: Path
    ) -> None:
        """Settings with = in value should parse correctly."""
        output_file = tmp_path / "output.jsonl"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--function",
                "settings_aware",
                "--args",
                "[1]",
                "-s",
                "vgi_verbose_mode=false",
                "-s",
                "greeting=Hello=World",
                "-s",
                "multiplier=1",
                "--output",
                str(output_file),
                "--format",
                "json",
                "--worker",
                example_worker,
            ],
        )
        assert result.exit_code == 0

        lines = output_file.read_text().strip().split("\n")
        row0 = json.loads(lines[0])
        # The value should be "Hello=World" (split only on first =)
        assert row0["greeting"] == "Hello=World"
