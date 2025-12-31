"""Command-line interface for the VGI client.

This module provides the CLI entry point for invoking VGI functions.

Usage:
    # Table-in-out functions (with input):
    vgi-client --input data.parquet --function echo
    vgi-client --input data.parquet --function sum_all_columns
    vgi-client --input data.parquet --function repeat_inputs --args '[3]'

    # Table functions (no input):
    vgi-client --function sequence --args '[100]'
    vgi-client --function range --args '[0, 10]'

    # Specify table input position (for functions where TableInput isn't first):
    vgi-client --input data.parquet --function transform --args '["prefix"]' \
        --table-input-position 1

"""

import json
import sys
from typing import Any

import pyarrow as pa

from vgi.client.client import Client, ClientError, log
from vgi.function import Arguments


class OutputWriter:
    """Handles writing output batches in various formats."""

    def __init__(
        self, output_file: str | None, format: str, schema: pa.Schema | None = None
    ):
        """Initialize the output writer.

        Args:
            output_file: Path to output file, "-" for stdout, or None for logging.
            format: Output format ("parquet", "csv", or "json").
            schema: Optional schema for the output data.

        """
        self.output_file = output_file
        self.format = format
        self.schema = schema
        self._writer: Any = None
        self._is_stdout = output_file == "-"
        self._first_write = True

    def _get_output_stream(self) -> Any:
        if self._is_stdout:
            return sys.stdout.buffer if self.format == "parquet" else sys.stdout
        return self.output_file

    def write_batch(self, batch: pa.RecordBatch) -> None:
        """Write a batch to the output destination in the configured format."""
        import pyarrow.csv as csv
        import pyarrow.parquet as pq

        if self.output_file is None:
            log.info("output_batch", num_rows=batch.num_rows, batch=batch)
            return

        if self.format == "parquet":
            if self._writer is None:
                if self._is_stdout:
                    self._writer = pq.ParquetWriter(
                        pa.PythonFile(sys.stdout.buffer, mode="w"), batch.schema
                    )
                else:
                    self._writer = pq.ParquetWriter(self.output_file, batch.schema)
            self._writer.write_batch(batch)

        elif self.format == "csv":
            output = self._get_output_stream()
            write_options = csv.WriteOptions(include_header=self._first_write)
            if self._is_stdout:
                csv.write_csv(
                    pa.Table.from_batches([batch]), sys.stdout.buffer, write_options
                )
            else:
                if self._first_write:
                    csv.write_csv(pa.Table.from_batches([batch]), output, write_options)
                else:
                    with open(output, "ab") as f:
                        csv.write_csv(
                            pa.Table.from_batches([batch]),
                            f,
                            csv.WriteOptions(include_header=False),
                        )
            self._first_write = False

        elif self.format == "json":
            table = pa.Table.from_batches([batch])
            rows = table.to_pylist()
            if self._is_stdout:
                for row in rows:
                    print(json.dumps(row))
            else:
                mode = "w" if self._first_write else "a"
                with open(self.output_file, mode) as f:
                    for row in rows:
                        f.write(json.dumps(row) + "\n")
            self._first_write = False

    def close(self) -> None:
        """Close the underlying writer if one exists."""
        if self._writer is not None:
            self._writer.close()


def main() -> None:
    """CLI entry point for vgi-client."""
    import click
    import pyarrow.parquet as pq

    @click.command()
    @click.option(
        "--input",
        "input_file",
        required=False,
        # This validates the that file exists.
        type=click.Path(exists=True),
        help="Path to input parquet file (omit for table functions without input)",
    )
    @click.option(
        "--output",
        "output_file",
        type=str,
        help="Path to output file (use - for stdout)",
    )
    @click.option(
        "--format",
        "output_format",
        type=click.Choice(["json", "csv", "parquet"]),
        default="json",
        help="Output format (default: json)",
    )
    @click.option(
        "--function",
        "function_name",
        required=True,
        type=str,
        help="Name of the function to run (e.g., echo, sum_all_columns, repeat_inputs)",
    )
    @click.option(
        "--args",
        "arguments",
        default="[]",
        type=str,
        help="JSON array of arguments to pass to the function (default: [])",
    )
    @click.option(
        "--server",
        "server_path",
        default="vgi-example-worker",
        type=str,
        help="Path to the VGI worker",
    )
    @click.option(
        "--worker-stderr",
        "worker_stderr",
        is_flag=True,
        default=False,
        help="Pass worker stderr through to CLI stderr",
    )
    @click.option(
        "--projection-id",
        "projection_ids",
        multiple=True,
        type=int,
        help="Projection column ID (can be specified multiple times)",
    )
    @click.option(
        "--max-workers",
        "max_workers",
        type=int,
        default=None,
        help="Maximum number of worker processes (clamps function's max_processes)",
    )
    @click.option(
        "--table-input-position",
        "table_input_position",
        type=int,
        default=None,
        help=(
            "Position in positional arguments where table input should be inserted "
            "(0-indexed). If not specified, table input is not included in positional "
            "args. E.g., --args '[\"prefix\"]' --table-input-position 1 inserts "
            'table input at position 1, resulting in ("prefix", TABLE_INPUT).'
        ),
    )
    def cli(
        input_file: str | None,
        output_file: str | None,
        output_format: str,
        function_name: str,
        arguments: str,
        server_path: str,
        worker_stderr: bool,
        projection_ids: tuple[int, ...],
        max_workers: int | None,
        table_input_position: int | None,
    ) -> None:
        """Invoke a VGI function and display results."""
        try:
            args_list = json.loads(arguments)
            if not isinstance(args_list, list):
                raise click.ClickException("--args must be a JSON array")
        except json.JSONDecodeError as e:
            log.error("invalid_json_arguments", error=str(e))
            raise click.ClickException(f"Invalid JSON in --args: {e}") from e

        # Validate table_input_position
        if table_input_position is not None:
            if input_file is None:
                raise click.ClickException(
                    "--table-input-position requires --input to be specified"
                )
            if table_input_position < 0:
                raise click.ClickException(
                    "--table-input-position must be non-negative"
                )
            if table_input_position > len(args_list):
                raise click.ClickException(
                    f"--table-input-position {table_input_position} is out of range "
                    f"for {len(args_list)} arguments (max: {len(args_list)})"
                )

        # Convert args_list to PyArrow scalars
        positional_args = tuple(pa.scalar(arg) for arg in args_list)

        log.info("starting_server", function=function_name, server_path=server_path)

        output_writer: OutputWriter | None = None
        try:
            with Client(
                server_path, passthrough_stderr=worker_stderr, max_workers=max_workers
            ) as client:
                if input_file is None:
                    # Table function (no input) - use table_function method
                    log.info("invoking_table_function", function=function_name)
                    output_iterator = client.table_function(
                        function_name=function_name,
                        arguments=Arguments(positional=positional_args, named={}),
                        projection_ids=list(projection_ids) if projection_ids else None,
                    )
                else:
                    # Table-in-out function - use table_in_out_function method
                    log.info("reading_input", file=input_file)
                    pf = pq.ParquetFile(input_file)

                    # If table_input_position is specified, log it for debugging
                    # The table input position tells the user where the table data
                    # appears in the function signature (e.g., position 1 means the
                    # table is the second argument). This is purely informational
                    # for the CLI user - the protocol handles table data separately.
                    if table_input_position is not None:
                        log.debug(
                            "table_input_position_specified",
                            position=table_input_position,
                            num_args=len(positional_args),
                        )

                    output_iterator = client.table_in_out_function(
                        function_name=function_name,
                        arguments=Arguments(positional=positional_args, named={}),
                        input=pf.iter_batches(),
                        projection_ids=list(projection_ids) if projection_ids else None,
                    )

                for output_batch in output_iterator:
                    if output_writer is None:
                        output_writer = OutputWriter(
                            output_file, output_format, output_batch.schema
                        )
                    output_writer.write_batch(output_batch)

            log.info("processing_complete", function=function_name)
        except ClientError as e:
            raise click.ClickException(str(e)) from e
        finally:
            if output_writer is not None:
                output_writer.close()

    cli()


if __name__ == "__main__":
    main()
