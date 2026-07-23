# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Command-line interface for the VGI client.

This module provides the CLI entry point for invoking VGI functions and
managing catalogs.

Usage:
    # Table-in-out functions (with input):
    vgi-client --input data.parquet --function echo
    vgi-client --input data.parquet --function sum_all_columns
    vgi-client --input data.parquet --function repeat_inputs --args '[3]'

    # Table functions (no input):
    vgi-client --function sequence --args '[100]'
    vgi-client --function sequence --args '[5]' --named-arg increment=10
    vgi-client --function range --args '[0, 10]'

    # Scalar functions (with input, single-column output):
    vgi-client --input data.parquet --function double \
        --args '["x"]' --type scalar

    # Specify table input position (for functions where TableInput isn't first):
    vgi-client --input data.parquet --function transform --args '["prefix"]' \
        --table-input-position 1

    # Output in Arrow IPC format (useful for debugging):
    vgi-client --function sequence --args '[10]' --format arrow-ipc -o out.arrow
    vgi-client --function echo --input data.parquet --format arrow-ipc -o -

    # Catalog operations (all nested under 'catalog'):
    vgi-client catalog list --worker vgi-fixture-worker
    vgi-client catalog attach example --worker vgi-fixture-worker
    vgi-client catalog schema list $ATTACH_ID --worker vgi-fixture-worker
    vgi-client catalog schema contents $ATTACH_ID main --worker vgi-fixture-worker
    vgi-client catalog table get $ATTACH_ID main users --worker vgi-fixture-worker
    vgi-client catalog transaction begin $ATTACH_ID --worker vgi-fixture-worker

"""

import io
import json
import logging
import sys
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
from pyarrow import ipc

from vgi.arguments import Arguments
from vgi.client.client import Client, ClientError

_logger = logging.getLogger("vgi.client.cli")

if TYPE_CHECKING:
    import pyarrow.parquet as pq


class OutputWriter:
    """Handles writing output batches in various formats.

    Supported formats:
        - json: JSON Lines format (one JSON object per row)
        - csv: CSV with header
        - parquet: Apache Parquet columnar format
        - arrow-ipc: Apache Arrow IPC streaming format (useful for debugging)

    The arrow-ipc format writes batches in the standard Arrow IPC streaming
    format, which can be read by any Arrow implementation. This is useful for:
        - Debugging VGI protocol issues
        - Inspecting raw output data with tools like pyarrow or arrow CLI
        - Piping data to other Arrow-aware tools

    """

    def __init__(self, output_file: str | None, format: str, schema: pa.Schema | None = None):
        """Initialize the output writer.

        Args:
            output_file: Path to output file, "-" for stdout, or None for logging.
            format: Output format ("parquet", "csv", "json", or "arrow-ipc").
            schema: Optional schema for the output data.

        """
        self.output_file = output_file
        self.format = format
        self.schema = schema
        self._writer: pq.ParquetWriter | ipc.RecordBatchStreamWriter | None = None
        self._is_stdout = output_file == "-"
        self._first_write = True
        self._output_file_handle: io.IOBase | None = None

    def _get_output_stream(self) -> Any:
        if self._is_stdout:
            if self.format in ("parquet", "arrow-ipc"):
                return sys.stdout.buffer
            return sys.stdout
        return self.output_file

    def write_batch(self, batch: pa.RecordBatch) -> None:
        """Write a batch to the output destination in the configured format."""
        import pyarrow.csv as csv
        import pyarrow.parquet as pq

        if self.output_file is None:
            _logger.info("output_batch num_rows=%s batch=%s", batch.num_rows, batch)
            return

        if self.format == "parquet":
            if self._writer is None:
                if self._is_stdout:
                    self._writer = pq.ParquetWriter(
                        pa.PythonFile(cast(io.IOBase, sys.stdout.buffer), mode="w"),
                        batch.schema,
                    )
                else:
                    self._writer = pq.ParquetWriter(self.output_file, batch.schema)
            self._writer.write_batch(batch)

        elif self.format == "arrow-ipc":
            if self._writer is None:
                if self._is_stdout:
                    sink = pa.PythonFile(cast(io.IOBase, sys.stdout.buffer), mode="w")
                else:
                    # Open file and keep handle for closing in close()
                    self._output_file_handle = open(  # noqa: SIM115
                        self.output_file, "wb"
                    )
                    sink = pa.PythonFile(self._output_file_handle, mode="w")
                self._writer = ipc.new_stream(sink, batch.schema)
            # Type narrowing for mypy
            assert isinstance(self._writer, ipc.RecordBatchStreamWriter)
            self._writer.write_batch(batch)

        elif self.format == "csv":
            output = self._get_output_stream()
            write_options = csv.WriteOptions(include_header=self._first_write)
            if self._is_stdout:
                csv.write_csv(pa.Table.from_batches([batch]), sys.stdout.buffer, write_options)
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
        if self._output_file_handle is not None:
            self._output_file_handle.close()


_CLI_EPILOG = """
\b
EXAMPLES:
  # Table function (generates data, no input)
  vgi-client --function sequence --args '[10]'
  vgi-client --function sequence --args '[5]' --named-arg increment=10
\b
  # Table-in-out function (transforms input)
  vgi-client --input data.parquet --function echo
\b
  # Scalar function (per-row, single output column)
  vgi-client --input in.parquet --function upper_case -t scalar
\b
  # Output to file with format
  vgi-client --function sequence --args '[5]' -o out.json
  vgi-client --function sequence --args '[5]' -o out.parquet -f parquet
  vgi-client --function sequence --args '[5]' -o - -f arrow-ipc
\b
  # Catalog operations
  vgi-client catalog list -w vgi-fixture-worker
  vgi-client catalog attach mydb -w vgi-fixture-worker

\b
FUNCTION TYPES:
  table         No input, generates data (sequence, range)
  table-in-out  Transforms input (echo, filter) - default with --input
  scalar        Per-row transform, single column output (upper_case)
  auto          Default: table-in-out if --input, else table

\b
ARGUMENT FORMAT (--args as JSON array):
  '[]'              No arguments
  '[10]'            Single integer
  '["name"]'        Single string (column name)
  '[0, 100, 5]'     Multiple integers
  '[true, 3.14]'    Mixed types

\b
NAMED ARGUMENTS (--named-arg key=value):
  --named-arg increment=2       Integer value
  --named-arg name="test"       String value (use JSON quotes)
  --named-arg flag=true         Boolean value

\b
SETTINGS (-s/--setting key=value):
  -s vgi_verbose_mode=true      Enable verbose mode
  -s greeting=Hello             String setting
  -s multiplier=2               Integer setting (passed as string)

\b
OUTPUT FORMATS (-f/--format):
  json       JSON Lines, one object per row (default)
  csv        CSV with header row
  parquet    Apache Parquet columnar format
  arrow-ipc  Arrow IPC stream (for debugging/piping)

\b
ENVIRONMENT VARIABLES:
  VGI_WORKER_DEBUG=1  Enable DEBUG logging on worker and stderr passthrough on client
  VGI_QUIET=1         Suppress worker startup logging
"""


def _create_cli() -> Any:
    """Create the CLI command group. Separated for testability."""
    import click
    import pyarrow.parquet as pq

    from vgi.client.cli_catalog import catalog

    @click.group(invoke_without_command=True, epilog=_CLI_EPILOG)
    @click.option(
        "--input",
        "input_file",
        required=False,
        type=click.Path(exists=True),
        help=(
            "Input parquet file path. Required for table-in-out and scalar functions. "
            "Omit for table functions (generators)."
        ),
    )
    @click.option(
        "--output",
        "-o",
        "output_file",
        type=str,
        help="Output file path. Use '-' for stdout. If omitted, outputs to log.",
    )
    @click.option(
        "--format",
        "-f",
        "output_format",
        type=click.Choice(["json", "csv", "parquet", "arrow-ipc"]),
        default="json",
        help="Output format: json (default), csv, parquet, or arrow-ipc.",
    )
    @click.option(
        "--function",
        "function_name",
        required=False,
        type=str,
        help="Function name to invoke (e.g., sequence, echo, upper_case).",
    )
    @click.option(
        "--schema",
        "schema_name",
        default="main",
        show_default=True,
        type=str,
        help="Catalog schema declaring the function. A worker may register one name in several schemas.",
    )
    @click.option(
        "--args",
        "arguments",
        default="[]",
        type=str,
        help="JSON array of positional arguments. Example: '[10]' or '[\"col\"]'.",
    )
    @click.option(
        "--worker",
        "-w",
        "worker_path",
        default="vgi-fixture-worker",
        type=str,
        help="VGI worker command or path. Default: vgi-fixture-worker.",
    )
    @click.option(
        "--type",
        "-t",
        "function_type",
        type=click.Choice(["auto", "table", "table-in-out", "scalar"]),
        default="auto",
        help=(
            "Function type: auto (default), table, table-in-out, or scalar. "
            "'auto' uses table-in-out if --input provided, otherwise table."
        ),
    )
    @click.option(
        "--worker-stderr",
        is_flag=True,
        default=False,
        help="Pass worker stderr through to CLI stderr (for debugging).",
    )
    @click.option(
        "--max-workers",
        "max_workers",
        type=int,
        default=None,
        help="Max worker processes. Clamps function's max_processes setting.",
    )
    @click.option(
        "--projection-id",
        "projection_ids",
        multiple=True,
        type=int,
        help="Column ID for projection pushdown. Can be repeated.",
    )
    @click.option(
        "--pushdown-filters",
        "pushdown_filters",
        type=str,
        default=None,
        help="Filter predicates as hex-encoded bytes for filter pushdown.",
    )
    @click.option(
        "--table-input-position",
        "table_input_position",
        type=int,
        default=None,
        help=(
            "Position (0-indexed) to insert table input in positional args. "
            "Example: --args '[\"prefix\"]' --table-input-position 1"
        ),
    )
    @click.option(
        "--attach-opaque-data",
        "attach_opaque_data",
        type=str,
        default=None,
        help="DuckDB attachment ID (hex string) for catalog context.",
    )
    @click.option(
        "--transaction-opaque-data",
        "transaction_opaque_data",
        type=str,
        default=None,
        help="DuckDB transaction ID (hex string) for transactional operations.",
    )
    @click.option(
        "--named-arg",
        "named_arg_list",
        multiple=True,
        type=str,
        help="Named argument as key=value. Can be repeated. E.g.: --named-arg x=2",
    )
    @click.option(
        "--setting",
        "-s",
        "setting_list",
        multiple=True,
        type=str,
        help="Setting as key=value. Can be repeated. E.g.: -s greeting=Hi",
    )
    @click.pass_context
    def cli(
        ctx: click.Context,
        input_file: str | None,
        output_file: str | None,
        output_format: str,
        function_name: str | None,
        schema_name: str,
        arguments: str,
        worker_path: str,
        worker_stderr: bool,
        projection_ids: tuple[int, ...],
        pushdown_filters: str | None,
        max_workers: int | None,
        table_input_position: int | None,
        attach_opaque_data: str | None,
        function_type: str,
        transaction_opaque_data: str | None,
        named_arg_list: tuple[str, ...],
        setting_list: tuple[str, ...],
    ) -> None:
        """VGI client - invoke functions and manage catalogs.

        QUICK START: Use --function to invoke a VGI function, or use the
        'catalog' subcommand for catalog operations. See examples below.
        """
        # If a subcommand is being invoked, skip function invocation
        if ctx.invoked_subcommand is not None:
            return

        # Legacy function invocation mode - requires --function
        if function_name is None:
            click.echo(ctx.get_help())
            return

        try:
            args_list = json.loads(arguments)
            if not isinstance(args_list, list):
                raise click.ClickException("--args must be a JSON array")
        except json.JSONDecodeError as e:
            _logger.error("invalid_json_arguments error=%s", e)
            raise click.ClickException(f"Invalid JSON in --args: {e}") from e

        # Validate table_input_position
        if table_input_position is not None:
            if input_file is None:
                raise click.ClickException("--table-input-position requires --input to be specified")
            if table_input_position < 0:
                raise click.ClickException("--table-input-position must be non-negative")
            if table_input_position > len(args_list):
                raise click.ClickException(
                    f"--table-input-position {table_input_position} is out of range "
                    f"for {len(args_list)} arguments (max: {len(args_list)})"
                )

        # Convert args_list to PyArrow scalars
        positional_args = tuple(pa.scalar(arg) for arg in args_list)

        # Parse named arguments into dict
        named_args: dict[str, pa.Scalar[Any]] = {}
        for named_arg in named_arg_list:
            if "=" not in named_arg:
                raise click.ClickException(f"Invalid --named-arg format: '{named_arg}'. Expected key=value.")
            key, value_str = named_arg.split("=", 1)
            # Try to parse value as JSON, fall back to string
            try:
                value = json.loads(value_str)
            except json.JSONDecodeError:
                # Treat as string if not valid JSON
                value = value_str
            named_args[key] = pa.scalar(value)

        # Parse settings into dict (settings are always strings in the protocol)
        settings: dict[str, str] | None = None
        if setting_list:
            settings = {}
            for setting in setting_list:
                if "=" not in setting:
                    raise click.ClickException(f"Invalid --setting format: '{setting}'. Expected key=value.")
                key, value_str = setting.split("=", 1)
                settings[key] = value_str

        # Parse attach_opaque_data from hex string if provided
        attach_opaque_data_bytes: bytes | None = None
        if attach_opaque_data is not None:
            try:
                attach_opaque_data_bytes = bytes.fromhex(attach_opaque_data)
            except ValueError as e:
                raise click.ClickException(f"Invalid --attach-opaque-data: must be a valid hex string: {e}") from e

        # Parse transaction_opaque_data from hex string if provided
        transaction_opaque_data_bytes: bytes | None = None
        if transaction_opaque_data is not None:
            try:
                transaction_opaque_data_bytes = bytes.fromhex(transaction_opaque_data)
            except ValueError as e:
                raise click.ClickException(f"Invalid --transaction-opaque-data: must be a valid hex string: {e}") from e

        # Parse pushdown_filters from hex string if provided
        pushdown_filters_bytes: bytes | None = None
        if pushdown_filters is not None:
            try:
                pushdown_filters_bytes = bytes.fromhex(pushdown_filters)
            except ValueError as e:
                raise click.ClickException(f"Invalid --pushdown-filters: must be a valid hex string: {e}") from e

        _logger.info("starting_worker function=%s worker_path=%s", function_name, worker_path)

        # Validate function_type requirements
        if function_type == "scalar" and input_file is None:
            raise click.ClickException("--type scalar requires --input to be specified")
        if function_type == "table-in-out" and input_file is None:
            raise click.ClickException("--type table-in-out requires --input to be specified")
        if function_type == "table" and input_file is not None:
            raise click.ClickException("--type table does not accept --input (table functions have no input)")

        output_writer: OutputWriter | None = None
        try:
            with Client(
                worker_path,
                passthrough_stderr=worker_stderr,
                worker_limit=max_workers,
                attach_opaque_data=attach_opaque_data_bytes,
            ) as client:
                # Determine effective function type
                if function_type == "auto":
                    effective_type = "table" if input_file is None else "table-in-out"
                else:
                    effective_type = function_type

                # Build arguments object
                func_args = Arguments(positional=positional_args, named=named_args)

                if effective_type == "table":
                    # Table function (no input)
                    _logger.info("invoking_table_function function=%s", function_name)
                    output_iterator = client.table_function(
                        function_name=function_name,
                        schema_name=schema_name,
                        arguments=func_args,
                        projection_ids=list(projection_ids) if projection_ids else None,
                        pushdown_filters=pushdown_filters_bytes,
                        transaction_opaque_data=transaction_opaque_data_bytes,
                        settings=settings,
                    )
                elif effective_type == "scalar":
                    # Scalar function (with input, single-column output)
                    assert input_file is not None  # Validated earlier
                    _logger.info("invoking_scalar_function function=%s", function_name)
                    _logger.info("reading_input file=%s", input_file)
                    pf = pq.ParquetFile(input_file)

                    output_iterator = client.scalar_function(
                        function_name=function_name,
                        schema_name=schema_name,
                        arguments=func_args,
                        input=pf.iter_batches(),
                        transaction_opaque_data=transaction_opaque_data_bytes,
                        settings=settings,
                    )
                else:
                    # Table-in-out function (with input)
                    assert input_file is not None  # Validated earlier
                    _logger.info("invoking_table_in_out_function function=%s", function_name)
                    _logger.info("reading_input file=%s", input_file)
                    pf = pq.ParquetFile(input_file)

                    # If table_input_position is specified, log it for debugging
                    # The table input position tells the user where the table data
                    # appears in the function signature (e.g., position 1 means the
                    # table is the second argument). This is purely informational
                    # for the CLI user - the protocol handles table data separately.
                    if table_input_position is not None:
                        _logger.debug(
                            "table_input_position_specified position=%s num_args=%s",
                            table_input_position,
                            len(positional_args),
                        )

                    output_iterator = client.table_in_out_function(
                        function_name=function_name,
                        schema_name=schema_name,
                        arguments=func_args,
                        input=pf.iter_batches(),
                        projection_ids=list(projection_ids) if projection_ids else None,
                        pushdown_filters=pushdown_filters_bytes,
                        transaction_opaque_data=transaction_opaque_data_bytes,
                        settings=settings,
                    )

                for output_batch in output_iterator:
                    if output_writer is None:
                        output_writer = OutputWriter(output_file, output_format, output_batch.schema)
                    output_writer.write_batch(output_batch)

            _logger.info("processing_complete function=%s", function_name)
        except ClientError as e:
            raise click.ClickException(str(e)) from e
        finally:
            if output_writer is not None:
                output_writer.close()

    # Add catalog subcommand group (schema/table/view/transaction nested under it)
    cli.add_command(catalog)

    return cli


# Module-level command for testing
cli = _create_cli()


def main() -> None:
    """CLI entry point for vgi-client."""
    cli()


if __name__ == "__main__":
    main()
