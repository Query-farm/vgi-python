#!/usr/bin/env python3
"""
VGI client that sends parquet data through a function.

Usage:
    vgi-client --input data.parquet --function echo
    vgi-client --input data.parquet --function sum_all_columns
    vgi-client --input data.parquet --function repeat_inputs --args '[3]'
"""

import io
import json
import subprocess
import sys
import traceback
from typing import Any

import click
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from pyarrow import ipc

# Configure structlog to match server output format
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(0),  # Show all levels
    logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
)

log = structlog.get_logger().bind(component="client")


def create_init_batch(
    function_name: str, arguments: list[Any], input_schema: pa.Schema
) -> pa.RecordBatch:
    """Create the initialization batch for the server."""
    # Create a struct field representing the input schema
    in_type_struct = pa.struct(
        [pa.field(f.name, f.type, f.nullable) for f in input_schema]
    )

    init_schema = pa.schema(
        [
            pa.field("function_name", pa.string()),
            pa.field("arguments", pa.string()),
            pa.field("in_type", in_type_struct),
        ]
    )

    # Create empty struct value for in_type (actual schema is in the field definition)
    in_type_value = {f.name: None for f in input_schema}

    init_batch = pa.RecordBatch.from_pydict(
        {
            "function_name": [function_name],
            "arguments": [json.dumps(arguments)],
            "in_type": [in_type_value],
        },
        schema=init_schema,
    )

    return init_batch


@click.command()
@click.option(
    "--input",
    "input_file",
    required=True,
    type=click.Path(exists=True),
    help="Path to input parquet file",
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
    default="vgi-example-server",
    type=str,
    help="Path to the vgi server",
)
def main(input_file: str, function_name: str, arguments: str, server_path: str) -> None:
    """Send parquet data through a VGI function and display results."""
    # Parse arguments
    try:
        args_list = json.loads(arguments)
        if not isinstance(args_list, list):
            raise click.ClickException("--args must be a JSON array")
    except json.JSONDecodeError as e:
        log.error("invalid_json_arguments", error=str(e))
        raise click.ClickException(f"Invalid JSON in --args: {e}") from e

    # Read input parquet file
    log.info("reading_input", file=input_file)
    table = pq.read_table(input_file)
    input_schema = table.schema

    log.info(
        "input_loaded",
        schema=str(input_schema),
        num_rows=table.num_rows,
        num_columns=len(input_schema),
    )

    # Start the server subprocess (stderr goes to screen)
    log.info("starting_server", function=function_name, server_path=server_path)
    proc = subprocess.Popen(
        [sys.executable, server_path],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=None,
        text=False,
        bufsize=0,
    )
    log.debug("server_started", pid=proc.pid)

    assert proc.stdout is not None, "stdout pipe not created"
    stdout_buffered = io.BufferedReader(proc.stdout)  # type: ignore[type-var]

    try:
        # Send initialization batch
        proc_stdin_sink = pa.PythonFile(proc.stdin)
        log.debug("sending_init_batch", function=function_name, arguments=args_list)
        init_batch = create_init_batch(function_name, args_list, input_schema)
        init_writer = ipc.new_stream(proc_stdin_sink, init_batch.schema)
        init_writer.write_batch(init_batch)
        # If the init_writer is closed here, it closes the pipe to the server
        # which isn't desired, so we leave it open.

        log.debug("reading_output_schema")
        msg = ipc.read_message(stdout_buffered)
        assert msg.type == "schema", f"Expected schema message, got {msg.type}"
        output_schema = ipc.read_schema(msg)
        log.info("output_schema_received", schema=str(output_schema))

        # Send data batches
        data_writer = ipc.new_stream(proc_stdin_sink, input_schema)
        batches = table.to_batches()
        log.info("sending_data_batches", num_batches=len(batches))
        output_reader = None
        for i, input_batch in enumerate(batches):
            while True:
                log.debug("sending_batch", batch_index=i, num_rows=input_batch.num_rows)
                data_writer.write_batch(input_batch)
                log.debug("batch_sent", batch_index=i)

                if output_reader is None:
                    output_reader = ipc.open_stream(stdout_buffered)

                log.debug("attempting_read_output", batch_index=i)
                output_batch, output_metadata = (
                    output_reader.read_next_batch_with_custom_metadata()
                )
                status = output_metadata.get("status") if output_metadata else None

                log.debug(
                    "received_output_batch",
                    num_rows=output_batch.num_rows,
                    status=status,
                    batch=output_batch,
                )

                if status == b"HAVE_MORE_OUTPUT":
                    # If there are more batches to read for this input,
                    # loop here sending the same input batch again, until the
                    # server indicates it is done with this input.
                    continue
                elif status == b"NEED_MORE_INPUT":
                    break
                else:
                    log.error("unexpected_status", status=status)
                    raise click.ClickException(
                        f"Unexpected status from server: {status}"
                    )

        empty_input_batch = pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in input_schema],
            schema=input_schema,
        )

        assert output_reader is not None, "output_reader not initialized"
        while True:
            # Send finalize signal
            log.debug("sending_finalize")

            data_writer.write_batch(
                empty_input_batch, custom_metadata={"type": "FINALIZE"}
            )

            output_batch, output_metadata = (
                output_reader.read_next_batch_with_custom_metadata()
            )
            status = output_metadata.get("status") if output_metadata else None
            log.debug(
                "received_finalize_batch",
                num_rows=output_batch.num_rows,
                status=status,
                batch=output_batch,
            )

            if status == b"HAVE_MORE_OUTPUT":
                continue
            elif status == b"FINISHED":
                break
            else:
                log.error("unexpected_finalize_status", status=status)
                raise click.ClickException(
                    f"Unexpected finalize status from server: {status}"
                )

        data_writer.close()
        proc_stdin_sink.close()

        log.info("processing_complete", function=function_name)

    except Exception as e:
        log.error("processing_error", error=str(e), traceback=traceback.format_exc())
        raise click.ClickException(f"Error: {e}") from e

    finally:
        proc.wait()
        if proc.returncode != 0:
            log.error("server_exited_with_error", returncode=proc.returncode)
        else:
            log.debug("server_exited", returncode=proc.returncode)


if __name__ == "__main__":
    main()
