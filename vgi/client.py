#!/usr/bin/env python3
"""
VGI client for sending data through VGI functions.

This module provides:
- Client: A class for programmatic interaction with VGI workers
- CLI: Command-line interface for processing parquet files

Usage (CLI):
    vgi-client --input data.parquet --function echo
    vgi-client --input data.parquet --function sum_all_columns
    vgi-client --input data.parquet --function repeat_inputs --args '[3]'

Usage (API):
    with Client("./my_worker.py") as client:
        for batch in client.table_in_out_function("echo", [], input_batches):
            print(batch)
"""

import io
import json
import subprocess
import sys
import threading
from collections.abc import Callable, Generator, Iterator
from typing import IO, Any

import pyarrow as pa
import structlog
from pyarrow import ipc

from vgi.function import Arguments, CallData

# Configure structlog to write to stderr
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(0),
    logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
)

log = structlog.get_logger().bind(component="client")


class ClientError(Exception):
    """Error raised by Client operations."""


class Client:
    """
    Client for communicating with VGI workers.

    This class manages the subprocess lifecycle and Arrow IPC communication
    with a VGI worker process.

    Example:
        with Client("./my_worker.py") as client:
            for batch in client.table_in_out_function("echo", [], input_batches):
                process(batch)
    """

    def __init__(self, server_path: str, passthrough_stderr: bool = False):
        """
        Initialize the VGI client.

        Args:
            server_path: Path to the VGI worker script to execute.
            passthrough_stderr: If True, worker stderr is passed through to
                the parent process's stderr. If False (default), stderr is
                captured and available via get_worker_stderr().
        """
        self.server_path = server_path
        self.passthrough_stderr = passthrough_stderr
        self._proc: subprocess.Popen[bytes] | None = None
        self._stdout_buffered: io.BufferedReader | None = None
        self._stdin_sink: pa.PythonFile | None = None
        self._stderr_buffer: list[bytes] = []
        self._stderr_lock = threading.Lock()
        self._stderr_thread: threading.Thread | None = None

    def _drain_stderr(self, stderr: IO[bytes]) -> None:
        """Background thread that continuously reads stderr."""
        while True:
            line = stderr.readline()
            if not line:
                break
            with self._stderr_lock:
                self._stderr_buffer.append(line)

    def get_worker_stderr(self) -> str:
        """Return all captured stderr from the worker process."""
        with self._stderr_lock:
            return b"".join(self._stderr_buffer).decode("utf-8", errors="replace")

    def start(self) -> None:
        """Start the worker subprocess."""
        if self._proc is not None:
            raise ClientError("Client already started")

        self._stderr_buffer = []

        log.debug("starting_server", server_path=self.server_path)
        self._proc = subprocess.Popen(
            self.server_path,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None if self.passthrough_stderr else subprocess.PIPE,
            text=False,
            bufsize=0,
            shell=True,
        )
        log.debug("server_started", pid=self._proc.pid)

        if self._proc.stdout is None:
            raise ClientError("Failed to create stdout pipe for worker subprocess")

        if not self.passthrough_stderr:
            if self._proc.stderr is None:
                raise ClientError("Failed to create stderr pipe for worker subprocess")

            self._stderr_thread = threading.Thread(
                target=self._drain_stderr, args=(self._proc.stderr,), daemon=True
            )
            self._stderr_thread.start()

        self._stdout_buffered = io.BufferedReader(self._proc.stdout)  # type: ignore[arg-type]
        self._stdin_sink = pa.PythonFile(self._proc.stdin)

    def stop(self) -> int:
        """
        Stop the worker subprocess.

        Returns:
            The subprocess return code.
        """
        if self._proc is None:
            raise ClientError("Client not started")

        if self._proc.stdin:
            self._proc.stdin.close()
        self._proc.wait()
        returncode = self._proc.returncode
        if returncode != 0:
            log.error("server_exited_with_error", returncode=returncode)
        else:
            log.debug("server_exited", returncode=returncode)

        # Wait for stderr thread to finish draining before returning.
        # The thread will exit naturally when stderr reaches EOF after
        # the process terminates.
        if self._stderr_thread is not None:
            self._stderr_thread.join(timeout=5.0)
            if self._stderr_thread.is_alive():
                log.warning("stderr_thread_did_not_terminate")

        self._proc = None
        self._stdout_buffered = None
        self._stdin_sink = None
        self._stderr_thread = None
        return returncode

    def __enter__(self) -> "Client":
        self.start()
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:
        self.stop()

    def table_in_out_function(
        self,
        *,
        function_name: str,
        arguments: Arguments,
        call_identifier: bytes | None,
        input: Iterator[pa.RecordBatch],
        bind_result_callback: Callable[[pa.RecordBatch], None] | None = None,
    ) -> Generator[pa.RecordBatch, None, None]:
        """
        Call a function on the worker with the given input data.

        Args:
            function_name: Name of the function to invoke.
            arguments: List of arguments to pass to the function.
            input: An iterator yielding input RecordBatches.

        Yields:
            Output RecordBatches from the function.

        Raises:
            ClientError: If communication with the worker fails.
        """
        if (
            self._proc is None
            or self._stdin_sink is None
            or self._stdout_buffered is None
        ):
            raise ClientError(
                "Client not started. Call start() or use context manager."
            )

        output_reader = None
        input_schema = None
        data_writer = None

        for batch_index, input_batch in enumerate(input):
            if not isinstance(input_batch, pa.RecordBatch):
                raise ClientError("Input iterator must yield RecordBatches")

            if batch_index == 0:
                input_schema = input_batch.schema
                # Send initialization batch
                log.debug(
                    "sending_init_batch", function=function_name, arguments=arguments
                )

                call_parameters_batch = CallData(
                    function_name=function_name,
                    arguments=arguments,
                    in_schema=input_schema,
                    call_identifier=call_identifier if call_identifier else b"",
                ).serialize()
                init_writer = ipc.new_stream(
                    self._stdin_sink, call_parameters_batch.schema
                )
                init_writer.write_batch(call_parameters_batch)
                # If we close init_writer here, the underlying pipe gets closed
                # and we can't send data batches, so we just leave it open.
                # It will be closed be closed when this function exists
                # which is fine.

                # Read the bind data.
                log.debug("reading_bind_schema")
                msg = ipc.read_message(self._stdout_buffered)
                if msg.type != "schema":
                    raise ClientError(f"Expected schema message, got {msg.type}")

                bind_result_schema = ipc.read_schema(msg)
                log.debug("bind_schema_received", schema=str(bind_result_schema))

                msg = ipc.read_message(self._stdout_buffered)
                if msg.type != "record batch":
                    raise ClientError(
                        f"Expected bind result record batch, got {msg.type}"
                    )
                bind_result_batch = ipc.read_record_batch(msg, bind_result_schema)

                if bind_result_callback is not None:
                    bind_result_callback(bind_result_batch)

                log.debug("bind_result_received")

                log.debug("bind_result", batch=bind_result_batch)

                log.debug("output_schema_received")

                # Send data batches
                data_writer = ipc.new_stream(self._stdin_sink, input_schema)
                log.debug("starting_data_batches")

            if data_writer is None:
                raise ClientError("Data writer was not initialized")
            while True:
                # Since a single batch may produce multiple output batches,
                # we need to collect them, because a generator can only yield
                # a single batch.
                #
                # Other implementations of vgi may yield multiple times per
                # input batch, but this is just a restriction of the python
                # generator interface.
                output_batches = []
                while True:
                    log.debug(
                        "sending_batch",
                        batch_index=batch_index,
                        num_rows=input_batch.num_rows,
                    )
                    # In DuckDB the same input batch is supplied if the
                    # funciton indicates it HAVE_MORE_OUTPUT so do the
                    # same thing here.
                    data_writer.write_batch(input_batch)
                    log.debug("batch_sent", batch_index=batch_index)

                    if output_reader is None:
                        output_reader = ipc.open_stream(self._stdout_buffered)

                    log.debug("attempting_read_output", batch_index=batch_index)
                    output_batch, output_metadata = (
                        output_reader.read_next_batch_with_custom_metadata()
                    )
                    status = output_metadata.get("status") if output_metadata else None

                    log.debug(
                        "received_output_batch",
                        num_rows=output_batch.num_rows,
                        status=status,
                    )

                    output_batches.append(output_batch)

                    if status == b"HAVE_MORE_OUTPUT":
                        continue
                    elif status == b"NEED_MORE_INPUT":
                        break
                    else:
                        raise ClientError(f"Unexpected status from server: {status}")

                combined_batches = list(
                    pa.Table.from_batches(output_batches).combine_chunks().to_batches()
                )
                # When PyArrow combines batches, if none of them have any rows,
                # you'll get an empty list of resulting batches. In that case,
                # just yield one of the original empty batches.
                if len(combined_batches) == 0:
                    large_batch = output_batches[0]
                else:
                    large_batch = combined_batches[0]

                input_batch = yield large_batch

                # If the input batch is None, we are done sending data
                # and should move to the finalize loop.
                if input_batch is None:
                    break

        if output_reader is None:
            raise ClientError("No data batches were processed before finalize")

        if input_schema is None:
            raise ClientError("No input batches were sent")
        # Send finalize signal
        empty_input_batch = pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in input_schema],
            schema=input_schema,
        )

        if data_writer is None:
            raise ClientError("Data writer was not initialized")

        while True:
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
            )

            yield output_batch

            if status == b"HAVE_MORE_OUTPUT":
                continue
            elif status == b"FINISHED":
                break
            else:
                raise ClientError(f"Unexpected finalize status from server: {status}")

        data_writer.close()
        log.debug("processing_complete", function=function_name)


class OutputWriter:
    """Handles writing output batches in various formats."""

    def __init__(
        self, output_file: str | None, format: str, schema: pa.Schema | None = None
    ):
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
        required=True,
        # This validates the that file exists.
        type=click.Path(exists=True),
        help="Path to input parquet file",
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
    def cli(
        input_file: str,
        output_file: str | None,
        output_format: str,
        function_name: str,
        arguments: str,
        server_path: str,
        worker_stderr: bool,
    ) -> None:
        """Send parquet data through a VGI function and display results."""
        try:
            args_list = json.loads(arguments)
            if not isinstance(args_list, list):
                raise click.ClickException("--args must be a JSON array")
        except json.JSONDecodeError as e:
            log.error("invalid_json_arguments", error=str(e))
            raise click.ClickException(f"Invalid JSON in --args: {e}") from e

        log.info("reading_input", file=input_file)
        pf = pq.ParquetFile(input_file)

        log.info("starting_server", function=function_name, server_path=server_path)

        output_writer: OutputWriter | None = None
        try:
            with Client(server_path, passthrough_stderr=worker_stderr) as client:
                for output_batch in client.table_in_out_function(
                    function_name=function_name,
                    arguments=Arguments(positional=args_list, named={}),
                    call_identifier=None,
                    input=pf.iter_batches(),
                ):
                    if output_writer is None:
                        output_writer = OutputWriter(
                            output_file, output_format, output_batch.schema
                        )

                    output_writer.write_batch(output_batch)
            log.info("processing_complete", function=function_name)
        except ClientError as e:
            log.error("processing_error", error=str(e))
            raise click.ClickException(str(e)) from e
        finally:
            if output_writer is not None:
                output_writer.close()

    cli()


if __name__ == "__main__":
    main()
