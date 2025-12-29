"""VGI client for communicating with VGI workers.

This module provides the Client class for programmatic interaction with VGI workers.
The client manages subprocess lifecycle and Arrow IPC communication.

Example:
    from vgi.client import Client
    from vgi.function import Arguments

    with Client("./my_worker.py") as client:
        for batch in client.table_in_out_function(
            function_name="echo",
            arguments=Arguments(positional=[], named={}),
            input=input_batches,
        ):
            process(batch)

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

from vgi.function import Arguments, Request
from vgi.ipc_utils import IPCError, read_ipc_batch
from vgi.table_function import GlobalStateInitInput

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

worker_log = structlog.get_logger().bind(component="worker")


class ClientError(Exception):
    """Error raised by Client operations."""


class Client:
    """Client for communicating with VGI workers.

    Manages the subprocess lifecycle and Arrow IPC communication with a VGI
    worker process. Use as a context manager to ensure proper cleanup.

    Example:
        with Client("./my_worker.py") as client:
            for batch in client.table_in_out_function(
                function_name="echo",
                arguments=Arguments(positional=[], named={}),
                input=input_batches,
            ):
                process(batch)

    """

    def _handle_log_message(
        self, output_batch: pa.RecordBatch, output_metadata: dict[bytes, bytes] | None
    ) -> bool:
        """Handle a log message from the worker if present.

        Args:
            output_batch: The output batch from the worker.
            output_metadata: Custom metadata from the batch.

        Returns:
            True if this was a log message (caller should continue to next batch),
            False if this was a regular data batch.

        Raises:
            ClientError: If the log message indicates a worker exception.

        """
        if output_metadata is None:
            return False

        if not (
            output_batch.num_rows == 0
            and output_metadata.get(b"log_level") is not None
            and output_metadata.get(b"log_message") is not None
        ):
            return False

        extra: dict[str, Any] = {}
        if output_metadata.get(b"log_extra") is not None:
            try:
                extra = json.loads(output_metadata[b"log_extra"].decode())
            except json.JSONDecodeError as e:
                log.error(
                    "failed_to_decode_log_extra",
                    error=str(e),
                    raw=output_metadata[b"log_extra"],
                )

        level_name = output_metadata[b"log_level"].decode().lower()
        worker_log._proxy_to_logger(
            level_name,
            output_metadata[b"log_message"].decode(),
            **extra,
        )

        if level_name == "exception":
            message = output_metadata[b"log_message"].decode()
            traceback = extra.get("traceback", "")
            full_message = f"Worker Exception: {message}\n{traceback}"
            raise ClientError(full_message)

        return True

    def _table_in_out_function_initialize_stream(
        self,
        *,
        function_name: str,
        arguments: Arguments,
        input_schema: pa.Schema,
        bind_result_callback: Callable[[pa.RecordBatch], None] | None,
        projection_ids: list[int] | None,
    ) -> ipc.RecordBatchStreamWriter:
        """Initialize the VGI protocol stream for a table-in-out function.

        Sends the Request, reads the bind result, sends GlobalStateInitInput,
        reads the init result, and returns a stream writer for data batches.

        Args:
            function_name: Name of the function to invoke.
            arguments: Arguments container with positional and named arguments.
            input_schema: Schema of the input batches.
            bind_result_callback: Optional callback for the bind result.
            projection_ids: Optional list of column indices to project.

        Returns:
            An IPC RecordBatchStreamWriter for sending data batches.

        Raises:
            ClientError: If protocol communication fails.
            OSError: If writing to the worker fails.

        """
        if self._stdin_sink is None or self._stdout_buffered is None:
            raise ClientError("Worker process not started. Call start() first.")

        # Send initialization batch
        log.debug("sending_init_batch", function=function_name, arguments=arguments)

        call_parameters_batch_bytes = Request(
            function_name=function_name,
            arguments=arguments,
            in_out_function_input_schema=input_schema,
            correlation_id=self.correlation_id,
            invocation_id=None,
        ).serialize()

        if self._stdin_sink.write(call_parameters_batch_bytes) != len(
            call_parameters_batch_bytes
        ):
            raise OSError("Failed to write call parameters record batch")

        # Read the bind result
        log.debug("reading_bind_result")
        try:
            bind_result_batch = read_ipc_batch(self._stdout_buffered, "bind_result")
        except IPCError as e:
            raise ClientError(str(e)) from e

        if bind_result_callback is not None:
            bind_result_callback(bind_result_batch)

        log.debug("bind_result_received", batch=bind_result_batch)

        # Send global state init input
        global_state_info_serialized_bytes = GlobalStateInitInput(
            projection_ids=projection_ids
        ).serialize()

        if self._stdin_sink.write(global_state_info_serialized_bytes) != len(
            global_state_info_serialized_bytes
        ):
            raise OSError("Failed to write global state init input record batch")

        # Read the init result
        log.debug("reading_init_result")
        try:
            _init_result_batch = read_ipc_batch(self._stdout_buffered, "init_result")
        except IPCError as e:
            raise ClientError(str(e)) from e
        log.debug("init_result_received")

        # Create and return the data stream writer
        data_writer = ipc.new_stream(self._stdin_sink, input_schema)
        log.debug("starting_data_batches")

        return data_writer

    def __init__(
        self,
        server_path: str,
        correlation_id: str = "",
        passthrough_stderr: bool = False,
    ):
        """Initialize the VGI client.

        Args:
            server_path: Path to the VGI worker script to execute.
            correlation_id: Optional identifier for request correlation in logs.
            passthrough_stderr: If True, worker stderr is passed through to
                the parent process's stderr. If False (default), stderr is
                captured and available via get_worker_stderr().

        """
        self.server_path = server_path
        self.correlation_id = correlation_id
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
        """Stop the worker subprocess.

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
        """Start the worker and return the client for context manager usage."""
        self.start()
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:
        """Stop the worker when exiting context."""
        self.stop()

    def table_in_out_function(
        self,
        *,
        function_name: str,
        input: Iterator[pa.RecordBatch],
        arguments: Arguments | None = None,
        bind_result_callback: Callable[[pa.RecordBatch], None] | None = None,
        projection_ids: list[int] | None = None,
    ) -> Generator[pa.RecordBatch, None, None]:
        """Call a table-in-out function on the worker with the given input data.

        This method implements the VGI streaming protocol: it sends input batches
        to the worker, handles HAVE_MORE_OUTPUT responses, and sends the FINALIZE
        signal when input is exhausted.

        Args:
            function_name: Name of the function to invoke (must be in worker registry).
            arguments: Arguments container with positional and named arguments.
            input: Iterator yielding input RecordBatches. The first batch's schema
                is used for the Request.in_out_function_input_schema.
            bind_result_callback: Optional callback invoked with the bind result
                RecordBatch after the worker responds. Useful for inspecting
                output schema or cardinality hints before processing begins.
            projection_ids: Optional list of column indices to project.

        Yields:
            Output RecordBatches from the function. Multiple input batches may be
            combined into a single output batch when the function returns
            HAVE_MORE_OUTPUT.

        Raises:
            ClientError: If communication with the worker fails or the worker
                returns an unexpected status.

        """
        if arguments is None:
            arguments = Arguments()

        if (
            self._proc is None
            or self._stdin_sink is None
            or self._stdout_buffered is None
        ):
            raise ClientError(
                "Client not started. Call start() or use context manager."
            )

        output_reader = None
        input_schema: pa.Schema | None = None
        data_writer: ipc.RecordBatchStreamWriter | None = None

        for batch_index, input_batch in enumerate(input):
            if not isinstance(input_batch, pa.RecordBatch):
                raise ClientError("Input iterator must yield RecordBatches")

            if batch_index == 0:
                input_schema = input_batch.schema
                data_writer = self._table_in_out_function_initialize_stream(
                    function_name=function_name,
                    arguments=arguments,
                    input_schema=input_schema,
                    bind_result_callback=bind_result_callback,
                    projection_ids=projection_ids,
                )

            if data_writer is None:
                raise ClientError("Protocol error: data_writer not initialized")

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
                    # function indicates HAVE_MORE_OUTPUT so do the
                    # same thing here.
                    data_writer.write_batch(input_batch)

                    if output_reader is None:
                        output_reader = ipc.open_stream(self._stdout_buffered)

                    output_batch, output_metadata = (
                        output_reader.read_next_batch_with_custom_metadata()
                    )
                    status = output_metadata.get("status") if output_metadata else None

                    log.debug(
                        "received_output_batch",
                        num_rows=output_batch.num_rows,
                        status=status,
                    )

                    if self._handle_log_message(output_batch, output_metadata):
                        continue

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
            raise ClientError("Protocol error: data_writer not initialized")

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

            if self._handle_log_message(output_batch, output_metadata):
                continue

            yield output_batch

            if status == b"HAVE_MORE_OUTPUT":
                continue
            elif status == b"FINISHED":
                break
            else:
                raise ClientError(f"Unexpected finalize status from server: {status}")

        data_writer.close()
        log.debug("processing_complete", function=function_name)
