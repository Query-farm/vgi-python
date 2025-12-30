"""VGI client for communicating with VGI workers.

This module provides the Client class for programmatic interaction with VGI workers.
The client manages subprocess lifecycle and Arrow IPC communication.

QUICK START
-----------
Use Client as a context manager to ensure proper cleanup:

    from vgi.client import Client
    from vgi.function import Arguments
    import pyarrow as pa

    # Create input batches
    batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})

    with Client("vgi-example-worker") as client:
        for output_batch in client.table_in_out_function(
            function_name="echo",
            arguments=Arguments(),
            input=iter([batch]),
        ):
            print(output_batch.to_pydict())

PARALLEL PROCESSING
-------------------
When a function returns max_processes > 1, the client automatically spawns
additional workers and distributes batches across them. Output order may
not match input order in parallel mode.

KEY CLASSES
-----------
    Client          - Main class for invoking functions on workers
    ClientError     - Exception raised on communication errors
    WorkerConnection - Internal: holds state for a worker subprocess

Methods
-------
client.start() : Start the worker subprocess
client.stop() : Stop the worker subprocess
client.table_in_out_function() : Invoke a function and stream results
client.get_worker_stderr() : Get captured stderr from worker

See Also
--------
vgi.worker.Worker : Base class for workers that Client spawns
vgi.function.Invocation : Invocation structure sent to workers
vgi.function.Arguments : Container for function arguments

"""

import io
import json
import os
import subprocess
import sys
import threading
from collections.abc import Callable, Generator, Iterator
from dataclasses import dataclass
from queue import Queue
from typing import IO, Any

import pyarrow as pa
import structlog
from pyarrow import ipc

from vgi.function import (
    PROTOCOL_VERSION,
    Arguments,
    GlobalInitResult,
    Invocation,
    ProtocolVersionError,
)
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


# Sentinel to signal end of input to worker threads
_END_OF_INPUT = object()


@dataclass
class WorkerConnection:
    """Holds state for a single worker subprocess connection."""

    proc: subprocess.Popen[bytes]
    stdout_buffered: Any  # io.BufferedReader - typed as Any due to subprocess IO quirks
    stdin_sink: pa.PythonFile
    worker_index: int
    data_writer: ipc.RecordBatchStreamWriter | None = None
    output_reader: ipc.RecordBatchStreamReader | None = None


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

        Sends the Invocation, reads the bind result, sends GlobalStateInitInput,
        reads the init result, spawns additional workers if needed, and returns
        a stream writer for data batches.

        Args:
            function_name: Name of the function to invoke.
            arguments: Arguments container with positional and named arguments.
            input_schema: Schema of the input batches.
            bind_result_callback: Optional callback for the bind result.
            projection_ids: Optional list of column indices to project.

        Returns:
            An IPC RecordBatchStreamWriter for sending data batches to the
            primary worker.

        Raises:
            ClientError: If protocol communication fails.
            OSError: If writing to the worker fails.

        """
        if self._stdin_sink is None or self._stdout_buffered is None:
            raise ClientError("Worker process not started. Call start() first.")

        # Send initialization batch
        log.debug("sending_init_batch", function=function_name, arguments=arguments)

        initial_request = Invocation(
            function_name=function_name,
            arguments=arguments,
            in_out_function_input_schema=input_schema,
            correlation_id=self.correlation_id,
            invocation_id=None,
        )
        call_parameters_batch_bytes = initial_request.serialize()

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

        # Extract max_processes and invocation_id from bind result
        max_processes_array = bind_result_batch.column(
            bind_result_batch.schema.get_field_index("max_processes")
        )
        max_processes = max_processes_array.cast(pa.int32()).to_pylist()[0]
        invocation_id_array = bind_result_batch.column(
            bind_result_batch.schema.get_field_index("invocation_id")
        )
        invocation_id = invocation_id_array.to_pylist()[0]

        # Validate protocol version from worker
        if "protocol_version_major" in bind_result_batch.schema.names:
            worker_major = bind_result_batch.column(
                bind_result_batch.schema.get_field_index("protocol_version_major")
            ).to_pylist()[0]
            worker_minor = bind_result_batch.column(
                bind_result_batch.schema.get_field_index("protocol_version_minor")
            ).to_pylist()[0]
            worker_version = (worker_major, worker_minor)

            # Verify the worker's version is compatible
            if worker_major != PROTOCOL_VERSION[0]:
                raise ProtocolVersionError(
                    f"Protocol version mismatch: client uses major version "
                    f"{PROTOCOL_VERSION[0]}, worker responded with major version "
                    f"{worker_major}. Major versions must match."
                )

            log.debug(
                "protocol_version_validated",
                client_version=PROTOCOL_VERSION,
                worker_version=worker_version,
            )
        # Limit max_processes to the number of CPUs available
        cpu_count = os.cpu_count() or 1
        if max_processes > cpu_count:
            log.debug(
                "limiting_max_processes_to_cpu_count",
                requested=max_processes,
                cpu_count=cpu_count,
            )
            max_processes = cpu_count

        # Limit max_processes to the user-specified max_workers if set
        if self._max_workers is not None and max_processes > self._max_workers:
            log.debug(
                "limiting_max_processes_to_max_workers",
                requested=max_processes,
                max_workers=self._max_workers,
            )
            max_processes = self._max_workers

        log.debug(
            "max_processes_determined",
            max_processes=max_processes,
            invocation_id=invocation_id.hex() if invocation_id else None,
        )

        # Send global state init input to primary worker
        global_state_info_serialized_bytes = GlobalStateInitInput(
            projection_ids=projection_ids
        ).serialize()

        if self._stdin_sink.write(global_state_info_serialized_bytes) != len(
            global_state_info_serialized_bytes
        ):
            raise OSError("Failed to write global state init input record batch")

        # Read the init result from primary worker
        log.debug("reading_init_result")
        try:
            init_result_batch = read_ipc_batch(self._stdout_buffered, "init_result")
        except IPCError as e:
            raise ClientError(str(e)) from e

        # Parse the GlobalInitResult
        global_init_result = GlobalInitResult.deserialize(init_result_batch)
        log.debug(
            "init_result_received",
            has_identifier=global_init_result.global_init_identifier is not None,
        )

        # Spawn additional workers if max_processes > 1
        if max_processes > 1:
            # Create request with global_init_identifier for additional workers
            request_with_init = Invocation(
                function_name=function_name,
                arguments=arguments,
                in_out_function_input_schema=input_schema,
                correlation_id=self.correlation_id,
                invocation_id=invocation_id,
                global_init_identifier=global_init_result,
            )

            # Spawn all worker subprocesses first (fast)
            for worker_index in range(1, max_processes):
                worker = self._spawn_worker(worker_index)
                self._additional_workers.append(worker)

            # Initialize all workers in parallel (overlaps Python startup time)
            init_errors: list[Exception] = []

            def init_worker(worker: WorkerConnection) -> None:
                try:
                    self._initialize_additional_worker(
                        worker, request_with_init, input_schema
                    )
                except Exception as e:
                    init_errors.append(e)

            init_threads: list[threading.Thread] = []
            for worker in self._additional_workers:
                t = threading.Thread(target=init_worker, args=(worker,))
                t.start()
                init_threads.append(t)

            for t in init_threads:
                t.join()

            if init_errors:
                raise ClientError(
                    f"Failed to initialize workers: {init_errors[0]}"
                ) from init_errors[0]

            log.debug(
                "additional_workers_spawned",
                count=len(self._additional_workers),
            )

        # Create and return the data stream writer for primary worker
        data_writer = ipc.new_stream(self._stdin_sink, input_schema)
        log.debug("starting_data_batches")

        return data_writer

    def __init__(
        self,
        server_path: str,
        correlation_id: str = "",
        passthrough_stderr: bool = False,
        max_workers: int | None = None,
    ):
        """Initialize the VGI client.

        Args:
            server_path: Path to the VGI worker script to execute.
            correlation_id: Optional identifier for request correlation in logs.
            passthrough_stderr: If True, worker stderr is passed through to
                the parent process's stderr. If False (default), stderr is
                captured and available via get_worker_stderr().
            max_workers: Optional maximum number of worker processes. If set,
                clamps the function's max_processes to this value.

        """
        self.server_path = server_path
        self.correlation_id = correlation_id
        self.passthrough_stderr = passthrough_stderr
        self._max_workers = max_workers
        self._proc: subprocess.Popen[bytes] | None = None
        self._stdout_buffered: io.BufferedReader | None = None
        self._stdin_sink: pa.PythonFile | None = None
        self._stderr_buffer: list[bytes] = []
        self._stderr_lock = threading.Lock()
        self._stderr_thread: threading.Thread | None = None
        # For multi-worker support
        self._additional_workers: list[WorkerConnection] = []
        self._stderr_threads: list[threading.Thread] = []

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

    def _spawn_worker(self, worker_index: int) -> WorkerConnection:
        """Spawn a new worker subprocess and return its connection.

        Args:
            worker_index: Index identifying this worker (for logging).

        Returns:
            WorkerConnection with the subprocess and I/O handles.

        """
        log.debug("spawning_worker", worker_index=worker_index)
        proc = subprocess.Popen(
            self.server_path,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None if self.passthrough_stderr else subprocess.PIPE,
            text=False,
            bufsize=0,
            shell=True,
        )
        log.debug("worker_spawned", worker_index=worker_index, pid=proc.pid)

        if proc.stdout is None:
            raise ClientError("Failed to create stdout pipe for worker subprocess")

        if not self.passthrough_stderr:
            if proc.stderr is None:
                raise ClientError("Failed to create stderr pipe for worker subprocess")
            stderr_thread = threading.Thread(
                target=self._drain_stderr, args=(proc.stderr,), daemon=True
            )
            stderr_thread.start()
            self._stderr_threads.append(stderr_thread)

        stdout_buffered = io.BufferedReader(proc.stdout)  # type: ignore[type-var]
        stdin_sink = pa.PythonFile(proc.stdin)

        return WorkerConnection(
            proc=proc,
            stdout_buffered=stdout_buffered,
            stdin_sink=stdin_sink,
            worker_index=worker_index,
        )

    def _initialize_additional_worker(
        self,
        worker: WorkerConnection,
        request_with_init: Invocation,
        input_schema: pa.Schema,
    ) -> None:
        """Initialize an additional worker with the global init result.

        Args:
            worker: The worker connection to initialize.
            request_with_init: Invocation containing the global_init_identifier.
            input_schema: Schema for the input data stream.

        """
        log.debug(
            "initializing_additional_worker",
            worker_index=worker.worker_index,
        )

        # Send the request with global_init_identifier
        request_bytes = request_with_init.serialize()
        if worker.stdin_sink.write(request_bytes) != len(request_bytes):
            raise OSError(f"Failed to write request to worker {worker.worker_index}")

        # Read the bind result (we already have output schema from first worker)
        try:
            _bind_result = read_ipc_batch(worker.stdout_buffered, "bind_result")
        except IPCError as e:
            raise ClientError(str(e)) from e

        # Create data writer for this worker
        worker.data_writer = ipc.new_stream(worker.stdin_sink, input_schema)
        log.debug(
            "additional_worker_initialized",
            worker_index=worker.worker_index,
        )

    def _stop_worker(self, worker: WorkerConnection) -> int:
        """Stop a worker subprocess and return its exit code."""
        if worker.proc.stdin:
            worker.proc.stdin.close()
        worker.proc.wait()
        returncode = worker.proc.returncode
        if returncode != 0:
            log.error(
                "worker_exited_with_error",
                worker_index=worker.worker_index,
                returncode=returncode,
            )
        else:
            log.debug(
                "worker_exited",
                worker_index=worker.worker_index,
                returncode=returncode,
            )
        return returncode

    def _process_batch_on_worker(
        self,
        worker: WorkerConnection,
        input_batch: pa.RecordBatch,
        batch_index: int,
    ) -> list[pa.RecordBatch]:
        """Process a single batch on a worker, handling HAVE_MORE_OUTPUT.

        Args:
            worker: The worker connection to use.
            input_batch: The input batch to process.
            batch_index: Index of the batch (for logging).

        Returns:
            List of output batches from processing this input.

        """
        if worker.data_writer is None:
            raise ClientError(
                f"Worker {worker.worker_index} data_writer not initialized"
            )

        output_batches: list[pa.RecordBatch] = []

        while True:
            log.debug(
                "sending_batch_to_worker",
                worker_index=worker.worker_index,
                batch_index=batch_index,
                num_rows=input_batch.num_rows,
            )
            worker.data_writer.write_batch(input_batch)

            if worker.output_reader is None:
                worker.output_reader = ipc.open_stream(worker.stdout_buffered)

            assert worker.output_reader is not None  # for type checker
            output_batch, output_metadata = (
                worker.output_reader.read_next_batch_with_custom_metadata()
            )
            status = output_metadata.get("status") if output_metadata else None

            log.debug(
                "received_output_from_worker",
                worker_index=worker.worker_index,
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
                raise ClientError(
                    f"Unexpected status from worker {worker.worker_index}: {status}"
                )

        return output_batches

    def _finalize_worker(
        self,
        worker: WorkerConnection,
        empty_batch: pa.RecordBatch,
    ) -> list[pa.RecordBatch]:
        """Send finalize signal to a worker and collect final outputs.

        Args:
            worker: The worker connection to finalize.
            empty_batch: Empty batch with correct schema for finalize signal.

        Returns:
            List of final output batches from this worker.

        """
        if worker.data_writer is None or worker.output_reader is None:
            raise ClientError(f"Worker {worker.worker_index} not properly initialized")

        output_batches: list[pa.RecordBatch] = []
        while True:
            log.debug("sending_finalize_to_worker", worker_index=worker.worker_index)
            worker.data_writer.write_batch(
                empty_batch, custom_metadata={"type": "FINALIZE"}
            )

            output_batch, output_metadata = (
                worker.output_reader.read_next_batch_with_custom_metadata()
            )
            status = output_metadata.get("status") if output_metadata else None
            log.debug(
                "received_finalize_from_worker",
                worker_index=worker.worker_index,
                num_rows=output_batch.num_rows,
                status=status,
            )

            if self._handle_log_message(output_batch, output_metadata):
                continue

            output_batches.append(output_batch)

            if status == b"HAVE_MORE_OUTPUT":
                continue
            elif status == b"FINISHED":
                break
            else:
                raise ClientError(
                    f"Unexpected finalize status from worker "
                    f"{worker.worker_index}: {status}"
                )

        worker.data_writer.close()
        return output_batches

    def _worker_thread_loop(
        self,
        worker: WorkerConnection,
        input_queue: "Queue[tuple[int, pa.RecordBatch] | None]",
        output_queue: "Queue[tuple[int, list[pa.RecordBatch]] | BaseException]",
    ) -> None:
        """Thread function that processes batches for a single worker.

        Pulls (batch_index, batch) tuples from input_queue, processes them,
        and pushes (batch_index, output_batches) to output_queue.
        Stops when it receives None from input_queue.

        Note: This only handles batch processing. Finalization is done separately
        after all worker threads complete to ensure all partial state is written.

        Args:
            worker: The worker connection to use.
            input_queue: Queue of (batch_index, batch) tuples, None signals end.
            output_queue: Queue for (batch_index, output_batches) results.

        """
        try:
            while True:
                item = input_queue.get()
                if item is None:
                    # End of input - signal thread completion
                    # Don't close data_writer yet - finalization will handle it
                    output_queue.put((-1, []))
                    break

                batch_index, input_batch = item
                outputs = self._process_batch_on_worker(
                    worker, input_batch, batch_index
                )
                output_queue.put((batch_index, outputs))
        except Exception as e:
            log.exception(
                "worker_thread_error",
                worker_index=worker.worker_index,
                error=str(e),
            )
            output_queue.put(e)

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
            The subprocess return code (of the primary worker).

        """
        if self._proc is None:
            raise ClientError("Client not started")

        # Stop additional workers first
        for worker in self._additional_workers:
            self._stop_worker(worker)
        self._additional_workers = []

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

        # Wait for additional stderr threads
        for stderr_thread in self._stderr_threads:
            stderr_thread.join(timeout=5.0)
            if stderr_thread.is_alive():
                log.warning("additional_stderr_thread_did_not_terminate")
        self._stderr_threads = []

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

        When multiple workers are available (max_processes > 1), batches are
        distributed across workers using a round-robin approach with threading.

        Args:
            function_name: Name of the function to invoke (must be in worker registry).
            arguments: Arguments container with positional and named arguments.
            input: Iterator yielding input RecordBatches. The first batch's schema
                is used for the Invocation.in_out_function_input_schema.
            bind_result_callback: Optional callback invoked with the bind result
                RecordBatch after the worker responds. Useful for inspecting
                output schema or cardinality hints before processing begins.
            projection_ids: Optional list of column indices to project.

        Yields:
            Output RecordBatches from the function. When using multiple workers,
            output order may not match input order.

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

                # If we have additional workers, switch to parallel processing
                if self._additional_workers:
                    yield from self._table_in_out_function_parallel(
                        input_batch=input_batch,
                        input_iterator=input,
                        input_schema=input_schema,
                        data_writer=data_writer,
                    )
                    return

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

    def _table_in_out_function_parallel(
        self,
        *,
        input_batch: pa.RecordBatch,
        input_iterator: Iterator[pa.RecordBatch],
        input_schema: pa.Schema,
        data_writer: ipc.RecordBatchStreamWriter,
    ) -> Generator[pa.RecordBatch, None, None]:
        """Process batches in parallel across multiple workers.

        This method is called when max_processes > 1. It distributes input
        batches across workers using threads and queues.

        Args:
            input_batch: The first input batch (already received).
            input_iterator: Iterator for remaining input batches.
            input_schema: Schema of input batches.
            data_writer: Data writer for the primary worker.

        Yields:
            Output RecordBatches from all workers.

        """
        # Create empty batch for finalize signals
        empty_batch = pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in input_schema],
            schema=input_schema,
        )

        # Create a WorkerConnection for the primary worker
        assert self._stdout_buffered is not None
        assert self._stdin_sink is not None
        primary_worker = WorkerConnection(
            proc=self._proc,  # type: ignore[arg-type]
            stdout_buffered=self._stdout_buffered,
            stdin_sink=self._stdin_sink,
            worker_index=0,
            data_writer=data_writer,
        )

        all_workers = [primary_worker] + self._additional_workers
        num_workers = len(all_workers)

        log.debug("starting_parallel_processing", num_workers=num_workers)

        # Create queues for each worker
        # Queue items are (batch_index, batch) tuples
        input_queues: list[Queue[tuple[int, pa.RecordBatch] | None]] = [
            Queue() for _ in range(num_workers)
        ]
        output_queue: Queue[tuple[int, list[pa.RecordBatch]] | BaseException] = Queue()

        # Start worker threads
        threads: list[threading.Thread] = []
        for i, worker in enumerate(all_workers):
            thread = threading.Thread(
                target=self._worker_thread_loop,
                args=(worker, input_queues[i], output_queue),
                daemon=True,
            )
            thread.start()
            threads.append(thread)

        # Distribute batches round-robin across workers
        batch_index = 0
        batches_sent = 0

        # Send first batch
        worker_idx = batch_index % num_workers
        input_queues[worker_idx].put((batch_index, input_batch))
        batches_sent += 1
        batch_index += 1

        # Send remaining batches
        for input_batch in input_iterator:
            worker_idx = batch_index % num_workers
            input_queues[worker_idx].put((batch_index, input_batch))
            batches_sent += 1
            batch_index += 1

        # Signal end of input to all workers
        # When data_writer is closed, the worker subprocess will receive EOF,
        # causing the generator to be closed and GeneratorExit to be raised.
        for q in input_queues:
            q.put(None)

        log.debug("all_batches_distributed", total_batches=batches_sent)

        # Collect outputs from all workers
        # We expect batches_sent regular outputs + num_workers thread completion signals
        outputs_expected = batches_sent + num_workers
        outputs_received = 0

        while outputs_received < outputs_expected:
            result = output_queue.get()

            # Check for exceptions from worker threads
            if isinstance(result, BaseException):
                raise ClientError(f"Worker thread failed: {result}") from result

            batch_idx, output_batches = result
            outputs_received += 1

            # Combine output batches if needed
            if output_batches:
                combined = list(
                    pa.Table.from_batches(output_batches).combine_chunks().to_batches()
                )
                if len(combined) == 0:
                    yield output_batches[0]
                else:
                    yield combined[0]

            log.debug(
                "output_received",
                batch_index=batch_idx,
                outputs_received=outputs_received,
                outputs_expected=outputs_expected,
            )

        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=5.0)
            if thread.is_alive():
                log.warning("worker_thread_did_not_terminate")

        log.debug("all_worker_threads_complete")

        # Close secondary workers' data writers first to ensure their
        # subprocesses finish and write any remaining state
        for worker in all_workers[1:]:
            if worker.data_writer is not None:
                worker.data_writer.close()
                log.debug(
                    "secondary_worker_closed",
                    worker_index=worker.worker_index,
                )

        # Wait for secondary worker subprocesses to complete
        for worker in all_workers[1:]:
            worker.proc.wait(timeout=5.0)
            log.debug(
                "secondary_worker_exited",
                worker_index=worker.worker_index,
                returncode=worker.proc.returncode,
            )

        # Now finalize the primary worker - all secondary workers have written state
        primary_worker = all_workers[0]
        log.debug("finalizing_primary_worker")
        final_outputs = self._finalize_worker(primary_worker, empty_batch)
        if final_outputs:
            combined = list(
                pa.Table.from_batches(final_outputs).combine_chunks().to_batches()
            )
            if len(combined) == 0:
                yield final_outputs[0]
            else:
                yield combined[0]

        log.debug("parallel_processing_complete")
