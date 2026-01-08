"""VGI client for communicating with VGI workers.

This module provides the Client class for programmatic interaction with VGI workers.
The client manages subprocess lifecycle and Arrow IPC communication.

QUICK START
-----------
Use Client as a context manager to ensure proper cleanup:

    from vgi.client import Client
    from vgi.arguments import Arguments
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
client.table_in_out_function() : Invoke a TableInOutGenerator and stream results
client.table_function() : Invoke a TableFunctionGenerator and stream results
client.scalar_function() : Invoke a ScalarFunction and stream results
client.get_worker_stderr() : Get captured stderr from worker

See Also
--------
vgi.worker.Worker : Base class for workers that Client spawns
vgi.invocation.Invocation : Invocation structure sent to workers
vgi.function.Arguments : Container for function arguments

"""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import threading
from collections.abc import Callable, Generator, Iterator
from dataclasses import dataclass
from queue import Queue
from typing import IO, Any, cast

import pyarrow as pa
import structlog
import structlog.stdlib
from pyarrow import ipc

from vgi.arguments import Arguments
from vgi.client.catalog_mixin import CatalogClientMixin
from vgi.function import FunctionInitInput
from vgi.invocation import InitResult, Invocation, InvocationType
from vgi.ipc_utils import IPCError, read_single_record_batch
from vgi.table_function import TableFunctionInitInput

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

log: structlog.stdlib.BoundLogger = structlog.get_logger().bind(component="client")

worker_log: structlog.stdlib.BoundLogger = structlog.get_logger().bind(
    component="worker"
)


class ClientError(Exception):
    """Error raised by Client operations."""


@dataclass
class WorkerConnection:
    """Holds state for a single worker subprocess connection."""

    proc: subprocess.Popen[bytes]
    stdout_buffered: io.BufferedReader[Any]
    stdin_sink: pa.PythonFile
    worker_index: int
    data_writer: ipc.RecordBatchStreamWriter | None = None
    output_reader: ipc.RecordBatchStreamReader | None = None


@dataclass
class _BindResult:
    """Parsed bind result from worker."""

    max_processes: int
    invocation_id: bytes | None
    output_schema: pa.Schema
    active_features: frozenset[str]
    raw_batch: pa.RecordBatch


class Client(CatalogClientMixin):
    """Client for communicating with VGI workers.

    Manages the subprocess lifecycle and Arrow IPC communication with a VGI
    worker process. Use as a context manager to ensure proper cleanup.

    Also provides catalog operations via CatalogClientMixin - these methods
    spawn ephemeral workers and don't require start()/stop().

    Example:
        with Client("./my_worker.py") as client:
            for batch in client.table_in_out_function(
                function_name="echo",
                arguments=Arguments(positional=[], named={}),
                input=input_batches,
            ):
                process(batch)

    """

    # Timeout for thread join operations (seconds)
    THREAD_JOIN_TIMEOUT: float = 5.0

    # Timeout for worker process wait operations (seconds)
    PROCESS_WAIT_TIMEOUT: float = 5.0

    @staticmethod
    def _combine_batches(batches: list[pa.RecordBatch]) -> pa.RecordBatch | None:
        """Combine multiple RecordBatches into a single RecordBatch.

        Converts the batches to a PyArrow Table, combines chunks, and converts
        back to a single batch. When all input batches have zero rows, PyArrow's
        combine_chunks returns an empty list; in that case, the first original
        batch is returned to preserve the schema.

        Args:
            batches: List of RecordBatches to combine. All batches must have
                compatible schemas.

        Returns:
            A single combined RecordBatch, or None if the input list is empty.

        """
        if not batches:
            return None

        combined = list(pa.Table.from_batches(batches).combine_chunks().to_batches())
        # If all batches were empty, combine_chunks returns empty list
        if len(combined) == 0:
            return batches[0]
        return combined[0]

    def _handle_log_message(
        self,
        output_batch: pa.RecordBatch,
        output_metadata: pa.KeyValueMetadata | None,
    ) -> bool:
        """Handle a log message from the worker if present.

        Detects log messages by checking for zero-row batches with vgi.log_level
        and vgi.log_message metadata keys. When detected, logs the message using
        structlog at the appropriate level. If the log level is "exception",
        raises a ClientError with the message and traceback.

        Args:
            output_batch: The output batch from the worker. A log message is
                detected when this has zero rows.
            output_metadata: Custom metadata dictionary from the batch. Must
                contain b"vgi.log_level" and b"vgi.log_message" keys for the
                batch to be treated as a log message. May optionally contain
                b"vgi.log_extra" with JSON-encoded additional context.

        Returns:
            True if this was a log message and the caller should continue to
            the next batch. False if this was a regular data batch.

        Raises:
            ClientError: If the log message has level "exception", containing
                the error message and traceback from the worker.

        """
        if output_metadata is None:
            return False

        if not (
            output_batch.num_rows == 0
            and output_metadata.get(b"vgi.log_level") is not None
            and output_metadata.get(b"vgi.log_message") is not None
        ):
            return False

        extra: dict[str, Any] = {}
        if output_metadata.get(b"vgi.log_extra") is not None:
            try:
                extra = json.loads(output_metadata[b"vgi.log_extra"].decode())
            except json.JSONDecodeError as e:
                log.error(
                    "failed_to_decode_log_extra",
                    error=str(e),
                    raw=output_metadata[b"vgi.log_extra"],
                )

        level_name = output_metadata[b"vgi.log_level"].decode().lower()
        worker_log._proxy_to_logger(
            level_name,
            output_metadata[b"vgi.log_message"].decode(),
            **extra,
        )

        if level_name == "exception":
            message = output_metadata[b"vgi.log_message"].decode()
            traceback = extra.get("traceback", "")
            full_message = f"Worker Exception: {message}\n{traceback}"
            raise ClientError(full_message)

        return True

    def _parse_bind_result(self, batch: pa.RecordBatch) -> _BindResult:
        """Parse the bind result batch from a worker into structured data.

        Extracts function metadata from the worker's bind response, including
        max_processes (cast to int32), invocation_id, active_features,
        and the output schema (deserialized from IPC bytes).

        Args:
            batch: The bind result RecordBatch from the worker. Expected columns:
                - max_processes: Maximum parallel workers the function supports
                - invocation_id: Unique identifier for this invocation (bytes)
                - output_schema: IPC-serialized Arrow schema for output batches
                - active_features: List of feature flags active for this invocation

        Returns:
            A _BindResult dataclass containing:
                - max_processes: int, the function's parallelism limit
                - invocation_id: bytes or None, unique invocation identifier
                - output_schema: pa.Schema, deserialized output schema
                - active_features: frozenset[str], features active for this invocation
                - raw_batch: The original RecordBatch for reference

        """
        if batch.num_rows != 1:
            raise ClientError(
                "Expected single-row RecordBatch for bind result,"
                f" got {batch.num_rows} rows"
            )
        max_processes_array = batch.column(
            batch.schema.get_field_index("max_processes")
        )
        max_processes_value = max_processes_array.cast(pa.int32()).to_pylist()[0]
        # max_processes should always be set in the bind result
        max_processes: int = (
            max_processes_value if max_processes_value is not None else 1
        )

        invocation_id_array = batch.column(
            batch.schema.get_field_index("invocation_id")
        )
        invocation_id = invocation_id_array.to_pylist()[0]

        # Extract active features - default to empty set for backward compatibility
        active_features: frozenset[str] = frozenset()
        if "active_features" in batch.schema.names:
            features_list = batch.column(
                batch.schema.get_field_index("active_features")
            ).to_pylist()[0]
            if features_list is not None:
                active_features = frozenset(features_list)

        # Extract output schema
        output_schema_bytes = batch.column(
            batch.schema.get_field_index("output_schema")
        ).to_pylist()[0]
        output_schema = pa.ipc.read_schema(pa.BufferReader(output_schema_bytes))  # type: ignore[arg-type]

        return _BindResult(
            max_processes=max_processes,
            invocation_id=invocation_id,
            output_schema=output_schema,
            active_features=active_features,
            raw_batch=batch,
        )

    def _validate_features(
        self,
        requested: frozenset[str],
        active: frozenset[str],
    ) -> None:
        """Validate that the worker activated only features we support.

        Checks that the active features returned by the worker are a subset
        of the features the client requested. This ensures the worker doesn't
        activate features the client cannot handle.

        Args:
            requested: The client_features sent in the Invocation.
            active: The active_features returned in the OutputSpec.

        Raises:
            ClientError: If the worker activated features not requested by
                the client.

        """
        if not active <= requested:
            unexpected = active - requested
            raise ClientError(f"Worker activated unsupported features: {unexpected}")

        log.debug(
            "features_validated",
            requested_features=requested,
            active_features=active,
        )

    def _determine_max_processes(self, requested: int) -> int:
        """Apply system and user limits to the function's requested max_processes.

        Clamps the requested parallelism to the lower of:
        1. The system's CPU count (from os.cpu_count(), defaulting to 1)
        2. The user-specified max_workers (if set via Client constructor)

        Args:
            requested: The max_processes value requested by the function,
                typically from the bind result.

        Returns:
            The effective max_processes after applying all limits. Always >= 1.

        """
        max_processes = requested

        # Limit to CPU count
        cpu_count = os.cpu_count() or 1
        if max_processes > cpu_count:
            log.debug(
                "limiting_max_processes_to_cpu_count",
                requested=max_processes,
                cpu_count=cpu_count,
            )
            max_processes = cpu_count

        # Limit to user-specified max_workers
        if self._max_workers is not None and max_processes > self._max_workers:
            log.debug(
                "limiting_max_processes_to_max_workers",
                requested=max_processes,
                max_workers=self._max_workers,
            )
            max_processes = self._max_workers

        return max_processes

    def _initialize_stream_common(
        self,
        *,
        function_name: str,
        arguments: Arguments,
        input_schema: pa.Schema | None,
        function_type: InvocationType,
        bind_result_callback: Callable[[pa.RecordBatch], None] | None,
        projection_ids: list[int] | None,
        settings: dict[str, str] | None = None,
        transaction_id: bytes | None = None,
    ) -> tuple[_BindResult, InitResult, Invocation]:
        """Perform the common initialization handshake with the primary worker.

        Executes the VGI protocol initialization sequence:
        1. Sends Invocation (function name, arguments, input schema)
        2. Reads and parses the bind result (output schema, max_processes, etc.)
        3. Invokes bind_result_callback if provided
        4. Validates protocol version compatibility
        5. Applies CPU/max_workers limits to max_processes
        6. Sends init data (FunctionInitInput or TableFunctionInitInput)
        7. Reads InitResult (shared state identifier for parallel workers)
        8. Creates an Invocation with global_execution_identifier for additional workers

        Args:
            function_name: Name of the function to invoke (must exist in worker
                registry).
            arguments: Arguments container with positional and named arguments
                to pass to the function.
            input_schema: Schema of input batches for table-in-out functions,
                or None for table functions that generate output without input.
            function_type: Type of function being invoked (SCALAR or TABLE).
                Determines what init data format is sent to the worker.
            bind_result_callback: Optional callback invoked with the raw bind
                result RecordBatch. Called before further processing.
            projection_ids: Optional list of column indices to project in the
                output. Passed to the worker via TableFunctionInitInput (ignored
                for scalar functions).
            settings: Optional dictionary of settings/pragmas to
                pass to the function. Functions that declare required_settings
                in their Meta class will validate these are present.
            transaction_id: Optional unique identifier for the DuckDB transaction.
                When provided, allows functions to participate in transactional
                semantics and correlate calls within the same transaction.

        Returns:
            A tuple of (bind_result, global_init_result, request_with_init):
                - bind_result: Parsed _BindResult with output_schema, max_processes
                - global_init_result: InitResult containing shared state ID
                - request_with_init: Invocation with global_execution_identifier set,
                  suitable for initializing additional parallel workers

        Raises:
            ClientError: If the worker process is not started, or if reading
                the bind result or init result fails.
            OSError: If writing the Invocation or init data fails.
            ClientError: If the worker activated unsupported features.

        """
        if self._stdin_sink is None or self._stdout_buffered is None:
            raise ClientError("Worker process not started. Call start() first.")

        # Send initialization batch
        log.debug("sending_init_batch", function=function_name, arguments=arguments)

        # Client features - currently empty, will be populated as features are added
        client_features: frozenset[str] = frozenset()

        initial_request = Invocation(
            function_name=function_name,
            input_schema=input_schema,
            function_type=function_type,
            correlation_id=self.correlation_id,
            invocation_id=None,
            arguments=arguments,
            client_features=client_features,
            attach_id=self._attach_id,
            settings=settings,
            transaction_id=transaction_id,
        )
        call_parameters_batch_bytes = initial_request.serialize()

        if self._stdin_sink.write(call_parameters_batch_bytes) != len(
            call_parameters_batch_bytes
        ):
            raise OSError("Failed to write call parameters record batch")

        # Read and parse bind result
        log.debug("reading_bind_result")
        try:
            bind_result_batch, bind_custom_metadata = read_single_record_batch(
                self._stdout_buffered, "bind_result"
            )
        except IPCError as e:
            raise ClientError(str(e)) from e

        # Check for bind-time exception (error metadata is in custom_metadata)
        if (
            bind_custom_metadata is not None
            and bind_result_batch.num_rows == 0
            and self._handle_log_message(bind_result_batch, bind_custom_metadata)
        ):
            # _handle_log_message raises ClientError for exceptions
            # If it returns True (non-exception log), unexpected for bind
            raise ClientError("Unexpected log message during bind")

        if bind_result_callback is not None:
            bind_result_callback(bind_result_batch)

        log.debug("bind_result_received", batch=bind_result_batch)

        bind_result = self._parse_bind_result(bind_result_batch)

        # Validate features
        self._validate_features(client_features, bind_result.active_features)

        # Apply limits to max_processes
        bind_result = _BindResult(
            max_processes=self._determine_max_processes(bind_result.max_processes),
            invocation_id=bind_result.invocation_id,
            output_schema=bind_result.output_schema,
            active_features=bind_result.active_features,
            raw_batch=bind_result.raw_batch,
        )

        log.debug(
            "max_processes_determined",
            max_processes=bind_result.max_processes,
            invocation_id=(
                bind_result.invocation_id.hex() if bind_result.invocation_id else None
            ),
        )

        if initial_request.function_type == InvocationType.SCALAR:
            # Scalar functions use empty init input
            init_serialized_bytes = FunctionInitInput().serialize()
        else:
            # Table functions (generator and table-in-out) use TableFunctionInitInput
            init_serialized_bytes = TableFunctionInitInput(
                projection_ids=projection_ids
            ).serialize()

        if self._stdin_sink.write(init_serialized_bytes) != len(init_serialized_bytes):
            raise OSError("Failed to write init record batch")

        # Read init result
        log.debug("reading_init_result")
        try:
            init_result_batch, _ = read_single_record_batch(
                self._stdout_buffered, "init_result"
            )
        except IPCError as e:
            raise ClientError(str(e)) from e

        global_init_result = InitResult.deserialize(init_result_batch)
        log.debug(
            "init_result_received",
            has_identifier=global_init_result.global_execution_identifier is not None,
        )

        # Create request with init for additional workers
        request_with_init = Invocation(
            function_name=function_name,
            input_schema=input_schema,
            function_type=function_type,
            correlation_id=self.correlation_id,
            invocation_id=bind_result.invocation_id,
            global_execution_identifier=global_init_result,
            arguments=arguments,
            transaction_id=transaction_id,
        )

        return bind_result, global_init_result, request_with_init

    def _spawn_additional_workers(
        self,
        max_processes: int,
        request_with_init: Invocation,
        init_fn: Callable[[WorkerConnection, Invocation], None],
    ) -> None:
        """Spawn and initialize additional worker subprocesses in parallel.

        First spawns all worker subprocesses sequentially (fast operation), then
        initializes all workers in parallel using threads. This overlaps the
        Python startup time across all workers for better performance.

        The spawned workers are appended to self._additional_workers list.

        If max_processes is 1 or less, this method returns immediately without
        spawning any workers.

        Args:
            max_processes: Total number of workers desired (including the primary
                worker). For example, if max_processes=4, this method spawns
                3 additional workers (indices 1, 2, 3).
            request_with_init: The Invocation containing the global_execution_identifier
                from the primary worker's initialization. This is sent to each
                additional worker so they share the same global state.
            init_fn: Callable that initializes a single worker. Called with
                (worker, request_with_init) for each spawned worker. Typically
                sends the invocation and reads the bind result.

        Raises:
            ClientError: If any worker fails to initialize. The exception wraps
                the first initialization error encountered.

        """
        if max_processes <= 1:
            return

        # Spawn all worker subprocesses first (fast)
        for worker_index in range(1, max_processes):
            worker = self._spawn_worker(worker_index)
            self._additional_workers.append(worker)

        # Initialize all workers in parallel (overlaps Python startup time)
        init_errors: list[Exception] = []

        def do_init(worker: WorkerConnection) -> None:
            try:
                init_fn(worker, request_with_init)
            except Exception as e:
                init_errors.append(e)

        init_threads: list[threading.Thread] = []
        for worker in self._additional_workers:
            t = threading.Thread(target=do_init, args=(worker,))
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

    def _initialize_function_stream(
        self,
        *,
        function_name: str,
        arguments: Arguments,
        input_schema: pa.Schema | None,
        function_type: InvocationType,
        bind_result_callback: Callable[[pa.RecordBatch], None] | None,
        projection_ids: list[int] | None,
        settings: dict[str, str] | None = None,
        transaction_id: bytes | None = None,
    ) -> tuple[ipc.RecordBatchStreamWriter | None, ipc.RecordBatchStreamReader | None]:
        """Initialize the VGI protocol stream and prepare for data transfer.

        Performs the full initialization sequence by calling _initialize_stream_common,
        then spawns additional workers if max_processes > 1, and finally sets up
        the appropriate I/O streams:

        For table-in-out functions (input_schema is not None):
            - Creates an IPC stream writer for sending input batches
            - Output reader is opened lazily after first input (worker blocks on input)

        For table functions (input_schema is None):
            - No data writer is created (no input to send)
            - Opens an IPC stream reader immediately for output

        Args:
            function_name: Name of the function to invoke (must exist in worker
                registry).
            arguments: Arguments container with positional and named arguments.
            input_schema: Schema of input batches for table-in-out functions,
                or None for table functions.
            function_type: Type of function being invoked (SCALAR or TABLE).
            bind_result_callback: Optional callback invoked with the raw bind
                result RecordBatch.
            projection_ids: Optional list of column indices to project.
            settings: Optional dictionary of settings/pragmas.
            transaction_id: Optional unique identifier for the DuckDB transaction.

        Returns:
            A tuple of (data_writer, output_reader):
                - data_writer: IPC stream writer for input batches, or None for
                  table functions
                - output_reader: IPC stream reader for output batches, or None
                  for table-in-out functions (opened lazily after sending input)

        Raises:
            ClientError: If protocol communication fails or worker initialization
                fails.
            OSError: If writing to the worker fails.
            ProtocolVersionError: If the worker's protocol version is incompatible.

        """
        bind_result, _, request_with_init = self._initialize_stream_common(
            function_name=function_name,
            arguments=arguments,
            input_schema=input_schema,
            function_type=function_type,
            bind_result_callback=bind_result_callback,
            projection_ids=projection_ids,
            settings=settings,
            transaction_id=transaction_id,
        )

        # Spawn additional workers if needed
        def init_worker(worker: WorkerConnection, request: Invocation) -> None:
            self._initialize_additional_worker(worker, request, input_schema)

        self._spawn_additional_workers(
            bind_result.max_processes,
            request_with_init,
            init_worker,
        )

        # Create data writer for table-in-out functions
        data_writer: ipc.RecordBatchStreamWriter | None = None
        if input_schema is not None:
            assert self._stdin_sink is not None
            data_writer = ipc.new_stream(self._stdin_sink, input_schema)
            log.debug("starting_data_batches")

        # Open output reader only for table functions (no input).
        # For table-in-out, output_reader is opened lazily after sending input
        # because the worker waits for input before writing output.
        output_reader: ipc.RecordBatchStreamReader | None = None
        if input_schema is None:
            assert self._stdout_buffered is not None
            output_reader = ipc.open_stream(self._stdout_buffered)
            log.debug("output_stream_opened")

        return data_writer, output_reader

    def __init__(
        self,
        server_path: str,
        correlation_id: str = "",
        passthrough_stderr: bool = False,
        max_workers: int | None = None,
        attach_id: bytes | None = None,
    ):
        """Initialize the VGI client.

        Creates a client configured to communicate with a VGI worker. The worker
        subprocess is not started until start() is called or the client is used
        as a context manager.

        Args:
            server_path: Shell command or path to the VGI worker executable.
                Executed via shell=True, so can include arguments (e.g.,
                "python worker.py --debug" or "./my_worker").
            correlation_id: Identifier attached to all requests for tracing and
                log correlation across distributed systems. Defaults to empty
                string.
            passthrough_stderr: If True, worker stderr is passed through to
                the parent process's stderr in real-time. If False (default),
                stderr is captured in a buffer and available via
                get_worker_stderr() after processing completes.
            max_workers: Maximum number of parallel worker processes. If set,
                overrides the function's max_processes when that value exceeds
                this limit. Also capped by os.cpu_count(). If None, uses the
                function's max_processes (still capped by CPU count).
            attach_id: Optional unique identifier for the DuckDB database
                attachment. When VGI is used from an attached database, this
                allows tracing calls back to that specific attachment.

        Example:
            >>> client = Client("vgi-example-worker", max_workers=4)
            >>> with client:
            ...     for batch in client.table_in_out_function(...):
            ...         process(batch)

        """
        self.server_path = server_path
        self.correlation_id = correlation_id
        self.passthrough_stderr = passthrough_stderr
        self._max_workers = max_workers
        self._attach_id = attach_id
        self._proc: subprocess.Popen[bytes] | None = None
        self._stdout_buffered: io.BufferedReader[Any] | None = None
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
        """Return all captured stderr from the worker processes.

        Returns stderr output from the primary worker and all additional workers
        spawned for parallel processing. The output is accumulated in a shared
        buffer throughout the client's lifetime.

        This method is thread-safe and can be called while processing is ongoing,
        though the buffer may not yet contain all output until the workers have
        completed.

        Returns:
            All captured stderr output as a UTF-8 decoded string. Invalid UTF-8
            sequences are replaced with the Unicode replacement character.

        Note:
            This method only returns data when passthrough_stderr=False was set
            in the constructor. When passthrough_stderr=True, stderr goes directly
            to the parent process's stderr and this method returns an empty string.

        """
        with self._stderr_lock:
            return b"".join(self._stderr_buffer).decode("utf-8", errors="replace")

    def _spawn_worker(self, worker_index: int) -> WorkerConnection:
        """Spawn a new worker subprocess and return its connection.

        Creates a subprocess running the server_path command, sets up stdin/stdout
        pipes for Arrow IPC communication, and starts a background thread to
        drain stderr if passthrough_stderr is False.

        Args:
            worker_index: Index identifying this worker (0 for primary, 1+ for
                additional workers). Used for logging and tracking.

        Returns:
            WorkerConnection containing the subprocess handle, buffered stdout
            reader, stdin sink wrapped as a PyArrow PythonFile, and the worker
            index. The data_writer and output_reader fields are None and must
            be initialized separately.

        Raises:
            ClientError: If stdout or stderr pipes fail to be created.

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
        assert proc.stdin is not None, "stdin pipe not created for worker"
        stdin_sink = pa.PythonFile(cast(io.IOBase, proc.stdin))

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
        input_schema: pa.Schema | None,
    ) -> None:
        """Initialize an additional worker with the shared global init state.

        Sends the Invocation (which includes the global_execution_identifier from the
        primary worker) to this worker, reads and discards the bind result (since
        the output schema was already obtained from the primary worker), and
        creates a data_writer on the worker if input_schema is provided.

        After this method returns, the worker is ready to receive input batches
        (for table-in-out functions) or produce output (for table functions).

        Args:
            worker: The worker connection to initialize. Must have valid stdin_sink
                and stdout_buffered handles. The data_writer field will be set
                if input_schema is not None.
            request_with_init: Invocation containing the global_execution_identifier
                from the primary worker's InitResult. This ensures all
                workers share the same initialization state.
            input_schema: Schema for the input data stream. If provided, a
                RecordBatchStreamWriter is created and assigned to worker.data_writer.
                Pass None for table functions that don't receive input batches.

        Raises:
            OSError: If writing the request to the worker's stdin fails.
            ClientError: If reading the bind result fails due to IPC errors.

        """
        log.debug(
            "initializing_additional_worker",
            worker_index=worker.worker_index,
        )

        # Send the request with global_execution_identifier
        request_bytes = request_with_init.serialize()
        if worker.stdin_sink.write(request_bytes) != len(request_bytes):
            raise OSError(f"Failed to write request to worker {worker.worker_index}")

        # Read the bind result (we already have output schema from first worker)
        try:
            _bind_result, _ = read_single_record_batch(
                worker.stdout_buffered, "bind_result"
            )
        except IPCError as e:
            raise ClientError(str(e)) from e

        # Create data writer for this worker (only for table-in-out functions)
        if input_schema is not None:
            worker.data_writer = ipc.new_stream(worker.stdin_sink, input_schema)

        log.debug(
            "additional_worker_initialized",
            worker_index=worker.worker_index,
        )

    def _stop_worker(self, worker: WorkerConnection) -> int:
        """Stop a worker subprocess and wait for it to exit.

        Closes the worker's stdin pipe to signal EOF, then waits for the
        subprocess to terminate. Logs an error if the worker exits with
        a non-zero return code.

        Args:
            worker: The worker connection to stop. The subprocess will be
                terminated and the connection should not be used after this call.

        Returns:
            The subprocess exit code. Returns 0 for normal termination,
            non-zero values indicate errors or abnormal termination.

        """
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

    def _join_threads(self, threads: list[threading.Thread]) -> None:
        """Wait for all threads to complete with timeout.

        Joins each thread with a timeout of THREAD_JOIN_TIMEOUT seconds.
        Logs a warning for any thread that does not terminate within the
        timeout period but does not raise an exception.

        Args:
            threads: List of Thread objects to wait for. Threads that have
                already completed will return immediately from join().

        """
        for thread in threads:
            thread.join(timeout=self.THREAD_JOIN_TIMEOUT)
            if thread.is_alive():
                log.warning("worker_thread_did_not_terminate")

    def _close_secondary_workers(
        self,
        workers: list[WorkerConnection],
        close_data_writers: bool = False,
    ) -> None:
        """Close secondary workers (all except the first) and wait for them to exit.

        Signals EOF to each secondary worker and waits for them to terminate.
        The primary worker (index 0) is not affected and remains running.

        For table-in-out functions, closing the data_writer sends an IPC stream
        end marker that triggers the worker to finalize. For table functions,
        closing stdin signals EOF directly.

        Args:
            workers: List of all workers where workers[0] is the primary worker
                and workers[1:] are secondary workers. Only secondary workers
                are closed by this method.
            close_data_writers: If True, closes each secondary worker's data_writer
                (used for table-in-out functions where input is streamed via IPC).
                If False, closes stdin directly (used for table functions that
                don't receive input batches).

        Raises:
            subprocess.TimeoutExpired: If a worker doesn't exit within
                PROCESS_WAIT_TIMEOUT seconds (propagated from proc.wait()).

        """
        secondary_workers = workers[1:]

        if close_data_writers:
            # Close data writers first to signal EOF to workers
            for worker in secondary_workers:
                if worker.data_writer is not None:
                    worker.data_writer.close()
                    log.debug(
                        "secondary_worker_data_writer_closed",
                        worker_index=worker.worker_index,
                    )
        else:
            # Close stdin for table functions
            for worker in secondary_workers:
                if worker.proc.stdin:
                    worker.proc.stdin.close()

        # Wait for all secondary workers to exit
        for worker in secondary_workers:
            worker.proc.wait(timeout=self.PROCESS_WAIT_TIMEOUT)
            log.debug(
                "secondary_worker_exited",
                worker_index=worker.worker_index,
                returncode=worker.proc.returncode,
            )

    def _create_primary_worker(
        self,
        *,
        data_writer: ipc.RecordBatchStreamWriter | None = None,
        output_reader: ipc.RecordBatchStreamReader | None = None,
    ) -> WorkerConnection:
        """Create a WorkerConnection wrapper for the primary worker subprocess.

        Wraps the client's existing primary subprocess (self._proc) and I/O
        handles into a WorkerConnection structure. This allows the primary
        worker to be used interchangeably with additional workers in parallel
        processing code.

        The primary worker uses worker_index=0.

        Args:
            data_writer: RecordBatchStreamWriter for sending input batches to
                the worker. Used for table-in-out functions. Pass None for
                table functions that don't receive input.
            output_reader: RecordBatchStreamReader for receiving output batches
                from the worker. Used for table functions where output is read
                immediately. Pass None for table-in-out functions where the
                reader is opened lazily after sending input.

        Returns:
            WorkerConnection wrapping the primary subprocess with the provided
            data_writer and output_reader.

        """
        assert self._stdout_buffered is not None
        assert self._stdin_sink is not None

        return WorkerConnection(
            proc=self._proc,  # type: ignore[arg-type]
            stdout_buffered=self._stdout_buffered,
            stdin_sink=self._stdin_sink,
            worker_index=0,
            data_writer=data_writer,
            output_reader=output_reader,
        )

    def _process_batch_on_worker(
        self,
        worker: WorkerConnection,
        input_batch: pa.RecordBatch,
        batch_index: int,
    ) -> list[pa.RecordBatch]:
        """Send a batch to a worker and collect all output batches.

        Writes the input batch to the worker's data_writer, then reads output
        batches from the worker. If the worker returns HAVE_MORE_OUTPUT status,
        continues reading until NEED_MORE_INPUT is received. Log messages from
        the worker are handled via _handle_log_message.

        The output_reader is opened lazily on the first call if not already open.

        Args:
            worker: The worker connection to use. Must have data_writer
                initialized. The output_reader will be created if None.
            input_batch: The input RecordBatch to send to the worker.
            batch_index: Index of this batch in the input sequence (for logging).

        Returns:
            List of output RecordBatches produced by processing this input batch.
            May contain zero or more batches depending on the function's behavior.

        Raises:
            ClientError: If worker.data_writer is None (not initialized), or if
                the worker returns an unexpected status (neither HAVE_MORE_OUTPUT
                nor NEED_MORE_INPUT), or if the worker raises an exception
                (detected via log message handling).

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
            status = output_metadata.get(b"status") if output_metadata else None

            log.debug(
                "received_output_from_worker",
                worker_index=worker.worker_index,
                num_rows=output_batch.num_rows,
                status=status,
            )

            if self._handle_log_message(output_batch, output_metadata):
                continue

            output_batches.append(output_batch)

            # status is None for scalar functions (which don't emit status)
            # and means we're done with this batch
            if status == b"HAVE_MORE_OUTPUT":
                continue
            elif status == b"NEED_MORE_INPUT" or status is None:
                break
            else:
                raise ClientError(
                    f"Unexpected status from worker {worker.worker_index}: {status!r}"
                )

        return output_batches

    def _finalize_worker(
        self,
        worker: WorkerConnection,
        empty_batch: pa.RecordBatch,
    ) -> list[pa.RecordBatch]:
        """Send FINALIZE signal to a worker and collect final output batches.

        Writes an empty batch with custom_metadata={"type": "FINALIZE"} to signal
        the worker that all input has been sent. The worker's finalize() method
        is then invoked to produce any final aggregated results.

        Continues reading output batches while the worker returns HAVE_MORE_OUTPUT
        status, until FINISHED status is received. Closes the worker's data_writer
        after finalization is complete.

        Args:
            worker: The worker connection to finalize. Must have both data_writer
                and output_reader initialized.
            empty_batch: An empty RecordBatch with the correct input schema.
                Used as the carrier for the FINALIZE signal metadata.

        Returns:
            List of final output RecordBatches from the worker's finalize() method.
            May be empty if the function produces no final output.

        Raises:
            ClientError: If worker.data_writer or worker.output_reader is None,
                or if the worker returns an unexpected status (neither
                HAVE_MORE_OUTPUT nor FINISHED), or if the worker raises an
                exception (detected via log message handling).

        """
        if worker.data_writer is None or worker.output_reader is None:
            raise ClientError(f"Worker {worker.worker_index} not properly initialized")

        output_batches: list[pa.RecordBatch] = []
        while True:
            log.debug("sending_finalize_to_worker", worker_index=worker.worker_index)
            worker.data_writer.write_batch(
                empty_batch, custom_metadata={b"type": b"FINALIZE"}
            )

            output_batch, output_metadata = (
                worker.output_reader.read_next_batch_with_custom_metadata()
            )
            status = output_metadata.get(b"status") if output_metadata else None
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
                    f"{worker.worker_index}: {status!r}"
                )

        worker.data_writer.close()
        return output_batches

    def _worker_thread_loop(
        self,
        worker: WorkerConnection,
        input_queue: Queue[tuple[int, pa.RecordBatch] | None],
        output_queue: Queue[tuple[int, list[pa.RecordBatch]] | BaseException],
    ) -> None:
        """Thread function that processes batches for a single worker.

        Runs in a dedicated thread, pulling (batch_index, batch) tuples from
        the input queue, processing them via _process_batch_on_worker, and
        pushing (batch_index, output_batches) tuples to the output queue.

        When None is received from input_queue, signals thread completion by
        pushing (-1, []) to output_queue and exits. The data_writer is NOT
        closed here; finalization is handled separately by the main thread
        after all worker threads complete.

        If an exception occurs during processing, it is caught, logged, and
        pushed to output_queue as the exception object itself (not wrapped
        in a tuple).

        Args:
            worker: The worker connection to use for processing batches.
            input_queue: Thread-safe queue providing (batch_index, RecordBatch)
                tuples for processing. A None value signals end of input.
            output_queue: Thread-safe queue for results. Receives either
                (batch_index, list[RecordBatch]) tuples for successful processing,
                (-1, []) to signal thread completion, or a BaseException if
                processing fails.

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
        """Start the primary worker subprocess.

        Spawns the worker process using the server_path configured in __init__,
        sets up stdin/stdout pipes for Arrow IPC communication, and starts a
        background thread to drain stderr (if passthrough_stderr is False).

        After this method returns, the client is ready to invoke functions via
        table_in_out_function() or table_function(). When using the context
        manager protocol (with statement), this method is called automatically.

        The stderr buffer is cleared when start() is called, so any stderr from
        previous runs is discarded.

        Raises:
            ClientError: If the client is already started (call stop() first),
                or if stdout/stderr pipes fail to be created.

        """
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

        self._stdout_buffered = io.BufferedReader(cast(io.RawIOBase, self._proc.stdout))
        assert self._proc.stdin is not None, "stdin pipe not created for worker"
        self._stdin_sink = pa.PythonFile(cast(io.IOBase, self._proc.stdin))

    def stop(self) -> int:
        """Stop all worker subprocesses and clean up resources.

        Terminates all workers in the following order:
        1. Stops all additional workers (spawned for parallel processing)
        2. Closes the primary worker's stdin pipe to signal EOF
        3. Waits for the primary worker process to terminate
        4. Waits for all stderr drain threads to complete (with timeout)
        5. Resets all internal state

        After this method returns, the client can be started again with start().
        When using the context manager protocol (with statement), this method
        is called automatically on exit.

        Returns:
            The exit code of the primary worker process. Returns 0 for normal
            termination, non-zero values indicate errors. Exit codes from
            additional workers are logged but not returned.

        Raises:
            ClientError: If the client was not started (call start() first).

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
            self._stderr_thread.join(timeout=self.THREAD_JOIN_TIMEOUT)
            if self._stderr_thread.is_alive():
                log.warning("stderr_thread_did_not_terminate")

        # Wait for additional stderr threads
        for stderr_thread in self._stderr_threads:
            stderr_thread.join(timeout=self.THREAD_JOIN_TIMEOUT)
            if stderr_thread.is_alive():
                log.warning("additional_stderr_thread_did_not_terminate")
        self._stderr_threads = []

        self._proc = None
        self._stdout_buffered = None
        self._stdin_sink = None
        self._stderr_thread = None
        return returncode

    def __enter__(self) -> Client:
        """Enter the context manager by starting the worker subprocess.

        Calls start() to spawn the primary worker process and prepare for
        function invocations. Returns self for use in the with statement.

        Returns:
            This Client instance, ready to invoke functions.

        Raises:
            ClientError: If the worker is already started or if subprocess
                creation fails.

        Example:
            with Client("vgi-example-worker") as client:
                for batch in client.table_in_out_function(...):
                    process(batch)

        """
        self.start()
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:
        """Exit the context manager by stopping all worker subprocesses.

        Calls stop() to terminate all workers and clean up resources. This
        ensures proper cleanup even if an exception occurred within the with
        block.

        Args:
            _exc_type: Exception type if an exception was raised, else None.
            _exc_val: Exception instance if an exception was raised, else None.
            _exc_tb: Traceback if an exception was raised, else None.

        Returns:
            None (does not suppress exceptions).

        """
        self.stop()

    def table_in_out_function(
        self,
        *,
        function_name: str,
        input: Iterator[pa.RecordBatch],
        arguments: Arguments | None = None,
        bind_result_callback: Callable[[pa.RecordBatch], None] | None = None,
        projection_ids: list[int] | None = None,
        settings: dict[str, str] | None = None,
        transaction_id: bytes | None = None,
    ) -> Generator[pa.RecordBatch, None, None]:
        """Invoke a table-in-out function on the worker and stream results.

        Implements the full VGI streaming protocol for table-in-out functions:
        1. Reads the first input batch to determine the input schema
        2. Sends Invocation to worker and receives bind result
        3. Spawns additional workers if max_processes > 1
        4. Distributes input batches to workers (round-robin for parallel mode)
        5. Collects output batches, handling HAVE_MORE_OUTPUT responses
        6. Sends FINALIZE signal when input is exhausted
        7. Yields final output batches from finalize()

        For parallel processing (max_processes > 1), input batches are distributed
        round-robin across workers using dedicated threads. Output order may not
        match input order in parallel mode. Only the primary worker receives the
        FINALIZE signal and produces final aggregated output.

        Args:
            function_name: Name of the function to invoke. Must exist in the
                worker's registry.
            input: Iterator yielding input RecordBatches. Must yield at least one
                batch. The first batch's schema is used to initialize the IPC
                stream. If the iterator is empty, no output is produced.
            arguments: Optional Arguments container with positional and named
                arguments to pass to the function. Defaults to empty Arguments().
            bind_result_callback: Optional callback invoked with the raw bind
                result RecordBatch before processing begins. Useful for inspecting
                output schema, max_processes, or cardinality hints.
            projection_ids: Optional list of column indices for column projection.
                Passed to the worker via TableFunctionInitInput.
            settings: Optional dictionary of settings/pragmas to
                pass to the function. Functions that declare required_settings
                in their Meta class will validate these are present.
            transaction_id: Optional unique identifier for the DuckDB transaction.
                When provided, allows functions to participate in transactional
                semantics and correlate calls within the same transaction.

        Yields:
            Output RecordBatches from the function. In single-worker mode, output
            order corresponds to input order. In parallel mode (max_processes > 1),
            output order is non-deterministic due to round-robin distribution.
            Final output from finalize() is always yielded last.

        Raises:
            ClientError: If the client is not started, input iterator yields
                non-RecordBatch objects, communication with the worker fails,
                or the worker returns an unexpected status or exception.

        Example:
            >>> with Client("vgi-example-worker") as client:
            ...     batches = [pa.RecordBatch.from_pydict({"x": [1, 2, 3]})]
            ...     for output in client.table_in_out_function(
            ...         function_name="echo",
            ...         input=iter(batches),
            ...     ):
            ...         print(output.to_pydict())

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

        # Get the first batch to determine schema and initialize
        for input_batch in input:
            if not isinstance(input_batch, pa.RecordBatch):
                raise ClientError("Input iterator must yield RecordBatches")

            input_schema = input_batch.schema
            data_writer, _ = self._initialize_function_stream(
                function_name=function_name,
                arguments=arguments,
                input_schema=input_schema,
                function_type=InvocationType.TABLE,
                bind_result_callback=bind_result_callback,
                projection_ids=projection_ids,
                settings=settings,
                transaction_id=transaction_id,
            )

            # Use parallel processing for all cases (handles both single and
            # multi-worker)
            assert data_writer is not None  # set when input_schema is not None
            yield from self._table_in_out_function_parallel(
                input_batch=input_batch,
                input_iterator=input,
                input_schema=input_schema,
                data_writer=data_writer,
            )
            return

    def _table_in_out_function_parallel(
        self,
        *,
        input_batch: pa.RecordBatch,
        input_iterator: Iterator[pa.RecordBatch],
        input_schema: pa.Schema,
        data_writer: ipc.RecordBatchStreamWriter,
    ) -> Generator[pa.RecordBatch, None, None]:
        """Process table-in-out batches across one or more workers using threads.

        Handles both single-worker and multi-worker cases uniformly. For each
        worker, spawns a dedicated thread that pulls batches from an input queue,
        sends them to the worker, and pushes results to a shared output queue.

        Processing flow:
        1. Creates worker connection objects for primary + additional workers
        2. Starts one thread per worker running _worker_thread_loop
        3. Distributes input batches round-robin to worker input queues
        4. Signals end-of-input to all workers via None sentinel
        5. Collects all output batches from shared output queue
        6. Waits for worker threads to complete
        7. Closes secondary workers
        8. Finalizes primary worker and yields final output

        The primary worker always receives the FINALIZE signal and produces any
        aggregated final output. Secondary workers only process batches without
        finalization.

        Args:
            input_batch: The first input batch, already consumed from the
                iterator by table_in_out_function().
            input_iterator: Iterator for remaining input batches. May be empty
                if all input was in the first batch.
            input_schema: Schema of all input batches. Used to create an empty
                batch for the FINALIZE signal.
            data_writer: IPC stream writer for the primary worker, already
                initialized by _initialize_function_stream().

        Yields:
            Output RecordBatches from processing, in non-deterministic order for
            multi-worker mode. When multiple batches are returned for a single
            input (HAVE_MORE_OUTPUT), they are combined into one batch. Final
            output from the primary worker's finalize() is yielded last.

        Raises:
            ClientError: If a worker thread fails with an exception.

        """
        # Create empty batch for finalize signals
        empty_batch = pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in input_schema],
            schema=input_schema,
        )

        primary_worker = self._create_primary_worker(data_writer=data_writer)
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
            combined = self._combine_batches(output_batches)
            if combined is not None:
                yield combined

            log.debug(
                "output_received",
                batch_index=batch_idx,
                outputs_received=outputs_received,
                outputs_expected=outputs_expected,
            )

        self._join_threads(threads)
        log.debug("all_worker_threads_complete")

        self._close_secondary_workers(all_workers, close_data_writers=True)

        # Now finalize the primary worker - all secondary workers have written state
        log.debug("finalizing_primary_worker")
        final_outputs = self._finalize_worker(primary_worker, empty_batch)
        # Yield finalize batches individually to preserve order
        yield from final_outputs

        log.debug("parallel_processing_complete")

    def _table_function_parallel(
        self,
        *,
        primary_output_reader: ipc.RecordBatchStreamReader,
    ) -> Generator[pa.RecordBatch, None, None]:
        """Read output from table function workers using parallel threads.

        Handles both single-worker and multi-worker cases uniformly. For each
        worker, spawns a dedicated thread that reads output batches and pushes
        them to a shared output queue. Output is yielded as it becomes available
        from any worker.

        Processing flow:
        1. Creates worker connection objects for primary + additional workers
        2. Starts one thread per worker to read output batches
        3. Each thread reads until the worker's IPC stream ends (StopIteration)
        4. Threads push batches to shared output queue, None when complete
        5. Main thread yields batches as they arrive from the queue
        6. When all workers finish, joins threads and closes secondary workers

        Unlike table-in-out functions, table functions don't receive input or
        need finalization - they simply generate output batches until complete.

        Args:
            primary_output_reader: IPC stream reader for the primary worker,
                already opened by _initialize_function_stream(). Additional
                workers open their readers lazily in their threads.

        Yields:
            Output RecordBatches from all workers in non-deterministic order.
            Batches are yielded as soon as they are available from any worker.

        Raises:
            ClientError: If a worker thread fails with an exception, or if
                a worker sends an exception-level log message.

        """
        primary_worker = self._create_primary_worker(
            output_reader=primary_output_reader
        )
        all_workers = [primary_worker] + self._additional_workers
        num_workers = len(all_workers)

        log.debug("starting_parallel_table_function", num_workers=num_workers)

        # Queue for collecting output from all workers
        output_queue: Queue[pa.RecordBatch | BaseException | None] = Queue()

        def read_worker_output(worker: WorkerConnection) -> None:
            """Thread function that reads all output from a single worker."""
            try:
                # Open output reader lazily if not already opened
                if worker.output_reader is None:
                    worker.output_reader = ipc.open_stream(worker.stdout_buffered)

                while True:
                    try:
                        output_batch, output_metadata = (
                            worker.output_reader.read_next_batch_with_custom_metadata()
                        )
                    except StopIteration:
                        # Worker finished producing output
                        output_queue.put(None)
                        break

                    log.debug(
                        "received_output_from_worker",
                        worker_index=worker.worker_index,
                        num_rows=output_batch.num_rows,
                    )

                    # Check for log messages (including exceptions)
                    if self._handle_log_message(output_batch, output_metadata):
                        continue

                    output_queue.put(output_batch)

            except Exception as e:
                log.exception(
                    "table_function_worker_thread_error",
                    worker_index=worker.worker_index,
                    error=str(e),
                )
                output_queue.put(e)

        # Start reader threads for all workers
        threads: list[threading.Thread] = []
        for worker in all_workers:
            thread = threading.Thread(
                target=read_worker_output,
                args=(worker,),
                daemon=True,
            )
            thread.start()
            threads.append(thread)

        # Collect outputs from all workers until all are done
        workers_finished = 0
        while workers_finished < num_workers:
            result = output_queue.get()

            # Check for exceptions from worker threads
            if isinstance(result, BaseException):
                raise ClientError(f"Worker thread failed: {result}") from result

            # None signals a worker finished
            if result is None:
                workers_finished += 1
                log.debug(
                    "worker_finished",
                    workers_finished=workers_finished,
                    total_workers=num_workers,
                )
                continue

            # Yield the output batch
            yield result

        self._join_threads(threads)
        log.debug("all_table_function_workers_complete")

        self._close_secondary_workers(all_workers, close_data_writers=False)
        log.debug("parallel_table_function_complete")

    def table_function(
        self,
        *,
        function_name: str,
        arguments: Arguments | None = None,
        bind_result_callback: Callable[[pa.RecordBatch], None] | None = None,
        projection_ids: list[int] | None = None,
        settings: dict[str, str] | None = None,
        transaction_id: bytes | None = None,
    ) -> Generator[pa.RecordBatch, None, None]:
        """Invoke a table function (source function) and stream output batches.

        Table functions generate output batches without receiving input data.
        They are useful for data sources, generators, or functions that produce
        results based solely on their arguments (e.g., sequence generators,
        file readers, API clients).

        Processing flow:
        1. Sends Invocation to worker (with input_schema=None)
        2. Receives bind result with output schema and max_processes
        3. Spawns additional workers if max_processes > 1
        4. Reads output batches from all workers in parallel
        5. Yields batches as they become available

        For parallel processing (max_processes > 1), output is read from all
        workers concurrently using threads. Output order is non-deterministic.

        Args:
            function_name: Name of the function to invoke. Must exist in the
                worker's registry and be a table function (not table-in-out).
            arguments: Optional Arguments container with positional and named
                arguments to pass to the function. Defaults to empty Arguments().
            bind_result_callback: Optional callback invoked with the raw bind
                result RecordBatch before processing begins. Useful for inspecting
                output schema, max_processes, or cardinality hints.
            projection_ids: Optional list of column indices for column projection.
                Passed to the worker via TableFunctionInitInput.
            settings: Optional dictionary of settings/pragmas to
                pass to the function. Functions that declare required_settings
                in their Meta class will validate these are present.
            transaction_id: Optional unique identifier for the DuckDB transaction.
                When provided, allows functions to participate in transactional
                semantics and correlate calls within the same transaction.

        Yields:
            Output RecordBatches from the function. In parallel mode
            (max_processes > 1), output order is non-deterministic.

        Raises:
            ClientError: If the client is not started, communication with the
                worker fails, or the worker returns an exception.

        Example:
            >>> with Client("vgi-example-worker") as client:
            ...     for batch in client.table_function(
            ...         function_name="generate_sequence",
            ...         arguments=Arguments(positional=[100]),
            ...     ):
            ...         print(batch.to_pydict())

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

        _, output_reader = self._initialize_function_stream(
            function_name=function_name,
            arguments=arguments,
            input_schema=None,
            function_type=InvocationType.TABLE,
            bind_result_callback=bind_result_callback,
            projection_ids=projection_ids,
            settings=settings,
            transaction_id=transaction_id,
        )

        if output_reader is None:
            raise ClientError("Protocol error: output_reader not initialized")

        # Use parallel processing for all cases (handles both single and multi-worker)
        yield from self._table_function_parallel(
            primary_output_reader=output_reader,
        )

    def scalar_function(
        self,
        *,
        function_name: str,
        input: Iterator[pa.RecordBatch],
        arguments: Arguments | None = None,
        bind_result_callback: Callable[[pa.RecordBatch], None] | None = None,
        settings: dict[str, str] | None = None,
        transaction_id: bytes | None = None,
    ) -> Generator[pa.RecordBatch, None, None]:
        """Invoke a scalar function on the worker and stream results.

        Scalar functions transform input batches to single-column output with
        1:1 row mapping. Processing ends when input is exhausted.

        Processing flow:
        1. Reads the first input batch to determine the input schema
        2. Sends Invocation to worker and receives bind result
        3. Spawns additional workers if max_processes > 1
        4. Distributes input batches to workers (round-robin for parallel mode)
        5. Collects and yields output batches

        For parallel processing (max_processes > 1), input batches are distributed
        round-robin across workers using dedicated threads. Output order may not
        match input order in parallel mode.

        Args:
            function_name: Name of the function to invoke. Must exist in the
                worker's registry.
            input: Iterator yielding input RecordBatches. Must yield at least one
                batch. The first batch's schema is used to initialize the IPC
                stream. If the iterator is empty, no output is produced.
            arguments: Optional Arguments container with positional and named
                arguments to pass to the function. Defaults to empty Arguments().
            bind_result_callback: Optional callback invoked with the raw bind
                result RecordBatch before processing begins. Useful for inspecting
                output schema or max_processes.
            settings: Optional dictionary of settings/pragmas to
                pass to the function. Functions that declare required_settings
                in their Meta class will validate these are present.
            transaction_id: Optional unique identifier for the DuckDB transaction.
                When provided, allows functions to participate in transactional
                semantics and correlate calls within the same transaction.

        Yields:
            Output RecordBatches from the function. Each output batch has a single
            column and the same number of rows as its corresponding input batch.
            In single-worker mode, output order corresponds to input order.
            In parallel mode (max_processes > 1), output order is non-deterministic.

        Raises:
            ClientError: If the client is not started, input iterator yields
                non-RecordBatch objects, communication with the worker fails,
                or the worker returns an unexpected status or exception.

        Example:
            >>> with Client("vgi-example-worker") as client:
            ...     batches = [pa.RecordBatch.from_pydict({"x": [1, 2, 3]})]
            ...     for output in client.scalar_function(
            ...         function_name="double_column",
            ...         input=iter(batches),
            ...         arguments=Arguments(positional=[pa.scalar("x")]),
            ...     ):
            ...         print(output.to_pydict())
            {'result': [2, 4, 6]}

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

        # Get the first batch to determine schema and initialize
        for input_batch in input:
            if not isinstance(input_batch, pa.RecordBatch):
                raise ClientError("Input iterator must yield RecordBatches")

            input_schema = input_batch.schema
            data_writer, _ = self._initialize_function_stream(
                function_name=function_name,
                arguments=arguments,
                input_schema=input_schema,
                function_type=InvocationType.SCALAR,
                bind_result_callback=bind_result_callback,
                projection_ids=None,  # Scalar functions don't use projection
                settings=settings,
                transaction_id=transaction_id,
            )

            # Use parallel processing for all cases (handles both single and
            # multi-worker)
            assert data_writer is not None  # set when input_schema is not None
            yield from self._scalar_function_parallel(
                input_batch=input_batch,
                input_iterator=input,
                data_writer=data_writer,
            )
            return

    def _scalar_function_parallel(
        self,
        *,
        input_batch: pa.RecordBatch,
        input_iterator: Iterator[pa.RecordBatch],
        data_writer: ipc.RecordBatchStreamWriter,
    ) -> Generator[pa.RecordBatch, None, None]:
        """Process scalar function batches across one or more workers using threads.

        Handles both single-worker and multi-worker cases uniformly.

        Processing flow:
        1. Creates worker connection objects for primary + additional workers
        2. Starts one thread per worker running _worker_thread_loop
        3. Distributes input batches round-robin to worker input queues
        4. Signals end-of-input to all workers via None sentinel
        5. Collects all output batches from shared output queue
        6. Waits for worker threads to complete
        7. Closes all workers

        Args:
            input_batch: The first input batch, already consumed from the
                iterator by scalar_function().
            input_iterator: Iterator for remaining input batches. May be empty
                if all input was in the first batch.
            data_writer: IPC stream writer for the primary worker, already
                initialized by _initialize_function_stream().

        Yields:
            Output RecordBatches from processing, in non-deterministic order for
            multi-worker mode.

        Raises:
            ClientError: If a worker thread fails with an exception.

        """
        primary_worker = self._create_primary_worker(data_writer=data_writer)
        all_workers = [primary_worker] + self._additional_workers
        num_workers = len(all_workers)

        log.debug("starting_scalar_parallel_processing", num_workers=num_workers)

        # Create queues for each worker
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
        for q in input_queues:
            q.put(None)

        log.debug("scalar_all_batches_distributed", total_batches=batches_sent)

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
            combined = self._combine_batches(output_batches)
            if combined is not None:
                yield combined

            log.debug(
                "scalar_output_received",
                batch_index=batch_idx,
                outputs_received=outputs_received,
                outputs_expected=outputs_expected,
            )

        self._join_threads(threads)
        log.debug("all_scalar_worker_threads_complete")

        # Close data writers to signal EOF to workers
        for worker in all_workers:
            if worker.data_writer is not None:
                worker.data_writer.close()

        # Wait for secondary workers to exit
        secondary_workers = all_workers[1:]
        for worker in secondary_workers:
            worker.proc.wait(timeout=self.PROCESS_WAIT_TIMEOUT)
            log.debug(
                "scalar_secondary_worker_exited",
                worker_index=worker.worker_index,
                returncode=worker.proc.returncode,
            )

        log.debug("scalar_parallel_processing_complete")
