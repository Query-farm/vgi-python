"""VGI Worker base class for hosting user-defined functions.

A worker is a subprocess that communicates via stdin/stdout using Arrow IPC.

The worker does not support running multiple function calls in the same process,
it is intended to be launched per-function-call by the Client.

Protocol:
1. Read init batch: {function_name, arguments, in_type struct}
2. Write output schema (serialized Arrow schema bytes)
3. Read data batches, process them through the function, write results back

Usage:
    from vgi.worker import Worker
    from vgi.table_in_out_function import Function

    class MyFunction(Function):
        ...

    class MyWorker(Worker):
        registry = {
            "my_function": MyFunction,
        }

    if __name__ == "__main__":
        MyWorker().run()
"""

import os
import sys
from dataclasses import dataclass

import pyarrow as pa
import structlog
from pyarrow import ipc

from vgi.function import FunctionOutputSpec, FunctionRequest
from vgi.table_in_out_function import (
    Function,
    ProtocolInput,
)

# Type alias for the function registry mapping names to Function classes
FunctionRegistry = dict[str, type[Function]]


@dataclass(frozen=True, slots=True)
class WorkerStats:
    """Statistics about a worker's processing run.

    Attributes:
        batch_count: Number of data batches processed.
        total_input_rows: Total number of input rows processed.
        total_output_rows: Total number of output rows produced.

    """

    batch_count: int
    total_input_rows: int
    total_output_rows: int


class Worker:
    """Base class for VGI workers that host user-defined functions.

    Subclass this and define a `registry` class attribute mapping function names
    to Function subclasses. The worker handles the VGI protocol:
    reading FunctionRequest, instantiating functions, and streaming batches.

    Example:
        class MyWorker(Worker):
            registry = {
                "echo": EchoFunction,
                "transform": TransformFunction,
            }

        if __name__ == "__main__":
            MyWorker().run()

    """

    registry: FunctionRegistry = {}

    def __init__(self) -> None:
        """Initialize the worker with structured logging."""
        structlog.configure(
            processors=[
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.dev.ConsoleRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(0),
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        )
        self.log = structlog.get_logger().bind(component="worker")

    def _read_invocation(self) -> FunctionRequest:
        """Read and parse the call data from stdin.

        Returns:
            FunctionRequest

        """
        self.log.debug("invocation_reading")

        # Read init messages manually (not via ipc.open_stream) to avoid PyArrow
        # closing the underlying pipe when the stream context exits.
        msg = ipc.read_message(sys.stdin)
        if msg.type != "schema":
            raise ValueError(f"Expected schema message, got {msg.type}")
        init_schema = ipc.read_schema(msg)

        msg = ipc.read_message(sys.stdin)
        if msg.type != "record batch":
            raise ValueError(f"Expected record batch, got {msg.type}")
        init_batch = ipc.read_record_batch(msg, init_schema)

        return FunctionRequest.deserialize(init_batch)

    def _read_init_data(self) -> pa.RecordBatch:
        """Read and parse the init data from stdin.

        Returns:
            pa.RecordBatch

        """
        self.log.debug("init_data_reading")

        # Read init messages manually (not via ipc.open_stream) to avoid PyArrow
        # closing the underlying pipe when the stream context exits.
        msg = ipc.read_message(sys.stdin)
        if msg.type != "schema":
            raise ValueError(f"Expected schema message, got {msg.type}")
        init_schema = ipc.read_schema(msg)

        msg = ipc.read_message(sys.stdin)
        if msg.type != "record batch":
            raise ValueError(f"Expected record batch, got {msg.type}")
        init_batch = ipc.read_record_batch(msg, init_schema)

        return init_batch

    def _process_batches(
        self,
        instance: Function,
        invocation: FunctionRequest,
        fn_log: structlog.stdlib.BoundLogger,
    ) -> WorkerStats:
        """Process data batches through the function.

        Reads input batches from stdin, sends them through the function's
        generator, and writes output batches to stdout. Handles the
        HAVE_MORE_OUTPUT and FINALIZE protocol states.

        Returns:
            WorkerStats with batch_count, total_input_rows, total_output_rows.

        """
        assert invocation.global_init_identifier is not None
        generator = instance.run()
        next(generator)  # Prime the run() generator

        with (
            ipc.new_stream(sys.stdout, instance.output_schema) as writer,
            ipc.open_stream(sys.stdin) as data_reader,
        ):
            # Validate data stream schema matches expected input schema
            if data_reader.schema != invocation.in_out_function_input_schema:
                expected = invocation.in_out_function_input_schema
                raise ValueError(
                    f"Data stream schema mismatch. Expected: {expected}, "
                    f"got: {data_reader.schema}"
                )

            batch_count = 0
            total_input_rows = 0
            total_output_rows = 0
            while True:
                fn_log.debug("batch_waiting")

                # The client drives the protocol: it handles HAVE_MORE_OUTPUT by
                # re-sending batches, and sends FINALIZE when done.
                try:
                    batch, metadata = data_reader.read_next_batch_with_custom_metadata()
                except StopIteration:
                    fn_log.debug("input_stream_ended")
                    break

                batch_count += 1
                total_input_rows += batch.num_rows
                fn_log.debug(
                    "batch_received",
                    batch_index=batch_count,
                    input_rows=batch.num_rows,
                )

                output = generator.send(ProtocolInput(batch=batch, metadata=metadata))
                fn_log.debug("batch_processed", output=output)
                output_rows = output.batch.num_rows if output.batch else 0
                total_output_rows += output_rows
                writer.write_batch(
                    output.batch, custom_metadata=output.metadata(invocation)
                )
                fn_log.debug(
                    "batch_written",
                    batch_index=batch_count,
                    output_rows=output_rows,
                    status=output.status.value if output.status else None,
                )
        return WorkerStats(
            batch_count=batch_count,
            total_input_rows=total_input_rows,
            total_output_rows=total_output_rows,
        )

    def run(self) -> None:
        """Run the worker, reading from stdin and writing to stdout."""
        self.log.info("worker_starting")
        sys.stdin = os.fdopen(0, "rb")
        sys.stdout = os.fdopen(1, "wb", buffering=0)

        invocation = self._read_invocation()

        fn_log = self.log.bind(function=invocation.function_name)
        fn_log.info("init_received", arguments=invocation.arguments)
        fn_log.debug(
            "input_schema_parsed", schema=str(invocation.in_out_function_input_schema)
        )

        if invocation.function_name not in self.registry:
            raise ValueError(f"Unknown function: {invocation.function_name}")

        instance = self.registry[invocation.function_name](invocation, fn_log)

        bind_result_bytes = FunctionOutputSpec(
            output_schema=instance.output_schema,
            max_processes=instance.max_processes(),
            invocation_id=instance.invocation_id(),
        ).serialize()

        if sys.stdout.write(bind_result_bytes) != len(bind_result_bytes):
            raise OSError("Failed to write bind result record batch")

        if invocation.global_init_identifier is None:
            fn_log.info("processing_init")
            init_result = instance.perform_init(self._read_init_data())
            init_result_bytes = init_result.serialize()
            if sys.stdout.write(init_result_bytes) != len(init_result_bytes):
                raise OSError("Failed to write init result record batch")
            fn_log.info("processing_init_complete", init_result=init_result)
            invocation = invocation.with_global_init_identifier(init_result)
        else:
            fn_log.info("retrieving_init")
            instance.retrieve_init(invocation.global_init_identifier)

        stats = self._process_batches(instance, invocation, fn_log)

        fn_log.info(
            "worker_complete",
            stats=stats,
        )
