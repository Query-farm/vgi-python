"""VGI Worker base class for hosting user-defined functions.

A worker is a subprocess that communicates via stdin/stdout using Arrow IPC.
Workers are spawned by Client for each function invocation.

QUICK START
-----------
Create a worker by subclassing Worker and defining a registry:

    from vgi.worker import Worker
    from vgi.table_in_out_function import TableInOutFunction

    class MyFunction(TableInOutFunction):
        def process(self, batch):
            _ = yield None
            while True:
                yield Output(batch)
                batch = yield None
                if batch is None:
                    break

    class MyWorker(Worker):
        registry = {
            "my_function": MyFunction,
        }

    if __name__ == "__main__":
        MyWorker().run()

PROTOCOL FLOW
-------------
1. Read Invocation: function name, arguments, input schema
2. Write OutputSpec: output schema, max_processes, invocation_id
3. Read/write GlobalStateInitInput/GlobalInitResult for initialization
4. Stream: read input batches -> process -> write output batches
5. Finalize: receive FINALIZE signal -> emit final results

KEY CLASSES
-----------
    Worker          - Base class to subclass (set registry attribute)
    FunctionRegistry - Type alias: dict[str, type[TableInOutFunction]]
    WorkerStats     - Statistics about processing (batch_count, rows)

See Also
--------
vgi.client.Client : Spawns workers and sends data to them
TableInOutFunction : Base class for functions hosted by workers
vgi.examples.worker : Example worker with built-in functions

"""

import os
import sys
from dataclasses import dataclass

import pyarrow as pa
import structlog
from pyarrow import ipc

from vgi.function import (
    Invocation,
    OutputSpec,
    negotiate_protocol_version,
)
from vgi.ipc_utils import read_ipc_batch
from vgi.table_in_out_function import (
    ProtocolInput,
    TableInOutFunction,
)

# Type alias for the function registry mapping names to Function classes
FunctionRegistry = dict[str, type[TableInOutFunction]]


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
    to TableInOutFunction subclasses. The worker handles the VGI protocol:
    reading Invocation, instantiating functions, and streaming batches.

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

    def _read_ipc_batch(self, context: str) -> pa.RecordBatch:
        """Read a schema + record batch pair from stdin.

        Args:
            context: Description for debug logging (e.g., "invocation", "init_data").

        Returns:
            The deserialized RecordBatch.

        Raises:
            IPCError: If unexpected message types are received.

        """
        self.log.debug(f"{context}_reading")
        return read_ipc_batch(sys.stdin, context)

    def _read_invocation(self) -> Invocation:
        """Read and parse the call data from stdin."""
        return Invocation.deserialize(self._read_ipc_batch("invocation"))

    def _read_init_data(self) -> pa.RecordBatch:
        """Read and parse the init data from stdin."""
        return self._read_ipc_batch("init_data")

    def _process_batches(
        self,
        instance: TableInOutFunction,
        invocation: Invocation,
        fn_log: structlog.stdlib.BoundLogger,
    ) -> WorkerStats:
        """Process data batches through the function.

        Reads input batches from stdin, sends them through the function's
        generator, and writes output batches to stdout. Handles the
        HAVE_MORE_OUTPUT and FINALIZE protocol states.

        Returns:
            WorkerStats with batch_count, total_input_rows, total_output_rows.

        """
        if invocation.global_init_identifier is None:
            raise ValueError("global_init_identifier is required but was None")
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
                    # Close the generator to signal that no more input will arrive.
                    # This allows functions to perform cleanup (e.g., saving state)
                    # by catching GeneratorExit.
                    generator.close()
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

        # Negotiate protocol version with client
        negotiated_version = negotiate_protocol_version(invocation.protocol_version)
        fn_log.debug(
            "protocol_version_negotiated",
            client_version=invocation.protocol_version,
            negotiated_version=negotiated_version,
        )

        bind_result_bytes = OutputSpec(
            output_schema=instance.output_schema,
            max_processes=instance.max_processes(),
            invocation_id=instance.create_invocation_id(),
            protocol_version=negotiated_version,
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
