"""
VGI Worker base class for hosting user-defined functions.

A worker is a subprocess that communicates via stdin/stdout using Arrow IPC.

The worker does not support running multiple function calls in the same process,
it is intended to be launched per-function-call by the Client.

Protocol:
1. Read init batch: {function_name, arguments, in_type struct}
2. Write output schema (serialized Arrow schema bytes)
3. Read data batches, process them through the function, write results back

Usage:
    from vgi.worker import Worker
    from vgi.table_in_out_function import TableInOutFunction, table_in_out_function

    @table_in_out_function
    class MyFunction(TableInOutFunction):
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

import pyarrow as pa
import structlog
from pyarrow import ipc

from vgi.function import CallData
from vgi.table_in_out_function import (
    BindResult,
    FunctionInput,
    TableInOutFunctionCallable,
)

# Type for decorated table functions
FunctionRegistry = dict[str, TableInOutFunctionCallable]


class Worker:
    """Base class for workers.

    Subclass this and define a `registry` class attribute mapping function names
    to decorated TableInOutFunction classes.

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

    def _read_call_data(self) -> CallData:
        """Read and parse the call data from stdin.

        Returns:
            CallData
        """
        self.log.debug("call_data_reading")

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

        return CallData.deserialize(init_batch)

    def _process_batches(
        self,
        bind_result: BindResult,
        in_schema: pa.Schema,
        fn_log: structlog.stdlib.BoundLogger,
    ) -> tuple[int, int, int]:
        """Process data batches through the function.

        Returns:
            Tuple of (batch_count, total_input_rows, total_output_rows)
        """
        with (
            ipc.new_stream(sys.stdout, bind_result.output_schema) as writer,
            ipc.open_stream(sys.stdin) as data_reader,
        ):
            # Validate data stream schema matches expected input schema
            if data_reader.schema != in_schema:
                raise ValueError(
                    f"Data stream schema mismatch. Expected: {in_schema}, "
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

                output = bind_result.generator.send(
                    FunctionInput(batch=batch, metadata=metadata)
                )
                output_rows = output.batch.num_rows if output.batch else 0
                total_output_rows += output_rows
                writer.write_batch(output.batch, custom_metadata=output.metadata())
                fn_log.debug(
                    "batch_written",
                    batch_index=batch_count,
                    output_rows=output_rows,
                    status=output.status.value if output.status else None,
                )

        return batch_count, total_input_rows, total_output_rows

    def run(self) -> None:
        """Run the worker, reading from stdin and writing to stdout."""
        self.log.info("worker_starting")
        sys.stdin = os.fdopen(0, "rb")
        sys.stdout = os.fdopen(1, "wb", buffering=0)

        call_data = self._read_call_data()

        fn_log = self.log.bind(function=call_data.function_name)
        fn_log.info("init_received", arguments=call_data.arguments)
        fn_log.debug("input_schema_parsed", schema=str(call_data.in_schema))

        if call_data.function_name not in self.registry:
            raise ValueError(f"Unknown function: {call_data.function_name}")

        bind_result = self.registry[call_data.function_name](call_data)
        next(bind_result.generator)

        bind_result_bytes = bind_result.serialize()
        if sys.stdout.write(bind_result_bytes) != len(bind_result_bytes):
            raise OSError("Failed to write bind result record batch")

        batch_count, total_input_rows, total_output_rows = self._process_batches(
            bind_result, call_data.in_schema, fn_log
        )

        fn_log.info(
            "worker_complete",
            batches_processed=batch_count,
            total_input_rows=total_input_rows,
            total_output_rows=total_output_rows,
        )
