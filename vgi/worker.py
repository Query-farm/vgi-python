"""
VGI Worker base class for hosting user-defined functions.

A worker is a subprocess that communicates via stdin/stdout using Arrow IPC.

Protocol:
1. Read init batch: {function_name, arguments, in_type struct}
2. Write output schema (serialized Arrow schema bytes)
3. Read data batches, process them through the function, write results back

Usage:
    from vgi.worker import VGIWorker
    from vgi.table_in_out_function import TableInOutFunction, table_in_out_function

    @table_in_out_function
    class MyFunction(TableInOutFunction):
        ...

    class MyWorker(VGIWorker):
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

from vgi.table_in_out_function import FunctionInput, TableFunction

# Type for decorated table functions
FunctionRegistry = dict[str, TableFunction]


class VGIWorker:
    """Base class for VGI workers.

    Subclass this and define a `registry` class attribute mapping function names
    to decorated TableInOutFunction classes.

    Example:
        class MyWorker(VGIWorker):
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

    def run(self) -> None:
        """Run the worker, reading from stdin and writing to stdout."""
        self.log.info("worker_starting")
        sys.stdin = os.fdopen(0, "rb")
        sys.stdout = os.fdopen(1, "wb", buffering=0)

        self.log.debug("init_stream_reading")

        # Read init messages manually (not via ipc.open_stream) to avoid PyArrow
        # closing the underlying pipe when the stream context exits.
        msg = ipc.read_message(sys.stdin)
        assert msg.type == "schema", f"Expected schema message, got {msg.type}"
        init_schema = ipc.read_schema(msg)

        msg = ipc.read_message(sys.stdin)
        assert msg.type == "record batch", f"Expected record batch, got {msg.type}"
        init_batch = ipc.read_record_batch(msg, init_schema)

        for field in ("function_name", "arguments", "in_type"):
            if field not in init_batch.schema.names:
                raise ValueError(f"Init batch missing required field: {field}")

        # Extract function_name and arguments
        function_name = init_batch.column("function_name")[0].as_py()
        arguments = init_batch.column("arguments")[0].as_py()
        fn_log = self.log.bind(function=function_name)
        fn_log.info("init_received", arguments=arguments)

        # Extract the input schema from in_type struct field
        in_type_field = init_schema.field("in_type")
        if not pa.types.is_struct(in_type_field.type):
            raise TypeError("in_type must be a struct")

        in_schema = pa.schema(
            [pa.field(f.name, f.type, f.nullable) for f in in_type_field.type]
        )
        fn_log.debug("input_schema_parsed", schema=str(in_schema))

        if function_name not in self.registry:
            raise ValueError(f"Unknown function: {function_name}")

        function_cls = self.registry[function_name]
        output_schema, fn_gen = function_cls(arguments, in_schema)
        next(fn_gen)

        # Send output schema to client (as raw bytes, not wrapped in an IPC stream)
        serialized_schema = output_schema.serialize().to_pybytes()

        if sys.stdout.write(serialized_schema) != len(serialized_schema):
            raise OSError("Failed to write complete output schema")

        with (
            ipc.new_stream(sys.stdout, output_schema) as writer,
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

                output = fn_gen.send(FunctionInput(batch=batch, metadata=metadata))
                output_rows = output.batch.num_rows if output.batch else 0
                total_output_rows += output_rows
                writer.write_batch(
                    output.batch,
                    custom_metadata={
                        "status": output.status.value if output.status else None
                    },
                )
                fn_log.debug(
                    "batch_written",
                    batch_index=batch_count,
                    output_rows=output_rows,
                    status=output.status.value if output.status else None,
                )

        fn_log.info(
            "worker_complete",
            batches_processed=batch_count,
            total_input_rows=total_input_rows,
            total_output_rows=total_output_rows,
        )
