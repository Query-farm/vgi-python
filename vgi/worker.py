"""VGI Worker base class for hosting user-defined functions.

A worker is a subprocess that communicates via stdin/stdout using Arrow IPC.
Workers are spawned by Client for each function invocation.

SUPPORTED FUNCTION TYPES
------------------------
The worker supports two function types, dispatched based on class inheritance:

1. TableInOutGeneratorFunction: Reads input batches, produces output batches.
   Use for transforming, filtering, or aggregating input data.

2. TableFunctionGenerator: Generates output batches without reading input.
   Use for data generation functions like sequence(), range(), random_sample().

QUICK START
-----------
Create a worker by subclassing Worker and listing your functions:

    from vgi.worker import Worker
    from vgi.table_in_out_function import TableInOutGeneratorFunction
    from vgi.table_function import TableFunctionGenerator

    class EchoFunction(TableInOutGeneratorFunction):
        # Transforms input batches
        ...

    class SequenceFunction(TableFunctionGenerator):
        # Generates output without input
        ...

    class MyWorker(Worker):
        functions = [EchoFunction, SequenceFunction]

    if __name__ == "__main__":
        MyWorker().run()

Function names are derived from metadata (Meta.name or class name converted to
snake_case). No manual name mapping required.

PROTOCOL FLOW (TableInOutGeneratorFunction)
-------------------------------------------
1. Read Invocation: function name, arguments, input schema
2. Write OutputSpec: output schema, max_processes, invocation_id
3. Read/write GlobalStateInitInput/GlobalInitResult for initialization
4. Stream: read input batches -> process -> write output batches
5. Finalize: receive FINALIZE signal -> emit final results

PROTOCOL FLOW (TableFunctionGenerator)
--------------------------------------
1. Read Invocation: function name, arguments (no input schema)
2. Write OutputSpec: output schema, max_processes, invocation_id
3. Read/write GlobalStateInitInput/GlobalInitResult for initialization
4. Generate: produce output batches until generator exhausted

KEY CLASSES
-----------
    Worker      - Base class to subclass (set functions attribute)
    WorkerStats - Statistics about processing (batch_count, rows)

See Also
--------
vgi.client.Client : Spawns workers and sends data to them
vgi.function.Function : Base class for all functions
vgi.examples.worker : Example worker with built-in functions

"""

import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from io import IOBase
from typing import cast

import pyarrow as pa
import structlog
import structlog.stdlib
from pyarrow import ipc

from vgi.function import (
    Function,
    Invocation,
    OutputSpec,
)
from vgi.ipc_utils import read_ipc_batch
from vgi.scalar_function import ScalarFunctionGenerator
from vgi.table_function import TableFunctionGenerator
from vgi.table_in_out_function import (
    ProtocolInput,
    TableInOutGeneratorFunction,
    _OutputStatus,
)


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

    Subclass this and define a `functions` class attribute listing your function
    classes. Function names are derived from metadata (Meta.name or snake_case
    of class name). The worker handles the VGI protocol: reading Invocation,
    instantiating functions, and streaming batches.

    Multiple functions can share the same name if they have different argument
    signatures (function overloading). The worker will select the appropriate
    function based on the invocation's arguments.

    Example:
        class MyWorker(Worker):
            functions = [EchoFunction, TransformFunction]

        if __name__ == "__main__":
            MyWorker().run()

    """

    functions: Sequence[type[Function]] = []
    _registry: dict[str, list[type[Function]]] | None = None

    @classmethod
    def _build_registry(cls) -> dict[str, list[type[Function]]]:
        """Build function name -> list of classes mapping from functions list.

        Multiple functions can share the same name if they have different
        argument signatures (overloading).
        """
        if cls._registry is not None:
            return cls._registry

        registry: dict[str, list[type[Function]]] = {}
        for func_cls in cls.functions:
            meta = func_cls.get_metadata()
            if meta.name not in registry:
                registry[meta.name] = []
            registry[meta.name].append(func_cls)

        cls._registry = registry
        return registry

    @staticmethod
    def _match_function(
        invocation: Invocation,
        candidates: Sequence[type[Function]],
    ) -> type[Function]:
        """Find the function that matches the invocation's arguments.

        Compares the invocation's positional and named arguments against each
        candidate function's parameter metadata to find a match.

        Args:
            invocation: The invocation with arguments to match.
            candidates: Sequence of function classes with the same name.

        Returns:
            The matching function class.

        Raises:
            ValueError: If no function matches or multiple functions match.

        """
        args = invocation.arguments
        num_positional = len(args.positional)
        named_keys = set(args.named.keys()) if args.named else set()

        matches: list[type[Function]] = []

        for func_cls in candidates:
            meta = func_cls.get_metadata()

            # Split parameters into positional and named (excluding TableInput)
            positional_params = [
                p
                for p in meta.parameters
                if isinstance(p.position, int) and not p.is_table_input
            ]
            named_params = [p for p in meta.parameters if isinstance(p.position, str)]

            # Check positional arguments
            required_positional = [p for p in positional_params if p.required]
            max_positional = len(positional_params)
            min_positional = len(required_positional)

            if not (min_positional <= num_positional <= max_positional):
                continue  # Wrong number of positional arguments

            # Check named arguments
            valid_named_keys = {p.position for p in named_params}
            required_named_keys = {p.position for p in named_params if p.required}

            # All provided named args must be valid
            if not named_keys.issubset(valid_named_keys):
                continue  # Unknown named argument

            # All required named args must be provided
            if not required_named_keys.issubset(named_keys):
                continue  # Missing required named argument

            matches.append(func_cls)

        if len(matches) == 0:
            # Build helpful error message
            param_summaries = []
            for func_cls in candidates:
                meta = func_cls.get_metadata()
                params = [p for p in meta.parameters if not p.is_table_input]
                param_str = ", ".join(
                    f"{p.name}: {p.type_name or '?'}"
                    + ("" if p.required else f" = {p.default}")
                    for p in params
                )
                param_summaries.append(f"  {func_cls.__name__}({param_str})")

            raise ValueError(
                f"No matching function '{invocation.function_name}' for arguments: "
                f"{num_positional} positional, named={sorted(named_keys)}. "
                f"Available overloads:\n" + "\n".join(param_summaries)
            )

        if len(matches) > 1:
            match_names = [m.__name__ for m in matches]
            raise ValueError(
                f"Ambiguous function call '{invocation.function_name}': "
                f"multiple overloads match: {match_names}"
            )

        return matches[0]

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
        self.log: structlog.stdlib.BoundLogger = structlog.get_logger().bind(
            component="worker"
        )

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

    def _process_scalar_batches(
        self,
        instance: ScalarFunctionGenerator,
        invocation: Invocation,
        fn_log: structlog.stdlib.BoundLogger,
    ) -> WorkerStats:
        """Process data batches through a scalar function.

        Similar to _process_batches but simplified:
        - No FINALIZE phase (ends when input exhausted)
        - HAVE_MORE_OUTPUT only used for log messages (not multiple output batches)

        Returns:
            WorkerStats with batch_count, total_input_rows, total_output_rows.

        """
        if invocation.global_init_identifier is None:
            raise ValueError(
                "global_init_identifier is required but was None. "
                "This is an internal protocol error - the worker should have set "
                "global_init_identifier after perform_init() completed successfully."
            )
        generator = instance.run()
        next(generator)  # Prime the run() generator

        with (
            ipc.new_stream(cast(IOBase, sys.stdout), instance.output_schema) as writer,
            ipc.open_stream(cast(IOBase, sys.stdin)) as data_reader,
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

                try:
                    batch, metadata = data_reader.read_next_batch_with_custom_metadata()
                except StopIteration:
                    fn_log.debug("input_stream_ended")
                    # Close the generator - no FINALIZE for scalar functions
                    generator.close()
                    break

                batch_count += 1
                total_input_rows += batch.num_rows
                fn_log.debug(
                    "batch_received",
                    batch_index=batch_count,
                    input_rows=batch.num_rows,
                )

                protocol_input = ProtocolInput(batch=batch, metadata=metadata)
                output = generator.send(protocol_input)

                # Handle log messages (HAVE_MORE_OUTPUT)
                while output.status == _OutputStatus.HAVE_MORE_OUTPUT:
                    fn_log.debug("log_message_received", output=output)
                    assert output.batch is not None
                    writer.write_batch(
                        output.batch, custom_metadata=output.metadata(invocation)
                    )
                    # Re-send same input to continue
                    output = generator.send(protocol_input)

                fn_log.debug("batch_processed", output=output)
                assert output.batch is not None
                output_rows = output.batch.num_rows
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

    def _process_batches(
        self,
        instance: TableInOutGeneratorFunction,
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
            raise ValueError(
                "global_init_identifier is required but was None. "
                "This is an internal protocol error - the worker should have set "
                "global_init_identifier after perform_init() completed successfully."
            )
        generator = instance.run()
        next(generator)  # Prime the run() generator

        with (
            ipc.new_stream(cast(IOBase, sys.stdout), instance.output_schema) as writer,
            ipc.open_stream(cast(IOBase, sys.stdin)) as data_reader,
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
                # After initial priming, batch is always set by the protocol
                assert output.batch is not None
                output_rows = output.batch.num_rows
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

    def _generate_batches(
        self,
        instance: TableFunctionGenerator,
        invocation: Invocation,
        fn_log: structlog.stdlib.BoundLogger,
    ) -> WorkerStats:
        """Generate output batches from a TableFunctionGenerator.

        Unlike _process_batches, this method doesn't read input batches.
        The function generates output independently.

        Returns:
            WorkerStats with batch_count=0, total_input_rows=0, total_output_rows.

        """
        generator = instance.run()

        with ipc.new_stream(cast(IOBase, sys.stdout), instance.output_schema) as writer:
            batch_count = 0
            total_output_rows = 0

            for output in generator:
                batch_count += 1
                # Table function generator always produces a batch
                assert output.batch is not None
                output_rows = output.batch.num_rows
                total_output_rows += output_rows

                writer.write_batch(
                    output.batch, custom_metadata=output.metadata(invocation)
                )
                fn_log.debug(
                    "batch_written",
                    batch_index=batch_count,
                    output_rows=output_rows,
                )

        return WorkerStats(
            batch_count=batch_count,
            total_input_rows=0,
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

        registry = self._build_registry()
        if invocation.function_name not in registry:
            available = sorted(registry.keys())
            raise ValueError(
                f"Unknown function: {invocation.function_name}. Available: {available}"
            )

        candidates = registry[invocation.function_name]
        func_cls = self._match_function(invocation, candidates)

        # Instantiate the function
        instance = func_cls(invocation=invocation, logger=fn_log)

        # Determine active features from client features
        # Worker can define supported features; active = intersection
        worker_features: frozenset[str] = frozenset()  # No features supported yet
        active_features = invocation.client_features & worker_features
        fn_log.debug(
            "features_negotiated",
            client_features=invocation.client_features,
            active_features=active_features,
        )

        bind_result_bytes = OutputSpec(
            output_schema=instance.output_schema,
            max_processes=instance.max_processes(),
            invocation_id=instance.create_invocation_id(),
            active_features=active_features,
        ).serialize()

        if sys.stdout.write(bind_result_bytes) != len(bind_result_bytes):
            raise OSError("Failed to write bind result record batch")

        if invocation.global_init_identifier is None:
            # Primary worker: perform init and store in storage
            fn_log.info("processing_init")
            init_result = instance.perform_init(self._read_init_data())
            init_result_bytes = init_result.serialize()
            if sys.stdout.write(init_result_bytes) != len(init_result_bytes):
                raise OSError("Failed to write init result record batch")
            fn_log.info("processing_init_complete", init_result=init_result)
            invocation = invocation.with_global_init_identifier(init_result)
        else:
            # Secondary worker: retrieve shared init from storage
            fn_log.info("retrieving_init")
            instance.retrieve_init(invocation.global_init_identifier)

        # Dispatch to appropriate processing method based on function type.
        # ScalarFunctionGenerator processes input batches to single-column output.
        # TableInOutGeneratorFunction reads input batches and produces output.
        # TableFunctionGenerator generates output without input batches.
        # Note: Check ScalarFunctionGenerator first since it doesn't inherit from
        # TableInOutGeneratorFunction, then TableInOutGeneratorFunction.
        if isinstance(instance, ScalarFunctionGenerator):
            stats = self._process_scalar_batches(instance, invocation, fn_log)
        elif isinstance(instance, TableInOutGeneratorFunction):
            stats = self._process_batches(instance, invocation, fn_log)
        elif isinstance(instance, TableFunctionGenerator):
            stats = self._generate_batches(instance, invocation, fn_log)
        else:
            raise TypeError(
                f"Unsupported function type: {type(instance).__name__}. "
                f"Functions must inherit from ScalarFunctionGenerator (for "
                f"scalar functions), TableInOutGeneratorFunction (for functions "
                f"that process input batches), or TableFunctionGenerator (for "
                f"functions that generate output without input). "
                f"See vgi.scalar_function, vgi.table_in_out_function, and "
                f"vgi.table_function modules."
            )

        fn_log.info(
            "worker_complete",
            stats=stats,
        )
