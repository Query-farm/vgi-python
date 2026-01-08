"""VGI Worker base class for hosting user-defined functions and catalogs.

A worker is a subprocess that communicates via stdin/stdout using Arrow IPC.
Workers are spawned by Client for each function invocation.

SUPPORTED FUNCTION TYPES
------------------------
The worker supports three function types, dispatched based on class inheritance:

1. ScalarFunctionGenerator: Transforms input batches to single-column output
   with 1:1 row mapping. Use for per-row computations like add(), upper(), etc.

2. TableInOutGenerator: Reads input batches, produces output batches.
   Use for transforming, filtering, or aggregating input data.

3. TableFunctionGenerator: Generates output batches without reading input.
   Use for data generation functions like sequence(), range(), random_sample().

QUICK START
-----------
Create a worker by subclassing Worker and listing your functions:

    from vgi.worker import Worker
    from vgi.scalar_function import ScalarFunction
    from vgi.table_in_out_function import TableInOutGenerator
    from vgi.table_function import TableFunctionGenerator

    class DoubleColumn(ScalarFunction):
        # Single-column output with 1:1 row mapping
        ...

    class EchoFunction(TableInOutGenerator):
        # Transforms input batches
        ...

    class SequenceFunction(TableFunctionGenerator):
        # Generates output without input
        ...

    class MyWorker(Worker):
        functions = [DoubleColumn, EchoFunction, SequenceFunction]

    if __name__ == "__main__":
        MyWorker().run()

Function names are derived from metadata (Meta.name or class name converted to
snake_case). No manual name mapping required.

PROTOCOL FLOW (ScalarFunctionGenerator)
---------------------------------------
1. Read Invocation: function name, arguments, input schema
2. Write OutputSpec: output schema, max_processes, invocation_id
3. Read/write FunctionInitInput/InitResult for initialization
4. Stream: read input batches -> compute -> write single-column output batches
   (ends when input exhausted, no FINALIZE phase)

PROTOCOL FLOW (TableInOutGenerator)
-------------------------------------------
1. Read Invocation: function name, arguments, input schema
2. Write OutputSpec: output schema, max_processes, invocation_id
3. Read/write TableFunctionInitInput/InitResult for initialization
4. Stream: read input batches -> process -> write output batches
5. Finalize: receive FINALIZE signal -> emit final results

PROTOCOL FLOW (TableFunctionGenerator)
--------------------------------------
1. Read Invocation: function name, arguments (no input schema)
2. Write OutputSpec: output schema, max_processes, invocation_id
3. Read/write TableFunctionInitInput/InitResult for initialization
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
from typing import IO, Any, cast

import pyarrow as pa
import structlog
import structlog.stdlib
from pyarrow import ipc

from vgi.catalog import CatalogInterface
from vgi.exceptions import SchemaValidationError
from vgi.function import (
    Function,
    OutputSpec,
)
from vgi.invocation import Invocation, InvocationType
from vgi.ipc_utils import read_single_record_batch
from vgi.scalar_function import ProtocolInput as ScalarProtocolInput
from vgi.scalar_function import ScalarFunctionGenerator
from vgi.table_function import TableFunctionGenerator
from vgi.table_in_out_function import (
    ProtocolInput,
    TableInOutGenerator,
)

# Schema for bind-time error batches (zero rows with error metadata)
_BIND_ERROR_SCHEMA = pa.schema([("_error", pa.null())])


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

    Catalog Interface:
        If `catalog_interface` is not set but `functions` is non-empty, a default
        read-only catalog interface is created automatically. This exposes the
        worker's functions via the catalog protocol, allowing clients to discover
        available functions.

        To customize the catalog, set `catalog_interface` to a CatalogInterface
        subclass. To disable the catalog entirely, set `catalog_interface = None`
        and `catalog_name = None`.

    Example:
        class MyWorker(Worker):
            functions = [EchoFunction, TransformFunction]

        if __name__ == "__main__":
            MyWorker().run()

    """

    functions: Sequence[type[Function[Any]]] = []
    catalog_interface: type[CatalogInterface] | None = None
    catalog_name: str | None = "functions"  # Set to None to disable default catalog
    _registry: dict[str, list[type[Function[Any]]]] | None = None
    _default_catalog_interface: type[CatalogInterface] | None = None

    @classmethod
    def _build_registry(cls) -> dict[str, list[type[Function[Any]]]]:
        """Build function name -> list of classes mapping from functions list.

        Multiple functions can share the same name if they have different
        argument signatures (overloading).
        """
        if cls._registry is not None:
            return cls._registry

        registry: dict[str, list[type[Function[Any]]]] = {}
        for func_cls in cls.functions:
            meta = func_cls.get_metadata()
            if meta.name not in registry:
                registry[meta.name] = []
            registry[meta.name].append(func_cls)

        cls._registry = registry
        return registry

    @classmethod
    def _get_catalog_interface(cls) -> type[CatalogInterface] | None:
        """Get the catalog interface to use for this worker.

        Returns the explicitly set catalog_interface if present. Otherwise,
        if functions are defined and catalog_name is set, creates a default
        ReadOnlyCatalogInterface that exposes the worker's functions.

        Returns:
            CatalogInterface class to instantiate, or None if no catalog.

        """
        # Use explicit catalog_interface if set
        if cls.catalog_interface is not None:
            return cls.catalog_interface

        # No default catalog if catalog_name is None or no functions
        if cls.catalog_name is None or not cls.functions:
            return None

        # Create default catalog interface if not already created
        if cls._default_catalog_interface is None:
            from vgi.catalog import ReadOnlyCatalogInterface

            # Create a dynamic subclass with the worker's functions
            cls._default_catalog_interface = cast(
                type[CatalogInterface],
                type(
                    f"{cls.__name__}Catalog",
                    (ReadOnlyCatalogInterface,),
                    {
                        "catalog_name": cls.catalog_name,
                        "functions": list(cls.functions),
                    },
                ),
            )

        return cls._default_catalog_interface

    @staticmethod
    def _match_function(
        invocation: Invocation,
        candidates: Sequence[type[Function[Any]]],
    ) -> type[Function[Any]]:
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

        matches: list[type[Function[Any]]] = []

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
            has_varargs = any(p.is_varargs for p in positional_params)
            min_positional = len(required_positional)

            if has_varargs:
                # Varargs: allow any number >= min_positional
                if num_positional < min_positional:
                    continue  # Too few positional arguments
            else:
                # Fixed positional: must be within [min, max]
                max_positional = len(positional_params)
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

    @staticmethod
    def _validate_required_settings(
        func_cls: type[Function[Any]], invocation: "Invocation"
    ) -> None:
        """Validate that all required settings are present in invocation.

        Functions can declare required settings via Meta.required_settings.
        This method checks that all required settings are provided in the
        invocation.settings dictionary.

        Args:
            func_cls: The function class to validate settings for.
            invocation: The invocation containing settings.

        Raises:
            ValueError: If required settings are missing from the invocation.

        """
        meta = func_cls.get_metadata()
        required = set(meta.required_settings)

        if not required:
            return  # No settings required

        provided = set(invocation.settings.keys()) if invocation.settings else set()
        missing = required - provided

        if missing:
            raise ValueError(
                f"Function '{meta.name}' requires settings {sorted(missing)} "
                f"but they were not provided. Provided settings: {sorted(provided)}"
            )

    @staticmethod
    def _suggest_similar_names(name: str, candidates: list[str]) -> list[str]:
        """Find function names similar to the given name.

        Uses prefix matching, substring matching, and character overlap to
        suggest likely alternatives for typos.

        Args:
            name: The unknown function name.
            candidates: List of valid function names.

        Returns:
            List of similar names, sorted by relevance.

        """
        if not candidates:
            return []

        name_lower = name.lower()
        scored: list[tuple[int, str]] = []

        for candidate in candidates:
            candidate_lower = candidate.lower()

            # Exact prefix match (highest priority)
            if candidate_lower.startswith(name_lower):
                scored.append((0, candidate))
            elif name_lower.startswith(candidate_lower):
                scored.append((1, candidate))
            # Substring matches
            elif name_lower in candidate_lower or candidate_lower in name_lower:
                scored.append((2, candidate))
            else:
                # Character overlap score (for typos)
                name_chars = set(name_lower)
                candidate_chars = set(candidate_lower)
                overlap = len(name_chars & candidate_chars)
                # Require at least half the characters to match
                if overlap > len(name_lower) // 2:
                    scored.append((10 - overlap, candidate))

        scored.sort(key=lambda x: (x[0], x[1]))
        return [candidate for _, candidate in scored]

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

    def _read_single_record_batch(self, context: str) -> pa.RecordBatch:
        """Read a schema + record batch pair from stdin.

        Args:
            context: Description for debug logging (e.g., "invocation", "init_input").

        Returns:
            The deserialized RecordBatch.

        Raises:
            IPCError: If unexpected message types are received.

        """
        self.log.debug(f"{context}_reading")
        return read_single_record_batch(sys.stdin, context)

    def _read_invocation(self) -> Invocation:
        """Read and parse the call data from stdin."""
        return Invocation.deserialize(self._read_single_record_batch("invocation"))

    def _read_init_input(self) -> pa.RecordBatch:
        """Read and parse the init data from stdin."""
        return self._read_single_record_batch("init_input")

    def _create_bind_error_batch(
        self,
        exception: Exception,
        invocation: Invocation,
    ) -> bytes:
        """Create a serialized error batch for bind-time exceptions.

        Args:
            exception: The exception that occurred during bind.
            invocation: The invocation being processed.

        Returns:
            Serialized Arrow IPC bytes containing error metadata.

        """
        import vgi.ipc_utils
        import vgi.log

        error_message = vgi.log.Message.from_exception(exception)

        # Create zero-row batch with minimal schema
        batch = pa.RecordBatch.from_pydict(
            {"_error": pa.nulls(0)},
            schema=_BIND_ERROR_SCHEMA,
        )

        # Add error metadata
        metadata = error_message.add_to_metadata(invocation)
        batch = batch.replace_schema_metadata(
            {k.encode(): v.encode() for k, v in metadata.items()}
        )

        # Serialize as IPC
        return vgi.ipc_utils.serialize_record_batch(batch)

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
        if invocation.global_execution_identifier is None:
            raise ValueError(
                "global_execution_identifier is required but was None. "
                "This is an internal protocol error - the worker should have set "
                "global_execution_identifier after initialize_global_state()."
            )
        generator = instance.run()
        next(generator)  # Prime the run() generator

        with (
            ipc.new_stream(cast(IOBase, sys.stdout), instance.output_schema) as writer,
            ipc.open_stream(cast(IOBase, sys.stdin)) as data_reader,
        ):
            # Validate data stream schema matches expected input schema
            if data_reader.schema != invocation.input_schema:
                raise SchemaValidationError(
                    "Data stream schema does not match expected input schema.",
                    expected=invocation.input_schema,
                    actual=data_reader.schema,
                    context="input stream to scalar function",
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

                protocol_input = ScalarProtocolInput(batch=batch, metadata=metadata)
                output = generator.send(protocol_input)

                # Handle log messages (indicated by log_message being set)
                while output.log_message is not None:
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
                )
        return WorkerStats(
            batch_count=batch_count,
            total_input_rows=total_input_rows,
            total_output_rows=total_output_rows,
        )

    def _process_batches(
        self,
        instance: TableInOutGenerator,
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
        if invocation.global_execution_identifier is None:
            raise ValueError(
                "global_execution_identifier is required but was None. "
                "This is an internal protocol error - the worker should have set "
                "global_execution_identifier after initialize_global_state()."
            )
        generator = instance.run()
        next(generator)  # Prime the run() generator

        with (
            ipc.new_stream(cast(IOBase, sys.stdout), instance.output_schema) as writer,
            ipc.open_stream(cast(IOBase, sys.stdin)) as data_reader,
        ):
            # Validate data stream schema matches expected input schema
            if data_reader.schema != invocation.input_schema:
                raise SchemaValidationError(
                    "Data stream schema does not match expected input schema.",
                    expected=invocation.input_schema,
                    actual=data_reader.schema,
                    context="input stream to table-in-out function",
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

    def _handle_catalog_invocation(
        self,
        invocation: Invocation,
        fn_log: structlog.stdlib.BoundLogger,
    ) -> None:
        """Handle a CatalogInterface method invocation.

        Catalog invocations use a simplified protocol without bind→init→stream
        phases. The function_name field contains the method name to call, and
        the input batch (read from stdin) contains method parameters as columns.

        Args:
            invocation: The catalog invocation with method name and parameters.
            fn_log: Logger bound to the function context.

        Raises:
            ValueError: If catalog_interface is not configured.

        """
        catalog_class = self._get_catalog_interface()
        if catalog_class is None:
            raise ValueError(
                "CatalogInterface invocation received but no catalog is available. "
                "Either set catalog_interface class attribute to a CatalogInterface "
                "subclass, or ensure functions are defined and catalog_name is set."
            )

        # Instantiate the catalog interface
        catalog = catalog_class()
        method_name = invocation.function_name

        # Cast stdout to binary IO (reassigned in run() to binary mode)
        stdout = cast(IO[bytes], sys.stdout)

        # Get the method
        if not hasattr(catalog, method_name):
            raise ValueError(
                f"Unknown catalog method: '{method_name}'. "
                f"CatalogInterface does not have a method named '{method_name}'."
            )
        method = getattr(catalog, method_name)

        # Read arguments from input batch (1 row with columns matching parameters)
        # For methods with no arguments, accept 0 rows (empty batch)
        args_batch = self._read_single_record_batch("catalog_args")
        if args_batch.num_rows == 0:
            # No arguments - kwargs is empty
            kwargs: dict[str, Any] = {}
        elif args_batch.num_rows == 1:
            # Convert batch columns to kwargs
            row = args_batch.to_pylist()[0]
            kwargs = {name: value for name, value in row.items()}
        else:
            raise ValueError(
                f"Catalog invocation expects 0 or 1 rows in argument batch, "
                f"got {args_batch.num_rows}"
            )

        fn_log.debug("catalog_method_call", method=method_name, kwargs=kwargs)

        # Call the method
        result = method(**kwargs)

        fn_log.debug("catalog_method_result", result=result)

        # Serialize and stream result
        # Result types:
        # - None → empty batch (0 rows, 0 columns)
        # - list of primitives → convert to single-column batch (e.g., catalogs())
        # - Dataclass with serialize() → serialize to bytes, write
        # - Iterable of dataclasses → stream multiple serialized items
        if result is None:
            # Write empty batch to signal no result
            batch = pa.RecordBatch.from_pydict({})
            stdout.write(batch.schema.serialize().to_pybytes())
            stdout.write(batch.serialize().to_pybytes())
        elif isinstance(result, list) and (
            not result or not hasattr(result[0], "serialize")
        ):
            # List of primitives (e.g., strings from catalogs())
            batch = pa.RecordBatch.from_pydict({"value": result})
            stdout.write(batch.schema.serialize().to_pybytes())
            stdout.write(batch.serialize().to_pybytes())
        elif hasattr(result, "serialize"):
            # Single dataclass result - write serialized bytes directly
            result_bytes = result.serialize()
            stdout.write(result_bytes)
        else:
            # Try to iterate (for schema_contents, schemas, etc.)
            try:
                for item in result:
                    if hasattr(item, "serialize"):
                        item_bytes = item.serialize()
                        stdout.write(item_bytes)
                    else:
                        raise TypeError(
                            f"Catalog result item has no serialize method: "
                            f"{type(item).__name__}"
                        )
                # Write empty batch to signal end of stream
                batch = pa.RecordBatch.from_pydict({})
                stdout.write(batch.schema.serialize().to_pybytes())
                stdout.write(batch.serialize().to_pybytes())
            except TypeError:
                raise TypeError(
                    f"Catalog method returned unsupported type: "
                    f"{type(result).__name__}. Expected None, a dataclass "
                    f"with serialize(), or an iterable of such dataclasses."
                ) from None

        fn_log.info("catalog_invocation_complete", method=method_name)

    def run(self) -> None:
        """Run the worker, reading from stdin and writing to stdout."""
        self.log.info("worker_starting")
        sys.stdin = os.fdopen(0, "rb")
        sys.stdout = os.fdopen(1, "wb", buffering=0)

        invocation = self._read_invocation()

        fn_log = self.log.bind(function=invocation.function_name)
        fn_log.info("init_received", arguments=invocation.arguments)
        fn_log.debug("input_schema_parsed", schema=str(invocation.input_schema))

        # Dispatch catalog invocations separately (simplified protocol)
        if invocation.function_type == InvocationType.CATALOG:
            self._handle_catalog_invocation(invocation, fn_log)
            return

        # Wrap bind phase in try-except to catch and report bind-time errors.
        # This covers: unknown function, argument matching, settings validation,
        # function instantiation, and output_schema generation.
        try:
            registry = self._build_registry()
            if invocation.function_name not in registry:
                available = sorted(registry.keys())
                suggestions = self._suggest_similar_names(
                    invocation.function_name, available
                )
                msg_lines = [
                    f"Unknown function: '{invocation.function_name}'",
                    "",
                ]
                if suggestions:
                    msg_lines.append("  Did you mean:")
                    for suggestion in suggestions[:3]:
                        msg_lines.append(f"    - {suggestion}")
                    msg_lines.append("")
                msg_lines.append(f"  Available functions: {available}")
                raise ValueError("\n".join(msg_lines))

            candidates = registry[invocation.function_name]
            func_cls = self._match_function(invocation, candidates)

            # Validate required settings before instantiation
            self._validate_required_settings(func_cls, invocation)

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
                max_processes=instance.max_processes,
                invocation_id=instance.create_invocation_id(),
                active_features=active_features,
            ).serialize()
        except (KeyboardInterrupt, SystemExit):
            raise  # Let these propagate normally
        except Exception as e:
            fn_log.exception("bind_failed", error=str(e))
            error_batch_bytes = self._create_bind_error_batch(e, invocation)
            sys.stdout.write(error_batch_bytes)
            sys.stdout.flush()
            return  # Exit cleanly after sending error

        if sys.stdout.write(bind_result_bytes) != len(bind_result_bytes):
            raise OSError("Failed to write bind result record batch")

        if invocation.global_execution_identifier is None:
            # Primary worker: perform init and store in storage
            fn_log.info("processing_init")
            init_result = instance.initialize_global_state(self._read_init_input())
            init_result_bytes = init_result.serialize()
            if sys.stdout.write(init_result_bytes) != len(init_result_bytes):
                raise OSError("Failed to write init result record batch")
            fn_log.info("processing_init_complete", init_result=init_result)
            invocation = invocation.with_global_execution_identifier(init_result)
        else:
            # Secondary worker: retrieve shared init from storage
            fn_log.info("retrieving_init")
            instance.load_global_state(invocation.global_execution_identifier)

        # Dispatch to appropriate processing method based on function type.
        # ScalarFunctionGenerator processes input batches to single-column output.
        # TableInOutGenerator reads input batches and produces output.
        # TableFunctionGenerator generates output without input batches.
        # Note: Check ScalarFunctionGenerator first since it doesn't inherit from
        # TableInOutGenerator, then TableInOutGenerator.
        if isinstance(instance, ScalarFunctionGenerator):
            stats = self._process_scalar_batches(instance, invocation, fn_log)
        elif isinstance(instance, TableInOutGenerator):
            stats = self._process_batches(instance, invocation, fn_log)
        elif isinstance(instance, TableFunctionGenerator):
            stats = self._generate_batches(instance, invocation, fn_log)
        else:
            raise TypeError(
                f"Unsupported function type: {type(instance).__name__}. "
                f"Functions must inherit from ScalarFunctionGenerator (for "
                f"scalar functions), TableInOutGenerator (for functions "
                f"that process input batches), or TableFunctionGenerator (for "
                f"functions that generate output without input). "
                f"See vgi.scalar_function, vgi.table_in_out_function, and "
                f"vgi.table_function modules."
            )

        fn_log.info(
            "worker_complete",
            stats=stats,
        )
