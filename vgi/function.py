"""Core data structures for VGI function calls and bind results.

This module defines the foundational classes used during function binding
in the VGI protocol. When a client invokes a function, it sends Request
describing the function name, arguments, and input schema. The worker
returns an OutputSpec describing the output schema and execution hints.

Classes:
    Arguments: Container for positional and named function arguments.
    Request: Complete function invocation request (name, args, schema).
    OutputSpec: Result from binding a function (output schema, etc).

The Request and OutputSpec are serialized to Arrow IPC format
for transmission between client and worker processes.

See Also:
    vgi.log: LogLevel and LogMessage for function diagnostics.
    vgi.table_function: Extended bind results with cardinality hints.
    vgi.table_in_out_function: Streaming table functions built on these primitives.

"""

import os
import uuid
from dataclasses import dataclass, replace
from typing import Any, ClassVar

import pyarrow as pa
import structlog

import vgi.util
from vgi.log import Level, Message

__all__ = [
    "Arguments",
    "Function",
    "GlobalInitResult",
    "InitStorage",
    "Level",
    "Message",
    "OutputSpec",
    "Request",
]


@dataclass(frozen=True, slots=True)
class Arguments:
    """Container for function call positional and named arguments.

    Arguments are passed to functions during invocation. They support both
    positional arguments (accessed by index) and named/keyword arguments.

    Serialization encodes arguments to a flat dictionary format suitable for
    Arrow IPC: positional args become "positional_0", "positional_1", etc.,
    and named args become "named_<name>".

    Attributes:
        positional: Tuple of positional argument values in order.
        named: Dictionary mapping argument names to values, or None if no named args.

    Example:
        # Function call: my_func("hello", 42, separator=",")
        args = Arguments(
            positional=("hello", 42),
            named={"separator": ","}
        )

    """

    positional: tuple[pa.Scalar | None, ...] = ()
    named: dict[str, pa.Scalar] | None = None

    def encoded_dict(self) -> dict[str, pa.Scalar | None]:
        """Convert arguments to a dictionary suitable for serialization.

        Positional arguments are stored with keys "positional_0", "positional_1", etc.
        Named arguments are stored with their actual names prefixed by "named_".

        The reason why a dictionary is used is to facilitate serialization with Arrow,
        which can easily handle flat structures, but doesn't handle variable typed
        arrays of arbitrary objects.

        Returns:
            Dictionary mapping argument names to their values.

        """
        return {
            f"positional_{index}": value for index, value in enumerate(self.positional)
        } | (
            {f"named_{name}": value for name, value in self.named.items()}
            if self.named
            else {}
        )

    def schema(self) -> pa.Schema:
        """Return Arrow schema for serializing these Arguments.

        Creates a schema with one field per argument: "positional_0", "positional_1",
        etc. for positional args, and "named_<name>" for named args. Field types
        are inferred from the argument values.

        Returns:
            Arrow schema matching the structure returned by encoded_dict().

        """
        return pa.RecordBatch.from_pylist([self.encoded_dict()]).schema

    @staticmethod
    def decode(data: pa.StructScalar) -> "Arguments":
        """Decode Arguments from a serialized dictionary.

        Args:
            data: Dictionary containing serialized argument fields.

        Returns:
            Deserialized Arguments instance.

        """
        positional: list[pa.Scalar | None] = []
        named: dict[str, pa.Scalar] = {}
        for key, value in data.items():
            if key.startswith("positional_"):
                index = int(key[len("positional_") :])
                while len(positional) <= index:
                    positional.append(None)
                positional[index] = value
            elif key.startswith("named_"):
                name = key[len("named_") :]
                named[name] = value
        return Arguments(positional=tuple(positional), named=named or None)


@dataclass(frozen=True, slots=True)
class GlobalInitResult:
    """Result from the global initialization phase of a function.

    When a function supports parallel execution (max_processes > 1), the first
    worker runs perform_init() which returns a GlobalInitResult. This result
    contains an identifier that is passed to all subsequent parallel workers
    via retrieve_init(), allowing them to share state or coordinate processing.

    Attributes:
        global_init_identifier: Opaque bytes that identify the initialized state.
            None if no global initialization was performed.

    """

    global_init_identifier: bytes | None = None

    _IDENTIFIER_FIELD_NAME: ClassVar[str] = "global_init_identifier"

    @classmethod
    def has_identifier(cls, data: pa.RecordBatch) -> bool:
        """Check if the RecordBatch contains a global_init_identifier field.

        Args:
            data: RecordBatch to check for the field.

        Returns:
            True if the field exists, False otherwise.

        """
        return cls._IDENTIFIER_FIELD_NAME in data.schema.names

    def schema(self) -> pa.Schema:
        """Return Arrow schema used when serializing GlobalInitResult.

        Returns:
            Arrow schema with fields for each serialized attribute.

        """
        return pa.schema(
            [
                pa.field(self._IDENTIFIER_FIELD_NAME, pa.binary(), nullable=True),
            ]
        )

    def serialize(self) -> bytes:
        """Serialize GlobalInitResult to an Arrow RecordBatch.

        Returns:
            RecordBatch containing serialized GlobalInitResult fields.

        """
        batch = pa.RecordBatch.from_pylist(
            [
                {
                    self._IDENTIFIER_FIELD_NAME: self.global_init_identifier,
                }
            ],
            schema=self.schema(),
        )
        return vgi.util.recordbatch_to_bytes(batch)

    @classmethod
    def deserialize(cls, data: pa.RecordBatch) -> "GlobalInitResult":
        """Deserialize GlobalInitResult from an Arrow RecordBatch.

        Args:
          data: RecordBatch containing serialized GlobalInitResult fields.

        Returns:
          Deserialized GlobalInitResult instance.

        """
        first_row = vgi.util.validate_single_row_batch(
            data, "GlobalInitResult", required_fields=[cls._IDENTIFIER_FIELD_NAME]
        )
        return GlobalInitResult(
            global_init_identifier=first_row[cls._IDENTIFIER_FIELD_NAME],
        )


@dataclass(frozen=True, slots=True)
class Request:
    """Complete function invocation request sent from client to worker.

    Request encapsulates all information needed to bind and execute a function:
    the function name, its arguments, the expected input schema (for table
    functions), and identifiers for logging and correlation.

    This is serialized to Arrow IPC format and sent as the first message when
    the client connects to a worker subprocess.

    Attributes:
        function_name: Name of the function to invoke, must exist in worker registry.
        arguments: Positional and named arguments passed to the function.
        in_out_function_input_schema: Arrow schema of input data (required for
            Function, None for scalar functions or functions that don't
            process input tables).
        correlation_id: String identifier for logging and correlation purposes.
        invocation_id: Unique bytes identifying this function binding. Used to
            correlate multiple parallel workers processing the same logical call.
        global_init_identifier: Optional result from global initialization phase.

    Example:
        invocation = Request(
            function_name="sum_columns",
            arguments=Arguments(positional=("col1", "col2")),
            in_out_function_input_schema=pa.schema([pa.field("col1", pa.int64())]),
            correlation_id="request-123",
            invocation_id=None,  # Set by worker after binding
        )

    """

    function_name: str
    in_out_function_input_schema: pa.Schema | None

    correlation_id: str
    # The unique identifier for the call, typically this may be a uuid.
    invocation_id: bytes | None

    global_init_identifier: GlobalInitResult | None = None
    arguments: Arguments = Arguments()

    def with_global_init_identifier(
        self, global_init_identifier: GlobalInitResult
    ) -> "Request":
        """Return a new Request with the given global_init_identifier."""
        return replace(self, global_init_identifier=global_init_identifier)

    def serialize(self) -> bytes:
        """Serialize Request to an Arrow RecordBatch.

        Returns:
            RecordBatch containing serialized Request fields.

        """
        args_dict = self.arguments.encoded_dict()
        encoded_batch = pa.RecordBatch.from_pylist([args_dict]).schema
        args_struct_type = pa.struct(
            [
                pa.field(name, encoded_batch.field(name).type)
                for name in encoded_batch.names
            ]
        )

        batch = pa.RecordBatch.from_pylist(
            [
                {
                    "function_name": self.function_name,
                    "arguments": args_dict,
                    "in_out_function_input_schema": (
                        self.in_out_function_input_schema.serialize().to_pybytes()
                        if self.in_out_function_input_schema
                        else None
                    ),
                    "invocation_id": self.invocation_id,
                    "correlation_id": self.correlation_id,
                }
            ],
            schema=pa.schema(
                [
                    pa.field("function_name", pa.string(), nullable=False),
                    pa.field("arguments", args_struct_type, nullable=True),
                    pa.field(
                        "in_out_function_input_schema", pa.binary(), nullable=True
                    ),
                    pa.field("invocation_id", pa.binary(), nullable=True),
                    pa.field("correlation_id", pa.string(), nullable=False),
                ]
            ),
        )
        return vgi.util.recordbatch_to_bytes(batch)

    @staticmethod
    def deserialize(data: pa.RecordBatch) -> "Request":
        """Deserialize Request from an Arrow RecordBatch.

        Args:
          data: RecordBatch containing serialized Request fields.

        Returns:
          Deserialized Request instance.

        Raises:
          ValueError: If RecordBatch is empty, has multiple rows, or missing
              required fields.

        """
        required_fields = [
            "function_name",
            "arguments",
            "in_out_function_input_schema",
            "invocation_id",
            "correlation_id",
        ]
        first_row = vgi.util.validate_single_row_batch(
            data, "Request", required_fields=required_fields
        )

        in_out_function_input_schema = None
        if first_row["in_out_function_input_schema"] is not None:
            in_out_function_input_schema = pa.ipc.read_schema(
                pa.py_buffer(first_row["in_out_function_input_schema"])
            )

        return Request(
            function_name=first_row["function_name"],
            arguments=Arguments.decode(data.column("arguments")[0]),
            in_out_function_input_schema=in_out_function_input_schema,
            invocation_id=first_row["invocation_id"],
            correlation_id=first_row["correlation_id"],
            global_init_identifier=GlobalInitResult(
                first_row[GlobalInitResult._IDENTIFIER_FIELD_NAME]
            )
            if GlobalInitResult._IDENTIFIER_FIELD_NAME in data.schema.names
            else None,
        )

    @staticmethod
    def pid() -> int:
        """Return the current process ID."""
        return os.getpid()


@dataclass(frozen=True, slots=True)
class OutputSpec:
    """Base result from binding a function.

    The bind result is created during function initialization and describes
    the function's output characteristics. It is serialized and sent to the
    client before any data processing begins.

    Attributes:
        output_schema: Arrow schema describing the structure of output batches.
        max_processes: Maximum parallel processes this function can utilize.
            Set to 1 for functions that must process sequentially (e.g.,
            aggregations). Higher values enable parallel execution.
        invocation_id: Unique bytes identifying this function invocation.
            Used to correlate multiple parallel workers processing the same
            logical function call.

    """

    output_schema: pa.Schema
    max_processes: int
    invocation_id: bytes

    def serialize_schema(self) -> pa.Schema:
        """Return the Arrow schema used when serializing this bind result.

        The schema defines the structure of the single-row RecordBatch that
        carries the bind result over IPC. Subclasses override this to add
        additional fields (e.g., cardinality estimates).

        Returns:
            Arrow schema with fields for each serialized attribute.

        """
        return pa.schema(
            [
                pa.field("output_schema", pa.binary(), nullable=False),
                pa.field("max_processes", pa.int64(), nullable=True),
                pa.field("invocation_id", pa.binary(), nullable=True),
            ]
        )

    def serialize_dict(self) -> dict[str, Any]:
        """Convert this bind result to a dictionary for Arrow serialization.

        The dictionary keys correspond to serialize_schema() field names.
        The output_schema is serialized to bytes; other fields are passed
        as-is. Subclasses override this to add additional fields.

        Returns:
            Dictionary mapping field names to serializable values.

        """
        return {
            "output_schema": self.output_schema.serialize().to_pybytes(),
            "max_processes": self.max_processes,
            "invocation_id": self.invocation_id,
        }

    def serialize(self) -> bytes:
        """Serialize the bind result to bytes for IPC transmission.

        Creates a single-row Arrow RecordBatch containing the bind result,
        then serializes it. The wire format is: schema bytes + batch bytes.

        Returns:
            Concatenated schema and batch bytes ready for IPC transmission.

        """
        bind_result_schema = self.serialize_schema()

        # TODO: add support for column level statistics
        bind_result_batch = pa.RecordBatch.from_pylist(
            [self.serialize_dict()],
            schema=bind_result_schema,
        )
        return vgi.util.recordbatch_to_bytes(bind_result_batch)


class InitStorage:
    """In-process storage for init values retrievable by ID."""

    def __init__(self) -> None:
        """Initialize empty storage."""
        self.contents: dict[bytes, Any] = {}

    def create(self, value: Any) -> bytes:
        """Store a value and return its unique key."""
        key = uuid.uuid4().bytes
        self.contents[key] = value
        return key

    def get(self, key: bytes) -> Any:
        """Retrieve a value by key, raising KeyError if not found."""
        if key not in self.contents:
            raise KeyError(f"Key {key.hex()} not found in InitStorage")
        return self.contents[key]

    def delete(self, key: bytes) -> None:
        """Delete a value by key if it exists."""
        if key in self.contents:
            del self.contents[key]

    def has(self, key: bytes) -> bool:
        """Check if a key exists in storage."""
        return key in self.contents


class Function:
    """Base class for all VGI functions.

    Functions are instantiated with Request describing the invocation,
    then queried for execution hints (max_processes, invocation_id).

    Subclasses should override methods to customize behavior:
    - max_processes(): Parallelization hint for the query planner
    - invocation_id(): Unique ID to correlate parallel workers

    See Also:
        vgi.table_function.Function: Adds cardinality hints.
        vgi.table_in_out_function.Function: Full streaming implementation.

    """

    init_storage: ClassVar[InitStorage] = InitStorage()

    def __init__(self, *, logger: structlog.stdlib.BoundLogger):
        """Initialize the function with a logger.

        Args:
            logger: Structured logger for function diagnostics.

        """
        self.logger = logger

    def max_processes(self) -> int:
        """Return maximum number of parallel processes this function can utilize.

        Override to enable parallel execution. Return 1 (default) for functions
        that must process sequentially (e.g., aggregations with shared state).

        Returns:
            Maximum parallel processes. Default is 1.

        """
        return 1

    def invocation_id(self) -> bytes:
        """Return unique identifier for this function invocation.

        When max_processes > 1, this ID correlates multiple parallel workers
        processing the same logical function call.

        Returns:
            Unique bytes identifier. Default is a random UUID.

        """
        return uuid.uuid4().bytes

    def perform_init(self, init_input: pa.RecordBatch) -> GlobalInitResult:
        """Perform any global initialization required before processing.

        This method is called once per worker process before any data
        batches are processed. Override to set up shared resources, load
        models, or perform expensive setup tasks.

        Args:
            init_input: An initial RecordBatch that may contain configuration
                or context information for initialization.

        """
        # If there is an id supplied, detect it so it will be passed on.
        if GlobalInitResult.has_identifier(init_input):
            return GlobalInitResult.deserialize(init_input)

        return GlobalInitResult()

    def retrieve_init(self, init_input: GlobalInitResult) -> None:
        """Retrieve init data from storage (default does nothing)."""

    @property
    def output_schema(self) -> pa.Schema:
        """Return the output schema (must be implemented by subclass)."""
        raise NotImplementedError("Must be implemented by subclass.")
