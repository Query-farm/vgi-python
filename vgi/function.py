"""Core data structures for VGI function calls and bind results.

This module defines the foundational classes used during function binding
in the VGI protocol. When a client invokes a function, it sends CallData
describing the function name, arguments, and input schema. The worker
returns a BindResult describing the output schema and execution hints.

Classes:
    Arguments: Container for positional and named function arguments.
    CallData: Complete function invocation request (name, args, input schema).
    BindResult: Base result from binding a function (output schema, parallelization).

The CallData and BindResult are serialized to Arrow IPC format for transmission
between client and worker processes.

See Also:
    vgi.table_function: Extended bind results with cardinality hints.
    vgi.table_in_out_function: Streaming table functions built on these primitives.
"""

import json
import os
import uuid
from dataclasses import dataclass
from functools import cached_property
from typing import Any

import pyarrow as pa

__all__ = ["Arguments", "CallData", "BindResult", "LogLevel", "LogMessage"]

import traceback
from enum import Enum

import vgi.util


class LogLevel(Enum):
    """Severity levels for log messages emitted during function processing.

    Levels are ordered from most to least severe. Use the appropriate level
    to indicate the nature of the message:

    Attributes:
        EXCEPTION: Unrecoverable error that terminated processing.
        ERROR: Significant error that may affect results but didn't terminate.
        WARN: Potential issue that should be reviewed but isn't necessarily wrong.
        INFO: General informational message about processing status.
        DEBUG: Detailed information useful for debugging.
        TRACE: Fine-grained tracing information for detailed diagnostics.
    """

    EXCEPTION = "EXCEPTION"
    ERROR = "ERROR"
    WARN = "WARN"
    INFO = "INFO"
    DEBUG = "DEBUG"
    TRACE = "TRACE"


@dataclass(frozen=True, slots=True)
class LogMessage:
    """Log message that can be returned from process_batch() via ProcessResult.

    LogMessage allows functions to emit diagnostic information during batch
    processing. Messages are attached to the output metadata and transmitted
    to the client alongside the output batch.

    Attributes:
        level: Severity level indicating the nature of the message.
        message: Human-readable log message text.

    Example:
        def process_batch(self, batch, is_finalize):
            if batch.num_rows == 0:
                return ProcessResult(
                    batch,
                    log_message=LogMessage.info("Received empty batch")
                )
            return ProcessResult(batch)
    """

    level: LogLevel
    message: str

    @classmethod
    def exception(cls, message: str) -> "LogMessage":
        """Create an EXCEPTION level log message.

        Use for unrecoverable errors that terminated processing.
        """
        return cls(LogLevel.EXCEPTION, message)

    @classmethod
    def error(cls, message: str) -> "LogMessage":
        """Create an ERROR level log message.

        Use for significant errors that may affect results.
        """
        return cls(LogLevel.ERROR, message)

    @classmethod
    def info(cls, message: str) -> "LogMessage":
        """Create an INFO level log message.

        Use for general informational messages about processing status.
        """
        return cls(LogLevel.INFO, message)

    def add_to_metadata(
        self, call_data: "CallData", metadata: dict[str, str] | None = None
    ) -> dict[str, str]:
        """Add log message fields to an existing metadata dictionary.

        Creates a new dictionary with 'log_level' and 'log_message' keys added.
        The log_message value is JSON containing the message text, call identifier,
        and process ID for correlation. Does not mutate the input dictionary.

        Args:
            call_data: The CallData for this function invocation, used to include
                the call_identifier in the log message for correlation.
            metadata: Existing metadata dict to augment, or None to create new.

        Returns:
            New dict containing original entries plus:
            - log_level: The LogLevel value (e.g., "INFO", "EXCEPTION")
            - log_message: JSON string with {message, call_id, pid}
        """
        result = dict(metadata) if metadata else {}
        result["log_level"] = self.level.value
        result["log_message"] = json.dumps(
            {
                "message": self.message,
                "call_id": call_data.call_identifier_hex,
                "pid": call_data.pid,
            }
        )
        return result

    @classmethod
    def from_exception(cls, exc: Exception) -> "LogMessage":
        """Create an EXCEPTION level log message from an Exception instance.

        Args:
            exc: Exception instance to create log message from.
        Returns:
            LogMessage with level EXCEPTION and message from the exception.
        """
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        return cls(LogLevel.EXCEPTION, f"{type(exc).__name__}: {exc}\n\n{tb}")


@dataclass(frozen=True, slots=True)
class Arguments:
    """Container for function call positional and named arguments.

    Arguments are passed to functions during invocation. They support both
    positional arguments (accessed by index) and named/keyword arguments.

    Serialization encodes arguments to a flat dictionary format suitable for
    Arrow IPC: positional args become "positional_0", "positional_1", etc.,
    and named args become "named_<name>".

    Attributes:
        positional: List of positional argument values in order.
        named: Dictionary mapping argument names to values.

    Example:
        # Function call: my_func("hello", 42, separator=",")
        args = Arguments(
            positional=["hello", 42],
            named={"separator": ","}
        )
    """

    positional: list[pa.Scalar | None]
    named: dict[str, pa.Scalar]

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
        } | {f"named_{name}": value for name, value in self.named.items()}

    def schema(self) -> pa.Schema:
        """Return Arrow schema used when serializing Arguments.

        The schema defines a single binary field for the serialized positional
        arguments. Named arguments are not currently supported.

        Returns:
            Arrow schema with fields for each serialized attribute.
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
        named: dict[str, Any] = {}
        for key, value in data.items():
            if key.startswith("positional_"):
                index = int(key[len("positional_") :])
                while len(positional) <= index:
                    positional.append(None)
                positional[index] = value
            elif key.startswith("named_"):
                name = key[len("named_") :]
                named[name] = value
        return Arguments(positional=positional, named=named)


@dataclass(frozen=True, slots=True)
class GlobalInitResult:
    """
    The result from running global init of any function.
    """

    global_init_identifier: bytes | None = None

    IDENTIFIER_FIELD_NAME = "global_init_identifier"

    @classmethod
    def has_identifier(cls, data: pa.RecordBatch) -> bool:
        """Check if the RecordBatch contains a global_init_identifier field.

        Args:
            data: RecordBatch to check for the field.
        Returns:
            True if the field exists, False otherwise.
        """
        return cls.IDENTIFIER_FIELD_NAME in data.schema.names

    def schema(self) -> pa.Schema:
        """Return Arrow schema used when serializing GlobalInitResult.

        Returns:
            Arrow schema with fields for each serialized attribute.
        """

        return pa.schema(
            [
                pa.field(self.IDENTIFIER_FIELD_NAME, pa.binary(), nullable=True),
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
                    self.IDENTIFIER_FIELD_NAME: self.global_init_identifier,
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
        if data.num_rows == 0:
            raise ValueError(
                "Cannot deserialize GlobalInitResult from empty RecordBatch"
            )
        if data.num_rows > 1:
            raise ValueError(
                "Expected single-row RecordBatch for GlobalInitResult deserialization"
            )

        first_row = data.to_pylist()[0]
        required_fields = [cls.IDENTIFIER_FIELD_NAME]

        for field in required_fields:
            if field not in first_row:
                raise ValueError(
                    f"Missing '{field}' field in GlobalInitResult RecordBatch"
                )

        return GlobalInitResult(
            global_init_identifier=first_row[cls.IDENTIFIER_FIELD_NAME],
        )


@dataclass(frozen=True, slots=True)
class CallData:
    """Complete function invocation request sent from client to worker.

    CallData encapsulates all information needed to bind and execute a function:
    the function name, its arguments, the expected input schema (for table
    functions), and a unique identifier for correlating parallel workers.

    This is serialized to Arrow IPC format and sent as the first message when
    the client connects to a worker subprocess.

    Attributes:
        function_name: Name of the function to invoke, must exist in worker registry.
        arguments: Positional and named arguments passed to the function.
        in_schema: Arrow schema of input data (required for TableInOutFunction,
            None for scalar functions or functions that don't process input tables).
        call_identifier: Unique bytes identifying this invocation. Used to correlate
            multiple parallel workers processing the same logical function call.

    Example:
        call_data = CallData(
            function_name="sum_columns",
            arguments=Arguments(positional=["col1", "col2"], named={}),
            in_schema=pa.schema([pa.field("col1", pa.int64())]),
            call_identifier=uuid.uuid4().bytes,
        )
    """

    function_name: str
    arguments: Arguments
    in_schema: pa.Schema | None
    call_identifier: bytes

    global_init_identifier: GlobalInitResult | None = None

    def with_global_init_identifier(
        self, global_init_identifier: GlobalInitResult
    ) -> "CallData":
        """Return a new CallData with the given global_init_identifier.
        Args:
            global_init_identifier: The GlobalInitResult to set.
        Returns:
            New CallData instance with updated global_init_identifier.
        """
        return CallData(
            function_name=self.function_name,
            arguments=self.arguments,
            in_schema=self.in_schema,
            call_identifier=self.call_identifier,
            global_init_identifier=global_init_identifier,
        )

    def serialize(self) -> bytes:
        """Serialize CallData to an Arrow RecordBatch.

        Returns:
            RecordBatch containing serialized CallData fields.
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
                    "in_schema": self.in_schema.serialize().to_pybytes()
                    if self.in_schema
                    else None,
                    "call_identifier": self.call_identifier,
                }
            ],
            schema=pa.schema(
                [
                    pa.field("function_name", pa.string(), nullable=False),
                    pa.field("arguments", args_struct_type, nullable=True),
                    pa.field("in_schema", pa.binary(), nullable=True),
                    pa.field("call_identifier", pa.binary(), nullable=True),
                ]
            ),
        )
        return vgi.util.recordbatch_to_bytes(batch)

    @staticmethod
    def deserialize(data: pa.RecordBatch) -> "CallData":
        """Deserialize CallData from an Arrow RecordBatch.

        Args:
          data: RecordBatch containing serialized CallData fields.

        Returns:
          Deserialized CallData instance.

        Raises:
          ValueError: If RecordBatch is empty, has multiple rows, or missing
              required fields.
        """
        if data.num_rows == 0:
            raise ValueError("Cannot deserialize CallData from empty RecordBatch")
        if data.num_rows > 1:
            raise ValueError(
                "Expected single-row RecordBatch for CallData deserialization"
            )

        first_row = data.to_pylist()[0]
        required_fields = ["function_name", "arguments", "in_schema", "call_identifier"]

        for field in required_fields:
            if field not in first_row:
                raise ValueError(f"Missing '{field}' field in CallData RecordBatch")

        in_schema = None
        if first_row["in_schema"] is not None:
            in_schema = pa.ipc.read_schema(pa.py_buffer(first_row["in_schema"]))

        return CallData(
            function_name=first_row["function_name"],
            arguments=Arguments.decode(data.column("arguments")[0]),
            in_schema=in_schema,
            call_identifier=first_row["call_identifier"],
            global_init_identifier=GlobalInitResult(
                first_row[GlobalInitResult.IDENTIFIER_FIELD_NAME]
            )
            if GlobalInitResult.IDENTIFIER_FIELD_NAME in data.schema.names
            else None,
        )

    @cached_property
    def pid(self) -> int:
        """Process ID of the worker handling this CallData.

        Returns:
            Process ID as an integer.
        """
        return os.getpid()

    @cached_property
    def call_identifier_hex(self) -> str:
        """Hexadecimal string representation of the call identifier.

        Returns:
            Hex string of the call_identifier bytes.
        """
        return self.call_identifier.hex()


@dataclass(frozen=True, slots=True)
class BindResult:
    """Base result from binding a function.

    The bind result is created during function initialization and describes
    the function's output characteristics. It is serialized and sent to the
    client before any data processing begins.

    Attributes:
        output_schema: Arrow schema describing the structure of output batches.
        max_processes: Maximum parallel processes this function can utilize.
            Set to 1 for functions that must process sequentially (e.g.,
            aggregations). Higher values enable parallel execution.
        call_identifier: Unique bytes identifying this function invocation.
            Used to correlate multiple parallel workers processing the same
            logical function call.

    See Also:
        TableFunctionBindResult: Extended version with cardinality hints.
    """

    output_schema: pa.Schema
    max_processes: int
    call_identifier: bytes

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
                pa.field("call_identifier", pa.binary(), nullable=True),
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
            "call_identifier": self.call_identifier,
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


class Function:
    """Base class for all VGI functions.

    Functions are instantiated with CallData describing the invocation,
    then queried for execution hints (max_processes, call_identifier).

    Subclasses should override methods to customize behavior:
    - max_processes(): Parallelization hint for the query planner
    - call_identifier(): Unique ID to correlate parallel workers

    See Also:
        vgi.table_function.TableFunction: Adds cardinality hints.
        vgi.table_in_out_function.TableInOutFunction: Full streaming implementation.
    """

    init_data: GlobalInitResult = GlobalInitResult()

    def __init__(self, call_data: CallData):
        """Initialize the function with call data.

        Args:
            call_data: Complete invocation request including function name,
                arguments, and input schema.
        """
        pass

    def max_processes(self) -> int:
        """Return maximum number of parallel processes this function can utilize.

        Override to enable parallel execution. Return 1 (default) for functions
        that must process sequentially (e.g., aggregations with shared state).

        Returns:
            Maximum parallel processes. Default is 1.
        """
        return 1

    def call_identifier(self) -> bytes:
        """Return unique identifier for this function invocation.

        When max_processes > 1, this ID correlates multiple parallel workers
        processing the same logical function call.

        Returns:
            Unique bytes identifier. Default is a random UUID.
        """
        return uuid.uuid4().bytes

    def process_init(self, input: pa.RecordBatch) -> GlobalInitResult:
        """Perform any global initialization required before processing.

        This method is called once per worker process before any data
        batches are processed. Override to set up shared resources, load
        models, or perform expensive setup tasks.

        Args:
            input: An initial RecordBatch that may contain configuration
                or context information for initialization.
        """

        # If there is an id supplied, detect it so it will be passed on.
        if GlobalInitResult.has_identifier(input):
            return GlobalInitResult.deserialize(input)

        return GlobalInitResult()
