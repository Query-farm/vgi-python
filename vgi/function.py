"""Core data structures for VGI function calls and bind results.

This module defines the foundational classes used during function binding
in the VGI protocol. When a client invokes a function, it sends Invocation
describing the function name, arguments, input schema, and function type.
The worker returns an OutputSpec describing the output schema and execution hints.

Classes:
    InvocationType: Enum distinguishing scalar vs table invocation types.
    Arguments: Container for positional and named function arguments.
    Invocation: Complete function invocation request (name, args, schema, type).
    OutputSpec: Result from binding a function (output schema, etc).
    Function: Base class for all VGI functions.

The Invocation and OutputSpec are serialized to Arrow IPC format
for transmission between client and worker processes.

See Also:
    vgi.scalar_function: Scalar functions with 1:1 row transforms.
    vgi.table_function: Table functions with cardinality hints.
    vgi.table_in_out_function: Streaming table functions for batch transforms.
    vgi.log: LogLevel and LogMessage for function diagnostics.

"""

import os
import uuid
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from functools import cached_property
from typing import (
    Any,
    ClassVar,
    Protocol,
    Self,
    TypeVar,
    cast,
    final,
    get_args,
    get_origin,
)

import pyarrow as pa
import structlog

import vgi.ipc_utils
import vgi.log
from vgi.exceptions import ExecutionIdentifierError, SchemaValidationError
from vgi.function_storage import FunctionStorage, FunctionStorageSqlite
from vgi.invocation import InitResult, Invocation
from vgi.metadata import DEFAULT_MAX_WORKERS, MetadataMixin, ResolvedMetadata
from vgi.output_complete import OutputComplete

__all__ = [
    "Function",
    "FunctionInitInput",
    "OutputSpec",
    "Serializable",
]


class Serializable(Protocol):
    """Protocol for objects that can be serialized to/from bytes.

    User-defined state classes should implement this protocol to be usable
    with the distributed function state storage framework.

    Example:
        @dataclass
        class MySumState:
            sums: dict[str, int]

            def serialize(self) -> bytes:
                import pickle
                return pickle.dumps(self.sums)

            @classmethod
            def deserialize(cls, data: bytes) -> Self:
                import pickle
                return cls(sums=pickle.loads(data))

    """

    def serialize(self) -> bytes:
        """Serialize this object to bytes."""
        ...

    @classmethod
    def deserialize(cls, data: bytes) -> Self:
        """Deserialize an object from bytes."""
        ...


# TypeVar for generic state types with Serializable bound
StateT = TypeVar("StateT", bound=Serializable)


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
        active_features: Feature flags that the worker will use for this
            invocation. This is the intersection of client_features from the
            Invocation and the worker's supported features.

    """

    output_schema: pa.Schema
    max_processes: int
    invocation_id: bytes
    active_features: frozenset[str] = frozenset()

    def serialize_schema(self) -> pa.Schema:
        """Return the Arrow schema used when serializing this bind result.

        The schema defines the structure of the single-row RecordBatch that
        carries the bind result over IPC. Subclasses override this to add
        additional fields (e.g., cardinality estimates).

        Returns:
            Arrow schema with fields for each serialized attribute.

        """
        fields: list[pa.Field[Any]] = [
            pa.field("output_schema", pa.binary(), nullable=False),
            pa.field("max_processes", pa.int64(), nullable=True),
            pa.field("invocation_id", pa.binary(), nullable=True),
            pa.field("active_features", pa.list_(pa.utf8()), nullable=False),
        ]
        return pa.schema(fields)

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
            "active_features": list(self.active_features),
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
        return vgi.ipc_utils.serialize_record_batch(bind_result_batch)


class FunctionInitInput:
    """Input sent to initialize global state for a Function.

    This is the base init input class for functions that don't require
    any initialization data (like scalar functions). It serializes to
    an empty single-row batch.
    """

    def serialize(self) -> bytes:
        """Serialize FunctionInitInput to bytes.

        Creates a single-row batch with an empty schema. The batch must have
        exactly 1 row so that deserialize can access row 0.
        """
        # Create a batch with 1 row using a struct array approach
        struct_array: pa.StructArray = pa.array([{}], type=pa.struct([]))  # type: ignore[assignment]
        batch = pa.RecordBatch.from_struct_array(struct_array)
        return vgi.ipc_utils.serialize_record_batch(batch)

    @classmethod
    def deserialize(cls, _batch: pa.RecordBatch) -> Self:
        """Deserialize FunctionInitInput from a RecordBatch.

        Args:
            _batch: RecordBatch (unused - FunctionInitInput has no fields).

        Returns:
            New FunctionInitInput instance.

        """
        return cls()

    @classmethod
    def deserialize_bytes(cls, data: bytes) -> Self:
        """Deserialize FunctionInitInput from bytes.

        Args:
            data: Serialized bytes.

        Returns:
            New FunctionInitInput instance.

        """
        batch = vgi.ipc_utils.deserialize_record_batch(data)
        return cls.deserialize(batch)


class Function[T: FunctionInitInput](ABC, MetadataMixin):
    """Base class for all VGI functions.

    Functions are instantiated with Invocation describing the invocation,
    then queried for execution hints (max_processes, invocation_id).

    Subclasses can define a nested Meta class to provide metadata:

        class MyFunction(TableInOutFunction):
            class Meta:
                name = "my_function"
                description = "Does something useful"
                max_workers = 4
                categories = ["transform"]

            count = Arg[int](0, doc="Number of iterations")

    Available Meta attributes:
        name: Function name for registration (default: class name to snake_case)
        description: Human-readable description (default: docstring first line)
        max_workers: Maximum parallel workers (default: unlimited)
        categories: Classification tags
        examples: List of SQL examples
        See vgi.metadata for all available attributes.

    Attributes:
        invocation: The Invocation containing function name, arguments, and schema.
        logger: Structured logger for function diagnostics.

    For distributed functions that need to share state across workers:
    - Use store_state() to persist worker state during GeneratorExit
    - Use collect_states() in finalize() to gather all worker states

    See Also:
        vgi.scalar_function.Function: Scalar 1:1 row transforms.
        vgi.table_function.Function: Table functions with cardinality hints.
        vgi.table_in_out_function.Function: Table functions that transform input tables.
        vgi.metadata: Metadata documentation for functions.

    """

    storage: ClassVar[FunctionStorage] = FunctionStorageSqlite()

    # Cache for resolved metadata
    _metadata_cache: ClassVar[ResolvedMetadata | None] = None

    # The unique identifier for init data in storage. Set by initialize_global_state()
    # or load_global_state(). Used to correlate parallel workers and for state storage.
    execution_identifier: bytes | None = None

    # This is the init data that may be been read.
    # Type inferred from generic parameter via _get_init_input_type()
    init_input: T | None = None

    # Cache for _get_init_input_type per class
    _init_input_type_cache: ClassVar[dict[type, type["FunctionInitInput"]]] = {}

    @classmethod
    def _get_init_input_type(cls) -> type["FunctionInitInput"]:
        """Get the InitInput type from the generic parameter.

        Walks the MRO to find Function[T] and extracts T.
        Result is cached per class.
        """
        if cls in cls._init_input_type_cache:
            return cls._init_input_type_cache[cls]

        for base in cls.__mro__:
            if hasattr(base, "__orig_bases__"):
                for orig_base in base.__orig_bases__:
                    origin = get_origin(orig_base)
                    if origin is not None and origin.__name__ == "Function":
                        args = get_args(orig_base)
                        if args:
                            init_type = cast(type["FunctionInitInput"], args[0])
                            cls._init_input_type_cache[cls] = init_type
                            return init_type

        # Fallback to base type if not found
        cls._init_input_type_cache[cls] = FunctionInitInput
        return FunctionInitInput

    def __init__(
        self,
        *,
        invocation: "Invocation",
        logger: structlog.stdlib.BoundLogger,
    ):
        """Initialize the function with invocation data and logger.

        Args:
            invocation: Complete invocation request including function name,
                arguments, and input schema.
            logger: Structured logger for function diagnostics.

        """
        self.invocation = invocation
        self.logger = logger
        self._validate_type_bounds()

    def _validate_type_bounds(self) -> None:
        """Validate type bounds for Arg[AnyArrow] arguments.

        Iterates over all Arg descriptors in the class hierarchy and validates
        that any Arg[AnyArrow] with type_bound specified has a column type that
        satisfies the predicate(s).

        Only applies to Arg[AnyArrow] with type_bound specified.
        Skips validation if no input_schema (e.g., table generators).
        """
        if self.invocation.input_schema is None:
            return

        from vgi.arguments import AnyArrow, Arg

        for klass in type(self).__mro__:
            for name, attr in vars(klass).items():
                if not isinstance(attr, Arg):
                    continue
                # Only validate AnyArrow arguments with type_bound
                if getattr(attr, "_type_param", None) is not AnyArrow:
                    continue
                if attr.type_bound is None:
                    continue

                # Get column name from resolved argument
                value = getattr(self, name)
                column_name = value.value if hasattr(value, "value") else value

                # Look up field and validate against type_bound
                field = self.invocation.input_schema.field(column_name)
                attr.validate_type_bound(field.type)

    @property
    def max_processes(self) -> int:
        """Maximum number of parallel processes this function can utilize.

        This property checks Meta.max_workers first. If not defined, returns
        the default of 99999 (effectively unlimited).

        To limit parallelism, define max_workers in your Meta class:

            class MyFunction(TableInOutFunction):
                class Meta:
                    max_workers = 1  # Single-threaded aggregation

        Returns:
            Maximum parallel processes.

        """
        meta = self.get_metadata()
        if meta.max_workers is not None:
            return meta.max_workers
        return DEFAULT_MAX_WORKERS

    def create_invocation_id(self) -> bytes:
        """Return unique identifier for this function invocation.

        When max_processes > 1, this ID correlates multiple parallel workers
        processing the same logical function call.

        Returns:
            Unique bytes identifier. Default is a random UUID.

        """
        return uuid.uuid4().bytes

    @property
    @abstractmethod
    def output_schema(self) -> pa.Schema:
        """Return the output schema (must be implemented by subclass)."""
        ...

    def store_state(self, state: Serializable) -> None:
        """Store this worker's state for later collection.

        Call this method during GeneratorExit handling in process() to persist
        intermediate state (e.g., partial aggregations) that will be collected
        by the primary worker during finalization.

        The state is keyed by (execution_identifier, process_id), so calling this
        multiple times from the same process will overwrite the previous state.

        Args:
            state: A Serializable object to store.

        Raises:
            ValueError: If execution_identifier has not been set.

        Example:
            def process(self, batch: pa.RecordBatch) -> OutputGenerator:
                _ = yield None
                try:
                    while True:
                        # accumulate state...
                        batch = yield None
                        if batch is None:
                            break
                except GeneratorExit:
                    self.store_state(MyState(accumulated_data))
                    raise

        """
        if self.execution_identifier is None:
            raise ExecutionIdentifierError(
                "Cannot store state: execution_identifier is not set. "
                "Call initialize_global_state() or load_global_state() first."
            )
        self.storage.worker_put(
            self.execution_identifier,
            os.getpid(),
            state.serialize(),
        )

    def collect_states(self, state_class: type[StateT]) -> list[StateT]:
        """Collect and delete all worker states for this invocation.

        Call this method in finalize() to gather states from all workers
        that participated in processing. The states are atomically fetched
        and deleted from storage.

        Args:
            state_class: The class to use for deserializing states. Must have
                a deserialize(bytes) classmethod.

        Returns:
            List of deserialized state objects from all workers.

        Raises:
            ValueError: If execution_identifier has not been set.

        Example:
            def finalize(self) -> OutputGenerator:
                _ = yield None
                states = self.collect_states(MyState)
                combined = combine_states(states)
                yield Output(combined.to_batch())

        """
        if self.execution_identifier is None:
            raise ExecutionIdentifierError(
                "Cannot collect states: execution_identifier is not set. "
                "Call initialize_global_state() or load_global_state() first."
            )
        state_bytes_list = self.storage.worker_collect(self.execution_identifier)
        return [state_class.deserialize(data) for data in state_bytes_list]

    def enqueue_work(self, work_items: list[bytes]) -> int:
        """Add work items to the queue for this invocation (low-level bytes API).

        Call this during initialization (initialize_global_state or setup) to populate
        the work queue that workers will pull from during process().

        For a typed alternative, see enqueue_work_items().

        Args:
            work_items: List of opaque bytes representing work items.
                The function is responsible for serializing/deserializing
                these bytes as needed.

        Returns:
            Number of items enqueued.

        Raises:
            ExecutionIdentifierError: If execution_identifier has not been set.

        Example:
            def initialize_global_state(self, init_input: pa.RecordBatch) -> InitResult:
                result = super().initialize_global_state(init_input)
                # Create work items (e.g., ranges to process)
                work_items = [struct.pack(">QQ", start, end) for start, end in ranges]
                self.enqueue_work(work_items)
                return result

        """
        if self.execution_identifier is None:
            raise ExecutionIdentifierError(
                "Cannot enqueue work: execution_identifier is not set. "
                "Call enqueue_work() after initialize_global_state() has completed."
            )
        return self.storage.queue_push(self.execution_identifier, work_items)

    def enqueue_work_items(self, work_items: Sequence[Serializable]) -> int:
        """Add typed work items to the queue for this invocation.

        This is a typed convenience method that handles serialization automatically.
        Work items must implement the Serializable protocol.

        Args:
            work_items: List of Serializable objects to enqueue.

        Returns:
            Number of items enqueued.

        Raises:
            ExecutionIdentifierError: If execution_identifier has not been set.

        Example:
            @dataclass
            class FileRange:
                path: str
                start: int
                end: int

                def serialize(self) -> bytes:
                    return pickle.dumps((self.path, self.start, self.end))

                @classmethod
                def deserialize(cls, data: bytes) -> Self:
                    path, start, end = pickle.loads(data)
                    return cls(path, start, end)

            def setup(self):
                ranges = [FileRange("a.csv", 0, 1000), FileRange("b.csv", 0, 500)]
                self.enqueue_work_items(ranges)

        """
        return self.enqueue_work([item.serialize() for item in work_items])

    def dequeue_work(self) -> bytes | None:
        """Claim and return the next work item from the queue (low-level bytes API).

        Each call atomically claims one item from the queue. Returns None
        when the queue is empty (all work has been claimed).

        Multiple workers can safely call this concurrently - each item
        will be returned to exactly one worker.

        For a typed alternative, see dequeue_work_item().

        Returns:
            Opaque bytes representing a work item, or None if queue is empty.

        Raises:
            ExecutionIdentifierError: If execution_identifier has not been set.

        Example:
            def process(self) -> OutputGenerator:
                while True:
                    work_data = self.dequeue_work()
                    if work_data is None:
                        break  # Queue empty, done
                    start, end = struct.unpack(">QQ", work_data)
                    # Generate output for this range...
                    yield Output(batch)

        """
        if self.execution_identifier is None:
            raise ExecutionIdentifierError(
                "Cannot dequeue work: execution_identifier is not set. "
                "Call initialize_global_state() or load_global_state() first."
            )
        return self.storage.queue_pop(self.execution_identifier)

    def dequeue_work_item(self, item_class: type[StateT]) -> StateT | None:
        """Claim and deserialize the next work item from the queue.

        This is a typed convenience method that handles deserialization automatically.
        The item_class must implement the Serializable protocol (have a deserialize
        classmethod).

        Each call atomically claims one item from the queue. Returns None
        when the queue is empty (all work has been claimed).

        Multiple workers can safely call this concurrently - each item
        will be returned to exactly one worker.

        Args:
            item_class: The class to use for deserializing the work item.
                Must have a deserialize(bytes) classmethod.

        Returns:
            Deserialized work item, or None if queue is empty.

        Raises:
            ExecutionIdentifierError: If execution_identifier has not been set.

        Example:
            @dataclass
            class FileRange:
                path: str
                start: int
                end: int

                def serialize(self) -> bytes:
                    return pickle.dumps((self.path, self.start, self.end))

                @classmethod
                def deserialize(cls, data: bytes) -> Self:
                    path, start, end = pickle.loads(data)
                    return cls(path, start, end)

            def process(self) -> OutputGenerator:
                while item := self.dequeue_work_item(FileRange):
                    # item is FileRange, fully typed
                    yield Output(self.process_range(item.path, item.start, item.end))

        """
        data = self.dequeue_work()
        if data is None:
            return None
        return item_class.deserialize(data)

    @final
    @cached_property
    def empty_output_batch(self) -> pa.RecordBatch:
        """Return an empty batch conforming to output_schema. Cached."""
        output_schema = self.output_schema
        return pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in output_schema],
            schema=output_schema,
        )

    @final
    def _validate_output_schema(self, batch: pa.RecordBatch) -> None:
        """Validate that a batch conforms to the expected output schema."""
        if batch.schema != self.output_schema:
            raise SchemaValidationError(
                "Output batch schema does not match expected output_schema.",
                expected=self.output_schema,
                actual=batch.schema,
                context=f"output from {type(self).__name__}",
            )

    @final
    def _should_terminate(self, result: OutputComplete) -> bool:
        """Check if processing should terminate due to an exception."""
        return (
            result.log_message is not None
            and result.log_message.level == vgi.log.Level.EXCEPTION
        )

    @final
    def _create_error_output(self, exception: Exception) -> OutputComplete:
        """Create an OutputComplete with error message from exception."""
        return OutputComplete(
            batch=self.empty_output_batch,
            log_message=vgi.log.Message.from_exception(exception),
        )

    @property
    def input_schema(self) -> pa.Schema:
        """Return the input schema from the invocation.

        This property is available for functions that receive input batches
        (ScalarFunction, TableInOutFunction). For TableFunctionGenerator,
        the invocation.input_schema is None.

        Raises:
            ValueError: If invocation.input_schema is None.

        """
        if self.invocation.input_schema is None:
            raise ValueError(
                "input_schema is not available for this function type. "
                "TableFunctionGenerator does not receive input batches."
            )
        return self.invocation.input_schema

    @property
    def settings(self) -> dict[str, str]:
        """Return all settings passed to this function.

        Settings are passed during the bind phase via Invocation.settings.
        Functions can declare required settings in Meta.required_settings.

        Returns:
            Dictionary of setting name to value. Empty dict if no settings.

        Example:
            tz = self.settings.get("TimeZone", "UTC")

        """
        return dict(self.invocation.settings or {})

    def get_setting(self, name: str, default: str | None = None) -> str | None:
        """Get a specific DuckDB setting value.

        Args:
            name: DuckDB setting name (e.g., "TimeZone", "threads").
            default: Value to return if setting is not present.

        Returns:
            Setting value as string, or default if not present.

        Example:
            tz = self.get_setting("TimeZone", "UTC")
            threads = self.get_setting("threads")

        """
        if self.invocation.settings is None:
            return default
        return self.invocation.settings.get(name, default)

    @final
    def _validate_input_schema(self, batch: pa.RecordBatch) -> None:
        """Validate that a batch conforms to the expected input schema."""
        if batch.schema != self.input_schema:
            raise SchemaValidationError(
                "Input batch schema does not match expected input_schema.",
                expected=self.input_schema,
                actual=batch.schema,
                context=f"input to {type(self).__name__}",
            )

    def initialize_global_state(self, init_input: pa.RecordBatch) -> InitResult:
        """Perform a new init call and store it in the storage."""
        init_type = self._get_init_input_type()
        self.init_input = cast(T, init_type.deserialize(init_input))
        assert self.init_input is not None
        self.execution_identifier = self.storage.global_put(self.init_input.serialize())
        return InitResult(self.execution_identifier)

    def load_global_state(self, init_input: InitResult) -> None:
        """Retrieve and store init data from the storage."""
        if init_input.global_execution_identifier is None:
            raise ExecutionIdentifierError(
                "Cannot retrieve init: global_execution_identifier is None. "
                "Ensure initialize_global_state() returns a valid InitResult."
            )
        self.execution_identifier = init_input.global_execution_identifier
        init_type = self._get_init_input_type()
        raw_bytes = self.storage.global_get(self.execution_identifier)
        self.init_input = cast(T, init_type.deserialize_bytes(raw_bytes))

    def setup(self) -> None:
        """Acquire resources before processing starts.

        Override to acquire resources like database connections, file handles,
        or external service clients. Called after init_input is available.

        Available at this point:
            - self.init_input: The init data (FunctionInitInput or
              TableFunctionInitInput)
            - self.execution_identifier: Storage key for distributed state
            - self.invocation: The complete invocation request

        """
        pass

    def teardown(self) -> None:
        """Release resources after processing completes.

        Always called, even if an error occurred during processing.

        """
        pass
