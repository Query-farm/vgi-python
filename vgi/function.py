"""Core data structures for VGI function calls and bind results.

This module defines the foundational classes used during function binding
in the VGI protocol. When a client invokes a function, it sends Invocation
describing the function name, arguments, and input schema. The worker
returns an OutputSpec describing the output schema and execution hints.

Classes:
    Arguments: Container for positional and named function arguments.
    Invocation: Complete function invocation request (name, args, schema).
    OutputSpec: Result from binding a function (output schema, etc).

The Invocation and OutputSpec are serialized to Arrow IPC format
for transmission between client and worker processes.

See Also:
    vgi.log: LogLevel and LogMessage for function diagnostics.
    vgi.table_function: Extended bind results with cardinality hints.
    vgi.table_in_out_function: Streaming table functions built on these primitives.

"""

import os
import sqlite3
import uuid
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, Self, TypeVar

import pyarrow as pa
import structlog

import vgi.util
from vgi.arguments import Arg, Arguments, ArgumentValidationError
from vgi.log import Level, Message
from vgi.metadata import MetadataMixin, ResolvedMetadata

if TYPE_CHECKING:
    pass

# Protocol version as (major, minor) tuple.
# Major version changes indicate breaking changes requiring code updates.
# Minor version changes are backward-compatible additions.
PROTOCOL_VERSION = (1, 0)

__all__ = [
    "Arg",
    "ArgumentValidationError",
    "Arguments",
    "Function",
    "GlobalInitResult",
    "Level",
    "Message",
    "OutputSpec",
    "PROTOCOL_VERSION",
    "ProtocolVersionError",
    "Invocation",
    "Serializable",
    "SqliteInitStorage",
    "SqliteWorkerStateStorage",
    "negotiate_protocol_version",
]


class ProtocolVersionError(Exception):
    """Raised when protocol version negotiation fails."""


def negotiate_protocol_version(
    client_version: tuple[int, int],
    worker_version: tuple[int, int] = PROTOCOL_VERSION,
) -> tuple[int, int]:
    """Negotiate the protocol version between client and worker.

    The negotiated version is the highest version both sides support.
    Major versions must match; minor version is the minimum of both.

    Args:
        client_version: Protocol version tuple (major, minor) from client.
        worker_version: Protocol version tuple (major, minor) the worker supports.

    Returns:
        The negotiated protocol version tuple (major, minor).

    Raises:
        ProtocolVersionError: If major versions are incompatible.

    """
    client_major, client_minor = client_version
    worker_major, worker_minor = worker_version

    if client_major != worker_major:
        raise ProtocolVersionError(
            f"Protocol version mismatch: client uses major version {client_major}, "
            f"worker uses major version {worker_major}. "
            f"Major versions must match."
        )

    # Use the minimum minor version both sides support
    negotiated_minor = min(client_minor, worker_minor)
    return (client_major, negotiated_minor)


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
class Invocation:
    """Complete function invocation request sent from client to worker.

    Invocation encapsulates all information needed to bind and execute a function:
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
        protocol_version: Protocol version tuple (major, minor) that the client
            supports. The worker will respond with its version in OutputSpec.

    Example:
        invocation = Invocation(
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
    protocol_version: tuple[int, int] = PROTOCOL_VERSION

    def with_global_init_identifier(
        self, global_init_identifier: GlobalInitResult
    ) -> "Invocation":
        """Return a new Invocation with the given global_init_identifier."""
        return replace(self, global_init_identifier=global_init_identifier)

    def serialize(self) -> bytes:
        """Serialize Invocation to an Arrow RecordBatch.

        Returns:
            RecordBatch containing serialized Invocation fields.

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
                    GlobalInitResult._IDENTIFIER_FIELD_NAME: (
                        self.global_init_identifier.global_init_identifier
                        if self.global_init_identifier
                        else None
                    ),
                    "protocol_version_major": self.protocol_version[0],
                    "protocol_version_minor": self.protocol_version[1],
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
                    pa.field(
                        GlobalInitResult._IDENTIFIER_FIELD_NAME,
                        pa.binary(),
                        nullable=True,
                    ),
                    pa.field("protocol_version_major", pa.int32(), nullable=False),
                    pa.field("protocol_version_minor", pa.int32(), nullable=False),
                ]
            ),
        )
        return vgi.util.recordbatch_to_bytes(batch)

    @staticmethod
    def deserialize(data: pa.RecordBatch) -> "Invocation":
        """Deserialize Invocation from an Arrow RecordBatch.

        Args:
          data: RecordBatch containing serialized Invocation fields.

        Returns:
          Deserialized Invocation instance.

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
            data, "Invocation", required_fields=required_fields
        )

        in_out_function_input_schema = None
        if first_row["in_out_function_input_schema"] is not None:
            in_out_function_input_schema = pa.ipc.read_schema(
                pa.py_buffer(first_row["in_out_function_input_schema"])
            )

        # Parse global_init_identifier - only create GlobalInitResult if field exists
        # and has a non-None value
        global_init_identifier = None
        if GlobalInitResult._IDENTIFIER_FIELD_NAME in data.schema.names:
            identifier_value = first_row[GlobalInitResult._IDENTIFIER_FIELD_NAME]
            if identifier_value is not None:
                global_init_identifier = GlobalInitResult(identifier_value)

        # Parse protocol version - default to (1, 0) for backward compatibility
        protocol_version = PROTOCOL_VERSION
        if "protocol_version_major" in data.schema.names:
            major = first_row["protocol_version_major"]
            minor = first_row.get("protocol_version_minor", 0)
            protocol_version = (major, minor)

        return Invocation(
            function_name=first_row["function_name"],
            arguments=Arguments.decode(data.column("arguments")[0]),
            in_out_function_input_schema=in_out_function_input_schema,
            invocation_id=first_row["invocation_id"],
            correlation_id=first_row["correlation_id"],
            global_init_identifier=global_init_identifier,
            protocol_version=protocol_version,
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
        protocol_version: Protocol version tuple (major, minor) that the worker
            will use. Must be <= the client's requested version.

    """

    output_schema: pa.Schema
    max_processes: int
    invocation_id: bytes
    protocol_version: tuple[int, int] = PROTOCOL_VERSION

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
                pa.field("protocol_version_major", pa.int32(), nullable=False),
                pa.field("protocol_version_minor", pa.int32(), nullable=False),
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
            "protocol_version_major": self.protocol_version[0],
            "protocol_version_minor": self.protocol_version[1],
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


def _get_default_db_path() -> str:
    """Return the default SQLite database path for VGI storage."""
    from pathlib import Path

    from platformdirs import user_state_dir

    state_dir = Path(user_state_dir("vgi"))
    state_dir.mkdir(parents=True, exist_ok=True)
    return str((state_dir / "vgi_storage.db").resolve())


class SqliteInitStorage:
    """SQLite-backed storage for init values shared across processes.

    This storage implementation uses SQLite with a well-known file location
    to allow multiple worker processes to share initialization state. This
    is necessary for distributed/parallel execution where workers run in
    separate subprocesses.

    The storage uses bytes-in-bytes-out, delegating serialization to callers.

    """

    def __init__(self, db_path: str | None = None) -> None:
        """Initialize SQLite storage.

        Args:
            db_path: Path to the SQLite database file. If None, uses a default
                location in the user's state directory.

        """
        self.db_path = db_path if db_path is not None else _get_default_db_path()
        self._ensure_table()

    def _connect(self) -> sqlite3.Connection:
        """Create a new database connection."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _ensure_table(self) -> None:
        """Create the storage table if it doesn't exist."""
        conn = self._connect()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS init_storage (
                    key BLOB PRIMARY KEY,
                    value BLOB NOT NULL,
                    created_at REAL DEFAULT (julianday('now'))
                )
            """)
            conn.commit()
        finally:
            conn.close()

    def create(self, value: bytes) -> bytes:
        """Store a value and return its unique key.

        Args:
            value: Serialized value bytes.

        Returns:
            Unique key for retrieving the value.

        """
        key = uuid.uuid4().bytes

        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO init_storage (key, value) VALUES (?, ?)",
                (key, value),
            )
            conn.commit()
        finally:
            conn.close()

        return key

    def get(self, key: bytes) -> bytes:
        """Retrieve a value by key, raising KeyError if not found.

        Args:
            key: Key returned from create().

        Returns:
            The stored value bytes.

        Raises:
            KeyError: If no value exists for this key.

        """
        conn = self._connect()
        try:
            cursor = conn.execute(
                "SELECT value FROM init_storage WHERE key = ?",
                (key,),
            )
            row = cursor.fetchone()
        finally:
            conn.close()

        if row is None:
            raise KeyError(f"Key {key.hex()} not found in SqliteInitStorage")

        value: bytes = row[0]
        return value

    def delete(self, key: bytes) -> None:
        """Delete a value by key if it exists."""
        conn = self._connect()
        try:
            conn.execute("DELETE FROM init_storage WHERE key = ?", (key,))
            conn.commit()
        finally:
            conn.close()

    def has(self, key: bytes) -> bool:
        """Check if a key exists in storage."""
        conn = self._connect()
        try:
            cursor = conn.execute(
                "SELECT 1 FROM init_storage WHERE key = ?",
                (key,),
            )
            return cursor.fetchone() is not None
        finally:
            conn.close()

    def cleanup_old_entries(self, max_age_days: float = 1.0) -> int:
        """Remove entries older than the specified age.

        Args:
            max_age_days: Maximum age in days for entries to keep.

        Returns:
            Number of entries deleted.

        """
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                DELETE FROM init_storage
                WHERE julianday('now') - created_at > ?
                """,
                (max_age_days,),
            )
            conn.commit()
            return int(cursor.rowcount)
        finally:
            conn.close()


class SqliteWorkerStateStorage:
    """SQLite storage for worker state in distributed functions.

    This storage allows distributed workers to persist their intermediate state
    (e.g., partial aggregations) which can later be collected by the primary
    worker during finalization.

    Each worker stores its state keyed by (invocation_id, process_id). The
    primary worker can then collect all states for a given invocation and
    delete them atomically.

    """

    def __init__(self, db_path: str | None = None) -> None:
        """Initialize SQLite worker state storage.

        Args:
            db_path: Path to the SQLite database file. If None, uses a default
                location in the user's state directory.

        """
        self.db_path = db_path if db_path is not None else _get_default_db_path()
        self._ensure_table()

    def _connect(self) -> sqlite3.Connection:
        """Create a new database connection."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _ensure_table(self) -> None:
        """Create the worker_state table if it doesn't exist."""
        conn = self._connect()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS worker_state (
                    invocation_id BLOB NOT NULL,
                    process_id INTEGER NOT NULL,
                    state_data BLOB NOT NULL,
                    created_at REAL DEFAULT (julianday('now')),
                    PRIMARY KEY (invocation_id, process_id)
                )
            """)
            conn.commit()
        finally:
            conn.close()

    def store(self, invocation_id: bytes, process_id: int, state_data: bytes) -> None:
        """Store or update state for a worker.

        If state already exists for this (invocation_id, process_id) pair,
        it will be replaced.

        Args:
            invocation_id: Unique identifier for the function invocation.
            process_id: Process ID of the worker storing the state.
            state_data: Serialized state bytes.

        """
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO worker_state
                (invocation_id, process_id, state_data, created_at)
                VALUES (?, ?, ?, julianday('now'))
                """,
                (invocation_id, process_id, state_data),
            )
            conn.commit()
        finally:
            conn.close()

    def collect_and_delete(self, invocation_id: bytes) -> list[bytes]:
        """Atomically fetch all states for an invocation and delete them.

        This is typically called by the primary worker during finalization
        to collect all worker states for aggregation.

        Args:
            invocation_id: Unique identifier for the function invocation.

        Returns:
            List of serialized state bytes from all workers.

        """
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                DELETE FROM worker_state
                WHERE invocation_id = ?
                RETURNING state_data
                """,
                (invocation_id,),
            )
            states = [row[0] for row in cursor.fetchall()]
            conn.commit()
            return states
        finally:
            conn.close()


class Function(MetadataMixin):
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

    For distributed functions that need to share state across workers:
    - Use store_state() to persist worker state during GeneratorExit
    - Use collect_states() in finalize() to gather all worker states

    See Also:
        vgi.table_function.Function: Adds cardinality hints.
        vgi.table_in_out_function.Function: Full streaming implementation.
        vgi.metadata: Complete metadata documentation.

    """

    init_storage: ClassVar[SqliteInitStorage] = SqliteInitStorage()
    state_storage: ClassVar[SqliteWorkerStateStorage] = SqliteWorkerStateStorage()

    # The unique identifier for init data in storage. Set by perform_init()
    # or retrieve_init(). Used to correlate parallel workers and for state storage.
    init_identifier: bytes | None = None

    # Cache for resolved metadata
    _metadata_cache: ClassVar[ResolvedMetadata | None] = None

    def __init__(self, *, logger: structlog.stdlib.BoundLogger):
        """Initialize the function with a logger.

        Args:
            logger: Structured logger for function diagnostics.

        """
        self.logger = logger

    def max_processes(self) -> int:
        """Return maximum number of parallel processes this function can utilize.

        This method checks Meta.max_workers first. If not defined, returns
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
        return 99999

    def create_invocation_id(self) -> bytes:
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

    def store_state(self, state: Serializable) -> None:
        """Store this worker's state for later collection.

        Call this method during GeneratorExit handling in process() to persist
        intermediate state (e.g., partial aggregations) that will be collected
        by the primary worker during finalization.

        The state is keyed by (init_identifier, process_id), so calling this
        multiple times from the same process will overwrite the previous state.

        Args:
            state: A Serializable object to store.

        Raises:
            ValueError: If init_identifier has not been set.

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
        if self.init_identifier is None:
            raise ValueError("init_identifier must be set before storing state")
        self.state_storage.store(
            self.init_identifier,
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
            ValueError: If init_identifier has not been set.

        Example:
            def finalize(self) -> OutputGenerator:
                _ = yield None
                states = self.collect_states(MyState)
                combined = combine_states(states)
                yield Output(combined.to_batch())

        """
        if self.init_identifier is None:
            raise ValueError("init_identifier must be set before collecting states")
        state_bytes_list = self.state_storage.collect_and_delete(self.init_identifier)
        return [state_class.deserialize(data) for data in state_bytes_list]
