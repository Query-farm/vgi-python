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
import sqlite3
import uuid
from dataclasses import dataclass, replace
from enum import Enum
from functools import cached_property
from typing import (
    Any,
    ClassVar,
    Protocol,
    Self,
    TypeVar,
    final,
)

import pyarrow as pa
import structlog

import vgi.ipc_utils
from vgi.arguments import Arg, Arguments, ArgumentValidationError
from vgi.log import Level, Message
from vgi.metadata import MetadataMixin, ResolvedMetadata

__all__ = [
    "Arg",
    "ArgumentValidationError",
    "Arguments",
    "Function",
    "InvocationType",
    "GlobalInitResult",
    "Level",
    "Message",
    "OutputSpec",
    "Invocation",
    "SchemaValidationError",
    "Serializable",
    "SqliteInitStorage",
    "SqliteWorkerStateStorage",
]


class SchemaValidationError(Exception):
    """Raised when a batch schema doesn't match the expected schema.

    This error is raised by the framework during input/output validation.
    It indicates a programming error where a batch doesn't conform to the
    declared schema.

    The error message includes detailed information about what differs:
    - Missing fields (in expected but not in actual)
    - Extra fields (in actual but not in expected)
    - Type mismatches (same field name, different types)
    - Field order differences

    Attributes:
        expected: The expected Arrow schema.
        actual: The actual Arrow schema that was received.
        context: Description of where the validation occurred.

    """

    def __init__(
        self,
        message: str,
        *,
        expected: "pa.Schema | None" = None,
        actual: "pa.Schema | None" = None,
        context: str = "",
    ) -> None:
        """Initialize with schema comparison details.

        Args:
            message: Base error message.
            expected: The expected Arrow schema.
            actual: The actual Arrow schema.
            context: Where the error occurred (e.g., "output from transform()").

        """
        self.expected = expected
        self.actual = actual
        self.context = context

        if expected is not None and actual is not None:
            full_message = self._build_detailed_message(message, expected, actual)
        else:
            full_message = message

        super().__init__(full_message)

    def _build_detailed_message(
        self, base_message: str, expected: "pa.Schema", actual: "pa.Schema"
    ) -> str:
        """Build a detailed message showing exactly what differs."""
        lines = [base_message, ""]

        if self.context:
            lines.append(f"  Context: {self.context}")
            lines.append("")

        # Build field maps for comparison
        expected_fields = {f.name: f for f in expected}
        actual_fields = {f.name: f for f in actual}

        expected_names = set(expected_fields.keys())
        actual_names = set(actual_fields.keys())

        # Find differences
        missing = expected_names - actual_names
        extra = actual_names - expected_names
        common = expected_names & actual_names

        # Check for type mismatches in common fields
        type_mismatches = []
        for name in common:
            exp_field = expected_fields[name]
            act_field = actual_fields[name]
            if exp_field.type != act_field.type:
                type_mismatches.append((name, exp_field.type, act_field.type))
            elif exp_field.nullable != act_field.nullable:
                exp_null = "nullable" if exp_field.nullable else "non-nullable"
                act_null = "nullable" if act_field.nullable else "non-nullable"
                type_mismatches.append((name, exp_null, act_null))

        # Check for order differences (only if names match but order differs)
        order_differs = False
        if not missing and not extra and not type_mismatches:
            expected_order = [f.name for f in expected]
            actual_order = [f.name for f in actual]
            if expected_order != actual_order:
                order_differs = True

        # Report missing fields
        if missing:
            lines.append("  Missing fields (expected but not found):")
            for name in sorted(missing):
                field = expected_fields[name]
                lines.append(f"    - {name}: {field.type}")

        # Report extra fields
        if extra:
            lines.append("  Extra fields (found but not expected):")
            for name in sorted(extra):
                field = actual_fields[name]
                lines.append(f"    - {name}: {field.type}")

        # Report type mismatches
        if type_mismatches:
            lines.append("  Type mismatches:")
            for name, exp_type, act_type in type_mismatches:
                lines.append(f"    - {name}: expected {exp_type}, got {act_type}")

        # Report order differences
        if order_differs:
            lines.append("  Field order differs:")
            lines.append(f"    Expected: {[f.name for f in expected]}")
            lines.append(f"    Actual:   {[f.name for f in actual]}")

        # Summary of schemas
        lines.append("")
        lines.append("  Expected schema:")
        for field in expected:
            nullable = " (nullable)" if field.nullable else ""
            lines.append(f"    {field.name}: {field.type}{nullable}")

        lines.append("  Actual schema:")
        for field in actual:
            nullable = " (nullable)" if field.nullable else ""
            lines.append(f"    {field.name}: {field.type}{nullable}")

        return "\n".join(lines)


class InvocationType(Enum):
    """Type of VGI invocation for protocol dispatch.

    Used by the client to determine the correct init data format to send
    to the worker. Scalar functions use FunctionInitInput (no projection),
    while table functions use TableFunctionInitInput (with projection support).

    Note: This is distinct from vgi.metadata.FunctionType which is used for
    DuckDB catalog registration and includes AGGREGATE.

    Attributes:
        SCALAR: Scalar function that transforms input batches to single-column output.
        TABLE: Table function (either generator or table-in-out) that produces
            multi-column output.

    """

    SCALAR = "scalar"
    TABLE = "table"


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
        return vgi.ipc_utils.serialize_record_batch(batch)

    @classmethod
    def deserialize(cls, data: pa.RecordBatch) -> "GlobalInitResult":
        """Deserialize GlobalInitResult from an Arrow RecordBatch.

        Args:
          data: RecordBatch containing serialized GlobalInitResult fields.

        Returns:
          Deserialized GlobalInitResult instance.

        """
        first_row = vgi.ipc_utils.validate_single_row_batch(
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
    functions), the function type, and identifiers for logging and correlation.

    This is serialized to Arrow IPC format and sent as the first message when
    the client connects to a worker subprocess.

    Attributes:
        function_name: Name of the function to invoke, must exist in worker registry.
        input_schema: Arrow schema of input data. Required for table-in-out and
            scalar functions that process input batches. None for table functions
            that generate output without input.
        function_type: Type of function being invoked (SCALAR or TABLE). Used by
            the client to determine the correct init data format to send.
        correlation_id: String identifier for logging and correlation purposes.
        invocation_id: Unique bytes identifying this function binding. Used to
            correlate multiple parallel workers processing the same logical call.
        global_init_identifier: Optional result from global initialization phase.
        arguments: Positional and named arguments passed to the function.
        client_features: Feature flags supported by the client. The worker will
            respond with active_features in OutputSpec indicating which features
            will be used for this invocation.
        attach_id: Optional unique identifier for the DuckDB database attachment.
            When VGI is used from an attached database, this allows tracing calls
            back to that specific attachment. None when not using attached databases.

    Example:
        invocation = Invocation(
            function_name="sum_columns",
            input_schema=pa.schema([pa.field("col1", pa.int64())]),
            function_type=InvocationType.TABLE,
            correlation_id="request-123",
            invocation_id=None,  # Set by worker after binding
            arguments=Arguments(positional=("col1", "col2")),
        )

    """

    function_name: str
    input_schema: pa.Schema | None
    function_type: InvocationType

    correlation_id: str
    # The unique identifier for the call, typically this may be a uuid.
    invocation_id: bytes | None

    global_init_identifier: GlobalInitResult | None = None
    arguments: Arguments = Arguments()
    client_features: frozenset[str] = frozenset()
    attach_id: bytes | None = None

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
                    "input_schema": (
                        self.input_schema.serialize().to_pybytes()
                        if self.input_schema
                        else None
                    ),
                    "function_type": self.function_type.value,
                    "invocation_id": self.invocation_id,
                    "correlation_id": self.correlation_id,
                    GlobalInitResult._IDENTIFIER_FIELD_NAME: (
                        self.global_init_identifier.global_init_identifier
                        if self.global_init_identifier
                        else None
                    ),
                    "client_features": list(self.client_features),
                    "attach_id": self.attach_id,
                }
            ],
            schema=pa.schema(
                [
                    pa.field("function_name", pa.string(), nullable=False),
                    pa.field("arguments", args_struct_type, nullable=True),
                    pa.field("input_schema", pa.binary(), nullable=True),
                    pa.field("function_type", pa.string(), nullable=False),
                    pa.field("invocation_id", pa.binary(), nullable=True),
                    pa.field("correlation_id", pa.string(), nullable=False),
                    pa.field(
                        GlobalInitResult._IDENTIFIER_FIELD_NAME,
                        pa.binary(),
                        nullable=True,
                    ),
                    pa.field("client_features", pa.list_(pa.utf8()), nullable=False),
                    pa.field("attach_id", pa.binary(), nullable=True),
                ]
            ),
        )
        return vgi.ipc_utils.serialize_record_batch(batch)

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
            "input_schema",
            "function_type",
            "invocation_id",
            "correlation_id",
        ]
        first_row = vgi.ipc_utils.validate_single_row_batch(
            data, "Invocation", required_fields=required_fields
        )

        input_schema = None
        if first_row["input_schema"] is not None:
            input_schema = pa.ipc.read_schema(pa.py_buffer(first_row["input_schema"]))

        # Parse function_type from string value
        function_type = InvocationType(first_row["function_type"])

        # Parse global_init_identifier - only create GlobalInitResult if field exists
        # and has a non-None value
        global_init_identifier = None
        if GlobalInitResult._IDENTIFIER_FIELD_NAME in data.schema.names:
            identifier_value = first_row[GlobalInitResult._IDENTIFIER_FIELD_NAME]
            if identifier_value is not None:
                global_init_identifier = GlobalInitResult(identifier_value)

        # Parse client_features - default to empty set for backward compatibility
        client_features: frozenset[str] = frozenset()
        if "client_features" in data.schema.names:
            features_list = first_row.get("client_features")
            if features_list is not None:
                client_features = frozenset(features_list)

        # Parse attach_id - optional field for database attachment tracking
        attach_id: bytes | None = None
        if "attach_id" in data.schema.names:
            attach_id = first_row.get("attach_id")

        return Invocation(
            function_name=first_row["function_name"],
            input_schema=input_schema,
            function_type=function_type,
            arguments=Arguments.decode(data.column("arguments")[0]),
            invocation_id=first_row["invocation_id"],
            correlation_id=first_row["correlation_id"],
            global_init_identifier=global_init_identifier,
            client_features=client_features,
            attach_id=attach_id,
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
        """Create the worker_state and work_queue tables if they don't exist."""
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
            conn.execute("""
                CREATE TABLE IF NOT EXISTS work_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    invocation_id BLOB NOT NULL,
                    work_item BLOB NOT NULL,
                    created_at REAL DEFAULT (julianday('now'))
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_work_queue_invocation
                ON work_queue(invocation_id)
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

    def enqueue_work(self, invocation_id: bytes, work_items: list[bytes]) -> int:
        """Add work items to the queue for an invocation.

        Args:
            invocation_id: Unique identifier for the function invocation.
            work_items: List of serialized work item bytes (opaque to storage).

        Returns:
            Number of items enqueued.

        """
        if not work_items:
            return 0
        conn = self._connect()
        try:
            conn.executemany(
                """
                INSERT INTO work_queue (invocation_id, work_item)
                VALUES (?, ?)
                """,
                [(invocation_id, item) for item in work_items],
            )
            conn.commit()
            return len(work_items)
        finally:
            conn.close()

    def dequeue_work(self, invocation_id: bytes) -> bytes | None:
        """Atomically claim and delete one work item from the queue.

        Returns None if the queue is empty.

        Args:
            invocation_id: Unique identifier for the function invocation.

        Returns:
            Serialized work item bytes, or None if queue is empty.

        """
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                DELETE FROM work_queue
                WHERE id = (
                    SELECT id FROM work_queue
                    WHERE invocation_id = ?
                    LIMIT 1
                )
                RETURNING work_item
                """,
                (invocation_id,),
            )
            row = cursor.fetchone()
            conn.commit()
            return row[0] if row else None
        finally:
            conn.close()

    def cleanup_queue(self, invocation_id: bytes) -> int:
        """Delete all remaining work items for an invocation.

        Args:
            invocation_id: Unique identifier for the function invocation.

        Returns:
            Number of items deleted.

        """
        conn = self._connect()
        try:
            cursor = conn.execute(
                "DELETE FROM work_queue WHERE invocation_id = ?",
                (invocation_id,),
            )
            conn.commit()
            return cursor.rowcount
        finally:
            conn.close()


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


class Function[T: FunctionInitInput](MetadataMixin):
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
        vgi.table_function.Function: Adds cardinality hints.
        vgi.table_in_out_function.Function: Full streaming implementation.
        vgi.metadata: Complete metadata documentation.

    """

    init_storage: ClassVar[SqliteInitStorage] = SqliteInitStorage()
    state_storage: ClassVar[SqliteWorkerStateStorage] = SqliteWorkerStateStorage()

    # Cache for resolved metadata
    _metadata_cache: ClassVar[ResolvedMetadata | None] = None

    # The unique identifier for init data in storage. Set by perform_init()
    # or retrieve_init(). Used to correlate parallel workers and for state storage.
    init_identifier: bytes | None = None

    # This is the init data that may be been read.
    InitDataCls: type[T]
    init_data: T | None = None

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
            raise ValueError(
                "init_identifier must be set before storing state. "
                "This is typically set automatically during perform_init() or "
                "retrieve_init(). Ensure your function calls super().perform_init() "
                "in perform_init(), or that the worker correctly calls "
                "retrieve_init() for secondary workers."
            )
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
            raise ValueError(
                "init_identifier must be set before collecting states. "
                "This is typically set automatically during perform_init() or "
                "retrieve_init(). Ensure your function calls super().perform_init() "
                "in perform_init(), or that the worker correctly calls "
                "retrieve_init() for secondary workers."
            )
        state_bytes_list = self.state_storage.collect_and_delete(self.init_identifier)
        return [state_class.deserialize(data) for data in state_bytes_list]

    def enqueue_work(self, work_items: list[bytes]) -> int:
        """Add work items to the queue for this invocation.

        Call this during initialization (perform_init or setup) to populate
        the work queue that workers will pull from during process().

        Args:
            work_items: List of opaque bytes representing work items.
                The function is responsible for serializing/deserializing
                these bytes as needed.

        Returns:
            Number of items enqueued.

        Raises:
            ValueError: If init_identifier has not been set.

        Example:
            def perform_init(self, init_input: pa.RecordBatch) -> GlobalInitResult:
                result = super().perform_init(init_input)
                # Create work items (e.g., ranges to process)
                work_items = [struct.pack(">QQ", start, end) for start, end in ranges]
                self.enqueue_work(work_items)
                return result

        """
        if self.init_identifier is None:
            raise ValueError(
                "init_identifier must be set before enqueuing work. "
                "Call enqueue_work() after perform_init() has completed, typically "
                "at the end of your perform_init() override or in setup()."
            )
        return self.state_storage.enqueue_work(self.init_identifier, work_items)

    def dequeue_work(self) -> bytes | None:
        """Claim and return the next work item from the queue.

        Each call atomically claims one item from the queue. Returns None
        when the queue is empty (all work has been claimed).

        Multiple workers can safely call this concurrently - each item
        will be returned to exactly one worker.

        Returns:
            Opaque bytes representing a work item, or None if queue is empty.

        Raises:
            ValueError: If init_identifier has not been set.

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
        if self.init_identifier is None:
            raise ValueError(
                "init_identifier must be set before dequeuing work. "
                "This is typically set automatically during perform_init() or "
                "retrieve_init(). Ensure your function calls super().perform_init() "
                "in perform_init(), or that the worker correctly calls "
                "retrieve_init() for secondary workers."
            )
        return self.state_storage.dequeue_work(self.init_identifier)

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

    def perform_init(self, init_input: pa.RecordBatch) -> GlobalInitResult:
        """Perform a new init call and store it in the storage."""
        self.init_data = self.InitDataCls.deserialize(init_input)
        assert self.init_data is not None
        self.init_identifier = self.init_storage.create(self.init_data.serialize())
        return GlobalInitResult(self.init_identifier)

    def retrieve_init(self, init_input: GlobalInitResult) -> None:
        """Retrieve and store init data from the storage."""
        if init_input.global_init_identifier is None:
            raise ValueError(
                "global_init_identifier is required but was None. "
                "This indicates the GlobalInitResult was not properly initialized. "
                "Ensure perform_init() returns a GlobalInitResult with a valid "
                "identifier."
            )
        self.init_identifier = init_input.global_init_identifier
        self.init_data = self.InitDataCls.deserialize_bytes(
            self.init_storage.get(self.init_identifier)
        )

    def setup(self) -> None:
        """Acquire resources before processing starts.

        Override to acquire resources like database connections, file handles,
        or external service clients. Called after init_data is available.

        Available at this point:
            - self.init_data: The init data (FunctionInitInput or
              TableFunctionInitInput)
            - self.init_identifier: Storage key for distributed state
            - self.invocation: The complete invocation request

        """
        pass

    def teardown(self) -> None:
        """Release resources after processing completes.

        Always called, even if an error occurred during processing.

        """
        pass
