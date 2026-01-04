"""Invocation data structures for VGI function calls.

This module defines the classes used for function invocation requests
in the VGI protocol.

Classes:
    InvocationType: Enum distinguishing scalar vs table invocation types.
    InitResult: Result from global initialization phase.
    Invocation: Complete function invocation request.

"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from enum import Enum
from typing import TYPE_CHECKING, ClassVar

import pyarrow as pa

import vgi.ipc_utils
from vgi.arguments import Arguments

if TYPE_CHECKING:
    pass

__all__ = [
    "InitResult",
    "Invocation",
    "InvocationType",
]


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


@dataclass(frozen=True, slots=True)
class InitResult:
    """Result from the global initialization phase of a function.

    When a function supports parallel execution (max_processes > 1), the first
    worker runs perform_init() which returns a InitResult. This result
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
        """Return Arrow schema used when serializing InitResult.

        Returns:
            Arrow schema with fields for each serialized attribute.

        """
        return pa.schema(
            [
                pa.field(self._IDENTIFIER_FIELD_NAME, pa.binary(), nullable=True),
            ]
        )

    def serialize(self) -> bytes:
        """Serialize InitResult to an Arrow RecordBatch.

        Returns:
            RecordBatch containing serialized InitResult fields.

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
    def deserialize(cls, data: pa.RecordBatch) -> InitResult:
        """Deserialize InitResult from an Arrow RecordBatch.

        Args:
          data: RecordBatch containing serialized InitResult fields.

        Returns:
          Deserialized InitResult instance.

        """
        first_row = vgi.ipc_utils.validate_single_row_batch(
            data, "InitResult", required_fields=[cls._IDENTIFIER_FIELD_NAME]
        )
        return InitResult(
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

    global_init_identifier: InitResult | None = None
    arguments: Arguments = Arguments()
    client_features: frozenset[str] = frozenset()
    attach_id: bytes | None = None

    def with_global_init_identifier(
        self, global_init_identifier: InitResult
    ) -> Invocation:
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
                    InitResult._IDENTIFIER_FIELD_NAME: (
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
                        InitResult._IDENTIFIER_FIELD_NAME,
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
    def deserialize(data: pa.RecordBatch) -> Invocation:
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

        # Parse global_init_identifier - only create InitResult if field exists
        # and has a non-None value
        global_init_identifier = None
        if InitResult._IDENTIFIER_FIELD_NAME in data.schema.names:
            identifier_value = first_row[InitResult._IDENTIFIER_FIELD_NAME]
            if identifier_value is not None:
                global_init_identifier = InitResult(identifier_value)

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
