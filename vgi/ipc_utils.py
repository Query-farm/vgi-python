"""IPC utility functions for Arrow message reading and writing.

This module provides helper functions for common IPC patterns used in the
VGI protocol, reducing code duplication between client and worker.

KEY FUNCTIONS
-------------
serialize_record_batch(batch, custom_metadata) : Serialize RecordBatch to bytes
deserialize_record_batch(data) : Deserialize bytes to (RecordBatch, metadata) tuple
read_single_record_batch(stream, context) : Read schema + batch from stream
validate_single_row_batch(batch, class_name, required_fields, custom_metadata,
    expected_protocol_state) : Validate batch and optionally verify protocol state
protocol_state_metadata(state) : Create metadata dict with protocol state
get_protocol_state(metadata) : Extract protocol state from metadata
merge_metadata(*metadata_dicts) : Merge multiple metadata dictionaries

KEY CLASSES
-----------
ProtocolState : Constants for protocol state names (INVOCATION, BIND_RESULT, etc.)

RecordBatchState : Wrapper for RecordBatch implementing Serializable protocol.
    Use this in distributed functions for storing/collecting state across workers.

IPCError : Exception raised on IPC communication errors

PROTOCOL STATE METADATA
-----------------------
All VGI protocol messages must include protocol state metadata to identify
the message type. This enables validation and helps debug synchronization issues.

Protocol states:
- INVOCATION: Client → Worker (function invocation request)
- BIND_RESULT: Worker → Client (OutputSpec with schema)
- INIT_INPUT: Client → Worker (initialization data)
- INIT_RESULT: Worker → Client (initialization result)
- DATA: Client → Worker (input data batches)
- OUTPUT: Worker → Client (output data batches)
- CATALOG_ARGS: Client → Worker (catalog operation arguments)
- CATALOG_RESULT: Worker → Client (catalog operation results)

Example - attaching protocol state when serializing:
    from vgi.ipc_utils import (
        ProtocolState, protocol_state_metadata, serialize_record_batch
    )

    metadata = protocol_state_metadata(ProtocolState.INVOCATION)
    data = serialize_record_batch(batch, custom_metadata=metadata)

Example - validating protocol state when deserializing:
    from vgi.ipc_utils import (
        ProtocolState, deserialize_record_batch, validate_single_row_batch
    )

    batch, metadata = deserialize_record_batch(data)
    row = validate_single_row_batch(
        batch, "Invocation",
        required_fields=["function_name"],
        custom_metadata=metadata,
        expected_protocol_state=ProtocolState.INVOCATION
    )

DISTRIBUTED STATE EXAMPLE
-------------------------
Store partial state when process() generator is closed:

    from vgi.ipc_utils import RecordBatchState

    def process(self, batch):
        _ = yield None
        partial_result = ...
        try:
            while True:
                # accumulate partial_result
                batch = yield None
                if batch is None:
                    break
        except GeneratorExit:
            state_batch = pa.RecordBatch.from_pydict({"sum": [total]})
            self.store_state(RecordBatchState(batch=state_batch))
            raise

    def finalize(self):
        _ = yield None
        states = self.collect_states(RecordBatchState)
        combined = pa.Table.from_batches([s.batch for s in states])
        yield Output(aggregate(combined))

See Also
--------
vgi.function.Serializable : Protocol that RecordBatchState implements
vgi.function.Function.store_state : Store state for distributed processing
vgi.function.Function.collect_states : Collect states from all workers

"""

import os
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Self

import pyarrow as pa
import structlog
from pyarrow import ipc

# IPC debug logging - enable with VGI_IPC_DEBUG=1
_IPC_DEBUG = os.environ.get("VGI_IPC_DEBUG", "").lower() in ("1", "true", "yes")
# IPC stats logging - enable with VGI_IPC_STATS=1 for aggregate stream stats
_IPC_STATS = os.environ.get("VGI_IPC_STATS", "").lower() in ("1", "true", "yes")
_ipc_log: structlog.stdlib.BoundLogger | None = None


def _get_ipc_log() -> structlog.stdlib.BoundLogger:
    """Get or create the IPC debug logger, configured to write to stderr."""
    global _ipc_log
    if _ipc_log is None:
        import sys

        # Configure structlog to write to stderr for IPC debugging
        structlog.configure(
            processors=[
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.dev.ConsoleRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(0),
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        )
        _ipc_log = structlog.get_logger().bind(component="ipc")
    return _ipc_log


def _schema_to_dict(schema: pa.Schema) -> dict[str, str]:
    """Convert Arrow schema to dict of {name: type} for logging."""
    return {field.name: str(field.type) for field in schema}


def _metadata_to_dict(
    metadata: pa.KeyValueMetadata | None,
) -> dict[str, str] | None:
    """Convert Arrow metadata to string dict for logging."""
    if metadata is None:
        return None
    return {
        k.decode() if isinstance(k, bytes) else k: v.decode()
        if isinstance(v, bytes)
        else v
        for k, v in metadata.items()
    }


class IPCError(Exception):
    """Error during IPC message reading or writing."""


# Protocol state metadata key
PROTOCOL_STATE_KEY = b"vgi.protocol_state"


class ProtocolState:
    """Protocol state names for IPC message validation.

    These constants identify the type of message being sent in the VGI protocol.
    Each message should include protocol state metadata so recipients can validate
    they're receiving the expected message type.

    Protocol flow for function invocation:
        1. Client sends: INVOCATION
        2. Worker sends: BIND_RESULT
        3. Client sends: INIT_INPUT
        4. Worker sends: INIT_RESULT
        5. Client sends: DATA (streaming input batches)
        6. Worker sends: OUTPUT (streaming output batches)

    Protocol flow for catalog operations:
        1. Client sends: INVOCATION (with catalog function type)
        2. Client sends: CATALOG_ARGS
        3. Worker sends: CATALOG_RESULT

    """

    INVOCATION = "invocation"
    BIND_RESULT = "bind_result"
    INIT_INPUT = "init_input"
    INIT_RESULT = "init_result"
    DATA = "data"
    OUTPUT = "output"
    CATALOG_ARGS = "catalog_args"
    CATALOG_RESULT = "catalog_result"


def protocol_state_metadata(state: str) -> pa.KeyValueMetadata:
    """Create metadata dict with protocol state indicator.

    Args:
        state: The protocol state name. Should be one of the ProtocolState constants.

    Returns:
        KeyValueMetadata with the protocol state.

    """
    return pa.KeyValueMetadata({PROTOCOL_STATE_KEY: state.encode()})


def merge_metadata(
    *metadata_dicts: pa.KeyValueMetadata | dict[bytes, bytes] | None,
) -> pa.KeyValueMetadata | None:
    """Merge multiple metadata dictionaries into one.

    Args:
        *metadata_dicts: Metadata dictionaries to merge. None values are skipped.

    Returns:
        Merged KeyValueMetadata, or None if all inputs were None/empty.

    """
    result: dict[bytes, bytes] = {}
    for md in metadata_dicts:
        if md is not None:
            for k, v in md.items():
                key = k if isinstance(k, bytes) else k.encode()
                val = v if isinstance(v, bytes) else v.encode()
                result[key] = val
    return pa.KeyValueMetadata(result) if result else None


def serialize_record_batch(
    batch: pa.RecordBatch, custom_metadata: pa.KeyValueMetadata | None = None
) -> bytes:
    """Serialize a RecordBatch to bytes in Arrow IPC stream format.

    Uses RecordBatchStreamWriter to produce a complete IPC stream with
    schema, batch, and end-of-stream marker.

    Args:
        batch: The RecordBatch to serialize.
        custom_metadata: Optional custom metadata to include in the stream schema.

    Returns:
        Complete Arrow IPC stream bytes including EOS marker.

    """
    buffer = BytesIO()
    with ipc.RecordBatchStreamWriter(buffer, batch.schema) as writer:
        writer.write_batch(batch, custom_metadata=custom_metadata)
    result = buffer.getvalue()

    if _IPC_DEBUG:
        _get_ipc_log().debug(
            "ipc_write",
            num_rows=batch.num_rows,
            schema=_schema_to_dict(batch.schema),
            metadata=_metadata_to_dict(custom_metadata),
            nbytes=len(result),
        )

    return result


def deserialize_record_batch(
    data: bytes,
) -> tuple[pa.RecordBatch, pa.KeyValueMetadata | None]:
    """Deserialize bytes back to a RecordBatch with custom metadata.

    Args:
        data: Bytes containing a serialized RecordBatch in Arrow IPC stream format.

    Returns:
        Tuple of (RecordBatch, custom_metadata). The custom_metadata may be None
        if no custom metadata was attached to the batch.

    Raises:
        IPCError: If more than a single batch is found, or no batches are found.

    """
    with ipc.open_stream(pa.BufferReader(data)) as reader:
        try:
            batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
        except StopIteration:
            raise IPCError("No RecordBatch found in provided data") from None

        if _IPC_DEBUG:
            _get_ipc_log().debug(
                "ipc_read",
                num_rows=batch.num_rows,
                schema=_schema_to_dict(batch.schema),
                metadata=_metadata_to_dict(custom_metadata),
                nbytes=len(data),
            )
        return batch, custom_metadata


def read_single_record_batch(
    stream: Any,
    context: str = "batch",
) -> tuple[pa.RecordBatch, pa.KeyValueMetadata | None]:
    """Read a single record batch from a stream.

    Args:
        stream: Stream to read from (must support binary reads, e.g., stdin pipe,
            BufferedReader). Type is Any to accommodate runtime reassignment
            of stdin/stdout to binary mode.
        context: Description for error messages (e.g., "invocation", "init_input").

    Returns:
        Tuple of (RecordBatch, custom_metadata). The custom_metadata may be None
        if no custom metadata was attached to the batch.

    Raises:
        IPCError: If more than a single batch is found, or no batches are found.

    """
    try:
        with ipc.open_stream(stream) as reader:
            try:
                batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
            except StopIteration:
                if _IPC_DEBUG:
                    _get_ipc_log().error(
                        "ipc_read: No record batch found in stream",
                        context=context,
                    )
                raise IPCError(f"No record batch found in {context} stream") from None

            try:
                reader.read_next_batch()
            except StopIteration:
                if _IPC_DEBUG:
                    _get_ipc_log().debug(
                        "ipc_read",
                        context=context,
                        num_rows=batch.num_rows,
                        schema=_schema_to_dict(batch.schema),
                        metadata=_metadata_to_dict(custom_metadata),
                    )
                return batch, custom_metadata

            if _IPC_DEBUG:
                _get_ipc_log().error(
                    "ipc_read: Multiple batches found in stream",
                    context=context,
                )
            raise IPCError(
                f"Expected single record batch in {context} stream, "
                f"but found multiple batches"
            )
    except Exception as e:
        raise IPCError(f"Error reading record batch from {context} stream: {e}") from e


def get_protocol_state(metadata: pa.KeyValueMetadata | None) -> str | None:
    """Extract the protocol state from batch metadata.

    Args:
        metadata: The batch's custom metadata.

    Returns:
        The protocol state string, or None if not present.

    """
    if metadata is None:
        return None
    value = metadata.get(PROTOCOL_STATE_KEY)
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else value


def validate_single_row_batch(
    data: pa.RecordBatch,
    class_name: str,
    required_fields: list[str] | None = None,
    custom_metadata: pa.KeyValueMetadata | None = None,
    expected_protocol_state: str | None = None,
) -> dict[str, Any]:
    """Validate a RecordBatch has exactly one row and return it as a dict.

    Args:
        data: The RecordBatch to validate.
        class_name: Name of the class being deserialized (for error messages).
        required_fields: Optional list of field names that must be present.
        custom_metadata: Optional custom metadata from the batch (for protocol
            state validation).
        expected_protocol_state: If provided, validate that the batch's protocol
            state matches this value.

    Returns:
        The first (and only) row as a dictionary.

    Raises:
        ValueError: If the batch is empty, has multiple rows, is missing
            required fields, or has wrong protocol state.

    """
    # Check protocol state first for better error messages
    if expected_protocol_state is not None:
        actual_state = get_protocol_state(custom_metadata)
        if actual_state is None:
            raise ValueError(
                f"Protocol state mismatch for {class_name}: "
                f"expected '{expected_protocol_state}', but no protocol state found. "
                f"Batch fields: {sorted(data.schema.names)}"
            )
        if actual_state != expected_protocol_state:
            raise ValueError(
                f"Protocol state mismatch for {class_name}: "
                f"expected '{expected_protocol_state}', got '{actual_state}'. "
                f"Batch fields: {sorted(data.schema.names)}"
            )

    if data.num_rows == 0:
        raise ValueError(f"Cannot deserialize {class_name} from empty RecordBatch")
    if data.num_rows > 1:
        raise ValueError(
            f"Expected single-row RecordBatch for {class_name} deserialization, "
            f"got {data.num_rows} rows"
        )

    first_row: dict[str, Any] = data.to_pylist()[0]

    if required_fields:
        found_fields = set(first_row.keys())
        missing = [f for f in required_fields if f not in found_fields]
        if missing:
            actual_state = get_protocol_state(custom_metadata)
            state_info = f" (protocol_state={actual_state})" if actual_state else ""
            raise ValueError(
                f"Missing fields in {class_name} RecordBatch: {missing}. "
                f"Found: {sorted(found_fields)}{state_info}"
            )

    return first_row


@dataclass
class RecordBatchState:
    """A RecordBatch wrapper implementing the Serializable protocol.

    This is a generic state container for distributed functions that need to
    store and collect RecordBatch data across workers.

    Example:
        def process(self, batch: pa.RecordBatch) -> OutputGenerator:
            _ = yield None
            try:
                while True:
                    # process batches...
                    batch = yield None
                    if batch is None:
                        break
            except GeneratorExit:
                self.store_state(RecordBatchState(batch=my_state_batch))
                raise

        def finalize(self) -> OutputGenerator:
            _ = yield None
            states = self.collect_states(RecordBatchState)
            table = pa.Table.from_batches([s.batch for s in states])
            # aggregate table...

    """

    batch: pa.RecordBatch

    def serialize(self) -> bytes:
        """Serialize the RecordBatch to bytes."""
        return serialize_record_batch(self.batch)

    @classmethod
    def deserialize(cls, data: bytes) -> Self:
        """Deserialize a RecordBatch from bytes."""
        batch, _ = deserialize_record_batch(data)
        return cls(batch=batch)
