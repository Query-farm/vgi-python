"""IPC utility functions for Arrow message reading and writing.

This module provides helper functions for common IPC patterns used in the
VGI protocol, reducing code duplication between client and worker.

KEY FUNCTIONS
-------------
serialize_record_batch(batch) : Serialize RecordBatch to bytes
deserialize_record_batch(data) : Deserialize bytes to RecordBatch
read_single_record_batch(stream, context) : Read schema + batch from stream
validate_single_row_batch(batch, class_name, required_fields)
    : Validate batch has exactly one row and return as dict

KEY CLASSES
-----------
RecordBatchState : Wrapper for RecordBatch implementing Serializable protocol.
    Use this in distributed functions for storing/collecting state across workers.

IPCError : Exception raised on IPC communication errors

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


def deserialize_record_batch(data: bytes) -> pa.RecordBatch:
    """Deserialize bytes back to a RecordBatch.

    Args:
        data: Bytes containing a serialized RecordBatch in Arrow IPC stream format.

    Returns:
        The deserialized RecordBatch.

    Raises:
        IPCError: If more than a single batch is found, or no batches are found.

    """
    with ipc.open_stream(pa.BufferReader(data)) as reader:
        for batch in reader:
            if _IPC_DEBUG:
                _get_ipc_log().debug(
                    "ipc_read",
                    num_rows=batch.num_rows,
                    schema=_schema_to_dict(batch.schema),
                    nbytes=len(data),
                )
            return batch
    raise IPCError("No RecordBatch found in provided data")


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


def validate_single_row_batch(
    data: pa.RecordBatch,
    class_name: str,
    required_fields: list[str] | None = None,
) -> dict[str, Any]:
    """Validate a RecordBatch has exactly one row and return it as a dict.

    Args:
        data: The RecordBatch to validate.
        class_name: Name of the class being deserialized (for error messages).
        required_fields: Optional list of field names that must be present.

    Returns:
        The first (and only) row as a dictionary.

    Raises:
        ValueError: If the batch is empty, has multiple rows, or is missing
            required fields.

    """
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
            raise ValueError(
                f"Missing fields in {class_name} RecordBatch: {missing}. "
                f"Found: {sorted(found_fields)}"
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
        return cls(batch=deserialize_record_batch(data))
