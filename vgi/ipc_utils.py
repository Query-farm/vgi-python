"""IPC utility functions for Arrow message reading and writing.

This module provides helper functions for common IPC patterns used in the
VGI protocol, reducing code duplication between client and worker.

"""

from typing import Any, cast

import pyarrow as pa
from pyarrow import ipc


class IPCError(Exception):
    """Error during IPC message reading or writing."""


def serialize_record_batch(batch: pa.RecordBatch) -> bytes:
    """Serialize a RecordBatch to bytes using Arrow IPC stream format.

    Args:
        batch: The RecordBatch to serialize.

    Returns:
        Bytes containing the serialized RecordBatch.

    """
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return cast(bytes, sink.getvalue().to_pybytes())


def deserialize_record_batch(data: bytes) -> pa.RecordBatch:
    """Deserialize bytes back to a RecordBatch.

    Args:
        data: Bytes containing a serialized RecordBatch in Arrow IPC stream format.

    Returns:
        The deserialized RecordBatch.

    """
    reader = ipc.open_stream(pa.BufferReader(data))
    return reader.read_next_batch()


def read_ipc_batch(
    stream: Any,
    context: str = "batch",
) -> pa.RecordBatch:
    """Read a schema + record batch pair from a stream.

    Reads IPC messages manually (not via ipc.open_stream) to avoid PyArrow
    closing the underlying pipe when the stream context exits.

    Args:
        stream: Stream to read from (must support binary reads, e.g., stdin pipe,
            BufferedReader). Type is Any to accommodate runtime reassignment
            of stdin/stdout to binary mode.
        context: Description for error messages (e.g., "invocation", "init_data").

    Returns:
        The deserialized RecordBatch.

    Raises:
        IPCError: If unexpected message types are received.

    """
    msg = ipc.read_message(stream)
    if msg.type != "schema":
        raise IPCError(f"Expected schema message for {context}, got {msg.type}")
    schema = ipc.read_schema(msg)

    msg = ipc.read_message(stream)
    if msg.type != "record batch":
        raise IPCError(f"Expected record batch for {context}, got {msg.type}")
    return ipc.read_record_batch(msg, schema)
