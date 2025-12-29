"""IPC utility functions for Arrow message reading and writing.

This module provides helper functions for common IPC patterns used in the
VGI protocol, reducing code duplication between client and worker.

"""

from typing import Any

import pyarrow as pa
from pyarrow import ipc


class IPCError(Exception):
    """Error during IPC message reading or writing."""


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
