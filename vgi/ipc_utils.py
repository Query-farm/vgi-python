"""IPC utility functions for Arrow message reading and writing.

This module provides helper functions for common IPC patterns used in the
VGI protocol, reducing code duplication between client and worker.

KEY FUNCTIONS
-------------
serialize_record_batch(batch) : Serialize RecordBatch to bytes
deserialize_record_batch(data) : Deserialize bytes to RecordBatch
read_ipc_batch(stream, context) : Read schema + batch from stream

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

from dataclasses import dataclass
from typing import Any, Self, cast

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
