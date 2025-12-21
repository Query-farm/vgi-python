"""Core data structures for VGI function bind results.

This module defines the foundational classes used to describe function outputs
during the bind phase of the VGI protocol. When a function is bound, it returns
a BindResult that describes the output schema, parallelization hints, and
optional cardinality estimates.

Classes:
    CardinalityInfo: Cardinality hints for query optimization.
    BindResult: Base result from binding a function.
    TableFunctionBindResult: Extended result with cardinality info for table functions.

The bind result is serialized to Arrow IPC format for transmission between
the client and worker processes.
"""

from dataclasses import dataclass
from typing import Any

import pyarrow as pa

__all__ = ["BindResult"]


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

        bind_result_bytes = (
            bind_result_batch.schema.serialize().to_pybytes()
            + bind_result_batch.serialize().to_pybytes()
        )
        return bind_result_bytes
