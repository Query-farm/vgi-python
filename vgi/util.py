"""Utility functions for Arrow IPC serialization.

This module provides low-level utilities for serializing and deserializing
Arrow RecordBatches. Most users should use the higher-level functions in
vgi.ipc_utils instead.

KEY FUNCTIONS
-------------
recordbatch_to_bytes(batch) : Serialize RecordBatch to schema + data bytes
bytes_to_recordbatch(data) : Deserialize bytes back to RecordBatch
validate_single_row_batch(data, class_name, required_fields)
    : Validate batch has exactly one row and return as dict

See Also
--------
vgi.ipc_utils : Higher-level IPC utilities for client/worker communication

"""

from typing import Any

import pyarrow as pa


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


def recordbatch_to_bytes(batch: pa.RecordBatch) -> bytes:
    """Serialize a RecordBatch to bytes (schema + data).

    Args:
        batch: The RecordBatch to serialize.

    Returns:
        Concatenated schema and batch bytes for IPC transmission.

    """
    result: bytes = (
        batch.schema.serialize().to_pybytes() + batch.serialize().to_pybytes()
    )
    return result


def bytes_to_recordbatch(data: bytes) -> pa.RecordBatch:
    """Deserialize a RecordBatch from bytes (schema + data).

    Args:
        data: Bytes produced by recordbatch_to_bytes().

    Returns:
        The deserialized RecordBatch.

    """
    reader = pa.ipc.open_stream(data)
    return reader.read_next_batch()
