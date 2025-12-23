import pyarrow as pa


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
