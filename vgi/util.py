import pyarrow as pa


def recordbatch_to_bytes(batch: pa.RecordBatch) -> bytes:
    """Serialize GlobalInitResult to bytes.

    Returns:
        bytes: Serialized GlobalInitResult.
    """
    result: bytes = (
        batch.schema.serialize().to_pybytes() + batch.serialize().to_pybytes()
    )
    return result
