# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Secret type descriptor for declarative worker secret type definitions.

This module provides the SecretTypeSpec class for defining secret types
that are registered with DuckDB's SecretManager during ATTACH.
"""

from dataclasses import dataclass
from typing import Any, ClassVar, Self, cast

import pyarrow as pa
from vgi_rpc.utils import serialize_record_batch_bytes

__all__ = [
    "SecretTypeSpec",
]


@dataclass(frozen=True)
class SecretTypeSpec:
    """Specification for a custom secret type registered at ATTACH.

    Defines the secret type name, description, and parameter schema.
    The schema is a standard Arrow schema where each field represents a
    secret parameter (key name -> value type). Fields that should be
    redacted in SHOW SECRETS are marked with {"redact": "true"} in
    their Arrow field metadata.

    Attributes:
        name: The secret type name (e.g., "vgi_example").
        description: Human-readable description.
        schema: Arrow schema defining the secret's key-value parameters.
        ARROW_SCHEMA: Arrow IPC schema used to (de)serialize this spec over the wire.

    Example:
        SecretTypeSpec(
            name="vgi_example",
            description="Example VGI secret for testing",
            schema=pa.schema([
                pa.field("secret_string", pa.string(), metadata={"redact": "true"}),
                pa.field("api_key", pa.string(), metadata={"redact": "true"}),
                pa.field("port", pa.int32()),
                pa.field("use_ssl", pa.bool_()),
                pa.field("timeout", pa.float64()),
            ]),
        )

    """

    name: str
    description: str
    schema: pa.Schema

    ARROW_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("name", pa.string(), nullable=False),
            pa.field("description", pa.string(), nullable=False),
            pa.field("parameters_schema", pa.binary(), nullable=False),
        ]  # type: ignore[arg-type]  # PyArrow field metadata typing limitation
    )

    def to_row_dict(self) -> dict[str, Any]:
        """Build the wire row: one key per column of :data:`ARROW_SCHEMA`.

        Split out of :meth:`serialize` for the same reason as
        :meth:`vgi.catalog._descriptor_spec._SpecBase.to_row_dict` — a key the
        schema does not declare is silently dropped and a column this dict omits
        is silently null, so the row has to be inspectable for the parity test
        to have anything to check.
        """
        return {
            "name": self.name,
            "description": self.description,
            # The parameters schema carries the per-field `redact` metadata.
            "parameters_schema": self.schema.serialize().to_pybytes(),
        }

    def serialize(self) -> bytes:
        """Serialize to Arrow IPC bytes."""
        batch = pa.RecordBatch.from_pylist(
            [self.to_row_dict()],
            schema=self.ARROW_SCHEMA,
        )
        return serialize_record_batch_bytes(batch)

    @classmethod
    def deserialize(cls, batch: pa.RecordBatch) -> Self:
        """Deserialize from Arrow RecordBatch."""
        from vgi_rpc.utils import _validate_single_row_batch

        row = _validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=["name", "description", "parameters_schema"],
        )
        # Deserialize the parameters schema from IPC bytes
        parameters_schema = pa.ipc.read_schema(pa.py_buffer(cast(bytes, row["parameters_schema"])))

        return cls(
            name=cast(str, row["name"]),
            description=cast(str, row["description"]),
            schema=parameters_schema,
        )
