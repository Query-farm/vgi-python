# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Shared SQL evaluation settings and bounded Arrow record envelopes."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Self

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.utils import IpcValidation

from vgi.typed_values import TypedValueBatch

MAX_SQL_RECORD_BYTES = 8 * 1024 * 1024


def _text(value: str, label: str, *, maximum: int = 4096, empty: bool = False) -> None:
    if not isinstance(value, str) or "\0" in value or (not empty and not value):
        raise ValueError(f"invalid {label}")
    if len(value.encode("utf-8")) > maximum:
        raise ValueError(f"{label} is too large")


class _SqlRecord(ArrowSerializableDataclass):
    @classmethod
    def deserialize_from_bytes(cls, data: bytes, ipc_validation: IpcValidation = IpcValidation.FULL) -> Self:
        """Bound the envelope before nested decoding and require one complete row."""
        if not isinstance(data, bytes) or not 0 < len(data) <= MAX_SQL_RECORD_BYTES:
            raise ValueError("invalid SQL record size")
        return cls.deserialize_from_batch(TypedValueBatch.deserialize_from_bytes(data).batch)

    @classmethod
    def deserialize_from_batch(
        cls,
        batch: pa.RecordBatch,
        custom_metadata: pa.KeyValueMetadata | None = None,
        *,
        ipc_validation: IpcValidation = IpcValidation.FULL,
    ) -> Self:
        """Validate full Arrow buffers, required fields and exact field definitions."""
        if batch.num_rows != 1 or not batch.schema.equals(cls.ARROW_SCHEMA, check_metadata=False):
            raise ValueError(f"{cls.__name__} requires one row with its complete schema")
        if batch.nbytes > MAX_SQL_RECORD_BYTES:
            raise ValueError("SQL record is too large")
        TypedValueBatch(batch)  # Includes required nested fields, not just Arrow buffer validity.
        return super().deserialize_from_batch(batch, custom_metadata, ipc_validation=IpcValidation.FULL)

    def serialize_to_bytes(self) -> bytes:
        """Enforce the same envelope bound for locally constructed requests."""
        if validate := getattr(self, "__post_init__", None):
            validate()
        result = super().serialize_to_bytes()
        if len(result) > MAX_SQL_RECORD_BYTES:
            raise ValueError("SQL record is too large")
        return result


@dataclass(frozen=True, kw_only=True)
class QueryEvaluationContext(_SqlRecord):
    """The complete versioned set of query-evaluation settings on the wire."""

    timezone: str = "UTC"
    collation: str = "binary"
    default_order: str = "asc"
    default_null_order: str = "nulls_last"
    preserve_insertion_order: bool = True

    def __post_init__(self) -> None:
        """Reject unknown enum values; providers certify supported contexts."""
        _text(self.timezone, "query timezone", maximum=256)
        _text(self.collation, "query collation", maximum=256)
        if self.default_order not in {"asc", "desc"}:
            raise ValueError("unknown default query order")
        if self.default_null_order not in {
            "nulls_first",
            "nulls_last",
            "nulls_first_on_asc_last_on_desc",
            "nulls_last_on_asc_first_on_desc",
        }:
            raise ValueError("unknown default NULL order")
        if type(self.preserve_insertion_order) is not bool:
            raise ValueError("preserve_insertion_order must be a boolean")

    def digest(self, encoding: str, semantic_profile: str) -> bytes:
        """Hash the documented length-prefixed UTF-8 fields, independently of IPC."""
        self.__post_init__()
        _text(encoding, "query encoding", maximum=256)
        _text(semantic_profile, "query profile", maximum=256)
        payload = bytearray(b"vgi.query.context.v1\0")
        for value in [
            encoding,
            semantic_profile,
            self.timezone,
            self.collation,
            self.default_order,
            self.default_null_order,
        ]:
            encoded = value.encode("utf-8")
            payload.extend(len(encoded).to_bytes(4, "big"))
            payload.extend(encoded)
        payload.append(int(self.preserve_insertion_order))
        return hashlib.sha256(payload).digest()
