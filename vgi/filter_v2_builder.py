# Copyright 2026 Query Farm LLC - https://query.farm

"""Builders for canonical VGI Filter Encoding v2 Arrow containers.

The protocol deliberately separates structural JSON from typed Arrow payloads.
These helpers provide one implementation for tests, clients, and the shared
cross-language conformance corpus instead of making every caller hand-build the
same one-row ``RecordBatch`` envelope.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from vgi.filter_v2 import (
    DUCKDB_SESSION_CONTEXT,
    FILTER_ENCODING,
    FILTER_VERSION,
    NO_EVALUATION_CONTEXT,
    EvaluationContext,
)


@dataclass(frozen=True, slots=True)
class FilterPayload:
    """One named, typed value carried beside ``filter_spec``."""

    field: pa.Field[Any]
    value: pa.Scalar[Any]


def value_payload(index: int, value: Any, data_type: pa.DataType | None = None) -> FilterPayload:
    """Build a ``value_N`` payload, preserving an explicitly supplied type."""
    if isinstance(value, pa.Scalar):
        scalar = value
    elif data_type is None:
        scalar = pa.scalar(value)
    else:
        scalar = pa.scalar(value, type=data_type)
    return FilterPayload(pa.field(f"value_{index}", scalar.type), scalar)


def type_payload(index: int, data_type: pa.DataType, *, metadata: Mapping[bytes, bytes] | None = None) -> FilterPayload:
    """Build a typed-NULL ``type_N`` cast-target payload."""
    field = pa.field(f"type_{index}", data_type, metadata=dict(metadata) if metadata is not None else None)
    return FilterPayload(field, pa.scalar(None, type=data_type))


def artifact_payload(index: int, value: Any, data_type: pa.DataType | None = None) -> FilterPayload:
    """Build an ``artifact_N`` payload for an algorithm-specific artifact."""
    if isinstance(value, pa.Scalar):
        scalar = value
    elif data_type is None:
        scalar = pa.scalar(value)
    else:
        scalar = pa.scalar(value, type=data_type)
    return FilterPayload(pa.field(f"artifact_{index}", scalar.type), scalar)


def _context_metadata(context: EvaluationContext) -> dict[bytes | str, bytes | str]:
    metadata: dict[bytes | str, bytes | str] = {
        b"vgi_filter_encoding": FILTER_ENCODING.encode(),
        b"vgi_filter_version": FILTER_VERSION.encode(),
        b"vgi_evaluation_context": context.profile.encode(),
    }
    if context.profile == NO_EVALUATION_CONTEXT:
        return metadata
    if context.profile != DUCKDB_SESSION_CONTEXT:
        raise ValueError(f"unknown evaluation-context profile {context.profile!r}")
    required = {
        "time_zone": context.time_zone,
        "calendar": context.calendar,
        "default_collation": context.default_collation,
        "ieee_floating_point_ops": context.ieee_floating_point_ops,
        "integer_division": context.integer_division,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        raise ValueError(f"incomplete DuckDB evaluation context: {', '.join(missing)}")
    metadata.update(
        {
            b"vgi_time_zone": str(context.time_zone).encode(),
            b"vgi_calendar": str(context.calendar).encode(),
            b"vgi_default_collation": str(context.default_collation).encode(),
            b"vgi_ieee_floating_point_ops": b"true" if context.ieee_floating_point_ops else b"false",
            b"vgi_integer_division": b"true" if context.integer_division else b"false",
        }
    )
    if context.provider_fingerprint is not None:
        metadata[b"vgi_context_provider_fingerprint"] = context.provider_fingerprint.encode()
    return metadata


def build_filter_batch(
    document: Mapping[str, object],
    payloads: Iterable[FilterPayload] = (),
    *,
    context: EvaluationContext | None = None,
) -> pa.RecordBatch:
    """Build the canonical one-row Arrow container for a v2 document."""
    context = context or EvaluationContext(profile=NO_EVALUATION_CONTEXT)
    payload_list = list(payloads)
    fields: list[pa.Field[Any]] = [pa.field("filter_spec", pa.string(), nullable=False)]
    arrays: list[pa.Array[Any]] = [
        pa.array([json.dumps(document, ensure_ascii=False, separators=(",", ":"))], type=pa.string())
    ]
    names = {"filter_spec"}
    for payload in payload_list:
        if payload.field.name in names:
            raise ValueError(f"duplicate filter payload field {payload.field.name!r}")
        names.add(payload.field.name)
        fields.append(payload.field)
        arrays.append(
            pa.array([payload.value], type=payload.field.type)
            if payload.value.is_valid
            else pa.nulls(1, type=payload.field.type)
        )
    return pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields, metadata=_context_metadata(context)))


def serialize_filter_batch(batch: pa.RecordBatch) -> bytes:
    """Serialize exactly one filter batch as an Arrow IPC stream."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def deserialize_filter_batch(data: bytes) -> pa.RecordBatch:
    """Read exactly one filter batch from an Arrow IPC stream."""
    with pa.ipc.open_stream(pa.py_buffer(data)) as reader:
        batches = list(reader)
    if len(batches) != 1:
        raise ValueError(f"filter IPC stream must contain exactly one batch, found {len(batches)}")
    return batches[0]
