# Copyright 2026 Query Farm LLC - https://query.farm

"""Tests for the canonical Filter v2 Arrow container builder."""

from __future__ import annotations

import json

import pyarrow as pa
import pytest

from vgi.filter_v2 import DUCKDB_SESSION_CONTEXT, EvaluationContext
from vgi.filter_v2_builder import (
    FilterPayload,
    artifact_payload,
    build_filter_batch,
    deserialize_filter_batch,
    serialize_filter_batch,
    type_payload,
    value_payload,
)


def _document() -> dict[str, object]:
    return {
        "encoding": "vgi.filters.v2",
        "semantics": "vgi.duckdb.standard.v1",
        "kind": "snapshot",
        "predicates": [],
    }


def test_builds_canonical_no_context_container() -> None:
    """The default container carries typed payloads and the no-context profile."""
    batch = build_filter_batch(
        _document(),
        [
            value_payload(0, 42, pa.int64()),
            type_payload(1, pa.timestamp("us", tz="UTC"), metadata={b"logical": b"timestamp"}),
            artifact_payload(2, b"artifact", pa.binary()),
        ],
    )

    assert batch.num_rows == 1
    assert batch.schema.names == ["filter_spec", "value_0", "type_1", "artifact_2"]
    assert json.loads(batch.column(0)[0].as_py()) == _document()
    assert batch.column(1)[0].as_py() == 42
    assert batch.column(2)[0].as_py() is None
    assert batch.schema.field("type_1").metadata == {b"logical": b"timestamp"}
    assert batch.column(3)[0].as_py() == b"artifact"
    assert batch.schema.metadata == {
        b"vgi_filter_encoding": b"vgi.filters.v2",
        b"vgi_filter_version": b"2",
        b"vgi_evaluation_context": b"vgi.none.v1",
    }


def test_builds_complete_duckdb_session_context() -> None:
    """Every DuckDB session setting and optional fingerprint is encoded."""
    context = EvaluationContext(
        profile=DUCKDB_SESSION_CONTEXT,
        time_zone="America/New_York",
        calendar="gregorian",
        default_collation="binary",
        ieee_floating_point_ops=False,
        integer_division=True,
        provider_fingerprint="duckdb:v1.5.5",
    )

    metadata = build_filter_batch(_document(), context=context).schema.metadata

    assert metadata == {
        b"vgi_filter_encoding": b"vgi.filters.v2",
        b"vgi_filter_version": b"2",
        b"vgi_evaluation_context": b"vgi.duckdb.session.v1",
        b"vgi_time_zone": b"America/New_York",
        b"vgi_calendar": b"gregorian",
        b"vgi_default_collation": b"binary",
        b"vgi_ieee_floating_point_ops": b"false",
        b"vgi_integer_division": b"true",
        b"vgi_context_provider_fingerprint": b"duckdb:v1.5.5",
    }


@pytest.mark.parametrize(
    "context,match",
    [
        (EvaluationContext(profile="vendor.unknown.v1"), "unknown evaluation-context profile"),
        (EvaluationContext(profile=DUCKDB_SESSION_CONTEXT), "incomplete DuckDB evaluation context"),
    ],
)
def test_rejects_invalid_context(context: EvaluationContext, match: str) -> None:
    """Unknown and incomplete context profiles cannot be emitted."""
    with pytest.raises(ValueError, match=match):
        build_filter_batch(_document(), context=context)


def test_rejects_duplicate_payload_names() -> None:
    """Payload fields have unique names and cannot replace filter_spec."""
    payload = value_payload(0, 1)
    with pytest.raises(ValueError, match="duplicate filter payload field"):
        build_filter_batch(_document(), [payload, payload])
    with pytest.raises(ValueError, match="duplicate filter payload field"):
        build_filter_batch(_document(), [FilterPayload(pa.field("filter_spec", pa.string()), pa.scalar("x"))])


def test_payload_helpers_preserve_arrow_scalars() -> None:
    """Callers may supply pre-typed Arrow scalars without type loss."""
    scalar = pa.scalar(7, type=pa.int16())
    assert value_payload(3, scalar) == FilterPayload(pa.field("value_3", pa.int16()), scalar)
    assert artifact_payload(4, scalar) == FilterPayload(pa.field("artifact_4", pa.int16()), scalar)


def test_ipc_round_trip_requires_exactly_one_batch() -> None:
    """The IPC helper accepts precisely the protocol's one-batch shape."""
    batch = build_filter_batch(_document())
    assert deserialize_filter_batch(serialize_filter_batch(batch)).equals(batch)

    for batch_count in (0, 2):
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            for _ in range(batch_count):
                writer.write_batch(batch)
        with pytest.raises(ValueError, match=f"exactly one batch, found {batch_count}"):
            deserialize_filter_batch(sink.getvalue().to_pybytes())
