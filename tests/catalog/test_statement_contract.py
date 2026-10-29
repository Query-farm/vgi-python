# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""The explicit mutation profile uses existing records without weakening reads."""

from dataclasses import replace
from typing import Any

import pyarrow as pa
import pytest

from vgi.catalog.query import (
    QUERY_ENCODING,
    QUERY_EXPLICIT,
    QUERY_STATEMENT,
    STATEMENT_RESULT_KEY,
    CatalogQueryPrepareRequest,
    CatalogQueryPrepareResult,
    ClientQueryCapabilities,
    QueryCapabilities,
    QueryEvaluationContext,
)
from vgi.query_parameters import QueryParameters

from .typing_helpers import present


def capability() -> QueryCapabilities:
    return QueryCapabilities(
        encoding=QUERY_ENCODING,
        semantic_profiles=[QUERY_EXPLICIT, QUERY_STATEMENT],
        execution_modes=["explicit", "statement"],
        backend_engine="duckdb",
        backend_version="qualified-fixture",
        capability_revision="statement-1",
        default_evaluation_context=QueryEvaluationContext(),
    )


def request() -> CatalogQueryPrepareRequest:
    return CatalogQueryPrepareRequest(
        attach_opaque_data=b"attachment",
        transaction_opaque_data=b"transaction",
        encoding=QUERY_ENCODING,
        semantic_profile=QUERY_STATEMENT,
        mode="statement",
        capability_revision="statement-1",
        query="DELETE FROM orders",
        parameter_schema=QueryParameters.empty().schema_bytes(),
        evaluation_context=QueryEvaluationContext(),
    )


def accepted(req: CatalogQueryPrepareRequest, schema: pa.Schema) -> CatalogQueryPrepareResult:
    return CatalogQueryPrepareResult(
        status="accepted",
        prepared_query_token=b"preparation",
        output_schema=schema.serialize().to_pybytes(),
        result_ordering="unspecified",
        context_digest=req.evaluation_context.digest(req.encoding, req.semantic_profile),
        expires_at_epoch_ms=2_000_000_000_000,
    )


def test_statements_require_explicit_offer_and_transactions() -> None:
    offered = ClientQueryCapabilities(
        encodings=[QUERY_ENCODING],
        semantic_profiles=[QUERY_EXPLICIT, QUERY_STATEMENT],
        engine_version="fixture",
    )
    assert present(capability().select(offered, supports_transactions=True)).execution_modes == [
        "explicit",
        "statement",
    ]
    assert present(capability().select(offered, supports_transactions=False)).execution_modes == ["explicit"]
    assert (
        capability().select(replace(offered, semantic_profiles=[QUERY_STATEMENT]), supports_transactions=False) is None
    )
    assert present(
        capability().select(replace(offered, semantic_profiles=[QUERY_EXPLICIT]), supports_transactions=True)
    ).execution_modes == ["explicit"]
    assert capability().select(None, supports_transactions=True) is None


def test_statement_context_owner_and_mode_are_not_inferred_from_sql() -> None:
    req = request()
    req.validate_agreement(capability())
    invalid_changes: list[dict[str, Any]] = [
        {"transaction_opaque_data": None},
        {"transaction_opaque_data": b""},
        {"semantic_profile": QUERY_EXPLICIT},
        {"mode": "explicit"},
    ]
    for changes in invalid_changes:
        with pytest.raises(ValueError):
            replace(req, **changes)
    with pytest.raises(ValueError, match="provider evaluation context"):
        replace(req, evaluation_context=QueryEvaluationContext(timezone="Etc/UTC")).validate_agreement(capability())
    with pytest.raises(ValueError, match="agreement"):
        req.validate_agreement(replace(capability(), semantic_profiles=[QUERY_EXPLICIT], execution_modes=["explicit"]))


@pytest.mark.parametrize("kind", [b"command", b"rows"])
def test_statement_preparation_uses_existing_record_and_schema_carriers(kind: bytes) -> None:
    req = request()
    field = pa.field("affected_rows", pa.int64()) if kind == b"command" else pa.field("id", pa.int32())
    result = accepted(req, pa.schema([field], metadata={STATEMENT_RESULT_KEY: kind}))
    result.validate_request(req)
    assert CatalogQueryPrepareRequest.deserialize_from_bytes(req.serialize_to_bytes()) == req
    assert CatalogQueryPrepareResult.deserialize_from_bytes(result.serialize_to_bytes()) == result
    read_request = replace(req, mode="explicit", semantic_profile=QUERY_EXPLICIT)
    read_result = replace(result, context_digest=read_request.evaluation_context.digest(QUERY_ENCODING, QUERY_EXPLICIT))
    with pytest.raises(ValueError, match="statement profile"):
        read_result.validate_request(read_request)


@pytest.mark.parametrize(
    "schema",
    [
        pa.schema([pa.field("affected_rows", pa.int64())]),
        pa.schema([pa.field("affected_rows", pa.int64())], metadata={STATEMENT_RESULT_KEY: b"unknown"}),
        pa.schema([pa.field("count", pa.int64())], metadata={STATEMENT_RESULT_KEY: b"command"}),
        pa.schema([pa.field("affected_rows", pa.int32())], metadata={STATEMENT_RESULT_KEY: b"command"}),
        pa.schema([pa.field("affected_rows", pa.int64(), nullable=False)], metadata={STATEMENT_RESULT_KEY: b"command"}),
        pa.schema([], metadata={STATEMENT_RESULT_KEY: b"rows"}),
    ],
)
def test_invalid_statement_results_reject_during_preparation(schema: pa.Schema) -> None:
    req = request()
    with pytest.raises(ValueError, match="statement"):
        accepted(req, schema).validate_request(req)


@pytest.mark.parametrize("second", [b"command", b"rows"])
def test_duplicate_result_kind_metadata_rejects(second: bytes) -> None:
    schema = pa.schema(
        [pa.field("affected_rows", pa.int64())],
        metadata=pa.KeyValueMetadata([(STATEMENT_RESULT_KEY, b"command"), (STATEMENT_RESULT_KEY, second)]),  # type: ignore[arg-type]  # Arrow supports duplicate metadata pairs.
    )
    req = request()
    with pytest.raises(ValueError, match="metadata keys must be unique"):
        accepted(req, schema).validate_request(req)
