# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""Mode, negotiation, output and schema invariants before remote execution."""

from dataclasses import replace
from typing import Any

import pyarrow as pa
import pytest

from vgi.catalog.dependencies import CatalogObjectRef
from vgi.catalog.query import (
    QUERY_AUTOMATIC,
    QUERY_ENCODING,
    QUERY_EXPLICIT,
    CatalogQueryPrepareRequest,
    CatalogQueryPrepareResult,
    ClientQueryCapabilities,
    QueryCapabilities,
    QueryEvaluationContext,
    QueryObjectReference,
)
from vgi.query_parameters import QueryParameters

from .typing_helpers import present

OUTPUT = pa.schema([pa.field("result", pa.int32())]).serialize().to_pybytes()
PARAMETERS = QueryParameters.from_batch(
    pa.record_batch([pa.array([None], type=pa.int32())], names=["1"]), mode="positional"
)


def capability() -> QueryCapabilities:
    return QueryCapabilities(
        encoding=QUERY_ENCODING,
        semantic_profiles=[QUERY_EXPLICIT, QUERY_AUTOMATIC],
        execution_modes=["explicit", "automatic"],
        backend_engine="duckdb",
        backend_version="pinned-test-build",
        capability_revision="policy-1",
        default_evaluation_context=QueryEvaluationContext(),
    )


def request(
    *, expected_output_schema: bytes | None = None, required_ordering: str = "unspecified"
) -> CatalogQueryPrepareRequest:
    return CatalogQueryPrepareRequest(
        attach_opaque_data=b"attach",
        encoding=QUERY_ENCODING,
        semantic_profile=QUERY_EXPLICIT,
        mode="explicit",
        capability_revision="policy-1",
        query="SELECT $1 AS result",
        parameter_schema=PARAMETERS.schema_bytes(),
        evaluation_context=QueryEvaluationContext(),
        expected_output_schema=expected_output_schema,
        required_ordering=required_ordering,
    )


def result(
    req: CatalogQueryPrepareRequest | None = None, *, validated_catalog_version: int | None = None
) -> CatalogQueryPrepareResult:
    req = req or request()
    return CatalogQueryPrepareResult(
        status="accepted",
        prepared_query_token=b"prepared-recipe",
        output_schema=OUTPUT,
        result_ordering="query_order",
        context_digest=req.evaluation_context.digest(req.encoding, req.semantic_profile),
        expires_at_epoch_ms=2_000_000_000_000,
        validated_catalog_version=validated_catalog_version,
    )


def table_ref(path: list[str] | None = None) -> QueryObjectReference:
    return QueryObjectReference(
        object=CatalogObjectRef(kind="table", schema_path=path or ["a.b", "nested"], name="T"),
        expected_schema=OUTPUT,
    )


def automatic() -> CatalogQueryPrepareRequest:
    return replace(
        request(),
        mode="automatic",
        semantic_profile=QUERY_AUTOMATIC,
        transaction_opaque_data=b"tx",
        catalog_version=0,
        referenced_objects=[table_ref()],
        expected_output_schema=OUTPUT,
    )


def test_context_digest_golden_vector_is_independent_of_arrow_layout() -> None:
    context = QueryEvaluationContext()
    expected = bytes.fromhex("0d33dfb516d67c8096fb174f0ae08a04f028ad5669e4416cde2973a2dd352d34")
    assert context.digest(QUERY_ENCODING, QUERY_EXPLICIT) == expected
    copy = QueryEvaluationContext.deserialize_from_bytes(context.serialize_to_bytes())
    assert copy.digest(QUERY_ENCODING, QUERY_EXPLICIT) == expected
    for changed in [replace(context, preserve_insertion_order=False), replace(context, timezone="Etc/UTC")]:
        assert changed.digest(QUERY_ENCODING, QUERY_EXPLICIT) != expected
    assert context.digest(QUERY_ENCODING, QUERY_AUTOMATIC) != expected


def test_absent_empty_and_unknown_offers_select_no_query_execution() -> None:
    supported = capability()
    assert supported.select(None, supports_transactions=True) is None
    for encodings, profiles in [
        ([], []),
        (["other.sql.v1"], [QUERY_EXPLICIT]),
        ([QUERY_ENCODING], ["other.profile.v1"]),
    ]:
        offer = ClientQueryCapabilities(encodings=encodings, semantic_profiles=profiles, engine_version="client-build")
        assert supported.select(offer, supports_transactions=True) is None


def test_selection_limits_modes_and_size_without_inferring_engine_equivalence() -> None:
    supported = capability()
    offer = ClientQueryCapabilities(
        encodings=[QUERY_ENCODING],
        semantic_profiles=[QUERY_EXPLICIT, QUERY_AUTOMATIC],
        engine_version="client-build",
        max_query_bytes=20,
    )
    selected = supported.select(offer, supports_transactions=False)
    assert selected is not None
    assert selected.execution_modes == ["explicit"]
    assert selected.semantic_profiles == [QUERY_EXPLICIT]
    assert selected.max_query_bytes == 20
    supported = replace(supported, semantic_profiles=[QUERY_EXPLICIT], execution_modes=["explicit"])
    assert present(supported.select(offer, supports_transactions=True)).execution_modes == ["explicit"]
    automatic_only = replace(offer, semantic_profiles=[QUERY_AUTOMATIC])
    assert capability().select(automatic_only, supports_transactions=False) is None


@pytest.mark.parametrize(
    "change",
    [
        {"encoding": "unknown"},
        {"semantic_profiles": ["unknown"]},
        {"execution_modes": ["automatic"]},
        {"execution_modes": ["explicit", "automatic", "write"]},
        {"capability_revision": ""},
        {"max_query_bytes": 0},
        {"max_query_bytes": True},
        {"backend_version": "bad\0version"},
    ],
)
def test_malformed_selected_capability_rejects(change: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        replace(capability(), **change)


def test_preparation_validates_the_immutable_attachment_contract() -> None:
    selected = capability()
    source = request()
    source.validate_agreement(selected)
    for changed in [
        replace(selected, capability_revision="changed"),
        replace(selected, max_query_bytes=4),
        replace(selected, default_evaluation_context=QueryEvaluationContext(timezone="Etc/UTC")),
        replace(selected, semantic_profiles=[QUERY_AUTOMATIC], execution_modes=["automatic"]),
    ]:
        with pytest.raises(ValueError):
            source.validate_agreement(changed)


def test_nested_records_keep_raw_identities_in_struct_fields() -> None:
    source = automatic()
    value = CatalogQueryPrepareRequest.deserialize_from_bytes(source.serialize_to_bytes())
    assert value == source
    assert value.referenced_objects[0].object.schema_path == ["a.b", "nested"]
    field = source.ARROW_SCHEMA.field("referenced_objects")
    assert pa.types.is_struct(field.type.value_type)
    assert pa.types.is_struct(field.type.value_type.field("object").type)
    assert value.catalog_version == 0  # Unknown metadata is not "unchanged".


@pytest.mark.parametrize(
    "change",
    [
        {"transaction_opaque_data": None},
        {"transaction_opaque_data": b""},
        {"catalog_version": None},
        {"catalog_version": -1},
        {"expected_output_schema": None},
        {"referenced_objects": []},
        {"referenced_objects": [table_ref(), table_ref()]},
        {
            "referenced_objects": [
                replace(table_ref(), object=CatalogObjectRef(kind="view", schema_path=["main"], name="v"))
            ]
        },
    ],
)
def test_automatic_context_requires_complete_snapshot_and_output_contract(change: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        replace(automatic(), **change)


@pytest.mark.parametrize(
    "change",
    [
        {"attach_opaque_data": b""},
        {"query": ""},
        {"query": "SELECT\0 1"},
        {"query": "é" * (1024 * 1024)},
        {"parameter_schema": PARAMETERS.serialize_to_bytes()},
        {"referenced_objects": [table_ref()]},
        {"expected_output_schema": OUTPUT + b"extra"},
        {"required_ordering": "ordered_maybe"},
        {"mode": "automatic"},
    ],
)
def test_invalid_preparation_rejects_before_provider_binding(change: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        replace(request(), **change)


def test_decline_is_exclusive_and_future_reason_codes_remain_pre_execution() -> None:
    declined = CatalogQueryPrepareResult(
        status="declined", reason_code="future_placement_policy", reason_detail="Not placed"
    )
    declined.validate_request(request())
    assert CatalogQueryPrepareResult.deserialize_from_bytes(declined.serialize_to_bytes()) == declined
    for field, value in [
        ("prepared_query_token", b"x"),
        ("output_schema", OUTPUT),
        ("estimated_rows", 0),
        ("context_digest", b"x" * 32),
        ("validated_catalog_version", 0),
        ("expires_at_epoch_ms", 5),
    ]:
        with pytest.raises(ValueError, match="acceptance fields"):
            invalid: dict[str, Any] = {field: value}
            replace(declined, **invalid)
    with pytest.raises(ValueError, match="reason code"):
        replace(declined, reason_code=None)


@pytest.mark.parametrize(
    "change",
    [
        {"status": "unknown"},
        {"reason_code": "bad"},
        {"prepared_query_token": b""},
        {"prepared_query_token": None},
        {"output_schema": None},
        {"output_schema": OUTPUT + b"extra"},
        {"result_ordering": None},
        {"context_digest": b"too short"},
        {"expires_at_epoch_ms": 0},
        {"expires_at_epoch_ms": True},
        {"estimated_bytes": -1},
    ],
)
def test_incomplete_or_contradictory_acceptance_rejects(change: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        replace(result(), **change)


def test_accepted_context_schema_and_order_are_mandatory_checks() -> None:
    source = request(expected_output_schema=OUTPUT, required_ordering="query_order")
    accepted = result(source)
    accepted.validate_request(source)
    for changed in [
        replace(accepted, context_digest=b"x" * 32),
        replace(accepted, result_ordering="unspecified"),
        replace(accepted, output_schema=pa.schema([pa.field("result", pa.int64())]).serialize().to_pybytes()),
        replace(accepted, output_schema=pa.schema([pa.field("renamed", pa.int32())]).serialize().to_pybytes()),
        replace(
            accepted,
            output_schema=pa.schema([pa.field("result", pa.int32(), metadata={b"logical": b"changed"})])
            .serialize()
            .to_pybytes(),
        ),
    ]:
        with pytest.raises(ValueError):
            changed.validate_request(source)
    source = automatic()
    with pytest.raises(ValueError, match="metadata version"):
        result(source).validate_request(source)
    result(source, validated_catalog_version=0).validate_request(source)


def test_mutated_nested_lists_and_malformed_outer_records_revalidate() -> None:
    source = automatic()
    source.referenced_objects.append(table_ref())
    with pytest.raises(ValueError, match="duplicate"):
        source.validate_agreement(capability())
    value = result()
    batch = pa.ipc.open_stream(value.serialize_to_bytes()).read_next_batch()
    with pytest.raises(ValueError, match="complete schema"):
        CatalogQueryPrepareResult.deserialize_from_batch(batch.slice(0, 0))
    with pytest.raises(ValueError, match="complete schema"):
        CatalogQueryPrepareResult.deserialize_from_batch(batch.drop_columns(["reason_detail"]))
    with pytest.raises(ValueError, match="trailing"):
        CatalogQueryPrepareResult.deserialize_from_bytes(value.serialize_to_bytes() + b"extra")
