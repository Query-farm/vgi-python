# Copyright 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Pure scalar negotiation validates contracts before existing stream execution."""

from __future__ import annotations

import json
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import TypedDict, cast

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient
from vgi_rpc.rpc import AuthContext, CallContext, RpcError, StreamSession, rpc_methods
from vgi_rpc.wire import write_request

from vgi.arguments import Arguments
from vgi.catalog.capabilities import ROW_SCALAR_EXPRESSION
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.sql import SqlExpression
from vgi.client import Client
from vgi.invocation import BindResponse, FunctionType
from vgi.meta_worker import MetaWorker
from vgi.protocol import (
    BindRequest,
    CatalogAttachRequest,
    InitRequest,
    TableFunctionProjectionRequest,
    TableFunctionProjectionResponse,
    VgiProtocol,
)
from vgi.serve import create_app
from vgi.table_function import BindParams, TableFunctionBase

from .projection_reference import BASE, ROWS, SOURCE, ProjectionReferenceWorker


def expression(sql: str, *, owner: list[str] | None = None, context: QueryEvaluationContext | None = None) -> bytes:
    return SqlExpression(
        sql=sql,
        profile=ROW_SCALAR_EXPRESSION,
        schema_path=owner or ["main"],
        evaluation_context=context or QueryEvaluationContext(),
    ).serialize_to_bytes()


def request(
    *sql: str, function: str = "projection_accepted", token: bytes | None = None, base: BindResponse | None = None
) -> TableFunctionProjectionRequest:
    return TableFunctionProjectionRequest(
        bind_call=BindRequest(
            function_name=function,
            function_type=FunctionType.TABLE,
            schema_path=["main"],
            arguments=Arguments(),
            attach_opaque_data=token,
        ),
        bind_result=base
        or BindResponse(output_schema=SOURCE, opaque_data=BASE, projection_expression_profiles=[ROW_SCALAR_EXPRESSION]),
        expressions=[expression(value) for value in sql],
        result_schema=pa.schema(
            [pa.field(f"p{i}", pa.int64(), metadata={b"slot": str(i).encode()}) for i in range(len(sql))]
        ),
    )


def accepted(
    proposal: TableFunctionProjectionRequest, indices: list[int] | None = None
) -> TableFunctionProjectionResponse:
    indices = [0] if indices is None else indices
    source = proposal.bind_result.output_schema
    metadata: dict[bytes | str, bytes | str] | None = (
        None if source.metadata is None else {key: value for key, value in source.metadata.items()}
    )
    return TableFunctionProjectionResponse(
        accepted_indices=indices,
        bind_result=BindResponse(
            output_schema=pa.schema([*source, *(proposal.result_schema.field(i) for i in indices)], metadata=metadata),
            opaque_data=b"complete replacement",
            projection_expression_profiles=proposal.bind_result.projection_expression_profiles,
            repeatability=proposal.bind_result.repeatability,
        ),
    )


def test_request_response_canonical_roundtrip_and_partial_append_contract() -> None:
    proposal = request('CAST("n" AS BIGINT)', "n + 1", 'CAST("other" AS BIGINT)')
    restored = TableFunctionProjectionRequest.deserialize_from_bytes(proposal.serialize_to_bytes())
    assert restored.bind_result.output_schema.equals(SOURCE, check_metadata=True)
    response = accepted(restored, [0, 2])
    response.validate_request(restored)
    decoded = TableFunctionProjectionResponse.deserialize_from_bytes(response.serialize_to_bytes())
    decoded.validate_request(restored)
    assert decoded.accepted_indices == [0, 2]
    assert decoded.bind_result is not None
    assert decoded.bind_result.output_schema.names == ["n", "other", "text", "p0", "p2"]
    assert decoded.bind_result.opaque_data == b"complete replacement"
    TableFunctionProjectionResponse().validate_request(restored)


@pytest.mark.parametrize("indices", [[0, 0], [1, 0], [-1], [True]])
def test_invalid_accepted_indices_reject(indices: list[int]) -> None:
    with pytest.raises(ValueError, match="indices"):
        TableFunctionProjectionResponse(accepted_indices=indices)


class _ProposalChange(TypedDict, total=False):
    """The fields varied by malformed proposal fixtures."""

    expressions: list[bytes]
    result_schema: pa.Schema
    bind_result: BindResponse


@pytest.mark.parametrize(
    "change",
    [
        {"expressions": []},
        {"expressions": [b"invalid"]},
        {"expressions": [expression("n", owner=["a.b"])]},
        {"result_schema": pa.schema([pa.field("n", pa.int64())])},
        {"result_schema": pa.schema([pa.field("p0", pa.int64())], metadata={b"wrong": b"scope"})},
        {"bind_result": BindResponse(output_schema=SOURCE)},
        {
            "bind_result": BindResponse(
                output_schema=SOURCE, lookup_names=["orphan"], projection_expression_profiles=[ROW_SCALAR_EXPRESSION]
            )
        },
    ],
)
def test_invalid_proposals_reject_before_callback(change: _ProposalChange) -> None:
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        replace(request("n"), **change)


def test_changed_contexts_and_unoffered_profiles_reject() -> None:
    proposal = request("n", "other")
    with pytest.raises(ValueError, match="context"):
        replace(
            proposal,
            expressions=[expression("n"), expression("other", context=QueryEvaluationContext(default_order="desc"))],
        )
    with pytest.raises(ValueError, match="profile"):
        replace(
            proposal,
            expressions=[
                replace(SqlExpression.deserialize_from_bytes(e), profile="other.expression.v1").serialize_to_bytes()
                for e in proposal.expressions
            ],
        )


def test_column_name_identity_uses_ascii_case_insensitive_comparison() -> None:
    with pytest.raises(ValueError, match="unambiguous"):
        replace(request("n"), result_schema=pa.schema([pa.field("N", pa.int64())]))
    source = BindResponse(
        output_schema=pa.schema([pa.field("ß", pa.int32())]),
        projection_expression_profiles=[ROW_SCALAR_EXPRESSION],
    )
    proposal = replace(request('"ß"', base=source), result_schema=pa.schema([pa.field("ss", pa.int64())]))
    proposal.__post_init__()


@pytest.mark.parametrize("change", ["prefix", "type", "nullability", "metadata", "count", "offer", "secret", "index"])
def test_malformed_acceptance_cannot_change_the_plan(change: str) -> None:
    proposal = request("n")
    response = accepted(proposal)
    bound = response.bind_result
    assert bound is not None
    schema = bound.output_schema
    if change == "prefix":
        schema = schema.set(0, schema.field(0).with_name("renamed"))
    elif change == "type":
        schema = schema.set(3, schema.field(3).with_type(pa.string()))
    elif change == "nullability":
        schema = schema.set(3, schema.field(3).with_nullable(False))
    elif change == "metadata":
        schema = schema.remove_metadata()
    elif change == "count":
        schema = SOURCE
    with pytest.raises(ValueError):
        if change == "secret":
            replace(response, bind_result=replace(bound, lookup_names=["secret"]))
        elif change == "index":
            replace(response, accepted_indices=[1]).validate_request(proposal)
        else:
            replace(
                response,
                bind_result=replace(
                    bound,
                    output_schema=schema,
                    projection_expression_profiles=[] if change == "offer" else bound.projection_expression_profiles,
                ),
            ).validate_request(proposal)
    assert proposal.bind_result.output_schema.equals(SOURCE, check_metadata=True)


def test_default_callback_declines_and_corrupted_envelopes_reject() -> None:
    proposal = request("n")
    # The default callback does not inspect bind parameters. Exercise it directly.
    assert (
        TableFunctionBase[None].on_projection(cast("BindParams[None]", None), proposal)
        == TableFunctionProjectionResponse()
    )
    payload = proposal.serialize_to_bytes()
    with pytest.raises(ValueError, match="trailing"):
        TableFunctionProjectionRequest.deserialize_from_bytes(payload + b"garbage")
    with pa.ipc.open_stream(payload) as reader:
        row = reader.read_next_batch()
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, row.schema) as writer:
        writer.write_batch(row)
        writer.write_batch(row)
    with pytest.raises(ValueError, match="exactly one"):
        TableFunctionProjectionRequest.deserialize_from_bytes(sink.getvalue().to_pybytes())


def client() -> Client:
    return Client.from_http(
        "http://vgi.test",
        httpx_client=_SyncTestClient(
            create_app(ProjectionReferenceWorker, describe=False, signing_key=b"projection-test-signing-key")
        ),
    )


def test_http_truncated_projection_body_is_a_bad_request() -> None:
    """A truncated custom-codec value must be rejected as malformed input."""
    transport = _SyncTestClient(
        create_app(ProjectionReferenceWorker, describe=False, signing_key=b"projection-test-signing-key")
    )
    payload = request('CAST("n" AS BIGINT)').serialize_to_bytes()
    body = write_request(
        "table_function_projection",
        rpc_methods(VgiProtocol)["table_function_projection"].params_schema,
        {"request": payload[:-9]},
        protocol=VgiProtocol.protocol_name,
        protocol_version=VgiProtocol.protocol_version,
    )
    response = transport.post(
        f"http://vgi.test/{VgiProtocol.protocol_name}/table_function_projection",
        content=body,
        headers={"content-type": "application/vnd.apache.arrow.stream"},
    )
    assert response.status_code == 400


def scan(proxy: VgiProtocol, call: BindRequest, bound: BindResponse) -> list[dict[str, object]]:
    # RPC clients expose a StreamSession where the service declaration returns Stream.
    execution = cast(
        StreamSession,
        proxy.init(
            request=InitRequest(bind_call=call, output_schema=bound.output_schema, bind_opaque_data=bound.opaque_data)
        ),
    )
    with closing(execution) as stream:
        batches = [output.batch for output in stream]
    assert len(batches) == 2  # Separate HTTP turns rehydrate the complete accepted state.
    assert all(batch.schema.equals(bound.output_schema, check_metadata=True) for batch in batches)
    return pa.Table.from_batches(batches).to_pylist()


@pytest.mark.parametrize("mode,indices", [("accepted", [0, 2]), ("partial", [0]), ("declined", [])])
def test_signed_http_partial_acceptance_decline_and_existing_stream_lifetime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str, indices: list[int]
) -> None:
    trace = tmp_path / "events.jsonl"
    monkeypatch.setenv("VGI_PROJECTION_TRACE", str(trace))
    with client() as c:
        token = c.catalog_attach(
            name="projection_reference", data_version_spec=None, implementation_version=None
        ).attach_opaque_data
        proposal = request(
            'CAST("n" AS BIGINT)',
            "CAST(text AS BIGINT)",
            'CAST("other" AS BIGINT)',
            function="projection_" + mode,
            token=token,
        )
        assert c._primary is not None
        proxy = c._primary.proxy
        original = proxy.bind(request=proposal.bind_call)
        proposal = replace(proposal, bind_result=original)
        result = proxy.table_function_projection(request=proposal)
        result.validate_request(proposal)
        assert result.accepted_indices == indices
        assert all(json.loads(line)["event"] not in {"init", "rows"} for line in trace.read_text().splitlines())
        bound = result.bind_result or original
        actual = scan(proxy, proposal.bind_call, bound)
        expected = [dict(row, **{f"p{i}": row["n" if i == 0 else "other"] for i in indices}) for row in ROWS]
        assert actual == expected
        # The original bind is still valid: negotiation did not mutate cached source state.
        assert scan(proxy, proposal.bind_call, original) == ROWS


def test_malformed_provider_response_rejects_before_init(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    trace = tmp_path / "events.jsonl"
    monkeypatch.setenv("VGI_PROJECTION_TRACE", str(trace))
    with client() as c:
        token = c.catalog_attach(
            name="projection_reference", data_version_spec=None, implementation_version=None
        ).attach_opaque_data
        proposal = request('CAST("n" AS BIGINT)', function="projection_malformed", token=token)
        assert c._primary is not None
        with pytest.raises(RpcError, match="append-only"):
            c._primary.proxy.table_function_projection(request=proposal)
    assert all(json.loads(line)["event"] not in {"init", "rows"} for line in trace.read_text().splitlines())


def test_accepted_execution_error_is_not_converted_to_decline_or_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trace = tmp_path / "events.jsonl"
    monkeypatch.setenv("VGI_PROJECTION_TRACE", str(trace))
    with client() as c:
        token = c.catalog_attach(
            name="projection_reference", data_version_spec=None, implementation_version=None
        ).attach_opaque_data
        proposal = request('CAST("n" AS BIGINT)', function="projection_execution_error", token=token)
        assert c._primary is not None
        result = c._primary.proxy.table_function_projection(request=proposal)
        assert result.accepted_indices == [0]
        assert result.bind_result is not None
        with pytest.raises(RpcError, match="failed after acceptance"):
            scan(c._primary.proxy, proposal.bind_call, result.bind_result)
    events = [json.loads(line)["event"] for line in trace.read_text().splitlines()]
    assert events.count("proposal") == events.count("init") == 1 and "rows" not in events


def test_meta_worker_preserves_signed_owner_and_projection_contract() -> None:
    worker = MetaWorker([ProjectionReferenceWorker(quiet=True)])
    worker._signing_key = b"projection-meta-signing-key"
    ctx = CallContext(auth=AuthContext.anonymous(), emit_client_log=lambda *args, **kwargs: None)
    token = worker.catalog_attach(
        CatalogAttachRequest(name="projection_reference", data_version_spec=None, implementation_version=None)
    ).attach_opaque_data
    proposal = request('CAST("n" AS BIGINT)', token=token)
    proposal = replace(proposal, bind_result=worker.bind(proposal.bind_call, ctx=ctx))
    response = worker.table_function_projection(proposal, ctx=ctx)
    response.validate_request(proposal)
    assert response.accepted_indices == [0]
