# Copyright 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""A bind-scoped promise must survive ordinary streams and source mutations."""

from contextlib import closing
from dataclasses import replace

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient
from vgi_rpc.rpc import RpcError

from vgi.arguments import Arguments
from vgi.client import Client
from vgi.invocation import BindRepeatability, BindResponse, FunctionType
from vgi.protocol import BindRequest, InitRequest
from vgi.serve import create_app

from .repeatability_reference import (
    BASELINE,
    SOURCE,
    BoundState,
    RepeatabilityWorker,
    Snapshot,
    Stable,
    replace_rows,
    seed_database,
)
from .test_projection_expressions import accepted
from .test_projection_expressions import request as projection_request

UPDATED = [(11, "eleven"), (12, "twelve"), (13, "thirteen"), (14, "fourteen")]


def expected(rows, generation):
    return [{"n": n, "label": label, "generation": generation} for n, label in rows]


def call(token, mode="stable"):
    return BindRequest(
        function_name="repeatability_" + mode,
        function_type=FunctionType.TABLE,
        schema_path=["main"],
        arguments=Arguments(),
        attach_opaque_data=token,
    )


def stream(proxy, invocation, bound):
    return proxy.init(
        request=InitRequest(bind_call=invocation, output_schema=bound.output_schema, bind_opaque_data=bound.opaque_data)
    )


def scan(proxy, invocation, bound):
    with closing(stream(proxy, invocation, bound)) as execution:
        batches = [output.batch for output in execution]
    assert len(batches) == 2
    assert all(batch.schema.equals(bound.output_schema, check_metadata=True) for batch in batches)
    return pa.Table.from_batches(batches).to_pylist()


@pytest.fixture
def backend(tmp_path, monkeypatch):
    database = tmp_path / "mutable.sqlite"
    seed_database(database)
    monkeypatch.setenv("VGI_REPEATABILITY_DB", str(database))
    app = create_app(RepeatabilityWorker, describe=False, signing_key=b"repeatability-http-test-key")
    with Client.from_http("http://vgi.test", httpx_client=_SyncTestClient(app)) as client:
        attached = client.catalog_attach(
            name="repeatability_reference", data_version_spec=None, implementation_version=None
        )
        yield database, client._primary.proxy, attached.attach_opaque_data


def test_default_is_unknown_and_wire_uses_nonnullable_dictionary_enum():
    value = BindResponse(output_schema=SOURCE)
    assert value.repeatability is BindRepeatability.UNKNOWN
    field = value.ARROW_SCHEMA.field("repeatability")
    assert field.type == pa.dictionary(pa.int16(), pa.string()) and not field.nullable
    for promise in BindRepeatability:
        encoded = replace(value, repeatability=promise).serialize_to_bytes()
        with pa.ipc.open_stream(encoded) as reader:
            row = reader.read_next_batch()
        assert row.column("repeatability").to_pylist() == [promise.name]
        assert BindResponse.deserialize_from_bytes(encoded).repeatability is promise


@pytest.mark.parametrize("invalid", [None, "UNKNOWN", "stable_within_query", 1])
def test_untyped_or_unknown_promises_reject(invalid):
    with pytest.raises(ValueError, match="repeatability"):
        BindResponse(output_schema=SOURCE, repeatability=invalid)


@pytest.mark.parametrize("wire_value", ["FUTURE_PROMISE", "unknown", "stable_within_query"])
def test_unknown_wire_enum_rejects(wire_value):
    value = BindResponse(output_schema=SOURCE)
    with pa.ipc.open_stream(value.serialize_to_bytes()) as reader:
        row = reader.read_next_batch()
    index = row.schema.get_field_index("repeatability")
    values = list(row.columns)
    values[index] = pa.DictionaryArray.from_arrays(pa.array([0], type=pa.int16()), pa.array([wire_value]))
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, row.schema) as writer:
        writer.write_batch(pa.RecordBatch.from_arrays(values, schema=row.schema))
    with pytest.raises((KeyError, ValueError)):
        BindResponse.deserialize_from_bytes(sink.getvalue().to_pybytes())


def test_missing_and_semantically_null_dictionary_promise_reject():
    with pa.ipc.open_stream(BindResponse(output_schema=SOURCE).serialize_to_bytes()) as reader:
        row = reader.read_next_batch()
    index = row.schema.get_field_index("repeatability")
    with pytest.raises(ValueError, match="repeatability"):
        BindResponse.deserialize_from_batch(row.remove_column(index))
    original = pa.array(["UNKNOWN"])
    # Retain the valid name bytes behind a null dictionary value. The index is valid.
    null_value = pa.Array.from_buffers(pa.string(), 1, [pa.py_buffer(b"\x00"), *original.buffers()[1:]], null_count=1)
    values = list(row.columns)
    values[index] = pa.DictionaryArray.from_arrays(pa.array([0], type=pa.int16()), null_value)
    malformed = pa.RecordBatch.from_arrays(values, schema=row.schema)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, row.schema) as writer:
        writer.write_batch(malformed)
    with pytest.raises(ValueError, match="repeatability"):
        BindResponse.deserialize_from_bytes(sink.getvalue().to_pybytes())


@pytest.mark.parametrize("promise", list(BindRepeatability))
def test_projection_acceptance_preserves_promise_exactly(promise):
    proposal = projection_request('CAST("n" AS BIGINT)')
    proposal = replace(proposal, bind_result=replace(proposal.bind_result, repeatability=promise))
    response = accepted(proposal)
    response.validate_request(proposal)
    other = next(value for value in BindRepeatability if value is not promise)
    changed = replace(response, bind_result=replace(response.bind_result, repeatability=other))
    with pytest.raises(ValueError, match="repeatability"):
        changed.validate_request(proposal)


@pytest.mark.parametrize("mode", ["stable", "unknown"])
def test_same_bind_snapshot_and_fresh_bind_after_external_commit(backend, mode):
    database, proxy, token = backend
    invocation = call(token, mode)
    original = proxy.bind(request=invocation)
    expected_promise = BindRepeatability.STABLE_WITHIN_QUERY if mode == "stable" else BindRepeatability.UNKNOWN
    assert original.repeatability is expected_promise
    assert scan(proxy, invocation, original) == expected(BASELINE, 1)
    assert replace_rows(database, UPDATED) == 2  # Independently committed real source mutation.
    assert scan(proxy, invocation, original) == (expected(BASELINE, 1) if mode == "stable" else expected(UPDATED, 2))
    fresh = proxy.bind(request=invocation)
    assert scan(proxy, invocation, fresh) == expected(UPDATED, 2)
    if mode == "stable":
        assert fresh.opaque_data != original.opaque_data
        # An independent bind does not mutate an earlier snapshot's portable state.
        assert scan(proxy, invocation, original) == expected(BASELINE, 1)


@pytest.mark.parametrize("mode", ["stable", "unknown"])
def test_http_continuation_does_not_reopen_mutable_source_between_batches(backend, mode):
    database, proxy, token = backend
    invocation = call(token, mode)
    bound = proxy.bind(request=invocation)
    with closing(stream(proxy, invocation, bound)) as execution:
        outputs = iter(execution)
        first = next(outputs).batch
        replace_rows(database, UPDATED)
        batches = [first, *(output.batch for output in outputs)]
    assert len(batches) == 2
    assert pa.Table.from_batches(batches).to_pylist() == expected(BASELINE, 1)


def test_accepted_projection_retains_original_snapshot_after_external_commit(backend):
    database, proxy, token = backend
    invocation = call(token)
    bound = proxy.bind(request=invocation)
    proposal = replace(projection_request('CAST("n" AS BIGINT)', '"n" + 1'), bind_call=invocation, bind_result=bound)
    response = proxy.table_function_projection(request=proposal)
    assert response.accepted_indices == [0]
    assert response.bind_result.repeatability is BindRepeatability.STABLE_WITHIN_QUERY
    replace_rows(database, UPDATED)
    assert scan(proxy, invocation, response.bind_result) == [dict(row, p0=row["n"]) for row in expected(BASELINE, 1)]
    assert scan(proxy, invocation, bound) == expected(BASELINE, 1)
    assert scan(proxy, invocation, proxy.bind(request=invocation)) == expected(UPDATED, 2)


def test_worker_revalidates_provider_repeatability_before_publication(backend, monkeypatch):
    _, proxy, token = backend
    invalid = BindResponse(output_schema=SOURCE)
    object.__setattr__(invalid, "repeatability", "STABLE_WITHIN_QUERY")
    monkeypatch.setattr(Stable, "on_bind", classmethod(lambda cls, params: invalid))
    with pytest.raises(RpcError, match="repeatability"):
        proxy.bind(request=call(token))


def test_stale_inline_metadata_is_distinct_from_fresh_source_bind(backend):
    _, proxy, token = backend
    info = proxy.catalog_table_get(
        attach_opaque_data=token,
        schema_path=["main"],
        name="stable_inline",
        transaction_opaque_data=None,
        at_unit=None,
        at_value=None,
    ).to_optional()
    bound = BindResponse.deserialize_from_bytes(info.bind_result)
    assert bound.repeatability is BindRepeatability.STABLE_WITHIN_QUERY
    state = BoundState.deserialize_from_bytes(bound.opaque_data)
    stale = Snapshot.deserialize_from_bytes(state.snapshot)
    assert stale.generation == 0 and stale.batch().column("n").to_pylist() == [-99]
    invocation = call(token)
    assert scan(proxy, invocation, proxy.bind(request=invocation)) == expected(BASELINE, 1)
