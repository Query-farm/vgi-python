# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""Public HTTP negotiation, typed execution, ownership and portable result lifecycle."""

import sqlite3
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient
from vgi_rpc.rpc import AuthContext, CallContext

from vgi import attach_capabilities
from vgi.arguments import Arguments
from vgi.catalog import CatalogAttachResult
from vgi.catalog.capabilities import CatalogCapabilities
from vgi.catalog.query import (
    QUERY_ENCODING,
    QUERY_EXECUTOR,
    QUERY_EXPLICIT,
    CatalogQueryPrepareRequest,
    CatalogQueryPrepareResult,
    QueryEvaluationContext,
)
from vgi.client import Client
from vgi.invocation import FunctionType
from vgi.meta_worker import MetaWorker
from vgi.protocol import BindRequest, CatalogAttachRequest, ClientCapabilities, InitRequest, VgiProtocol
from vgi.query_parameters import QueryParameters
from vgi.serve import create_app

from .query_reference import OFFER, QueryReferenceCatalog, QueryReferenceProvider, QueryReferenceWorker
from .typing_helpers import present

KEY = b"query-reference-signing-key"
CTX = CallContext(auth=AuthContext.anonymous(), emit_client_log=lambda *args, **kwargs: None)
CAPS = ClientCapabilities(
    engine="fixture", native_formats=[], catalogs=[], can_stream=True, filter_encodings=[], query_pushdown=OFFER
)


@pytest.fixture
def backend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> QueryReferenceWorker:
    monkeypatch.setattr(QueryReferenceCatalog, "database_path", str(tmp_path / "query.sqlite"), raising=False)
    worker = QueryReferenceWorker(quiet=True)
    worker._signing_key = KEY
    return worker


def client(attach: bytes | None = None) -> Client:
    return Client.from_http(
        "http://vgi.test",
        attach_opaque_data=attach,
        httpx_client=_SyncTestClient(create_app(QueryReferenceWorker, describe=False, signing_key=KEY)),
    )


def attach(c: Client, capabilities: ClientCapabilities = CAPS) -> CatalogAttachResult:
    return c.catalog_attach(
        name="queries", data_version_spec=None, implementation_version=None, client_capabilities=capabilities
    )


def parameters(value: int | str | None = 3, name: str = "value", dtype: pa.DataType | None = None) -> QueryParameters:
    return QueryParameters.from_batch(
        pa.record_batch([pa.array([value], type=dtype or pa.int32())], names=[name]),
        mode="positional" if name == "1" else "named",
    )


def request(
    attached: CatalogAttachResult,
    values: QueryParameters | None = None,
    query: str = "SELECT $value AS result",
    tx: bytes | None = None,
) -> CatalogQueryPrepareRequest:
    return CatalogQueryPrepareRequest(
        attach_opaque_data=attached.attach_opaque_data,
        encoding=QUERY_ENCODING,
        semantic_profile=QUERY_EXPLICIT,
        mode="explicit",
        capability_revision="finite-reference-1",
        query=query,
        parameter_schema=(values or parameters()).schema_bytes(),
        evaluation_context=QueryEvaluationContext(),
        transaction_opaque_data=tx,
        required_ordering="query_order",
    )


def bind_request(
    attached: CatalogAttachResult,
    prepared: CatalogQueryPrepareResult,
    values: QueryParameters | None = None,
    tx: bytes | None = None,
) -> BindRequest:
    return BindRequest(
        function_name=QUERY_EXECUTOR,
        schema_path=[],
        function_type=FunctionType.TABLE,
        attach_opaque_data=attached.attach_opaque_data,
        transaction_opaque_data=tx,
        arguments=Arguments(
            positional=(
                pa.scalar(prepared.prepared_query_token, type=pa.binary()),
                pa.scalar((values or parameters()).serialize_to_bytes(), type=pa.binary()),
            )
        ),
    )


def counts() -> dict[str, int]:
    with closing(sqlite3.connect(QueryReferenceCatalog.database_path)) as db:
        return {
            table: db.execute(f"SELECT count(*) FROM query_{table}").fetchone()[0]
            for table in ["preparations", "executions", "fragments"]
        } | {"starts": db.execute("SELECT starts FROM query_audit").fetchone()[0]}


def test_query_agreement_is_optional_private_and_portable(backend: QueryReferenceWorker) -> None:
    with client() as c:
        no_offer = attach(c, replace(CAPS, query_pushdown=None))
        selected = attach(c)
        assert no_offer.query_pushdown is None
        assert present(selected.query_pushdown).semantic_profiles == [QUERY_EXPLICIT]
        assert selected.attach_opaque_data_required
        with pytest.raises(Exception, match="query|capability"):
            c.catalog_query_prepare(request(no_offer))
    other = QueryReferenceWorker(quiet=True)
    other._signing_key = KEY
    plain = other._open_attach_context(selected.attach_opaque_data, None)
    assert plain is not None
    assert attach_capabilities.agreement(plain) == CatalogCapabilities()
    offer, agreement = present(attach_capabilities.query_agreement(plain))
    assert offer == OFFER and agreement == selected.query_pushdown
    assert other._unwrap_attach_full(selected.attach_opaque_data) == attach_capabilities.split(plain)[1]
    for payload in [b"VGI\0CAPQ\0", b"VGI\0CAPQ\0\0\0\0\1x", plain[:25]]:
        with pytest.raises(ValueError):
            attach_capabilities.agreement(payload)
    with client() as c:
        result = c.catalog_query_prepare(request(selected))
        assert result.status == "accepted"
    assert counts()["starts"] == 0


@pytest.mark.parametrize("name,query", [("value", "SELECT $value AS result"), ("1", "SELECT $1 AS result")])
def test_prepare_bind_do_not_execute_and_values_rebind_without_casts(
    backend: QueryReferenceWorker, name: str, query: str
) -> None:
    with client() as c:
        attached = attach(c)
        first = parameters(None, name)
        prepared = c.catalog_query_prepare(request(attached, first, query))
        bound = backend.bind(bind_request(attached, prepared, first), CTX)
        assert bound.output_schema == pa.schema([pa.field("result", pa.int32())])
        assert counts()["starts"] == 0
        for value in [None, 7, -2]:
            batches = list(
                c.catalog_query_execute(
                    attach_opaque_data=attached.attach_opaque_data,
                    prepared=prepared,
                    parameters=parameters(value, name),
                )
            )
            assert pa.Table.from_batches(batches).to_pydict() == {"result": [value]}
        assert counts() == {"preparations": 1, "executions": 0, "fragments": 0, "starts": 3}
        with pytest.raises(Exception, match="schema"):
            list(
                c.catalog_query_execute(
                    attach_opaque_data=attached.attach_opaque_data,
                    prepared=prepared,
                    parameters=parameters(7, name, pa.int64()),
                )
            )
        assert counts()["starts"] == 3


def test_repeated_placeholder_empty_result_and_ordered_multibatch(backend: QueryReferenceWorker) -> None:
    with client() as c:
        attached = attach(c)
        for query, values, expected in [
            ("SELECT $value + $value AS result", parameters(3), [6]),
            (
                "SELECT i * $factor AS result FROM numbers ORDER BY i",
                parameters(5, "factor"),
                [i * 5 for i in range(7)],
            ),
            ("SELECT i AS result FROM numbers WHERE false", QueryParameters.empty(), []),
        ]:
            prepared = c.catalog_query_prepare(request(attached, values, query))
            batches = list(
                c.catalog_query_execute(
                    attach_opaque_data=attached.attach_opaque_data, prepared=prepared, parameters=values
                )
            )
            assert [v for b in batches for v in b.column(0).to_pylist()] == expected
            assert counts()["executions"] == counts()["fragments"] == 0
        declined = c.catalog_query_prepare(request(attached, query="DELETE FROM numbers"))
        assert declined.status == "declined" and declined.reason_code == "unsupported_query"
        assert counts()["starts"] == 3


def test_receipts_bind_attachment_transaction_revision_and_release(
    backend: QueryReferenceWorker, monkeypatch: pytest.MonkeyPatch
) -> None:
    with client() as c:
        attached, other = attach(c), attach(c)
        tx = present(c.catalog_transaction_begin(attach_opaque_data=attached.attach_opaque_data))
        prepared = c.catalog_query_prepare(request(attached, tx=tx))
        for wrong_attach, wrong_tx in [(other, tx), (attached, None)]:
            with pytest.raises(Exception, match="opaque|query|transaction"):
                backend.bind(bind_request(wrong_attach, prepared, tx=wrong_tx), CTX)
        monkeypatch.setattr(QueryReferenceProvider, "revision", "changed")
        with pytest.raises(Exception, match="reattach"):
            backend.bind(bind_request(attached, prepared, tx=tx), CTX)
        c.catalog_query_release(
            attach_opaque_data=attached.attach_opaque_data,
            prepared_query_token=present(prepared.prepared_query_token),
            transaction_opaque_data=tx,
        )
        c.catalog_query_release(
            attach_opaque_data=attached.attach_opaque_data,
            prepared_query_token=present(prepared.prepared_query_token),
            transaction_opaque_data=tx,
        )
        assert counts()["preparations"] == counts()["starts"] == 0
        monkeypatch.setattr(QueryReferenceProvider, "revision", "finite-reference-1")
        with pytest.raises(Exception, match="missing"):
            backend.bind(bind_request(attached, prepared, tx=tx), CTX)


@pytest.mark.parametrize(
    "change",
    [
        {"projection_ids": [0, 0]},
        {"projection_ids": []},
        {"row_limit": 1},
        {"split_tokens": []},
        {"execution_id": b"secondary", "init_opaque_data": b"token"},
        {"tablesample_percentage": 10.0},
    ],
)
def test_query_executor_rejects_alternate_init_before_start(
    backend: QueryReferenceWorker, change: dict[str, Any]
) -> None:
    with client() as c:
        attached = attach(c)
        prepared = c.catalog_query_prepare(request(attached))
        call = bind_request(attached, prepared)
        bound = backend.bind(call, CTX)
        init = InitRequest(bind_call=call, output_schema=bound.output_schema, **change)
        with pytest.raises(ValueError):
            backend.init(init, CTX)
        assert counts()["starts"] == 0


def test_generator_close_transaction_end_and_detach_cleanup(backend: QueryReferenceWorker) -> None:
    with client() as c:
        attached = attach(c)
        values = QueryParameters.empty()
        query = "SELECT i AS result FROM numbers ORDER BY i"
        prepared = c.catalog_query_prepare(request(attached, values, query))
        rows = c.catalog_query_execute(
            attach_opaque_data=attached.attach_opaque_data, prepared=prepared, parameters=values
        )
        assert next(rows).column(0).to_pylist() == [0, 1]
        rows.close()
        assert counts()["executions"] == counts()["fragments"] == 0
        tx = present(c.catalog_transaction_begin(attach_opaque_data=attached.attach_opaque_data))
        prepared = c.catalog_query_prepare(request(attached, values, query, tx))
        c.catalog_transaction_rollback(attach_opaque_data=attached.attach_opaque_data, transaction_opaque_data=tx)
        with pytest.raises(Exception, match="transaction|missing"):
            list(
                c.catalog_query_execute(
                    attach_opaque_data=attached.attach_opaque_data,
                    prepared=prepared,
                    parameters=values,
                    transaction_opaque_data=tx,
                )
            )
        c.catalog_detach(attach_opaque_data=attached.attach_opaque_data)
        assert counts()["preparations"] == 0


def test_meta_worker_routes_prepare_and_release_with_private_agreement(backend: QueryReferenceWorker) -> None:
    meta = MetaWorker([backend])
    meta._signing_key = KEY
    attached = meta.catalog_attach(
        CatalogAttachRequest(
            name="queries", data_version_spec=None, implementation_version=None, client_capabilities=CAPS
        )
    )
    prepared = meta.catalog_query_prepare(request(attached))
    assert meta.bind(bind_request(attached, prepared), CTX).output_schema.names == ["result"]
    cast(VgiProtocol, meta).catalog_query_release(
        attach_opaque_data=attached.attach_opaque_data, prepared_query_token=present(prepared.prepared_query_token)
    )
    assert counts()["preparations"] == 0


def test_quota_and_expiry_are_bounded_and_cleanup_needs_no_client_release(
    backend: QueryReferenceWorker, monkeypatch: pytest.MonkeyPatch
) -> None:
    with client() as c:
        attached = attach(c)
        monkeypatch.setattr(QueryReferenceProvider, "max_resources", 1)
        c.catalog_query_prepare(request(attached))
        with pytest.raises(Exception, match="quota"):
            c.catalog_query_prepare(request(attached))
        with closing(sqlite3.connect(QueryReferenceCatalog.database_path)) as db, db:
            db.execute("UPDATE query_preparations SET expires=0")
        provider = backend._get_catalog().query_provider
        assert isinstance(provider, QueryReferenceProvider)
        provider.sweep()
        assert counts()["preparations"] == 0
        assert c.catalog_query_prepare(request(attached)).status == "accepted"


def test_fresh_worker_resumes_ordered_fragments_after_preparation_release(backend: QueryReferenceWorker) -> None:
    with client() as c:
        attached = attach(c)
        values = QueryParameters.empty()
        prepared = c.catalog_query_prepare(request(attached, values, "SELECT i AS result FROM numbers ORDER BY i"))
    with client(attached.attach_opaque_data) as c:
        call = bind_request(attached, prepared, values)
        cursor = c.table_scan_resumable(function_name=QUERY_EXECUTOR, schema_path=[], arguments=call.arguments)
        first, token = cursor.next()
        assert first is not None
        assert first.column(0).to_pylist() == [0, 1] and token is not None
        cursor.close()  # HTTP releases the local reader; the execution has its own lease.
        c.catalog_query_release(
            attach_opaque_data=attached.attach_opaque_data, prepared_query_token=present(prepared.prepared_query_token)
        )
        assert counts()["preparations"] == 0 and counts()["starts"] == 1
    # A different app/Worker and connection recover only the shared fragments.
    with client() as c:
        with pytest.raises(ValueError, match="table_scan_continue"):
            c.table_scan_resumable(function_name=QUERY_EXECUTOR, schema_path=[], resume_token=token)
        cursor = c.table_scan_continue(resume_token=token)
        rows = [0, 1]
        while True:
            batch, _ = cursor.next()
            if batch is None:
                break
            rows.extend(int(present(value)) for value in batch.column(0).to_pylist())
        cursor.close()
    assert rows == list(range(7))
    assert counts() == {"preparations": 0, "executions": 0, "fragments": 0, "starts": 1}


@pytest.mark.parametrize("failure", ["missing", "wrong_schema", "expired", "policy"])
def test_failed_continuation_never_reexecutes_or_falls_back(
    backend: QueryReferenceWorker, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    with client() as c:
        attached = attach(c)
        values = QueryParameters.empty()
        prepared = c.catalog_query_prepare(request(attached, values, "SELECT i AS result FROM numbers ORDER BY i"))
    with client(attached.attach_opaque_data) as c:
        cursor = c.table_scan_resumable(
            function_name=QUERY_EXECUTOR, schema_path=[], arguments=bind_request(attached, prepared, values).arguments
        )
        _, token = cursor.next()
        cursor.close()
        assert token is not None
    if failure == "policy":
        monkeypatch.setattr(QueryReferenceProvider, "revision", "changed")
    else:
        with closing(sqlite3.connect(QueryReferenceCatalog.database_path)) as db, db:
            if failure == "missing":
                db.execute("DELETE FROM query_executions")
            elif failure == "expired":
                db.execute("UPDATE query_executions SET expires=0")
            else:
                from vgi_rpc.utils import serialize_record_batch_bytes

                wrong = serialize_record_batch_bytes(pa.record_batch([pa.array(["wrong"])], names=["result"]))
                db.execute("UPDATE query_fragments SET payload=? WHERE position>0", (wrong,))
    with client() as c:
        cursor = c.table_scan_continue(resume_token=token)
        with pytest.raises(Exception, match="query|schema|reattach"):
            cursor.next()
        cursor.close()
    assert counts()["starts"] == 1
    assert counts()["executions"] == counts()["fragments"] == 0


def test_query_receipts_are_authentication_bound_and_tampering_rejects(
    backend: QueryReferenceWorker, monkeypatch: pytest.MonkeyPatch
) -> None:
    import vgi.query_execution as execution

    alice = AuthContext(domain="fixture", authenticated=True, principal="alice")
    bob = replace(alice, principal="bob")
    for module in ["vgi.worker", "vgi.query_execution"]:
        monkeypatch.setattr(f"{module}.current_auth", lambda: alice)
    attached = backend.catalog_attach(
        CatalogAttachRequest(
            name="queries", data_version_spec=None, implementation_version=None, client_capabilities=CAPS
        )
    )
    prepared = backend.catalog_query_prepare(request(attached))
    assert prepared.prepared_query_token is not None
    assert attached.query_pushdown is not None
    tampered = replace(
        prepared,
        prepared_query_token=present(prepared.prepared_query_token)[:-1]
        + bytes([prepared.prepared_query_token[-1] ^ 1]),
    )
    with pytest.raises(Exception, match="opaque|query"):
        backend.bind(bind_request(attached, tampered), CTX)
    # Even if the enclosing attachment could be opened, a receipt cannot be
    # reinterpreted under a different principal's query resource owner.
    owner = execution._owner(backend, attached.attach_opaque_data, None)
    other_scope = execution.owner_from_plaintext(b"0" * 16 + owner.attach_opaque_data)
    with pytest.raises(Exception, match="opaque|query"):
        execution._open(backend, prepared.prepared_query_token, other_scope, attached.query_pushdown, "prepare")
    for module in ["vgi.worker", "vgi.query_execution"]:
        monkeypatch.setattr(f"{module}.current_auth", lambda: bob)
    with pytest.raises(Exception, match="opaque|query|attach"):
        backend.bind(bind_request(attached, prepared), CTX)
    assert counts()["starts"] == 0


def test_reserved_executor_is_hidden_and_cannot_be_registered(backend: QueryReferenceWorker) -> None:
    from vgi.query_execution import executor_type
    from vgi.worker import Worker

    internal = executor_type(backend)
    assert internal is executor_type(backend)
    assert QUERY_EXECUTOR not in type(backend)._build_registry()

    class InvalidWorker(Worker):
        functions = [internal]

    with pytest.raises(ValueError, match="reserved"):
        InvalidWorker._build_registry()
