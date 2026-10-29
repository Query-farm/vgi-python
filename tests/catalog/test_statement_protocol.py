# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""Real DuckDB transactions through signed HTTP and existing query streaming RPCs.

The in-process HTTP fixture exercises real protocol serialization and sealing,
not a mock SQL backend. Its one process owns transaction connections: these
tests do not qualify portable owners, production recovery or native writes.
"""

from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any, NoReturn

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient
from vgi_rpc.rpc import AuthContext, CallContext

from vgi.arguments import Arguments
from vgi.catalog import CatalogAttachResult, TableInfo, TransactionOpaqueData
from vgi.catalog.query import (
    QUERY_ENCODING,
    QUERY_EXECUTOR,
    QUERY_EXPLICIT,
    QUERY_STATEMENT,
    STATEMENT_RESULT_KEY,
    CatalogQueryPrepareRequest,
    CatalogQueryPrepareResult,
    QueryEvaluationContext,
    read_query_schema,
)
from vgi.client import Client
from vgi.invocation import BindResponse, FunctionType
from vgi.protocol import BindRequest, ClientCapabilities, VgiProtocol
from vgi.query_parameters import QueryParameters
from vgi.query_provider import QueryExecution, QueryOwner, QueryPreparation
from vgi.serve import create_app
from vgi.table_function import TableInOutFunctionInitPhase

from .statement_reference import (
    BASELINE,
    COMMAND,
    CREATE,
    DELETE,
    DROP,
    INSERT,
    INSERT_RETURNING,
    OFFER,
    RETURNING,
    REVISION,
    SELECT,
    SIGNING_KEY,
    UPDATE,
    UPDATE_RETURNING,
    StatementAuthority,
    StatementReferenceCatalog,
    StatementReferenceProvider,
    StatementReferenceWorker,
)
from .typing_helpers import present

CAPS = ClientCapabilities(
    engine="fixture", native_formats=[], catalogs=[], can_stream=True, filter_encodings=[], query_pushdown=OFFER
)
CTX = CallContext(auth=AuthContext.anonymous(), emit_client_log=lambda *args, **kwargs: None)


@pytest.fixture
def backend(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[StatementReferenceWorker, StatementAuthority]]:
    authority = StatementAuthority(tmp_path / "statements.duckdb")
    monkeypatch.setattr(StatementReferenceCatalog, "authority", authority)
    worker = StatementReferenceWorker(quiet=True)
    worker._signing_key = SIGNING_KEY
    try:
        yield worker, authority
    finally:
        authority.close()


def client() -> Client:
    return Client.from_http(
        "http://vgi.test",
        httpx_client=_SyncTestClient(create_app(StatementReferenceWorker, describe=False, signing_key=SIGNING_KEY)),
    )


def attach(c: Client, capabilities: ClientCapabilities = CAPS) -> CatalogAttachResult:
    return c.catalog_attach(
        name="statements", data_version_spec=None, implementation_version=None, client_capabilities=capabilities
    )


def parameters(**values: int | str | None) -> QueryParameters:
    if not values:
        return QueryParameters.empty(mode="named")
    schema = pa.schema([(name, pa.int64() if name == "id" else pa.string()) for name in values])
    return QueryParameters.from_batch(pa.RecordBatch.from_pylist([values], schema=schema), mode="named")


def request(
    attached: CatalogAttachResult,
    tx: bytes | None,
    query: str,
    values: QueryParameters | None = None,
    *,
    read_only: bool = False,
) -> CatalogQueryPrepareRequest:
    return CatalogQueryPrepareRequest(
        attach_opaque_data=attached.attach_opaque_data,
        transaction_opaque_data=tx,
        encoding=QUERY_ENCODING,
        semantic_profile=QUERY_EXPLICIT if read_only else QUERY_STATEMENT,
        mode="explicit" if read_only else "statement",
        capability_revision=REVISION,
        query=query,
        parameter_schema=(values or parameters()).schema_bytes(),
        evaluation_context=QueryEvaluationContext(),
    )


def bind_request(
    attached: CatalogAttachResult, tx: bytes, prepared: CatalogQueryPrepareResult, values: QueryParameters
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
                pa.scalar(values.serialize_to_bytes(), type=pa.binary()),
            )
        ),
    )


def execute(
    c: Client, attached: CatalogAttachResult, tx: bytes, query: str, values: QueryParameters | None = None
) -> pa.Table:
    values = values or parameters()
    prepared = c.catalog_query_prepare(request(attached, tx, query, values, read_only=query == SELECT))
    assert prepared.status == "accepted"
    batches = list(
        c.catalog_query_execute(
            attach_opaque_data=attached.attach_opaque_data,
            prepared=prepared,
            parameters=values,
            transaction_opaque_data=tx,
        )
    )
    return pa.Table.from_batches(batches, schema=read_query_schema(present(prepared.output_schema)))


def begin(c: Client, attached: CatalogAttachResult) -> bytes:
    return present(c.catalog_transaction_begin(attach_opaque_data=attached.attach_opaque_data))


def finish(c: Client, attached: CatalogAttachResult, tx: bytes, operation: str = "rollback") -> None:
    getattr(c, f"catalog_transaction_{operation}")(
        attach_opaque_data=attached.attach_opaque_data, transaction_opaque_data=tx
    )


def committed_rows(authority: StatementAuthority) -> list[tuple[Any, ...]]:
    return authority.connection.execute(SELECT).fetchall()


def table(c: Client, attached: CatalogAttachResult, tx: bytes | None = None) -> TableInfo | None:
    return c.table_get(
        attach_opaque_data=attached.attach_opaque_data,
        transaction_opaque_data=None if tx is None else TransactionOpaqueData(tx),
        schema_path=["main"],
        name="added",
    )


def version(c: Client, attached: CatalogAttachResult, tx: bytes | None = None) -> int:
    return c.catalog_version(
        attach_opaque_data=attached.attach_opaque_data,
        transaction_opaque_data=None if tx is None else TransactionOpaqueData(tx),
    )


@pytest.mark.parametrize("query", [INSERT, INSERT_RETURNING])
@pytest.mark.parametrize("completion", ["commit", "rollback"])
def test_typed_null_prepare_bind_release_and_real_transaction_effects(
    backend: tuple[StatementReferenceWorker, StatementAuthority], query: str, completion: str
) -> None:
    worker, authority = backend
    with client() as c:
        attached = attach(c)
        assert present(attached.query_pushdown).execution_modes == ["explicit", "statement"]
        tx = begin(c, attached)
        values = parameters(id=10, label=None)
        prepared = c.catalog_query_prepare(request(attached, tx, query, values))
        bound = worker.bind(bind_request(attached, tx, prepared, values), CTX)
        expected = RETURNING if query == INSERT_RETURNING else COMMAND
        assert bound.output_schema.equals(expected, check_metadata=True)
        assert authority.starts == 0
        assert committed_rows(authority) == BASELINE
        assert next(iter(authority.transactions.values())).connection.execute(SELECT).fetchall() == BASELINE
        batches = list(
            c.catalog_query_execute(
                attach_opaque_data=attached.attach_opaque_data,
                prepared=prepared,
                parameters=values,
                transaction_opaque_data=tx,
            )
        )
        result = pa.Table.from_batches(batches)
        assert result.schema.equals(expected, check_metadata=True)
        assert result.to_pydict() == (
            {"id": [10], "label": [None]} if query == INSERT_RETURNING else {"affected_rows": [1]}
        )
        assert authority.starts == 1
        assert not authority.executions
        assert committed_rows(authority) == BASELINE  # EOS is not COMMIT.
        for _ in range(2):
            c.catalog_query_release(
                attach_opaque_data=attached.attach_opaque_data,
                transaction_opaque_data=tx,
                prepared_query_token=present(prepared.prepared_query_token),
            )
        assert not authority.preparations
        finish(c, attached, tx, completion)
        assert not authority.transactions
    # Reconnect through public RPC, while the one provider process still owns the database.
    with client() as c:
        attached = attach(c)
        tx = begin(c, attached)
        result = execute(c, attached, tx, SELECT)
        expected_rows = BASELINE + ([(10, None)] if completion == "commit" else [])
        assert list(zip(result.column(0).to_pylist(), result.column(1).to_pylist(), strict=True)) == expected_rows
        finish(c, attached, tx)


def test_update_delete_counts_and_empty_returning(backend: tuple[StatementReferenceWorker, StatementAuthority]) -> None:
    _, authority = backend
    with client() as c:
        attached = attach(c)
        tx = begin(c, attached)
        assert execute(c, attached, tx, UPDATE, parameters(id=0, label="changed")).to_pydict() == {"affected_rows": [1]}
        assert execute(c, attached, tx, DELETE, parameters(id=0)).to_pydict() == {"affected_rows": [1]}
        assert execute(c, attached, tx, DELETE, parameters(id=0)).to_pydict() == {"affected_rows": [0]}
        assert execute(c, attached, tx, UPDATE_RETURNING, parameters(label="all")).num_rows == 2
        for value in [1, 2]:
            execute(c, attached, tx, DELETE, parameters(id=value))
        empty = execute(c, attached, tx, UPDATE_RETURNING, parameters(label="none"))
        assert empty.num_rows == 0 and empty.schema.metadata[STATEMENT_RESULT_KEY] == b"rows"
        assert not authority.executions
        finish(c, attached, tx, "commit")
        assert committed_rows(authority) == []


@pytest.mark.parametrize("completion", ["commit", "rollback"])
def test_ddl_is_transactional_and_existing_version_rpc_sees_new_metadata(
    backend: tuple[StatementReferenceWorker, StatementAuthority], completion: str
) -> None:
    worker, authority = backend
    with client() as c:
        attached = attach(c)
        tx = begin(c, attached)
        prepared = c.catalog_query_prepare(request(attached, tx, CREATE))
        worker.bind(bind_request(attached, tx, prepared, parameters()), CTX)
        assert table(c, attached, tx) is None and authority.starts == 0
        assert prepared.validated_catalog_version == version(c, attached, tx) == 1
        result = execute(c, attached, tx, CREATE)
        assert result.to_pydict() == {"affected_rows": [None]}
        assert result.schema.equals(COMMAND, check_metadata=True)
        assert present(table(c, attached, tx)).name == "added"
        assert version(c, attached, tx) == 2
        assert table(c, attached) is None and version(c, attached) == 1
        # Preparation describes the old snapshot; it is never mutated into a completion receipt.
        assert prepared.validated_catalog_version == 1
        finish(c, attached, tx, completion)
        assert (table(c, attached) is not None) == (completion == "commit")
        assert version(c, attached) == (2 if completion == "commit" else 1)
        if completion == "commit":
            tx = begin(c, attached)
            assert execute(c, attached, tx, DROP).to_pydict() == {"affected_rows": [None]}
            assert table(c, attached, tx) is None and version(c, attached, tx) == 3
            finish(c, attached, tx)
            assert table(c, attached) is not None and version(c, attached) == 2


def test_statement_profile_does_not_authorize_read_profile_mutation_or_scripts(
    backend: tuple[StatementReferenceWorker, StatementAuthority],
) -> None:
    _, authority = backend
    with client() as c:
        attached = attach(c)
        tx = begin(c, attached)
        declined = c.catalog_query_prepare(request(attached, tx, INSERT, parameters(id=10, label="x"), read_only=True))
        assert declined.status == "declined"
        for sql in ["BEGIN", "COMMIT", "ROLLBACK", "SET threads=8", CREATE + "; " + INSERT, "COPY items TO 'x.csv'"]:
            assert c.catalog_query_prepare(request(attached, tx, sql)).status == "declined"
        with pytest.raises(ValueError, match="transaction"):
            request(attached, None, CREATE)
        assert authority.starts == 0 and committed_rows(authority) == BASELINE
        finish(c, attached, tx, "commit")


def test_unoffered_statement_and_wrong_owner_reject_before_effects(
    backend: tuple[StatementReferenceWorker, StatementAuthority],
) -> None:
    _, authority = backend
    with client() as c:
        readonly = attach(c, replace(CAPS, query_pushdown=replace(OFFER, semantic_profiles=[QUERY_EXPLICIT])))
        readonly_tx = begin(c, readonly)
        with pytest.raises(Exception, match="agreement|profile|capability"):
            c.catalog_query_prepare(request(readonly, readonly_tx, CREATE))
        finish(c, readonly, readonly_tx)
        attached, other = attach(c), attach(c)
        tx, other_tx = begin(c, attached), begin(c, other)
        values = parameters(id=10, label="x")
        prepared = c.catalog_query_prepare(request(attached, tx, INSERT, values))
        for owner, token in [(other, other_tx), (attached, other_tx), (attached, None)]:
            with pytest.raises(Exception, match="opaque|query|transaction"):
                list(
                    c.catalog_query_execute(
                        attach_opaque_data=owner.attach_opaque_data,
                        prepared=prepared,
                        parameters=values,
                        transaction_opaque_data=token,
                    )
                )
        wrong = QueryParameters.from_batch(
            pa.record_batch([pa.array([10], type=pa.int32()), pa.array(["x"])], names=["id", "label"]), mode="named"
        )
        with pytest.raises(Exception, match="schema"):
            list(
                c.catalog_query_execute(
                    attach_opaque_data=attached.attach_opaque_data,
                    prepared=prepared,
                    parameters=wrong,
                    transaction_opaque_data=tx,
                )
            )
        assert authority.starts == 0 and committed_rows(authority) == BASELINE
        finish(c, attached, tx, "commit")
        finish(c, other, other_tx)


@pytest.mark.parametrize("failure", ["cancel", "result_error", "wrong_result_schema", "expired_result"])
def test_incomplete_result_poisons_whole_transaction_and_rollback_undoes_prior_statements(
    backend: tuple[StatementReferenceWorker, StatementAuthority], failure: str
) -> None:
    _, authority = backend
    with client() as c:
        attached = attach(c)
        tx = begin(c, attached)
        execute(c, attached, tx, INSERT, parameters(id=10, label="earlier"))
        if failure == "result_error":
            authority.fail_read_at = 1
        values = parameters(label="mutated")
        prepared = c.catalog_query_prepare(request(attached, tx, UPDATE_RETURNING, values))
        stream = c.catalog_query_execute(
            attach_opaque_data=attached.attach_opaque_data,
            prepared=prepared,
            parameters=values,
            transaction_opaque_data=tx,
        )
        assert next(stream).num_rows == 2
        assert authority.starts == 2
        with pytest.raises(Exception, match="pending|rollback"):
            finish(c, attached, tx, "commit")
        if failure == "cancel":
            stream.close()
        else:
            result = next(iter(authority.executions.values()))
            if failure == "wrong_result_schema":
                result.batches[1] = pa.record_batch([pa.array(["bad"])], names=["wrong"])
            elif failure == "expired_result":
                result.expires = 0
            with pytest.raises(Exception, match="failure|schema|expired"):
                list(stream)
        assert next(iter(authority.transactions.values())).failed
        with pytest.raises(Exception, match="failed|rollback"):
            finish(c, attached, tx, "commit")
        assert authority.starts == 2  # No retry after effects or local fallback.
        finish(c, attached, tx)
        assert committed_rows(authority) == BASELINE
        assert not authority.executions and not authority.preparations


def test_backend_execution_error_requires_rollback_of_prior_dml_and_ddl(
    backend: tuple[StatementReferenceWorker, StatementAuthority],
) -> None:
    _, authority = backend
    with client() as c:
        attached = attach(c)
        tx = begin(c, attached)
        execute(c, attached, tx, INSERT, parameters(id=10, label="earlier"))
        execute(c, attached, tx, CREATE)
        with pytest.raises(Exception, match="already exists"):
            execute(c, attached, tx, CREATE)
        assert authority.starts == 3
        with pytest.raises(Exception, match="failed|rollback"):
            finish(c, attached, tx, "commit")
        finish(c, attached, tx)
        assert committed_rows(authority) == BASELINE
        assert table(c, attached) is None and version(c, attached) == 1


@pytest.mark.parametrize(
    "malformed",
    ["missing_row", "empty_batch", "two_rows", "extra_row", "extra_empty_batch", "negative_count", "changed_schema"],
)
def test_malformed_command_completion_invalidates_transaction_after_real_effect(
    backend: tuple[StatementReferenceWorker, StatementAuthority], monkeypatch: pytest.MonkeyPatch, malformed: str
) -> None:
    _, authority = backend
    original = StatementReferenceProvider.start

    def malformed_result(
        self: StatementReferenceProvider,
        *,
        owner: QueryOwner,
        preparation: QueryPreparation,
        parameters: QueryParameters,
        execution_id: bytes,
    ) -> QueryExecution:
        execution = original(
            self, owner=owner, preparation=preparation, parameters=parameters, execution_id=execution_id
        )
        result = authority.executions[execution.handle]
        valid = result.batches[0]
        variants = {
            "missing_row": [],
            "empty_batch": [valid.slice(0, 0)],
            "two_rows": [pa.record_batch([pa.array([1, 1], type=pa.int64())], schema=COMMAND)],
            "extra_row": [valid, valid],
            "extra_empty_batch": [valid, valid.slice(0, 0)],
            "negative_count": [pa.record_batch([pa.array([-1], type=pa.int64())], schema=COMMAND)],
            "changed_schema": [valid.replace_schema_metadata(None)],
        }
        result.batches = variants[malformed]
        return execution

    monkeypatch.setattr(StatementReferenceProvider, "start", malformed_result)
    with client() as c:
        attached = attach(c)
        tx = begin(c, attached)
        with pytest.raises(Exception, match="completion row|schema|affected_rows"):
            execute(c, attached, tx, INSERT, parameters(id=10, label="effect already occurred"))
        assert authority.starts == 1
        assert not authority.executions
        transaction = next(iter(authority.transactions.values()))
        assert transaction.failed
        assert transaction.connection.execute(SELECT).fetchall() == BASELINE + [(10, "effect already occurred")]
        with pytest.raises(Exception, match="failed|rollback"):
            finish(c, attached, tx, "commit")
        finish(c, attached, tx)
        assert committed_rows(authority) == BASELINE
        assert not authority.transactions and not authority.preparations


def test_lost_init_reply_simulation_does_not_retry_and_pending_execution_prevents_commit(
    backend: tuple[StatementReferenceWorker, StatementAuthority], monkeypatch: pytest.MonkeyPatch
) -> None:
    _, authority = backend
    with client() as c:
        attached = attach(c)
        tx = begin(c, attached)
        values = parameters(id=10, label="once")
        prepared = c.catalog_query_prepare(request(attached, tx, INSERT, values))
        original = c._do_init
        opened = []

        def lose_reply(
            proxy: VgiProtocol,
            bind_request: BindRequest,
            bind_response: BindResponse,
            *,
            projection_ids: list[int] | None = None,
            pushdown_filters_batch: pa.RecordBatch | None = None,
            phase: TableInOutFunctionInitPhase | None = None,
            execution_id: bytes | None = None,
            init_opaque_data: bytes | None = None,
            finalize_state_id: bytes | None = None,
            split_tokens: list[bytes] | None = None,
            join_keys: list[pa.RecordBatch] | None = None,
            substream_id: bytes | None = None,
        ) -> NoReturn:
            # Real signed-HTTP init succeeds, but the application never receives its
            # returned stream. This simulates the ambiguous client boundary, not
            # a packet-loss transport or a provider's crash recovery.
            opened.append(
                original(
                    proxy,
                    bind_request,
                    bind_response,
                    projection_ids=projection_ids,
                    pushdown_filters_batch=pushdown_filters_batch,
                    phase=phase,
                    execution_id=execution_id,
                    init_opaque_data=init_opaque_data,
                    finalize_state_id=finalize_state_id,
                    split_tokens=split_tokens,
                    join_keys=join_keys,
                    substream_id=substream_id,
                )
            )
            raise OSError("simulated lost init reply")

        monkeypatch.setattr(c, "_do_init", lose_reply)
        with pytest.raises(OSError, match="lost init reply"):
            list(
                c.catalog_query_execute(
                    attach_opaque_data=attached.attach_opaque_data,
                    prepared=prepared,
                    parameters=values,
                    transaction_opaque_data=tx,
                )
            )
        assert authority.starts == 1 and len(authority.executions) == 1
        with pytest.raises(Exception, match="pending|rollback"):
            finish(c, attached, tx, "commit")
        finish(c, attached, tx)
        for stream in opened:
            stream.close()
        assert committed_rows(authority) == BASELINE and not authority.executions


@pytest.mark.parametrize("operation", ["commit", "rollback"])
def test_completion_is_owner_bound_and_idempotent_without_reexecuting(
    backend: tuple[StatementReferenceWorker, StatementAuthority], operation: str
) -> None:
    _, authority = backend
    with client() as c:
        attached, other = attach(c), attach(c)
        tx = begin(c, attached)
        execute(c, attached, tx, INSERT, parameters(id=10, label="once"))
        finish(c, attached, tx, operation)
        finish(c, attached, tx, operation)
        assert authority.starts == 1 and not authority.transactions
        assert committed_rows(authority) == BASELINE + ([(10, "once")] if operation == "commit" else [])
        opposite = "rollback" if operation == "commit" else "commit"
        with pytest.raises(Exception, match="opposite operation"):
            finish(c, attached, tx, opposite)
        with pytest.raises(Exception, match="opaque|transaction|attachment"):
            finish(c, other, tx, operation)
        c.catalog_detach(attach_opaque_data=attached.attach_opaque_data)
        assert not authority.completed


def test_parameter_and_result_budgets_reject_at_the_correct_effect_boundary(
    backend: tuple[StatementReferenceWorker, StatementAuthority], monkeypatch: pytest.MonkeyPatch
) -> None:
    _, authority = backend
    with client() as c:
        attached = attach(c)
        tx = begin(c, attached)
        with pytest.raises(Exception, match="8 KiB limit"):
            execute(c, attached, tx, INSERT, parameters(id=10, label="x" * 8192))
        assert authority.starts == 0
        assert not next(iter(authority.transactions.values())).failed
        finish(c, attached, tx, "commit")
        tx = begin(c, attached)
        monkeypatch.setattr(authority, "max_result_bytes", 1)
        with pytest.raises(Exception, match="byte quota"):
            execute(c, attached, tx, INSERT_RETURNING, parameters(id=10, label="inserted"))
        assert authority.starts == 1
        assert next(iter(authority.transactions.values())).failed
        with pytest.raises(Exception, match="failed|rollback"):
            finish(c, attached, tx, "commit")
        finish(c, attached, tx)
        assert committed_rows(authority) == BASELINE


@pytest.mark.parametrize("cleanup", ["detach", "expiry"])
def test_abandoned_owner_cleanup_rolls_back_real_effects(
    backend: tuple[StatementReferenceWorker, StatementAuthority], cleanup: str
) -> None:
    _, authority = backend
    with client() as c:
        attached = attach(c)
        tx = begin(c, attached)
        execute(c, attached, tx, INSERT, parameters(id=10, label="uncommitted"))
        if cleanup == "detach":
            c.catalog_detach(attach_opaque_data=attached.attach_opaque_data)
        else:
            next(iter(authority.transactions.values())).expires = 0
            authority.sweep()
        assert not authority.transactions and not authority.preparations and not authority.executions
        assert committed_rows(authority) == BASELINE
