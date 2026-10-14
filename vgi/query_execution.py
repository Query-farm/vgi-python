# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""SDK-owned preparation receipts and the reserved catalog-query TABLE executor."""

from __future__ import annotations

import hashlib
import time
from contextlib import suppress
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Annotated, Any, cast

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, crypto
from vgi_rpc.rpc import AuthContext, OutputCollector, current_auth

from vgi import attach_capabilities
from vgi.arguments import Arg
from vgi.catalog.query import (
    MAX_QUERY_TOKEN_BYTES,
    QUERY_AUTOMATIC,
    QUERY_EXECUTOR,
    QUERY_EXPLICIT,
    CatalogQueryPrepareRequest,
    CatalogQueryPrepareResult,
    ClientQueryCapabilities,
    QueryCapabilities,
    _QueryRecord,
    read_query_schema,
)
from vgi.exceptions import CatalogCapabilityError, OpaqueDataNotRecognizedError
from vgi.invocation import BindResponse, FunctionType, GlobalInitResponse
from vgi.logical_types import require_logical_batch, require_logical_profiles, require_logical_values
from vgi.query_parameters import QueryParameters
from vgi.query_provider import CatalogQueryProvider, QueryExecution, QueryOwner, QueryPreparation
from vgi.table_function import BindParams, InitParams, ProcessParams, TableFunctionGenerator
from vgi.typed_values import TypedValueBatch, _validate_scalar

if TYPE_CHECKING:
    from vgi.protocol import BindRequest, InitRequest
    from vgi.worker import Worker

_TICKET_VERSION = 81
_MAX_PREPARE_LIFETIME_MS = 60 * 60 * 1000
_MAX_EXECUTION_LIFETIME_MS = 24 * 60 * 60 * 1000


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _provider(worker: Worker) -> CatalogQueryProvider:
    provider = worker._get_catalog().query_provider
    if not isinstance(provider, CatalogQueryProvider):
        raise CatalogCapabilityError("catalog query", QUERY_EXPLICIT)
    return provider


def owner_from_plaintext(plaintext: bytes, transaction: bytes | None = None) -> QueryOwner:
    """Construct a resource owner only after the normal attachment authentication."""
    from vgi.worker import _attach_aad

    if len(plaintext) < 16 or transaction == b"":
        raise OpaqueDataNotRecognizedError("query_owner")
    auth = current_auth() or AuthContext.anonymous()
    binding = bytearray(b"vgi.query.owner.v1\0")
    for part in [_attach_aad(auth), plaintext, transaction or b""]:
        binding.extend(len(part).to_bytes(8, "big"))
        binding.extend(part)
    return QueryOwner(
        scope=hashlib.sha256(binding).digest(),
        attach_opaque_data=plaintext[16:],
        transaction_opaque_data=transaction,
        auth=auth,
    )


def _owner(worker: Worker, attachment: bytes | None, transaction: bytes | None) -> QueryOwner:
    if not attachment or transaction == b"":
        raise OpaqueDataNotRecognizedError("query_owner")
    plaintext = worker._unwrap_attach_full(attachment)
    assert plaintext is not None
    return owner_from_plaintext(plaintext, worker._unwrap_tx_opt(transaction, attachment))


def negotiate(
    worker: Worker, plaintext: bytes, offer: ClientQueryCapabilities | None, *, supports_transactions: bool
) -> QueryCapabilities | None:
    """Select only support returned by the provider for the actual client build."""
    if offer is None:
        return None
    offer.__post_init__()
    provider = worker._get_catalog().query_provider
    if provider is None:
        return None
    available = provider.capabilities(owner=owner_from_plaintext(plaintext), offer=offer)
    return available.select(offer, supports_transactions=supports_transactions) if available is not None else None


def _selected(worker: Worker, attachment: bytes) -> tuple[ClientQueryCapabilities, QueryCapabilities]:
    plaintext = worker._open_attach_context(attachment, current_auth())
    assert plaintext is not None
    agreement = attach_capabilities.query_agreement(plaintext)
    if agreement is None:
        raise CatalogCapabilityError("catalog query", QUERY_EXPLICIT)
    return agreement


def _current(
    worker: Worker, attachment: bytes | None, transaction: bytes | None
) -> tuple[CatalogQueryProvider, QueryOwner, QueryCapabilities]:
    owner = _owner(worker, attachment, transaction)
    assert attachment is not None
    offer, selected = _selected(worker, attachment)
    provider = _provider(worker)
    available = provider.capabilities(owner=owner, offer=offer)
    if (
        available is None
        or available.select(offer, supports_transactions=QUERY_AUTOMATIC in selected.semantic_profiles) != selected
    ):
        raise CatalogCapabilityError("catalog query agreement changed; reattach", QUERY_EXPLICIT)
    return provider, owner, selected


def _capability_digest(selected: QueryCapabilities) -> bytes:
    return hashlib.sha256(selected.serialize_to_bytes()).digest()


def _fingerprint(preparation: QueryPreparation) -> bytes:
    preparation.__post_init__()
    request = preparation.request.serialize_to_bytes()
    result = preparation.result.serialize_to_bytes()
    return hashlib.sha256(len(request).to_bytes(8, "big") + request + result).digest()


@dataclass(frozen=True, kw_only=True)
class _QueryTicket(_QueryRecord):
    kind: str
    owner: bytes
    capability: bytes
    handle: bytes
    contract: bytes
    expires_at_epoch_ms: int

    def __post_init__(self) -> None:
        if self.kind not in {"prepare", "execution"}:
            raise ValueError("unknown query ticket kind")
        if any(not isinstance(v, bytes) or len(v) != 32 for v in [self.owner, self.capability, self.contract]):
            raise ValueError("invalid query ticket digest")
        if not isinstance(self.handle, bytes) or not 0 < len(self.handle) <= 32768:
            raise ValueError("invalid query resource handle")
        if type(self.expires_at_epoch_ms) is not int or not 0 < self.expires_at_epoch_ms < 2**63:
            raise ValueError("invalid query ticket expiry")


def _seal(worker: Worker, ticket: _QueryTicket) -> bytes:
    payload = ticket.serialize_to_bytes()
    if worker._signing_key is not None:
        payload = crypto.seal_bytes(
            payload, worker._signing_key, aad=b"vgi.query.ticket.v1\0" + ticket.owner, version=_TICKET_VERSION
        )
    if len(payload) > MAX_QUERY_TOKEN_BYTES:
        raise ValueError("query ticket exceeds the protocol size limit")
    return payload


def _open(
    worker: Worker,
    payload: bytes,
    owner: QueryOwner,
    selected: QueryCapabilities,
    kind: str,
    *,
    check_expiry: bool = True,
) -> _QueryTicket:
    try:
        if not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_QUERY_TOKEN_BYTES:
            raise ValueError("invalid ticket size")
        if worker._signing_key is not None:
            payload = crypto.open_bytes(
                payload, worker._signing_key, aad=b"vgi.query.ticket.v1\0" + owner.scope, version=_TICKET_VERSION
            )
        ticket = _QueryTicket.deserialize_from_bytes(payload)
        if ticket.owner != owner.scope or ticket.capability != _capability_digest(selected) or ticket.kind != kind:
            raise ValueError("query ticket scope mismatch")
        if check_expiry and ticket.expires_at_epoch_ms <= _now_ms():
            raise ValueError("query ticket expired")
    except (ValueError, TypeError, crypto.SealError) as error:
        raise OpaqueDataNotRecognizedError("query_token") from error
    return ticket


def prepare(worker: Worker, request: CatalogQueryPrepareRequest) -> CatalogQueryPrepareResult:
    """Validate the attachment agreement, then seal an execution-free preparation."""
    request.__post_init__()
    request.serialize_to_bytes()  # Enforce the aggregate envelope bound on direct calls too.
    provider, owner, selected = _current(worker, request.attach_opaque_data, request.transaction_opaque_data)
    request.validate_agreement(selected)
    logical_profiles = worker._effective_catalog_capabilities(request.attach_opaque_data).read
    require_logical_profiles(read_query_schema(request.parameter_schema), selected_profiles=logical_profiles)
    response = provider.prepare(owner=owner, request=request)
    if not isinstance(response, CatalogQueryPrepareResult):
        raise TypeError("provider must return a CatalogQueryPrepareResult")
    try:
        response.validate_request(request)
        if response.status == "declined":
            return response
        require_logical_profiles(read_query_schema(response.output_schema), selected_profiles=logical_profiles)
        assert response.prepared_query_token is not None and response.expires_at_epoch_ms is not None
        now = _now_ms()
        if not now < response.expires_at_epoch_ms <= now + _MAX_PREPARE_LIFETIME_MS:
            raise ValueError("provider preparation expiry is outside the bounded lifetime")
        preparation = QueryPreparation(request=request, result=response)
        token = _seal(
            worker,
            _QueryTicket(
                kind="prepare",
                owner=owner.scope,
                capability=_capability_digest(selected),
                handle=response.prepared_query_token,
                contract=_fingerprint(preparation),
                expires_at_epoch_ms=response.expires_at_epoch_ms,
            ),
        )
    except Exception:
        if response.prepared_query_token:
            with suppress(Exception):
                provider.release(owner=owner, recipe=response.prepared_query_token)
        raise
    return replace(response, prepared_query_token=token)


def release(worker: Worker, attachment: bytes, token: bytes, transaction: bytes | None) -> None:
    """Permit owner cleanup after expiry or withdrawal, without accepting new execution."""
    owner = _owner(worker, attachment, transaction)
    _, selected = _selected(worker, attachment)
    ticket = _open(worker, token, owner, selected, "prepare", check_expiry=False)
    _provider(worker).release(owner=owner, recipe=ticket.handle)


@dataclass(frozen=True)
class _QueryArguments:
    prepared_query_token: Annotated[bytes, Arg(0)]
    parameter_values: Annotated[bytes, Arg(1)]


@dataclass
class _QueryStreamState(ArrowSerializableDataclass):
    position: int = 0
    closed: bool = False

    def __post_init__(self) -> None:
        if type(self.position) is not int or not 0 <= self.position < 2**63 or type(self.closed) is not bool:
            raise ValueError("invalid query continuation")


def _check_bind(request: BindRequest) -> None:
    if request.function_type != FunctionType.TABLE or request.schema_path not in (None, []):
        raise ValueError("catalog query executor is a schema-less TABLE function")
    if len(request.arguments.positional) != 2 or request.arguments.named:
        raise ValueError("catalog query executor requires exactly a receipt and one parameter batch")
    for value in request.arguments.positional:
        if value is None or not value.is_valid or value.type != pa.binary():
            raise ValueError("query executor arguments must be non-NULL binary values")
    if any(
        getattr(request, name) is not None
        for name in ["input_schema", "settings", "secrets", "at_unit", "at_value", "copy_from", "copy_to"]
    ):
        raise ValueError(
            "catalog query executor does not accept input, settings, secrets, time travel or COPY contexts"
        )
    if request.resolved_secrets_provided:
        raise ValueError("catalog query authority comes from its attachment")


def _bound(
    worker: Worker, request: BindRequest, arguments: _QueryArguments
) -> tuple[CatalogQueryProvider, QueryOwner, QueryCapabilities, QueryPreparation, QueryParameters]:
    _check_bind(request)
    provider, owner, selected = _current(worker, request.attach_opaque_data, request.transaction_opaque_data)
    ticket = _open(worker, arguments.prepared_query_token, owner, selected, "prepare")
    preparation = provider.resolve(owner=owner, recipe=ticket.handle)
    preparation.request.validate_agreement(selected)
    if _fingerprint(preparation) != ticket.contract:
        raise OpaqueDataNotRecognizedError("query_token")
    parameters = QueryParameters.deserialize_from_bytes(arguments.parameter_values)
    parameters.validate_prepared_schema(preparation.request.parameter_schema)
    assert request.attach_opaque_data is not None
    logical_profiles = worker._effective_catalog_capabilities(request.attach_opaque_data).read
    require_logical_profiles(read_query_schema(preparation.result.output_schema), selected_profiles=logical_profiles)
    require_logical_values(TypedValueBatch(parameters.batch), selected_profiles=logical_profiles)
    return provider, owner, selected, preparation, parameters


def check_init(request: InitRequest) -> None:
    """Reject alternate initialization paths before the generic secondary-init fast path."""
    if request.is_secondary or any(
        getattr(request, name) is not None
        for name in [
            "pushdown_filters",
            "join_keys",
            "split_tokens",
            "row_limit",
            "phase",
            "finalize_state_id",
            "init_opaque_data",
            "substream_id",
            "order_by_column_name",
            "order_by_direction",
            "order_by_null_order",
            "order_by_limit",
            "tablesample_percentage",
            "tablesample_seed",
        ]
    ):
        raise ValueError("catalog query executor does not accept splits, secondary readers or scan pushdown")
    if request.projection_ids is not None and request.projection_ids != list(range(len(request.output_schema))):
        raise ValueError("catalog query executor requires its complete output projection")


def executor_type(worker: Worker) -> type[TableFunctionGenerator[Any, Any]]:
    """One internal executor per Worker, reused for all SQL and HTTP continuations."""
    if cached := worker.__dict__.get("_query_executor_type"):
        return cast(type[TableFunctionGenerator[Any, Any]], cached)

    class QueryExecutor(TableFunctionGenerator[_QueryArguments, _QueryStreamState]):
        FunctionArguments = _QueryArguments

        class Meta:
            name = QUERY_EXECUTOR
            projection_pushdown = False
            filter_pushdown = False
            auto_apply_filters = False

        @classmethod
        def on_bind(cls, params: BindParams[_QueryArguments]) -> BindResponse:
            _, _, _, preparation, _ = _bound(worker, params.bind_call, params.args)
            return BindResponse(output_schema=read_query_schema(preparation.result.output_schema))

        @classmethod
        def on_init(cls, params: InitParams[_QueryArguments]) -> GlobalInitResponse:
            check_init(params.init_call)
            provider, owner, selected, preparation, parameters = _bound(worker, params.init_call.bind_call, params.args)
            expected = read_query_schema(preparation.result.output_schema)
            if not params.output_schema.equals(expected, check_metadata=True):
                raise ValueError("query initialization changed the accepted output schema")
            execution = provider.start(
                owner=owner, preparation=preparation, parameters=parameters, execution_id=params.execution_id
            )
            if not isinstance(execution, QueryExecution):
                raise TypeError("provider must return a portable QueryExecution")
            try:
                if not _now_ms() < execution.expires_at_epoch_ms <= _now_ms() + _MAX_EXECUTION_LIFETIME_MS:
                    raise ValueError("provider execution expiry is outside the bounded lifetime")
                token = _seal(
                    worker,
                    _QueryTicket(
                        kind="execution",
                        owner=owner.scope,
                        capability=_capability_digest(selected),
                        handle=execution.handle,
                        contract=_fingerprint(preparation),
                        expires_at_epoch_ms=execution.expires_at_epoch_ms,
                    ),
                )
            except Exception:
                with suppress(Exception):
                    provider.close(owner=owner, handle=execution.handle, reason="error")
                raise
            return GlobalInitResponse(execution_id=params.execution_id, max_workers=1, opaque_data=token)

        @classmethod
        def initial_state(cls, params: ProcessParams[_QueryArguments]) -> _QueryStreamState:
            return _QueryStreamState()

        @classmethod
        def process(
            cls, params: ProcessParams[_QueryArguments], state: _QueryStreamState, out: OutputCollector
        ) -> None:
            state.__post_init__()
            if state.closed:
                out.finish()
                return
            assert params.init_call is not None and params.init_response is not None
            call = params.init_call.bind_call
            owner = _owner(worker, call.attach_opaque_data, call.transaction_opaque_data)
            assert call.attach_opaque_data is not None and params.init_response.opaque_data is not None
            _, selected = _selected(worker, call.attach_opaque_data)
            ticket = _open(worker, params.init_response.opaque_data, owner, selected, "execution", check_expiry=False)
            provider = _provider(worker)
            try:
                _current(worker, call.attach_opaque_data, call.transaction_opaque_data)
                if ticket.expires_at_epoch_ms <= _now_ms():
                    raise OpaqueDataNotRecognizedError("query_token")
                if any(
                    value is not None
                    for value in [params.current_pushdown_filters, params.if_none_match, params.if_modified_since]
                ):
                    raise ValueError("catalog query executor rejects dynamic filters and result-cache validators")
                batch = provider.read(owner=owner, handle=ticket.handle, position=state.position)
                if batch is None:
                    provider.close(owner=owner, handle=ticket.handle, reason="eos")
                    state.closed = True
                    out.finish()
                    return
                if not isinstance(batch, pa.RecordBatch) or not batch.schema.equals(
                    params.output_schema, check_metadata=True
                ):
                    raise ValueError("query result batch changed the accepted schema")
                batch.validate(full=True)
                require_logical_batch(
                    batch,
                    selected_profiles=worker._effective_catalog_capabilities(call.attach_opaque_data).read,
                )
                for i, definition in enumerate(batch.schema):
                    for value in batch.column(i):
                        _validate_scalar(value, definition)
                out.emit(batch)
                state.position += 1
            except Exception:
                state.closed = True
                with suppress(Exception):
                    provider.close(owner=owner, handle=ticket.handle, reason="error")
                raise

        @classmethod
        def on_cancel(cls, params: ProcessParams[_QueryArguments], state: _QueryStreamState) -> None:
            if state.closed:
                return
            assert params.init_call is not None and params.init_response is not None
            call = params.init_call.bind_call
            owner = _owner(worker, call.attach_opaque_data, call.transaction_opaque_data)
            assert call.attach_opaque_data is not None and params.init_response.opaque_data is not None
            _, selected = _selected(worker, call.attach_opaque_data)
            ticket = _open(worker, params.init_response.opaque_data, owner, selected, "execution", check_expiry=False)
            _provider(worker).close(owner=owner, handle=ticket.handle, reason="cancel")
            state.closed = True

    worker.__dict__["_query_executor_type"] = QueryExecutor
    return QueryExecutor
