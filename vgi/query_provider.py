# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Provider boundary for catalog queries over the ordinary VGI TABLE lifecycle.

Providers own logical-catalog authorization, backend transactions, portable
preparations, execution owners and bounded cleanup. No backend cursor is ever
serialized into a client continuation. An owner loss is an execution error,
never permission to execute the query again.
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa
from vgi_rpc.rpc import AuthContext

from vgi.catalog.query import (
    CatalogQueryPrepareRequest,
    CatalogQueryPrepareResult,
    ClientQueryCapabilities,
    QueryCapabilities,
)
from vgi.query_parameters import QueryParameters


@dataclass(frozen=True, kw_only=True)
class QueryOwner:
    """Current authenticated owner, with provider attachment/transaction bytes.

    ``scope`` is an SDK-derived owner key, including the framework attachment
    identity, authentication identity and transaction. Providers bind every
    stored resource to it and recheck current authorization on every operation.
    It does not replace access checks or represent a catalog/data version.
    """

    scope: bytes
    attach_opaque_data: bytes
    transaction_opaque_data: bytes | None
    auth: AuthContext


@dataclass(frozen=True, kw_only=True)
class QueryPreparation:
    """Immutable preparation reloaded by its provider-owned receipt.

    The result token is the provider's private lookup recipe here. The SDK seals
    it before returning it on the wire. SQL, schemas and result metadata must
    survive reconnects without retaining a process-local relation or cursor.
    """

    request: CatalogQueryPrepareRequest
    result: CatalogQueryPrepareResult

    def __post_init__(self) -> None:
        """Require a complete accepted result for exactly this request."""
        if self.result.status != "accepted":
            raise ValueError("stored query preparation must be accepted")
        self.result.validate_request(self.request)


@dataclass(frozen=True, kw_only=True)
class QueryExecution:
    """Serializable owner/fragment locator with its own bounded lifetime."""

    handle: bytes
    expires_at_epoch_ms: int


class CatalogQueryProvider:
    """Optional provider API; these methods are not new streaming RPCs.

    Implementations must bound preparation/execution count, bytes and lifetimes.
    ``prepare`` and ``resolve`` must not run a result query. ``start`` executes
    once per successful invocation; neither ambiguous failures nor owner loss
    are retried. Reads either return the identical batch for a repeated position
    or reject replay. Closing/releasing resources is idempotent for their owner.

    Transaction end, attachment revocation, normal EOS, errors and cancellation
    close executions. Since cancellation is best effort, expired resources must
    also be reclaimed without requiring a well-behaved client.
    """

    def capabilities(self, *, owner: QueryOwner, offer: ClientQueryCapabilities) -> QueryCapabilities | None:
        """Return support certified for this client build, without executing SQL."""
        return None

    def prepare(self, *, owner: QueryOwner, request: CatalogQueryPrepareRequest) -> CatalogQueryPrepareResult:
        """Parse, authorize and describe; only legitimate placement failures decline."""
        raise NotImplementedError("catalog query preparation is not implemented")

    def resolve(self, *, owner: QueryOwner, recipe: bytes) -> QueryPreparation:
        """Reload immutable, authorized preparation metadata without executing it."""
        raise NotImplementedError("catalog query receipt resolution is not implemented")

    def start(
        self,
        *,
        owner: QueryOwner,
        preparation: QueryPreparation,
        parameters: QueryParameters,
        execution_id: bytes,
    ) -> QueryExecution:
        """Revalidate schema/policy/snapshot, then start one fresh execution."""
        raise NotImplementedError("catalog query execution is not implemented")

    def read(self, *, owner: QueryOwner, handle: bytes, position: int) -> pa.RecordBatch | None:
        """Read one ordered batch or EOS; never reconstruct an execution by rerunning SQL."""
        raise NotImplementedError("catalog query reading is not implemented")

    def close(self, *, owner: QueryOwner, handle: bytes, reason: str) -> None:
        """Idempotently interrupt and release this owner's execution resources."""
        raise NotImplementedError("catalog query cleanup is not implemented")

    def release(self, *, owner: QueryOwner, recipe: bytes) -> None:
        """Release a preparation without canceling executions already started from it."""
        raise NotImplementedError("catalog query release is not implemented")
