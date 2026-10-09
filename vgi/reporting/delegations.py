# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Proposed vgi.delegations.v1 records and RPC interface.

These are importable protocol definitions, not a service implementation.
Behavior and worker policy are documented in docs/design/reporting-protocols/.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, ClassVar, Protocol

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import ProducerState, Stream

from vgi.reporting._arrow import non_null_list
from vgi.reporting._metadata import sql_read
from vgi.reporting.common import (
    Ack,
    DelegationKey,
    DelegationRecord,
    DelegationWrite,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class DelegationPutResult(ArrowSerializableDataclass):
    """Metadata for an atomic credential update, without credential values."""

    delegations: Annotated[list[DelegationRecord], non_null_list(pa.struct(DelegationRecord.ARROW_SCHEMA))]


class DelegationsProtocol(Protocol):
    """Proposed vgi.delegations.v1 records and RPC interface; implementations supply all methods."""

    protocol_name: ClassVar[str] = "vgi.delegations.v1"
    protocol_version: ClassVar[str] = "1.0.0"

    def put_delegations(
        self,
        request_id: str,
        delegations: Annotated[list[DelegationWrite], non_null_list(pa.struct(DelegationWrite.ARROW_SCHEMA))],
    ) -> DelegationPutResult:
        """Put delegations.

        Store or replace the caller's delegations, keyed by (kind, location, catalog_name, attachment_id); duplicate
        keys in one request are invalid_request.
        """
        ...

    @sql_read(row_type=DelegationRecord, table="delegations")
    def list_delegations(self) -> Stream[ProducerState]:
        """The caller's delegations with expires_at and updated_at. Never grants or tickets."""
        ...

    def revoke_delegation(self, key: DelegationKey, expected_version: int, request_id: str) -> Ack:
        """Destroy exactly the caller's matching delegation."""
        ...
