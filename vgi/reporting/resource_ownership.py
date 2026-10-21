# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Optional worker-controlled ownership discovery for advertised resources."""

from dataclasses import dataclass
from typing import Annotated, ClassVar, Protocol

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import ProducerState, Stream

from vgi.reporting._arrow import non_null_list
from vgi.reporting._metadata import sql_read
from vgi.reporting.discovery import OwnerDiscoveryCapabilities
from vgi.reporting.ownership import OwnershipCandidate


@dataclass(frozen=True, slots=True, kw_only=True)
class OwnershipResource(ArrowSerializableDataclass):
    """A locally hosted protocol and supported ownership resource kind."""

    protocol: str
    resource_kind: str


@dataclass(frozen=True, slots=True, kw_only=True)
class OwnershipInfo(ArrowSerializableDataclass):
    """Resource types supported by this worker's ownership directory."""

    resources: Annotated[list[OwnershipResource], non_null_list(pa.struct(OwnershipResource.ARROW_SCHEMA))]


class OwnershipProtocol(Protocol):
    """Discovery is advisory; mutations revalidate identity, permissions and version."""

    protocol_name: ClassVar[str] = "vgi.ownership.v1"
    protocol_version: ClassVar[str] = "1.0.0"

    @sql_read()
    def get_ownership_info(self) -> OwnershipInfo:
        """Advertise supported local protocol/resource pairs."""
        ...

    @sql_read()
    def get_ownership_capabilities(
        self, protocol: str, resource_kind: str, resource_id: str
    ) -> OwnerDiscoveryCapabilities:
        """Authorize the resource and return its search policy and query hint."""
        ...

    @sql_read(row_type=OwnershipCandidate)
    def find_owners(
        self, protocol: str, resource_kind: str, resource_id: str, query: str = "", limit: int = 20
    ) -> Stream[ProducerState]:
        """Stream at most limit eligible owners across all continuations; obey advertised search policy."""
        ...
