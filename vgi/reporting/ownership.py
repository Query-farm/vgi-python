# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Optional worker-controlled owner discovery for report and folder transfers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, ClassVar, Literal, Protocol

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi.reporting._arrow import non_null_list
from vgi.reporting.common import Ownership


@dataclass(frozen=True, slots=True, kw_only=True)
class OwnershipCandidate(ArrowSerializableDataclass):
    """A complete ownership assignment accepted as a candidate by this worker.

    Clients display the label and description and submit ownership unchanged.
    Canonical identities, owner kinds and parent authorities are worker-owned.
    """

    label: str
    description: str
    ownership: Ownership


@dataclass(frozen=True, slots=True, kw_only=True)
class OwnershipOptions(ArrowSerializableDataclass):
    """Bounded, resource-specific results; an empty list is a valid answer."""

    query_hint: str
    candidates: Annotated[list[OwnershipCandidate], non_null_list(pa.struct(OwnershipCandidate.ARROW_SCHEMA))]
    has_more: bool


class ReportOwnershipProtocol(Protocol):
    """Optional companion to ReportsProtocol, advertised through hosted protocols.

    Workers authorize discovery against the resource and control whether searches
    are partial, exact identifier lookups, email resolution, or another scheme.
    Results disclose only candidates available to this caller for this resource.
    A candidate is not a grant: set_ownership/set_folder_ownership must revalidate
    the complete assignment and caller at mutation time, with expected_version.
    The parent authority may remain unchanged or be supplied by the worker.
    """

    protocol_name: ClassVar[str] = "vgi.reports.ownership.v1"
    protocol_version: ClassVar[str] = "1.0.0"

    def find_owners(
        self,
        resource_kind: Annotated[Literal["report", "folder"], ArrowType(pa.string())],
        resource_id: str,
        query: str = "",
        limit: int = 20,
    ) -> OwnershipOptions:
        """Return up to limit candidates (1–100); has_more asks the user to refine query.

        An empty query may return suggestions or no results. query_hint explains
        the worker's accepted search input, including exact-only lookup policies.
        Invalid limits/identifiers use INVALID_ARGUMENT; authorization follows
        the report store's standard permission/not-found disclosure rules.
        """
        ...
