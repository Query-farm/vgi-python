# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Proposed vgi.reports.v1 records and RPC interface.

These are importable protocol definitions, not a service implementation.
Behavior and worker policy are documented in docs/design/reporting-protocols/.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, ClassVar, Protocol

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import ProducerState, Stream

from vgi.reporting._arrow import Instant, non_null_list
from vgi.reporting._enums import RevisionKind
from vgi.reporting._metadata import FetchedColumn, sql_read
from vgi.reporting.common import (
    Ack,
    DataSource,
    Ownership,
    OwnershipInput,
    ParameterSpec,
    PrincipalRef,
    ResourceMeta,
    ServiceInfo,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class ReportEnvelope(ArrowSerializableDataclass):
    """Public report metadata, source attachments and parameter declarations."""

    title: str
    description: str = ""
    tags: Annotated[list[str], non_null_list(pa.string())] = field(default_factory=list)
    body_format: str
    data_sources: Annotated[list[DataSource], non_null_list(pa.struct(DataSource.ARROW_SCHEMA))] = field(
        default_factory=list
    )
    parameters: Annotated[list[ParameterSpec], non_null_list(pa.struct(ParameterSpec.ARROW_SCHEMA))] = field(
        default_factory=list
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class ReportRow(ResourceMeta):
    """Visible report revision, current folder and management metadata, without body bytes."""

    report_id: str
    folder_id: str | None
    head_revision_id: str
    published_revision_id: str | None
    revision_served: str
    revision_number: int
    envelope: ReportEnvelope | None
    body_sha256: str | None
    redacted: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class ReportResult(ReportRow):
    """One visible report revision, including its nullable redacted body."""

    body: bytes | None


@dataclass(frozen=True, slots=True, kw_only=True)
class RevisionRow(ArrowSerializableDataclass):
    """Immutable revision identity plus publication and redaction metadata."""

    report_id: str
    revision_id: str
    revision_number: int
    author: PrincipalRef
    created_at: Instant
    kind: RevisionKind
    message: str
    envelope: ReportEnvelope | None
    body_sha256: str | None
    published_at: Instant | None
    published_by: PrincipalRef | None
    redacted_at: Instant | None
    redacted_by: PrincipalRef | None
    redaction_reason: str | None


@dataclass(frozen=True, slots=True, kw_only=True)
class FolderRecord(ResourceMeta):
    """Persistent folder; a null parent selects root.

    Workers may set folder policies using their own authorization model.
    Inherited allowed_actions reports caller-specific permission hints.
    """

    folder_id: str
    parent_folder_id: str | None
    name: str


@dataclass(frozen=True, slots=True, kw_only=True)
class ReportsInfo(ServiceInfo):
    """Report store formats, limits and caller-specific root actions."""

    body_formats: Annotated[list[str], non_null_list(pa.string())]
    root_allowed_actions: Annotated[list[str], non_null_list(pa.string())]


class ReportsProtocol(Protocol):
    """Proposed vgi.reports.v1 records and RPC interface; implementations supply all methods.

    Workers may define and enforce policies on folders and reports. The protocol
    exposes permission hints and standard refusals; policy implementation and
    inheritance are worker choices.
    """

    protocol_name: ClassVar[str] = "vgi.reports.v1"
    protocol_version: ClassVar[str] = "1.0.0"

    @sql_read(table="info")
    def get_report_service_info(self) -> ReportsInfo:
        """display_name, writable, body formats, limits and allowed actions at root."""
        ...

    @sql_read(
        row_type=ReportRow,
        table="reports",
        fetched_columns=(
            FetchedColumn(
                column="body",
                method="get_report",
                arguments=(("report_id", "report_id"), ("revision_id", "revision_served")),
            ),
        ),
    )
    def list_reports(
        self,
        query: str = "",
        folder_id: str | None = None,
        recursive: bool = True,
        tags: Annotated[list[str], non_null_list(pa.string())] | None = None,
        owned_by_me: bool = False,
        published_only: bool = False,
    ) -> Stream[ProducerState]:
        """Visible reports beneath folder_id; None selects root, recursive=False selects direct children."""
        ...

    @sql_read()
    def get_report(self, report_id: str, revision_id: str | None = None) -> ReportResult:
        """Envelope and body.

        With no revision id, editors get the head and everyone else the published revision; revision_served says
        which.
        """
        ...

    @sql_read(row_type=RevisionRow)
    def list_revisions(self, report_id: str) -> Stream[ProducerState]:
        """History, newest first, tombstones included."""
        ...

    def create_report(
        self,
        request_id: str,
        envelope: ReportEnvelope,
        body: bytes,
        message: str = "",
        ownership: OwnershipInput | None = None,
        folder_id: str | None = None,
    ) -> ReportResult:
        """New report with revision 1 in folder_id, or at root when None."""
        ...

    def move_report(self, report_id: str, expected_version: int, request_id: str, folder_id: str | None) -> ReportRow:
        """Move a report, with None selecting root; preserve report ID, revisions and publication."""
        ...

    def commit_revision(
        self,
        report_id: str,
        expected_revision_id: str,
        request_id: str,
        envelope: ReportEnvelope,
        body: bytes,
        kind: RevisionKind = "edit",
        message: str = "",
    ) -> ReportResult:
        """New head; conflict if the head moved."""
        ...

    def publish(
        self, report_id: str, revision_id: str | None, expected_published_revision_id: str | None, request_id: str
    ) -> ReportResult:
        """Move the published pointer; null revision_id unpublishes."""
        ...

    def delete_report(self, report_id: str, expected_version: int, request_id: str) -> Ack:
        """Delete report."""
        ...

    def redact_revision(
        self, report_id: str, revision_id: str, expected_version: int, request_id: str, reason: str
    ) -> RevisionRow:
        """Requires redact."""
        ...

    def set_ownership(
        self, report_id: str, expected_version: int, request_id: str, ownership: Ownership
    ) -> ReportResult:
        """Transfer management ownership; preserve author and execution credentials."""
        ...

    @sql_read(row_type=FolderRecord, table="folders")
    def list_folders(self, parent_folder_id: str | None = None, recursive: bool = True) -> Stream[ProducerState]:
        """Visible child folders, including empty ones; None selects root, recursive=True includes descendants."""
        ...

    @sql_read()
    def get_folder(self, folder_id: str) -> FolderRecord:
        """Current folder metadata, ownership and caller-specific actions."""
        ...

    def create_folder(
        self,
        request_id: str,
        name: str,
        parent_folder_id: str | None = None,
        ownership: OwnershipInput | None = None,
    ) -> FolderRecord:
        """Create an empty folder under parent_folder_id, or at root when None."""
        ...

    def update_folder(
        self, folder_id: str, expected_version: int, request_id: str, name: str, parent_folder_id: str | None
    ) -> FolderRecord:
        """Atomically rename and/or move a folder; both destination values are required, cycles forbidden."""
        ...

    def delete_folder(self, folder_id: str, expected_version: int, request_id: str) -> Ack:
        """Delete only an empty folder, checking all children atomically, including those hidden from the caller."""
        ...

    def set_folder_ownership(
        self, folder_id: str, expected_version: int, request_id: str, ownership: Ownership
    ) -> FolderRecord:
        """Transfer folder management; preserve descendants, their explicit ownership and all report content."""
        ...
