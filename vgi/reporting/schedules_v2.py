# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Proposed vgi.schedules.v2 records and RPC interface.

These are importable protocol definitions, not a service implementation.
Behavior and worker policy are documented in docs/design/reporting-protocols/.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, ClassVar, Protocol

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import ProducerState, Stream

from vgi.reporting._arrow import Instant, Json, non_null_list
from vgi.reporting._enums import InlineMode, OutputFormat
from vgi.reporting._metadata import sql_read
from vgi.reporting.common import (
    Ack,
    CredentialStatus,
    DataSource,
    Destination,
    DestinationResult,
    ExecutionIdentity,
    ExecutionInput,
    Notification,
    Output,
    OwnerKey,
    Ownership,
    OwnershipCapabilities,
    OwnershipInput,
    OwnershipScope,
    ParamValue,
    PreviewJob,
    RecoveryEvent,
    ResourceMeta,
    RetryPolicy,
    Run,
    RunStep,
    RunSummary,
    ServiceInfo,
    StepAttempt,
    StepResolution,
    Trigger,
    TriggerPreview,
    ValidationResult,
)
from vgi.reporting.sources import ObjectRef, QuerySource, ResolvedSource, RevisionSelection


@dataclass(frozen=True, slots=True, kw_only=True)
class ScheduleRunSummaryV2(RunSummary):
    """Run status associated with its schedule, without the frozen definition."""

    schedule_id: str


@dataclass(frozen=True, slots=True, kw_only=True)
class ScheduleRunV2(Run):
    """ScheduleV2 run with its frozen definition and selected report revision."""

    schedule_id: str
    schedule: ScheduleV2
    resolved_sources: Annotated[list[ResolvedSource], non_null_list(pa.struct(ResolvedSource.ARROW_SCHEMA))]


@dataclass(frozen=True, slots=True, kw_only=True)
class RenderReportActionV2(ArrowSerializableDataclass):
    """Pinned or tracked report rendering action."""

    source: ObjectRef
    selection: RevisionSelection = field(default_factory=lambda: RevisionSelection(mode="published"))
    outputs: Annotated[list[str], non_null_list(pa.string())] = field(default_factory=lambda: ["application/pdf"])


@dataclass(frozen=True, slots=True, kw_only=True)
class RunQueryActionV2(ArrowSerializableDataclass):
    """SQL setup/query action and requested output format."""

    source: QuerySource
    output_format: OutputFormat = "parquet"


@dataclass(frozen=True, slots=True, kw_only=True)
class ActionV2(ArrowSerializableDataclass):
    """Tagged action; only the selected payload is populated.

    data_sources belongs to condition/vendor SQL. A query source supplies its own
    bindings and public parameters; conflicting attachment aliases are invalid.
    """

    kind: str
    render_report: RenderReportActionV2 | None = None
    run_query: RunQueryActionV2 | None = None
    data_sources: Annotated[list[DataSource], non_null_list(pa.struct(DataSource.ARROW_SCHEMA))] = field(
        default_factory=list
    )
    custom_json: Json | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class DeliveryV2(ArrowSerializableDataclass):
    """Destinations and presentation preferences for one action delivery."""

    destinations: Annotated[list[Destination], non_null_list(pa.struct(Destination.ARROW_SCHEMA))]
    inline: InlineMode = "summary"
    attach: Annotated[list[str], non_null_list(pa.string())] = field(default_factory=list)


@dataclass(frozen=True, slots=True, kw_only=True)
class ScheduleAlertsV2(ArrowSerializableDataclass):
    """Separate, worker-validated recipients for advertised schedule alert events.

    The worker owns warning thresholds, incident grouping and delivery policy.
    Report delivery recipients are never implicitly subscribed to alerts.
    """

    destinations: Annotated[list[Destination], non_null_list(pa.struct(Destination.ARROW_SCHEMA))]
    events: Annotated[list[str], non_null_list(pa.string())]


@dataclass(frozen=True, slots=True, kw_only=True)
class ScheduleIssueV2(ArrowSerializableDataclass):
    """Current worker-reported attention item, without credentials or raw errors."""

    kind: str
    message: str
    run_id: str | None = None
    expires_at: Instant | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class ScheduleV2(ArrowSerializableDataclass):
    """Editable schedule definition; ownership and execution identity are separate."""

    title: str
    trigger: Trigger
    action: ActionV2
    condition_sql: str = ""
    parameter_values: Annotated[list[ParamValue], non_null_list(pa.struct(ParamValue.ARROW_SCHEMA))] = field(
        default_factory=list
    )
    deliveries: Annotated[list[DeliveryV2], non_null_list(pa.struct(DeliveryV2.ARROW_SCHEMA))] = field(
        default_factory=list
    )
    enabled: bool = True
    alerts: ScheduleAlertsV2 | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class ScheduleRecordV2(ResourceMeta):
    """Persistent schedule definition, management identity and current status."""

    schedule_id: str
    definition: ScheduleV2
    execution_identity: ExecutionIdentity
    next_fire_at: Instant | None
    last_run: ScheduleRunSummaryV2 | None
    disabled_reason: str | None
    credentials: Annotated[list[CredentialStatus], non_null_list(pa.struct(CredentialStatus.ARROW_SCHEMA))]
    issues: Annotated[list[ScheduleIssueV2], non_null_list(pa.struct(ScheduleIssueV2.ARROW_SCHEMA))] = field(
        default_factory=list
    )
    last_successful_at: Instant | None = None
    last_delivery_at: Instant | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class ScheduleSummaryV2(ResourceMeta):
    """Browseable schedule status without SQL, recipients, credentials or histories."""

    schedule_id: str
    title: str
    action_kind: str
    enabled: bool
    execution_identity: ExecutionIdentity
    next_fire_at: Instant | None
    last_run: ScheduleRunSummaryV2 | None
    disabled_reason: str | None
    issues: Annotated[list[ScheduleIssueV2], non_null_list(pa.struct(ScheduleIssueV2.ARROW_SCHEMA))]
    last_successful_at: Instant | None
    last_delivery_at: Instant | None


@dataclass(frozen=True, slots=True, kw_only=True)
class ScheduleTestV2(ArrowSerializableDataclass):
    """Safe preview results and prospective messages without delivery."""

    evaluated_at: Instant
    parameter_values: Annotated[list[ParamValue], non_null_list(pa.struct(ParamValue.ARROW_SCHEMA))]
    condition_value: bool | None
    condition_evaluated: bool
    outputs: Annotated[list[Output], non_null_list(pa.struct(Output.ARROW_SCHEMA))]
    messages: Annotated[list[Notification], non_null_list(pa.struct(Notification.ARROW_SCHEMA))]


@dataclass(frozen=True, slots=True, kw_only=True)
class SchedulePreviewJobV2(PreviewJob):
    """Typed preview result, populated exactly on success."""

    result: ScheduleTestV2 | None


@dataclass(frozen=True, slots=True, kw_only=True)
class SchedulerInfoV2(ServiceInfo):
    """Supported schedule actions, clocks, limits and retry policy."""

    ownership: OwnershipCapabilities
    action_kinds: Annotated[list[str], non_null_list(pa.string())]
    output_formats: Annotated[list[str], non_null_list(pa.string())]
    relative_tokens: Annotated[list[str], non_null_list(pa.string())]
    tzdb_version: str
    retry_policy: RetryPolicy
    alert_events: Annotated[list[str], non_null_list(pa.string())] = field(default_factory=list)


class SchedulesProtocolV2(Protocol):
    """Proposed vgi.schedules.v2 records and RPC interface; implementations supply all methods.

    OwnershipCapabilities defines all list/scan scope semantics. Exact owner
    cannot be combined with a non-all ownership_scope. parent_owner matches
    only the recorded direct management parent; folder recursion never expands
    directory ancestry. Filters select authorized resources and never grant access.
    """

    protocol_name: ClassVar[str] = "vgi.schedules.v2"
    protocol_version: ClassVar[str] = "2.0.0"

    @sql_read(table="info")
    def get_scheduler_info(self) -> SchedulerInfoV2:
        """ActionV2 kinds, output formats, minimum interval, relative tokens, limits."""
        ...

    @sql_read()
    def preview_trigger(self, trigger: Trigger, after: Instant, count: int = 10) -> TriggerPreview:
        """Next N fire times, a plain-English description, DST warnings."""
        ...

    @sql_read(row_type=ScheduleSummaryV2, table="schedules")
    def list_schedules(
        self,
        action_kind: str = "",
        source: ObjectRef | None = None,
        ownership_scope: OwnershipScope = "all",
        owner: OwnerKey | None = None,
        parent_owner: OwnerKey | None = None,
        execution_principal_id: str | None = None,
        query: str = "",
        enabled: bool | None = None,
        issue_kind: str = "",
        limit: int | None = None,
        order_by: str = "id",
    ) -> Stream[ProducerState]:
        """Summaries; filters combine with AND, a non-all ownership_scope cannot be combined with owner.

        query is a literal title substring; issue_kind selects a current issue.
        Orders: id, title, updated_desc, with schedule_id as stable tie-breaker.
        limit bounds the whole stream; continuation preserves snapshot and filters.
        """
        ...

    @sql_read(row_type=ScheduleRecordV2)
    def scan_schedules(
        self,
        action_kind: str = "",
        source: ObjectRef | None = None,
        ownership_scope: OwnershipScope = "all",
        owner: OwnerKey | None = None,
        parent_owner: OwnerKey | None = None,
        execution_principal_id: str | None = None,
        query: str = "",
        enabled: bool | None = None,
        issue_kind: str = "",
        limit: int | None = None,
        order_by: str = "id",
    ) -> Stream[ProducerState]:
        """Full definitions for explicit exports, with exactly the list filters and ordering."""
        ...

    @sql_read()
    def get_schedule(self, schedule_id: str) -> ScheduleRecordV2:
        """Read."""
        ...

    def create_schedule(
        self,
        request_id: str,
        schedule: ScheduleV2,
        ownership: OwnershipInput | None = None,
        execution: ExecutionInput | None = None,
    ) -> ScheduleRecordV2:
        """Create schedule.

        Ownership and execution identity are independently resolved by the host; grant_required if the execution
        principal lacks a grant for a required location.
        """
        ...

    def update_schedule(
        self, schedule_id: str, expected_version: int, request_id: str, schedule: ScheduleV2
    ) -> ScheduleRecordV2:
        """Includes pausing and resuming via enabled."""
        ...

    def delete_schedule(self, schedule_id: str, expected_version: int, request_id: str) -> Ack:
        """Delete schedule."""
        ...

    def validate_schedule(self, schedule: ScheduleV2, execution: ExecutionInput | None = None) -> ValidationResult:
        """Check structure, policy and source readiness without user SQL or rendering; incomplete is not valid."""
        ...

    def start_test_run(
        self, request_id: str, schedule: ScheduleV2, *, execution: ExecutionInput | None = None, as_of: Instant
    ) -> SchedulePreviewJobV2:
        """Admit a bounded read-only preview; reject effects before dispatch and send no deliveries."""
        ...

    @sql_read()
    def get_test_run(self, job_id: str) -> SchedulePreviewJobV2:
        """Read caller-authorized preview status/results until expiry."""
        ...

    def cancel_test_run(self, job_id: str, expected_version: int, request_id: str) -> SchedulePreviewJobV2:
        """Request best-effort cancellation; a nonterminal result does not confirm cessation."""
        ...

    def run_now(self, schedule_id: str, request_id: str, ignore_condition: bool = False) -> ScheduleRunV2:
        """New manual run; use retry_run to recover an existing run."""
        ...

    @sql_read(row_type=ScheduleRunSummaryV2, table="runs")
    def list_runs(
        self,
        schedule_id: str = "",
        status: str = "",
        since: Instant | None = None,
        before: Instant | None = None,
        execution_principal_id: str | None = None,
        limit: int | None = None,
    ) -> Stream[ProducerState]:
        """Summaries ordered by descending (scheduled_for, run_id); since inclusive, before exclusive.

        Execution filters match the frozen principal. Recheck current resource
        access on every continuation. Invalid/reversed time ranges are rejected.
        """
        ...

    @sql_read()
    def get_run(self, run_id: str) -> ScheduleRunV2:
        """Run history."""
        ...

    def cancel_run(self, run_id: str, expected_version: int, request_id: str) -> ScheduleRunV2:
        """Cancel run."""
        ...

    def retry_run(self, run_id: str, expected_version: int, request_id: str) -> ScheduleRunV2:
        """Resume frozen work from a safe checkpoint."""
        ...

    def resolve_run(
        self,
        run_id: str,
        expected_version: int,
        request_id: str,
        resolutions: Annotated[list[StepResolution], non_null_list(pa.struct(StepResolution.ARROW_SCHEMA))],
        note: str,
    ) -> ScheduleRunV2:
        """Record worker-verified outcomes; never replay a step."""
        ...

    def set_ownership(
        self, schedule_id: str, expected_version: int, request_id: str, ownership: Ownership
    ) -> ScheduleRecordV2:
        """Transfer management; preserve data and execution authority."""
        ...

    def set_execution_principal(
        self, schedule_id: str, expected_version: int, request_id: str, principal_id: str
    ) -> ScheduleRecordV2:
        """Select a separately authorized execution identity for future work."""
        ...

    @sql_read(row_type=RunStep)
    def list_run_steps(self, run_id: str, limit: int | None = None) -> Stream[ProducerState]:
        """Authorized snapshot ordered by step_index; attempts are separate rows."""
        ...

    @sql_read(row_type=StepAttempt)
    def list_step_attempts(self, run_id: str, step_index: int, limit: int | None = None) -> Stream[ProducerState]:
        """Authorized attempt history in ascending attempt order."""
        ...

    @sql_read(row_type=RecoveryEvent)
    def list_recovery_events(self, run_id: str, limit: int | None = None) -> Stream[ProducerState]:
        """Authorized recovery audit in stable admission order; recheck access on continuation."""
        ...

    @sql_read(row_type=DestinationResult)
    def list_run_destinations(self, run_id: str, limit: int | None = None) -> Stream[ProducerState]:
        """Authorized per-destination outcomes in original dispatch order."""
        ...
