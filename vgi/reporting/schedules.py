# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Proposed vgi.schedules.v1 records and RPC interface.

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
from vgi.reporting._enums import InlineMode, OutputFormat, ReportTracking
from vgi.reporting._metadata import sql_read
from vgi.reporting.common import (
    Ack,
    CredentialStatus,
    DataSource,
    Destination,
    ExecutionIdentity,
    ExecutionInput,
    Notification,
    Output,
    Ownership,
    OwnershipInput,
    ParamValue,
    ReportRef,
    ResourceMeta,
    RetryPolicy,
    Run,
    ServiceInfo,
    StepResolution,
    Trigger,
    TriggerPreview,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class ScheduleRun(Run):
    """Schedule run with its frozen definition and selected report revision."""

    schedule_id: str
    schedule: Schedule
    revision_id: str | None


@dataclass(frozen=True, slots=True, kw_only=True)
class RenderReportAction(ArrowSerializableDataclass):
    """Pinned or tracked report rendering action."""

    report: ReportRef
    track: ReportTracking = "published"
    outputs: Annotated[list[str], non_null_list(pa.string())] = field(default_factory=lambda: ["application/pdf"])


@dataclass(frozen=True, slots=True, kw_only=True)
class RunQueryAction(ArrowSerializableDataclass):
    """SQL setup/query action and requested output format."""

    setup_sql: str = ""
    sql: str
    output_format: OutputFormat = "parquet"


@dataclass(frozen=True, slots=True, kw_only=True)
class Action(ArrowSerializableDataclass):
    """Tagged schedule action; only the selected payload is populated."""

    kind: str
    render_report: RenderReportAction | None = None
    run_query: RunQueryAction | None = None
    data_sources: Annotated[list[DataSource], non_null_list(pa.struct(DataSource.ARROW_SCHEMA))] = field(
        default_factory=list
    )
    custom_json: Json | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class Delivery(ArrowSerializableDataclass):
    """Destinations and presentation preferences for one action delivery."""

    destinations: Annotated[list[Destination], non_null_list(pa.struct(Destination.ARROW_SCHEMA))]
    inline: InlineMode = "summary"
    attach: Annotated[list[str], non_null_list(pa.string())] = field(default_factory=list)


@dataclass(frozen=True, slots=True, kw_only=True)
class Schedule(ArrowSerializableDataclass):
    """Editable schedule definition; ownership and execution identity are separate."""

    title: str
    trigger: Trigger
    action: Action
    condition_sql: str = ""
    parameter_values: Annotated[list[ParamValue], non_null_list(pa.struct(ParamValue.ARROW_SCHEMA))] = field(
        default_factory=list
    )
    deliveries: Annotated[list[Delivery], non_null_list(pa.struct(Delivery.ARROW_SCHEMA))] = field(default_factory=list)
    enabled: bool = True


@dataclass(frozen=True, slots=True, kw_only=True)
class ScheduleRecord(ResourceMeta):
    """Persistent schedule definition, management identity and current status."""

    schedule_id: str
    definition: Schedule
    execution_identity: ExecutionIdentity
    next_fire_at: Instant | None
    last_run: ScheduleRun | None
    disabled_reason: str | None
    credentials: Annotated[list[CredentialStatus], non_null_list(pa.struct(CredentialStatus.ARROW_SCHEMA))]


@dataclass(frozen=True, slots=True, kw_only=True)
class ScheduleTest(ArrowSerializableDataclass):
    """Safe preview results and prospective messages without delivery."""

    evaluated_at: Instant
    parameter_values: Annotated[list[ParamValue], non_null_list(pa.struct(ParamValue.ARROW_SCHEMA))]
    condition_value: bool | None
    condition_evaluated: bool
    outputs: Annotated[list[Output], non_null_list(pa.struct(Output.ARROW_SCHEMA))]
    messages: Annotated[list[Notification], non_null_list(pa.struct(Notification.ARROW_SCHEMA))]


@dataclass(frozen=True, slots=True, kw_only=True)
class SchedulerInfo(ServiceInfo):
    """Supported schedule actions, clocks, limits and retry policy."""

    action_kinds: Annotated[list[str], non_null_list(pa.string())]
    output_formats: Annotated[list[str], non_null_list(pa.string())]
    relative_tokens: Annotated[list[str], non_null_list(pa.string())]
    tzdb_version: str
    retry_policy: RetryPolicy


class SchedulesProtocol(Protocol):
    """Proposed vgi.schedules.v1 records and RPC interface; implementations supply all methods."""

    protocol_name: ClassVar[str] = "vgi.schedules.v1"
    protocol_version: ClassVar[str] = "1.0.0"

    @sql_read(table="info")
    def get_scheduler_info(self) -> SchedulerInfo:
        """Action kinds, output formats, minimum interval, relative tokens, limits."""
        ...

    @sql_read()
    def preview_trigger(self, trigger: Trigger, after: Instant, count: int = 10) -> TriggerPreview:
        """Next N fire times, a plain-English description, DST warnings."""
        ...

    @sql_read(row_type=ScheduleRecord, table="schedules")
    def list_schedules(
        self, action_kind: str = "", report_id: str = "", owned_by_me: bool = False
    ) -> Stream[ProducerState]:
        """Read."""
        ...

    @sql_read()
    def get_schedule(self, schedule_id: str) -> ScheduleRecord:
        """Read."""
        ...

    def create_schedule(
        self,
        request_id: str,
        schedule: Schedule,
        ownership: OwnershipInput | None = None,
        execution: ExecutionInput | None = None,
    ) -> ScheduleRecord:
        """Create schedule.

        Ownership and execution identity are independently resolved by the host; grant_required if the execution
        principal lacks a grant for a required location.
        """
        ...

    def update_schedule(
        self, schedule_id: str, expected_version: int, request_id: str, schedule: Schedule
    ) -> ScheduleRecord:
        """Includes pausing and resuming via enabled."""
        ...

    def delete_schedule(self, schedule_id: str, expected_version: int, request_id: str) -> Ack:
        """Delete schedule."""
        ...

    def test_run(self, schedule: Schedule, *, execution: ExecutionInput | None = None, as_of: Instant) -> ScheduleTest:
        """Preview unsaved read-only work at as_of; return outputs, condition and prospective messages.

        Reject persistent/external effects before dispatch; deliver nothing.
        """
        ...

    def run_now(self, schedule_id: str, request_id: str, ignore_condition: bool = False) -> ScheduleRun:
        """New manual run; use retry_run to recover an existing run."""
        ...

    @sql_read(row_type=ScheduleRun, table="runs")
    def list_runs(self, schedule_id: str = "", status: str = "", since: Instant | None = None) -> Stream[ProducerState]:
        """Run history."""
        ...

    @sql_read()
    def get_run(self, run_id: str) -> ScheduleRun:
        """Run history."""
        ...

    def cancel_run(self, run_id: str, expected_version: int, request_id: str) -> ScheduleRun:
        """Cancel run."""
        ...

    def retry_run(self, run_id: str, expected_version: int, request_id: str) -> ScheduleRun:
        """Resume frozen work from a safe checkpoint."""
        ...

    def resolve_run(
        self,
        run_id: str,
        expected_version: int,
        request_id: str,
        resolutions: Annotated[list[StepResolution], non_null_list(pa.struct(StepResolution.ARROW_SCHEMA))],
        note: str,
    ) -> ScheduleRun:
        """Record worker-verified outcomes; never replay a step."""
        ...

    def set_ownership(
        self, schedule_id: str, expected_version: int, request_id: str, ownership: Ownership
    ) -> ScheduleRecord:
        """Transfer management; preserve data and execution authority."""
        ...

    def set_execution_principal(
        self, schedule_id: str, expected_version: int, request_id: str, principal_id: str
    ) -> ScheduleRecord:
        """Select a separately authorized execution identity for future work."""
        ...
