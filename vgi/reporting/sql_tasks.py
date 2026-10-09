# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Proposed vgi.sql_tasks.v1 records and RPC interface.

These are importable protocol definitions, not a service implementation.
Behavior and worker policy are documented in docs/design/reporting-protocols/.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, ClassVar, Protocol

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import ProducerState, Stream

from vgi.reporting._arrow import Instant, Json, SchemaIpc, non_null_list
from vgi.reporting._enums import HealthState, IncrementalKind, LoadMode, SchemaChangePolicy, TargetKind, TransactionMode
from vgi.reporting._metadata import sql_read
from vgi.reporting.common import (
    Ack,
    CredentialStatus,
    DataSource,
    Destination,
    ExecutionIdentity,
    ExecutionInput,
    Ownership,
    OwnershipInput,
    ParamValue,
    ResourceMeta,
    RetryPolicy,
    Run,
    RunError,
    ServiceInfo,
    StepResolution,
    Trigger,
    TriggerPreview,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskRun(Run):
    """Task run with frozen inputs, load counts and typed watermark JSON."""

    task_id: str
    task: SqlTask
    full_refresh: bool
    watermark_before: Json | None
    watermark_after: Json | None
    rows_inserted: int | None
    rows_updated: int | None
    rows_deleted: int | None
    target_row_count: int | None


@dataclass(frozen=True, slots=True, kw_only=True)
class Target(ArrowSerializableDataclass):
    """Authorized catalog table or host target for a load."""

    kind: TargetKind
    alias: str = ""
    schema: str = "main"
    table: str
    create_if_missing: bool = True


@dataclass(frozen=True, slots=True, kw_only=True)
class LoadBody(ArrowSerializableDataclass):
    """Read-only source query and transactional load configuration."""

    setup_sql: str = ""
    query: str
    target: Target
    mode: LoadMode
    key_columns: Annotated[list[str], non_null_list(pa.string())] = field(default_factory=list)
    delete_missing: bool = False
    snapshot_column: str = "_snapshot_at"
    retain_snapshots: int = 0
    on_schema_change: SchemaChangePolicy = "fail"


@dataclass(frozen=True, slots=True, kw_only=True)
class ScriptBody(ArrowSerializableDataclass):
    """Ordered statements and explicit transaction/watermark behavior."""

    statements: Annotated[list[str], non_null_list(pa.string())]
    transaction: TransactionMode = "single"
    watermark_sql: str = ""


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskBody(ArrowSerializableDataclass):
    """Tagged load, script or vendor body; unused payloads are None."""

    kind: str
    load: LoadBody | None = None
    script: ScriptBody | None = None
    custom_json: Json | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class Incremental(ArrowSerializableDataclass):
    """Watermark mode, typed initial value and optional fixed-duration lookback."""

    kind: IncrementalKind = "none"
    cursor_column: str = ""
    initial_json: Json | None = None
    lookback: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskNotify(ArrowSerializableDataclass):
    """Notification destinations and selected task health transitions."""

    destinations: Annotated[list[Destination], non_null_list(pa.struct(Destination.ARROW_SCHEMA))] = field(
        default_factory=list
    )
    on: Annotated[list[str], non_null_list(pa.string())] = field(
        default_factory=lambda: ["failing", "recovered", "disabled"]
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlTask(ArrowSerializableDataclass):
    """Editable SQL task definition, trigger and incremental policy."""

    title: str
    description: str = ""
    trigger: Trigger
    data_sources: Annotated[list[DataSource], non_null_list(pa.struct(DataSource.ARROW_SCHEMA))]
    body: TaskBody
    incremental: Incremental = field(default_factory=Incremental)
    condition_sql: str = ""
    parameter_values: Annotated[list[ParamValue], non_null_list(pa.struct(ParamValue.ARROW_SCHEMA))] = field(
        default_factory=list
    )
    notify: TaskNotify = field(default_factory=TaskNotify)
    stale_after_seconds: int = 0
    timeout_seconds: int = 3600
    enabled: bool = True


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskHealth(ArrowSerializableDataclass):
    """Current task health and most recent success/failure observations."""

    state: HealthState
    since: Instant
    last_success_at: Instant | None
    last_failure_at: Instant | None
    last_error: RunError | None
    consecutive_failures: int


@dataclass(frozen=True, slots=True, kw_only=True)
class ResolvedTarget(ArrowSerializableDataclass):
    """Stable target identity that survives management ownership changes."""

    target_id: str
    location: str
    catalog_name: str
    schema: str
    table: str


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskRecord(ResourceMeta):
    """Persistent task definition, target mapping, progress and health."""

    task_id: str
    definition: SqlTask
    execution_identity: ExecutionIdentity
    next_fire_at: Instant | None
    last_run: TaskRun | None
    watermark_json: Json | None
    target: ResolvedTarget | None
    target_row_count: int | None
    disabled_reason: str | None
    credentials: Annotated[list[CredentialStatus], non_null_list(pa.struct(CredentialStatus.ARROW_SCHEMA))]
    health: TaskHealth


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskTest(ArrowSerializableDataclass):
    """Rollback-only test observations; unknown counts/schema are None."""

    evaluated_at: Instant
    parameter_values: Annotated[list[ParamValue], non_null_list(pa.struct(ParamValue.ARROW_SCHEMA))]
    condition_value: bool | None
    condition_evaluated: bool
    watermark_before: Json | None
    watermark_after: Json | None
    rows_inserted: int | None
    rows_updated: int | None
    rows_deleted: int | None
    target_schema: SchemaIpc | None


@dataclass(frozen=True, slots=True, kw_only=True)
class TasksInfo(ServiceInfo):
    """Supported task modes, host catalog, limits and progress guarantees."""

    body_kinds: Annotated[list[str], non_null_list(pa.string())]
    load_modes: Annotated[list[str], non_null_list(pa.string())]
    host_location: str | None
    host_catalog_name: str | None
    atomic_host_watermarks: bool
    relative_tokens: Annotated[list[str], non_null_list(pa.string())]
    tzdb_version: str
    retry_policy: RetryPolicy


class SqlTasksProtocol(Protocol):
    """Proposed vgi.sql_tasks.v1 records and RPC interface; implementations supply all methods."""

    protocol_name: ClassVar[str] = "vgi.sql_tasks.v1"
    protocol_version: ClassVar[str] = "1.0.0"

    @sql_read(table="info")
    def get_tasks_info(self) -> TasksInfo:
        """Get tasks info.

        Body kinds, modes, host store catalog and location, whether host watermarks are atomic, minimum interval,
        limits.
        """
        ...

    @sql_read()
    def preview_trigger(self, trigger: Trigger, after: Instant, count: int = 10) -> TriggerPreview:
        """As in schedules."""
        ...

    @sql_read(row_type=TaskRecord, table="tasks")
    def list_tasks(
        self, owned_by_me: bool = False, target_id: str = "", health_state: str = ""
    ) -> Stream[ProducerState]:
        """Read, with health; health_state filters ("everything failing")."""
        ...

    @sql_read()
    def get_task(self, task_id: str) -> TaskRecord:
        """Read, with health; health_state filters ("everything failing")."""
        ...

    def create_task(
        self,
        request_id: str,
        task: SqlTask,
        ownership: OwnershipInput | None = None,
        execution: ExecutionInput | None = None,
    ) -> TaskRecord:
        """Create task.

        Ownership and execution identity are independently resolved by the host; grant_required for any source
        without a usable delegation.
        """
        ...

    def update_task(self, task_id: str, expected_version: int, request_id: str, task: SqlTask) -> TaskRecord:
        """Includes pausing via enabled. Changing the target, mode or cursor column clears a stored watermark."""
        ...

    def delete_task(self, task_id: str, expected_version: int, request_id: str, drop_target: bool = False) -> Ack:
        """drop_target drops a host table; a catalog target is never dropped."""
        ...

    def test_run(self, task: SqlTask, *, execution: ExecutionInput | None = None, as_of: Instant) -> TaskTest:
        """Test run.

        Tests a load or single script in a transaction that is always rolled back; returns counts, old/new
        watermark, target schema and condition value. Refuses bodies, statements or CALLs whose effects cannot be
        rolled back, including per_statement and none scripts.
        """
        ...

    def run_now(
        self, task_id: str, request_id: str, ignore_condition: bool = False, full_refresh: bool = False
    ) -> TaskRun:
        """New manual run; use retry_run to recover an existing run.

        full_refresh runs with $watermark = NULL and, for load, replace semantics.
        """
        ...

    def set_watermark(
        self, task_id: str, expected_version: int, request_id: str, watermark_json: Json | None
    ) -> TaskRecord:
        """Backfill or reset a stored watermark; null clears it.

        Refused for derived, whose watermark is the target's data.
        """
        ...

    @sql_read(row_type=TaskRun, table="runs")
    def list_runs(self, task_id: str = "", status: str = "", since: Instant | None = None) -> Stream[ProducerState]:
        """Run history.

        task_id empty lists runs across every task the caller can read, so "what failed overnight" is one call.
        """
        ...

    @sql_read()
    def get_run(self, run_id: str) -> TaskRun:
        """Run history.

        task_id empty lists runs across every task the caller can read, so "what failed overnight" is one call.
        """
        ...

    def cancel_run(self, run_id: str, expected_version: int, request_id: str) -> TaskRun:
        """Rolls back an open transaction; per_statement keeps what already committed."""
        ...

    def retry_run(self, run_id: str, expected_version: int, request_id: str) -> TaskRun:
        """Resume frozen work from a safe checkpoint."""
        ...

    def resolve_run(
        self,
        run_id: str,
        expected_version: int,
        request_id: str,
        resolutions: Annotated[list[StepResolution], non_null_list(pa.struct(StepResolution.ARROW_SCHEMA))],
        note: str,
    ) -> TaskRun:
        """Record worker-verified outcomes; never replay a step."""
        ...

    def set_ownership(self, task_id: str, expected_version: int, request_id: str, ownership: Ownership) -> TaskRecord:
        """Transfer management; preserve data and execution authority."""
        ...

    def set_execution_principal(
        self, task_id: str, expected_version: int, request_id: str, principal_id: str
    ) -> TaskRecord:
        """Select a separately authorized execution identity for future work."""
        ...
