# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Shared reporting records.

These are importable protocol definitions, not a service implementation.
Every call runs within one tenant selected and authorized by trusted host
authentication/routing, including single-tenant installations. Resource IDs,
owner IDs and principal IDs are opaque, stable, non-recycled host-canonical
identifiers that disambiguate tenants and identity issuers. Bare provider-local
IDs are insufficient when they can collide. Keys never select or switch the
authenticated tenant; foreign-tenant resources and identities are refused.

Caches, request-id replay records, jobs and continuations are bound to the host,
authenticated tenant and caller. Reauthentication must preserve that binding;
a continuation cannot preserve revoked access. Workers evaluate current policy
and authoritative effective membership without requiring directory enumeration.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Literal

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi.reporting._arrow import Instant, Json, non_null_list
from vgi.reporting._enums import (
    CredentialState,
    DelegationKind,
    DeliveryStatus,
    EffectOutcome,
    ExecutionState,
    LimitUnit,
    ParameterType,
    ParamKind,
    RecoveryState,
    RenderStatus,
    ResolvedOutcome,
    RunStatus,
    RunTriggerKind,
    Severity,
    StepStatus,
    TriggerKind,
)

OwnershipScope = Annotated[Literal["all", "self", "member", "self_or_member"], ArrowType(pa.string())]

# Teams use group identities. One directory entity has one canonical kind/id,
# never interchangeable team/group aliases. Extensions use <vendor>.<name>.
STANDARD_OWNER_KINDS = ("principal", "group", "organization", "project", "workspace")


@dataclass(frozen=True, slots=True, kw_only=True)
class PrincipalRef(ArrowSerializableDataclass):
    """Canonical human or service actor; group owners are never execution actors.

    Labels and email are display metadata, never authority. The host resolves
    issuer/tenant mappings and eligibility; kind alone grants no permission.
    """

    id: str
    kind: Annotated[Literal["user", "service"], ArrowType(pa.string())]
    display_name: str
    email: str | None


@dataclass(frozen=True, slots=True, kw_only=True)
class OwnerKey(ArrowSerializableDataclass):
    """Host-canonical management identity, matched by exact (kind, id).

    Standard kinds are principal, group (including teams), organization,
    project and workspace; advertised extensions use <vendor>.<name>. Principal
    owner IDs equal the corresponding PrincipalRef.id, for user or service
    actors. No key implies membership, a role, or permission to act as its owner.
    """

    kind: str
    id: str


@dataclass(frozen=True, slots=True, kw_only=True)
class OwnerRef(OwnerKey):
    """Canonical owner and display label; workers never authorize supplied labels."""

    display_name: str


@dataclass(frozen=True, slots=True, kw_only=True)
class Ownership(ArrowSerializableDataclass):
    """One direct owner and one durable management parent in the authorized tenant.

    parent_owner_ref is the recorded direct authority, not the full organization
    hierarchy, directory membership or folder containment. A principal owner
    requires a non-principal durable parent; a root organizational owner may
    have no parent. Parent changes require explicit authorized ownership changes.

    Ownership never grants execution consent or credentials. Transfers preserve
    execution identity, but workers revalidate resource-bound execution consent
    after a transfer and before dispatch, disabling work if it is no longer valid.
    """

    owner_ref: OwnerRef
    parent_owner_ref: OwnerRef | None


@dataclass(frozen=True, slots=True, kw_only=True)
class OwnershipCapabilities(ArrowSerializableDataclass):
    """Finite ownership vocabulary and collection filters supported by a worker.

    Service-info policy applies to every listed resource kind, including folders;
    context-specific identity capabilities can narrow the assignable kinds.
    all/self are required scopes. member/self_or_member are optional as a pair,
    requiring nonempty membership_owner_kinds, a subset of non-principal
    owner_kinds. An empty list disables membership filtering, not exact checks.

    all selects every currently readable match; self selects resources directly
    owned by the authenticated principal (user or service). member selects only
    resources whose direct owner is an advertised membership kind and for which
    authoritative effective membership holds, including nested groups according
    to the identity authority's rules. Parent membership, authorship, delegated
    administration and ACL visibility alone never satisfy member. self_or_member
    is the deduplicated union. Every selection still requires resource access.

    Membership is evaluated server-side without client-supplied membership lists.
    Unsupported scopes fail UNIMPLEMENTED; an unavailable/incomplete membership
    decision fails UNAVAILABLE rather than returning empty or broadened results.
    """

    owner_kinds: Annotated[list[str], non_null_list(pa.string())]
    parent_owner_kinds: Annotated[list[str], non_null_list(pa.string())]
    scopes: Annotated[list[OwnershipScope], non_null_list(pa.string())]
    membership_owner_kinds: Annotated[list[str], non_null_list(pa.string())]


@dataclass(frozen=True, slots=True, kw_only=True)
class ResourceMeta(ArrowSerializableDataclass):
    """Versioned ownership and attribution shared by persistent resources.

    allowed_actions is a caller-specific hint, never a grant. Worker policy
    independently authorizes reads, edits, transfers and execution using current
    membership/roles; membership does not imply every action. Folder recursion
    and placement never rewrite explicit ownership or its management parent.
    """

    version: int
    ownership: Ownership
    created_at: Instant
    created_by: PrincipalRef
    updated_at: Instant
    updated_by: PrincipalRef
    allowed_actions: Annotated[list[str], non_null_list(pa.string())]


@dataclass(frozen=True, slots=True, kw_only=True)
class OwnershipInput(ArrowSerializableDataclass):
    """Creation-time preferences; None requests defaults in the authenticated tenant.

    Workers resolve canonical keys and labels and authorize the complete owner
    and parent assignment. Supplied display metadata cannot confer authority.
    """

    owner_ref: OwnerRef | None = None
    parent_owner_ref: OwnerRef | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class ExecutionInput(ArrowSerializableDataclass):
    """Canonical user/service execution ID; None selects the authenticated caller.

    Team/group ownership never chooses an execution actor. The host must resolve
    and separately authorize the actor, including consent and credential use.
    """

    principal_id: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class ExecutionIdentity(ArrowSerializableDataclass):
    """Resolved execution principal and its current readiness."""

    principal: PrincipalRef
    state: ExecutionState
    reason: str | None


@dataclass(frozen=True, slots=True, kw_only=True)
class Ack(ArrowSerializableDataclass):
    """Acknowledgement of an admitted mutation."""

    applied: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class Limit(ArrowSerializableDataclass):
    """Worker-chosen limit; None means unlimited and zero means zero."""

    name: str
    value: int | None
    unit: LimitUnit


@dataclass(frozen=True, slots=True, kw_only=True)
class ServiceInfo(ArrowSerializableDataclass):
    """Common advertised protocol identity, writability and limits."""

    protocol_version: str
    display_name: str
    writable: bool
    limits: Annotated[list[Limit], non_null_list(pa.struct(Limit.ARROW_SCHEMA))]


@dataclass(frozen=True, slots=True, kw_only=True)
class RetryPolicy(ArrowSerializableDataclass):
    """Worker retry budgets and backoff bounds; attempts include the first dispatch."""

    max_attempts: int
    max_elapsed_seconds: int
    initial_backoff_seconds: int
    max_backoff_seconds: int


@dataclass(frozen=True, slots=True, kw_only=True)
class DataSource(ArrowSerializableDataclass):
    """Non-secret session attachment reference with an explicit remote catalog."""

    alias: str
    attachment_id: str
    location: str
    catalog_name: str
    label: str = ""
    required: bool = True


@dataclass(frozen=True, slots=True, kw_only=True)
class DelegationKey(ArrowSerializableDataclass):
    """Exact catalog attachment or standalone service credential key."""

    kind: DelegationKind
    location: str
    catalog_name: str
    attachment_id: str


@dataclass(frozen=True, slots=True, kw_only=True)
class Delegation(DelegationKey):
    """Write-only grant and ticket; None expiry means unlimited."""

    grant: str
    ticket: str
    expires_at: Instant | None


@dataclass(frozen=True, slots=True, kw_only=True)
class DelegationWrite(ArrowSerializableDataclass):
    """Compare-and-swap credential write; version zero requires an absent key."""

    delegation: Delegation
    expected_version: int


@dataclass(frozen=True, slots=True, kw_only=True)
class DelegationRecord(DelegationKey):
    """Readable delegation metadata; never includes a grant or ticket."""

    version: int
    expires_at: Instant | None
    updated_at: Instant


@dataclass(frozen=True, slots=True, kw_only=True)
class CredentialStatus(DelegationKey):
    """Readiness of one required delegation without its credentials."""

    state: CredentialState
    expires_at: Instant | None


@dataclass(frozen=True, slots=True, kw_only=True)
class ParameterOption(ArrowSerializableDataclass):
    """One display label and JSON scalar accepted by a select parameter."""

    label: str
    value_json: Json


@dataclass(frozen=True, slots=True, kw_only=True)
class ParameterSpec(ArrowSerializableDataclass):
    """Public parameter type, default and bounded static select values.

    Workers advertise and enforce max_parameter_options; dynamic directory/data
    selectors are not encoded as unbounded option arrays.
    """

    key: str
    label: str
    type: ParameterType
    required: bool = False
    default_json: Json = "null"
    options: Annotated[list[ParameterOption], non_null_list(pa.struct(ParameterOption.ARROW_SCHEMA))] = field(
        default_factory=list
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class ParamValue(ArrowSerializableDataclass):
    """Tagged literal JSON or relative period; exactly one payload is present."""

    key: str
    kind: ParamKind
    json: Json | None = None
    relative: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class ReportRef(ArrowSerializableDataclass):
    """Report service and ID, with an optional pinned revision."""

    service_url: str
    report_id: str
    revision_id: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class Destination(ArrowSerializableDataclass):
    """Channel kind and address interpreted by the notify worker."""

    kind: str
    address: str


@dataclass(frozen=True, slots=True, kw_only=True)
class Field(ArrowSerializableDataclass):
    """A display label/value pair, also used for notification correlation entries."""

    label: str
    value: str


@dataclass(frozen=True, slots=True, kw_only=True)
class Attachment(ArrowSerializableDataclass):
    """Immutable attachment content and a service-fetchable download URL."""

    filename: str
    media_type: str
    url: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True, slots=True, kw_only=True)
class Artifact(Attachment):
    """Produced attachment with the expiry of its temporary URL."""

    expires_at: Instant | None


@dataclass(frozen=True, slots=True, kw_only=True)
class Output(ArrowSerializableDataclass):
    """Action output, optionally with a downloadable artifact and known row count."""

    artifact: Artifact | None
    media_type: str
    row_count: int | None


@dataclass(frozen=True, slots=True, kw_only=True)
class RunError(ArrowSerializableDataclass):
    """Sanitized persisted error; canonical code and stable application kind."""

    code: str
    kind: str
    location: str | None
    message: str


@dataclass(frozen=True, slots=True, kw_only=True)
class DestinationResult(ArrowSerializableDataclass):
    """One destination outcome, including ambiguous provider acceptance."""

    destination: Destination
    status: DeliveryStatus
    reason: str | None
    message: str | None
    provider_reference: str | None


@dataclass(frozen=True, slots=True, kw_only=True)
class Notification(ArrowSerializableDataclass):
    """Portable message content with explicit attribution and correlation."""

    title: str
    summary: str
    body_markdown: str = ""
    body_html: Attachment | None = None
    link_url: str = ""
    link_label: str = ""
    fields: Annotated[list[Field], non_null_list(pa.struct(Field.ARROW_SCHEMA))] = field(default_factory=list)
    image: Attachment | None = None
    attachments: Annotated[list[Attachment], non_null_list(pa.struct(Attachment.ARROW_SCHEMA))] = field(
        default_factory=list
    )
    severity: Severity = "info"
    thread_key: str = ""
    on_behalf_of: PrincipalRef
    correlation: Annotated[list[Field], non_null_list(pa.struct(Field.ARROW_SCHEMA))] = field(default_factory=list)


@dataclass(frozen=True, slots=True, kw_only=True)
class Trigger(ArrowSerializableDataclass):
    """Cron or once trigger; instants are aware UTC datetimes on the wire."""

    kind: TriggerKind
    cron: str | None = None
    run_at: Instant | None = None
    time_zone: str = "UTC"
    start_at: Instant | None = None
    end_at: Instant | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class TriggerPreview(ArrowSerializableDataclass):
    """Deterministic fire instants and the worker time-zone database version."""

    fire_times: Annotated[list[Instant], non_null_list(pa.timestamp("us", "UTC"))]
    description: str
    warnings: Annotated[list[str], non_null_list(pa.string())]
    tzdb_version: str


@dataclass(frozen=True, slots=True, kw_only=True)
class StepAttempt(ArrowSerializableDataclass):
    """Immutable observed attempt timing, effect outcome and failure."""

    attempt: int
    started_at: Instant
    finished_at: Instant | None
    effect_outcome: EffectOutcome
    error: RunError | None


@dataclass(frozen=True, slots=True, kw_only=True)
class RunStep(ArrowSerializableDataclass):
    """Step status and last attempt; full attempt history is a separate stream."""

    step_index: int
    operation_id: str
    kind: str
    status: StepStatus
    effect_outcome: EffectOutcome
    attempt_count: int
    last_attempt: StepAttempt | None
    rows_affected: int | None
    receiver_reference: str | None
    deduplication_expires_at: Instant | None


@dataclass(frozen=True, slots=True, kw_only=True)
class Recovery(ArrowSerializableDataclass):
    """Current recovery state, retry timing and actions permitted for the caller."""

    state: RecoveryState
    failed_step_index: int | None
    next_retry_at: Instant | None
    attempts_remaining: int
    reason: str | None
    allowed_actions: Annotated[list[str], non_null_list(pa.string())]


@dataclass(frozen=True, slots=True, kw_only=True)
class StepResolution(ArrowSerializableDataclass):
    """Outcome and receipt/audit evidence to be verified by the worker."""

    step_index: int
    outcome: ResolvedOutcome
    evidence_ref: str


@dataclass(frozen=True, slots=True, kw_only=True)
class RecoveryEvent(ArrowSerializableDataclass):
    """Audit record for an authorized recovery action."""

    occurred_at: Instant
    actor: PrincipalRef
    action: str
    note: str
    resolutions: Annotated[list[StepResolution], non_null_list(pa.struct(StepResolution.ARROW_SCHEMA))]


@dataclass(frozen=True, slots=True, kw_only=True)
class RunSummary(ArrowSerializableDataclass):
    """Bounded run status; step attempts and recovery history are streamed reads."""

    run_id: str
    version: int
    trigger_kind: RunTriggerKind
    scheduled_for: Instant
    admitted_at: Instant
    started_at: Instant | None
    finished_at: Instant | None
    status: RunStatus
    skip_reason: str | None
    execution_principal: PrincipalRef
    resource_version: int
    error: RunError | None
    recovery: Recovery
    allowed_actions: Annotated[list[str], non_null_list(pa.string())]


@dataclass(frozen=True, slots=True, kw_only=True)
class Run(RunSummary):
    """Frozen run inputs and bounded outputs, without nested histories.

    list_run_steps, list_step_attempts, list_recovery_events and
    list_run_destinations expose independently consumable history rows.
    """

    parameter_values: Annotated[list[ParamValue], non_null_list(pa.struct(ParamValue.ARROW_SCHEMA))]
    outputs: Annotated[list[Output], non_null_list(pa.struct(Output.ARROW_SCHEMA))]


@dataclass(frozen=True, slots=True, kw_only=True)
class ValidationIssue(ArrowSerializableDataclass):
    """A safe field-specific validation failure or incomplete check."""

    field: str
    code: str
    message: str


@dataclass(frozen=True, slots=True, kw_only=True)
class ValidationResult(ArrowSerializableDataclass):
    """Advisory validation without user SQL or rendering.

    complete=False means one or more checks could not finish; valid cannot be
    true in that case. A valid result never grants future execution authority.
    Workers bound issues and report truncation explicitly through complete=False.
    """

    valid: bool
    complete: bool
    issues: Annotated[list[ValidationIssue], non_null_list(pa.struct(ValidationIssue.ARROW_SCHEMA))]


@dataclass(frozen=True, slots=True, kw_only=True)
class PreviewJob(ArrowSerializableDataclass):
    """Versioned, caller-authorized execution preview with a typed child result.

    Starts deduplicate request_id. A worker may complete synchronously. Success
    has a result, failure an error, and terminal jobs have zero polling delay.
    Cancellation is best effort; results expire at expires_at. Tests preserve
    the original read-only/rollback-only restrictions and never deliver messages.

    Preview services advertise finite max_preview_seconds (seconds) and
    max_preview_result_bytes (bytes) in their service limits. Exceeding either
    fails the job with RESOURCE_EXHAUSTED, never silently truncates a success.
    """

    job_id: str
    version: int
    status: RenderStatus
    created_at: Instant
    finished_at: Instant | None
    poll_after_seconds: int
    expires_at: Instant
    error: RunError | None
    allowed_actions: Annotated[list[str], non_null_list(pa.string())]


@dataclass(frozen=True, slots=True, kw_only=True)
class KeyValue(ArrowSerializableDataclass):
    """Lossless tagged scalar used by canonical alert keys and watermark JSON."""

    type: str
    value: str
