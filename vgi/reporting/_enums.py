# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Known string choices, encoded as utf8 rather than dictionary-encoded Enum.

Literal annotations assist callers and reviewers. Workers enforce allowed input
values; clients must still tolerate unknown output values from newer workers.
Plain strings are retained for explicitly open vendor kinds and action hints.
"""

from typing import Annotated, Literal

import pyarrow as pa
from vgi_rpc import ArrowType

ExecutionState = Annotated[Literal["ready", "needs_credentials", "unavailable"], ArrowType(pa.string())]
CredentialState = Annotated[Literal["ok", "expiring", "expired", "missing"], ArrowType(pa.string())]
LimitUnit = Annotated[Literal["bytes", "rows", "seconds", "count", "utf8_bytes"], ArrowType(pa.string())]
ParameterType = Annotated[
    Literal["text", "number", "date", "boolean", "select", "multi_select", "date_range"], ArrowType(pa.string())
]
ParamKind = Annotated[Literal["literal", "relative"], ArrowType(pa.string())]
Severity = Annotated[Literal["info", "warning", "critical"], ArrowType(pa.string())]
DeliveryStatus = Annotated[Literal["accepted", "refused", "failed", "unknown"], ArrowType(pa.string())]
TriggerKind = Annotated[Literal["cron", "once"], ArrowType(pa.string())]
EffectOutcome = Annotated[Literal["none", "committed", "rolled_back", "partial", "unknown"], ArrowType(pa.string())]
ResolvedOutcome = Annotated[Literal["committed", "rolled_back", "partial"], ArrowType(pa.string())]
StepStatus = Annotated[Literal["pending", "running", "succeeded", "failed", "skipped"], ArrowType(pa.string())]
RecoveryState = Annotated[
    Literal[
        "none", "retry_scheduled", "retryable", "needs_reauthorization", "needs_resolution", "exhausted", "resolved"
    ],
    ArrowType(pa.string()),
]
RunStatus = Annotated[
    Literal["pending", "running", "retry_wait", "succeeded", "skipped", "failed", "cancelled"], ArrowType(pa.string())
]
RunTriggerKind = Annotated[Literal["schedule", "manual", "test"], ArrowType(pa.string())]
RevisionKind = Annotated[Literal["edit", "agent", "restore", "import"], ArrowType(pa.string())]
RenderStatus = Annotated[Literal["queued", "running", "succeeded", "failed", "cancelled"], ArrowType(pa.string())]
ReportTracking = Annotated[Literal["pinned", "published", "head"], ArrowType(pa.string())]
OutputFormat = Annotated[Literal["none", "arrow", "parquet", "csv"], ArrowType(pa.string())]
InlineMode = Annotated[Literal["none", "summary", "image", "table"], ArrowType(pa.string())]
TargetKind = Annotated[Literal["catalog", "host"], ArrowType(pa.string())]
LoadMode = Annotated[Literal["replace", "append", "merge", "snapshot"], ArrowType(pa.string())]
SchemaChangePolicy = Annotated[Literal["fail", "add_columns"], ArrowType(pa.string())]
TransactionMode = Annotated[Literal["single", "per_statement", "none"], ArrowType(pa.string())]
IncrementalKind = Annotated[Literal["none", "derived", "stored"], ArrowType(pa.string())]
HealthState = Annotated[Literal["healthy", "failing", "stale", "paused", "disabled", "new"], ArrowType(pa.string())]
AlertRuleState = Annotated[Literal["ok", "firing", "error", "disabled"], ArrowType(pa.string())]
AlertInstanceState = Annotated[
    Literal["pending", "firing", "resolving", "resolved", "superseded"], ArrowType(pa.string())
]
AlertEventKind = Annotated[
    Literal[
        "transition",
        "notification",
        "acknowledged",
        "unacknowledged",
        "snoozed",
        "unsnoozed",
        "subscribed",
        "unsubscribed",
        "rule_error",
        "rule_recovered",
        "superseded",
    ],
    ArrowType(pa.string()),
]
DelegationKind = Annotated[Literal["catalog", "service"], ArrowType(pa.string())]
