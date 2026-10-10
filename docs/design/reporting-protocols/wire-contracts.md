# Reporting wire contracts

Normative behavior for all seven proposed reporting protocols. The authoritative
fields, types, defaults and RPC signatures are real Python dataclasses and
`typing.Protocol` interfaces in `vgi/reporting/`, reproduced in the generated
[Python reference](python-contracts.md). This document defines behavior that
cannot be expressed by type annotations: authorization, lifecycle transitions,
retry evidence, scheduling and scalar encodings. These protocols are proposed;
no reporting service implementation is registered by importing their definitions.

## Python types and RPC envelopes

Every record inherits `ArrowSerializableDataclass` and declares its fields with
Python annotations. Records are frozen, keyword-only dataclasses. Nested defaults
use `field(default_factory=...)`; `None` alone marks nullable fields, and a
nullable field without a default is still required. Each class exposes the
framework-derived `ARROW_SCHEMA`. Known string choices use `Literal` with an
explicit UTF-8 Arrow encoding; open vendor kinds remain strings. Clients must
handle unknown output values without assuming success.

Methods take direct named parameters. Scalar arguments become individual typed
RPC columns; structured arguments such as `ReportEnvelope`, `Schedule` and
`Notification` remain dataclasses encoded as binary IPC columns. Unary dataclass
results use `result: binary`. Nested dataclasses inside a record are structs.
Defaults live in method signatures; clients materialize them before serialization.
No signature has a mutable default. For optional method arguments, `tags=None`
means no tag filter (equivalent to `[]`), and `ownership=None` or `execution=None`
means the same worker policy defaults as an empty `OwnershipInput()` or
`ExecutionInput()`. These equivalences do not apply to other nullable fields.
Producer methods use `Stream[ProducerState]`; their explicit `@sql_read(row_type=...)`
annotation fixes the output row schema without prescribing a worker's state type.
There is no change to the vgi-rpc transport.

Python `int`, `str`, `bytes`, `float` and `bool` map to int64, UTF-8, binary,
float64 and bool. `Instant` is an aware `datetime` annotated as UTC microseconds.
`SchemaIpc` and `RowIpc` accept actual Arrow schemas and record batches, encoded
as binary IPC; a row batch must contain exactly one row. All list elements are
explicitly non-null in the Arrow schema. `Json` is JSON text, validated by the
worker. These aliases and all record definitions appear in the generated reference.

Type annotations define serialization, not authorization or all input validation.
Workers enforce the semantic constraints below, including finite numeric values,
valid JSON, aware times and compatible tagged payloads. No implicit string/number
conversion is allowed. Counts, durations and byte sizes are nonnegative; versions
start at 1 and monotonically increase. Empty strings/lists and None differ.

IDs are nonempty opaque UTF-8 strings, scoped to the hosting service and resource
kind, compared byte-for-byte. Input `request_id` is 1–128 printable ASCII bytes.
Run IDs specifically are randomly generated UUIDv4 strings in lowercase
hyphenated form, allowing operation IDs to be shared with other services.
Enum and field names are case-sensitive. `Json` is an alias for `str`
containing one valid JSON value, with duplicate object keys rejected; it is not
a new Arrow type. `SchemaIpc` serializes one Arrow IPC Schema message.
`RowIpc` serializes a complete Arrow IPC stream with its schema,
exactly one record batch of one row, and EOS; no external dictionaries or buffers.

`ServiceInfo.protocol_version` is exactly `"1.0.0"` for these initial contracts.
All instants are UTC microseconds since the Unix epoch. JSON examples use RFC
3339 UTC strings only for readability. Reporting expiry fields use `null` for
no expiry; adapters convert the shipped export helper's NULL or positive
infinity to this value. They never send JSON Infinity. Other Identity/ticket
encodings are unchanged. A credential expiry supplied by a caller is advisory;
the host checks validity with the issuer and must not extend authority from it.

## Shared records

Records and signatures: [generated Python definitions](reference/common.md).

`OwnerRef.kind` is `principal`, `group`, `project`, `workspace`, or a
`<vendor>.<name>` kind. IDs are resolved by the hosting service, not by a global
directory. Input display names and email are never authority; the service
resolves authoritative labels. `OwnershipInput` nulls ask the host to choose
its default ownership and durable parent. The returned `Ownership` is fully
resolved; a personal owner must have a durable parent able to manage the object.
Root organizational owners may have no parent. `ExecutionInput.principal_id =
null` selects the authenticated caller on create/test, never on an identity
change. `ExecutionIdentity.state` is `ready`, `needs_credentials`, or
`unavailable`; `reason` is null only for `ready`.

`CredentialStatus.state` is `ok`, `expiring`, `expired`, or `missing`.
`DelegationKey.kind` is `catalog` or `service`; service keys have empty catalog
and attachment strings and an empty ticket. Catalog values require all three
nonempty. Keys additionally belong to the authenticated execution principal's
delegation namespace; that principal is not a caller-selectable field of put,
list or revoke. Optional sources may be absent; aliases must be unique.

`Limit.unit` is `bytes`, `rows`, `seconds`, `count`, or `utf8_bytes`. A null value
means unlimited; zero means zero, not unknown. Each info record below names its
required limits. Implementations choose their values and may add vendor-prefixed
limits. `RetryPolicy.max_attempts` counts the initial attempt and is at least 1;
1 disables automatic retries. The elapsed budget starts at first dispatch of
that step. Backoff and jitter are worker policy within the advertised bounds;
`next_retry_at` reports the actual chosen time. Receiver deduplication expiry
and replay safety can cut this budget short.

`DestinationResult.status` is `accepted`, `refused`, `failed`, or `unknown`,
with the meanings in [notify.md](notify.md#delivery-and-retry-outcomes).
Correlation fields use `Field.label` as key, sorted by UTF-8 bytes, without
duplicates. Digests are lowercase SHA-256 hex. Download URLs may expire without
changing resource versions; they are temporary access links, not content IDs.

## Lists and SQL rows

Producer methods have an explicit row dataclass and emit batches with exactly
its `ARROW_SCHEMA`. There is **no application-level page size, page token, or page
envelope** in v1. HTTP pagination uses the existing vgi-rpc producer continuation
contract: its zero-row control batch carries `vgi_rpc.stream_state#b64` and the
call's `vgi_rpc.call_state#b64` in IPC custom metadata. The SDK resumes the same
method at `/<method>/exchange` using the transport's opaque resume state. It
must preserve protocol routing and authentication. Pipe transports stream to
EOS directly. SQL adapters consume these continuations and never expose control
batches as rows. This clarifies the previous phrase “paged by continuation
tokens”; it does not invent another pagination protocol.

Filters are fixed at stream admission. Lists sort by the resource ID ascending
using UTF-8 byte order, except revisions by descending revision number, runs by
descending `(scheduled_for, run_id)`, and events by ascending `(occurred_at,
event_id)`. Delegations sort by the four key fields and destinations by
`(kind, address)`, in declaration order. Each stream freezes its matching IDs
and row revisions at admission; later creations/edits do not enter that stream.
Access is checked again on continuation: revoked rows are omitted, and access
to a required parent being revoked fails `not_found`. Deletions/redactions may
remove rows; a snapshot must never resurrect redacted bodies. If the worker
can no longer honor the cursor, it fails `cursor_expired` / `FAILED_PRECONDITION`;
the client restarts explicitly. Workers choose cursor retention and storage.
Replaying the same cursor must return the same authorized suffix or an explicit
cursor error; it must not advance a live cursor twice and silently skip rows.

Empty text/list filters select all accessible objects. Folder selectors use the
root/recursive rules below. Report queries inspect the served envelope title, description and tags, joined with newlines: normalize both the query and searchable text to NFC and lowercase Unicode, then compare a literal substring. Leading/trailing query whitespace is ignored. This does not search report bodies. Schedule/task/rule predicates inspect their `definition` fields; their `query` remains a case-sensitive literal substring of title (destination display name or address for destinations). Queries are not SQL LIKE or full-text syntax. Tags require all supplied tags. `owned_by_me` means direct
principal ownership by the caller, not authorship or team membership. `since`
is inclusive: runs compare `scheduled_for`, instances `updated_at`, and events
`occurred_at`. Status/kind/state filters are exact. No implicit server result
limit may silently truncate a stream; quota failure is explicit.

SQL tables flatten exactly the corresponding list row record. Reports add the
nullable `body` fetched through `get_report(report_id, revision_served)`. A get
must return that revision or fail; the adapter never substitutes head. Redacted
bodies are null. The table retains the metadata from its list row, even if a
concurrent get returns newer resource-level pointers. Other list/get pairs have
identical row schemas and require no fetched fields. Permission hints may be
stale; every RPC enforces current access.

## Ownership and execution lifecycle

Authorship is immutable historical attribution. Removing an author from an
identity provider never implies deleting reports, folders, tasks, rules, or their data.
The worker chooses ownership inheritance, offboarding integration, and identity
resolution. The interface exposes `ownership`, `created_by`, and (for runnable
resources) `execution_identity`; clients must not infer any one from another.
Parent management authority allows an authorized administrator to transfer an
orphaned resource even when its personal owner no longer exists. It does not
grant automatic access to data on another worker.

`set_ownership` (or `set_folder_ownership` for folders) changes management ownership, increments resource `version`,
and preserves attribution, execution identity, delegations, target identity and
data. It requires `transfer_ownership` under current policy and acceptance of
the destination owner/parent by the host. No credentials are copied or revealed.
The host can apply the same transition after its own offboarding event; no
standard identity-provider event consumer is required. Authorization changes
and transfers record actor and time in the worker's audit store.

`set_execution_principal` is separate and requires `change_execution_principal`.
The worker must validate both the caller's right to use the proposed identity
and that identity's consent/delegations; naming an ID never authorizes
impersonation. It requires no active run, live evaluator, or unresolved effects.
It atomically changes the identity, increments `version`, and disables future
execution until the required delegations are ready and a manager enables it.
It never edits a run's frozen principal. Selecting the current principal is
an idempotent no-op after the same authorization and precondition checks. The grants used remain those issued
by each source worker to the selected execution identity; ambient scheduler
credentials are not a fallback. A worker may support an independent service
identity through its own supported grant-provisioning policy; v1 does not add
an impersonation or grant-issuance API. Binding a different execution identity also revalidates configured
destinations, subscribers and historical-detail access; administrative ownership
alone cannot authorize disclosure of that identity's data.

If execution authority disappears, preserve objects and data, stop new dispatch,
and expose `needs_credentials` or `unavailable`. Parent administrators may
provision a replacement execution identity. A management transfer alone cannot
revive revoked personal grants. A host target remains at its existing stable
target ID on transfer; a worker using per-owner physical stores must retain the
authorized mapping or migrate it itself without granting the new execution
identity access to unrelated tables. Data-worker authorization remains final.

## Triggers and parameter values

Records and signatures: [generated Python definitions](reference/common.md).

For `kind = once`, `run_at` is required and `cron` is null. For `cron`, the
reverse holds. Start is inclusive, end exclusive, and start must precede end.
`preview_trigger.after` is exclusive; its explicit instant makes preview
deterministic. `count` is 1–100. Trigger instants must be in Gregorian years 1–4095; the
exclusive projection horizon is 4096-01-01T00:00:00Z, matching the reference
crontimes horizon. Preview returns fewer results when the bounded trigger or
that horizon has fewer occurrences, including an impossible calendar date. Time zones are IANA names, including
UTC; workers publish their IANA database release in info and preview.

The v1 cron grammar is exactly five fields: minute 0–59, hour 0–23, day 1–31,
month 1–12, weekday 0–7 (0 and 7 are Sunday). Accept `*`, comma lists, inclusive
numeric ranges, and `/positive_step` on a wildcard or range. Reject names,
macros, seconds/year fields, `?`, `L`, `W`, and `#`. Steps start at the range's
lower bound. Minute/hour/month must match; when both day fields are restricted,
day-of-month OR day-of-week matches; otherwise the restricted field controls.
A day field spanning its entire domain is unrestricted. Evaluate local wall
minutes: a nonexistent spring-forward minute does not fire; both distinct UTC
instants of a repeated fall-back minute fire. Run uniqueness uses the UTC
instant. A worker using croner/vgi-crontimes must enforce this profile even if
its library accepts more syntax or uses different DST defaults.

After downtime, coalesce missed eligible occurrences into one run for the most
recent missed instant; `scheduled_for` is that instant, not restart time. An
overlapping firing is recorded as skipped rather than deferred. Manual runs
use their admission instant. Relative parameters resolve from `scheduled_for`
in the trigger's zone; alert evaluation uses its admitted evaluation instant.
Persist the resolved values before dispatch and preserve them across retries.

`ParamValue.kind = literal` requires `json` and null `relative`; `relative`
requires the reverse. Duplicate/unknown keys are errors. Parameter types map
to JSON as follows: `text` to string, `number` to finite IEEE binary64,
`boolean` to boolean, `date` to `YYYY-MM-DD`, `date_range` to exactly
`{"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}`, `select` to one option's JSON
scalar, and `multi_select` to a duplicate-free array of option scalars. SQL
bindings are respectively VARCHAR, DOUBLE, BOOLEAN, DATE, STRUCT(start DATE,
end DATE), the homogeneous option type, and LIST of that type. Options must
be homogeneous non-null strings, booleans or finite numbers; other types
require an empty options list. A required value cannot be absent or JSON null.
An omitted value uses its declared default; optional null binds SQL NULL.
Undeclared query/task parameters may use the same JSON scalar/array/date-range
shapes; strings without a ParameterSpec bind VARCHAR, not an inferred DATE.

Date ranges are half-open `[start, end)` with start strictly before end. Relative
tokens resolve using local calendar dates: `today` and `yesterday` are dates;
`last_n_days:N` is `[today-N, today)` (N positive); `previous_week` is the
preceding Monday-to-Monday range; `previous_month` and `previous_quarter` are
complete preceding calendar periods; `month_to_date` and `year_to_date` end
at tomorrow's local date, including today. A date token supplied to a
date-range parameter expands to that date and its following day. Range tokens
are invalid for scalar parameters. No 24-hour arithmetic substitutes for
calendar days around DST.

## Run state and recovery

Records and signatures: [generated Python definitions](reference/common.md).

Run status is `pending`, `running`, `retry_wait`, `succeeded`, `skipped`,
`failed`, or `cancelled`. A failed run may be explicitly resumed; its attempt
and recovery history remains immutable. `trigger_kind` is `schedule`, `manual`,
or `test`. Skip reasons are `condition_false`, `condition_null`, or `overlap`.
Steps use `pending`, `running`, `succeeded`, `failed`, or `skipped`.
Effect outcomes are `none`, `committed`, `rolled_back`, `partial`, or `unknown`.
Step indices are zero-based and stable; `(run_id, step_index)` is the logical
operation identity. `operation_id` is lowercase SHA-256 of UTF-8 canonical JSON
`["run", run_id, decimal_string(step_index)]`. Persist it before dispatch and
use it as the receiver request ID where supported; retries never regenerate
it from mutable resource state. Delivery steps each have a distinct index.
Step kinds are `prepare`, `condition`, `query`, `statement`, `load`, `commit`,
`save_watermark`, `render`, `store_output`, `notify`, or `<vendor>.<name>`.
Attempt numbers start at 1. Schedule/task snapshots are frozen on admission. Every observed failure is stored on its
attempt before retrying; log output alone is insufficient. Current `error` is
null after successful recovery but the attempt error is retained.

Recovery state is `none`, `retry_scheduled`, `retryable`, `needs_reauthorization`,
`needs_resolution`, `exhausted`, or `resolved`. Only `retry_scheduled` has a
non-null `next_retry_at` and run status `retry_wait`. `retryable` means a safe
manual retry is available; `exhausted` means the automatic budget is spent,
not that manual replay is safe. `allowed_actions` is the caller-specific subset
of `retry`, `reauthorize`, `resolve`, and `cancel`. `reauthorize` directs the
client to the existing delegation flow, not a new credential mutation method.

Use the evidence table in [schedules.md](schedules.md#execution-and-retries).
Transient, safely replayable work retries with bounded backoff; invalid SQL or
policy refusals fail without automatic retry; missing/rejected grants require
reauthorization; unknown/partial effects require resolution. Authentication
refresh cannot turn unknown writes into safe retries. Ordinary safe failures
need not block a later firing; unresolved effects do. Worker policy controls
retry budgets and repeated-failure disabling, within these safety rules.

`retry_run` resumes the same run and frozen inputs/principal at its first safely
incomplete step. It never repeats confirmed committed steps, re-resolves dates,
or generates new operation IDs. It requires `retry`, matching run version,
usable credentials for the same frozen identity and attachment semantics,
no competing active run, and proof the old executor stopped
or is fenced at the receiver. It can add a new bounded attempt budget under
worker policy and records a recovery event; that budget never extends receiver
deduplication retention. If required outputs/snapshots are unavailable, refuse
`recovery_required` / `FAILED_PRECONDITION`. Corrected SQL, a new execution
identity, or different parameters require a new `run_now`, not retry.

`resolve_run` requires `resolve`, matching run version, a nonempty note, and
one resolution per affected step without duplicates. Outcomes are `committed`,
`rolled_back`, or `partial`; `evidence_ref` is a nonempty worker-interpreted
receipt or audit reference. The hosting worker validates evidence and executor
cessation, or refuses `recovery_required`; a caller assertion alone is not proof.
The RPC atomically records accepted evidence and increments run version. It
does not execute SQL, send messages, or declare the run successful. A committed
step can be resumed past only if its outputs/progress are recoverable. A partial
step is acknowledged as partial and never automatically replayed. If the worker
cannot establish a usable checkpoint, the run remains failed and has no retry
action. When all ambiguity is resolved, recovery is `resolved`; future new work
still requires an authorized explicit re-enable of the resource. History keeps
the original outcome and the later resolution separately.

`run_now` creates a new run with a new operation identity. Automatic firings
deduplicate on resource ID and scheduled instant; separate manual admissions
use request IDs and may share a clock instant. It is not a recovery
shortcut: any unresolved effects or possibly live writer block admission.
Cancellation is a request to stop, not evidence of rollback. Return `cancelled`
only after cessation; persist committed/partial effects and require resolution
for unknown outcomes. Run versions advance with every persisted state change.

## Reports contract

Records and signatures: [generated Python definitions](reference/reports.md).

Reports info requires `max_body_bytes: bytes` and `max_parameters: count` in
limits, plus `max_folder_depth: count` and `max_folder_name_bytes: bytes`.
The latter notation specifies limit name and unit, not extra fields. Root has
depth zero; a folder directly below root has depth one. Limits may be unlimited
using the shared null convention. Read-only service info advertises no root
mutation actions.

`ReportRow.envelope` is the selected visible revision: head for an editor,
otherwise published. With `published_only`, both list and its fetched body
use the published revision, including for editors. Unpublished reports are
omitted for non-editors. `get_report(null)` selects head for editors and
published for others; explicit historical/draft access is checked by policy.
Revision enumeration filters revisions by that same access policy. Redacted
rows have null envelope, body/hash; metadata remains. Published fields on a
revision describe its most recent publication, not a publication history.
Report metadata version increments for commits, publication, redaction, moves and
ownership changes; content revisions remain separately immutable. Null
`publish.revision_id` unpublishes; null expected pointer means currently
unpublished. SQL uses `envelope.title` for the revision's title and `folder_id`
for the report's current location. There is no revision-level `path` field;
restoring an old revision never moves the report to a historical folder.

### Report folders

`FolderRecord` is a persistent, versioned resource, including when empty. It
has one stable, never-reused `folder_id` and one nullable `parent_folder_id`.
Null denotes the virtual root of this report service; root has no folder ID,
record, owner or mutable version. It cannot be renamed, moved or deleted.
`ReportRow.folder_id` is likewise null for root. An empty string is invalid
for either selector. Each live report has exactly one folder or root location;
shortcuts and multiple parents are outside this contract.

Folder names are nonempty Unicode strings, normalized to NFC by the worker
before validation and storage. Names cannot be `.` or `..`, contain `/`,
backslash, or ASCII control characters (U+0000–001F and U+007F), or start/end
with Unicode whitespace. Length limits count UTF-8 bytes after normalization.
Sibling folder names are unique under case-sensitive comparison of the
normalized name, including at root. Duplicate names fail `ALREADY_EXISTS` with kind `folder_name_in_use`, without
revealing a hidden sibling. Report titles are independent and may duplicate
each other or a folder name. IDs, not display paths, identify every operation.

`list_folders(parent_folder_id, recursive)` returns children of that folder,
excluding the selected folder itself. `list_reports(folder_id, recursive)`
returns reports in that folder. In either call, null selects root and
`recursive=False` restricts results to direct children; `True` includes all
descendants. Both default to root with recursion enabled, giving whole-library
SQL tables. A non-null selector must name a readable existing folder or fail
`not_found`. Returned rows reflect the worker's authorization decisions under
the shared visibility conventions. Hidden ancestor names and child counts are
not synthesized into rows. ID ordering and continuation
snapshots follow the shared list rules; selected-subtree membership is frozen
at admission, while continuation access checks still apply.
SQL equality or `IS NULL` predicates on `folder_id`/`parent_folder_id` describe
direct membership. They may be pushed into these selectors only with
`recursive=False`; otherwise the adapter must filter locally.

`create_report` places the report atomically in its requested folder.
`move_report` changes only its location and resource version/audit metadata;
all revisions, published pointers, report references and explicit ownership
remain intact. Reads of old revisions still return current report placement.
`update_folder` requires both the complete target name and parent, so null
means move to root, never leave unchanged. It increments only that folder's
version/audit metadata. Descendant IDs, explicit ownership, versions and
report revision histories remain unchanged. Changing a folder's parent cannot
make it its own ancestor or exceed the depth limit for any descendant.

All mutations use the shared request-ID convention. Existing-folder mutations
and report moves require the target resource's `expected_version`. Creating,
moving or deleting children does not increment a parent's version. The worker
must atomically validate destination existence, naming uniqueness, cycle/depth
constraints and authorization with the update. This includes concurrent opposing
moves and a destination deleted while a child is being created/moved into it.
`delete_folder` checks emptiness against all live child folders and reports,
including hidden ones; a nonempty folder fails `conflict` and nothing is deleted.
Tombstones may remain under worker retention policy but cannot reappear in a
deleted folder. Validated no-op moves/updates succeed without incrementing
versions. Stale versions, duplicate names and nonempty deletion fail `conflict`;
cycles/invalid names fail `invalid_request`, and advertised limits use
`quota_exceeded`. Refusals never return inaccessible child names or IDs.

The worker supplies caller-specific `root_allowed_actions` and per-resource
`allowed_actions`. Folder hints describe operations such as `read`,
`create_folder`, `create_report`, `rename`, `move`, `delete` and
`transfer_ownership`; root has no rename, move, delete or ownership operation.
These are advisory operation hints, not a prescribed permission model.

### Folder policies

Workers may set and enforce policies on folders and reports using their own
authorization mechanisms. Permission assignment, inheritance, evaluation,
configuration and the effect of moves on access are worker decisions. The
protocol defines no policy hooks, ACL schema or required evaluation procedure.
It communicates the worker's decisions through permission hints and the shared
visibility, refusal and RPC/SQL parity conventions.

Folder containment remains distinct from management ownership:
`parent_folder_id` is not `ownership.parent_owner_ref`. Moving a folder or
transferring its ownership preserves descendants' explicit ownership and report
data, and never copies credentials. `set_folder_ownership` uses the shared
transfer lifecycle. Any resulting permission changes follow worker policy.

## Rendering contract

Records and signatures: [generated Python definitions](reference/render.md).

Required limits: `max_body_bytes: bytes`, `max_timeout_seconds: seconds`.
Timeout is positive and within the limit. Parameters must be literals. Status
is `queued`, `running`, `succeeded`, `failed`, or `cancelled`; terminal results
have zero poll delay and non-null finish time, failures a non-null error.
Render queries must be read-only, including invoked functions; setup may only
create temporary session objects. A renderer refuses a report requiring
persistent or external effects. Render-job access belongs to the authenticated
caller that admitted it, subject to the host's administrative policy.

## Schedules contract

Records and signatures: [generated Python definitions](reference/schedules.md).

Required limits: `minimum_interval_seconds: seconds`, `max_timeout_seconds:
seconds`, `max_result_bytes: bytes`. Exactly the selected action's payload is
non-null; vendor kinds use only `custom_json`. Render actions take sources from
the resolved report and require an empty action-level source list. `pinned`
requires a revision ID; `head` and `published` require null. Output formats are
`none`, `arrow`, `parquet`, `csv`; inline modes `none`, `summary`, `image`,
`table`. `test_run` resolves parameters at `as_of`, validates access as on
create, and accepts only read-only work and temporary setup, including vendor
actions. Refuse effectful work before dispatch; no deliveries occur. Empty
condition means true but `condition_evaluated = false`; SQL NULL is represented
by null plus `condition_evaluated = true`.

## SQL tasks contract

Records and signatures: [generated Python definitions](reference/sql_tasks.md).

Required limits: `minimum_interval_seconds: seconds`, `max_timeout_seconds:
seconds`, `max_result_bytes: bytes`. Host location/catalog identify the caller's
authorized catalog, not unrestricted internal storage; both are null when none
is provisioned. The selected body alone is non-null. Target kind is `catalog`
or `host`; catalog requires a source alias, host an empty alias. Task mode and
incremental combinations follow the matrix in [sql_tasks.md](sql_tasks.md#incremental-loads).
Health states are defined there. Watermark JSON uses a `KeyValue` below, not an
untyped JSON number; null clears/means no progress. Script watermark SQL must
return exactly one row and one scalar of that type. Cursor type cannot change
without an explicit reset/full refresh. Lookback is a positive fixed-duration
`PT...H...M...S` (integer hours/minutes, seconds with up to six fractional digits);
calendar months/years are invalid. It applies only to timestamp cursors.
Edits to body, data sources, incremental configuration or parameters,
`set_watermark`, and target deletion require no active run or unresolved effects.
A target/body edit cannot let an old run advance the new definition's progress.
Deleting a runnable resource requires cessation/resolution of its active work;
disabling a resource prevents new admission but does not itself cancel a run.
Tests use rollback-only execution as specified in the task document and produce
no persistent effects or notifications. Unknown counts are null, never zero.

## Alerts contract

Records and signatures: [generated Python definitions](reference/alerts.md).

Required limits: `minimum_interval_seconds: seconds`, `max_instances: count`,
`max_snooze_seconds: seconds`, `max_result_bytes: bytes`. Rule state is `ok`,
`firing`, `error`, or `disabled`. Instance state is `pending`, `firing`,
`resolving`, `resolved`, or `superseded`. Event kinds are `transition`,
`notification`, `acknowledged`, `unacknowledged`, `snoozed`, `unsnoozed`,
`subscribed`, `unsubscribed`, `rule_error`, `rule_recovered`, and `superseded`.
State transitions supply from/to; other events leave both null. `actor` is null
for automatic events. Error events supply an error, notification events the
per-destination results; other events use null/empty respectively.

Evaluation and tests are read-only, with temporary setup only. Failed evaluations
leave instances unchanged, store the failure event, and retry reads within the
advertised budget; later regular evaluations may recover automatically. Missing
authority disables dispatch until credentials/identity are repaired. Notification
steps retain stable event/destination IDs and obey notify's ambiguous-delivery
rules independently; a delivery error never reruns a committed state transition.
Provider reconciliation is the notify worker's policy and is exposed as event
outcomes, not a new alert-run mutation interface.

Details are one-row Arrow IPC values so rules with unrelated columns still share
one fixed instance schema. Callers lacking `view_details` receive null for both
detail fields; redaction of access is not an empty row. Fired details are null
before first firing. Current and fired schemas can differ after query edits.
`canonical_key` is always visible to subscribers, as specified in alerts.md.

`snooze.expected_version` guards the instance when present, otherwise the rule;
creation increments that object's version. `until` is always required and
bounded; `until_resolved` requires an instance and ends at the earlier of
resolution or `until`. Unsnooze guards the snooze version. Instance versions
advance on evaluation or manual changes. At most one subscription per caller
and rule is allowed; a second subscribe with different destinations is
`conflict`; unsubscribe/recreate changes them. Subscription listing exposes
only the caller's subscription unless policy grants subscription management.

### Canonical keys and scalar values

Records and signatures: [generated Python definitions](reference/common.md).

`canonical_key` is RFC 8785 JSON of an array of `[type, value]` pairs in
`key_columns` order. Both members are strings, so wide integers and decimals
never pass through a JSON number. The empty key list encodes `[]`. Allowed
types/encodings:

| Type tag | Value string |
| --- | --- |
| `bool` | `true` or `false` |
| `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64` | Base-10 integer within that type, no plus sign/leading zeros; zero is `0` |
| `utf8` | Exact Unicode string, no normalization |
| `binary` | RFC 4648 base64 with padding, no whitespace |
| `date32` | Signed base-10 days since 1970-01-01 |
| `timestamp_us_utc` | Signed base-10 microseconds since Unix epoch; zoned timestamps normalize to UTC |
| `decimal128(p,s)`, `decimal256(p,s)` | Signed base-10 unscaled integer; tag includes decimal precision and scale without spaces |
| `float32`, `float64` | Big-endian IEEE-754 bits as 8/16 lowercase hex digits; negative zero normalizes to positive zero; NaN/infinity rejected |

Other key types and nulls fail evaluation with `invalid_key`, including nested
values and timezone-naive timestamps. Timestamp values at other resolutions
must convert exactly to microseconds or fail; dates and times are not guessed
from strings. Changing a key's logical type changes its identity. Detail
columns retain their full Arrow types and have no such scalar restriction.
Watermarks use JSON objects `{"type": tag, "value": value}` with this encoding;
only integer, decimal, date and timestamp tags are valid ordered cursors.

The instance ID is lowercase SHA-256 of UTF-8 RFC 8785 JSON for
`[rule_id, decimal_string(key_generation), canonical_key_array]`; embed the
array itself, not its JSON text. Field names and detail values do not enter the
digest. Generation starts at 1 and changes with ordered key-column changes as
specified in alerts.md.

## Notify and delegations contracts

Records and signatures: [generated Python definitions](reference/notify.md). See also [delegations](reference/delegations.md).

Unary calls return one row of their declared record; check results and put
results remain list fields of that row, including through SQL for checks.
Each channel requires `max_text_bytes: utf8_bytes` and `max_attachment_bytes:
bytes`. Check/send results preserve input destination order. Duplicate
destinations are invalid. Delegation writes are atomic per request; duplicate
keys are invalid. `expected_version = 0` means absent; otherwise replace only
the matching version and increment it. Revocation removes only the matching
version. These preconditions prevent an old renewal or revoke from overwriting
new credentials. Caller-scoped list returns no secret fields. No delegation
get-info method is added solely for limits; refusals use standard quota details.

## Mutation and error conventions

Every effectful public mutation above has a `request_id`. Within at least
24 hours of first admission, deduplicate by `(authenticated principal,
protocol ID, method, request_id)`. Compare all supplied fields after default
expansion by typed value (byte fields exactly); a different payload is
`invalid_request`. JSON-text inputs compare by exact UTF-8 text, not an
implementation's JSON normalization. The same request returns the original
admission result without repeating effects, even if its expected version is now
stale. Authenticate and authorize before returning a stored response; loss of
access never reveals a formerly accessible object. Redaction overrides stored
response replay: return a current tombstone or `not_found`, never a cached body. Concurrent identical calls
share one admission. Do not cache pre-admission refusals. Secrets in request
fingerprints/results stay protected and never enter error messages.

Preconditions are the fields in each signature, not a blanket inferred rule:
reports use head/published revision checks for those specific changes; other
existing objects use versions. Creates use request IDs, not expected versions;
subscription/snooze parent checks are defined above. Test methods are restricted
to read-only/rollback-only work and have no persistent effect to deduplicate.
Resolving a run, cancelling it, transferring ownership and credential renewal
all participate in the same mutation convention. A best-effort cancellation
returning a nonterminal object acknowledges a request, not successful cessation.

Use README's shared vgi-rpc refusal details. Additional refusal kinds are
`recovery_required` and `execution_identity_unavailable` (`FAILED_PRECONDITION`,
`PreconditionFailure` naming run/step or principal), and `cursor_expired`
(`FAILED_PRECONDITION`, `ResourceInfo` naming the list). Persisted `RunError`
contains the canonical code name, stable kind and sanitized message; it is not
an untyped replacement for typed RPC error details. `execution_unknown` uses
`UNKNOWN`; invalid SQL/schema/key/watermark/condition use `INVALID_ARGUMENT`;
grant problems `FAILED_PRECONDITION`; policy `PERMISSION_DENIED`; result limits
`RESOURCE_EXHAUSTED`; timeouts `DEADLINE_EXCEEDED`; internal `INTERNAL`.
Source/transaction failures preserve the receiver's canonical code when known;
without a receiver code, use `UNAVAILABLE` for `source_unavailable` and
`INTERNAL` for `transaction_failed`, unless the effect outcome is unknown.
Stable run kinds are `grant_expired`, `grant_required`, `source_unavailable`,
`query_failed`, `condition_invalid`, `result_too_large`, `policy`,
`render_timeout`, `internal`, `execution_unknown`, `transaction_failed`,
`schema_mismatch`, `watermark_invalid`, `target_unwritable`, `invalid_key`,
`too_many_instances`, and `execution_identity_unavailable`. Target permission
failures use `PERMISSION_DENIED`, instance limits `RESOURCE_EXHAUSTED`, and
missing execution identity `FAILED_PRECONDITION`.
Codes alone do not authorize replay. Version/revision conflicts return the current relevant
version/revision in `ErrorInfo.metadata`: `version`, `head_revision_id`, or
`published_revision_id` as applicable. Integers are decimal strings; an absent
published pointer is an empty metadata string. Optional `updated_by` is a
principal ID and `updated_at` is RFC 3339 UTC with six fractional digits, because
this standard metadata map only permits strings.

## Required conformance examples

These expected values are normative; ports share schema fixtures and executable
vectors implementing them when the protocol lands.

| Input | Expected result |
| --- | --- |
| Unlimited delegation expiry from export (`+inf` or NULL) | Reporting `expires_at = null` |
| Missing trigger `run_at` for once, or naive timestamp | `invalid_request` |
| `30 2 * * *`, America/New_York, preview after 2026-03-07T07:30:00Z | Next is 2026-03-09T06:30:00Z; March 8's local 02:30 does not exist |
| `30 1 * * *`, America/New_York, preview after 2026-11-01T05:00:00Z, count 2 | 2026-11-01T05:30:00Z and 2026-11-01T06:30:00Z |
| Scheduled 2026-10-01T12:00:00Z, UTC, `previous_month`; retried November 2 | Both use `[2026-09-01, 2026-10-01)` |
| 2026-03-09T04:00:00Z, America/New_York, `last_n_days:1` | `[2026-03-08, 2026-03-09)`; a 23-hour local day |
| Key int64 9007199254740993 | `[["int64","9007199254740993"]]`, with no precision loss |
| Decimal128(10,2) 12.30 | `[["decimal128(10,2)","1230"]]` |
| Float64 negative zero | `[["float64","0000000000000000"]]` |
| Rule r_1, generation 1, string West | Hash input `["r_1","1",[["utf8","West"]]]`; digest `383a4b99d7af0363c42887a05e52446741bd2a422438fc53f4beff0f54075c91` |
| Owner leaves; parent transfers report | Report ID/body/history and execution principal unchanged; ownership version advances |
| Committed insert loses response | `unknown`, no automatic retry, `needs_resolution`; `run_now` refused until resolved and old writer stopped |
| Same retry request after version changes | Original admitted result; no new step dispatch |
| Same request ID with different body | `invalid_request` |
| Rule details have unrelated schemas | Both instance batches use the same outer schema; one-row IPC preserves each detail schema |

## Worker operational discretion

Workers choose deployment, storage engines, backups/restores, encryption-key
administration, upgrades, retention, quotas, monitoring and offboarding
integration. No reporting protocol standardizes those administrative workflows.
They must honor accepted mutation/idempotency and advertised run guarantees,
protect delegated credentials, and expose failures and limits through this
interface. After restoration, an implementation that cannot establish a
dispatched operation's outcome must report unknown and reconcile rather than
silently replay it. The mechanism for satisfying that contract is the worker's.

## Optional report ownership lookup

`vgi.reports.ownership.v1` is independently advertised alongside `vgi.reports.v1`.
Its exact [Python contract](reference/ownership.md) defines `find_owners(resource_kind, resource_id, query="", limit=20)`.
`resource_kind` is `report` or `folder`; `limit` is an int64 in 1–100.
The unary result is the usual `result: binary` Arrow IPC record: `OwnershipOptions`
contains non-null `query_hint: string`, `candidates: list<OwnershipCandidate>`
with non-null elements, and `has_more: bool`. Each candidate has non-null
`label`, `description` strings and a complete `Ownership` value.

Discovery requires authority to transfer the selected resource. Workers control
search matching and input, including exact email lookup or directory search,
and disclose only eligible candidates. Empty queries may return suggestions or
an empty list with instructions in `query_hint`. `has_more` asks the user to
refine the query; clients never infer identities from labels, emails, or kinds.
Clients submit the selected complete ownership unchanged to `set_ownership` or
`set_folder_ownership` with the current resource version and a stable request ID.
The mutation must revalidate the caller, canonical identities, parent authority
and eligibility atomically. Discovery is not a grant and may become stale.
Authorship and execution credentials are unchanged. Workers without this optional
interface retain their existing ownership mutations and administration workflows.
