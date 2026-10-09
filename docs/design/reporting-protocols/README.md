# Reporting protocols: reports, rendering, scheduled actions, alerts, notifications

Status: draft for review · revised 2026-10-08 (SQL reads, recovery, ownership, folders and exact wire contracts)

Seven optional, separately versioned vgi-rpc protocols that any VGI worker or
standalone service may host:

| Protocol | Job | Methods | Spec |
| --- | --- | --- | --- |
| `vgi.reports.v1` | Report store: folders, revisions, publishing, redaction | 17 | [reports.md](reports.md) |
| `vgi.report_render.v1` | Render a report body into PDF, HTML or PNG | 4 | [render.md](render.md) |
| `vgi.schedules.v1` | Run an action (render a report, run a query, a vendor kind) on a trigger, as its execution principal, with an optional condition and deliveries | 16 | [schedules.md](schedules.md) |
| `vgi.alerts.v1` | Rules whose query returns the rows meeting a condition; one stateful instance per key; acknowledge, snooze, subscribe | 18 | [alerts.md](alerts.md) |
| `vgi.notify.v1` | Deliver a message to email, Slack, Teams, Discord or a webhook | 4 | [notify.md](notify.md) |
| `vgi.sql_tasks.v1` | Run SQL that changes data on a trigger: load a query into a table (replace, append, merge, snapshot), incrementally by watermark, or a script of statements and `CALL`s in one transaction | 17 | [sql_tasks.md](sql_tasks.md) |
| `vgi.delegations.v1` | Hold an execution principal's catalog delegations (grant + ticket per attachment reference) and standalone service grants | 3 | [credentials.md](credentials.md) |

Cupola's localStorage reports become the first client. The groundwork shipped
in every vgi-rpc port and VGI SDK ([prerequisites.md](prerequisites.md)):
multi-protocol hosting, the gRPC error model, `Identity.v1` everywhere, and
grants accepted as bearer credentials.

## Design rules

1. **The protocol is the unit of optionality.** A host serves a protocol whole
   or not at all; every method is required. What varies between hosts is data
   in `get_*_info` (e.g. `writable`), never the method set.
2. **Composable across hosts.** Nothing assumes co-location. A report can read
   catalogs on several workers, live on a third, be scheduled on a fourth and
   rendered on a fifth. What makes that work is two shared types in
   [credentials.md](credentials.md): a portable **session description**
   (`DataSource` list) and **delegations**: a grant plus a worker-sealed
   attach ticket per attachment reference, from `vgi_export_session()`, plus
   grant-only delegations for standalone services.
3. **Wire contract only.** Each spec says what crosses the wire and what a
   client can rely on. How the reference implementation builds its engine
   (leases, ticks, warm sessions, retries, retention) is in clearly marked,
   non-normative notes.
4. **Explicit execution authority.** Unattended data access uses the selected
   execution principal's delegated grants, never ambient scheduler credentials.
   The worker decides which identities it supports and how they are provisioned.
   Management ownership and historical authorship do not imply data authority.

## Decisions

| Topic | Decision |
| --- | --- |
| Where reports live | Any host may implement `vgi.reports.v1`; a dedicated report service is the expected enterprise shape. A report names any number of catalogs in `data_sources`. |
| Report body | Standard envelope (title, tags, data sources, parameters); the body is opaque bytes tagged with a format such as `cupola.evidence/1`. |
| Report organization | Persistent nested folders with stable IDs, including empty folders. Report placement is current resource metadata; moving or renaming folders preserves report revisions and references. Delete only empty folders. |
| History | Append-only numbered revisions, one published pointer, tombstone redaction. Drafts and restore are client-side. |
| Ownership | Immutable authorship, worker-resolved management owner and durable parent, separate execution identity; transfers never delete data or copy credentials. |
| Recovery | Persist failures, retry only with replay-safety evidence, expose retry/reauthorization/resolution state and actions. `run_now` creates new work; `retry_run` resumes frozen work. |
| Wire types | Real dataclasses and Protocol classes in `vgi/reporting/` define schemas/signatures. [Python reference](python-contracts.md) is generated; [wire behavior](wire-contracts.md) defines lifecycle and encoding rules. |
| Operations | Deployment, backup/restore, key administration, retention and offboarding integration are the implementing worker's policy. |
| Access control | Workers may define and enforce policies on folders and reports. The protocol exposes caller-specific `allowed_actions` and standard refusals; permission models, inheritance and implementation remain worker choices. |
| Data access | Any host may run a session: the scheduler, the renderer, an alert evaluator, any worker. A session is rebuilt anywhere from its `DataSource` list plus the execution principal's grants, with one `ATTACH … bearer_token` per source. |
| Unattended sessions | The client serializes its attachments with `vgi_export_session()` and assigns explicit attachment references. Each host holds catalog delegations per execution principal and reference, plus grant-only service delegations for standalone report stores. A location/catalog match never substitutes one attachment for another. Runners use `ATTACH … (bearer_token, attach_ticket)` for catalogs. Schedules and rules carry no credentials. Lifetime is each worker's policy; the framework default becomes unlimited unless a worker sets a maximum. |
| Scheduling | Generic actions: `render_report`, `run_query`, and `<vendor>.<name>` kinds. Optional `condition_sql` returns one boolean. Triggers are cron or once on the wire; presets are UI. |
| Report versions in schedules | The owner pins a revision, or follows the published revision or head and accepts that later edits run with the selected execution principal's grants. |
| SQL tasks | Their own protocol on the shared engine, for SQL that changes data. Bodies are a declarative `load` or a `script`. Targets are an authorized writable catalog or an isolated host store. Incremental loads append or merge without deleting missing rows; replace/snapshot require full results. Watermark and replay guarantees have explicit preconditions. One written database per transaction; external effects are not transactional. |
| Alerts | Their own protocol. Rows are instances; notify on transitions; subscriptions with link-only details unless the owner shares them; acknowledge per instance, snooze per instance or rule, always with an end. |
| Delivery | `vgi.notify.v1`, channel-agnostic. Each service owns its destination policy and unsubscribe list. |
| Serverless | The reference scheduler is driven by an operator `tick` that a platform cron can call over HTTP; this is reference implementation, not protocol ([schedules.md](schedules.md#reference-scheduler-non-normative)). |
| SQL access | Read-only tables and table functions derived from explicitly annotated RPC reads. Mutations and operations that execute user SQL remain RPC-only. SQL mutation support is not a v1 requirement or a prerequisite for reporting. |
| Naming | Report-only protocols are scoped (`vgi.report_render.v1`, later `vgi.report_sharing.v1`); the rest are general. |
| Discovery | Reflection for what an endpoint hosts; one catalog tag per protocol for where services live. |

## Context

**Cupola reports today** (`vgi-web-frontend/src/lib/evidence/`): one
zod-validated JSON document per report (Evidence Markdown `source`, `setupSql`,
typed parameters, appearance and a single `serviceUrl`) in localStorage, keyed
per worker and per browser. History is a list of revisions with no author;
sharing is exporting a file; PDF export is built from the live DOM. Other
catalogs a report queries are referenced by alias in SQL and never declared.

**What vgi-rpc and the SDKs give us** (shipped 2026-10-06):

- `Worker.hosted_protocols()` hosts extra protocols on every transport;
  `vgi_rpc.Reflection.v1` `list_protocols` says which.
- Errors carry `error_code`, `error_kind` and typed `error_details`; clients
  expose `is_retryable()` and never retry automatically.
- `vgi_rpc.Identity.v1` `issue_grant` mints a delegated credential for the
  calling principal (login no older than 900 s), and a worker with grant keys
  accepts its own sealed `vgig1.` grants as `Authorization: Bearer`.
- VGI's `ATTACH` takes a `bearer_token` option, treated as a secret.

## Goals

1. A report has one durable identity that any authorized person can open from
   any browser or client.
2. Every saved change is an immutable, numbered revision with an author;
   publishing gives readers a stable version while editors keep working.
3. A report can query any number of VGI workers and declares which.
4. Reports, queries and other actions run on a schedule without a browser, as
   their execution principal, optionally only when a condition holds, and deliver to email,
   chat or webhooks.
5. People get alerted when data meets a condition, once per affected entity,
   with the values that triggered it.
6. Each protocol is optional and easy to implement; a worker can ship read-only
   reports from files in one line.

## Non-goals for v1

- ACL management (a later `vgi.report_sharing.v1`), a standard report body,
  users/groups/tenants (they come from the identity provider), bulk messaging,
  named publish channels, server-side drafts, and server implementations in
  every port (servers ship in Python and TypeScript; other ports get generated
  client types).

## Architecture

```mermaid
flowchart TB
    cupola["Cupola"]
    reports["vgi.reports.v1<br/>report service"]
    sched["vgi.schedules.v1 + vgi.delegations.v1<br/>scheduler"]
    alerts["vgi.alerts.v1 + vgi.delegations.v1<br/>alert evaluator"]
    render["vgi.report_render.v1<br/>renderer"]
    notify["vgi.notify.v1"]
    w1["sales worker<br/>vgi.v2 + Identity.v1"]
    w2["hr worker<br/>vgi.v2 + Identity.v1"]

    cupola -- "save, publish" --> reports
    cupola -- "issue_grant, as the user" --> w1 & w2 & reports
    cupola -- "vgi_export_session, put_delegations,<br/>schedules, rules" --> sched & alerts
    sched -- "read revision (execution principal's grant)" --> reports
    sched -- "body + sources + grants" --> render
    render & sched & alerts -- "ATTACH … bearer_token (execution principal's grant)" --> w1 & w2
    sched & alerts -- send --> notify
```

The boxes are roles, not deployments: one process may host several of them
(the reference `vgi-report-serve` hosts reports, schedules, SQL tasks, alerts and
delegations), or each may be its own service. Every arrow carrying data access uses
the execution principal's grant for the location it goes to, and nothing else.

## Shared conventions

**Naming and versioning.** `vgi.<name>.v1`, `protocol_version = "1.0.0"`.
Minor versions only add methods or defaulted fields; breaking changes ship as
`.v2`, hosted side by side. No unions on the wire: every choice is an explicit
`kind` field plus plain values.

### Errors

Every refusal is a vgi-rpc error with a canonical code, a reason
(`error_kind`) and catalog details. Kinds are shared across the protocols.

| `error_kind` | `error_code` | Details | Client behaviour |
| --- | --- | --- | --- |
| `not_found` | `NOT_FOUND` | `ResourceInfo`; optional access hint | Show "Ask {contact} for access" when a hint is present |
| `action_denied` | `PERMISSION_DENIED` | `ErrorInfo.metadata {action}` | Explain; don't retry |
| `read_only_service` | `PERMISSION_DENIED` | `ResourceInfo` | Hide editing; `writable` should already have said so |
| `conflict` | `ABORTED` | `ResourceInfo`, `ErrorInfo.metadata` with the current version or head (`head_revision_id`, `updated_by`, `updated_at`) | Reload, then review, reapply or save as a copy |
| `invalid_request` | `INVALID_ARGUMENT` | `BadRequest` | Show field-level detail |
| `grant_required` | `FAILED_PRECONDITION` | `PreconditionFailure`, one violation per missing catalog reference or service grant, naming its kind, location and attachment reference where applicable | Connect or reauthenticate those sources |
| `quota_exceeded` | `RESOURCE_EXHAUSTED` | `QuotaFailure`; `RetryInfo` for rate limits | Show the limit |
| `recovery_required` | `FAILED_PRECONDITION` | `PreconditionFailure` naming the run/step | Resolve outcome or restore a usable checkpoint |
| `execution_identity_unavailable` | `FAILED_PRECONDITION` | `PreconditionFailure` naming the principal | A manager selects an authorized execution identity |
| `cursor_expired` | `FAILED_PRECONDITION` | `ResourceInfo` naming the list | Explicitly restart the list |
| `service_unavailable` | `UNAVAILABLE` | `RetryInfo` (required) | Retry with backoff only when replay is safe; never cache |

The access hint on `not_found` is `LocalizedMessage`, `Help.links` (request
access) and `ErrorInfo.metadata.access_contact`; no custom detail type.

**Visibility.** Return `not_found` for objects the caller can't read, so ids
can't be probed; list methods filter by read permission.

**Permission hints.** Returned objects carry `allowed_actions` for the caller:
`read`, `edit`, `publish`, `redact`, `delete`, `schedule`, `run`, `manage`,
`transfer_ownership`, `change_execution_principal`, plus recovery and alert
actions. Identity references are displayable metadata, never authorization. Advisory; the server enforces on every call.

**Exact contracts.** Importable Python dataclasses and Protocol classes are
authoritative for all seven protocols' layouts, method arguments/results,
nullability and defaults. The [Python reference](python-contracts.md) is generated
from that source; [wire behavior](wire-contracts.md) specifies identities,
recovery, clocks and scalar encodings.
Method tables in individual specs are navigation summaries, not alternate schemas.

**Ownership.** `created_by` is history. `ownership` identifies a management
owner and durable parent, supplied and enforced by the hosting worker.
Schedules, tasks and rules additionally expose `execution_identity`. Author
removal never deletes objects or data. `set_ownership` transfers management;
Folders expose `set_folder_ownership` for the same lifecycle.
`set_execution_principal` separately changes future execution authority. Parent
administrators can manage orphaned resources without inheriting personal grants.
See the [lifecycle contract](wire-contracts.md#ownership-and-execution-lifecycle).

**Concurrency and idempotency.** Every effectful mutation takes `request_id`;
existing-resource preconditions are explicit in each canonical signature.
Deduplication is scoped to principal, protocol and method for at least 24 hours.
Exact repeats return their admission result; payload changes are invalid.
Admission deduplication does not make the SQL in a run idempotent.

**Lists** use fixed Arrow row schemas and existing vgi-rpc producer continuations,
with no extra application paging envelope. **Times** are `timestamp[us, UTC]`;
optional instants and unlimited reporting expiry are NULL. Parameters and alert
keys use the exact encodings in the wire contract. `body_sha256` hashes the
body bytes as sent. [Recovery](wire-contracts.md#run-state-and-recovery) preserves
failed attempts and freezes inputs rather than silently starting a new run.

**Transports.** Hosted on every transport through `hosted_protocols()`. On
HTTP callers have identities; on stdio and unix the caller is the operator,
which suits tests and single-user tools. Grants are HTTP only.

## Unattended sessions across workers

The full model is in [credentials.md](credentials.md). In short:

- **Serialize:** while logged in, the client runs `vgi_export_session()`. Each
  attached worker returns a grant (who) and an attach ticket (what: the
  attachment's options, secrets included, sealed so only that worker can open
  them). Only catalogs the user attached themselves are exported.
- **Store:** each host keeps the execution principal's catalog delegations by explicit
  attachment reference, and standalone service grants by location, via
  `put_delegations`. Neither secret is returned.
- **Reattach:** any runner attaches each report source with
  `ATTACH … (bearer_token '<grant>', attach_ticket '<ticket>')`, only at the
  delegation's own location, only for that job. It never sees an option or secret.

**Known limits:**

- Grants carry no identity-provider claims. A claim-based `AccessPolicy`, on a
  data worker or the report store, sees unattended work with none and must
  decide from the principal or fail closed.
- Scopes are advisory; by default a grant is as powerful as its owner on that
  worker.
- Non-VGI attachments (files, `:memory:`, other database types) can't be
  serialized.
- With no maximum lifetime set, grants and tickets never expire. Deleting a
  delegation prevents future dispatch from that host; it does not invalidate
  credentials already forwarded to an active job. Worker key rotation
  invalidates the corresponding grants or tickets.

## Discovery

1. **Reflection.** `list_protocols` on an endpoint says which protocols it
   hosts. Clients never probe by calling and catching.
2. **Catalog tags.** A worker that relies on services elsewhere returns them in
   `CatalogAttachResult.tags`, one per protocol, following
   `vgi_secret_service_url`: `vgi_reports_url`, `vgi_report_render_url`,
   `vgi_schedules_url`, `vgi_alerts_url`, `vgi_notify_url`. Values must be https
   (localhost excepted) and same-site or on an operator allowlist; the client
   confirms each with reflection.

When catalogs in one session point at different report services, each report
lives in exactly one, and the library groups by service.

## SQL binding

A host that serves a reporting protocol and `vgi.v2` exposes that protocol's
annotated reads as a schema in its VGI catalog. For example:

```sql
ATTACH 'ops' (TYPE vgi, LOCATION 'https://reports.example.com');

SELECT envelope.title, published_revision_id FROM ops.reports.reports;
SELECT schedule_id, status, error.kind FROM ops.schedules.runs;
```

Creation, editing, publishing, delegation storage, starting/cancelling jobs,
acknowledgements and sending messages use their existing RPC methods. No
reporting mutation is registered as a SQL table function or procedure, and
protocol tables do not support `INSERT`, `UPDATE` or `DELETE`. Methods such as
`test_run` and `test_rule` also remain RPC-only because they execute supplied
SQL. This restriction concerns these service protocols; SQL tasks can still
write to authorized data catalogs through their ordinary VGI write support.

**The read binding is derived.** A generic SDK adapter uses the RPC definition
plus an explicit list of safe read methods, list tables and fetched columns.
It does not infer safety from a method name or expose every RPC automatically.
Each spec's SQL-binding section supplies those annotations. The read binding
is normative: the same SQL reads work against conforming implementations.

### Derivation rules

1. **Schema.** `vgi.<name>.v1` is the schema `<name>`; a `.v2` hosted beside it
   is `<name>_v2`.
2. **Annotated reads are table functions**, named as their RPC methods, with
   named arguments derived directly from the method signature and its defaults.
   Unary results produce
   one row of their declared result
   record, retaining list fields; producer streams return their batches. Unannotated methods have
   no SQL entry point. All annotated reads must be free of persistent or
   external effects, including through any delegated operation.
3. **`get_*_info()` is also the one-row table `info`** when that method exists.
4. **A list method may be annotated as a table.** Its columns are the list row
   schema. Only predicates with equivalent RPC filter semantics may be pushed
   down; other predicates remain local. Lists with required arguments stay
   table functions.
5. **Fetched columns.** A table may name a get method for columns absent from
   the list result. Fetch those columns only when selected, for the same
   resource revision represented by the row; never combine one revision's
   envelope with another's body. The exact mapping and redaction behavior are
   in [wire-contracts.md](wire-contracts.md#lists-and-sql-rows).
6. **Read-only registration.** Register no DML handlers and no entry points
   for the remaining RPC methods. A read may also be invoked with DuckDB's
   ordinary `CALL` syntax; this adds no mutation entry point and requires no
   distinction between CALL and SELECT.
7. **Types are the RPC's.** Arrow records map to `STRUCT`, lists to `LIST`, and
   field names to column names. New field names must be usable as DuckDB
   identifiers without quoting.
8. **These protocol reads never return credentials.** Write-only RPC inputs such as
   grants and tickets are omitted from read schemas and tables. There are no
   synthetic writable columns such as `request_id` or commit `message`.
9. **Identity and access** are the attachment's: the same caller, access
   policy, filtered lists and `not_found` behavior as the corresponding RPC.

### Semantics

- Reads are allowed in autocommit and explicit transactions. They have the
  underlying RPC's consistency guarantees; entering a DuckDB transaction does
  not create a snapshot across a remote report store and run history.
- Unavailable mutation entry points and attempted table DML fail before any
  service mutation is invoked. Applications call the authenticated RPC for
  mutations and retain its concurrency and idempotency rules.
- SQL errors contain `[<error_kind>] <message>` followed by catalog details as
  JSON, matching the RPC refusal without exposing credentials.
- Binding, EXPLAIN and PREPARE may perform read-only schema discovery but must
  never run a job, deliver a message or mutate stored resources.

### Scope and compatibility

The initial read adapter uses existing VGI catalog and table-function
facilities. It does not depend on new SQL execution-context fields,
CALL-origin enforcement, or a coordinated wire revision. These were needed
for the earlier SQL-mutation proposal, which is no longer a requirement.
An implementation spike must verify the read mapping against existing APIs;
any concrete missing capability gets its own scoped design.

RPC service work and Cupola integration may proceed alongside the read adapter.
There is no dependency on query pushdown. SQL mutations can be reconsidered
later if a real client needs them, with a separate contract and compatibility
review; never expose effectful RPCs as ordinary table functions as a shortcut.

### Drift guards

- Derive the read catalog from RPC classes and explicit read annotations;
  reject annotations naming missing methods or fields at startup.
- `scripts/regen_generated.py --only vgi-python --check` checks the generated
  Python reference and read-binding inventory against the actual definitions.
- Run every RPC method's conformance suite over RPC, and the annotated read
  subset over SQL. Assert matching rows, filters, identity and refusals.
- Extension tests assert that DML and attempts to call omitted mutation
  methods have zero service effects, including through SELECT, CALL, CTEs,
  EXPLAIN and prepared statements. No SQL-context capability is required.

## Developer experience

The protocol and record definitions are real imports today:

```python
from vgi_rpc.rpc import rpc_methods
from vgi.reporting.reports import ReportEnvelope, ReportsProtocol

envelope = ReportEnvelope(title="Sales", body_format="cupola.evidence/1")
report_schema = ReportEnvelope.ARROW_SCHEMA
get_report_arguments = rpc_methods(ReportsProtocol)["get_report"].params_schema
# A connected client uses direct arguments:
# client.get_report(report_id="sales", revision_id=None)
# client.create_report(request_id="create-sales-1", envelope=envelope, body=b"# Sales")
```

The following store/worker integration is a proposed implementation API; the
storage classes do not exist yet; authorization integration is a worker choice:

```python
from pathlib import Path

from vgi.reporting import ReportsProtocol
from vgi.reporting.store import ReadOnlyReportStore, SharedStorageReportStore
from vgi.worker import Worker


class SalesWorker(Worker):
    functions = [...]

    @classmethod
    def hosted_protocols(cls):
        # Ship .cupola-reports.json exports as read-only reports.
        store = ReadOnlyReportStore.from_files(Path(__file__).parent / "reports")
        return [(ReportsProtocol, store)]


class TeamWorker(Worker):
    functions = [...]

    @classmethod
    def hosted_protocols(cls):
        return [(ReportsProtocol, SharedStorageReportStore.from_env())]
```

Workers choose how to configure and enforce authorization for these stores,
including folder policies. The protocol exposes the resulting permission hints
and refusals without prescribing policy hooks or inheritance rules.
Reference stores sit on `FunctionStorage`, so sqlite, Azure SQL and Cloudflare
Durable Objects come free. **Conformance** is per protocol: every RPC method, error mapping, concurrency,
idempotency and authorship, plus parity for the SQL read subset; pytest
parameterized by URL so another implementation can run it too.

## Implementation plan

The build plan, its status and the decisions made along the way are in
[IMPLEMENTATION.md](IMPLEMENTATION.md). It is the single source for what to
build next.

## Review history

**First review** (principal engineer, BI UX lead, DX lead): grants accepted as
bearers (shipped), credentials bound to locations, run uniqueness, no unions,
the gRPC error model, `test_run`, plain-English trigger previews, structured
run errors, `label`s on data sources, `writable` services.

**Second review** (senior engineer, simplification and composability). Taken:

This table records the original decisions; the third review below supersedes
the per-catalog credential key and adapter-only SQL assumptions.

| Finding | Change |
| --- | --- |
| Three shapes for one credential, keyed by alias | One `Delegation` per owner per catalog; `DataSource` is the shareable description |
| Grants per schedule: renewal toil, and `purpose` named a schedule id before it existed | Per owner per catalog in `vgi.delegations.v1`, minted by `vgi_export_session()` |
| Minting at author-controlled locations leaks the user's session | Mint only at attached or confirmed locations |
| "Same grant" for the secret service can't verify there | Replaced by worker-sealed attach tickets, which carry secret options without exposing them |
| Report sources copied at schedule creation go stale | Taken from the revision rendered, every run |
| Claim-based policies on the report store fail for grant callers | Documented; reference `AccessPolicy` decides by principal |
| Notify assumed co-location (unsubscribe, attachments) | Notify owns the suppression list; attachments use signed URLs |
| Server drafts, publications log, undelete, copied_from, labels | Removed; reports 15 → 9 methods |
| Trigger presets and misfire policy on the wire | Cron or once; presets are UI |
| Engine rules (driver, leases, retries, retention, session hardening, grouping) as protocol | Moved to non-normative reference notes |
| Merges | `publish` unpublishes; `set_enabled`, `reauthorize`, `retry_run` folded in; one `conflict` kind; `set_acknowledged`; no `get_notification` or `partial_ok` |

Not taken, by decision: custom action kinds stay; alert subscriptions stay;
schedules may follow a report's latest revision as an accepted risk; the
unlimited default grant lifetime stands.

**Third review** (2026-10-07, execution and identity):

| Finding | Change |
| --- | --- |
| Location/catalog keys collide for different attachments | Explicit `attachment_id` references and owner-selected source mappings |
| Standalone report stores have no credential path | Grant-only service delegations acquired directly through Identity |
| User SQL can reach a shared writable host store | Mandatory isolation or authorized catalog access; reference uses per-owner stores |
| Incremental merge/replace can delete retained history | Validated mode matrix; no incremental deletion of missing rows |
| Retryability was treated as replay safety | Persisted execution outcomes, safe-stage retries and reconciliation of ambiguous effects |
| Python adapter cannot distinguish CALL/SELECT or transaction modes | Effect annotations, extension enforcement and a coordinated SQL-context wire revision |
| Changing alert keys can reuse superseded instance IDs | Monotonic key generations in identity, evaluation state and notifications |

**2026-10-08 scope decision.** SQL reads ship first; mutations remain on RPC.
This supersedes the third review's SQL-context wire revision and P2 extension
prerequisite. The credential, isolation, retry and alert-identity corrections
remain in force. The read adapter and report service can be developed together.

**2026-10-08 lifecycle and contract decision.** Recovery now has distinct
retry and evidence-based resolution operations. Ownership has a durable parent
and is independent of attribution and execution identity. Exact wire records,
method signatures, existing transport pagination, UTC/null handling, cron/DST
semantics and lossless alert keys are specified in `wire-contracts.md`. This
supersedes the earlier decision to fold retry into `run_now`. Deployment and
operational administration remain the implementing worker's discretion.

**2026-10-08 Python contract decision.** Replace the handwritten IDL with
real Python dataclasses and seven vgi-rpc Protocol interfaces. Methods take
direct parameters; structured domain values remain dataclasses. The existing
framework supplies scalar argument columns, binary IPC dataclass arguments/results
and Arrow struct encoding within records. Stream row schemas and safe SQL reads
are explicit annotations. Each protocol has a separate generated reference page;
the overview links to them. Serialization tests keep these contracts reviewable. Service
implementations, storage and the SQL adapter remain separate work.

**2026-10-08 Folder decision.** Reports and folders form a hierarchy beneath
a virtual service root. Folder IDs and parent IDs replace the envelope's path
string; report location is independent of immutable content revisions.
Direct RPC methods manage folders and move reports. `reports.folders` and
folder-filtered report reads support SQL browsing. Workers may set policies on
folders; authorization mechanisms, inheritance and ownership defaults remain
worker choices.

## Resolved questions

| Question | Answer |
| --- | --- |
| Features in `protocol_hash`? | Moot: `features` is always empty |
| Publish channels? | One pointer |
| History removal? | Tombstone redaction |
| Grant lifetime? | The worker's; framework default unlimited unless set |
| DuckDB tables? | Yes, on the reference service, read-only |
| Artifact retention? | Operator policy (reference: 90 days) |
| Tags? | One flat tag per protocol |
| Notify host? | New `vgi-notify` repo |
| Scheduling others' reports? | Anyone with `schedule`; runs as its selected execution principal; pin or follow |
| Where data access runs? | Anywhere; sessions are portable |
| Grant storage? | Per execution principal, catalog attachment reference or standalone service location |

## Open questions

None outstanding.
