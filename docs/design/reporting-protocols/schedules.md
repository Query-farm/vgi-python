# vgi.schedules.v1

Python dataclasses and RPC interface: [generated reference](reference/schedules.md).
Lifecycle and encoding rules: [wire behavior](wire-contracts.md).

A schedule runs an **action** on a trigger, as its execution principal, and delivers what the
action produced. Rendering a report is one action; running a query is another;
implementations may add their own. Each firing is a run that records what ran,
with which values, whether its condition held, and where the outputs went.
Shared conventions are in [README.md](README.md#shared-conventions); sessions
and grants are in [credentials.md](credentials.md).

A host of `vgi.schedules.v1` also hosts `vgi.delegations.v1`: a schedule
carries no credentials, and runs reattach each source with the execution principal's
delegation for it.

## Model

The fields, defaults and Arrow types are defined by the
[Python dataclasses](reference/schedules.md).

Schedule editors offer presets ("weekdays at 8:00"); they compile to cron, and
`preview_trigger` describes the cron back in plain English. Missed eligible firings coalesce to the most recent missed instant. Exact
cron grammar, DST behavior, preview bounds and relative periods are in the
[trigger contract](wire-contracts.md#triggers-and-parameter-values).

The canonical `ScheduleRecord` contains `definition`, ownership, attribution,
execution identity, version, credential status and run summary; timestamps and
missing values use the exact shared types.

## Actions

| Kind | Session | Outputs |
| --- | --- | --- |
| `render_report` | The report's `data_sources`, rendered by `vgi.report_render.v1` | The rendered media types |
| `run_query` | The action's `data_sources` | The result in `output_format`, plus `row_count` |
| `<vendor>.<name>` | The action's `data_sources` | Defined by the implementation |

- **`render_report`** reads the revision to run on every run (pinned, latest
  published, or head) with the execution principal's `service` delegation for the report
  service's exact location, takes
  `data_sources` from **that** revision's envelope, checks the execution principal holds
  catalog delegations for their attachment references, and sends body, sources
  and those catalog delegations to a renderer. The report-store grant stays
  with the scheduler. This works when the report store hosts no VGI catalog
  ([service grants](credentials.md#standalone-service-grants)).
- **`run_query`** rebuilds the session from `data_sources` and the execution principal's
  catalog delegations, runs `setup_sql` then `sql` with parameters bound as
  `$key`, and stores the result. Both SQL fields may have effects and follow
  the execution rules below. Where the session runs is the implementation's
  choice.
- **Custom kinds** (`acme.refresh_extract`) carry their arguments in
  `custom_json`, validated at create time. They declare `data_sources` so
  grants are checked the same way. A client that doesn't know a kind can still
  list, pause, run and delete the schedule and read its runs.
- `get_scheduler_info` lists the kinds a scheduler supports; any other kind is
  `invalid_request`.

**Tracking report versions.** `pinned` runs exactly one revision. `published`
and `head` follow the report as it changes, which means later edits by the
report's editors run with the schedule execution principal's grants and go to the owner's
destinations. That is an accepted risk the owner chooses; schedule editors warn
when someone follows a report they can't edit, and runs record the revision
they rendered.

## Conditions

`condition_sql` decides whether a run proceeds. It is one query that must
return **exactly one row with one `BOOLEAN` column**, run in the action's
session with the same parameters. It must be read-only, including functions
it invokes; the host validates this before executing it.

| Result | Run outcome |
| --- | --- |
| `true` | Proceed and deliver |
| `false` / `NULL` | `skipped` (`condition_false` / `condition_null`) |
| Any other shape | `failed`, `condition_invalid` |

For `run_query` it runs after the query and can read the result as the view
`action_result` ("send only if there are late orders" is `SELECT count(*) > 0
FROM action_result`). For `render_report` and custom kinds it runs before the
action, so a quiet day costs one small query instead of a render.

## Methods

U = unary, S = producer stream. All are required.

| Method | Kind | Purpose |
| --- | --- | --- |
| `get_scheduler_info(…)` | U | Action kinds, output formats, minimum interval, relative tokens, limits |
| `preview_trigger(…)` | U | Next N fire times, a plain-English description, DST warnings |
| `list_schedules(…)` / `get_schedule(…)` | S / U | Read |
| `create_schedule(…)` | U | Ownership and execution identity are independently resolved by the host; `grant_required` if the execution principal lacks a grant for a required location |
| `update_schedule(…)` | U | Includes pausing and resuming via `enabled` |
| `delete_schedule(…)` | U | |
| `test_run(…)` | U | Preview unsaved read-only work at `as_of`; return outputs, condition and prospective messages. Reject persistent/external effects before dispatch; deliver nothing |
| `run_now(…)` | U | New manual run; use `retry_run` to recover an existing run |
| `list_runs(…)` / `get_run(…)` | S / U | Run history |
| `cancel_run(…)` | U | |
| `retry_run(…)` | U | Resume frozen work from a safe checkpoint |
| `resolve_run(…)` | U | Record worker-verified outcomes; never replay a step |
| `set_ownership(…)` | U | Transfer management; preserve data and execution authority |
| `set_execution_principal(…)` | U | Select a separately authorized execution identity for future work |

**Errors:** the shared kinds in [README.md](README.md#errors), notably
`grant_required` (`FAILED_PRECONDITION`, one `PreconditionFailure` violation per
catalog reference or service location without a usable delegation) and
`conflict`.

## Runs

A run records `run_id`, `trigger_kind` (`schedule`, `manual`), `scheduled_for`,
`started_at`, `finished_at`, `status` (`pending`, `running`, `retry_wait`, `succeeded`,
`skipped`, `failed`, `cancelled`), `skip_reason`, the `revision_id` rendered,
the resolved parameter values, outputs (`media_type`, `row_count`), one result
per destination, the step identities/attempts and effect outcomes below, and on
failure `error`:

The fields, defaults and Arrow types are defined by the
[Python dataclasses](reference/schedules.md).

The protocol guarantees two things about runs, and leaves the rest to the
implementation:

- Automatic runs are unique on `(schedule_id, scheduled_for)`, so a firing happens once
  however many scheduler replicas there are.
- A schedule has at most one active run; a firing during one is `skipped` with
  reason `overlap`.

### Execution and retries

Run uniqueness and `request_id` deduplication govern admission, not the effects
of SQL inside the run. `is_retryable()` describes a failure's transience; a
runner also needs evidence that replay is safe. These rules apply to schedules,
SQL tasks and any shared run engine, regardless of its lease implementation.

Before dispatching an effectful step, persist its intent and stable
`operation_id = (run_id, step_index)`. Freeze the run's action, parameters,
resolved report revision and step inputs before their dispatch so recovery
cannot replay the same ID with a different payload. Record attempts separately.
Each step
exposes `effect_outcome`: `none`, `committed`, `rolled_back`, `partial`, or
`unknown`. A lost executor or response after effectful dispatch is `unknown` until
reconciled; it is never assumed to mean rollback. Persist confirmed results
before advancing to a later step.

| Evidence at failure | Permitted recovery |
| --- | --- |
| No effectful operation dispatched | Retry bounded metadata/authentication/preparation work when the error is retryable |
| Validated read-only operation failed | Retry within budget before publishing its result; preserve any required snapshot |
| All effects are transactional and rollback is confirmed | Retry that transaction when the error is retryable; never repeat earlier committed steps |
| Receiver durably deduplicates the operation | Retry with the identical operation ID and payload only within its guaranteed retention window |
| Some effects committed or an outcome is unknown | Do not replay automatically; reconcile from the receiver's durable result or fail the run |

An arbitrary `run_query`, script, setup statement or external `CALL` is not
assumed idempotent or transactionally reversible. A SQL task's `per_statement`
mode may resume after a confirmed checkpoint, but cannot repeat preceding
committed statements. Fencing a scheduler lease protects its own state; it
does not fence an already dispatched remote write. Before dispatching another
writer, the runner must establish that the prior executor has stopped or that
the target rejects its fencing token.

Every observed failure is durably recorded on its attempt before a retry;
process logs alone are insufficient. `get_run` and SQL reads expose the failed
step, known outcome, retry state, next retry time and caller-specific recovery
actions. Budgets and backoff are the worker's advertised policy.

Unreconciled effects fail with `RunError.kind = execution_unknown`, canonical
code `UNKNOWN`, and disable automatic firings with that reason. Do not advance
a stored watermark or report success. A manual `run_now` creates a new run and
may repeat previous effects; clients show the prior outcome. It is refused
while effects remain unresolved or an earlier executor might be writing. Re-enabling
automatic execution requires reconciliation and confirmation that the old
executor cannot continue.

For sends and render starts, persist the original request ID and first-dispatch
time. The shared 24-hour deduplication guarantee bounds retry eligibility:
after that window, an unconfirmed request requires reconciliation or fails as
unknown. Generating a fresh request ID is not recovery. Notification-provider
ambiguity follows [notify.md](notify.md#delivery-and-retry-outcomes).

### Recovery workflow

The [run contract](wire-contracts.md#run-state-and-recovery) defines the exact
records and RPCs. Clients show the failed step and offer only permitted actions:

- `retry_run` resumes the same run, action/revision, resolved parameters and
  operation IDs at a safe checkpoint. It never repeats committed steps.
- `reauthorize` uses the existing delegation flow, followed by safe retry or
  explicit re-enable. It cannot resolve an ambiguous write.
- `resolve_run` records outcomes supported by evidence the worker validates,
  plus actor, time and note. It neither sends SQL nor marks success. A partial
  effect remains partial; a committed step without recoverable outputs cannot
  simply be skipped to manufacture success.
- Corrected SQL or changed execution identity requires a new run. Re-enabling
  after resolution is explicit; the worker verifies old-executor cessation.

Invalid SQL fails until corrected. Missing/rejected grants stop new dispatch.
Ordinary safely failed work need not disable future firings; unknown effects
always block them. The worker decides how to reconcile receipts and how many
safe retries to attempt. This interface does not prescribe its queue, storage,
backup, key administration or operational workflow.

## Relative values

Tokens resolve at the run's frozen `scheduled_for` in the trigger's zone,
including on delayed execution and retry. Date ranges are half-open; the
[wire contract](wire-contracts.md#triggers-and-parameter-values) specifies every
token, DST behavior and conformance example. Deliveries display these stored
values rather than recalculating from the current clock.

## Ownership and execution

`created_by`, `ownership` (owner and durable parent), and `execution_identity`
are distinct fields. The worker resolves and enforces them. `set_ownership`
transfers management without deleting data or copying grants;
`set_execution_principal` separately selects authorized future execution and
requires the resource to be idle with no unresolved effects. Neither operation
rewrites prior runs or attribution. Author offboarding and automatic parent
inheritance are worker policy. Exact methods and transition rules are in the
[lifecycle contract](wire-contracts.md#ownership-and-execution-lifecycle).

## SQL binding

Schema `schedules`. Derived by the [shared read-binding rules](README.md#sql-binding).

| Table | List / get |
| --- | --- |
| `schedules` | `list_schedules` / `get_schedule` |
| `runs` | `list_runs` / `get_run` |

Read table functions: `get_scheduler_info`, `preview_trigger`, `list_schedules`,
`get_schedule`, `list_runs` and `get_run`. With no `schedule_id`, `list_runs`
covers every schedule the caller can read. All other methods are RPC-only,
including `test_run`, which executes supplied SQL and is restricted to safe
preview work by the wire contract.

```sql
SELECT schedule_id, definition.title, definition.enabled FROM ops.schedules.schedules
WHERE definition.title LIKE 'Weekly%';

SELECT schedule_id, scheduled_for, error.kind, error.message
FROM ops.schedules.runs WHERE status = 'failed' AND started_at > now() - INTERVAL 1 DAY;
```

## Reference scheduler (non-normative)

How the reference scheduler is built. None of this is required of another
implementation.

**Driven by `tick`, so it runs long-running or serverless.** The engine keeps no
clock. Two calls on durable state do all the work: `tick(budget_seconds)` claims
due schedules, due alert rules and runs with a step due; `advance_run(run_id,
budget_seconds)` advances its persisted condition, action, render polling,
output storage and delivery steps until it finishes, waits, or runs out of
budget. It applies the execution and retry rules above. A long-running
`vgi-report-serve` calls `tick` on a loop. Serverless deployments are ticked by
a platform cron's HTTP request (a Cloudflare Cron Trigger, EventBridge, any
cron), and `tick` fans out one `advance_run` request per run to its own URL.
Both are methods of `vgi.scheduler_driver.v1`, an operator protocol callable
only by allowlisted principals (`VGI_SCHEDULER_DRIVER_PRINCIPALS`); `tick` takes
no time argument, so a caller can wake the scheduler but not move its clock.
Duplicate ticks are no-ops. Platform cron's one-minute floor is the scheduler's
minimum interval. Queries need native DuckDB with the VGI extension, so on
Cloudflare the Worker is only the clock.

**Sessions** are fresh per run, built from `data_sources` plus the execution principal's delegations, with
`enable_external_access = false` and `lock_configuration = true` after the
attaches, and memory, row and time limits. They are dropped with their grants
when the run ends.

**Reliability.** Per-schedule leases with fencing tokens on `FunctionStorage`
counters. Retries require both a retryable error and replay-safety evidence,
honour `RetryInfo`, and stay within the run budget. Five consecutive failed
runs disable the schedule and notify the owner; an unknown effect disables it
immediately. Sends record intent with the wire contract's stable step `operation_id` as
`request_id` and retain per-destination outcomes. Never resend confirmed deliveries
or retry unknown ones beyond the deduplication window. An expired or rejected
grant fails the run with `grant_expired`.

**Outputs** are copied into the scheduler's store and kept for the operator's
retention (90 days by default). Delivery links point at an authenticated run
page; attachments use short-lived signed URLs the notify service can fetch.

**State** lives in `FunctionStorage`: sqlite for long-running and development,
Azure SQL or Cloudflare Durable Objects for serverless. The grant encryption key
comes from the platform's secret store.

## Deferred

- Change detection ("only if results changed"), fan-out per parameter value,
  chained actions, business-day and fiscal relative tokens.
