# vgi.schedules.v1

A schedule runs an **action** on a trigger, as its owner, and delivers what the
action produced. Rendering a report is one action; running a query is another;
implementations may add their own. Each firing is a run that records what ran,
with which values, whether its condition held, and where the outputs went.
Shared conventions are in [README.md](README.md#shared-conventions); sessions
and grants are in [credentials.md](credentials.md).

A host of `vgi.schedules.v1` also hosts `vgi.delegations.v1`: a schedule
carries no credentials, and runs reattach each source with the owner's
delegation for it.

## Model

```python
@dataclass
class Schedule:
    title: str
    trigger: Trigger
    action: Action
    condition_sql: str = ""              # empty = always; see Conditions
    parameter_values: list[ParamValue] = []
    deliveries: list[Delivery] = []
    enabled: bool = True

@dataclass
class Trigger:
    kind: str                            # cron | once
    cron: str = ""                       # 5-field cron (croner dialect, vgi-crontimes)
    run_at: str = ""                     # kind=once: ISO 8601
    time_zone: str = "UTC"               # IANA
    start_at: str = ""
    end_at: str = ""

@dataclass
class Action:
    kind: str                            # render_report | run_query | <vendor>.<name>
    render_report: RenderReportAction = ...   # kind = render_report
    run_query: RunQueryAction = ...           # kind = run_query
    data_sources: list[DataSource] = []       # run_query and vendor kinds; credentials.md
    custom_json: str = ""                     # kind = <vendor>.<name>, validated by that implementation

@dataclass
class RenderReportAction:
    report: ReportRef                    # {service_url, report_id, revision_id}
    track: str = "published"             # pinned | published | head
    outputs: list[str] = ["application/pdf"]

@dataclass
class RunQueryAction:
    setup_sql: str = ""
    sql: str                             # its result is the action's output
    output_format: str = "parquet"       # none | arrow | parquet | csv

@dataclass
class ParamValue:
    key: str
    kind: str                            # literal | relative
    json: str = ""                       # kind=literal
    relative: str = ""                   # kind=relative, e.g. previous_month, last_n_days:7

@dataclass
class Delivery:
    destinations: list[Destination]      # vgi.notify.v1 {kind, address}
    inline: str = "summary"              # none | summary | image (report) | table (query)
    attach: list[str] = []               # output media types to attach; empty = link only
```

Schedule editors offer presets ("weekdays at 8:00"); they compile to cron, and
`preview_trigger` describes the cron back in plain English. A missed firing
after downtime runs once.

**Records** add `schedule_id`, `owner`, `version`, `next_fire_at`, `last_run`,
`disabled_reason`, `credentials` (per required location: `ok`, `expiring` with
a time, `expired`, `missing`) and `allowed_actions`.

## Actions

| Kind | Session | Outputs |
| --- | --- | --- |
| `render_report` | The report's `data_sources`, rendered by `vgi.report_render.v1` | The rendered media types |
| `run_query` | The action's `data_sources` | The result in `output_format`, plus `row_count` |
| `<vendor>.<name>` | The action's `data_sources` | Defined by the implementation |

- **`render_report`** reads the revision to run on every run (pinned, latest
  published, or head) with the owner's grant on the report service, takes
  `data_sources` from **that** revision's envelope, checks the owner holds
  grants for them, and sends body, sources and grants to a renderer.
- **`run_query`** rebuilds the session from `data_sources` and the owner's
  grants, runs `setup_sql` then `sql` with parameters bound as `$key`, and
  stores the result. Where the session runs is the implementation's choice.
- **Custom kinds** (`acme.refresh_extract`) carry their arguments in
  `custom_json`, validated at create time. They declare `data_sources` so
  grants are checked the same way. A client that doesn't know a kind can still
  list, pause, run and delete the schedule and read its runs.
- `get_scheduler_info` lists the kinds a scheduler supports; any other kind is
  `invalid_request`.

**Tracking report versions.** `pinned` runs exactly one revision. `published`
and `head` follow the report as it changes, which means later edits by the
report's editors run with the schedule owner's grants and go to the owner's
destinations. That is an accepted risk the owner chooses; schedule editors warn
when someone follows a report they can't edit, and runs record the revision
they rendered.

## Conditions

`condition_sql` decides whether a run proceeds. It is one query that must
return **exactly one row with one `BOOLEAN` column**, run in the action's
session with the same parameters.

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
| `get_scheduler_info()` | U | Action kinds, output formats, minimum interval, relative tokens, limits |
| `preview_trigger(trigger, count)` | U | Next N fire times, a plain-English description, DST warnings |
| `list_schedules(action_kind, report_id, owned_by_me)` / `get_schedule(schedule_id)` | S / U | Read |
| `create_schedule(request_id, schedule)` | U | Owner is the caller; `grant_required` if the owner lacks a grant for a required location |
| `update_schedule(schedule_id, expected_version, schedule)` | U | Includes pausing and resuming via `enabled` |
| `delete_schedule(schedule_id, expected_version)` | U | |
| `test_run(schedule)` | U | Run an unsaved schedule once and return its outputs, row count, condition value and the messages it would send. Delivers nothing |
| `run_now(schedule_id, request_id, ignore_condition)` | U | Manual run, also the way to retry |
| `list_runs(schedule_id, status, since)` / `get_run(run_id)` | S / U | Run history |
| `cancel_run(run_id)` | U | |

**Errors:** the shared kinds in [README.md](README.md#errors), notably
`grant_required` (`FAILED_PRECONDITION`, one `PreconditionFailure` violation per
location without a usable grant) and `conflict`.

## Runs

A run records `run_id`, `trigger_kind` (`schedule`, `manual`), `scheduled_for`,
`started_at`, `finished_at`, `status` (`pending`, `running`, `succeeded`,
`skipped`, `failed`, `cancelled`), `skip_reason`, the `revision_id` rendered,
the resolved parameter values, outputs (`media_type`, `row_count`), one result
per destination, and on failure `error`:

```python
@dataclass
class RunError:
    code: str                  # canonical vgi_rpc code
    kind: str                  # grant_expired | grant_required | source_unavailable | query_failed
                               # | condition_invalid | result_too_large | policy | render_timeout | internal
    location: str = ""         # the data source involved, if any
    message: str = ""          # developer-facing
```

The protocol guarantees two things about runs, and leaves the rest to the
implementation:

- Runs are unique on `(schedule_id, scheduled_for)`, so a firing happens once
  however many scheduler replicas there are.
- A schedule has at most one active run; a firing during one is `skipped` with
  reason `overlap`.

## Relative values

Tokens resolve in the trigger's time zone: `today`, `yesterday`,
`last_n_days:N`, `previous_week` (ISO weeks, Monday start), `previous_month`,
`previous_quarter`, `month_to_date`, `year_to_date`. Pinned with test vectors.
A `date_range` parameter resolves to `{start, end}`. Resolved values are stored
on the run and shown in deliveries ("Period: Sep 1–30, 2026").

## SQL binding

Schema `schedules`. Derived by the rules in [README.md](README.md#sql-binding); this is the annotation.

| Table | List / get | `INSERT` | `UPDATE` | `DELETE` |
| --- | --- | --- | --- | --- |
| `schedules` | `list_schedules` / `get_schedule` | `create_schedule` | `update_schedule`, expected = the row's `version` | `delete_schedule`, same |
| `runs` | `list_runs` / `get_run` | | | |

`list_runs` with no `schedule_id` lists runs across every schedule the caller
can read. `run_now`, `cancel_run`, `test_run` and `preview_trigger` are
procedures.

```sql
UPDATE ops.schedules.schedules SET enabled = false WHERE title LIKE 'Weekly%';

SELECT schedule_id, scheduled_for, error.kind, error.message
FROM ops.schedules.runs WHERE status = 'failed' AND started_at > now() - INTERVAL 1 DAY;
```

## Reference scheduler (non-normative)

How the reference scheduler is built. None of this is required of another
implementation.

**Driven by `tick`, so it runs long-running or serverless.** The engine keeps no
clock. Two calls on durable state do all the work: `tick(budget_seconds)` claims
due schedules, due alert rules and runs with a step due; `advance_run(run_id,
budget_seconds)` moves one run through its persisted, idempotent steps
(condition, action, poll the render, store outputs, deliver to each
destination) until it finishes, waits, or runs out of budget. A long-running
`vgi-report-serve` calls `tick` on a loop. Serverless deployments are ticked by
a platform cron's HTTP request (a Cloudflare Cron Trigger, EventBridge, any
cron), and `tick` fans out one `advance_run` request per run to its own URL.
Both are methods of `vgi.scheduler_driver.v1`, an operator protocol callable
only by allowlisted principals (`VGI_SCHEDULER_DRIVER_PRINCIPALS`); `tick` takes
no time argument, so a caller can wake the scheduler but not move its clock.
Duplicate ticks are no-ops. Platform cron's one-minute floor is the scheduler's
minimum interval. Queries need native DuckDB with the VGI extension, so on
Cloudflare the Worker is only the clock.

**Sessions** are fresh per run, built from `data_sources` plus the owner's delegations, with
`enable_external_access = false` and `lock_configuration = true` after the
attaches, and memory, row and time limits. They are dropped with their grants
when the run ends.

**Reliability.** Per-schedule leases with fencing tokens on `FunctionStorage`
counters. Retryable failures (by `is_retryable()`) retry within the run,
honouring `RetryInfo`; five consecutive failed runs disable the schedule and
notify the owner. Sends record their intent before calling `send`, with
`request_id = hash(run_id, delivery index)`, so a resumed run never delivers
twice. An expired or rejected grant fails the run with `grant_expired`.

**Outputs** are copied into the scheduler's store and kept for the operator's
retention (90 days by default). Delivery links point at an authenticated run
page; attachments use short-lived signed URLs the notify service can fetch.

**State** lives in `FunctionStorage`: sqlite for long-running and development,
Azure SQL or Cloudflare Durable Objects for serverless. The grant encryption key
comes from the platform's secret store.

## Deferred

- Change detection ("only if results changed"), fan-out per parameter value,
  chained actions, business-day and fiscal relative tokens.
