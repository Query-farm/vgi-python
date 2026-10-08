# vgi.sql_tasks.v1

A SQL task runs SQL **that changes data** on a trigger, as its owner, in a
session rebuilt from its data sources and the owner's delegations. A task can
load a query's result into a table (replace it, append to it, merge into it,
or keep dated snapshots), fetch only what's new since the last run, or run a
script of statements in one transaction, `CALL`s included. Each firing is a run
that records what it changed and where the watermark moved. Shared conventions
are in [README.md](README.md#shared-conventions); sessions and delegations are
in [credentials.md](credentials.md). A host of `vgi.sql_tasks.v1` also hosts
`vgi.delegations.v1`.

**Why not a schedule.** A schedule produces outputs and delivers them; it has
no destination table and no memory between runs. A task writes, keeps a
watermark across runs, and is managed as the dataset it maintains: how fresh it
is, what the last run loaded, reset and backfill. Either protocol can be hosted
without the other; the reference service runs both on one engine.

## Model

```python
@dataclass
class SqlTask:
    title: str
    description: str = ""
    trigger: Trigger                     # as vgi.schedules.v1: cron | once
    data_sources: list[DataSource]       # the session; credentials.md
    body: TaskBody
    incremental: Incremental = ...       # default kind = none
    condition_sql: str = ""              # empty = always; see Conditions
    parameter_values: list[ParamValue] = []
    notify: TaskNotify = ...
    stale_after_seconds: int = 0         # health is `stale` with no success this long; 0 = never
    timeout_seconds: int = 3600
    enabled: bool = True

@dataclass
class TaskBody:
    kind: str                            # load | script | <vendor>.<name>
    load: LoadBody = ...                 # kind = load
    script: ScriptBody = ...             # kind = script
    custom_json: str = ""                # kind = <vendor>.<name>

@dataclass
class LoadBody:
    setup_sql: str = ""
    query: str                           # the rows to load; may use $watermark
    target: Target
    mode: str                            # replace | append | merge | snapshot
    key_columns: list[str] = []          # mode = merge: match on these
    delete_missing: bool = False         # mode = merge: delete target rows the query no longer returns
    snapshot_column: str = "_snapshot_at"  # mode = snapshot
    retain_snapshots: int = 0            # mode = snapshot: keep the newest N; 0 = all
    on_schema_change: str = "fail"       # fail | add_columns

@dataclass
class ScriptBody:
    statements: list[str]                # run in order; may use $watermark and parameters
    transaction: str = "single"          # single | per_statement | none
    watermark_sql: str = ""              # incremental.kind = stored: returns the new watermark

@dataclass
class Target:
    kind: str                            # catalog | host
    alias: str = ""                      # kind = catalog: one of data_sources, written under the owner's delegation
    schema: str = "main"
    table: str
    create_if_missing: bool = True       # created from the query's result schema

@dataclass
class Incremental:
    kind: str = "none"                   # none | derived | stored
    cursor_column: str = ""              # load: the column that only grows (updated_at, id)
    initial_json: str = ""               # the watermark for the first run; empty = NULL
    lookback: str = ""                   # e.g. "PT2H": re-read late-arriving rows; needs mode = merge

@dataclass
class TaskNotify:
    destinations: list[Destination] = [] # vgi.notify.v1
    on: list[str] = ["failing", "recovered", "disabled"]
                                         # failing | recovered | stale | disabled | every_failure | success
```

**Records** add `task_id`, `owner`, `version`, `next_fire_at`, `last_run`,
`watermark_json`, `target_row_count` where the target reports it,
`disabled_reason`, `credentials` (as schedules), `allowed_actions`, and the
task's health (below).

## Bodies

### `load`

Runs `setup_sql`, then writes `query`'s result to the target in one
transaction:

| Mode | Effect |
| --- | --- |
| `replace` | The target holds exactly the result. Readers see the old rows or the new, never neither |
| `append` | Inserts the result |
| `merge` | Upserts on `key_columns`; with `delete_missing`, deletes target rows whose key the result no longer returns |
| `snapshot` | Appends the result with `snapshot_column` set to the run's `scheduled_for`, then deletes snapshots beyond `retain_snapshots` |

A missing target is created from the result's schema when `create_if_missing`.
A result whose schema no longer matches fails the run (`schema_mismatch`)
unless `on_schema_change = add_columns`, which adds new columns as nullable and
fills missing ones with `NULL`; a changed type always fails.

### `script`

Runs `statements` in order, with parameters bound as `$key`:

| `transaction` | Meaning |
| --- | --- |
| `single` | One transaction around all statements: all commit or none do |
| `per_statement` | Each statement commits on its own; a failure stops the script and earlier statements stay committed |
| `none` | No transaction is opened; for statements that can't run inside one |

A `CALL` is a statement like any other. `CALL sales.main.refresh_extract(...)`
invokes a table function on the sales worker under the owner's delegation, which
is how a task asks a worker to do work that isn't SQL. Work outside any worker
is a vendor kind.

### Vendor kinds

`<vendor>.<name>` carries its arguments in `custom_json`, validated at create
time, as in schedules. It declares `data_sources` so delegations are checked
the same way. A client that doesn't know a kind can still list, pause, run and
delete the task and read its runs.

## Transactions and atomicity

**One written database per transaction.** DuckDB reads from any number of
attached databases in a transaction but writes to only one. A `single` script
or a `load` may read every source; its writes must all go to one database. A
task that writes two databases uses `per_statement` and accepts that a failure
can leave the first written and the second not. Implementations check this at
create time where they can and otherwise fail the run with
`transaction_failed`.

**A remote target's commit is that worker's.** Writing a `catalog` target
commits through the worker's VGI catalog transaction. Atomicity, isolation and
what readers see during the write are that catalog's guarantees.

## Incremental loads

The watermark is the high-water mark of what has been loaded. It's bound as
`$watermark` in `query`, `statements` and `condition_sql` (`NULL` on the first
run unless `initial_json` is set).

| `kind` | Where the watermark comes from | Guarantee |
| --- | --- | --- |
| `none` | No watermark | |
| `derived` | `SELECT max(cursor_column) FROM target`, read inside the run's transaction just before writing | **Exactly once.** The read and the write commit together, and at most one run is active, so a crashed run loads nothing and the next run starts where the target ends |
| `stored` | Saved by the service after a successful run: `max(cursor_column)` of the loaded rows (`load`), or the single value `watermark_sql` returns, run last inside the transaction (`script`) | **At least once.** The service saves the watermark after the target commits, so a crash between the two re-reads the last window. Make the write idempotent (`merge`) and that is harmless |

`derived` needs a `load` with a target that has `cursor_column`. `stored` works
for any body, including a script whose watermark isn't a column, a target the
runner can't query, and a source-side cursor such as an API page token. On a
`host` target an implementation may store the watermark in the same database as
the data, which makes `stored` exactly once too; `get_tasks_info` says whether
it does.

**`lookback`** subtracts an ISO 8601 duration from a timestamp watermark to
re-read rows that arrived late, and requires `mode = merge` so the overlap
doesn't duplicate. **A watermark never moves backwards** on its own: a run whose
new watermark is lower than the old one fails with `watermark_invalid`. Moving
it back is a backfill, done explicitly with `set_watermark`.

## Destinations

- **`catalog`:** one of the task's `data_sources`, written under the owner's
  delegation for it. The data lands on that worker (a DuckLake catalog, a
  database worker, anything with VGI write support), and that worker's access
  control governs both the write and every later read.
- **`host`:** a table in the service's own store, served by the service as a
  read-only catalog over `vgi.v2`. The catalog's name and location are in
  `get_tasks_info`. Who can read it is the service's `AccessPolicy`; by default,
  the task's owner and whoever the owner shares the task with.

## Conditions

`condition_sql` runs before the body in the task's session and returns one
`BOOLEAN`, with the results as in schedules (`false`/`NULL` skip the run). It
can read `$watermark`, so "only if anything is newer than what we have" is
`SELECT max(updated_at) > $watermark FROM sales.main.orders`.

## Tracking health

Every task carries a health summary, kept current by the service as runs
finish and as time passes:

```python
@dataclass
class TaskHealth:
    state: str                    # healthy | failing | stale | paused | disabled | new
    since: str                    # when the task entered this state
    last_success_at: str = ""
    last_failure_at: str = ""
    last_error: RunError = ...    # the most recent failure's error
    consecutive_failures: int = 0
```

| State | Meaning |
| --- | --- |
| `new` | Never run |
| `healthy` | The last finished run succeeded or was skipped by its condition, and the task isn't stale |
| `failing` | The last finished run failed; `consecutive_failures` counts the streak |
| `stale` | No success within `stale_after_seconds`, even though nothing failed (runs skipped, cancelled, or the trigger stopped firing) |
| `paused` | `enabled = false`, set by a person |
| `disabled` | Disabled by the service, with `disabled_reason` (for example, repeated failures or an expired delegation) |

**Notifications follow state changes, not runs.** `failing` notifies once when
a streak starts, `recovered` once when it ends, `stale` and `disabled` once on
entry. `every_failure` and `success` notify per run, for those who want it.
Each message carries the task, the state, the run and its `RunError`.

**History is queryable in SQL.** The `tasks` and `runs` tables (see SQL
binding) carry health and run history, so an alert rule can watch tasks too
("any task in the finance folder failing for over an hour").

## Methods

U = unary, S = producer stream. All are required.

| Method | Kind | Purpose |
| --- | --- | --- |
| `get_tasks_info()` | U | Body kinds, modes, `host` store catalog and location, whether `host` watermarks are atomic, minimum interval, limits |
| `preview_trigger(trigger, count)` | U | As in schedules |
| `list_tasks(owned_by_me, target, health_state)` / `get_task(task_id)` | S / U | Read, with health; `health_state` filters ("everything failing") |
| `create_task(request_id, task)` | U | Owner is the caller; `grant_required` for any source without a usable delegation |
| `update_task(task_id, expected_version, task)` | U | Includes pausing via `enabled`. Changing the target, mode or cursor column clears a `stored` watermark |
| `delete_task(task_id, expected_version, drop_target)` | U | `drop_target` drops a `host` table; a `catalog` target is never dropped |
| `test_run(task)` | U | Runs an unsaved task inside a transaction that is always rolled back. Returns row counts per statement, the old and new watermark, the target schema it would create and the condition value. A `CALL` whose function acts outside DuckDB still has that effect |
| `run_now(task_id, request_id, ignore_condition, full_refresh)` | U | Manual run, also the way to retry. `full_refresh` runs with `$watermark = NULL` and, for `load`, `replace` semantics |
| `set_watermark(task_id, expected_version, watermark_json)` | U | Backfill or reset a `stored` watermark; empty clears it. Refused for `derived`, whose watermark is the target's data |
| `list_runs(task_id, status, since)` / `get_run(run_id)` | S / U | Run history. `task_id` empty lists runs across every task the caller can read, so "what failed overnight" is one call |
| `cancel_run(run_id)` | U | Rolls back an open transaction; `per_statement` keeps what already committed |

**Errors:** the shared kinds in [README.md](README.md#errors).

## Runs

As in schedules (unique on `(task_id, scheduled_for)`, at most one active run,
an overlapping firing is `skipped` with reason `overlap`), recording in
addition: `watermark_before`, `watermark_after`, per statement its index, kind
and `rows_affected`, and for `load` the rows inserted, updated and deleted and
the target's row count afterwards, and `attempt` when the implementation
retried within the run. `RunError.kind` adds `transaction_failed`,
`schema_mismatch`, `watermark_invalid` and `target_unwritable` to the schedule
kinds.

## SQL binding

Schema `sql_tasks`. Derived by the rules in [README.md](README.md#sql-binding); this is the annotation.

| Table | List / get | `INSERT` | `UPDATE` | `DELETE` |
| --- | --- | --- | --- | --- |
| `tasks` | `list_tasks` / `get_task` | `create_task` | `update_task`, expected = the row's `version` | `delete_task`, same (`drop_target` false; use `CALL` to drop) |
| `runs` | `list_runs` / `get_run` | | | |

`run_now`, `cancel_run`, `test_run`, `set_watermark` and `preview_trigger` are
procedures. A task's own script can call them, and any other protocol's
procedures (`CALL ops.notify.send(…)`).

```sql
SELECT title, health.state, health.consecutive_failures, health.last_error.message
FROM ops.sql_tasks.tasks WHERE health.state IN ('failing', 'stale');

CALL ops.sql_tasks.run_now(task_id := 't_orders', request_id := 'manual-2026-10-08',
                           ignore_condition := false, full_refresh := true);
```

## Reference implementation (non-normative)

- **Engine.** The same `tick` / `advance_run` engine as the reference scheduler,
  so tasks run long-running or serverless alike. A task's step list is
  condition, body, save watermark, notify.
- **Sessions** are built like schedule sessions (fresh, hardened, limited). The
  `host` store is attached writable before configuration is locked; every other
  source is attached with the owner's delegation.
- **The `host` store** is a DuckLake catalog by default, so `replace` and
  `snapshot` keep time travel for free, and the stored watermark lives in the
  same catalog, which makes `host` targets exactly once.
- **Bodies compile to SQL.** `merge` is `MERGE INTO` where the target supports
  it, or a delete-and-insert in one transaction where it doesn't. Runs record
  the compiled statements.

## Deferred

- Dependencies between tasks ("after the orders load succeeds"), partitioned
  backfills, change data capture from source logs, and per-row quarantine of
  rows that fail a check.
