# vgi.sql_tasks.v1

Python dataclasses and RPC interface: [generated reference](reference/sql_tasks.md).
Lifecycle and encoding rules: [wire behavior](wire-contracts.md).

A SQL task runs SQL **that changes data** on a trigger, as its execution principal, in a
session rebuilt from its data sources and the execution principal's delegations. A task can
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

The fields, defaults and Arrow types are defined by the
[Python dataclasses](reference/sql_tasks.md).

The canonical `TaskRecord` contains the definition, ownership, execution
identity, health, stable resolved target and exact run/progress records. Unknown
counts are NULL. `watermark_json` uses the lossless typed scalar encoding in the
[wire contract](wire-contracts.md#canonical-keys-and-scalar-values).

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

A `load` query must be read-only. Its setup may create session-local temporary
objects but cannot mutate persistent data or invoke external effects; use a
script for that work. This makes the target transaction the load's only
persistent write boundary.

A missing target is created from the result's schema when `create_if_missing`.
A result whose schema no longer matches fails the run (`schema_mismatch`)
unless `on_schema_change = add_columns`, which adds new columns as nullable and
fills missing ones with `NULL`; a changed type always fails.

### `script`

Runs `statements` in order, with parameters bound as `$key`:

| `transaction` | Meaning |
| --- | --- |
| `single` | One transaction around all transactional statements: all commit or none do; nontransactional effects are refused |
| `per_statement` | Each statement commits on its own; a failure stops the script and earlier statements stay committed |
| `none` | No transaction is opened; for statements that can't run inside one |

A `CALL` is a statement like any other. `CALL sales.main.refresh_extract(...)`
invokes a table function on the sales worker under the execution principal's delegation, which
is how a task asks a worker to do work that isn't SQL. Work outside any worker
is a vendor kind.

A `CALL` participates in `single` only when its worker explicitly guarantees
that all effects enlist in the task's transaction. Unknown and external effects
require `per_statement` or `none`; wrapping them in `BEGIN` does not make them
reversible. This concerns data-worker functions: reporting mutations such as
`notify.send` have no SQL entry point. Transaction-control statements in a
`single` or `per_statement` body are `invalid_request`; the runner owns those
boundaries.

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

The [execution and retry contract](schedules.md#execution-and-retries) applies
to setup, body, commit and notification steps. A retryable commit error with
no confirmed outcome does not authorize another append or script execution.
Persist the outcome of each statement in `per_statement`; a checkpoint is
confirmed only after its commit result is known. Unknown commits require
reconciliation and block automatic runs, including a later scheduled firing.

## Incremental loads

The watermark is the high-water mark of what has been loaded. It's bound as
`$watermark` in `query`, `statements` and `condition_sql` (`NULL` on the first
run unless `initial_json` is non-null).

| `kind` | Where the watermark comes from | Guarantee |
| --- | --- | --- |
| `none` | No watermark | |
| `derived` | `SELECT max(cursor_column) FROM target`, read inside the run's transaction just before writing | Atomic target data and progress, under the preconditions below. A confirmed rollback leaves neither advanced; after a confirmed commit, the target contains the progress |
| `stored` | Saved after a confirmed successful run: `max(cursor_column)` of loaded rows (`load`), or the single value `watermark_sql` returns (`script`) | Data may commit before service progress. Reprocessing the last window is at least once, not safe replay for arbitrary writes; use an idempotent merge or reconcile progress before another run |

Validate the following matrix at create/update, `test_run` and run admission:

| Body / load mode | Allowed incremental kinds | Additional rules |
| --- | --- | --- |
| `load`, `replace` or `snapshot` | `none` | Query represents the complete dataset; no incremental window |
| `load`, `append` | `none`, `derived`, `stored` | No lookback; a repeated stored window needs reconciliation before appending again |
| `load`, `merge` | `none`, `derived`, `stored` | `delete_missing` requires `none`; incremental merges require `delete_missing = false` |
| `script` | `none`, `stored` | Stored progress requires `transaction = single` and `watermark_sql`; no lookback |
| Vendor body | `none` in v1 | Other combinations require a later specified contract |

Violations are `invalid_request` with field-level details. In particular,
`WHERE cursor > $watermark` is a partial result: it must never delete old rows
through `replace` or `delete_missing`. Bounded deletion and incremental
snapshots are deferred. An explicit `full_refresh` of an incremental load is
the sole override: it requires a query whose NULL-watermark branch returns
the complete dataset, replaces the target, and resets progress to that result
using the selected watermark kind's commit protocol. An empty full refresh
clears progress. Record the override on the run. On `stored`, a committed
refresh whose progress update is unconfirmed must be reconciled before any
incremental run; do not reuse the old watermark against the replaced target.

`derived` requires a queryable transactional target with `cursor_column` and
one writer for that target, including other tasks and external writers. The
cursor must order complete source windows: use `cursor > $watermark` (or the
documented merge lookback), with no later-arriving row at an already passed
cursor unless it lies within that lookback. An initial NULL watermark must
select the initial window explicitly. These source guarantees are the task
author's responsibility; `max(cursor_column)` alone does not establish them.
Exactly-once advancement applies only with these guarantees and a confirmed
transaction outcome, not to arbitrary SQL or external effects.

`stored` supports a single-transaction script whose cursor is not a column;
`watermark_sql` runs last inside that transaction. On a `host` load, an
implementation may atomically store progress with the target data;
`get_tasks_info` advertises that narrower guarantee. It does not make a
script's external effects or unrelated databases atomic. Empty incremental
results retain the previous watermark; invalid/null cursors in nonempty load
results fail with `watermark_invalid`. Validate progress before target commit.

**`lookback`** subtracts the fixed-duration ISO 8601 subset in the wire contract from a timestamp watermark to
re-read rows that arrived late, and requires `mode = merge` so the overlap
doesn't duplicate. **A watermark never moves backwards** on its own: a run whose
new watermark is lower than the old one fails with `watermark_invalid`. Moving
it back requires explicit `set_watermark` for stored progress, or the
documented `full_refresh` override.

## Destinations

- **`catalog`:** one of the task's `data_sources`, written under the execution principal's
  delegation for it. The data lands on that worker (a DuckLake catalog, a
  database worker, anything with VGI write support), and that worker's access
  control governs both the write and every later read.
- **`host`:** a table in the service's own store, served by the service as a
  read-only catalog over `vgi.v2`. The catalog's name and location are in
  `get_tasks_info`. Who can read it is the service's `AccessPolicy`; by default,
  the task's owner and whoever the owner shares the task with.

**Host isolation is required on the execution path too.** User SQL must never
receive an unrestricted attachment to a shared store or the scheduler's
control database. `enable_external_access = false` and locked configuration
do not authorize access to already attached tables. A host must provide an
isolated per-owner store or route every table operation through a catalog
that enforces the execution principal's permissions within the task's
authorized target namespace, including reads, writes, DDL, views
and function calls. A schema naming convention is not an access boundary.
`Target.schema` and `Target.table` resolve within that authorized namespace;
task sharing grants the documented read access, not write access to another
owner's store. Host-managed progress and run records are never exposed as
writable user tables.

## Conditions

`condition_sql` runs before the body in the task's session and returns one
`BOOLEAN`, with the results as in schedules (`false`/`NULL` skip the run). It
can read `$watermark`, so "only if anything is newer than what we have" is
`SELECT max(updated_at) > $watermark FROM sales.main.orders`.

## Tracking health

Every task carries a health summary, kept current by the service as runs
finish and as time passes:

The fields, defaults and Arrow types are defined by the
[Python dataclasses](reference/sql_tasks.md).

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
| `get_tasks_info(…)` | U | Body kinds, modes, `host` store catalog and location, whether `host` watermarks are atomic, minimum interval, limits |
| `preview_trigger(…)` | U | As in schedules |
| `list_tasks(…)` / `get_task(…)` | S / U | Read, with health; `health_state` filters ("everything failing") |
| `create_task(…)` | U | Ownership and execution identity are independently resolved by the host; `grant_required` for any source without a usable delegation |
| `update_task(…)` | U | Includes pausing via `enabled`. Changing the target, mode or cursor column clears a `stored` watermark |
| `delete_task(…)` | U | `drop_target` drops a `host` table; a `catalog` target is never dropped |
| `test_run(…)` | U | Tests a load or `single` script in a transaction that is always rolled back; returns counts, old/new watermark, target schema and condition value. Refuses bodies, statements or CALLs whose effects cannot be rolled back, including `per_statement` and `none` scripts |
| `run_now(…)` | U | New manual run; use `retry_run` to recover an existing run. `full_refresh` runs with `$watermark = NULL` and, for `load`, `replace` semantics |
| `set_watermark(…)` | U | Backfill or reset a `stored` watermark; null clears it. Refused for `derived`, whose watermark is the target's data |
| `list_runs(…)` / `get_run(…)` | S / U | Run history. `task_id` empty lists runs across every task the caller can read, so "what failed overnight" is one call |
| `cancel_run(…)` | U | Rolls back an open transaction; `per_statement` keeps what already committed |
| `retry_run(…)` | U | Resume frozen work from a safe checkpoint |
| `resolve_run(…)` | U | Record worker-verified outcomes; never replay a step |
| `set_ownership(…)` | U | Transfer management; preserve data and execution authority |
| `set_execution_principal(…)` | U | Select a separately authorized execution identity for future work |

**Errors:** the shared kinds in [README.md](README.md#errors).

## Runs

Recovery uses schedules' `retry_run` and `resolve_run` contracts, including
stored-watermark reconciliation. Changing credentials or owner cannot clear
unknown writes. An ownership transfer preserves the stable target and data;
hosts using per-owner stores retain or migrate its authorized mapping.

As in schedules (automatic runs unique on `(task_id, scheduled_for)`, at most one active run,
an overlapping firing is `skipped` with reason `overlap`), recording in
addition: `watermark_before`, `watermark_after`, per statement its index, kind
and `rows_affected`, and for `load` the rows inserted, updated and deleted and
the target's row count afterwards, and `attempt` when the implementation
retried within the run. `RunError.kind` adds `transaction_failed`,
`schema_mismatch`, `watermark_invalid` and `target_unwritable` to the schedule
kinds.

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

Schema `sql_tasks`. Derived by the [shared read-binding rules](README.md#sql-binding).

| Table | List / get |
| --- | --- |
| `tasks` | `list_tasks` / `get_task` |
| `runs` | `list_runs` / `get_run` |

Read table functions: `get_tasks_info`, `preview_trigger`, `list_tasks`,
`get_task`, `list_runs` and `get_run`. All other methods, including `test_run`,
`run_now` and watermark changes, are RPC-only. Scripts can query these tables,
but cannot invoke reporting mutations such as `notify.send` through SQL.
The run engine sends configured notifications through RPC after execution.

```sql
SELECT definition.title, health.state, health.consecutive_failures, health.last_error.message
FROM ops.sql_tasks.tasks WHERE health.state IN ('failing', 'stale');
```

## Reference implementation (non-normative)

- **Engine.** The same `tick` / `advance_run` engine as the reference scheduler,
  so tasks run long-running or serverless alike. A task's step list is
  condition, body, save watermark, notify.
- **Sessions** are built like schedule sessions (fresh, hardened, limited).
  User SQL receives only the task's authorized host target namespace and sources attached
  with the execution principal's catalog delegations. Shared reads use the authorized VGI
  catalog. No other owner's raw store or service control store is attached.
- **The `host` store** starts as a separate DuckLake catalog per management
  owner by default. Stable target mappings survive ownership transfers; neither
  identity changes nor transfers expose unrelated tables from the old store.
  Service progress remains in `FunctionStorage`, outside user SQL's attached
  store, so the reference advertises non-atomic `stored` watermarks. `derived`
  progress still uses the target transaction. An implementation advertising
  atomic host progress must enforce an authorized catalog boundary around
  private progress tables even for owner scripts; per-owner isolation alone
  does not hide those tables from their owner. Sharing is enforced by the
  read-only VGI catalog, not by handing out raw store access.
- **Bodies compile to SQL.** `merge` is `MERGE INTO` where the target supports
  it, or a delete-and-insert in one transaction where it doesn't. Runs record
  the compiled statements.

## Deferred

- Dependencies between tasks ("after the orders load succeeds"), partitioned
  backfills, change data capture from source logs, and per-row quarantine of
  rows that fail a check.
