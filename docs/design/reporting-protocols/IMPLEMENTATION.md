# Implementing the reporting protocols

The build plan for the protocols in this directory, written so an agent with no
other context can pick up where the last one stopped. Read this file, then the
[README](README.md), [credentials.md](credentials.md) and the spec of the phase
you're on. **Update the status table when a phase lands**, in the same commit.

## Status

| Phase | Status | Landed in |
| --- | --- | --- |
| P0 Specs committed, HTML builder in the repo | done | this commit |
| P1 Unlimited default grant lifetime | not started | |
| P2 SQL-binding framework (vgi-python) | not started | |
| P3 `vgi.reports.v1` | not started | |
| P4 `vgi.delegations.v1` | not started | |
| P5 Run engine and `vgi.schedules.v1` (`run_query`) | not started | |
| P6 `vgi.sql_tasks.v1` | not started | |
| P7 `vgi.notify.v1` reference and deliveries | not started | |
| P8 `vgi.alerts.v1` | not started | |
| P9 `vgi.report_render.v1` and `render_report` | not started | |
| P10 Cupola client | not started (can start after P3) | |

Already shipped and not part of this plan: multi-protocol hosting, the gRPC
error model, `Identity.v1`, grants as bearers, attach tickets
(`vgi.attach_tickets.v1`, `vgi_export_session()`, the `attach_ticket` ATTACH
option), full `vgi.v2` parity across the seven SDKs.

## Decisions

Settled; don't reopen them without the user. The README's Decisions table has
the protocol-level ones; these are the build-level ones and late changes.

| # | Decision |
| --- | --- |
| D1 | The protocol is the unit of optionality; every method is required. |
| D2 | `vgi.delegations.v1` (not "held attachments"): `Delegation`, `put_delegations`, `list_delegations`, `revoke_delegation`. |
| D3 | `vgi.sql_tasks.v1` is its own protocol on the shared run engine, for SQL that changes data. Both destination kinds (`catalog`, `host`) and both watermark kinds (`derived`, `stored`). Scripts run statements in one transaction by default; `CALL` is a statement. |
| D4 | `vgi.schedules.v1`'s `run_query` stays as specified (it may write). The overlap with SQL tasks is accepted. |
| D5 | The delegation field stays `grant`. DuckDB accepts it unquoted (the extension's `attach_ticket/round_trip.test` does `SELECT grant FROM …`). |
| D6 | The SQL binding is normative and **derived** from the RPC definition by a generic adapter plus a per-protocol annotation; never hand-written ([README](README.md#sql-binding)). |
| D7 | Protocol-table writes are refused inside an explicit multi-statement transaction (each row is an RPC that `ROLLBACK` can't undo). Revisit only if a real use needs it. |
| D8 | Specs stay in `docs/design/reporting-protocols/` until each protocol ships; a shipped protocol's spec then moves to `docs/protocol/`. |
| D9 | These are service protocols. The reference implementation is Python; other SDKs implement one only when someone wants to host it natively. The extension tests the SQL bindings cross-language. |
| D10 | Build order is by dependency, P1 to P9. Each phase ends usable, with conformance over both surfaces. |

## Working rules

**Where work happens**

- Build and test on the EC2 host `ec2-user@13.217.25.2` in a directory you
  create and own, not the shared Mac. Debug builds, no LTO, at most 8 jobs.
  Kill only processes you started, by PID; never a broad `pkill`.
- Never edit `~/Development/vgi` or `~/Development/vgi-typescript` in place;
  use a git worktree. vgi-python's main checkout may hold the user's
  uncommitted files: never stage what you didn't write.
- Implement, never skip: missing SDK functionality is ported, not excluded
  from a cross-SDK test.

**Testing**

- vgi-python: `uv run ruff check --fix . && uv run ruff format . && uv run mypy vgi/`,
  then `uv run pytest -n auto`.
- Feature tests that cross languages go in the extension as sqllogictests
  (`~/Development/vgi/test/sql/integration/<protocol>/`). Every local
  `unittest` run passes `--test-dir <tree>` (otherwise it silently runs its
  compiled-in tree) and `--test-config test/configs/no_error_skip.json`
  (otherwise HTTP-lane errors become skips).
- Anything a test starts in the background, the test stops: record PIDs in the
  parent shell, never inside `$( … )`.

**Commits and releases**

- Agents commit on a branch and report; they don't push or tag.
- Docs and script-only changes go straight to `main`, no PR.
- A package release is CI-gated: push the release commit to `main`, wait for
  **every** push-triggered workflow on that SHA to succeed (zero runs is a
  failure: check the repo name from `git remote`), then create the GitHub
  release or push the tag, whichever that repo publishes on. Retry GitHub 5xx.
- Commit messages are findings-style (what was wrong or missing, how it was
  found, what changed, how it was verified) and end with the session's
  `Co-Authored-By` trailer.

## Phases

### P0 Specs committed (done)

`docs/design/reporting-protocols/` committed, with
`scripts/build_reporting_protocols_html.py`, which regenerates
`reporting-protocols.html` from the markdown. Rerun it after editing a spec.

### P1 Unlimited default grant lifetime

The design says a grant's lifetime is the issuing worker's policy and unlimited
unless set. vgi-rpc still defaults to 7 days (`DEFAULT_MAX_TTL_SECONDS` in
`vgi_rpc/grants.py`), so every unattended job would stop a week after its
owner's last login.

- Decide the wire form of "no expiry" in the grant spec first (an absent or
  zero `exp`), with test vectors.
- Unset `VGI_RPC_GRANT_MAX_TTL_SECONDS` means no maximum, in vgi-rpc and all
  six ports. Attach tickets follow the worker's grant maximum, so check the
  SDKs' ticket expiry the same way.
- **Done when** the shared vectors pass in every port, `expires_at` is `+inf`
  (or `NULL` in `vgi_export_session()`) for an unlimited worker, and the ports
  are released.

P1 doesn't block P2 to P4; development can set an explicit TTL meanwhile.

### P2 SQL-binding framework (vgi-python)

The generic adapter that serves a hosted RPC protocol as a VGI catalog schema,
built before any protocol so each ships both surfaces.

- Protocol classes for every protocol: Arrow-typed records and the `Protocol`
  class, plus the SQL annotation (tables, get methods for fetched columns, DML
  methods). Proposed home `vgi/reporting/`; adjust if the codebase suggests
  better.
- The adapter, implementing the [README's derivation rules](README.md#derivation-rules)
  1 to 9 and the semantics (conflict, `request_id`, autocommit, error text).
- `scripts/regen_generated.py --check` renders each spec's SQL-binding table
  from the annotations.
- **Done when** each derivation rule and semantic has a test against a toy
  protocol, and a protocol with an annotation that names a missing method or a
  column the row lacks fails at startup.

### P3 `vgi.reports.v1`

- The store on `FunctionStorage`, `AccessPolicy` (by principal: grant callers
  have no claims), `ReadOnlyReportStore`, `vgi-report-serve`.
- Pin every row schema in [reports.md](reports.md) exactly; the SQL binding
  makes them a contract.
- A conformance suite that runs against any implementation over RPC and over
  SQL.
- Extension sqllogictests `test/sql/integration/reports/` against the Python
  service.
- **Done when** the conformance suite passes over both surfaces and Cupola
  could save, list, publish and read history with no localStorage.

### P4 `vgi.delegations.v1`

- `put_delegations`, `list_delegations`, `revoke_delegation`; grants and tickets
  encrypted under a key outside the store, never returned.
- **Done when** this round trip passes as a sqllogictest: `vgi_export_session()`,
  `INSERT INTO … delegations BY NAME …`, then a runner reattaches each source
  with `ATTACH … (bearer_token, attach_ticket)` from the stored delegation and
  reads data, and `revoke_delegation` makes the next reattach fail.

### P5 Run engine and `vgi.schedules.v1`

- The shared engine from [schedules.md](schedules.md#reference-scheduler-non-normative):
  `tick` / `advance_run`, `vgi.scheduler_driver.v1` (allowlisted), leases,
  idempotent steps, hardened per-run sessions built from delegations.
- `vgi.schedules.v1` with `run_query`; deliveries wait for P7.
- **Done when** a schedule runs as its owner from delegations, runs are unique
  on `(schedule_id, scheduled_for)` across two replicas, overlap is `skipped`,
  and a missing delegation is `grant_required`.

### P6 `vgi.sql_tasks.v1`

- `load` (four modes), `script` (three transaction kinds), both watermark
  kinds, `lookback`, the `host` store (DuckLake by default), health and
  transition notifications (deliveries once P7 lands).
- **Done when** `derived` loads exactly once across a crash injected between
  write and commit, `stored` replays at most the last window, a script that
  writes two databases in `single` fails with `transaction_failed`, and
  `test_run` leaves the target untouched.

### P7 `vgi.notify.v1` reference and deliveries

- A reference service with a recording fake channel and a webhook channel;
  destination policy and suppression list. Wire schedule deliveries and task
  notifications to it. The production service is the separate `vgi-notify`
  repo, later.

### P8 `vgi.alerts.v1`

- Rules, instances and states, notifications on transitions, acknowledge,
  snooze, subscriptions, on the shared engine. Pin the instance row schema
  (it's loose in [alerts.md](alerts.md) today).

### P9 `vgi.report_render.v1` and `render_report`

- Depends on a headless Cupola renderer. Then enable the `render_report`
  schedule action.

### P10 Cupola client

- Starts after P3: reports client and migration off localStorage, then
  delegation on save and renewal on login (P4), schedule and task editors (P5,
  P6), "Alert me when…" (P8). TypeScript types and clients come from codegen.

## Known gaps to close while building

- Row schemas are prose in places ("instances with state and times"). Each
  phase pins its protocol's rows; the regen check keeps the spec in step.
- `get_*_info` limits (sizes, minimum intervals) have no values yet; choose
  them in the phase and record them in the spec.
