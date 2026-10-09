# Implementing the reporting protocols

The build plan for the protocols in this directory, written so an agent with no
other context can pick up where the last one stopped. Read this file, then the
[README](README.md), [credentials.md](credentials.md) and the spec of the phase
you're on. **Update the status table when a phase lands**, in the same commit.

## Status

| Phase | Status | Landed in |
| --- | --- | --- |
| P0 Specs and HTML builder | done | existing design |
| Python contract definitions and generated reference | implemented; proposed interfaces | `vgi/reporting/` |
| P1 Unlimited default grant lifetime | not started | |
| P2 Read-only SQL adapter (can proceed alongside P3) | not started | |
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
| D2 | `vgi.delegations.v1`: catalog rows keyed by execution principal and explicit attachment reference plus location/catalog; grant-only service rows keyed by execution principal and location. Never substitute a same-location catalog attachment. |
| D3 | `vgi.sql_tasks.v1` is its own protocol on the shared run engine, for SQL that changes data. Both destination kinds (`catalog`, `host`) and both watermark kinds (`derived`, `stored`). Scripts run statements in one transaction by default; `CALL` is a statement. |
| D4 | `vgi.schedules.v1`'s `run_query` stays as specified (it may write). The overlap with SQL tasks is accepted. |
| D5 | The delegation field stays `grant`. DuckDB accepts it unquoted (the extension's `attach_ticket/round_trip.test` does `SELECT grant FROM …`). |
| D6 | The read-only SQL binding is normative and **derived** from RPC definitions plus explicit read annotations. Mutations remain RPC-only ([README](README.md#sql-binding)). |
| D7 | No reporting mutation has a SQL entry point or DML handler in v1. SQL mutation support and its execution-context/engine prerequisites are deferred, not mandatory. |
| D8 | Specs stay in `docs/design/reporting-protocols/` until each protocol ships; a shipped protocol's spec then moves to `docs/protocol/`. |
| D9 | These are service protocols. The reference implementation is Python; other SDKs implement one only when someone wants to host it natively. The extension tests the SQL bindings cross-language. |
| D10 | Build by dependency with a usable RPC service per phase and SQL conformance for its read subset. P2 and P3 may proceed together; broad SQL support cannot block report-store RPC work. |
| D11 | Retryability and replay safety are separate. Persist step outcomes, never automatically replay unknown effects, and fence or stop an old writer before starting another. |
| D12 | User SQL cannot attach an unrestricted shared host store or control database. The reference uses per-owner stores; internal metadata stays inaccessible. |
| D13 | Incremental loads only append or merge without deleting missing rows. Full refresh is an explicit complete-result override. |
| D14 | Alert instance IDs include a monotonic key generation and lossless typed keys; changing key columns never reuses a superseded generation. |
| D15 | Authorship, management ownership with a durable parent, and execution identity are separate. The hosting worker resolves/enforces them; transfers preserve data and never copy grants. |
| D16 | `retry_run` resumes frozen work; `resolve_run` records worker-verified outcomes. Failures and recovery actions are queryable. `run_now` always creates new work. |
| D17 | Python dataclasses/Protocol classes in `vgi/reporting/` define schemas, defaults and signatures; [generated reference](python-contracts.md) reproduces source. [Wire behavior](wire-contracts.md) defines lifecycle, clocks and encodings. |
| D18 | Deployment, backups, restoration, key administration, upgrades, retention and offboarding integration are worker policy, subject to advertised protocol guarantees. |
| D19 | Persistent folders have stable IDs and parent IDs; report location is outside its immutable envelope. Reorganization preserves content and references. Folder deletion requires emptiness; workers choose access inheritance. |
| D20 | Workers may define and enforce folder/report policies. The protocol exposes permission hints and standard refusals; it prescribes no policy interface, inheritance model or evaluation algorithm. |

The 2026-10-07 review fixed attachment identity, isolation, retries and alert
generations. The 2026-10-08 scope decision replaces the SQL-mutation plan with
read-only SQL plus RPC mutations. Reporting no longer requires a new SQL
execution-context wire revision. The same day's lifecycle decision adds usable
recovery, independent ownership/execution identities, and exact shared contracts.
These services remain proposed; operational administration stays with workers.

## Working rules

**Where work happens**

- Edit and validate documentation locally, including Python contract checks, HTML generation and strict
  MkDocs builds. The remote rule below is for native implementation builds.

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
`reporting-protocols.html` and separate `reference/*.html` pages from the markdown. After a Python contract change, run
`uv run python scripts/regen_generated.py --only vgi-python` first, then
`uv run --script scripts/build_reporting_protocols_html.py`. Never edit the
generated `python-contracts.md` index or `reference/*.md` pages by hand.

### P1 Unlimited default grant lifetime

The design says a grant's lifetime is the issuing worker's policy and unlimited
unless set. vgi-rpc still defaults to 7 days (`DEFAULT_MAX_TTL_SECONDS` in
`vgi_rpc/grants.py`), so every unattended job would stop a week after its
execution principal's last login.

- Decide the wire form of "no expiry" in the grant spec first (an absent or
  zero `exp`), with test vectors.
- Unset `VGI_RPC_GRANT_MAX_TTL_SECONDS` means no maximum, in vgi-rpc and all
  six ports. Attach tickets follow the worker's grant maximum, so check the
  SDKs' ticket expiry the same way.
- **Done when** the shared vectors pass in every port, `expires_at` is `+inf`
  (or `NULL` in `vgi_export_session()`) for an unlimited worker, and the ports
  are released.

P1 doesn't block P2 to P4; development can set an explicit TTL meanwhile.

### P2 Read-only SQL adapter

Build the generic adapter alongside P3. It serves explicitly annotated RPC
reads through existing VGI catalog/table-function facilities; the report
service and Cupola's RPC client do not wait for engine work or a coordinated
wire revision.

- Start with a toy implementation and reports, using the existing domain dataclasses
  and direct method signatures in `vgi/reporting/`. The definitions cover all
  seven protocols; implement each service only when its phase lands.
- Use the existing `@sql_read` annotations for safe methods, table rows and
  fetched columns. Implement
  the [shared derivation rules](README.md#derivation-rules), permission parity,
  filtering, pinned-revision body fetching and SQL error mapping.
- Register no reporting DML handlers or mutation functions. Exclude test/run
  methods that execute supplied SQL, even if they send no notifications.
- `scripts/regen_generated.py --only vgi-python --check` checks generated Python
  documentation and read bindings. Extend conformance fixtures from the Python records
  and compare field order, types, nullability and defaults across SDKs. Exercise
  existing producer continuations, empty streams, revoked access and expired
  cursors; no second application paging envelope is added.
- **Done when** each derivation rule has a toy-protocol check, invalid
  annotations fail startup, and RPC/SQL reads agree on rows and authorization.
  Extension tests must assert zero mutations for DML or attempted calls to
  omitted methods through SELECT, CALL, CTEs, EXPLAIN and prepared statements.
  Ordinary 2.1.0 VGI clients need no new SQL-context capability for these reads.

### P3 `vgi.reports.v1`

- The store on `FunctionStorage`, worker-chosen authorization (grant callers
  have no claims), `ReadOnlyReportStore`, `vgi-report-serve`.
- Implement `ReportRow`, `ReportResult` and `RevisionRow` exactly. Test visible
  revision selection, published-only listing, pinned body fetches and redaction.
- Implement `FolderRecord`, root action hints and all folder RPC methods. Test
  empty folders, nested creation, direct-child/subtree browsing, Unicode sibling
  uniqueness, placement independent of revisions, and stable IDs after moves.
  Cover concurrent reparenting cycles, subtree depth limits, child creation
  racing with deletion, and nonempty deletion with hidden children. Parent
  versions do not track child edits; enforce structural invariants atomically.
- Add worker-owned ownership resolution, `set_ownership` and
  `set_folder_ownership`. Test a departed
  author, parent-admin takeover, preserved history/data and denied transfers;
  authorization must never use authorship as a substitute for current ownership.
  Exercise the worker's chosen folder policies and the shared permission-hint,
  refusal and continuation contracts. File publishers expose stable folder identities and
  reject every folder mutation; flat imports default to root.
- A conformance suite for every RPC method and the SQL read subset, runnable
  against any implementation.
- Extension sqllogictests `test/sql/integration/reports/` against the Python
  service.
- **Done when** RPC conformance and SQL read parity pass, and Cupola can save,
  organize in nested folders, list, publish and read history with no localStorage. The RPC service can be
  implemented and exercised by Cupola while the read adapter is being built.

### P4 `vgi.delegations.v1`

- `put_delegations`, `list_delegations`, `revoke_delegation`; grants and tickets
  encrypted under a key outside the store, never returned. Validate both kinds
  and exact keys; reject duplicate keys in one request. Catalog references are
  reporting-client metadata, not a change to the shipped export schema.
- **Done when** an integration test exports with `vgi_export_session()`, adds
  the client's attachment references, stores them through `put_delegations`
  RPC, and reattaches with `ATTACH … (bearer_token, attach_ticket)`. SQL lists
  only delegation metadata; RPC revocation makes the next lookup fail. Test two
  attachments with identical location/catalog but different options and version
  specs; renewal and revocation of either reference must leave the other intact.
- Test execution-principal mapping of someone else's report sources without alias/location
  fallback. Test service-grant acquisition, renewal and revocation against a
  report store hosting reports and Identity only, with no VGI catalog or tickets.
  Test atomic versioned renewals, stale revocations and unlimited-expiry
  normalization; a manager cannot read or provision another identity's grants
  simply by choosing that identity in a resource.

### P5 Run engine and `vgi.schedules.v1`

- The shared engine from [schedules.md](schedules.md#reference-scheduler-non-normative):
  `tick` / `advance_run`, `vgi.scheduler_driver.v1` (allowlisted), leases,
  persisted intents and step outcomes, hardened per-run sessions built from
  catalog delegations, and the normative retry/reconciliation contract.
- `vgi.schedules.v1` with `run_query`; deliveries wait for P7.
- Implement independent execution identity and the recovery records/RPCs.
  Reauthorization replaces credentials for the same authorized identity;
  ownership transfer never rewrites frozen runs. Identity changes require idle,
  resolved work and an authorized explicit re-enable.
- **Done when** a schedule runs as its execution principal from delegations, runs are unique
  on `(schedule_id, scheduled_for)` across two replicas, overlap is `skipped`,
  and a missing reference is `grant_required`. Inject a lost response after a
  write, an expired lease while its writer is still alive, and a crash before
  recording a completed step; prove no automatic replay of unknown effects.
  Read-only retries and confirmed rollbacks must still recover within budget.
  Test failed-attempt history, next retry times, duplicate recovery requests,
  evidence rejection, partial effects, and preserved parameters across delayed
  retries. Run the normative cron/DST and relative-period vectors.

### P6 `vgi.sql_tasks.v1`

- `load` (four modes), `script` (three transaction kinds), both watermark
  kinds, `lookback`, the `host` store (DuckLake by default), health and
  transition notifications (deliveries once P7 lands).
- Enforce the incremental mode matrix and full-refresh override. Establish
  owner isolation before executing any user SQL; the reference gives each
  owner a separate store and keeps control/progress state outside it.
  Serialize derived loads by target across tasks; document the required
  exclusion of external writers rather than relying on a per-task lease.
- **Done when** derived progress follows confirmed commit/rollback across
  crashes, unknown commits stop automatic work, and stored-progress recovery
  never blindly replays an append or script. Test empty windows, lookback,
  rejected incremental replace/snapshot/delete-missing combinations, and
  explicit full refresh. Verify that two owners cannot read/write/drop each
  other's host tables or service metadata, including through views and CALLs.
  A script writing two databases in `single` fails with `transaction_failed`;
  `test_run` leaves targets untouched and rejects nontransactional effects.
  Transfer ownership of a populated host target: retain target ID/data, permit
  authorized new management, and deny access to unrelated old-owner tables.

### P7 `vgi.notify.v1` reference and deliveries

- A reference service with a recording fake channel and a webhook channel;
  destination policy and suppression list. Wire schedule deliveries and task
  notifications to it. The production service is the separate `vgi-notify`
  repo, later.
- Test per-destination persisted outcomes, partial acceptance, ambiguous
  provider responses and the 24-hour deduplication boundary. A retry cannot
  resend a confirmed destination or silently turn unknown into failed.

### P8 `vgi.alerts.v1`

- Rules, instances and states, notifications on transitions, acknowledge,
  snooze, subscriptions, on the shared engine. Implement the fixed instance
  schema with one-row Arrow IPC details and the lossless key encodings. Test
  unrelated detail schemas, wide integers, decimals, signed zero and denied
  detail access after an ownership transfer.
- Persist key generations and fence evaluation results against both rule
  version and generation. Run identity vectors for A → B → A key changes,
  equal values, restarts, stale evaluations, reminders and notification threads.

### P9 `vgi.report_render.v1` and `render_report`

- Depends on a headless Cupola renderer. Then enable the `render_report`
  schedule action.
- End-to-end test a standalone report store with a service grant and two
  same-location catalog references. Forward only required catalog delegations
  to the renderer; never forward the report-store grant.

### P10 Cupola client

- Starts after P3: reports client and migration off localStorage, then explicit
  attachment-reference mappings and standalone service grants (P4). Preserve
  mappings on renewal and require mapping new sources when scheduling another
  owner's report. Schedule/task editors validate incremental modes and show
  failed steps, retry times and permitted retry/reauthorization/resolution actions
  (P5, P6). Distinguish resume from a new run; expose ownership and execution
  identity separately. Alerts follow in P8.
  TypeScript types and clients come from codegen.

## Contract acceptance and worker policy

- Use the Python records in `vgi/reporting/` and [behavior examples](wire-contracts.md)
  as the source for SDK types and conformance fixtures; implementations must not independently
  choose nullability, enum encodings, timestamp units or mutation preconditions.
- Each phase supplies executable schema and behavior vectors before its protocol
  ships. Validate recovery after failure, ownership changes without data loss,
  pagination/authorization, and the temporal/key examples relevant to that phase.
- `get_*_info` publishes the typed limits and retry policy chosen by that worker;
  numeric defaults are implementation policy, not missing protocol decisions.
- Workers choose deployment, backups, restoration, key administration, upgrades,
  retention, diagnostics and offboarding integration. Reference implementation
  notes are examples, not new protocol APIs or deployment requirements. Any
  worker must still honor acknowledged writes, deduplication and outcome safety;
  an uncertain restored operation is unknown, never an assumed rollback.
