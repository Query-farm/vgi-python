# VGI-Python Documentation — Acceptance Criteria (review-ready v1)

> Status: DRAFT for senior DX-engineer review. Derived from a requirements interview.
> This document defines what "done and good" means for the documentation rework
> before the site goes live at `vgi-python.query.farm`.

## North Star

**A developer who has never used VGI can build and run a real worker fast.**
Everything on the site is optimized around that job-to-be-done; depth is available
but never blocks the fast path.

## Target audience (mixed — serve all via progressive disclosure)

The reader could be a Python developer new to DuckDB/Arrow, a DuckDB/SQL user newer
to Python, or someone fluent in both. Therefore:

- The happy path is skimmable by experts (dense, copy-paste-ready).
- Newcomers are served by **progressive disclosure**: inline "New to Arrow? →" /
  "New to DuckDB extensions? →" callouts and links, not walls of prerequisite text.
- We never assume knowledge silently; we either explain briefly or link out.

## Information architecture — Diátaxis

Top-level navigation is reorganized into the four Diátaxis modes (this directly
addresses the current "hard to orient" problem):

1. **Tutorial** — one guided, end-to-end "build your first worker" path.
2. **How-to guides** — task-oriented recipes ("Add a table function", "Run over
   HTTP with auth", "Persist aggregate state").
3. **Concepts** — explanations: worker lifecycle (bind/init/process/finalize),
   transports, the Arrow data model, catalogs & ATTACH, parallel workers.
4. **API Reference** — the existing auto-generated mkdocstrings pages.

The current 11 hand-written guides are **re-homed** into How-to vs Concepts (not
left in a flat "Guides" bucket).

## Scope

### In scope for v1 (must be fully documented: tutorial coverage + how-to + runnable example)

- **All four function patterns**: scalar, table, table-in-out, aggregate.
- **Catalogs / ATTACH model** — how functions are surfaced to DuckDB.
- **State storage** — `FunctionStorage` backends for stateful/aggregate functions.
- **Auth + HTTP transport** — running a worker over HTTP with bearer/JWT auth.
- **Filter pushdown & column statistics** — optimizer integration for table functions.

### Out of scope for v1 (reference-only / deferred — must NOT block launch)

- Transactor (transactional DB access)
- External storage / large-payload offload (S3/GCS)
- Observability (OpenTelemetry / Sentry)
- Sharding / meta-worker, cross-language client codegen, standalone secret service

These remain available in the auto-generated API reference but get no tutorial/how-to
investment in v1.

## Headline acceptance test (Time-To-First-Success)

> **An unfamiliar developer, working unaided from the docs, has both a custom
> scalar function AND a custom table function callable from DuckDB within
> ≤20 minutes.**

- "Callable from DuckDB" = `SELECT my_cat.my_scalar(col) FROM t` returns rows, and
  `SELECT * FROM my_cat.my_table(args)` returns rows.
- Engine for the timed path: **Haybarn** (`uvx haybarn-cli`) as the primary happy
  path; a stock-DuckDB variant (`INSTALL vgi FROM community; LOAD vgi;`) shown in a
  callout/tab for portability.
- Every place a test participant gets stuck is logged and fixed before sign-off.

## Per-page orientation standard (applies to every tutorial / how-to / concept page)

Each page must contain:

1. **Lead "what + who" line** — one sentence at the top: what this page is and who
   it's for (reader self-orients in <10 s).
2. **Prerequisites stated** — explicit assumed knowledge, prior steps, and required
   extras (`vgi-python[http]`, etc.), with links.
3. **At least one complete, runnable example** — no elisions; covered by the CI
   example tests (see Quality Gates).
4. **"Next steps" links** — a closing section pointing to the logical next page(s);
   no dead ends.

## Example correctness bar

- **100% of Python code blocks are copy-paste runnable and CI-tested** (e.g. via
  `pytest-examples`, already a dev dependency).
- The tutorial worker is **built and queried end-to-end in an automated test**.
- A broken example fails the build.

## Quality gates (all three required to sign off v1)

1. **Fresh-dev usability test** — ≥1 developer unfamiliar with VGI completes the
   headline acceptance test (scalar + table from DuckDB, ≤20 min, unaided). All
   stumbling points resolved.
2. **Senior DX reviewer rubric** — named senior DX engineer(s) score the site
   against a written checklist: orientation, scannability, completeness vs the
   in-scope list, correctness, navigation, and the per-page standard above. All
   must-fix items resolved before merge.
3. **Automated quality gates in CI**:
   - `mkdocs build --strict` passes with zero warnings (no broken links / refs).
   - All documentation examples execute successfully.
   - Link check + prose/style lint pass.

## Definition of Done (v1)

- [ ] Diátaxis nav live (Tutorial / How-to / Concepts / API Reference); existing
      guides re-homed.
- [ ] Guided tutorial takes a reader from zero → scalar + table function queried
      from Haybarn, with the stock-DuckDB variant noted.
- [ ] How-to + runnable example exists for each in-scope topic (4 patterns +
      catalogs + state storage + auth/HTTP + pushdown/stats).
- [ ] Concept pages cover lifecycle, transports, Arrow model, catalogs, parallelism.
- [ ] Every page meets the 4-point orientation standard.
- [ ] All examples runnable and CI-tested; tutorial validated end-to-end in CI.
- [ ] Out-of-scope topics confined to reference; not advertised as v1 guides.
- [ ] All three quality gates passed and signed off.

## Open items to confirm with reviewers

- Named senior DX reviewer(s) and the recruited fresh-dev test participant.
- Final wording/threshold of the prose-style lint (e.g. Vale ruleset), if adopted.
