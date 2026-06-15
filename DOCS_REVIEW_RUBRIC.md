# vgi-python docs — senior DX review rubric

> Status: review checklist for the review-ready v1 of the documentation
> (see `DOCS_ACCEPTANCE_CRITERIA.md`). A reviewer scores each item Pass / Fix /
> N/A. Every **Fix** must be resolved (or explicitly waived) before the site
> goes live at `vgi-python.query.farm`.

Reviewer: ________________   Date: ________________   Commit: ________________

## 1. Orientation (the problem this rework targets)

- [ ] The home page makes it obvious in <10 s what vgi-python is and where to start.
- [ ] Top-level nav clearly separates **Tutorial / How-to / Concepts / API Reference**
      (Diátaxis); a newcomer can tell which to open for their need.
- [ ] Every tutorial/how-to/concept page opens with a **"what + who"** line.
- [ ] No page is a dead end — each ends with **"Next steps"** links.

## 2. The fast path (job-to-be-done: ship a worker fast)

- [ ] The tutorial gets a reader from zero → a **scalar + table** function callable from
      DuckDB, and is realistically completable in **≤20 minutes**.
- [ ] The first step yields a working query quickly (scalar before table).
- [ ] Haybarn is the primary path; the stock-DuckDB variant is present and correct.
- [ ] Copy-paste works: the worker shown is complete and runnable as-is.

## 3. Completeness vs. the in-scope list

- [ ] All four function patterns are documented with a runnable example: scalar, table,
      table-in-out, aggregate.
- [ ] Catalogs / ATTACH, state storage, auth + HTTP, and filter pushdown & stats each have a
      how-to.
- [ ] Out-of-scope topics (transactor, external storage, observability, sharding/codegen/secret
      service) are reference-only and not advertised as v1 guides.

## 4. Correctness

- [ ] Every code example is accurate and runs (CI: `test_documentation_examples.py` +
      `test_examples_workers.py` green).
- [ ] SQL snippets use correct catalog/function names and match the worker shown.
- [ ] Conceptual claims (lifecycle phases, transports, Arrow semantics) are accurate.
- [ ] API reference renders for every in-scope module (CI: `mkdocs build --strict` green).

## 5. Scannability & progressive disclosure

- [ ] Pages use headings, tables, and short paragraphs; an expert can skim.
- [ ] Newcomer background is in collapsible callouts, not blocking the main flow.
- [ ] Prerequisites and required extras (`[http]`, `[oauth]`, …) are stated where needed.

## 6. Navigation & polish

- [ ] No broken links (CI: lychee + strict build green).
- [ ] Search returns sensible results for common terms (worker, scalar, aggregate, ATTACH).
- [ ] Light/dark themes, logo, and code-copy all work.

## Automated gates (must be green at review time)

- [ ] `mkdocs build --strict` — zero warnings
- [ ] `pytest tests/test_documentation_examples.py tests/test_examples_workers.py`
- [ ] lychee link-check
- [ ] Vale prose lint (advisory until vocab is tuned; note residual warnings)

## Sign-off

- [ ] All **Fix** items resolved or waived (waivers noted below).
- [ ] Fresh-dev usability test passed (see `DOCS_USABILITY_TEST.md`).

Waivers / notes:
