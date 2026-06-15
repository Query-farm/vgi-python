# vgi-python docs — fresh-dev usability test

> The headline acceptance gate: an unfamiliar developer, working **only** from the
> docs, builds and runs a worker. This protocol scripts that test and captures
> what to fix. See `DOCS_ACCEPTANCE_CRITERIA.md` for the full criteria.

## Goal (pass condition)

> A developer **new to VGI**, working **unaided** from the published docs, reaches a custom
> **scalar AND table** function callable from DuckDB in **≤20 minutes**.

"Callable from DuckDB" means both:

- `SELECT greetings.greeting('Alice')` returns `Hello, Alice!`, and
- `SELECT * FROM greetings.greeting_series(3)` returns 3 rows.

## Participant criteria

- Comfortable writing Python; **has not** used VGI before.
- A mix is ideal across runs: at least one participant new to DuckDB extensions, and at least
  one new to Apache Arrow (this is the "serve all" audience we're validating).
- Has Python 3.13+ and `uv` installed (or we install them first, off the clock).

## Facilitator rules

- **Do not help.** Point the participant at the docs home page and the timer; then observe in
  silence. Answer only "I can't help with that — what would you try?"
- Record the clock at each milestone and **every** point of confusion verbatim.
- Capture the participant's words ("I don't know what a catalog is here") — those become doc fixes.

## Script

1. Start screen recording (or take notes) and the timer.
2. Give the participant only this: *"Using the vgi-python docs, build a worker that adds two
   functions to DuckDB — one that greets a name, and one that generates a series of greetings —
   and run both from SQL. Start at the docs home page."*
3. Observe until success or 30 minutes elapsed (let them run past 20 so we learn where the tail is).
4. Debrief: what was confusing, what was missing, what they expected to find and didn't.

## Milestones (record the time reached)

| Milestone | Time | Notes |
|---|---|---|
| Found the tutorial / starting point | | |
| Worker file written | | |
| SQL engine launched (Haybarn or DuckDB) | | |
| Worker attached (`ATTACH …`) | | |
| **Scalar query returned a result** | | |
| **Table query returned rows** | | |
| Total time to success (or DNF) | | |

## Stumble log

| # | Where (page / step) | What happened | Severity (block/slow/nit) | Fix |
|---|---|---|---|---|
| 1 | | | | |
| 2 | | | | |
| 3 | | | | |

## Outcome

- [ ] Passed (≤20 min, unaided, both functions) — participant: ____________  time: ______
- [ ] Did not pass — root cause: ____________________________________________

**Every block/slow stumble must produce a doc fix (or an explicit waiver) before sign-off.**

## Runs

| Date | Participant background | Result | Time | Fixes filed |
|---|---|---|---|---|
| | | | | |
