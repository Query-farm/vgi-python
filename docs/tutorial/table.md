---
description: "Tutorial step 2: add a table function that generates rows, callable from DuckDB."
---

# 2. Add a table function

**What this is:** the second tutorial step — extend your worker with a **table** function that
generates rows.<br>
**Who it's for:** anyone who's finished
[step 1](scalar.md). About **10 minutes**.

A table function produces rows from its arguments (no input table):
`SELECT * FROM series(3)` returns three rows.

## Step 1 — Extend the worker

Update your worker to add a `Series` table function. The full file — scalar function unchanged,
table function added — is below. Save it as `calc_worker.py`:

```python
--8<-- "examples/calc_worker.py"
```

The new pieces, compared to step 1:

1. **`SeriesArgs`** — a typed arguments dataclass. `Arg(0, ...)` makes `count` the first positional
   SQL argument.
2. **`Series`** — the generator. `FIXED_SCHEMA` declares its output columns; `process` emits the
   rows with `out.emit(...)` and signals completion with `out.finish()`. Here it emits everything
   in one call — no state to track. The `@bind_fixed_schema` and `@init_single_worker` decorators
   wire up the bind/init lifecycle for the common single-worker case.

??? info "Scalar vs. table — when do I use which?"
    Use a **scalar** function when output has exactly one row per input row (a transform). Use a
    **table** function when you generate rows independent of any input — a sequence, a data source,
    an API result set. There are three more patterns (table-in-out, aggregate, and buffering)
    covered in the [how-to guides](../how-to/function-patterns.md).

??? info "Generating a lot of rows? Stream with state"
    `process` is actually called *repeatedly* until you call `out.finish()`. For large results you
    don't build one giant batch — you emit a bounded chunk per call and remember your place in a
    small **state** object. That's the next thing to learn:
    [streaming with state](../how-to/function-patterns.md#streaming-with-state).

## Step 2 — Attach and call it

Re-attach the updated worker, then call both functions:

```sql
ATTACH 'calc' (TYPE vgi, LOCATION 'uv run calc_worker.py');

-- The scalar from step 1 still works:
SELECT calc.double(21);

-- The new table function generates rows:
SELECT * FROM calc.series(3);
-- ┌─────┐
-- │ n   │
-- ├─────┤
-- │ 0   │
-- │ 1   │
-- │ 2   │
-- └─────┘
```

That's both function patterns running from SQL. 🎉

??? success "It didn't work?"
    - **`Binder Error: table function ... does not exist`** — the SQL name is the snake_case of the
      class name (`Series` → `series`) and a table function is called in `FROM`, not `SELECT`:
      `SELECT * FROM calc.series(3)`, not `SELECT calc.series(3)`.
    - **The query hangs and never returns** — a table generator must call `out.finish()` when it has
      no more rows. Without it the framework keeps calling `process` forever.
    - **`ATTACH` errors after editing** — if `calc` is already attached from step 1, `DETACH calc;`
      first (or attach under a new name).

## Next steps

- **More function patterns** → [How-to: function patterns](../how-to/function-patterns.md) covers
  table-in-out (streaming transforms), aggregates, buffering, and a string-valued scalar.
- **Understand what just happened** → [Concepts: worker lifecycle](../concepts/index.md) explains
  bind → init → process → finish and the transports.
- **Look up the exact API** → the [API Reference](../api/index.md) documents every class and
  argument type.
