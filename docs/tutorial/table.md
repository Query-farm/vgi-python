---
description: "Tutorial step 2: add a table function that generates rows, callable from DuckDB."
---

# 2. Add a table function

**What this is:** the second tutorial step — extend your worker with a **table** function that
generates rows. **Who it's for:** anyone who's finished
[step 1](scalar.md). About **10 minutes**.

A table function produces rows from its arguments (no input table):
`SELECT * FROM greeting_series(3)` returns three rows.

## Step 1 — Extend the worker

Update your worker to add a `GreetingSeries` table function. The full file — scalar function
unchanged, table function added — is below. Save it as `greeting_worker.py`:

```python
--8<-- "examples/greeting_worker.py"
```

The new pieces, compared to step 1:

1. **`GreetingSeriesArgs`** — a typed arguments dataclass. `Arg(0, ...)` makes `count` the first
   positional SQL argument.
2. **`GreetingSeriesState`** — a small cursor tracking how many rows we've emitted. It extends
   `ArrowSerializableDataclass` so the same worker also works over the HTTP transport (the
   framework requires serializable state for table generators).
3. **`GreetingSeries`** — the generator. `FIXED_SCHEMA` declares its output columns; `process`
   emits one batch per call and signals completion with `out.finish()`. The `@bind_fixed_schema`
   and `@init_single_worker` decorators wire up the bind/init lifecycle for the common
   single-worker case.

??? info "Scalar vs. table — when do I use which?"
    Use a **scalar** function when output has exactly one row per input row (a transform). Use a
    **table** function when you generate rows independent of any input — a sequence, a data
    source, an API result set. There are two more shapes (table-in-out and aggregate) covered in
    the [how-to guides](../how-to/index.md).

## Step 2 — Attach and call it

Re-attach the updated worker (or use a fresh catalog name), then call both functions:

```sql
ATTACH 'greetings' (TYPE vgi, LOCATION 'uv run greeting_worker.py');

-- The scalar from step 1 still works:
SELECT greetings.greeting('Alice');

-- The new table function generates rows:
SELECT * FROM greetings.greeting_series(3);
-- ┌────────────────────┐
-- │ greeting           │
-- ├────────────────────┤
-- │ Hello, friend #0!  │
-- │ Hello, friend #1!  │
-- │ Hello, friend #2!  │
-- └────────────────────┘
```

That's both function patterns running from SQL. 🎉

## Next steps

- **More function shapes** → [How-to: function patterns](../how-to/index.md) covers table-in-out
  (streaming transforms) and aggregate functions.
- **Understand what just happened** → [Concepts: worker lifecycle](../concepts/index.md) explains
  bind → init → process → finish and the transports.
- **Look up the exact API** → the [API Reference](../api/index.md) documents every class and
  argument type.
