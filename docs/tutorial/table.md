---
description: "Tutorial step 2: add a table function that generates rows, callable from DuckDB."
---

# 2. Add a table function

**What this is:** the second tutorial step — extend your worker with a **table** function that
generates rows. **Who it's for:** anyone who's finished
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
2. **`SeriesState`** — a small cursor tracking how many rows we've emitted. It extends
   `ArrowSerializableDataclass` so the same worker also works over the HTTP transport (the
   framework requires serializable state for table generators).
3. **`Series`** — the generator. `FIXED_SCHEMA` declares its output columns; `process` emits one
   batch per call and signals completion with `out.finish()`. The `@bind_fixed_schema` and
   `@init_single_worker` decorators wire up the bind/init lifecycle for the common single-worker
   case.

??? info "Scalar vs. table — when do I use which?"
    Use a **scalar** function when output has exactly one row per input row (a transform). Use a
    **table** function when you generate rows independent of any input — a sequence, a data source,
    an API result set. There are two more shapes (table-in-out and aggregate) covered in the
    [how-to guides](../how-to/function-patterns.md).

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

## Next steps

- **More function shapes** → [How-to: function patterns](../how-to/function-patterns.md) covers
  table-in-out (streaming transforms), aggregates, and a string-valued scalar.
- **Understand what just happened** → [Concepts: worker lifecycle](../concepts/index.md) explains
  bind → init → process → finish and the transports.
- **Look up the exact API** → the [API Reference](../api/index.md) documents every class and
  argument type.
