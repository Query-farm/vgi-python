---
description: "Task-oriented recipes for building VGI workers: function patterns, catalogs, state, auth, and optimizer integration."
---

# How-to guides

**What this is:** focused, task-oriented recipes for getting a specific thing done. **Who it's
for:** developers who've finished the [tutorial](../tutorial/index.md) and want to build something
real. Each guide assumes you can already write and run a basic worker.

## Recipes

- **[Function patterns](function-patterns.md)** — scalar, table, table-in-out, and aggregate
  functions, with a runnable worker for each. *(Start here.)*
- **[Expose a catalog](catalogs.md)** — surface schemas, functions, tables, and views to DuckDB
  via `ATTACH`.
- **[Persist state across workers](state-storage.md)** — shared, durable state for distributed
  aggregates.
- **[Serve over HTTP with auth](http-auth.md)** — run a worker as a network service and gate it
  with bearer/JWT auth.
- **[Integrate with the optimizer](pushdown-and-statistics.md)** — accept pushed-down filters and
  report column statistics.
- **Describe your functions** — metadata for introspection: [Function Metadata](../metadata.md)
- **Use the CLI** — invoke functions and inspect workers from the shell: [CLI](../cli.md)

Each recipe links to a deeper **reference** page (Function API, Catalog Interface, Shared Storage,
Authentication, Filter Pushdown, Column Statistics) for the full contract.

## Next steps

- New here? Start with the [tutorial](../tutorial/index.md).
- Want the "why" behind the API? See [Concepts](../concepts/index.md).
- Need exact signatures? See the [API Reference](../api/index.md).
