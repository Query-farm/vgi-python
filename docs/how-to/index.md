---
description: "Task-oriented recipes for building VGI workers: function patterns, catalogs, state, auth, and optimizer integration."
---

# How-to guides

**What this is:** focused, task-oriented recipes for getting a specific thing done. **Who it's
for:** developers who've finished the [tutorial](../tutorial/index.md) and want to build something
real. Each guide assumes you can already write and run a basic worker.

!!! note "Under construction"
    These guides are being reworked into the recipe format described in
    [the docs contributor guide](../contributing-docs.md). Links below currently point to the
    existing reference material while the rewrite is in progress.

## Recipes

- **[Function patterns](function-patterns.md)** — scalar, table, table-in-out, and aggregate
  functions, with a runnable worker for each. *(Start here.)* Deeper reference:
  [Function API](../generator-api.md) · [Aggregate functions](../aggregate-functions.md)
- **Expose a catalog** — surface schemas, tables, and views to DuckDB via `ATTACH`:
  [Catalog Interface](../catalog-interface.md)
- **Persist state** — keep per-group state across invocations:
  [Shared Storage](../shared-storage.md)
- **Run over HTTP with auth** — serve a worker over HTTP and authenticate callers:
  [Authentication](../authentication.md)
- **Integrate with the optimizer** — accept pushed-down filters and report statistics:
  [Filter Pushdown](../filter-pushdown.md) · [Column Statistics](../column-statistics.md)
- **Describe your functions** — metadata for introspection: [Function Metadata](../metadata.md)
- **Use the CLI** — invoke functions and inspect workers from the shell: [CLI](../cli.md)

## Next steps

- New here? Start with the [tutorial](../tutorial/index.md).
- Want the "why" behind the API? See [Concepts](../concepts/index.md).
- Need exact signatures? See the [API Reference](../api/index.md).
