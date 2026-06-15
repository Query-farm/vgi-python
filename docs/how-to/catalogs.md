---
description: "How to expose a VGI worker as a DuckDB catalog: schemas, function qualification, and tables/views via ATTACH."
---

# Expose a catalog

**What this is:** how a worker presents itself to DuckDB as a catalog — a named namespace of
schemas, functions, tables, and views you reach with `ATTACH`.<br>
**Who it's for:** developers who've finished the [tutorial](../tutorial/index.md) and want to
understand how their functions get qualified names, or who want to expose data (not just functions).

## Prerequisites

- You can build and run a worker (see the [tutorial](../tutorial/index.md)).
- Familiarity with the function patterns is helpful: [Function patterns](function-patterns.md).

## The model

Every worker exposes one **`Catalog`** with a name. Inside it are one or more **`Schema`**
namespaces (DuckDB's default is `main`), each holding functions — and optionally tables and views.
You attach the catalog and address its contents by name:

```sql
ATTACH 'calc' (TYPE vgi, LOCATION 'uv run calc_worker.py');

-- catalog.function  (functions in `main` are reachable as catalog.name)
SELECT calc.double(21);

-- catalog.schema.object  (fully qualified)
SELECT * FROM calc.main.series(3);
```

The worker from the tutorial is exactly this — a catalog named `calc` with a `main` schema holding
the two functions:

```python
--8<-- "examples/calc_worker.py"
```

The SQL name of a function is the snake_case of its class name (`Double` → `double`), unless you
override it with a `Meta.name` (as `sum_worker.py` does for `vgi_sum`).

## Exposing data: tables and views

A catalog can expose more than functions:

- **`View`** — a named SQL query DuckDB evaluates. Pure SQL; no data provider needed:

    ```python
    from vgi.catalog import View
    View(name="recent", definition="SELECT * FROM calc.series(5)")
    ```

- **`Table`** — a queryable table. Define it with an explicit `columns` schema (you supply the
    scan) or back it with a `TableFunctionGenerator` so the schema is derived from the function.

Both are passed to a `Schema(..., tables=[...], views=[...])`. The full set of options —
constraints, generated columns, column comments, filter requirements — is covered in the
[Catalog Interface reference](../catalog-interface.md).

## Next steps

- **Persist per-group state** → [State storage](state-storage.md).
- **Full catalog options** (tables, views, constraints) → [Catalog Interface reference](../catalog-interface.md).
- **Exact API** → [API Reference: Catalogs](../api/catalogs.md).
