---
description: "Tutorial step 1: build and run a scalar VGI function callable from DuckDB."
---

# 1. Your first scalar function

**What this is:** the first tutorial step — build a worker with one **scalar** function and call
it from SQL. **Who it's for:** first-time VGI users who've met the
[prerequisites](index.md#prerequisites). About **10 minutes**.

A scalar function maps one row to one row: `greeting("Alice")` → `"Hello, Alice!"`.

## Step 1 — Write the worker

Create a file called `greeting_scalar_worker.py`:

```python
--8<-- "examples/greeting_scalar_worker.py"
```

What's going on:

1. **The `# /// script` header** is [inline script
   metadata](https://packaging.python.org/en/latest/specifications/inline-script-metadata/) — it
   tells `uv run` which dependencies to provision, so there's no virtualenv to create and nothing
   to `pip install` first.
2. **`Greeting.compute` receives a whole column** (`pa.StringArray`) and returns a column of the
   same length. The `Annotated[..., Param(...)]` and `Annotated[..., Returns()]` types *are* the
   schema — VGI derives the SQL signature from them.
3. **`GreetingWorker` exposes a catalog** named `greetings` containing the function.

??? info "Why a column instead of a single value?"
    VGI hands your function a batch of rows as an Arrow array, not one value at a time. Operating
    on the whole column with `pyarrow.compute` (here `pc.binary_join_element_wise`) is what keeps
    it fast. If you've written a DuckDB UDF before, this is the vectorized equivalent.

## Step 2 — Launch a SQL engine and attach the worker

=== "Haybarn (recommended)"

    [Haybarn](https://github.com/Query-farm-haybarn/haybarn) is Query.Farm's DuckDB distribution.
    It ships the `vgi` extension and runs with no install via `uvx`:

    ```bash
    uvx haybarn-cli
    ```

    At the prompt, attach your worker. `LOCATION` is the command Haybarn runs to launch it:

    ```sql
    ATTACH 'greetings' (TYPE vgi, LOCATION 'uv run greeting_scalar_worker.py');
    ```

=== "Stock DuckDB"

    With stock [DuckDB](https://duckdb.org/), load the `vgi` extension from the community
    repository first:

    ```sql
    INSTALL vgi FROM community;
    LOAD vgi;
    ATTACH 'greetings' (TYPE vgi, LOCATION 'uv run greeting_scalar_worker.py');
    ```

## Step 3 — Call your function

```sql
SELECT greetings.greeting('Alice');
-- ┌──────────────────────────┐
-- │ Hello, Alice!            │
-- └──────────────────────────┘
```

Over a real column:

```sql
SELECT greetings.greeting(name) FROM (VALUES ('Alice'), ('Bob')) AS t(name);
```

You've built and run your first VGI function. 🎉

??? success "It didn't work?"
    - **`Catalog Error: unknown type "vgi"`** — the extension isn't loaded. On stock DuckDB run
      `INSTALL vgi FROM community; LOAD vgi;` first; on Haybarn it's built in.
    - **The `ATTACH` hangs or errors immediately** — run `uv run greeting_scalar_worker.py`
      directly in a terminal. The worker speaks Arrow over stdin/stdout, so it *looks* like it
      hangs waiting for input — that's expected. You're checking for an import error or traceback
      on stderr.
    - **`Binder Error: function not found`** — the SQL name is the snake_case of the class name
      (`Greeting` → `greeting`), qualified by the catalog name from `ATTACH`.

## Next steps

- **[2. Add a table function](table.md)** — generate rows from an argument.
