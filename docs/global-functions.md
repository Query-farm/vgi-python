# Global functions

A VGI catalog normally exposes its functions under its own name — `mycat.main.my_func()`.
Some functions aren't *about* the attached catalog at all: diagnostics, converters, format
helpers. For those, a worker can ask the client to also publish them into the **global**
function namespace (DuckDB's `system.main`), so they can be called unqualified:

```sql
ATTACH 'acme' AS acme (TYPE vgi, LOCATION 'acme-worker');
SELECT * FROM acme_table_info('acme');   -- no catalog qualifier
```

This mirrors how `ducklake_table_info` is reachable after `LOAD ducklake`, except that a VGI
worker's globals appear at `ATTACH` time rather than at extension load.

## Declaring globals

Add the function classes to `Catalog.global_functions` and pick a prefix:

```python
from vgi.catalog import Catalog, Schema

CATALOG = Catalog(
    name="acme",
    global_function_prefix="acme",
    global_functions=[TableInfoFunction, ChecksumFunction],
    schemas=[
        Schema(name="main", functions=[TableInfoFunction, ChecksumFunction, ...]),
    ],
)
```

All four function types are supported: scalar, aggregate, table, and table-buffering.

**Every global must also appear in exactly one `Schema.functions`.** This is enforced in
`Catalog.__post_init__`. Bind dispatch is keyed on `(schema_name, name)`, so a function listed
only in `global_functions` would be registered by the client but never dispatchable. Keeping it
schema-resident also means the qualified name (`acme.main.table_info()`) keeps working, which is
the unambiguous fallback if the global name is unavailable.

### The prefix

`global_function_prefix` is applied by the client to form the globally visible name:

| prefix | function name | published as |
|---|---|---|
| `"acme"` | `table_info` | `acme_table_info` |
| `None` | `table_info` | `table_info` |

The prefix must match `[a-z_][a-z0-9_]*` — it is concatenated into a SQL identifier.

**Prefer a prefix.** `system.main` is shared by every extension and every attached VGI worker in
the process. An unprefixed name like `table_info` is very likely to be claimed by someone else,
in which case yours is silently skipped (see below). A prefix that identifies your worker makes
collisions essentially impossible.

## What the client does with them

Registration is **best-effort and advisory**. Do not build anything that requires a global name
to resolve — treat it as an ergonomic alias for the schema-qualified name, which is the one with
guarantees.

Specifically, on the DuckDB side:

- **First attach wins.** If the name is already owned by a different worker, yours is skipped and
  logged; your `ATTACH` still succeeds and the function stays reachable at its qualified path.
- **Re-`ATTACH` is idempotent.** Attaching the same worker again reuses the existing registration
  and refreshes its connection state.
- **The user can turn it off.** `ATTACH ... (TYPE vgi, global_functions false)` suppresses
  publishing entirely for that attach.
- **Registration outlives `DETACH`.** DuckDB has no API to unregister a function, so the entry
  persists for the life of the process. Calling it after `DETACH` raises an error telling the user
  to re-`ATTACH` rather than silently reconnecting to a detached catalog.

Use `vgi_global_functions()` to see what is currently published, who owns it, and whether it is
still live.

## Protocol

Globals ride on the existing attach response — there is no extra round trip and no separate RPC.
`CatalogAttachResult` carries:

- `global_functions: list[bytes]` — IPC-serialized `FunctionInfo` records. `name` and
  `schema_name` are the real dispatch coordinates; the prefix is *not* baked into `name`.
- `global_function_prefix: str` — empty string means publish bare names.

Both fields are additive and default to empty, so a worker that doesn't set them advertises
nothing. Introduced in protocol version 1.3.0.
