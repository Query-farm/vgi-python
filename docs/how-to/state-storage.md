---
description: "How to give VGI functions shared, persistent state across worker processes — the default SQLite backend and cloud alternatives."
---

# Persist state across workers

**What this is:** how functions that span **multiple worker processes** (notably distributed
aggregates) share and persist state.<br>
**Who it's for:** developers building aggregates or any
function that coordinates partial results across workers.

## Prerequisites

- You've built an aggregate or multi-worker function (see
  [Function patterns → Aggregate](function-patterns.md#aggregate)).
- For the Azure backend: `pip install vgi-python[azure]`. SQLite and Cloudflare DO need no extra.

!!! note "`vgi-serve`"
    The commands below use `vgi-serve`, the CLI installed with `vgi-python` that runs a worker
    module as a long-lived process (the production counterpart to the tutorial's `uv run`). The
    `--http` flag serves it over HTTP instead of stdin/stdout.

## Two kinds of "state" — don't confuse them

- **Generator cursor state** — the small `ArrowSerializableDataclass` a table generator keeps
  *within one scan* (see [streaming with state](function-patterns.md#streaming-with-state)). It
  lives in the worker for the duration of the call.
- **Shared storage** (this page) — state that must outlive a single call or be shared across
  **separate worker processes**, e.g. combining partial aggregate results. This is backed by a
  pluggable store.

## The default: SQLite (zero config)

Under the subprocess transport, shared storage "just works" — all workers share a local SQLite
database (WAL mode) at the platform state directory. Nothing to configure:

```bash
vgi-serve my_worker.py
```

## Choosing a backend

Select a backend with the `VGI_WORKER_SHARED_STORAGE` environment variable:

```bash
# Local / subprocess (default)
VGI_WORKER_SHARED_STORAGE=sqlite vgi-serve my_worker.py

# Azure cloud (requires vgi-python[azure])
VGI_WORKER_SHARED_STORAGE=azure-sql vgi-serve my_worker.py --http

# Edge / multi-cloud
VGI_WORKER_SHARED_STORAGE=cloudflare-do vgi-serve my_worker.py --http
```

| Backend | Value | Use case | Dependencies |
|---|---|---|---|
| SQLite | `sqlite` (default) | local / subprocess | none (stdlib) |
| Azure SQL | `azure-sql` | Azure deployments | `vgi-python[azure]` |
| Cloudflare DO | `cloudflare-do` | edge / multi-cloud | none extra — uses `httpx`, which ships with `vgi-python`; needs a Worker endpoint + token |

The per-backend setup (connection strings, credentials, table provisioning) is documented in the
[Shared Storage reference](../shared-storage.md).

## Next steps

- **Aggregates** that use this → [Function patterns → Aggregate](function-patterns.md#aggregate).
- **Full backend setup** → [Shared Storage reference](../shared-storage.md).
- **Exact API** → [API Reference: State storage](../api/storage.md).
