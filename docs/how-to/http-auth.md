---
description: "How to serve a VGI worker over HTTP and authenticate callers with bearer tokens or JWT/OAuth."
---

# Serve over HTTP with authentication

**What this is:** how to run a worker over **HTTP** instead of a subprocess, and gate it with
authentication.<br>
**Who it's for:** developers deploying a worker as a network service.<br>
**Requires:** `pip install vgi-python[http]` (and `[oauth]` for JWT).

## Prerequisites

- A working worker (see the [tutorial](../tutorial/index.md)).
- `pip install vgi-python[http]`; for JWT/OAuth also `vgi-python[oauth]`.

## Serve over HTTP

The same worker that runs over a subprocess also runs over HTTP — add `--http`:

```bash
vgi-serve my_worker.py --http
```

DuckDB still attaches the worker over HTTP the usual way (`ATTACH ... (TYPE vgi, LOCATION
'http://...')`). You only need the **Python `Client`** when you're calling the worker *from Python*
rather than from SQL — for tests, scripts, or another service. It connects with `transport="http"`
instead of spawning a subprocess, and exposes the same call methods:

```python
# illustrative — calling the worker from Python over HTTP
from vgi.client import Client

with Client(transport="http", base_url="http://localhost:8080", bearer_token="token1") as client:
    ...  # same .scalar_function() / .table_function() calls as the subprocess transport
```

## Add authentication

The quickest setup is static bearer tokens via an environment variable — comma-separated
`token=principal` pairs:

```bash
VGI_BEARER_TOKENS="token1=alice,token2=bob" vgi-serve my_worker.py --http
```

Unauthenticated requests get HTTP 401. Authenticated requests carry the principal in an
`AuthContext`. Your function reads it through an optional `ctx` parameter: declare `ctx` in the
signature and the framework injects a per-call `CallContext` (auth, logging, transport info) — you
don't pass it from SQL.

```python
# illustrative — reading the injected ctx in a function
class Secret(ScalarFunction):
    @classmethod
    def compute(cls, value, *, ctx) -> ...:
        ctx.auth.require_authenticated()      # raises if anonymous
        principal = ctx.auth.principal         # "alice"
        ...
```

For JWT/JWKS and RFC 9728 OAuth resource metadata, set `VGI_JWT_ISSUER` / `VGI_JWT_AUDIENCE`
(requires `vgi-python[oauth]`) — see the [Authentication reference](../authentication.md) for the
full variable list and programmatic `make_wsgi_app(authenticate=...)` API.

## Next steps

- **Full auth options** (JWT, OAuth metadata, custom callbacks) →
  [Authentication reference](../authentication.md).
- **Request context** (`ctx.auth`, logging) → [API Reference: Auth & Secrets](../api/auth.md).
