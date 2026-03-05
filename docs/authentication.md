# Authentication

VGI supports bearer token, JWT/JWKS, and RFC 9728 OAuth resource metadata
for HTTP transport. Authentication is fully optional — when unconfigured,
all requests are anonymous.

## Quick Start: Static Bearer Tokens

The simplest auth setup uses static bearer tokens via environment variable:

```bash
VGI_BEARER_TOKENS="token1=alice,token2=bob" vgi-serve my_worker.py --http
```

Each entry is split on the first `=`, so principals may contain `=` (e.g.
base64 values). However, **tokens must not contain `=` or `,`** because
those characters are used as delimiters.

Unauthenticated requests receive HTTP 401. Authenticated requests include
the principal in the `AuthContext` available to functions.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `VGI_BEARER_TOKENS` | Comma-separated `token=principal` pairs for static bearer auth |
| `VGI_JWT_ISSUER` | JWT issuer URL (requires `vgi[oauth]` extra) |
| `VGI_JWT_AUDIENCE` | JWT audience string (required when `VGI_JWT_ISSUER` is set) |
| `VGI_JWT_JWKS_URI` | JWKS endpoint URL (auto-discovered from issuer if omitted) |
| `VGI_OAUTH_RESOURCE` | OAuth resource URL for RFC 9728 metadata |
| `VGI_OAUTH_AUTH_SERVERS` | Comma-separated authorization server URLs |
| `VGI_OAUTH_SCOPES` | Comma-separated supported scopes (optional) |
| `VGI_OAUTH_RESOURCE_NAME` | Human-readable resource name (optional) |
| `VGI_OAUTH_CLIENT_ID` | Client ID for MCP compatibility (optional, URL-safe chars only) |

When both `VGI_BEARER_TOKENS` and `VGI_JWT_ISSUER` are set, they are
chained — JWT validation is attempted first, falling back to bearer token
lookup.

## Programmatic API

```python test="skip"
from vgi.serve import create_app, load_worker_class
from vgi.auth import bearer_authenticate_static, OAuthResourceMetadata
from vgi_rpc.rpc import AuthContext

# Static bearer tokens
authenticate = bearer_authenticate_static(tokens={
    "secret-token-1": AuthContext(principal="alice", authenticated=True, domain="bearer"),
    "secret-token-2": AuthContext(principal="bob", authenticated=True, domain="bearer"),
})

app = create_app(
    load_worker_class("my_worker:MyWorker"),
    authenticate=authenticate,
    oauth_resource_metadata=OAuthResourceMetadata(
        resource="https://api.example.com",
        authorization_servers=("https://auth.example.com",),
        client_id="my-client-id",
    ),
)
```

When `authenticate` is passed programmatically, environment variables are
ignored.

## Accessing Auth in Functions

### Scalar Functions (Auth annotation)

Use `Auth` with `Annotated` on a `compute()` parameter:

```python test="skip"
from typing import Annotated
import pyarrow as pa
from vgi import ScalarFunction, Param, Returns, Auth
from vgi.auth import AuthContext

class WhoAmI(ScalarFunction):
    class Meta:
        name = "whoami"

    @classmethod
    def compute(
        cls,
        x: Annotated[pa.Int64Array, Param(doc="dummy input")],
        auth: Annotated[AuthContext, Auth()],
    ) -> Annotated[pa.StringArray, Returns()]:
        name = auth.principal or "anonymous"
        return pa.array([name] * len(x))
```

### Table Functions (ProcessParams)

Table functions access auth via `params.auth_context`:

```python test="skip"
from vgi.table_function import TableFunctionGenerator, ProcessParams

class SecureTable(TableFunctionGenerator):
    @classmethod
    def process(cls, params, state, out):
        if not params.auth_context.authenticated:
            raise PermissionError("Authentication required")
        # ... produce output ...
```

### Bind-Time Auth

Auth is also available during `on_bind()` via `params.auth_context` on
both `BindParams` (table functions) and `BindParameters` (scalar functions).

## Transport Behavior

| Transport | Auth Behavior |
|-----------|---------------|
| HTTP with `authenticate` | Auth validated per-request, `AuthContext` propagated |
| HTTP without `authenticate` | All requests anonymous |
| Stdio (subprocess) | Always `AuthContext.anonymous()` |

## AuthContext API

```python test="skip"
from vgi_rpc.rpc import AuthContext

# Check authentication
ctx = AuthContext(principal="alice", authenticated=True, domain="bearer")
assert ctx.authenticated is True
assert ctx.principal == "alice"
assert ctx.domain == "bearer"

# Anonymous context
anon = AuthContext.anonymous()
assert anon.authenticated is False
assert anon.principal is None

# Require authentication (raises PermissionError if not authenticated)
ctx.require_authenticated()  # OK
anon.require_authenticated()  # raises PermissionError
```
