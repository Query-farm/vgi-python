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
| `VGI_JWT_ISSUER` | JWT issuer URL (requires `vgi-python[oauth]` extra) |
| `VGI_JWT_AUDIENCE` | JWT audience string, comma-separated for multiple audiences (required when `VGI_JWT_ISSUER` is set) |
| `VGI_JWT_JWKS_URI` | JWKS endpoint URL (auto-discovered from issuer if omitted) |
| `VGI_OAUTH_RESOURCE` | OAuth resource URL for RFC 9728 metadata |
| `VGI_OAUTH_AUTH_SERVERS` | Comma-separated authorization server URLs |
| `VGI_OAUTH_SCOPES` | Comma-separated supported scopes (optional) |
| `VGI_OAUTH_RESOURCE_NAME` | Human-readable resource name (optional) |
| `VGI_OAUTH_CLIENT_ID` | Client ID for MCP compatibility (optional, URL-safe chars only) |
| `VGI_OAUTH_CLIENT_SECRET` | Client secret for OAuth (optional, URL-safe chars only) |
| `VGI_OAUTH_USE_ID_TOKEN` | When `1`/`true`/`yes`, clients use OIDC `id_token` as Bearer instead of `access_token` |

When both `VGI_BEARER_TOKENS` and `VGI_JWT_ISSUER` are set, they are
chained — JWT validation is attempted first, falling back to bearer token
lookup.

## Grants and resolved tokens as bearer credentials

`vgi_rpc.Identity.v1`'s `issue_grant` mints a standing credential that
unattended automation later presents as an ordinary bearer. Over HTTP the
worker accepts two kinds of identity credential after the deployment's own
authenticator:

1. **Sealed grants** (opt-in). Set `VGI_RPC_GRANT_KEYS` (or
   `vgi-serve --http --grant-key KEY`, repeatable). `issue_grant` then mints a
   `vgig1.` grant sealed with the first key, unless the worker overrides
   `mint_grant`. Every key verifies, so to rotate you add the new key first and
   drop the old one once its grants have expired. A request carrying
   `Authorization: Bearer vgig1.…` is authenticated as the grant's owner, with
   `domain="grant"` and claims `grant_id`, `scopes` and `purpose`. The claims
   carry no `auth_time`, so a grant cannot mint another grant.
2. **`resolve_token`**. A worker that overrides
   [`Worker.resolve_token`][vgi.worker.Worker.resolve_token] has it consulted
   for any other bearer, with `domain="token"`. `None` means the credential is
   unknown, which is a 401 unless something else accepts it.
   `AuthUnavailableError` is a 503 with the hook's `Retry-After`.

The order is the deployment's authenticator (JWT, static bearer), then sealed
grants, then `resolve_token`. A `vgig1.` token that does not verify is a 401
that stops the chain: it never reaches `resolve_token`. A JWS-shaped token or
one over 4096 bytes never reaches the hook either. Your own `authenticate`
callback must raise `ValueError` for a credential it does not recognise, so the
chain moves on. One that answers anonymous for every request ends the chain
before grants are checked. With no authenticator configured, a request with no
`Authorization` header stays anonymous as before.

| Variable | Description |
|----------|-------------|
| `VGI_RPC_GRANT_KEYS` | Comma-separated standard-base64 keys, 32 bytes each. The first mints and all verify. Unset means grants are off. A malformed key stops startup |
| `VGI_RPC_GRANT_AUDIENCE` | Bound into every grant, so two deployments that share a key still reject each other's grants |
| `VGI_RPC_GRANT_MAX_TTL_SECONDS` | Ceiling on a grant's lifetime (default 7 days) |

The startup rules are unchanged. Overriding `resolve_token` still requires
`VGI_INTROSPECT_PRINCIPALS`. Grant keys alone need no allowlist, because
minting answers only for the caller. Grants and `resolve_token` bearers are
HTTP-only, like the rest of Identity. When `VGI_PROXY_PROOF_MODE` is on, both
are accepted only *inside* the proxy-proof gate, never as an alternative to it.

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
        use_id_token_as_bearer=True,  # use OIDC id_token as Bearer
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
