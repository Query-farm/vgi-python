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
| `VGI_JWT_ALGORITHMS` | Comma-separated allow-list of JWS algorithms (e.g. `ES256`). `none` and `HS*` are refused. Unset keeps joserfc's recommended set (`ES256` only in hosted mode) |
| `VGI_ORCHARD_HOSTED` | `1` turns on [Orchard hosted mode](#orchard-hosted-mode) |
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

`vgi_rpc.Identity.v2`'s `issue_grant` mints a standing credential that
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

## Orchard hosted mode

`VGI_ORCHARD_HOSTED=1` runs the worker as an Orchard *Field Worker*. The
hosted base image sets it; you do not set it per deployment. Callers present a
*Pass*: a short-lived [RFC 9068](https://www.rfc-editor.org/rfc/rfc9068) JWT
access token issued by the Orchard Front Desk for one canonical Desk Address.
The worker verifies every Pass locally and accepts no other credential. The
full design is in the extension repository's `docs/location-delegation.md`.

```bash
VGI_ORCHARD_HOSTED=1 \
VGI_JWT_ISSUER=https://auth.queryfarm.cloud \
VGI_JWT_AUDIENCE=https://desk.queryfarm.cloud/d/dep_123 \
VGI_JWT_JWKS_URI=https://auth.queryfarm.cloud/.well-known/jwks.json \
vgi-serve my_app:MyWorker --http
```

**Startup fails closed.** The worker exits with an error and a non-zero status
unless all three of `VGI_JWT_ISSUER`, `VGI_JWT_AUDIENCE` and
`VGI_JWT_JWKS_URI` are set. Unset and empty are both errors. Each takes
exactly one value: there is no comma splitting, and a value containing a comma
is refused. It also refuses to start when any other way in is configured:

- `VGI_BEARER_TOKENS`;
- sealed-grant keys (`VGI_RPC_GRANT_KEYS` or `--grant-key`);
- a worker that overrides `resolve_token` or `mint_grant`;
- `VGI_OAUTH_RESOURCE` (a Field Worker never advertises `resource_metadata`);
- `VGI_PROXY_PROOF_MODE` other than `off`, or Iroh / peer identity;
- a custom `authenticate` callback passed to `create_app`;
- stdio (no `--http`, for both `vgi-serve` and `Worker.main`), `--tcp`, `--unix`
  or `--iroh-raw-upstream`, which carry no Pass;
- `--server granian`, which cannot cap a request body before authentication
  (see below);
- a `VGI_ORCHARD_HOSTED` value other than `1`/`0`/`true`/`false`/`yes`/`no`/`on`/`off`.

**Verification:**

- algorithm `ES256` only (`VGI_JWT_ALGORITHMS` overrides; `none` and `HS*`
  are always refused);
- JOSE header `typ` must be `at+jwt` (case-insensitive, `application/` prefix
  optional);
- `iss` and `aud` must match exactly;
- `exp` is required, and `exp` / `nbf` are checked with 60 s of clock skew;
- the JWKS is cached for at most 10 minutes, or less if its
  `Cache-Control: max-age` is shorter. When that time is up the keys are
  discarded. If the re-fetch fails, requests get a 503 until it succeeds;
  stale keys are never used;
- a Pass with an unknown `kid` triggers a JWKS re-fetch at most once every 60
  seconds. Between re-fetches it is simply rejected.

**Every request needs a Pass**, except `HEAD /health` (the Front Desk's
liveness probe) and CORS preflights, which browsers send without credentials.
That includes `GET /health`, `OPTIONS /health`, the landing and describe pages
and unknown paths. A request without a valid Pass, including one with no
`Authorization` header at all, gets:

```text
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Bearer error="invalid_token"
VGI-Auth-Reason: missing_credential | invalid_credential | expired_credential
VGI-Supported-Encodings: ...        (and the other VGI capability headers)
```

There is no `resource_metadata` in the challenge. If the JWKS cannot be
fetched, the answer is 503 with `Retry-After` instead: the worker cannot tell
whether the Pass is valid, so it does not claim that it isn't. Authentication
runs before the app reads, size-checks or decompresses the request body.

**Request-body cap.** Waitress receives a whole request body before it calls
the app, so before the Pass is checked. In hosted mode its buffer limit is the
worker's request cap: `VGI_RPC_MAX_REQUEST_BYTES`, or 32 MiB when that is
unset. The cap is advertised as `VGI-Max-Request-Bytes`, and larger requests
go through upload URLs. A request whose `Content-Length` is over the cap gets
waitress's 413 before any of the body is read. What remains is bounded: a
caller without a Pass can make the worker hold at most one capped body per
open connection (waitress accepts up to 100 connections by default) until
the 401. granian has no such cap, so hosted mode refuses `--server granian`. CORS is
open (`Access-Control-Allow-Origin: *`), because the only credential is the
bearer Pass. The `Authorization` header is never logged, including under
`--debug`.

**Function code sees the Pass's claims** through the usual
[`Auth()`](#scalar-functions-auth-annotation) / `ProcessParams.auth` path, with
their JSON types (`email_verified` is a `bool`, `exp` an `int`).
`AuthContext.principal` is `sub` and `AuthContext.domain` is `"orchard"`.

| Claim | Meaning |
|-------|---------|
| `sub` | Users: the WorkOS user id. Workloads: an opaque Registry id |
| `kind` | `user` or `workload` |
| `email`, `email_verified` | Users only |
| `name` | The user's name, or the name an admin gave the Workload |
| `org_id`, `org_name` | The workspace that owns the deployment |
| `federated_iss`, `federated_sub` | Workloads signed in through a Trust Rule only |
| `client_id`, `iat`, `nbf`, `exp`, `jti` | Standard token claims |

A valid Pass means "a member, or a Workload, of the workspace that owns this
deployment". Anything finer is the worker's own decision. Code that authorizes
by `sub` or `name` must also check `kind`, because a Workload's id and name
could look like a user's.

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
