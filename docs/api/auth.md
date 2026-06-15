# Auth & Secrets

HTTP workers authenticate requests via a pluggable callback that populates `CallContext.auth` with
an `AuthContext`. Bearer-token and JWT/JWKS authenticators ship in `vgi.auth`. Secrets (credentials)
flow through the secret protocol so workers never see raw values unless explicitly resolved. See the
[Authentication](../authentication.md) guide.

`AuthContext` and `CallContext` are re-exported from `vgi-rpc`. The bearer/chain authenticators
require `pip install vgi-python[http]`; JWT authentication additionally requires `[oauth]`.

## Auth

::: vgi.auth

## Secret protocol

::: vgi.secret_protocol

## Secret service

::: vgi.secret_service
