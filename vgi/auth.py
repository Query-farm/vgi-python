# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Convenience re-exports of authentication types from vgi-rpc.

Core types (always available):
    AuthContext, CallContext

HTTP auth factories (require ``vgi[http]``):
    bearer_authenticate, bearer_authenticate_static, chain_authenticate,
    OAuthResourceMetadata, AuthUnavailableError

Token introspection (requires ``vgi[http]``):
    TokenIdentity, TokenResolver

JWT auth (requires ``vgi[oauth]``):
    jwt_authenticate
"""

from __future__ import annotations

import contextlib

from vgi_rpc.rpc import AuthContext, CallContext

__all__ = [
    "AuthContext",
    "CallContext",
]

# HTTP auth helpers — available when vgi[http] is installed.
with contextlib.suppress(ImportError):
    from vgi_rpc.http import (  # noqa: F401
        AuthUnavailableError,
        OAuthResourceMetadata,
        bearer_authenticate,
        bearer_authenticate_static,
        chain_authenticate,
        parse_client_id,
        parse_client_secret,
        parse_device_code_client_id,
        parse_device_code_client_secret,
    )

    # Not re-exported by ``vgi_rpc.http`` itself, so the private module is the
    # only import path.  Re-exported here so a worker that implements
    # ``resolve_token`` never has to name a private module.
    from vgi_rpc.http.server._introspect import TokenIdentity, TokenResolver  # noqa: F401

    __all__ += [
        "AuthUnavailableError",
        "OAuthResourceMetadata",
        "TokenIdentity",
        "TokenResolver",
        "bearer_authenticate",
        "bearer_authenticate_static",
        "chain_authenticate",
        "parse_client_id",
        "parse_client_secret",
        "parse_device_code_client_id",
        "parse_device_code_client_secret",
    ]

# JWT auth — available when vgi[oauth] is installed (requires authlib).
with contextlib.suppress(ImportError):
    from vgi_rpc.http._oauth_jwt import jwt_authenticate  # noqa: F401

    __all__ += ["jwt_authenticate"]
