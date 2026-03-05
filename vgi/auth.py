"""Convenience re-exports of authentication types from vgi-rpc.

Core types (always available):
    AuthContext, CallContext

HTTP auth factories (require ``vgi[http]``):
    bearer_authenticate, bearer_authenticate_static, chain_authenticate,
    OAuthResourceMetadata

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
        OAuthResourceMetadata,
        bearer_authenticate,
        bearer_authenticate_static,
        chain_authenticate,
        parse_client_id,
    )

    __all__ += [
        "OAuthResourceMetadata",
        "bearer_authenticate",
        "bearer_authenticate_static",
        "chain_authenticate",
        "parse_client_id",
    ]

# JWT auth — available when vgi[oauth] is installed (requires authlib).
with contextlib.suppress(ImportError):
    from vgi_rpc.http._oauth_jwt import jwt_authenticate  # noqa: F401

    __all__ += ["jwt_authenticate"]
