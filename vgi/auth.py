# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Convenience re-exports of authentication types from vgi-rpc.

Core types (always available):
    AuthContext, CallContext

HTTP auth factories (require ``vgi[http]``):
    bearer_authenticate, bearer_authenticate_static, chain_authenticate,
    OAuthResourceMetadata, AuthUnavailableError

Token introspection (always available):
    TokenIdentity, TokenResolver

JWT auth (requires ``vgi[oauth]``):
    jwt_authenticate
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from vgi_rpc.rpc import AuthContext, CallContext

# ``vgi_rpc.Identity.v1`` lives at the RPC layer as of vgi-rpc 0.46.0 -- it was
# an HTTP JSON route (``POST {prefix}/__introspect_token__``) through 0.45.x,
# which meant it existed on exactly one transport.  So this is no longer gated
# on the ``http`` extra.  Still not re-exported by ``vgi_rpc.rpc`` itself, so
# the private module remains the only import path; re-exported here so a worker
# that implements ``resolve_token`` never has to name one.
from vgi_rpc.rpc._token_identity import TokenIdentity

#: What :meth:`vgi.worker.Worker.resolve_token` is.  Upstream dropped its own
#: alias when identity moved to the RPC layer; it is spelled out here so the
#: name ``vgi.auth.TokenResolver`` keeps working.
TokenResolver = Callable[[str], "TokenIdentity | None"]

__all__ = [
    "AuthContext",
    "CallContext",
    "TokenIdentity",
    "TokenResolver",
]

# HTTP auth helpers (``vgi[http]``) and JWT auth (``vgi[oauth]``), resolved on
# first access rather than at import. ``vgi/__init__`` imports this module, and
# ``vgi_rpc.http`` brings in the HTTP server and client stack (falcon, httpx2,
# cryptography, joserfc): importing it here put about 180 ms on every
# ``import vgi``, and so on the start of every subprocess worker, which never
# uses it. ``from vgi.auth import bearer_authenticate`` works as before; when
# the extra is missing it raises ImportError, as it did.
_LAZY_EXPORTS = {
    "AuthUnavailableError": "vgi_rpc.http",
    "OAuthResourceMetadata": "vgi_rpc.http",
    "bearer_authenticate": "vgi_rpc.http",
    "bearer_authenticate_static": "vgi_rpc.http",
    "chain_authenticate": "vgi_rpc.http",
    "parse_client_id": "vgi_rpc.http",
    "parse_client_secret": "vgi_rpc.http",
    "parse_device_code_client_id": "vgi_rpc.http",
    "parse_device_code_client_secret": "vgi_rpc.http",
    "jwt_authenticate": "vgi_rpc.http._oauth_jwt",
}

if TYPE_CHECKING:
    from vgi_rpc.http import (
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
    from vgi_rpc.http._oauth_jwt import jwt_authenticate


def __getattr__(name: str) -> Any:
    """Import an HTTP or JWT auth helper on first access (PEP 562).

    Args:
        name: The attribute being looked up.

    Returns:
        The helper, which is then cached in this module's namespace.

    Raises:
        AttributeError: If ``name`` is not a helper this module re-exports, or
            its extra is not installed.
    """
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    try:
        value = getattr(importlib.import_module(module_name), name)
    except ImportError as exc:
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}: it needs {module_name}, which failed to import ({exc})"
        ) from exc
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """List this module's names, the lazily imported helpers included.

    Returns:
        The module's attribute names.
    """
    return sorted({*globals(), *_LAZY_EXPORTS})
