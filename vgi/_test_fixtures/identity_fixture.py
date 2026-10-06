# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Opt the HTTP fixture into ``vgi_rpc.Identity.v1`` with the conformance policy.

Enabled by ``vgi-fixture-http --identity`` (or ``VGI_FIXTURE_IDENTITY=1``) so
the cross-SDK hosted-protocols group can run its identity cases with
``vgi-rpc-test-hosted --url ... --identity``. Off by default, so the DuckDB
integration runs see the same worker as before.

The policy is pinned by vgi-rpc's ``IDENTITY_CONFORMANCE_FIXTURE.md``. The
hooks and constants come straight from the reference
(``vgi_rpc.conformance.identity_fixture``) rather than being transcribed.
Copying them would let this fixture drift from the contract it claims to meet.

.. warning::

   The header authenticator below is trivially spoofable by anyone who can
   reach the port. It is a test fixture and must never be deployed.
"""

from __future__ import annotations

from typing import Any

from vgi_rpc.conformance.identity_fixture import (
    AUTH_TIME_HEADER,
    INTROSPECTOR_PRINCIPAL,
    PRINCIPAL_HEADER,
    conformance_mint_grant,
    conformance_resolve_token,
)
from vgi_rpc.rpc import AuthContext

from vgi._test_fixtures.worker import ExampleWorker
from vgi.auth import IssuedGrant, TokenIdentity

__all__ = ["INTROSPECT_PRINCIPALS", "IdentityExampleWorker", "conformance_authenticate"]

#: The introspector allowlist the conformance policy pins: exactly one principal.
INTROSPECT_PRINCIPALS = [INTROSPECTOR_PRINCIPAL]


class IdentityExampleWorker(ExampleWorker):
    """``ExampleWorker`` whose identity hooks follow the conformance policy.

    It replaces ``ExampleWorker`` in the fixture's ``MetaWorker``, so it is the
    only child that implements the hooks.
    """

    @classmethod
    def resolve_token(cls, token: str) -> TokenIdentity | None:
        """Resolve per the conformance policy (§3.3)."""
        return conformance_resolve_token(token)

    @classmethod
    def mint_grant(cls, principal: str, purpose: str, scopes: list[str], ttl_seconds: int) -> IssuedGrant:
        """Mint per the conformance policy (§3.4)."""
        return conformance_mint_grant(principal, purpose, scopes, ttl_seconds)


def conformance_authenticate(req: Any) -> AuthContext:
    """Derive the caller from ``X-Conformance-Principal`` / ``X-Conformance-Auth-Time``.

    An absent principal header means *unauthenticated*. The auth-time header is
    placed in the claims verbatim and unparsed, because the guard does the
    parsing and the fixture only carries the value.
    """
    principal = req.get_header(PRINCIPAL_HEADER)
    if not principal:
        return AuthContext(domain=None, authenticated=False, principal=None)
    claims: dict[str, object] = {}
    auth_time = req.get_header(AUTH_TIME_HEADER)
    if auth_time is not None:
        claims["auth_time"] = auth_time
    return AuthContext(domain="conformance", authenticated=True, principal=principal, claims=claims)
