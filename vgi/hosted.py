# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Orchard hosted mode (``VGI_ORCHARD_HOSTED=1``).

A hosted worker (a *Field Worker*) serves VGI over HTTP to callers who hold a
*Pass*: a short-lived RFC 9068 access token (``typ: at+jwt``, ES256) issued by
the Orchard Front Desk for exactly one canonical Desk Address. The worker
verifies Passes locally against the Front Desk's JWKS and accepts nothing
else. Everything in this module fails closed:

- Startup is refused unless ``VGI_JWT_ISSUER``, ``VGI_JWT_AUDIENCE`` and
  ``VGI_JWT_JWKS_URI`` are all set to a single non-empty value.
- Startup is refused when any other credential is configured: static bearer
  tokens, sealed-grant keys, a ``resolve_token`` / ``mint_grant`` override,
  OAuth resource metadata, a proxy-proof gate, Iroh or other peer identity,
  or a custom ``authenticate`` callback.
- Every request except ``HEAD /health`` (and CORS preflights, which carry no
  credentials) must present a valid Pass, or gets a 401 with
  ``WWW-Authenticate: Bearer error="invalid_token"``.
- The JWKS is cached for at most 10 minutes; once that passes and the
  re-fetch fails, verification fails (HTTP 503) rather than using stale keys.

The design is in the extension repo's ``docs/location-delegation.md``
("The Pass" and "The Field Worker: SDK hosted mode").
"""

from __future__ import annotations

import os
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NoReturn

if TYPE_CHECKING:
    import falcon
    from vgi_rpc.rpc import AuthContext

__all__ = [
    "HOSTED_CHALLENGE",
    "HOSTED_ENV",
    "HostedConfig",
    "hosted_authenticate",
    "hosted_mode_enabled",
    "is_hosted_authenticate",
    "refuse_in_hosted_mode",
    "resolve_hosted_config",
]

#: Environment variable that turns hosted mode on. Baked into the hosted base image.
HOSTED_ENV = "VGI_ORCHARD_HOSTED"

#: The challenge on every hosted-mode 401. No ``resource_metadata``: a Field
#: Worker never sends a client off to discover an authorization server.
HOSTED_CHALLENGE = 'Bearer error="invalid_token"'

#: Default and only-by-override signing algorithm for Passes.
DEFAULT_ALGORITHMS: tuple[str, ...] = ("ES256",)
#: Required JOSE ``typ`` (RFC 9068).
REQUIRED_TYP = "at+jwt"
#: Clock skew allowed on ``exp`` / ``nbf``.
LEEWAY_SECONDS = 60
#: Hard upper bound on how long JWKS keys are trusted.
JWKS_MAX_AGE_SECONDS = 600.0
#: Minimum time between JWKS re-fetches triggered by an unknown ``kid``.
JWKS_REFRESH_MIN_INTERVAL_SECONDS = 60.0
#: Timeout for each JWKS request.
JWKS_FETCH_TIMEOUT_SECONDS = 10.0

_TRUE = frozenset({"1", "true", "yes", "on"})
_FALSE = frozenset({"", "0", "false", "no", "off"})

_ATTR = "_vgi_orchard_hosted"


def refuse_in_hosted_mode(reason: str) -> NoReturn:
    """Print a hosted-mode startup error and exit non-zero.

    Args:
        reason: What is wrong and how to fix it.

    Raises:
        SystemExit: Always, with status 1.

    """
    sys.stderr.write(f"Error: {HOSTED_ENV}=1 (Orchard hosted mode): {reason}\n")
    sys.exit(1)


def hosted_mode_enabled() -> bool:
    """Return whether ``VGI_ORCHARD_HOSTED`` turns hosted mode on.

    Returns:
        True for ``1``/``true``/``yes``/``on`` (any case), False when unset
        or ``0``/``false``/``no``/``off``.

    Raises:
        SystemExit: For any other value. An unrecognised value is treated as
            a configuration error rather than "off", so a typo cannot quietly
            start a worker without authentication.

    """
    raw = os.environ.get(HOSTED_ENV)
    if raw is None:
        return False
    value = raw.strip().lower()
    if value in _TRUE:
        return True
    if value in _FALSE:
        return False
    sys.stderr.write(f"Error: {HOSTED_ENV} must be 1 or 0 (or true/false), got {raw!r}\n")
    sys.exit(1)


@dataclass(frozen=True)
class HostedConfig:
    """Verified hosted-mode settings, read from the environment.

    Attributes:
        issuer: The Front Desk issuer (``iss``), matched exactly.
        audience: The one canonical Desk Address (``aud``), matched exactly.
        jwks_uri: Where the Front Desk publishes its signing keys.
        algorithms: Accepted JWS algorithms (``ES256`` unless overridden).

    """

    issuer: str
    audience: str
    jwks_uri: str
    algorithms: tuple[str, ...]


def _single_value(name: str) -> str:
    """Read one required, single-valued variable. Unset counts as empty."""
    value = (os.environ.get(name) or "").strip()
    if not value:
        refuse_in_hosted_mode(f"{name} must be set (unset and empty are both errors).")
    if "," in value:
        refuse_in_hosted_mode(
            f"{name} must be exactly one value; got a comma-separated list. "
            "A Pass is issued for one canonical Desk Address and one issuer."
        )
    return value


def resolve_hosted_config() -> HostedConfig:
    """Read and validate the hosted-mode environment.

    Returns:
        The configuration.

    Raises:
        SystemExit: When a required variable is missing, empty or
            multi-valued, or ``VGI_JWT_ALGORITHMS`` is unusable.

    """
    issuer = _single_value("VGI_JWT_ISSUER")
    audience = _single_value("VGI_JWT_AUDIENCE")
    jwks_uri = _single_value("VGI_JWT_JWKS_URI")
    raw_algorithms = os.environ.get("VGI_JWT_ALGORITHMS")
    if raw_algorithms is None or not raw_algorithms.strip():
        algorithms = DEFAULT_ALGORITHMS
    else:
        algorithms = tuple(a.strip() for a in raw_algorithms.split(",") if a.strip())
        lowered = [a.lower() for a in algorithms]
        if not algorithms or any(a == "none" or a.startswith("hs") for a in lowered):
            refuse_in_hosted_mode(
                f"VGI_JWT_ALGORITHMS={raw_algorithms!r} is not allowed; Passes are asymmetric "
                "(default ES256), never 'none' or HS*."
            )
    return HostedConfig(issuer=issuer, audience=audience, jwks_uri=jwks_uri, algorithms=algorithms)


def hosted_authenticate(config: HostedConfig) -> Callable[[falcon.Request], AuthContext]:
    """Build the Pass verifier for *config*.

    Args:
        config: Hosted-mode settings from :func:`resolve_hosted_config`.

    Returns:
        A vgi-rpc authenticate callback, marked so :func:`is_hosted_authenticate`
        recognises it.

    Raises:
        SystemExit: When the ``oauth`` extra (joserfc) is not installed.

    """
    try:
        from vgi_rpc.http._oauth_jwt import jwt_authenticate
    except ImportError:
        refuse_in_hosted_mode(
            "JWT verification requires the oauth extra. "
            "Install with: pip install vgi[oauth]  (or: uv sync --extra oauth)"
        )
    authenticate = jwt_authenticate(
        issuer=config.issuer,
        audience=config.audience,
        jwks_uri=config.jwks_uri,
        algorithms=config.algorithms,
        required_typ=REQUIRED_TYP,
        require_exp=True,
        single_audience=True,
        leeway=LEEWAY_SECONDS,
        jwks_max_age=JWKS_MAX_AGE_SECONDS,
        jwks_refresh_min_interval=JWKS_REFRESH_MIN_INTERVAL_SECONDS,
        jwks_fetch_timeout=JWKS_FETCH_TIMEOUT_SECONDS,
        domain="orchard",
    )
    setattr(authenticate, _ATTR, True)
    return authenticate


def is_hosted_authenticate(fn: object) -> bool:
    """Return whether *fn* was built by :func:`hosted_authenticate`.

    Args:
        fn: Any authenticate callback, or ``None``.

    Returns:
        True only for the hosted Pass verifier.

    """
    return getattr(fn, _ATTR, False) is True


def check_environment_conflicts() -> None:
    """Refuse to start when the environment configures another credential.

    Raises:
        SystemExit: On ``VGI_BEARER_TOKENS``, ``VGI_RPC_GRANT_KEYS``,
            ``VGI_OAUTH_RESOURCE`` or an active ``VGI_PROXY_PROOF_MODE``.

    """
    from vgi_rpc.grants import GRANT_KEYS_ENV

    for name, what in (
        ("VGI_BEARER_TOKENS", "static bearer tokens"),
        (GRANT_KEYS_ENV, "sealed-grant keys"),
        ("VGI_OAUTH_RESOURCE", "OAuth resource metadata (a Field Worker never advertises resource_metadata)"),
    ):
        if (os.environ.get(name) or "").strip():
            refuse_in_hosted_mode(f"{name} is set ({what}); the only credential a hosted worker accepts is a Pass.")
    proof_mode = (os.environ.get("VGI_PROXY_PROOF_MODE") or "off").strip().lower()
    if proof_mode != "off":
        refuse_in_hosted_mode("VGI_PROXY_PROOF_MODE must be off; hosted workers authenticate by Pass only.")


def check_create_app(
    worker_cls: Any,
    *,
    authenticate: object,
    grant_keys: object,
    explicit_grant_keys: Sequence[str] | None = None,
    oauth_resource_metadata: object,
    peer_identity_providers: Sequence[object],
    peer_authentication_policy: object,
    proxy_proof_required: bool | None,
    iroh_bridge_issuer: str | None,
) -> None:
    """Refuse any ``create_app`` configuration that admits a non-Pass caller.

    Args:
        worker_cls: The worker class (checked for identity hook overrides).
        authenticate: The authenticate callback passed in, if any.
        grant_keys: Resolved sealed-grant configuration, or ``None``.
        explicit_grant_keys: ``--grant-key`` values, when known.
        oauth_resource_metadata: OAuth resource metadata, if any.
        peer_identity_providers: Configured peer identity providers.
        peer_authentication_policy: Configured peer authentication policy.
        proxy_proof_required: Whether a proxy proof is required.
        iroh_bridge_issuer: The Iroh bridge issuer, if any.

    Raises:
        SystemExit: On the first conflict found.

    """
    check_environment_conflicts()
    if authenticate is not None and not is_hosted_authenticate(authenticate):
        refuse_in_hosted_mode("a custom authenticate callback is configured; hosted workers verify Passes only.")
    if grant_keys is not None or explicit_grant_keys:
        refuse_in_hosted_mode("sealed-grant keys (--grant-key / VGI_RPC_GRANT_KEYS) are configured.")
    resolver = getattr(worker_cls, "_introspect_resolver", None)
    if callable(resolver) and resolver() is not None:
        refuse_in_hosted_mode(f"{getattr(worker_cls, '__name__', worker_cls)} overrides resolve_token().")
    minter = getattr(worker_cls, "_grant_minter", None)
    if callable(minter) and minter() is not None:
        refuse_in_hosted_mode(f"{getattr(worker_cls, '__name__', worker_cls)} overrides mint_grant() (issue_grant).")
    if oauth_resource_metadata is not None:
        refuse_in_hosted_mode("OAuth resource metadata is configured; a Field Worker never advertises it.")
    if peer_identity_providers or peer_authentication_policy is not None or iroh_bridge_issuer is not None:
        refuse_in_hosted_mode("peer identity (Iroh, Tailscale, ...) is configured; hosted workers accept Passes only.")
    if proxy_proof_required:
        refuse_in_hosted_mode("a proxy proof is required; hosted workers authenticate by Pass only.")
