# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Identity credentials accepted as HTTP bearers: sealed grants and ``resolve_token``.

vgi-rpc's ``Identity.v1`` mints grants "presented later by unattended
automation as an ordinary bearer". These tests close that loop through
vgi-python's own HTTP surface (``create_app``), not vgi-rpc in isolation:

- with ``VGI_RPC_GRANT_KEYS`` set, ``issue_grant`` mints a sealed grant and a
  ``vgi.v2`` call presenting it is authenticated as the grant's owner;
- a worker that overrides ``resolve_token`` has it consulted for bearers;
- the shared vectors (``vgi_rpc/conformance/grant_token_vectors.json``) are
  accepted and rejected through the full chain;
- VGI's proxy-proof gate still guards every request when grants are on.
"""

from __future__ import annotations

import base64
import json
import time
import types
from collections.abc import Iterator
from importlib.resources import files
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc.grants import GRANT_KEYS_ENV, GrantKeys, mint_grant_token
from vgi_rpc.http import http_connect
from vgi_rpc.http._testing import _SyncTestClient
from vgi_rpc.rpc import AuthContext
from vgi_rpc.rpc._token_identity import Identity

from tests.test_serve import _PeerAuthWorker
from vgi.arguments import Arguments
from vgi.auth import TokenIdentity
from vgi.client import Client, ClientError
from vgi.rpc_server import build_rpc_server, resolve_grant_keys
from vgi.serve import create_app

_KEY_B64 = base64.b64encode(bytes(range(32))).decode()
_OTHER_KEY_B64 = base64.b64encode(bytes(range(32, 64))).decode()
_SIGNING_KEY = b"grant-auth-test-signing-key-0001"
_IDENTITY = "vgi_rpc.Identity.v1"
_GRANT_ENV = (GRANT_KEYS_ENV, "VGI_RPC_GRANT_AUDIENCE", "VGI_RPC_GRANT_MAX_TTL_SECONDS")

_VECTORS = json.loads(files("vgi_rpc.conformance").joinpath("grant_token_vectors.json").read_text(encoding="utf-8"))


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """No ambient grant, auth or allowlist configuration leaks into a test."""
    for key in (*_GRANT_ENV, "VGI_INTROSPECT_PRINCIPALS", "VGI_BEARER_TOKENS", "VGI_JWT_ISSUER"):
        monkeypatch.delenv(key, raising=False)
    for key in ("VGI_PROXY_PROOF_MODE", "VGI_PROXY_PROOF_ORIGIN_ID", "VGI_PROXY_PROOF_SECRETS"):
        monkeypatch.delenv(key, raising=False)
    yield


def _header_authenticate(req: Any) -> AuthContext:
    """Deployment authenticator: ``X-Test-Principal`` with a fresh ``auth_time``.

    Raises ``ValueError`` for a bare bearer, as the chain contract requires, so
    grants and ``resolve_token`` get to answer it.
    """
    principal = req.get_header("X-Test-Principal")
    if principal:
        return AuthContext(domain="test", authenticated=True, principal=principal, claims={"auth_time": time.time()})
    if req.get_header("Authorization"):
        raise ValueError("not a test-principal request")
    return AuthContext.anonymous()


def _whoami(app: Any, *, bearer: str) -> str:
    """Call ``peer_auth_echo`` (a vgi.v2 scalar function) and return its principal."""
    client = _SyncTestClient(app, default_headers={"Authorization": f"Bearer {bearer}"})
    with Client.from_http("http://vgi.test", httpx_client=client) as vgi:
        out = list(
            vgi.scalar_function(
                function_name="peer_auth_echo",
                schema_path=["main"],
                arguments=Arguments(positional=(pa.scalar("value"),)),
                input=iter((pa.record_batch({"value": pa.array([1], type=pa.int64())}),)),
            )
        )
    return str(out[0].column("result")[0].as_py())


def _app(worker: type = _PeerAuthWorker, **kwargs: Any) -> Any:
    return create_app(worker, describe=False, signing_key=_SIGNING_KEY, **kwargs)


class TestGrantKeyConfiguration:
    """Opt-in, from the environment or ``--grant-key``; malformed keys stop startup."""

    def test_no_key_means_grants_off(self) -> None:
        """No key means grants off."""
        assert resolve_grant_keys() is None
        assert _IDENTITY not in build_rpc_server(_PeerAuthWorker(quiet=True), transport="http").bindings

    def test_env_keys_parse(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Env keys parse."""
        monkeypatch.setenv(GRANT_KEYS_ENV, f"{_KEY_B64},{_OTHER_KEY_B64}")
        monkeypatch.setenv("VGI_RPC_GRANT_AUDIENCE", "aud")
        keys = resolve_grant_keys()
        assert keys is not None
        assert keys.keys == (bytes(range(32)), bytes(range(32, 64)))
        assert keys.audience == "aud"

    def test_explicit_keys_win_over_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Explicit keys win over env."""
        monkeypatch.setenv(GRANT_KEYS_ENV, _OTHER_KEY_B64)
        keys = resolve_grant_keys([_KEY_B64])
        assert keys is not None
        assert keys.keys == (bytes(range(32)),)

    @pytest.mark.parametrize("bad", ["not-base64!", base64.b64encode(b"short").decode()])
    def test_malformed_key_refuses_to_start(self, monkeypatch: pytest.MonkeyPatch, bad: str) -> None:
        """Malformed key refuses to start."""
        monkeypatch.setenv(GRANT_KEYS_ENV, bad)
        with pytest.raises(SystemExit):
            _app()

    def test_keys_alone_host_issue_grant_without_an_allowlist(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Minting is not an oracle, so the introspection allowlist rule does not apply."""
        monkeypatch.setenv(GRANT_KEYS_ENV, _KEY_B64)
        server = build_rpc_server(_PeerAuthWorker(quiet=True), transport="http", grant_keys=resolve_grant_keys())
        assert server.bindings[_IDENTITY].methods.keys() == {"issue_grant"}

    def test_keys_are_http_only(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Keys are http only."""
        monkeypatch.setenv(GRANT_KEYS_ENV, _KEY_B64)
        server = build_rpc_server(_PeerAuthWorker(quiet=True), transport="unix", grant_keys=resolve_grant_keys())
        assert _IDENTITY not in server.bindings


class TestSealedGrantEndToEnd:
    """issue_grant with fresh auth, then a vgi.v2 call with ``Bearer <grant>``, as the owner."""

    def test_minted_grant_authenticates_as_its_owner(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Minted grant authenticates as its owner."""
        monkeypatch.setenv(GRANT_KEYS_ENV, _KEY_B64)
        app = _app(authenticate=_header_authenticate)

        minter = _SyncTestClient(app, default_headers={"X-Test-Principal": "alice@example.com"})
        with http_connect(Identity, client=minter) as identity:  # type: ignore[type-abstract]
            grant = identity.issue_grant(purpose="nightly-report", scopes=["read"], ttl_seconds=600)
        assert grant.token.startswith("vgig1.")

        assert _whoami(app, bearer=grant.token) == "alice@example.com"

    def test_tampered_grant_is_refused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Tampered grant is refused."""
        monkeypatch.setenv(GRANT_KEYS_ENV, _KEY_B64)
        keys = GrantKeys.parse([_KEY_B64])
        token, _ = mint_grant_token(keys, principal="alice", scopes=[], purpose="p", ttl_seconds=60)
        tampered = token[:-2] + ("A" if token[-2] != "A" else "B") + token[-1]
        with pytest.raises(ClientError):
            _whoami(_app(authenticate=_header_authenticate), bearer=tampered)

    def test_unknown_bearer_without_grants_configured_stays_unaccepted(self) -> None:
        """No keys, no hook: nothing changes -- the deployment authenticator decides."""
        app = _app(authenticate=_header_authenticate)
        with pytest.raises(ClientError):
            _whoami(app, bearer="vgig1.AAAA")


class _ResolvingWorker(_PeerAuthWorker):
    """Resolves one opaque credential; counts calls so routing is observable."""

    calls: list[str] = []

    @classmethod
    def resolve_token(cls, token: str) -> TokenIdentity | None:
        """Resolve token."""
        cls.calls.append(token)
        return TokenIdentity(principal="svc-reporting") if token == "opaque-api-key" else None


class TestResolveTokenBearer:
    """A worker's resolve_token is consulted for bearers it did not mint."""

    @pytest.fixture
    def app(self, monkeypatch: pytest.MonkeyPatch) -> Any:
        """App."""
        monkeypatch.setenv("VGI_INTROSPECT_PRINCIPALS", "proxy-a")
        monkeypatch.setenv(GRANT_KEYS_ENV, _KEY_B64)
        _ResolvingWorker.calls = []
        return _app(_ResolvingWorker, authenticate=_header_authenticate)

    def test_resolved_bearer_authenticates(self, app: Any) -> None:
        """Resolved bearer authenticates."""
        assert _whoami(app, bearer="opaque-api-key") == "svc-reporting"

    def test_unresolved_bearer_is_refused(self, app: Any) -> None:
        """Unresolved bearer is refused."""
        with pytest.raises(ClientError):
            _whoami(app, bearer="unknown-key")

    def test_a_bad_grant_never_reaches_the_resolver(self, app: Any) -> None:
        """A bad grant never reaches the resolver."""
        with pytest.raises(ClientError):
            _whoami(app, bearer="vgig1.AAAA")
        assert "vgig1.AAAA" not in _ResolvingWorker.calls

    def test_allowlist_rule_unchanged(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Allowlist rule unchanged."""
        monkeypatch.delenv("VGI_INTROSPECT_PRINCIPALS", raising=False)
        with pytest.raises(SystemExit):
            _app(_ResolvingWorker)


def _vector_env(monkeypatch: pytest.MonkeyPatch, verify_keys: list[str], audience: str, max_ttl: int) -> None:
    monkeypatch.setenv(GRANT_KEYS_ENV, ",".join(verify_keys))
    monkeypatch.setenv("VGI_RPC_GRANT_AUDIENCE", audience)
    monkeypatch.setenv("VGI_RPC_GRANT_MAX_TTL_SECONDS", str(max_ttl))


def _at(monkeypatch: pytest.MonkeyPatch, now: float) -> None:
    """Pin the grant verifier's clock without touching the process clock."""
    import vgi_rpc.grants

    monkeypatch.setattr(vgi_rpc.grants, "time", types.SimpleNamespace(time=lambda: now))


class TestSharedVectors:
    """The shared vectors, accepted and rejected through create_app's whole chain."""

    @pytest.mark.parametrize("case", _VECTORS["mint"], ids=lambda c: c["name"])
    def test_minted_vector_authenticates(self, monkeypatch: pytest.MonkeyPatch, case: dict[str, Any]) -> None:
        """Minted vector authenticates."""
        verify = [case["minting_key_b64"], *_VECTORS["defaults"]["verify_keys_b64"]]
        _vector_env(monkeypatch, list(dict.fromkeys(verify)), case["audience"], case["max_ttl_seconds"])
        _at(monkeypatch, case["now"] + 1)
        assert _whoami(_app(), bearer=case["token"]) == case["claims"]["principal"]

    @pytest.mark.parametrize("case", _VECTORS["accept"], ids=lambda c: c["name"])
    def test_accept_case(self, monkeypatch: pytest.MonkeyPatch, case: dict[str, Any]) -> None:
        """Accept case."""
        d = _VECTORS["defaults"]
        _vector_env(
            monkeypatch,
            case.get("verify_keys_b64", d["verify_keys_b64"]),
            case.get("audience", d["audience"]),
            case.get("max_ttl_seconds", d["max_ttl_seconds"]),
        )
        _at(monkeypatch, case.get("now", d["now"]))
        assert _whoami(_app(), bearer=case["token"]) != "anonymous"

    @pytest.mark.parametrize("case", _VECTORS["reject"], ids=lambda c: c["name"])
    def test_reject_case(self, monkeypatch: pytest.MonkeyPatch, case: dict[str, Any]) -> None:
        """Reject case."""
        d = _VECTORS["defaults"]
        _vector_env(
            monkeypatch,
            case.get("verify_keys_b64", d["verify_keys_b64"]),
            case.get("audience", d["audience"]),
            case.get("max_ttl_seconds", d["max_ttl_seconds"]),
        )
        _at(monkeypatch, case.get("now", d["now"]))
        with pytest.raises(ClientError):
            _whoami(_app(), bearer=case["token"])


class TestProxyProofGateStillGuards:
    """Grants go inside VGI's proxy-proof gate, never beside it."""

    @pytest.fixture
    def gated(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Gated."""
        monkeypatch.setenv("VGI_PROXY_PROOF_MODE", "require")
        monkeypatch.setenv("VGI_PROXY_PROOF_ORIGIN_ID", "worker-a")
        monkeypatch.setenv("VGI_PROXY_PROOF_SECRETS", "k:" + "11" * 32)
        monkeypatch.setenv(GRANT_KEYS_ENV, _KEY_B64)

    def test_gated_worker_with_grants_starts(self, gated: None) -> None:
        """vgi-rpc refuses to OR grants beside a proxy gate; create_app composes inside it."""
        from vgi.serve import _resolve_authenticate

        _app(authenticate=_resolve_authenticate())

    def test_valid_grant_without_proof_is_refused(self, gated: None) -> None:
        """Valid grant without proof is refused."""
        from vgi.serve import _resolve_authenticate

        keys = GrantKeys.parse([_KEY_B64])
        token, _ = mint_grant_token(keys, principal="alice", scopes=[], purpose="p", ttl_seconds=60)
        with pytest.raises(ClientError):
            _whoami(_app(authenticate=_resolve_authenticate()), bearer=token)
