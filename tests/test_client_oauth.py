# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for vgi.client.Client's OAuth wiring (oauth=/oauth_refresh_token=/oauth_flow=).

The device-code/PKCE flow logic itself is tested exhaustively in
vgi-rpc-python's tests/test_oauth_client.py (mock IdP, no network). These
tests cover the integration seam: does `Client` build a `VgiOAuthAuth` with
the right configuration, does construction-time validation reject bad
combinations, and does a full round trip actually work when driven through
`Client`'s real request path (via the `httpx_client=` escape hatch, wired to
a mock IdP the same way vgi-rpc-python's own tests are).
"""

from __future__ import annotations

import httpx2
import pytest
from vgi_rpc.http import VgiOAuthAuth

from vgi.client.client import Client

RESOURCE_METADATA_URL = "https://api.example.com/.well-known/oauth-protected-resource"
DEVICE_AUTH_URL = "https://auth.example.com/device/code"
TOKEN_URL = "https://auth.example.com/token"


class _MockIdp:
    """Minimal mock IdP + protected resource for one full device-code round trip."""

    def __init__(self) -> None:
        self.device_auth_calls = 0
        self.granted_token = "mock-access-token"

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        """Route a request to the matching mock endpoint by exact URL."""
        url = str(request.url)
        if url == RESOURCE_METADATA_URL:
            return httpx2.Response(
                200,
                json={
                    "resource": "https://api.example.com",
                    "authorization_servers": ["https://auth.example.com"],
                    "device_code_client_id": "device-client",
                    "device_code_client_secret": "device-secret",
                },
            )
        if url == "https://auth.example.com/.well-known/openid-configuration":
            return httpx2.Response(
                200,
                json={
                    "token_endpoint": TOKEN_URL,
                    "device_authorization_endpoint": DEVICE_AUTH_URL,
                    "grant_types_supported": ["urn:ietf:params:oauth:grant-type:device_code"],
                },
            )
        if url == DEVICE_AUTH_URL:
            self.device_auth_calls += 1
            return httpx2.Response(
                200,
                json={
                    "device_code": "devcode-123",
                    "user_code": "ABCD-EFGH",
                    "verification_uri": "https://auth.example.com/activate",
                    "expires_in": 600,
                    "interval": 0,
                },
            )
        if url == TOKEN_URL:
            return httpx2.Response(200, json={"access_token": self.granted_token, "token_type": "Bearer"})
        if url.endswith("/vgi/catalogs"):
            token = request.headers.get("authorization", "")
            if token:
                return httpx2.Response(200, json={"authorization": token})
            return httpx2.Response(
                401,
                headers={"www-authenticate": f'Bearer resource_metadata="{RESOURCE_METADATA_URL}"'},
            )
        return httpx2.Response(404, json={"error": "not_found", "url": url})  # pragma: no cover


class TestConstructorValidation:
    """Tests for the mutually-exclusive oauth/bearer_token/httpx_client combinations."""

    def test_oauth_and_bearer_token_raises(self) -> None:
        """Passing both oauth=True and bearer_token= raises ValueError."""
        with pytest.raises(ValueError, match="mutually exclusive"):
            Client(transport="http", base_url="https://example.com", oauth=True, bearer_token="tok")

    def test_oauth_refresh_token_and_bearer_token_raises(self) -> None:
        """oauth_refresh_token= (which implies oauth=True) also conflicts with bearer_token=."""
        with pytest.raises(ValueError, match="mutually exclusive"):
            Client(
                transport="http",
                base_url="https://example.com",
                oauth_refresh_token="seed",
                bearer_token="tok",
            )

    def test_oauth_and_httpx_client_raises(self) -> None:
        """Passing both oauth=True and httpx_client= raises ValueError."""
        with pytest.raises(ValueError, match="httpx_client"):
            Client(
                transport="http",
                base_url="https://example.com",
                oauth=True,
                httpx_client=httpx2.Client(),
            )

    def test_invalid_oauth_flow_raises(self) -> None:
        """An unrecognized oauth_flow value raises ValueError, not a silent no-op."""
        with pytest.raises(ValueError, match="oauth_flow"):
            Client(transport="http", base_url="https://example.com", oauth=True, oauth_flow="carrier-pigeon")  # type: ignore[arg-type]

    def test_invalid_oauth_prompt_raises(self) -> None:
        """An unrecognized oauth_prompt value raises ValueError, not a silent no-op."""
        with pytest.raises(ValueError, match="oauth_prompt"):
            Client(transport="http", base_url="https://example.com", oauth=True, oauth_prompt="shout")  # type: ignore[arg-type]

    def test_bearer_token_alone_still_works(self) -> None:
        """The existing bearer_token= path is unaffected by the new oauth params."""
        client = Client(transport="http", base_url="https://example.com", bearer_token="tok")
        assert client._bearer_token == "tok"
        assert not client._use_oauth


class TestOAuthWiring:
    """Tests that Client(oauth=...) builds a correctly-configured VgiOAuthAuth."""

    def test_oauth_true_builds_vgi_oauth_auth(self) -> None:
        """oauth=True lazily builds a VgiOAuthAuth with the client's own oauth_* settings."""
        client = Client(
            transport="http",
            base_url="https://example.com",
            oauth=True,
            oauth_flow="device_code",
            oauth_timeout_seconds=42.0,
            oauth_prompt="consent",
        )
        assert client.oauth_identity() is None  # nothing has authenticated yet
        client._get_or_create_httpx_client()
        assert isinstance(client._oauth_auth, VgiOAuthAuth)
        assert client._oauth_auth._flow == "device_code"  # noqa: SLF001 -- white-box wiring check
        assert client._oauth_auth._timeout_seconds == 42.0  # noqa: SLF001
        assert client._oauth_auth._prompt == "consent"  # noqa: SLF001
        client._oauth_auth.close()

    def test_oauth_refresh_token_seeds_the_auth_object(self) -> None:
        """oauth_refresh_token= implies oauth=True and seeds the refresh token."""
        client = Client(
            transport="http",
            base_url="https://example.com",
            oauth_refresh_token="pre-seeded-token",
        )
        assert client._use_oauth
        client._get_or_create_httpx_client()
        assert client._oauth_auth is not None
        assert client._oauth_auth._token.refresh_token == "pre-seeded-token"  # noqa: SLF001
        client._oauth_auth.close()


class TestOAuthEndToEnd:
    """A full device-code login round trip through the exact client Client would use.

    Uses the httpx_client= escape hatch to wire a VgiOAuthAuth against a mock
    IdP (httpx2.MockTransport) -- the same technique vgi-rpc-python's own
    tests use. The device-code/PKCE flow mechanics are covered exhaustively
    there (mock IdP, no network); this test's job is narrower: prove
    ``Client`` actually issues its RPCs through the httpx2.Client it was
    handed (so a real attach would carry the OAuth-obtained token), and that
    a 401-triggered login on that client completes end to end.
    """

    def test_client_issues_requests_through_the_oauth_authed_httpx_client(self) -> None:
        """A 401 on the client Client._get_or_create_httpx_client() returns triggers a real login."""
        idp = _MockIdp()
        auth = VgiOAuthAuth(
            base_url="https://example.com",
            flow="device_code",
            timeout_seconds=5.0,
            transport=httpx2.MockTransport(idp.handler),
        )
        httpx_client = httpx2.Client(
            transport=httpx2.MockTransport(idp.handler), auth=auth, base_url="https://example.com"
        )
        # Not started (no worker spawned) -- this test only exercises the HTTP/OAuth
        # plumbing, not a full RPC call, so client.stop() (which requires start())
        # doesn't apply; the httpx_client/auth are closed directly instead.
        client = Client(transport="http", base_url="https://example.com", httpx_client=httpx_client)
        try:
            # This is the exact client object Client's own RPC machinery sends
            # every request through -- not a parallel one built just for the test.
            assert client._get_or_create_httpx_client() is httpx_client

            resp = httpx_client.get("/vgi/catalogs")
            assert resp.status_code == 200
            assert resp.json()["authorization"] == f"Bearer {idp.granted_token}"
            assert idp.device_auth_calls == 1

            # A second request reuses the cached token -- no second login.
            httpx_client.get("/vgi/catalogs")
            assert idp.device_auth_calls == 1
        finally:
            httpx_client.close()
            auth.close()
