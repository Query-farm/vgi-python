# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Orchard hosted mode (``VGI_ORCHARD_HOSTED=1``), end to end.

Runs ``vgi-serve --http`` as a real process in hosted mode against a local
JWKS server holding a freshly generated ES256 key, then checks the contract in
``docs/authentication.md`` ("Orchard hosted mode"):

- a valid Pass reaches function code, with claims in their JSON types;
- no Pass, a tampered claim, the wrong ``aud``, an expired Pass, the wrong
  ``typ`` and RS256 / HS256 tokens all get 401 with
  ``WWW-Authenticate: Bearer error="invalid_token"``, the capability headers
  and no ``resource_metadata``; ``HEAD /health`` alone is anonymous;
- the ``Authorization`` header never reaches the logs, even with ``--debug``;
- every misconfiguration refuses to start, with a non-zero exit.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

joserfc = pytest.importorskip("joserfc")
from joserfc import jwt  # noqa: E402
from joserfc.jwk import ECKey, OctKey, RSAKey  # noqa: E402

from vgi.arguments import Arguments  # noqa: E402
from vgi.client import Client  # noqa: E402

ISSUER = "https://auth.orchard.test"
AUDIENCE = "https://desk.orchard.test/d/dep_123"
FIXTURE = "vgi._test_fixtures.orchard_hosted"
_REPO = Path(__file__).resolve().parent.parent

EC = ECKey.generate_key("P-256", parameters={"kid": "desk-es256-1"}, private=True)
RSA = RSAKey.generate_key(2048, parameters={"kid": "desk-rs256-1"}, private=True)

_AUTH_ENV = (
    "VGI_BEARER_TOKENS",
    "VGI_JWT_ISSUER",
    "VGI_JWT_AUDIENCE",
    "VGI_JWT_JWKS_URI",
    "VGI_JWT_ALGORITHMS",
    "VGI_RPC_GRANT_KEYS",
    "VGI_OAUTH_RESOURCE",
    "VGI_OAUTH_AUTH_SERVERS",
    "VGI_PROXY_PROOF_MODE",
    "VGI_INTROSPECT_PRINCIPALS",
    "VGI_ORCHARD_HOSTED",
)


# ---------------------------------------------------------------------------
# Passes
# ---------------------------------------------------------------------------


def _pass(
    key: ECKey | RSAKey | OctKey = EC,
    *,
    alg: str = "ES256",
    typ: str = "at+jwt",
    **overrides: Any,
) -> str:
    now = int(time.time())
    claims: dict[str, Any] = {
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": "user_01M7KNJYC18T",
        "kind": "user",
        "email": "ada@example.com",
        "email_verified": True,
        "name": "Ada Lovelace",
        "org_id": "org_01M7",
        "org_name": "Acme Analytics",
        "client_id": "vgi-duckdb",
        "iat": now,
        "nbf": now,
        "exp": now + 900,
        "jti": f"jti-{time.monotonic_ns()}",
    }
    claims.update(overrides)
    claims = {name: value for name, value in claims.items() if value is not None}
    header = {"alg": alg, "typ": typ, "kid": key.kid}
    return jwt.encode(header, claims, key, algorithms=[alg], default_type=None)


def _tamper(token: str, **changes: Any) -> str:
    header, payload, signature = token.split(".")
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    claims.update(changes)
    forged = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()
    return f"{header}.{forged}.{signature}"


# ---------------------------------------------------------------------------
# A local JWKS endpoint
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def jwks_uri() -> Iterator[str]:
    """Serve the EC and RSA public keys as a JWKS on loopback."""
    body = json.dumps({"keys": [EC.as_dict(private=False), RSA.as_dict(private=False)]}).encode()

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/jwks"
    finally:
        server.shutdown()
        server.server_close()


def _env(jwks: str | None, **overrides: str | None) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k not in _AUTH_ENV}
    env.update(
        {
            "VGI_ORCHARD_HOSTED": "1",
            "VGI_JWT_ISSUER": ISSUER,
            "VGI_JWT_AUDIENCE": AUDIENCE,
            "VGI_JWT_JWKS_URI": jwks or "",
            "VGI_SIGNING_KEY": "orchard-hosted-test-signing-key-0001",
            "PYTHONUNBUFFERED": "1",
        }
    )
    for key, value in overrides.items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value
    return env


def _serve_argv(worker: str = "OrchardHostedWorker", *extra: str) -> list[str]:
    return [
        sys.executable, "-m", "vgi.serve", f"{FIXTURE}:{worker}",
        "--http", "--host", "127.0.0.1", "--port", "0", *extra,
    ]  # fmt: skip


@dataclass
class _Worker:
    url: str
    proc: subprocess.Popen[str]
    log: Path


@pytest.fixture(scope="module")
def worker(jwks_uri: str, tmp_path_factory: pytest.TempPathFactory) -> Iterator[_Worker]:
    """Run a hosted-mode HTTP worker with --debug, logging to a file."""
    log = tmp_path_factory.mktemp("orchard") / "worker.log"
    with log.open("w") as log_file:
        proc = subprocess.Popen(
            _serve_argv("OrchardHostedWorker", "--debug"),
            cwd=_REPO,
            env=_env(jwks_uri),
            stdout=subprocess.PIPE,
            stderr=log_file,
            text=True,
        )
        assert proc.stdout is not None
        deadline = time.monotonic() + 60
        port = None
        captured: list[str] = []
        while time.monotonic() < deadline:
            line = proc.stdout.readline()
            if not line:
                break
            captured.append(line)
            if line.startswith("PORT:"):
                port = int(line.strip().split(":", 1)[1])
                break
        if port is None:
            proc.kill()
            pytest.fail(f"hosted worker did not start: {''.join(captured)}\n{log.read_text()}")
        # Keep draining stdout so the worker never blocks on a full pipe.
        drained: list[str] = captured

        def _drain() -> None:
            assert proc.stdout is not None
            for out_line in proc.stdout:
                drained.append(out_line)

        threading.Thread(target=_drain, daemon=True).start()
        try:
            yield _Worker(url=f"http://127.0.0.1:{port}", proc=proc, log=log)
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
            log.with_suffix(".stdout").write_text("".join(drained))


def _request(
    worker: _Worker, method: str, path: str, token: str | None = None, headers: dict[str, str] | None = None
) -> tuple[int, dict[str, str], bytes]:
    req = urllib.request.Request(worker.url + path, method=method, data=b"" if method == "POST" else None)
    for name, value in (headers or {}).items():
        req.add_header(name, value)
    if token is not None:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read()
    except urllib.error.HTTPError as err:
        return err.code, {k.lower(): v for k, v in err.headers.items()}, err.read()


def _claims(worker: _Worker, token: str) -> dict[str, Any]:
    with Client.from_http(worker.url, bearer_token=token) as client:
        out = list(
            client.scalar_function(
                function_name="pass_claims",
                schema_path=["main"],
                arguments=Arguments(positional=(pa.scalar("x"),)),
                input=iter((pa.record_batch({"x": pa.array([1], type=pa.int64())}),)),
            )
        )
    payload: dict[str, Any] = json.loads(out[0].column("result")[0].as_py())
    return payload


# Tokens used anywhere in the module, for the log-leak check at the end.
_USED_TOKENS: list[str] = []


def _use(token: str) -> str:
    _USED_TOKENS.append(token)
    return token


# ---------------------------------------------------------------------------
# A running hosted worker
# ---------------------------------------------------------------------------


class TestHostedWorker:
    """Requests against a real hosted-mode worker."""

    def test_valid_pass_reaches_function_with_json_types(self, worker: _Worker) -> None:
        """A valid Pass works and function code sees the claims with their JSON types."""
        token = _use(_pass())
        result = _claims(worker, token)
        assert result["principal"] == "user_01M7KNJYC18T"
        assert result["domain"] == "orchard"
        claims = result["claims"]
        assert claims["email_verified"] is True
        assert isinstance(claims["exp"], int)
        assert claims["kind"] == "user"
        assert claims["email"] == "ada@example.com"
        assert claims["name"] == "Ada Lovelace"
        assert claims["org_id"] == "org_01M7"
        assert claims["org_name"] == "Acme Analytics"

    def test_head_health_is_anonymous(self, worker: _Worker) -> None:
        """HEAD /health is the only request that needs no Pass."""
        status, headers, _ = _request(worker, "HEAD", "/health")
        assert status == 200
        assert "vgi-supported-encodings" in headers

    @pytest.mark.parametrize(
        ("method", "path"),
        [("GET", "/health"), ("OPTIONS", "/health"), ("GET", "/"), ("POST", "/vgi.v3/catalog_attach")],
    )
    def test_no_pass_401(self, worker: _Worker, method: str, path: str) -> None:
        """No Authorization header is never anonymous."""
        status, headers, _ = _request(worker, method, path, headers={"Origin": "https://shell.example.com"})
        self._assert_401(status, headers)

    @pytest.mark.parametrize(
        "case",
        [
            "garbage",
            "tampered",
            "wrong_aud",
            "multi_aud",
            "expired",
            "wrong_typ",
            "rs256",
            "hs256",
            "wrong_iss",
            "no_exp",
        ],
    )
    def test_bad_pass_401(self, worker: _Worker, case: str) -> None:
        """Every invalid Pass gets the same 401."""
        now = int(time.time())
        makers: dict[str, Callable[[], str]] = {
            "garbage": lambda: "not-a-jwt",
            "tampered": lambda: _tamper(_pass(), org_id="org_EVIL"),
            "wrong_aud": lambda: _pass(aud="https://desk.orchard.test/d/dep_other"),
            "multi_aud": lambda: _pass(aud=[AUDIENCE, "https://desk.orchard.test/d/dep_other"]),
            "expired": lambda: _pass(iat=now - 1200, nbf=now - 1200, exp=now - 120),
            "wrong_typ": lambda: _pass(typ="JWT"),
            "rs256": lambda: _pass(RSA, alg="RS256"),
            "hs256": lambda: _pass(OctKey.import_key(b"k" * 32, {"kid": "desk-es256-1"}), alg="HS256"),
            "wrong_iss": lambda: _pass(iss="https://evil.test"),
            "no_exp": lambda: _pass(exp=None),
        }
        token = makers[case]()
        status, headers, _ = _request(
            worker, "POST", "/vgi.v3/catalog_attach", _use(token), headers={"Origin": "https://shell.example.com"}
        )
        self._assert_401(status, headers)

    def test_preflight_open_cors(self, worker: _Worker) -> None:
        """CORS is open; a preflight needs no Pass."""
        status, headers, _ = _request(
            worker,
            "OPTIONS",
            "/vgi.v3/catalog_attach",
            headers={
                "Origin": "https://shell.example.com",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "authorization",
            },
        )
        assert status == 200
        assert headers.get("access-control-allow-origin") == "*"

    def test_zz_authorization_never_logged(self, worker: _Worker) -> None:
        """No token used in this module appears in the worker's --debug output."""
        # Self-contained (xdist gives each process its own worker): one
        # accepted and several rejected Passes, all through this worker.
        _claims(worker, _use(_pass()))
        for bad in (_tamper(_pass(), sub="mallory"), _pass(aud="https://other.test"), _pass(typ="JWT")):
            status, _headers, _body = _request(worker, "POST", "/vgi.v3/catalog_attach", _use(bad))
            assert status == 401
        time.sleep(0.5)
        text = worker.log.read_text()
        assert "Auth failure" in text, "expected the worker's debug log to be captured"
        assert "vgi_rpc.access" in text or '"method"' in text, "expected access-log records in the debug log"
        for token in _USED_TOKENS:
            assert token not in text
            # A JWS signature is unique to its token; catch partial logging too.
            if token.count(".") == 2:
                assert token.split(".")[2] not in text
        assert "Bearer " not in text

    @staticmethod
    def _assert_401(status: int, headers: dict[str, str]) -> None:
        assert status == 401
        assert headers.get("www-authenticate") == 'Bearer error="invalid_token"'
        assert "resource_metadata" not in headers.get("www-authenticate", "")
        assert "vgi-supported-encodings" in headers
        assert "vgi-accept-max-response-bytes-support" in headers or "vgi-externalization-enabled" in headers


# ---------------------------------------------------------------------------
# Startup refusals
# ---------------------------------------------------------------------------


def _refused(env: dict[str, str], *extra: str, worker: str = "OrchardHostedWorker") -> str:
    proc = subprocess.run(
        _serve_argv(worker, *extra),
        cwd=_REPO,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode != 0, f"worker started when it should have refused:\n{proc.stdout}\n{proc.stderr}"
    assert "PORT:" not in proc.stdout
    return proc.stderr


_JWKS = "http://127.0.0.1:9/jwks"  # never contacted: startup refuses first


class TestStartupRefusals:
    """Each misconfiguration stops the worker with a clear error."""

    @pytest.mark.parametrize("name", ["VGI_JWT_ISSUER", "VGI_JWT_AUDIENCE", "VGI_JWT_JWKS_URI"])
    def test_unset(self, name: str) -> None:
        """An unset required variable refuses."""
        assert name in _refused(_env(_JWKS, **{name: None}))

    @pytest.mark.parametrize("name", ["VGI_JWT_ISSUER", "VGI_JWT_AUDIENCE", "VGI_JWT_JWKS_URI"])
    def test_empty(self, name: str) -> None:
        """An empty required variable refuses, the same as unset."""
        assert name in _refused(_env(_JWKS, **{name: "  "}))

    def test_comma_audience(self) -> None:
        """The audience is one canonical Desk Address, never a list."""
        stderr = _refused(_env(_JWKS, VGI_JWT_AUDIENCE=f"{AUDIENCE},https://desk.orchard.test/d/other"))
        assert "VGI_JWT_AUDIENCE" in stderr
        assert "exactly one" in stderr

    def test_bearer_tokens(self) -> None:
        """Static bearer tokens refuse."""
        assert "VGI_BEARER_TOKENS" in _refused(_env(_JWKS, VGI_BEARER_TOKENS="tok=alice"))

    def test_grant_keys_env(self) -> None:
        """Sealed-grant keys from the environment refuse."""
        key = base64.b64encode(bytes(range(32))).decode()
        assert "VGI_RPC_GRANT_KEYS" in _refused(_env(_JWKS, VGI_RPC_GRANT_KEYS=key))

    def test_grant_keys_flag(self) -> None:
        """Sealed-grant keys from --grant-key refuse."""
        key = base64.b64encode(bytes(range(32))).decode()
        assert "grant" in _refused(_env(_JWKS), "--grant-key", key).lower()

    def test_resolve_token_override(self) -> None:
        """A worker that overrides resolve_token refuses."""
        assert "resolve_token" in _refused(_env(_JWKS, VGI_INTROSPECT_PRINCIPALS="ops"), worker="ResolveTokenWorker")

    def test_oauth_resource(self) -> None:
        """OAuth resource metadata refuses (no resource_metadata in hosted mode)."""
        stderr = _refused(_env(_JWKS, VGI_OAUTH_RESOURCE="https://x.test", VGI_OAUTH_AUTH_SERVERS=ISSUER))
        assert "VGI_OAUTH_RESOURCE" in stderr

    def test_hs_algorithm_override(self) -> None:
        """VGI_JWT_ALGORITHMS may not name HS* or none."""
        assert "VGI_JWT_ALGORITHMS" in _refused(_env(_JWKS, VGI_JWT_ALGORITHMS="ES256,HS256"))

    def test_unrecognised_flag_value(self) -> None:
        """A typo in VGI_ORCHARD_HOSTED refuses instead of meaning 'off'."""
        assert "VGI_ORCHARD_HOSTED" in _refused(_env(_JWKS, VGI_ORCHARD_HOSTED="yes please"))

    @pytest.mark.parametrize("flag", [["--tcp", "127.0.0.1:0"], ["--unix", "/tmp/vgi-orchard-hosted-refuse.sock"]])
    def test_unauthenticated_transports(self, flag: list[str]) -> None:
        """Worker.main refuses --tcp and --unix: they carry no Pass."""
        proc = subprocess.run(
            [sys.executable, "-m", FIXTURE, *flag],
            cwd=_REPO,
            env=_env(_JWKS),
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert proc.returncode != 0
        assert "only --http" in proc.stderr

    def test_iroh_raw_upstream(self) -> None:
        """vgi-serve refuses --iroh-raw-upstream in hosted mode."""
        argv = [sys.executable, "-m", "vgi.serve", f"{FIXTURE}:OrchardHostedWorker", "--iroh-raw-upstream", "0"]
        proc = subprocess.run(argv, cwd=_REPO, env=_env(_JWKS), capture_output=True, text=True, timeout=60)
        assert proc.returncode != 0
        assert "only --http" in proc.stderr

    def test_worker_main_http_serves(self, jwks_uri: str) -> None:
        """Worker.main --http also runs in hosted mode (and HEAD /health is anonymous there too)."""
        proc = subprocess.Popen(
            [sys.executable, "-m", FIXTURE, "--http", "--host", "127.0.0.1", "--port", "0"],
            cwd=_REPO,
            env=_env(jwks_uri),
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        try:
            assert proc.stdout is not None
            port = None
            for line in proc.stdout:
                if line.startswith("PORT:"):
                    port = int(line.strip().split(":", 1)[1])
                    break
            assert port is not None
            url = f"http://127.0.0.1:{port}"
            # PORT: is printed just before waitress binds; retry briefly.
            deadline = time.monotonic() + 20
            while True:
                try:
                    head = urllib.request.Request(url + "/health", method="HEAD")
                    with urllib.request.urlopen(head, timeout=30) as r:
                        assert r.status == 200
                    break
                except urllib.error.URLError as exc:
                    if isinstance(exc, urllib.error.HTTPError) or time.monotonic() > deadline:
                        raise
                    time.sleep(0.1)
            with pytest.raises(urllib.error.HTTPError) as info:
                urllib.request.urlopen(urllib.request.Request(url + "/health"), timeout=30)
            assert info.value.code == 401
        finally:
            proc.terminate()
            proc.wait(timeout=15)


class TestResolveAuthenticate:
    """In-process checks of the environment wiring."""

    @pytest.fixture(autouse=True)
    def _clean(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for key in _AUTH_ENV:
            monkeypatch.delenv(key, raising=False)

    def test_hosted_returns_pass_verifier(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """_resolve_authenticate builds the hosted verifier in hosted mode."""
        from vgi.hosted import is_hosted_authenticate
        from vgi.serve import _resolve_authenticate

        for key, value in _env(_JWKS).items():
            if key.startswith("VGI_"):
                monkeypatch.setenv(key, value)
        assert is_hosted_authenticate(_resolve_authenticate())

    def test_create_app_refuses_custom_authenticate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """create_app refuses a caller-supplied authenticator in hosted mode."""
        from vgi_rpc.rpc import AuthContext

        from vgi._test_fixtures.orchard_hosted import OrchardHostedWorker
        from vgi.serve import create_app

        for key, value in _env(_JWKS).items():
            if key.startswith("VGI_"):
                monkeypatch.setenv(key, value)
        with pytest.raises(SystemExit):
            create_app(OrchardHostedWorker, signing_key=b"k" * 32, authenticate=lambda req: AuthContext.anonymous())

    def test_jwt_algorithms_outside_hosted_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """VGI_JWT_ALGORITHMS also applies to ordinary JWT auth; HS* refuses."""
        from vgi.serve import _resolve_jwt_authenticate

        monkeypatch.setenv("VGI_JWT_ISSUER", ISSUER)
        monkeypatch.setenv("VGI_JWT_AUDIENCE", AUDIENCE)
        monkeypatch.setenv("VGI_JWT_JWKS_URI", _JWKS)
        monkeypatch.setenv("VGI_JWT_ALGORITHMS", "ES256")
        assert _resolve_jwt_authenticate() is not None
        monkeypatch.setenv("VGI_JWT_ALGORITHMS", "HS256")
        with pytest.raises(SystemExit):
            _resolve_jwt_authenticate()

    @pytest.mark.parametrize("value", ["0", "false", "", "off"])
    def test_off_values(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        """Hosted mode is off for the usual false values."""
        from vgi.hosted import hosted_mode_enabled

        monkeypatch.setenv("VGI_ORCHARD_HOSTED", value)
        assert hosted_mode_enabled() is False
