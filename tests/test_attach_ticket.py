# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Attach tickets (``vgi.attach_tickets.v1``): format, sealing, hosting, redemption.

- The shared vectors (``vgi/_test_fixtures/attach_ticket_vectors.json``) mint,
  open, reject and redeem exactly as written, and the file matches its
  generator byte for byte.
- ``seal_attach`` refuses anonymous callers and options the catalog does not
  declare, and caps the lifetime at the grant maximum.
- The protocol is hosted only on HTTP, only with an explicitly configured
  signing key, and only when the worker can issue grants.
- ``catalog_attach`` redeems ``vgi_attach_ticket`` before any catalog code,
  refuses any other option beside it, and routes a ``MetaWorker`` by the
  sealed catalog.
- End to end over HTTP: a logged-in user seals their attach and mints a grant;
  a runner holding only the grant reattaches with only the ticket and reads
  the same row.
"""

from __future__ import annotations

import base64
import importlib.util
import json
import math
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Annotated, Any

import pyarrow as pa
import pytest
from vgi_rpc.errors import error_code_of, error_details_of, error_kind_of
from vgi_rpc.grants import GRANT_KEYS_ENV, GRANT_MAX_TTL_ENV
from vgi_rpc.http import http_connect
from vgi_rpc.http._testing import _SyncTestClient
from vgi_rpc.rpc import AuthContext, RpcError
from vgi_rpc.rpc._token_identity import Identity
from vgi_rpc.utils import deserialize_record_batch, serialize_record_batch_bytes

from vgi._test_fixtures.ticket_probe import TicketProbeWorker, api_key_digest
from vgi._test_fixtures.twin_catalogs import TwinAWorker
from vgi.attach_ticket import (
    ATTACH_TICKETS_PROTOCOL_NAME,
    AttachTicket,
    AttachTickets,
    AttachTicketsImpl,
    SealAttachRequest,
    _encode_payload,
    attach_ticket_aad,
    mint_attach_ticket,
    open_attach_ticket,
    redeem_attach_ticket,
)
from vgi.auth import IssuedGrant
from vgi.catalog.attach_option import AttachOption, AttachOptionSpec
from vgi.client import Client
from vgi.meta_worker import MetaWorker
from vgi.protocol import CatalogAttachRequest, VgiProtocol
from vgi.rpc_server import build_rpc_server, resolve_grant_keys
from vgi.serve import create_app
from vgi.worker import Worker

_ROOT = Path(__file__).resolve().parent.parent
_VECTORS_PATH = _ROOT / "vgi" / "_test_fixtures" / "attach_ticket_vectors.json"
_VECTORS: dict[str, Any] = json.loads(_VECTORS_PATH.read_text(encoding="utf-8"))
_DEFAULTS = _VECTORS["defaults"]

_GRANT_KEY_B64 = base64.b64encode(bytes(range(32))).decode()
_SIGNING_KEY = b"attach-ticket-test-signing-key-1"
_IDENTITY = "vgi_rpc.Identity.v1"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """No ambient key, grant or auth configuration leaks into a test."""
    for key in (
        GRANT_KEYS_ENV,
        GRANT_MAX_TTL_ENV,
        "VGI_RPC_GRANT_AUDIENCE",
        "VGI_SIGNING_KEY",
        "VGI_SIGNING_KEY_MINTED",
        "VGI_INTROSPECT_PRINCIPALS",
        "VGI_BEARER_TOKENS",
        "VGI_JWT_ISSUER",
        "VGI_PROXY_PROOF_MODE",
    ):
        monkeypatch.delenv(key, raising=False)
    yield


def _key(case: dict[str, Any]) -> bytes:
    return base64.b64decode(case.get("signing_key_b64", _DEFAULTS["signing_key_b64"]))


def _options_batch(options: dict[str, Any]) -> pa.RecordBatch | None:
    return pa.RecordBatch.from_pylist([options]) if options else None


def _as_user(monkeypatch: pytest.MonkeyPatch, principal: str | None, domain: str = "jwt") -> None:
    """Make ``current_auth()`` inside the worker answer as *principal* (``None``: anonymous)."""
    auth = (
        AuthContext.anonymous()
        if principal is None
        else AuthContext(principal=principal, authenticated=True, domain=domain)
    )
    monkeypatch.setattr("vgi.worker.current_auth", lambda: auth)


# ---------------------------------------------------------------------------
# Vectors
# ---------------------------------------------------------------------------


def _load_generator() -> Any:
    spec = importlib.util.spec_from_file_location(
        "gen_attach_ticket_vectors", _ROOT / "scripts" / "gen_attach_ticket_vectors.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestVectors:
    """The shared vectors, which every SDK must reproduce exactly."""

    def test_file_matches_generator(self) -> None:
        """Regenerate with scripts/gen_attach_ticket_vectors.py after a format change."""
        assert _VECTORS_PATH.read_text(encoding="utf-8") == _load_generator().render()

    @pytest.mark.parametrize("case", _VECTORS["mint"], ids=lambda c: c["name"])
    def test_mint(self, case: dict[str, Any]) -> None:
        """Fixed key, nonce, clock and id produce the exact token, payload and AAD."""
        token, claims = mint_attach_ticket(
            _key(case),
            principal=case["principal"],
            catalog_name=case["catalog_name"],
            options_ipc=base64.b64decode(case["options_ipc_b64"]),
            data_version_spec=case["data_version_spec"],
            implementation_version=case["implementation_version"],
            issued_at=case["issued_at"],
            expires_at=case["expires_at"],
            ticket_id=case["ticket_id"],
            nonce=bytes.fromhex(case["nonce_hex"]),
        )
        assert token == case["token"]
        assert _encode_payload(claims).hex() == case["payload_hex"]
        assert attach_ticket_aad(case["principal"]).hex() == case["aad_hex"]

    @pytest.mark.parametrize("case", _VECTORS["accept"], ids=lambda c: c["name"])
    def test_accept(self, case: dict[str, Any]) -> None:
        """Accept."""
        claims = open_attach_ticket(
            _key(case), case["token"], principal=case["principal"], now=case.get("now", _DEFAULTS["now"])
        )
        expected = case["claims"]
        assert claims.issued_at == expected["issued_at"]
        assert claims.expires_at == expected["expires_at"]
        assert claims.ticket_id == expected["ticket_id"]
        assert claims.catalog_name == expected["catalog_name"]
        assert claims.data_version_spec == expected["data_version_spec"]
        assert claims.implementation_version == expected["implementation_version"]
        assert claims.options_ipc == base64.b64decode(expected["options_ipc_b64"])

    @pytest.mark.parametrize("case", _VECTORS["reject"], ids=lambda c: c["name"])
    def test_reject(self, case: dict[str, Any]) -> None:
        """Reject, with the right kind and code, never echoing the ticket."""
        with pytest.raises(Exception) as excinfo:
            open_attach_ticket(
                _key(case), case["token"], principal=case["principal"], now=case.get("now", _DEFAULTS["now"])
            )
        exc = excinfo.value
        assert error_kind_of(exc) == case["error_kind"]
        expected_code = "INVALID_ARGUMENT" if case["error_kind"] == "attach_ticket_invalid" else "FAILED_PRECONDITION"
        assert error_code_of(exc) == expected_code
        assert error_details_of(exc)
        if len(case["token"]) > 12:
            assert case["token"] not in str(exc)

    @pytest.mark.parametrize("case", _VECTORS["redeem"], ids=lambda c: c["name"])
    def test_redeem(self, case: dict[str, Any]) -> None:
        """What catalog_attach does with the options it receives."""
        request = CatalogAttachRequest(
            name="whatever-the-runner-typed",
            options=_options_batch(case["options"]),
            data_version_spec=None,
            implementation_version=None,
        )
        principal = case["principal"]
        auth = (
            AuthContext(principal=principal, authenticated=True, domain="grant")
            if principal
            else AuthContext.anonymous()
        )
        kwargs: dict[str, Any] = {
            "signing_key": base64.b64decode(_DEFAULTS["signing_key_b64"]),
            "auth": auth,
            "now": _DEFAULTS["now"],
        }
        if "error_kind" in case:
            with pytest.raises(Exception) as excinfo:
                redeem_attach_ticket(request, case["options"], **kwargs)
            assert error_kind_of(excinfo.value) == case["error_kind"]
            return
        restored = redeem_attach_ticket(request, case["options"], **kwargs)
        if case["result"] is None:
            assert restored is None
            return
        assert restored is not None
        assert restored.name == case["result"]["catalog_name"]
        assert restored.data_version_spec == case["result"]["data_version_spec"]
        assert restored.implementation_version == case["result"]["implementation_version"]
        got = restored.options.to_pylist()[0] if restored.options is not None else {}
        assert got == case["result"]["options"]


class TestSealing:
    """Round trips the vectors do not pin."""

    def test_round_trip_random_nonce_and_id(self) -> None:
        """Round trip random nonce and id."""
        t1, c1 = mint_attach_ticket(
            _SIGNING_KEY, principal="alice", catalog_name="c", options_ipc=b"", issued_at=10, expires_at=0
        )
        t2, c2 = mint_attach_ticket(
            _SIGNING_KEY, principal="alice", catalog_name="c", options_ipc=b"", issued_at=10, expires_at=0
        )
        assert t1 != t2 and c1.ticket_id != c2.ticket_id
        assert open_attach_ticket(_SIGNING_KEY, t1, principal="alice", now=1e12) == c1

    def test_aad_is_principal_only_not_domain(self) -> None:
        """Sealed under one login domain, opened under the grant domain: only the principal binds."""
        token, _ = mint_attach_ticket(
            _SIGNING_KEY, principal="alice", catalog_name="c", options_ipc=b"", issued_at=10, expires_at=0
        )
        assert open_attach_ticket(_SIGNING_KEY, token, principal="alice").catalog_name == "c"

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"principal": ""},
            {"catalog_name": ""},
            {"expires_at": 5},
            {"ticket_id": "ABC"},
            {"options_ipc": b"\x00" * (16 * 1024 + 1)},
        ],
    )
    def test_mint_refuses_what_open_would_reject(self, kwargs: dict[str, Any]) -> None:
        """Mint refuses what open would reject."""
        args: dict[str, Any] = {
            "principal": "alice",
            "catalog_name": "c",
            "options_ipc": b"",
            "issued_at": 10,
            "expires_at": 0,
        } | kwargs
        with pytest.raises(ValueError):
            mint_attach_ticket(_SIGNING_KEY, **args)


# ---------------------------------------------------------------------------
# Reserved option name
# ---------------------------------------------------------------------------


class TestReservedOptionName:
    """``vgi_attach_ticket`` cannot be declared by any catalog."""

    @pytest.mark.parametrize("name", ["vgi_attach_ticket", "VGI_Attach_Ticket"])
    def test_spec_refuses_reserved_name(self, name: str) -> None:
        """Spec refuses reserved name."""
        with pytest.raises(ValueError, match="reserved"):
            AttachOptionSpec(name=name, desc="", type=pa.string(), default=None)

    def test_declaring_it_fails_at_class_definition(self) -> None:
        """Declaring it fails at class definition."""
        with pytest.raises(ValueError, match="reserved"):

            class _Bad(Worker):
                class AttachOptions:
                    vgi_attach_ticket: Annotated[str, AttachOption(desc="nope")] = ""


# ---------------------------------------------------------------------------
# Hosting conditions
# ---------------------------------------------------------------------------


class _MintingWorker(TicketProbeWorker):
    """Mints its own grants: no grant keys needed to host tickets."""

    @classmethod
    def mint_grant(cls, principal: str, purpose: str, scopes: list[str], ttl_seconds: int) -> IssuedGrant:
        """Mint grant."""
        return IssuedGrant(token=f"own-{principal}", expires_at=time.time() + ttl_seconds)


def _server(worker: Any, *, transport: str = "http", configured: bool = True, keys: bool = True) -> Any:
    if keys:
        import os

        os.environ[GRANT_KEYS_ENV] = _GRANT_KEY_B64
    try:
        return build_rpc_server(
            worker,
            transport=transport,  # type: ignore[arg-type]
            grant_keys=resolve_grant_keys() if keys else None,
            signing_key_configured=configured,
        )
    finally:
        import os

        os.environ.pop(GRANT_KEYS_ENV, None)


def _keyed(worker: Any) -> Any:
    worker._signing_key = _SIGNING_KEY
    return worker


class TestHosting:
    """Hosted on HTTP with a configured key and the ability to issue grants; absent otherwise."""

    def test_hosted_with_configured_key_and_grant_keys(self) -> None:
        """Hosted with configured key and grant keys."""
        server = _server(_keyed(TicketProbeWorker(quiet=True)))
        assert server.bindings[ATTACH_TICKETS_PROTOCOL_NAME].methods.keys() == {"seal_attach"}

    def test_hosted_with_own_mint_grant(self) -> None:
        """Hosted with own mint grant."""
        server = _server(_keyed(_MintingWorker(quiet=True)), keys=False)
        assert ATTACH_TICKETS_PROTOCOL_NAME in server.bindings

    def test_absent_without_grants(self) -> None:
        """Absent without grants."""
        assert ATTACH_TICKETS_PROTOCOL_NAME not in _server(_keyed(TicketProbeWorker(quiet=True)), keys=False).bindings

    def test_absent_with_a_minted_key(self) -> None:
        """Absent with a minted key."""
        server = _server(_keyed(TicketProbeWorker(quiet=True)), configured=False)
        assert ATTACH_TICKETS_PROTOCOL_NAME not in server.bindings
        assert _IDENTITY in server.bindings

    def test_absent_without_a_key(self) -> None:
        """Absent without a key."""
        assert ATTACH_TICKETS_PROTOCOL_NAME not in _server(TicketProbeWorker(quiet=True)).bindings

    @pytest.mark.parametrize("transport", ["pipe", "unix", "tcp", "iroh"])
    def test_http_only(self, transport: str) -> None:
        """Http only."""
        assert (
            ATTACH_TICKETS_PROTOCOL_NAME
            not in _server(_keyed(TicketProbeWorker(quiet=True)), transport=transport).bindings
        )

    def test_metaworker_hosted(self) -> None:
        """Metaworker hosted."""
        meta = MetaWorker([TwinAWorker(quiet=True), TicketProbeWorker(quiet=True)])
        meta._signing_key = _SIGNING_KEY
        assert ATTACH_TICKETS_PROTOCOL_NAME in _server(meta).bindings

    def test_create_app_explicit_key_hosts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """create_app with VGI_SIGNING_KEY from the environment hosts it."""
        monkeypatch.setenv(GRANT_KEYS_ENV, _GRANT_KEY_B64)
        monkeypatch.setenv("VGI_SIGNING_KEY", "operator-configured-key")
        assert ATTACH_TICKETS_PROTOCOL_NAME in _protocols(create_app(TicketProbeWorker))

    def test_create_app_minted_key_does_not(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No VGI_SIGNING_KEY: create_app mints one, and a minted key never hosts tickets."""
        monkeypatch.setenv(GRANT_KEYS_ENV, _GRANT_KEY_B64)
        assert ATTACH_TICKETS_PROTOCOL_NAME not in _protocols(create_app(TicketProbeWorker))

    def test_create_app_key_minted_by_parent_does_not(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A key a pre-fork parent minted and exported is not a configured key."""
        from vgi.serve import resolve_shared_signing_key

        monkeypatch.setenv(GRANT_KEYS_ENV, _GRANT_KEY_B64)
        resolve_shared_signing_key(propagate_to_children=True)
        assert ATTACH_TICKETS_PROTOCOL_NAME not in _protocols(create_app(TicketProbeWorker))


def _protocols(app: Any) -> set[str]:
    from vgi_rpc.rpc._reflection import Reflection

    with http_connect(Reflection, client=_SyncTestClient(app)) as reflection:  # type: ignore[type-abstract]
        return {p.protocol for p in reflection.list_protocols().protocols}


# ---------------------------------------------------------------------------
# seal_attach rules (the implementation, called directly)
# ---------------------------------------------------------------------------


class _Ctx:
    def __init__(self, principal: str | None) -> None:
        self.auth = (
            AuthContext.anonymous()
            if principal is None
            else AuthContext(principal=principal, authenticated=True, domain="jwt")
        )


def _impl(max_ttl: int | None = 3600) -> AttachTicketsImpl:
    return AttachTicketsImpl(_keyed(TicketProbeWorker(quiet=True)), max_ttl_seconds=max_ttl)


def _seal(impl: AttachTicketsImpl, principal: str | None = "alice", **kwargs: Any) -> AttachTicket:
    request: dict[str, Any] = {
        "catalog_name": "ticket_probe",
        "options": _options_batch({"region": "eu-west-2", "api_key": "sk-1"}),
    } | kwargs
    return impl.seal_attach(SealAttachRequest(**request), _Ctx(principal))  # type: ignore[arg-type]


def _violations(exc: BaseException) -> list[str]:
    (detail,) = error_details_of(exc)
    return [v["field"] for v in detail["field_violations"]]


class TestSealAttach:
    """seal_attach validates against the declared options and caps the lifetime."""

    def test_seals_for_the_caller(self) -> None:
        """Seals for the caller."""
        ticket = _seal(_impl())
        claims = open_attach_ticket(_SIGNING_KEY, ticket.ticket, principal="alice")
        assert claims.catalog_name == "ticket_probe"
        batch, _ = deserialize_record_batch(claims.options_ipc)
        assert batch.to_pylist() == [{"region": "eu-west-2", "api_key": "sk-1"}]

    def test_anonymous_is_action_denied(self) -> None:
        """Anonymous is action denied."""
        with pytest.raises(Exception) as excinfo:
            _seal(_impl(), principal=None)
        assert error_kind_of(excinfo.value) == "action_denied"
        assert error_code_of(excinfo.value) == "PERMISSION_DENIED"

    @pytest.mark.parametrize(
        ("kwargs", "field"),
        [
            ({"catalog_name": "nope"}, "catalog_name"),
            ({"options": _options_batch({"region": "x", "api_key": "k", "colour": "red"})}, "options.colour"),
            ({"options": _options_batch({"region": "x"})}, "options.api_key"),
            ({"options": None}, "options.api_key"),
            (
                {"options": _options_batch({"api_key": "k", "vgi_attach_ticket": "vgia1.x"})},
                "options.vgi_attach_ticket",
            ),
            ({"options": pa.RecordBatch.from_pylist([{"api_key": "a"}, {"api_key": "b"}])}, "options"),
            ({"options": _options_batch({"api_key": "k" * (17 * 1024)})}, "options"),
            ({"ttl_seconds": -1}, "ttl_seconds"),
        ],
    )
    def test_invalid_request(self, kwargs: dict[str, Any], field: str) -> None:
        """Invalid request."""
        with pytest.raises(Exception) as excinfo:
            _seal(_impl(), **kwargs)
        assert error_kind_of(excinfo.value) == "invalid_request"
        assert error_code_of(excinfo.value) == "INVALID_ARGUMENT"
        assert field in _violations(excinfo.value)

    def test_option_names_are_case_insensitive(self) -> None:
        """Option names are case insensitive."""
        _seal(_impl(), options=_options_batch({"API_KEY": "k"}))

    @pytest.mark.parametrize(
        ("ttl", "ceiling", "expected"),
        [(0, 3600, 3600), (60, 3600, 60), (99999, 3600, 3600), (0, None, None), (60, None, 60)],
    )
    def test_lifetime(self, ttl: int, ceiling: int | None, expected: int | None) -> None:
        """expires_at = min(requested, grant maximum); 0 asks for the maximum; no maximum is +inf."""
        before = int(time.time())
        ticket = _seal(_impl(ceiling), ttl_seconds=ttl)
        claims = open_attach_ticket(_SIGNING_KEY, ticket.ticket, principal="alice")
        if expected is None:
            assert claims.expires_at == 0 and math.isinf(ticket.expires_at)
        else:
            assert before + expected <= claims.expires_at <= int(time.time()) + expected
            assert ticket.expires_at == claims.expires_at

    def test_max_ttl_from_grant_keys_and_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Max ttl from grant keys and env."""
        from vgi.attach_ticket import resolve_ticket_max_ttl

        assert resolve_ticket_max_ttl(None) is None
        monkeypatch.setenv(GRANT_MAX_TTL_ENV, "120")
        assert resolve_ticket_max_ttl(None) == 120
        monkeypatch.setenv(GRANT_KEYS_ENV, _GRANT_KEY_B64)
        assert resolve_ticket_max_ttl(resolve_grant_keys()) == 120


# ---------------------------------------------------------------------------
# Redemption inside catalog_attach
# ---------------------------------------------------------------------------


def _ticket(principal: str = "alice", **kwargs: Any) -> str:
    args: dict[str, Any] = {
        "principal": principal,
        "catalog_name": "ticket_probe",
        "options_ipc": serialize_record_batch_bytes(
            pa.RecordBatch.from_pylist([{"region": "ap-south-1", "api_key": "sk-9"}])
        ),
        "issued_at": int(time.time()),
        "expires_at": int(time.time()) + 600,
    } | kwargs
    return mint_attach_ticket(_SIGNING_KEY, **args)[0]


def _probe_row(worker: Any, attach: bytes) -> tuple[str, str]:
    plain = worker._unwrap_attach(attach)
    region, digest = bytes(plain).split(b"\x00", 1)
    return region.decode(), digest.decode()


class TestRedemption:
    """catalog_attach restores the sealed attach before the catalog sees anything."""

    def _attach(self, worker: Any, options: dict[str, Any], name: str = "ticket_probe") -> Any:
        return worker.catalog_attach(
            CatalogAttachRequest(
                name=name, options=_options_batch(options), data_version_spec=None, implementation_version=None
            )
        )

    def test_restores_sealed_options(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Restores sealed options."""
        _as_user(monkeypatch, "alice", domain="grant")
        worker = _keyed(TicketProbeWorker(quiet=True))
        result = self._attach(worker, {"vgi_attach_ticket": _ticket()})
        assert _probe_row(worker, result.attach_opaque_data) == ("ap-south-1", api_key_digest("sk-9"))

    def test_sealed_catalog_name_wins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Sealed catalog name wins."""
        _as_user(monkeypatch, "alice")
        worker = _keyed(TicketProbeWorker(quiet=True))
        result = self._attach(worker, {"vgi_attach_ticket": _ticket()}, name="some_alias")
        assert _probe_row(worker, result.attach_opaque_data)[0] == "ap-south-1"

    def test_no_other_option_alongside(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No other option alongside."""
        _as_user(monkeypatch, "alice")
        with pytest.raises(Exception) as excinfo:
            self._attach(_keyed(TicketProbeWorker(quiet=True)), {"vgi_attach_ticket": _ticket(), "region": "x"})
        assert error_kind_of(excinfo.value) == "invalid_request"
        assert _violations(excinfo.value) == ["options.region"]

    def test_other_principal_is_invalid(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Other principal is invalid."""
        _as_user(monkeypatch, "bob", domain="grant")
        with pytest.raises(Exception) as excinfo:
            self._attach(_keyed(TicketProbeWorker(quiet=True)), {"vgi_attach_ticket": _ticket()})
        assert error_kind_of(excinfo.value) == "attach_ticket_invalid"

    def test_expired_is_failed_precondition(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Expired is failed precondition."""
        _as_user(monkeypatch, "alice")
        old = int(time.time()) - 7200
        with pytest.raises(Exception) as excinfo:
            self._attach(
                _keyed(TicketProbeWorker(quiet=True)),
                {"vgi_attach_ticket": _ticket(issued_at=old, expires_at=old + 600)},
            )
        assert error_kind_of(excinfo.value) == "attach_ticket_expired"
        assert error_code_of(excinfo.value) == "FAILED_PRECONDITION"

    def test_no_key_never_redeems(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Subprocess transports have no key, so a ticket never reaches the catalog."""
        _as_user(monkeypatch, "alice")
        with pytest.raises(Exception) as excinfo:
            self._attach(TicketProbeWorker(quiet=True), {"vgi_attach_ticket": _ticket()})
        assert error_kind_of(excinfo.value) == "attach_ticket_invalid"

    def test_restored_options_reach_loggable_attach_options_not_the_ticket(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The lifecycle log sees the restored options through loggable_attach_options, never the ticket."""
        _as_user(monkeypatch, "alice")
        worker = _keyed(TicketProbeWorker(quiet=True))
        seen: list[dict[str, Any]] = []
        cat = worker._get_catalog()

        def _loggable(options: dict[str, Any]) -> dict[str, Any]:
            seen.append(dict(options))
            return {}

        monkeypatch.setattr(cat, "loggable_attach_options", _loggable)
        token = _ticket()
        self._attach(worker, {"vgi_attach_ticket": token})
        assert seen == [{"region": "ap-south-1", "api_key": "sk-9"}]

    def test_metaworker_routes_by_sealed_catalog(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The request names twin_a; the ticket seals ticket_probe; ticket_probe serves it."""
        _as_user(monkeypatch, "alice")
        meta = MetaWorker([TwinAWorker(quiet=True), TicketProbeWorker(quiet=True)])
        meta._signing_key = _SIGNING_KEY
        result = self._attach(meta, {"vgi_attach_ticket": _ticket()}, name="twin_a")
        assert _probe_row(meta._workers[1], result.attach_opaque_data) == ("ap-south-1", api_key_digest("sk-9"))

    def test_metaworker_refuses_extra_options(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Metaworker refuses extra options."""
        _as_user(monkeypatch, "alice")
        meta = MetaWorker([TwinAWorker(quiet=True), TicketProbeWorker(quiet=True)])
        meta._signing_key = _SIGNING_KEY
        with pytest.raises(Exception) as excinfo:
            self._attach(meta, {"vgi_attach_ticket": _ticket(), "api_key": "x"})
        assert error_kind_of(excinfo.value) == "invalid_request"


# ---------------------------------------------------------------------------
# End to end over HTTP
# ---------------------------------------------------------------------------


def _header_authenticate(req: Any) -> AuthContext:
    """``X-Test-Principal`` with a fresh ``auth_time``; a bare bearer falls through to grants."""
    principal = req.get_header("X-Test-Principal")
    if principal:
        return AuthContext(domain="test", authenticated=True, principal=principal, claims={"auth_time": time.time()})
    if req.get_header("Authorization"):
        raise ValueError("not a test-principal request")
    return AuthContext.anonymous()


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch) -> Any:
    """The probe worker over HTTP with a configured signing key and grant keys."""
    monkeypatch.setenv(GRANT_KEYS_ENV, _GRANT_KEY_B64)
    return create_app(TicketProbeWorker, describe=False, signing_key=_SIGNING_KEY, authenticate=_header_authenticate)


def _attach_over_http(client: _SyncTestClient, options: dict[str, Any], name: str = "ticket_probe") -> bytes:
    with http_connect(VgiProtocol, client=client) as vgi:  # type: ignore[type-abstract]
        result = vgi.catalog_attach(
            request=CatalogAttachRequest(
                name=name, options=_options_batch(options), data_version_spec=None, implementation_version=None
            )
        )
    assert result.attach_opaque_data is not None
    return bytes(result.attach_opaque_data)


def _read_probe(client: _SyncTestClient, attach: bytes) -> dict[str, Any]:
    with Client.from_http("http://vgi.test", httpx_client=client, attach_opaque_data=attach) as vgi:
        batches = list(vgi.table_function(function_name="ticket_probe", schema_path=["main"]))
    rows = pa.Table.from_batches(batches).to_pylist()
    assert len(rows) == 1
    return rows[0]


class TestEndToEnd:
    """Seal while logged in, then reattach anywhere with a grant and the ticket alone."""

    def test_grant_plus_ticket_reattaches_as_the_user(self, app: Any) -> None:
        """Grant plus ticket reattaches as the user."""
        options = {"region": "eu-west-2", "api_key": "sk-live-secret"}
        user = _SyncTestClient(app, default_headers={"X-Test-Principal": "alice@example.com"})
        original = _read_probe(user, _attach_over_http(user, options))
        assert original == {"region": "eu-west-2", "api_key_sha256": api_key_digest("sk-live-secret")}

        with http_connect(AttachTickets, client=user) as tickets:  # type: ignore[type-abstract]
            sealed = tickets.seal_attach(
                request=SealAttachRequest(catalog_name="ticket_probe", options=_options_batch(options))
            )
        with http_connect(Identity, client=user) as identity:  # type: ignore[type-abstract]
            grant = identity.issue_grant(purpose="vgi.unattended", scopes=[], ttl_seconds=600)
        assert sealed.ticket.startswith("vgia1.") and "sk-live-secret" not in sealed.ticket
        assert sealed.expires_at <= time.time() + 7 * 24 * 3600 + 5

        runner = _SyncTestClient(app, default_headers={"Authorization": f"Bearer {grant.token}"})
        reattached = _attach_over_http(runner, {"vgi_attach_ticket": sealed.ticket})
        assert _read_probe(runner, reattached) == original

    def test_another_users_grant_is_refused(self, app: Any) -> None:
        """Another users grant is refused."""
        alice = _SyncTestClient(app, default_headers={"X-Test-Principal": "alice"})
        bob = _SyncTestClient(app, default_headers={"X-Test-Principal": "bob"})
        with http_connect(AttachTickets, client=alice) as tickets:  # type: ignore[type-abstract]
            sealed = tickets.seal_attach(
                request=SealAttachRequest(catalog_name="ticket_probe", options=_options_batch({"api_key": "k"}))
            )
        with http_connect(Identity, client=bob) as identity:  # type: ignore[type-abstract]
            bobs_grant = identity.issue_grant(purpose="p", scopes=[], ttl_seconds=600)
        runner = _SyncTestClient(app, default_headers={"Authorization": f"Bearer {bobs_grant.token}"})
        with pytest.raises(RpcError) as excinfo:
            _attach_over_http(runner, {"vgi_attach_ticket": sealed.ticket})
        assert excinfo.value.error_kind == "attach_ticket_invalid"
        assert excinfo.value.error_code == "INVALID_ARGUMENT"
        assert sealed.ticket not in str(excinfo.value)

    def test_extra_option_is_refused_over_the_wire(self, app: Any) -> None:
        """Extra option is refused over the wire."""
        alice = _SyncTestClient(app, default_headers={"X-Test-Principal": "alice"})
        with http_connect(AttachTickets, client=alice) as tickets:  # type: ignore[type-abstract]
            sealed = tickets.seal_attach(
                request=SealAttachRequest(catalog_name="ticket_probe", options=_options_batch({"api_key": "k"}))
            )
        with pytest.raises(RpcError) as excinfo:
            _attach_over_http(alice, {"vgi_attach_ticket": sealed.ticket, "region": "us-west-1"})
        assert excinfo.value.error_kind == "invalid_request"

    def test_anonymous_seal_is_action_denied(self, app: Any) -> None:
        """Anonymous seal is action denied."""
        request = SealAttachRequest(catalog_name="ticket_probe", options=_options_batch({"api_key": "k"}))
        with (
            http_connect(AttachTickets, client=_SyncTestClient(app)) as tickets,  # type: ignore[type-abstract]
            pytest.raises(RpcError) as excinfo,
        ):
            tickets.seal_attach(request=request)
        assert excinfo.value.error_kind == "action_denied"
        assert excinfo.value.error_code == "PERMISSION_DENIED"

    def test_undeclared_option_is_invalid_request(self, app: Any) -> None:
        """Undeclared option is invalid request."""
        alice = _SyncTestClient(app, default_headers={"X-Test-Principal": "alice"})
        request = SealAttachRequest(catalog_name="ticket_probe", options=_options_batch({"api_key": "k", "x": 1}))
        with (
            http_connect(AttachTickets, client=alice) as tickets,  # type: ignore[type-abstract]
            pytest.raises(RpcError) as excinfo,
        ):
            tickets.seal_attach(request=request)
        assert excinfo.value.error_kind == "invalid_request"
        assert excinfo.value.error_code == "INVALID_ARGUMENT"


def test_fixture_http_server_hosts_attach_tickets_when_keyed(http_worker: Any) -> None:
    """The real ``vgi-fixture-http`` (a MetaWorker) hosts tickets once keyed.

    Regression: the fixture keyed only the MetaWorker's children, so the served
    MetaWorker had no signing key and ``build_rpc_server`` never hosted
    ``vgi.attach_tickets.v1``. Every cross-SDK attach_ticket test against the
    Python fixture would have failed.
    """
    from vgi_rpc.rpc._reflection import Reflection

    base_url = http_worker(env={"VGI_SIGNING_KEY": "ab" * 32, GRANT_KEYS_ENV: _GRANT_KEY_B64})
    with http_connect(Reflection, base_url) as reflection:  # type: ignore[type-abstract]
        hosted = {p.protocol for p in reflection.list_protocols().protocols}
    assert ATTACH_TICKETS_PROTOCOL_NAME in hosted
