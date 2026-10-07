# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""``Client.list_protocols`` / ``Client.describe_protocol`` over reflection.

Every worker hosts ``vgi_rpc.Reflection.v1`` on every transport. These tests
ask the fixture worker what it hosts through the Python ``Client`` on each
transport it speaks, and check the answer against the worker's own
``RpcServer`` bindings. The cross-language coverage of the same surface is the
C++ repo's ``test/sql/integration/reflection/``.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

import pytest
from vgi_rpc import ReflectionNotSupportedError
from vgi_rpc.rpc import RpcError

from tests.conftest import SUBPROCESS_FIXTURE_WORKER
from vgi._test_fixtures.worker import ExampleWorker
from vgi.client import Client, ClientError, HostedProtocol
from vgi.protocol import VgiProtocol
from vgi.rpc_server import build_rpc_server

_SECONDARY = "conformance.Secondary.v1"
_REFLECTION = "vgi_rpc.Reflection.v1"
_EXPECTED_NAMES = ["vgi.v2", _SECONDARY, _REFLECTION]
_HASH = re.compile(r"^[0-9a-f]{64}$")


def _server_view() -> list[HostedProtocol]:
    """Return what the fixture worker's own server binds, as the client would report it."""
    server = build_rpc_server(ExampleWorker(quiet=True), transport="pipe")
    return [
        HostedProtocol(name=b.name, version=b.version or "", hash=b.protocol_hash) for b in server.bindings.values()
    ]


def _assert_fixture_listing(listing: list[HostedProtocol]) -> None:
    """The fixture's hosted set: ``vgi.v2`` first, the secondary, then reflection."""
    assert all(isinstance(p, HostedProtocol) for p in listing)
    assert [p.name for p in listing] == _EXPECTED_NAMES
    assert listing[0].version == VgiProtocol.protocol_version
    for p in listing:
        assert _HASH.match(p.hash), f"{p.name} hash {p.hash!r} is not 64 lowercase hex chars"
    assert listing == _server_view()


class TestListProtocols:
    """Each transport the fixture worker speaks reports the same hosted set."""

    def test_client_transport(self, client_transport: Callable[[], Client]) -> None:
        """Launch / pooled subprocess / HTTP, per the shared transport matrix."""
        with client_transport() as client:
            _assert_fixture_listing(client.list_protocols())

    def test_stdio_direct(self) -> None:
        """A directly spawned stdin/stdout worker, independent of the default matrix."""
        with Client(SUBPROCESS_FIXTURE_WORKER, pool=None) as client:
            first = client.list_protocols()
            _assert_fixture_listing(first)
            # The pipe is still usable for vgi.v2 after a reflection call.
            assert client.list_protocols() == first

    def test_http(self, _shared_http_base_url: Callable[[], str]) -> None:
        """``Client.from_http`` against the fixture HTTP server."""
        pytest.importorskip("vgi_rpc.http")
        with Client.from_http(_shared_http_base_url()) as client:
            _assert_fixture_listing(client.list_protocols())

    def test_requires_started_client(self) -> None:
        """An unstarted client raises ``ClientError`` rather than spawning a worker."""
        with pytest.raises(ClientError, match="not started"):
            Client(SUBPROCESS_FIXTURE_WORKER, pool=None).list_protocols()


class TestDescribeProtocol:
    """``describe_protocol`` returns one protocol's methods."""

    def test_describes_secondary(self, client_transport: Callable[[], Client]) -> None:
        """The secondary's description matches its listed hash."""
        with client_transport() as client:
            listed = {p.name: p for p in client.list_protocols()}
            described = client.describe_protocol(_SECONDARY)
        assert described.protocol_name == _SECONDARY
        assert described.protocol_hash == listed[_SECONDARY].hash
        assert "echo_string" in described.methods

    def test_describes_vgi_v2(self) -> None:
        """``vgi.v2`` describes with its version and the client's own method set."""
        with Client(SUBPROCESS_FIXTURE_WORKER, pool=None) as client:
            described = client.describe_protocol("vgi.v2")
        assert described.protocol_version == VgiProtocol.protocol_version
        assert {"bind", "init"} <= set(described.methods)

    def test_unhosted_protocol_raises(self) -> None:
        """A protocol the worker does not host is a ``ClientError``."""
        with (
            Client(SUBPROCESS_FIXTURE_WORKER, pool=None) as client,
            pytest.raises(ClientError, match="nope.v1"),
        ):
            client.describe_protocol("nope.v1")


def _patch_listing(monkeypatch: pytest.MonkeyPatch, error: RpcError) -> None:
    """Make vgi-rpc's ``list_protocols``, as the client calls it, raise *error*."""

    def _fail(target: object) -> Any:
        raise error

    monkeypatch.setattr("vgi.client.client.rpc_list_protocols", _fail)


class TestPreReflectionWorker:
    """A worker without reflection is reported as hosting only ``vgi.v2``.

    Which server answers mean "no reflection" is vgi-rpc's call (it raises
    ``ReflectionNotSupportedError``, tested there); the client's job is only
    to map that one exception to the fallback and everything else to
    ``ClientError``.
    """

    def test_falls_back_to_vgi_v2(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``ReflectionNotSupportedError`` yields one ``vgi.v2`` entry with an empty hash."""
        error = ReflectionNotSupportedError(
            "ProtocolNotSupportedError",
            "not hosted",
            "",
            error_code="UNIMPLEMENTED",
            error_kind="protocol_not_supported",
        )
        _patch_listing(monkeypatch, error)
        with Client(SUBPROCESS_FIXTURE_WORKER, pool=None) as client:
            assert client.list_protocols() == [HostedProtocol(name="vgi.v2", version="", hash="")]

    def test_other_errors_raise(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Any other reflection failure is a ``ClientError``, not a silent fallback."""
        _patch_listing(monkeypatch, RpcError("RuntimeError", "boom", "", error_code="INTERNAL"))
        with Client(SUBPROCESS_FIXTURE_WORKER, pool=None) as client, pytest.raises(ClientError, match="boom"):
            client.list_protocols()
