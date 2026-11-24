# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Protocol version negotiation in vgi-python (``PROTOCOL_VERSIONS_V1``).

vgi-rpc does the advertising; these pin what a VGI worker advertises (every
protocol it gates, on every server it builds), the ``VGI_TEST_PROTOCOL_RANGE``
fixture switch, the generated supported-window constants, and the schema
snapshot / compatibility check.
"""

from __future__ import annotations

import copy
import io
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc.protocol_versions import VersionRange, parse_versions

from tests._hosted_protocols_worker import HostingWorker
from vgi.codegen import protocol_schema_snapshot as snap
from vgi.codegen.protocol_version import supported_window, window_constants
from vgi.protocol import VgiProtocol
from vgi.rpc_server import TEST_PROTOCOL_RANGE_ENV, build_rpc_server
from vgi.secret_protocol import VgiSecretProtocol


def _gated(server: Any) -> dict[str, str]:
    """What a server's bindings say it gates, as name -> MIN-MAX."""
    return {
        name: str(b.version_range)
        for name, b in server.bindings.items()
        if b.version_range is not None and not b.version_exempt
    }


class TestWhatAWorkerAdvertises:
    """``build_rpc_server`` is the one builder; its servers advertise every gated protocol."""

    @pytest.mark.parametrize("transport", ["pipe", "unix", "tcp", "http"])
    def test_every_gated_protocol_is_advertised(self, transport: str, monkeypatch: pytest.MonkeyPatch) -> None:
        """vgi.v3 first, at exactly 3.4; nothing ungated."""
        monkeypatch.delenv(TEST_PROTOCOL_RANGE_ENV, raising=False)
        server = build_rpc_server(HostingWorker(quiet=True), transport=transport)  # type: ignore[arg-type]
        advertised = parse_versions(server.protocol_versions)
        assert {k: str(v) for k, v in advertised.items()} == _gated(server)
        assert server.protocol_versions.startswith("vgi.v3=3.4-3.4")

    def test_http_app_sends_the_header(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Every HTTP response from a worker app, a 404 included."""
        from vgi_rpc.http import make_wsgi_app

        monkeypatch.delenv(TEST_PROTOCOL_RANGE_ENV, raising=False)
        from falcon import testing

        server = build_rpc_server(HostingWorker(quiet=True), transport="http")
        client = testing.TestClient(make_wsgi_app(server, token_key=b"k" * 32))
        for resp in (client.simulate_head("/health"), client.simulate_post("/nope.v1/x")):
            assert resp.headers.get("VGI-Protocol-Versions") == server.protocol_versions

    def test_secret_service_advertises_the_secret_protocol(self) -> None:
        """The standalone secret endpoint gates and advertises ``vgi.secret.v1``."""
        from falcon import testing

        from vgi.secret_service import create_secret_app

        class _Secrets:
            def secret_lookup(self, path: str, type: str) -> None:  # noqa: A002 -- the protocol's parameter name
                del path, type

        app = create_secret_app(_Secrets(), signing_key=b"k" * 32)
        resp = testing.TestClient(app).simulate_head("/health")
        assert resp.headers.get("VGI-Protocol-Versions") == "vgi.secret.v1=1.0-1.0"


class TestFixtureSwitch:
    """``VGI_TEST_PROTOCOL_RANGE`` makes any fixture impersonate another version."""

    def test_bare_range_applies_to_the_worker_protocol(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``3.3-3.3``: the worker now advertises and accepts only 3.3."""
        monkeypatch.setenv(TEST_PROTOCOL_RANGE_ENV, "3.3-3.3")
        server = build_rpc_server(HostingWorker(quiet=True), transport="pipe")
        assert parse_versions(server.protocol_versions)["vgi.v3"] == VersionRange((3, 3), (3, 3))
        assert server.bindings["vgi.v3"].version_range == VersionRange((3, 3), (3, 3))

    def test_named_ranges(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``name=MIN-MAX`` names the protocol explicitly."""
        monkeypatch.setenv(TEST_PROTOCOL_RANGE_ENV, "vgi.v3=3.3-3.4")
        server = build_rpc_server(HostingWorker(quiet=True), transport="pipe")
        assert server.protocol_versions.startswith("vgi.v3=3.3-3.4")

    @pytest.mark.parametrize("value", ["3.x", "vgi.v3=nope", "vgi.v3=3.4-3.3"])
    def test_malformed_refuses_to_start(self, value: str, monkeypatch: pytest.MonkeyPatch) -> None:
        """A switch that silently did nothing would pass its test for the wrong reason."""
        monkeypatch.setenv(TEST_PROTOCOL_RANGE_ENV, value)
        with pytest.raises(SystemExit, match=TEST_PROTOCOL_RANGE_ENV):
            build_rpc_server(HostingWorker(quiet=True), transport="pipe")

    def test_refusal_names_both_windows_end_to_end(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A 3.4 client against a 3.3-3.3 fixture: ``protocol_version_unsupported`` naming both."""
        import threading

        from pyarrow import ipc
        from vgi_rpc.conformance._protocol_versions_pytest import request_body
        from vgi_rpc.rpc import RpcError, _dispatch_log_or_error, make_pipe_pair, rpc_methods

        monkeypatch.setenv(TEST_PROTOCOL_RANGE_ENV, "3.3-3.3")
        server = build_rpc_server(HostingWorker(quiet=True), transport="pipe")
        schema = rpc_methods(VgiProtocol)["bind"].params_schema
        body = request_body(
            "vgi.v3", "bind", schema, version="3.4.0", client_versions="vgi.v3=3.4-3.4", values={"request": b""}
        )
        client_t, server_t = make_pipe_pair()
        thread = threading.Thread(target=server.serve, args=(server_t,), daemon=True)
        thread.start()
        try:
            client_t.writer.write(body)
            client_t.writer.flush()
            reader = ipc.open_stream(client_t.reader)
            with pytest.raises(RpcError) as info:
                for batch, md in iter(reader.read_next_batch_with_custom_metadata, None):
                    _dispatch_log_or_error(batch, md)
        finally:
            client_t.close()
            thread.join(timeout=5)
            server_t.close()
        err = info.value
        assert err.error_kind == "protocol_version_unsupported"
        assert "vgi.v3 3.3-3.3" in err.error_message and "3.4-3.4" in err.error_message
        assert "too new" in err.error_message


class TestGeneratedWindow:
    """§7.3: the extension's supported-window constants come from the Protocol declarations."""

    def test_windows(self) -> None:
        """3.4 advertises 3.4-3.4; the secret protocol 1.0-1.0."""
        assert supported_window(VgiProtocol) == ("3.4", "3.4")
        assert window_constants(VgiProtocol, "VGI_PROTOCOL_VERSION") == {
            "VGI_PROTOCOL_VERSION_MIN": "3.4",
            "VGI_PROTOCOL_VERSION_MAX": "3.4",
            "VGI_PROTOCOL_VERSIONS_CLIENT": "vgi.v3=3.4-3.4",
        }
        assert (
            window_constants(VgiSecretProtocol, "VGI_SECRET_PROTOCOL_VERSION")["VGI_SECRET_PROTOCOL_VERSIONS_CLIENT"]
            == "vgi.secret.v1=1.0-1.0"
        )

    def test_headers_carry_them(self) -> None:
        """Both generated headers emit the window constants."""
        from vgi.codegen import cpp_protocol_version, cpp_secret_protocol_version

        main, secret = io.StringIO(), io.StringIO()
        cpp_protocol_version.emit(main)
        cpp_secret_protocol_version.emit(secret)
        assert 'VGI_PROTOCOL_VERSION_MIN = "3.4";' in main.getvalue()
        assert 'VGI_PROTOCOL_VERSIONS_CLIENT = "vgi.v3=3.4-3.4";' in main.getvalue()
        assert 'VGI_SECRET_PROTOCOL_VERSION_MAX = "1.0";' in secret.getvalue()

    def test_no_type_declared(self) -> None:
        """A protocol without a version has no window."""

        class _Bare:
            pass

        with pytest.raises(TypeError):
            supported_window(_Bare)


# ---------------------------------------------------------------------------
# Snapshots (§8)
# ---------------------------------------------------------------------------


def _b64(fields: list[pa.Field[Any]]) -> str:
    import base64

    return base64.b64encode(pa.schema(fields).serialize().to_pybytes()).decode()


def _doc(record_fields: list[pa.Field[Any]], methods: dict[str, str] | None = None) -> dict[str, Any]:
    empty = _b64([])
    return {
        "format": 1,
        "protocol": "demo.v1",
        "protocol_version": "1.0.0",
        "methods": {
            name: {"method_type": kind, "params": empty, "result": "", "header": ""}
            for name, kind in (methods or {"ping": "unary"}).items()
        },
        "records": {"Rec": {"schema": _b64(record_fields)}},
    }


_A = pa.field("a", pa.int64(), nullable=False)
_B = pa.field("b", pa.utf8(), nullable=True)


class TestCompatibility:
    """Additive passes; everything else needs a bump."""

    def test_the_repository_is_compatible(self) -> None:
        """CI's check: the current types match the checked-in snapshots."""
        out = io.StringIO()
        assert snap.check(snap.default_protocols(), out=out), out.getvalue()

    @pytest.mark.parametrize(
        ("new_fields", "methods"),
        [
            ([_A, _B], None),  # appended nullable field
            ([_A.with_nullable(True)], None),  # became nullable
            ([_A], {"ping": "unary", "pong": "unary"}),  # new method
        ],
    )
    def test_additive(self, new_fields: list[pa.Field[Any]], methods: dict[str, str] | None) -> None:
        """No breaking findings."""
        report = snap.compare(_doc([_A]), _doc(new_fields, methods))
        assert report.breaking == [] and report.additive

    @pytest.mark.parametrize(
        ("old_fields", "new_fields", "needle"),
        [
            ([_A], [], "removed or renamed"),
            ([_A], [pa.field("a", pa.int32(), nullable=False)], "type changed"),
            ([_A], [_A, pa.field("c", pa.int8(), nullable=False)], "required"),
            ([_A, _B], [_B, _A], "moved"),
            ([_B], [_B.with_nullable(False)], "non-nullable"),
            (
                [pa.field("s", pa.struct([_A]))],
                [pa.field("s", pa.struct([pa.field("a", pa.utf8(), nullable=False)]))],
                "s.a: type changed",
            ),
            ([pa.field("l", pa.list_(pa.int64()))], [pa.field("l", pa.list_(pa.int32()))], "type changed"),
        ],
    )
    def test_breaking(self, old_fields: list[pa.Field[Any]], new_fields: list[pa.Field[Any]], needle: str) -> None:
        """Each is reported."""
        report = snap.compare(_doc(old_fields), _doc(new_fields))
        assert any(needle in line for line in report.breaking), report.breaking

    def test_method_changes(self) -> None:
        """A removed method and a changed method type are breaking."""
        old = _doc([_A], {"ping": "unary", "gone": "unary"})
        new = _doc([_A], {"ping": "stream"})
        report = snap.compare(old, new)
        assert any("gone" in line for line in report.breaking)
        assert any("unary -> stream" in line for line in report.breaking)

    def test_record_removed_and_schema_appearing(self) -> None:
        """A removed record, and a result schema that appears where there was none."""
        old = _doc([_A])
        new = copy.deepcopy(old)
        new["records"] = {}
        new["methods"]["ping"]["result"] = _b64([_A])
        report = snap.compare(old, new)
        assert any("record Rec" in line for line in report.breaking)
        assert any("appeared" in line for line in report.breaking)

    def test_check_against_a_directory(self, tmp_path: Path) -> None:
        """Missing, older, newer and breaking snapshots, through ``check``."""
        out = io.StringIO()
        assert not snap.check([VgiSecretProtocol], root=tmp_path, out=out)
        assert "no snapshot" in out.getvalue()

        snap.write([VgiSecretProtocol], root=tmp_path)
        assert snap.check([VgiSecretProtocol], root=tmp_path, out=io.StringIO())

        path = snap.snapshot_path("vgi.secret.v1", "1.0.0", tmp_path)
        doc = json.loads(path.read_text())
        doc["methods"]["retired_method"] = doc["methods"]["secret_lookup"]
        path.write_text(json.dumps(doc))
        out = io.StringIO()
        assert not snap.check([VgiSecretProtocol], root=tmp_path, out=out)
        assert "retired_method" in out.getvalue()

        newer = tmp_path / "vgi.secret.v1" / "9.0"
        newer.mkdir()
        (newer / "snapshot.json").write_text(json.dumps(doc))
        out = io.StringIO()
        assert not snap.check([VgiSecretProtocol], root=tmp_path, out=out)
        assert "older than the latest snapshot" in out.getvalue()

    def test_a_bumped_version_passes(self, tmp_path: Path) -> None:
        """Current newer than the latest snapshot: pass with a reminder."""
        old = tmp_path / "vgi.secret.v1" / "0.9"
        old.mkdir(parents=True)
        (old / "snapshot.json").write_text(json.dumps(_doc([_A])))
        (tmp_path / "vgi.secret.v1" / "junk").mkdir()
        out = io.StringIO()
        assert snap.check([VgiSecretProtocol], root=tmp_path, out=out)
        assert "version bumped" in out.getvalue()

    def test_cli(self, tmp_path: Path) -> None:
        """``write`` then ``check`` through ``main``."""
        assert snap.main(["write", "--root", str(tmp_path)]) == 0
        assert snap.main(["check", "--root", str(tmp_path)]) == 0
