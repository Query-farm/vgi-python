# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""``Worker.hosted_protocols`` and the single server builder.

Every transport builds its ``RpcServer`` through
:func:`vgi.rpc_server.build_rpc_server`, so a worker's extra protocols and
reflection are hosted everywhere and identity only on HTTP. The end-to-end
tests drive each transport through its real entry point (a subprocess running
``Worker.main`` / ``MetaWorker.serve``, or ``create_app`` for HTTP) and ask
reflection what is hosted.
"""

from __future__ import annotations

import ast
import contextlib
import os
import queue
import subprocess
import sys
import tempfile
import threading
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any, ClassVar, Protocol

import pytest
from vgi_rpc.rpc import StderrMode, SubprocessTransport, tcp_connect, unix_connect
from vgi_rpc.rpc._reflection import Reflection

from tests._hosted_protocols_worker import CounterProtocol, EchoImpl, EchoProtocol, HostingWorker
from vgi.auth import TokenIdentity
from vgi.rpc_server import build_rpc_server
from vgi.worker import Worker

_WORKER_SCRIPT = str(Path(__file__).with_name("_hosted_protocols_worker.py"))
_REPO_ROOT = Path(__file__).resolve().parent.parent

_EXTRAS = ["vgi_test.Echo.v1", "vgi_test.Counter.v1"]
_EXPECTED = ["vgi.v2", *_EXTRAS, "vgi_rpc.Reflection.v1"]
_IDENTITY = "vgi_rpc.Identity.v1"

# Protocol classes are abstract to mypy; the connect helpers want concrete types.
_REFLECTION: type[Any] = Reflection
_ECHO: type[Any] = EchoProtocol
_COUNTER: type[Any] = CounterProtocol

_POSIX_ONLY = pytest.mark.skipif(sys.platform == "win32", reason="AF_UNIX launcher path is POSIX-only")


class _IdentityHostingWorker(HostingWorker):
    """Opts into ``vgi_rpc.Identity.v1`` as well."""

    @classmethod
    def resolve_token(cls, token: str) -> TokenIdentity | None:
        """Resolve one fixed credential."""
        return TokenIdentity(principal="alice") if token == "good" else None


def _names(proxy: Any) -> list[str]:
    """Return the protocol names reflection reports, in order."""
    return [p.protocol for p in proxy.list_protocols().protocols]


def _env() -> dict[str, str]:
    """Return the child environment, with startup logging suppressed."""
    return {**os.environ, "VGI_QUIET": "1"}


@contextlib.contextmanager
def _listening(*args: str, prefix: str) -> Iterator[str]:
    """Run the worker script with *args* and yield its discovery line's address."""
    proc = subprocess.Popen(
        [sys.executable, _WORKER_SCRIPT, *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        env=_env(),
    )
    lines: queue.Queue[str] = queue.Queue()

    def _pump() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            lines.put(line.strip())

    threading.Thread(target=_pump, daemon=True).start()
    try:
        while True:
            try:
                line = lines.get(timeout=30)
            except queue.Empty:
                pytest.fail(f"worker never printed a {prefix} discovery line")
            if line.startswith(prefix):
                yield line[len(prefix) :]
                return
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


@contextlib.contextmanager
def _over_pipe(protocol: type, *extra_args: str) -> Iterator[Any]:
    """Spawn the worker on stdin/stdout and yield a *protocol* proxy."""
    from vgi_rpc.rpc import RpcConnection

    transport = SubprocessTransport([sys.executable, _WORKER_SCRIPT, *extra_args], stderr=StderrMode.DEVNULL)
    try:
        with RpcConnection(protocol, transport) as proxy:  # type: ignore[var-annotated]
            yield proxy
    finally:
        transport.close()


class TestEveryTransport:
    """The same hosted set — extras then reflection — on each transport."""

    def test_pipe(self) -> None:
        """``Worker.run`` (stdin/stdout) hosts the extras and reflection, and routes to them."""
        with _over_pipe(_REFLECTION, "--quiet") as proxy:
            assert _names(proxy) == _EXPECTED
        with _over_pipe(_ECHO, "--quiet") as echo:
            assert echo.echo(text="hi") == "echo:hi"

    def test_pipe_via_meta_worker(self) -> None:
        """``MetaWorker.serve`` (the fixture worker's path) hosts its children's protocols."""
        with _over_pipe(_REFLECTION, "--meta") as proxy:
            assert _names(proxy) == _EXPECTED
        with _over_pipe(_COUNTER, "--meta") as counter:
            assert counter.count(text="four") == 4

    @_POSIX_ONLY
    def test_unix(self) -> None:
        """``Worker.main --unix`` (the DuckDB launcher path) hosts the same set."""
        sock_dir = tempfile.mkdtemp(prefix="vgihp")
        path = os.path.join(sock_dir, "w.sock")
        with _listening("--quiet", "--unix", path, "--idle-timeout", "30", prefix="UNIX:") as bound:
            with unix_connect(_REFLECTION, bound) as proxy:
                assert _names(proxy) == _EXPECTED
            with unix_connect(_ECHO, bound) as echo:
                assert echo.echo(text="sock") == "echo:sock"

    @_POSIX_ONLY
    def test_unix_via_meta_worker(self) -> None:
        """``MetaWorker.serve --unix`` hosts the same set."""
        sock_dir = tempfile.mkdtemp(prefix="vgihp")
        path = os.path.join(sock_dir, "m.sock")
        with (
            _listening("--meta", "--unix", path, "--idle-timeout", "30", prefix="UNIX:") as bound,
            unix_connect(_REFLECTION, bound) as proxy,
        ):
            assert _names(proxy) == _EXPECTED

    def test_tcp(self) -> None:
        """``Worker.main --tcp`` hosts the same set."""
        with _listening("--quiet", "--tcp", "127.0.0.1:0", "--idle-timeout", "30", prefix="TCP:") as bound:
            host, _, port = bound.rpartition(":")
            with tcp_connect(_REFLECTION, host, int(port)) as proxy:
                assert _names(proxy) == _EXPECTED
            with tcp_connect(_COUNTER, host, int(port)) as counter:
                assert counter.count(text="abc") == 3

    def test_http(self) -> None:
        """``create_app`` hosts the same set over HTTP."""
        from vgi_rpc.http import http_connect
        from vgi_rpc.http._testing import _SyncTestClient

        from vgi.serve import create_app

        app = create_app(HostingWorker, prefix="", describe=True, signing_key=b"hosted-protocols-test")
        client = _SyncTestClient(app)
        with http_connect(_REFLECTION, client=client) as proxy:
            assert _names(proxy) == _EXPECTED
        with http_connect(_ECHO, client=client) as echo:
            assert echo.echo(text="web") == "echo:web"


class TestIdentityPlacement:
    """Identity is framework-owned and hosted only where callers authenticate."""

    def test_http_hosts_identity_last_when_opted_in(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Identity follows reflection, after the worker's extras."""
        from vgi_rpc.http import http_connect
        from vgi_rpc.http._testing import _SyncTestClient

        from vgi.serve import create_app

        monkeypatch.setenv("VGI_INTROSPECT_PRINCIPALS", "proxy-a")
        app = create_app(_IdentityHostingWorker, prefix="", describe=True, signing_key=b"hosted-protocols-test")
        with http_connect(_REFLECTION, client=_SyncTestClient(app)) as proxy:
            assert _names(proxy) == [*_EXPECTED, _IDENTITY]

    def test_http_without_hook_hosts_no_identity(self) -> None:
        """No ``resolve_token``/``mint_grant`` override, no identity."""
        server = build_rpc_server(HostingWorker(quiet=True), transport="http")
        assert _IDENTITY not in server.bindings

    @pytest.mark.parametrize("transport", ["pipe", "unix", "tcp", "iroh"])
    def test_unauthenticated_transports_never_host_identity(
        self, transport: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No identity, and so no allowlist requirement: building must not exit."""
        monkeypatch.delenv("VGI_INTROSPECT_PRINCIPALS", raising=False)
        server = build_rpc_server(_IdentityHostingWorker(quiet=True), transport=transport)
        assert list(server.bindings) == _EXPECTED

    def test_http_identity_still_requires_allowlist(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The refuse-to-start rule is unchanged by the consolidation."""
        monkeypatch.delenv("VGI_INTROSPECT_PRINCIPALS", raising=False)
        with pytest.raises(SystemExit):
            build_rpc_server(_IdentityHostingWorker(quiet=True), transport="http")


class TestHookContract:
    """Called once; default empty; bad answers fail early and name the hook."""

    def test_default_is_empty(self) -> None:
        """A worker that does not override the hook hosts only ``vgi.v2`` and reflection."""
        from tests.test_serve import _SingleWorker

        assert tuple(_SingleWorker.hosted_protocols()) == ()
        server = build_rpc_server(_SingleWorker(quiet=True), transport="pipe")
        assert list(server.bindings) == ["vgi.v2", "vgi_rpc.Reflection.v1"]

    def test_called_once_per_build(self) -> None:
        """The hook is consulted once per server, so its answer is fixed per process."""
        calls: list[int] = []

        class _Counting(HostingWorker):
            @classmethod
            def hosted_protocols(cls) -> Sequence[tuple[type, object]]:
                calls.append(1)
                return super().hosted_protocols()

        build_rpc_server(_Counting(quiet=True), transport="unix")
        assert calls == [1]

    def test_describe_false_drops_only_reflection(self) -> None:
        """``--no-describe`` removes reflection, never the worker's extras."""
        server = build_rpc_server(HostingWorker(quiet=True), transport="http", describe=False)
        assert list(server.bindings) == ["vgi.v2", *_EXTRAS]

    def test_unknown_transport_rejected(self) -> None:
        """A typo in a transport label fails loudly rather than defaulting."""
        with pytest.raises(ValueError, match="Unknown transport"):
            build_rpc_server(HostingWorker(quiet=True), transport="carrier-pigeon")  # type: ignore[arg-type]

    @staticmethod
    def _worker_returning(value: object) -> Worker:
        class _Bad(HostingWorker):
            @classmethod
            def hosted_protocols(cls) -> Any:
                return value

        return _Bad(quiet=True)

    def test_reserved_prefix_points_at_the_identity_hooks(self) -> None:
        """``vgi_rpc.`` is reserved; the message says how identity is enabled."""

        class _Reserved(Protocol):
            protocol_name: ClassVar[str] = "vgi_rpc.Mine.v1"

            def echo(self, text: str) -> str: ...

        worker = self._worker_returning(((_Reserved, EchoImpl()),))
        with pytest.raises(ValueError, match=r"_Bad\.hosted_protocols\(\).*reserved.*resolve_token"):
            build_rpc_server(worker, transport="pipe")

    def test_duplicate_name_rejected(self) -> None:
        """A protocol name is a routing key, so it may appear once."""
        worker = self._worker_returning(((_ECHO, EchoImpl()), (_ECHO, EchoImpl())))
        with pytest.raises(
            ValueError, match=r"_Bad\.hosted_protocols\(\) lists protocol name 'vgi_test\.Echo\.v1' twice"
        ):
            build_rpc_server(worker, transport="pipe")

    def test_worker_own_protocol_name_rejected(self) -> None:
        """An extra may not shadow ``vgi.v2``."""

        class _Shadow(Protocol):
            protocol_name: ClassVar[str] = "vgi.v2"

            def echo(self, text: str) -> str: ...

        worker = self._worker_returning(((_Shadow, EchoImpl()),))
        with pytest.raises(ValueError, match="the worker's own protocol"):
            build_rpc_server(worker, transport="pipe")

    def test_invalid_name_rejected(self) -> None:
        """vgi-rpc's name check runs, and the error names the hook and entry."""

        class _BadName(Protocol):
            protocol_name: ClassVar[str] = "not a name!"

            def echo(self, text: str) -> str: ...

        worker = self._worker_returning(((_BadName, EchoImpl()),))
        with pytest.raises(ValueError, match=r"_Bad\.hosted_protocols\(\) entry 0 \(_BadName\)"):
            build_rpc_server(worker, transport="pipe")

    @pytest.mark.parametrize(
        "value",
        [None, "vgi_test.Echo.v1", [EchoProtocol], [(_ECHO,)], [("EchoProtocol", EchoImpl())]],
    )
    def test_wrong_shape_rejected(self, value: object) -> None:
        """Anything but a sequence of ``(type, object)`` pairs is a ``TypeError``."""
        with pytest.raises(TypeError, match=r"_Bad\.hosted_protocols\(\)"):
            build_rpc_server(self._worker_returning(value), transport="pipe")

    def test_meta_worker_rejects_two_children_hosting_one_name(self) -> None:
        """Two children cannot both host one protocol name."""
        from vgi.meta_worker import MetaWorker

        class _Twin(HostingWorker):
            catalog_name = "twin"

        meta = MetaWorker([HostingWorker(quiet=True), _Twin(quiet=True)])
        with pytest.raises(ValueError, match=r"MetaWorker\.hosted_protocols\(\) lists protocol name"):
            build_rpc_server(meta, transport="pipe")


class TestMetaWorkerIdentity:
    """A ``MetaWorker`` hosts Identity for the one child that implements the hooks."""

    def test_one_child_with_hooks_hosts_identity_on_http(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The child's hook is what the hosted protocol calls."""
        from vgi.meta_worker import MetaWorker

        class _Plain(HostingWorker):
            catalog_name = "plain"

            @classmethod
            def hosted_protocols(cls) -> Sequence[tuple[type, object]]:
                return ()

        monkeypatch.setenv("VGI_INTROSPECT_PRINCIPALS", "proxy-a")
        meta = MetaWorker([_Plain(quiet=True), _IdentityHostingWorker(quiet=True)])
        assert meta._introspect_resolver() is not None
        assert meta._grant_minter() is None
        server = build_rpc_server(meta, transport="http")
        assert list(server.bindings) == [*_EXPECTED, _IDENTITY]
        assert list(build_rpc_server(meta, transport="pipe").bindings) == _EXPECTED

    def test_no_child_with_hooks_hosts_no_identity(self) -> None:
        """Absent, as for a plain worker."""
        from vgi.meta_worker import MetaWorker

        meta = MetaWorker([HostingWorker(quiet=True)])
        assert _IDENTITY not in build_rpc_server(meta, transport="http").bindings

    def test_two_children_with_hooks_refuse_to_start(self) -> None:
        """Two resolvers would answer one credential two ways; refuse rather than pick."""
        from vgi.meta_worker import MetaWorker

        class _OtherIdentity(HostingWorker):
            catalog_name = "other"

            @classmethod
            def hosted_protocols(cls) -> Sequence[tuple[type, object]]:
                return ()

            @classmethod
            def resolve_token(cls, token: str) -> TokenIdentity | None:
                return None

        meta = MetaWorker([_IdentityHostingWorker(quiet=True), _OtherIdentity(quiet=True)])
        with pytest.raises(ValueError, match=r"each implement resolve_token\(\)"):
            build_rpc_server(meta, transport="http")


class TestFixtureWorker:
    """The fixture worker hosts the vgi-rpc reference secondary, and opts into Identity on request."""

    def test_example_worker_hosts_the_secondary(self) -> None:
        """``vgi-fixture-worker`` lists ``vgi.v2`` then ``conformance.Secondary.v1``."""
        from vgi._test_fixtures.worker import ExampleWorker

        server = build_rpc_server(ExampleWorker(quiet=True), transport="pipe")
        assert list(server.bindings)[:2] == ["vgi.v2", "conformance.Secondary.v1"]

    def test_identity_fixture_hosts_identity_with_the_conformance_allowlist(self) -> None:
        """``vgi-fixture-http --identity`` builds Identity without reading the environment."""
        from vgi._test_fixtures.identity_fixture import INTROSPECT_PRINCIPALS, IdentityExampleWorker

        server = build_rpc_server(
            IdentityExampleWorker(quiet=True), transport="http", introspect_principals=INTROSPECT_PRINCIPALS
        )
        assert _IDENTITY in server.bindings


class TestSingleConstructionSite:
    """Structural guard: no transport builds its own ``RpcServer``.

    Absence alone is a weak guard (a transport could stop building a server at
    all), so each entry point is also checked for a ``build_rpc_server`` call
    naming its transport.
    """

    #: Files allowed to call ``RpcServer(...)``: the builder itself, and
    #: servers for protocols other than the worker's.
    _ALLOWED = frozenset({"vgi/rpc_server.py", "vgi/secret_service.py", "vgi/transactor/server.py"})

    #: (file, qualified function) -> transport literal it must pass, or
    #: ``None`` where the transport is computed.
    _ENTRY_POINTS: ClassVar[dict[tuple[str, str], str | None]] = {
        ("vgi/worker.py", "Worker.run"): "pipe",
        ("vgi/worker.py", "Worker.main"): "unix",
        ("vgi/worker.py", "Worker.serve_tcp"): "tcp",
        ("vgi/serve.py", "create_app"): "http",
        ("vgi/serve.py", "main"): "iroh",
        ("vgi/meta_worker.py", "MetaWorker.serve"): None,
        ("vgi/_test_fixtures/http_server.py", "main"): "http",
    }

    @staticmethod
    def _calls(tree: ast.AST, name: str) -> list[ast.Call]:
        return [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and (
                (isinstance(node.func, ast.Name) and node.func.id == name)
                or (isinstance(node.func, ast.Attribute) and node.func.attr == name)
            )
        ]

    @staticmethod
    def _functions(tree: ast.Module) -> dict[str, ast.AST]:
        found: dict[str, ast.AST] = {}

        def visit(node: ast.AST, prefix: str) -> None:
            for child in ast.iter_child_nodes(node):
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    found.setdefault(f"{prefix}{child.name}", child)
                elif isinstance(child, ast.ClassDef):
                    visit(child, f"{prefix}{child.name}.")

        visit(tree, "")
        return found

    def test_no_rpc_server_construction_outside_the_builder(self) -> None:
        """Only the builder (and other-protocol servers) call ``RpcServer``."""
        offenders = []
        for path in sorted((_REPO_ROOT / "vgi").rglob("*.py")):
            rel = path.relative_to(_REPO_ROOT).as_posix()
            if rel in self._ALLOWED:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            offenders += [f"{rel}:{call.lineno}" for call in self._calls(tree, "RpcServer")]
        assert offenders == [], f"build the server with vgi.rpc_server.build_rpc_server instead: {offenders}"

    def test_builder_constructs_exactly_once(self) -> None:
        """The builder itself has one construction site."""
        tree = ast.parse((_REPO_ROOT / "vgi/rpc_server.py").read_text(encoding="utf-8"))
        assert len(self._calls(tree, "RpcServer")) == 1

    @pytest.mark.parametrize(("rel", "qualname"), sorted(_ENTRY_POINTS))
    def test_entry_point_calls_the_builder(self, rel: str, qualname: str) -> None:
        """Each transport entry point builds through the helper, for its transport."""
        tree = ast.parse((_REPO_ROOT / rel).read_text(encoding="utf-8"))
        fn = self._functions(tree).get(qualname)
        assert fn is not None, f"{rel}: {qualname} not found"
        calls = self._calls(fn, "build_rpc_server")
        assert calls, f"{rel}: {qualname} does not call build_rpc_server"
        expected = self._ENTRY_POINTS[(rel, qualname)]
        if expected is not None:
            transports = {
                kw.value.value
                for call in calls
                for kw in call.keywords
                if kw.arg == "transport" and isinstance(kw.value, ast.Constant)
            }
            assert expected in transports, f"{rel}: {qualname} builds for {transports}, expected {expected!r}"
