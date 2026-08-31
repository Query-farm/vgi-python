# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Zero-boilerplate CLI for serving VGI workers.

Loads any [`Worker`][] by module reference and serves it — stdio by default
(matching vgi-rpc's ``run_server()``), ``--http`` for cloud deployment.

Usage::

    # Stdio (default) — for subprocess/pipe use by vgi-client or DuckDB
    vgi-serve my_worker.py
    vgi-serve my_app.workers:ProductionWorker

    # HTTP — for cloud deployment
    vgi-serve my_worker.py --http
    vgi-serve my_worker.py --http --host 0.0.0.0 --port 8080

Programmatic API::

    from vgi.serve import create_app, load_worker_class

    app = create_app(load_worker_class("my_app:MyWorker"))
    # Use with gunicorn: gunicorn app -w 4 -b 0.0.0.0:8080
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import logging
import os
import secrets
import sys
from collections.abc import Callable, Iterable, Sequence
from types import ModuleType
from typing import TYPE_CHECKING, Any, Literal

from vgi.logging_config import LogFormat, LogLevel
from vgi.profiling import maybe_start_profile

if TYPE_CHECKING:
    import falcon
    from vgi_rpc.otel import OtelConfig
    from vgi_rpc.rpc import AuthContext, PeerAuthenticationPolicy, PeerIdentityProvider

    from vgi.worker import Worker

_logger = logging.getLogger("vgi.serve")

__all__ = [
    "create_app",
    "load_worker_class",
    "main",
]


def load_worker_class(reference: str) -> type[Worker]:
    """Load a [`Worker`][] subclass from a module reference string.

    Accepts several reference formats:

    - ``module:ClassName`` — import *module* and return *ClassName*
    - ``module`` — import *module* and auto-discover the single `Worker` subclass
    - ``./path/to/file.py`` or ``path.py`` — load from file path
    - ``./path/to/file.py:ClassName`` — load from file path, return *ClassName*

    Auto-discovery finds `Worker` subclasses **defined** in the module (ignores
    imported ones by checking ``__module__``).

    Args:
        reference: Module reference string.

    Returns:
        The `Worker` subclass.

    Raises:
        SystemExit: If the reference is invalid, module can't be loaded,
            no `Worker` subclass is found, or multiple are found.

    """
    from vgi.worker import Worker

    # Split off class name if present
    class_name: str | None = None
    module_ref: str = reference

    if ":" in reference:
        head, tail = reference.rsplit(":", 1)
        # A lone Windows drive-letter colon ("C:\path\worker.py") is part
        # of the path, not a module:Class separator.
        if not (len(head) == 1 and head.isalpha() and tail[:1] in ("\\", "/")):
            module_ref, class_name = head, tail

    # Load the module
    module = _load_module(module_ref)

    if class_name is not None:
        obj = getattr(module, class_name, None)
        if obj is None:
            sys.stderr.write(f"Error: {class_name!r} not found in {module_ref!r}\n")
            sys.exit(1)
        if not (isinstance(obj, type) and issubclass(obj, Worker) and obj is not Worker):
            sys.stderr.write(f"Error: {class_name!r} in {module_ref!r} is not a Worker subclass\n")
            sys.exit(1)
        return obj

    # Auto-discover: find Worker subclasses defined in this module
    candidates: list[type[Worker]] = []
    for name in dir(module):
        obj = getattr(module, name)
        if (
            isinstance(obj, type)
            and issubclass(obj, Worker)
            and obj is not Worker
            and obj.__module__ == module.__name__
        ):
            candidates.append(obj)

    if len(candidates) == 0:
        sys.stderr.write(f"Error: no Worker subclass found in {module_ref!r}\n")
        sys.exit(1)
    if len(candidates) > 1:
        names = ", ".join(c.__name__ for c in candidates)
        sys.stderr.write(
            f"Error: multiple Worker subclasses found in {module_ref!r}: {names}\n"
            f"Specify one with {module_ref}:ClassName\n"
        )
        sys.exit(1)

    return candidates[0]


def _load_module(module_ref: str) -> ModuleType:
    """Import a module by dotted name or file path."""
    # File path: ends with .py or starts with ./ or /
    if module_ref.endswith(".py") or module_ref.startswith(("./", "/")):
        path = os.path.abspath(module_ref)
        if not os.path.isfile(path):
            sys.stderr.write(f"Error: file not found: {path}\n")
            sys.exit(1)

        # Derive a module name from the file name
        mod_name = os.path.basename(path).removesuffix(".py")
        spec = importlib.util.spec_from_file_location(mod_name, path)
        if spec is None or spec.loader is None:
            sys.stderr.write(f"Error: could not load module from {path}\n")
            sys.exit(1)

        module = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = module
        spec.loader.exec_module(module)
        return module

    # Dotted module name
    try:
        return importlib.import_module(module_ref)
    except ImportError as exc:
        sys.stderr.write(f"Error: could not import {module_ref!r}: {exc}\n")
        sys.exit(1)


def _make_frontend_redirect(frontend_url: str, prefix: str) -> object:
    """Create a Falcon resource that redirects to the external frontend."""
    import html as _html

    # Build the redirect HTML — the service URL is injected at request time
    # so it adapts to the actual host/port the server is running on.
    _redirect_template = (
        "<!DOCTYPE html><html><head>"
        '<meta http-equiv="refresh" content="0;url={redirect_url}">'
        "</head><body>"
        'Redirecting to <a href="{redirect_url}">VGI Frontend</a>...'
        "</body></html>"
    )
    _frontend_base = frontend_url.rstrip("/")
    _prefix = prefix

    class _FrontendRedirectResource:
        def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
            scheme = req.forwarded_scheme or req.scheme
            host = req.forwarded_host or req.host
            service_url = f"{scheme}://{host}{_prefix}"
            redirect_url = f"{_frontend_base}?service={service_url}"
            # Pass auth token in URL fragment so the frontend can use it
            # for cross-origin API calls (cookie is bound to this origin).
            token = req.cookies.get("_vgi_auth")
            if token:
                redirect_url += f"#token={token}"
            resp.status = "302 Found"
            resp.set_header("Location", redirect_url)
            resp.set_header("Cache-Control", "no-cache")
            resp.content_type = "text/html; charset=utf-8"
            resp.text = _redirect_template.format(redirect_url=_html.escape(redirect_url))

    return _FrontendRedirectResource()


def create_app(
    worker_cls: type[Worker],
    *,
    prefix: str = "",
    cors_origins: str = "*",
    describe: bool = True,
    signing_key: bytes | None = None,
    log_level: int = logging.INFO,
    authenticate: Callable[[falcon.Request], AuthContext] | None = None,
    peer_identity_providers: Sequence[PeerIdentityProvider] = (),
    peer_authentication_policy: PeerAuthenticationPolicy | None = None,
    peer_service_name: str | None = None,
    peer_resolution_timeout: float = 5.0,
    peer_provider_concurrency: int = 64,
    proxy_proof_required: bool | None = None,
    oauth_resource_metadata: Any = None,
    otel_config: OtelConfig | None = None,
    max_stream_response_bytes: int | None = None,
    max_externalized_response_bytes: int | None = None,
    introspect_principals: Iterable[str] | None = None,
    introspect_rate_limit: int | None = None,
) -> falcon.App[Any, Any]:
    """Create a WSGI app for a VGI worker.

    Returns a standard WSGI app usable with gunicorn, uwsgi, waitress, or
    any WSGI server.

    Args:
        worker_cls: The [`Worker`][] subclass to serve.
        prefix: URL prefix for RPC endpoints.
        cors_origins: Allowed CORS origins.
        describe: Enable worker + API description pages.
        signing_key: Shared signing key for state tokens.  When ``None``,
            a random per-process key is generated (tokens are invalid
            across workers).  Set via ``VGI_SIGNING_KEY`` env var or
            pass explicitly for multi-process deployments.
        log_level: Logging level for the worker instance.
        authenticate: Optional callback that validates each HTTP request
            and returns an `AuthContext`. When ``None``, all requests are
            anonymous.
        peer_identity_providers: Transport identity evidence providers. Their
            immutable results are exposed on each call context independently
            of application authentication.
        peer_authentication_policy: Optional policy that composes peer
            evidence with the result of ``authenticate``. A policy may
            preserve, replace, or reject the request authentication.
        peer_service_name: Logical destination supplied to providers for
            destination-scoped identity or capability evidence.
        peer_resolution_timeout: Total provider-resolution deadline per HTTP
            request, in seconds.
        peer_provider_concurrency: Maximum active provider callbacks for this
            application, including callbacks that ignore cancellation.
        proxy_proof_required: Whether to advertise ``VGI-Proxy-Proof-Required``
            so a proxy can confirm this worker actually enforces the proof it
            mints. ``None`` (the default) derives it from
            ``VGI_PROXY_PROOF_MODE``, which is also where the gate itself comes
            from — so the advertisement cannot drift from the posture. Pass a
            bool only when supplying a hand-built gate via ``authenticate``.
        oauth_resource_metadata: Optional `OAuthResourceMetadata` for
            RFC 9728 discovery endpoint.
        otel_config: Optional OpenTelemetry configuration.  When provided,
            instruments the RPC server with tracing and/or metrics.
        max_stream_response_bytes: HTTP-only.  When set, producer stream
            responses may pack multiple Arrow batches into a single HTTP
            response up to this byte budget before emitting a continuation
            token.  Default ``None`` keeps the current one-batch-per-response
            behaviour.
        max_externalized_response_bytes: HTTP-only.  Cap on a single
            *externalized* response — the payload uploaded to blob storage and
            replaced on the wire by a pointer.  Set it to whatever a load
            balancer, API gateway or object-store policy in front of this
            worker will actually carry.  Unlike ``max_stream_response_bytes``
            this is a hard cap on every method type with no continuation
            escape, because bytes already uploaded cannot be un-uploaded.
            ``None`` (the default) means no cap.
        introspect_principals: Principals permitted to call
            ``__introspect_token__``.  Only consulted when the worker class
            overrides ``resolve_token``.  ``None`` reads
            ``VGI_INTROSPECT_PRINCIPALS``.
        introspect_rate_limit: Introspection requests allowed per caller per
            second.  ``None`` reads ``VGI_INTROSPECT_RATE_LIMIT``, defaulting
            to 20.

    Returns:
        A Falcon WSGI application.

    """
    try:
        from vgi_rpc.http import make_wsgi_app
    except ImportError:
        sys.stderr.write(
            "Error: HTTP dependencies not installed.\nInstall with: pip install vgi[http]  (or: uv sync --extra http)\n"
        )
        sys.exit(1)

    from vgi_rpc.rpc import RpcServer

    from vgi.otel import VgiTracer
    from vgi.protocol import VgiProtocol

    # Resolve the signing key once, here, so the worker (which seals catalog
    # opaque-data envelopes) and the HTTP state-token machinery share the same
    # key. See resolve_shared_signing_key for why every process serving this
    # deployment has to agree on it.
    if signing_key is None:
        signing_key, is_ephemeral = resolve_shared_signing_key(propagate_to_children=False)
        _warn_if_ephemeral_signing_key(is_ephemeral=is_ephemeral, multiprocess=False)

    if proxy_proof_required is None:
        proxy_proof_required = (os.environ.get("VGI_PROXY_PROOF_MODE") or "").strip().lower() == "require"

    worker = worker_cls(quiet=True, log_level=log_level)
    worker._vgi_tracer = VgiTracer.create(otel_config)
    worker._signing_key = signing_key
    from vgi.worker import _get_vgi_version

    server = RpcServer(VgiProtocol, worker, enable_describe=describe, server_version=_get_vgi_version())

    # Absent unless the worker class actually implements the lookup. Passing
    # ``None`` leaves the route unrouted rather than routed-and-refusing, which
    # is what keeps a dependency upgrade from growing a credential oracle.
    introspect_resolver = worker_cls._introspect_resolver()
    introspect_kwargs: dict[str, Any] = {}
    if introspect_resolver is not None:
        introspect_kwargs = {
            "introspect_resolver": introspect_resolver,
            "introspect_principals": _resolve_introspect_principals(introspect_principals),
            "introspect_rate_limit": _resolve_introspect_rate_limit(introspect_rate_limit),
        }

    wsgi_app = make_wsgi_app(
        server,
        prefix=prefix,
        cors_origins=cors_origins,
        token_key=signing_key,
        authenticate=authenticate,
        peer_identity_providers=peer_identity_providers,
        peer_authentication_policy=peer_authentication_policy,
        peer_service_name=peer_service_name,
        peer_resolution_timeout=peer_resolution_timeout,
        peer_provider_concurrency=peer_provider_concurrency,
        proxy_proof_required=proxy_proof_required,
        oauth_resource_metadata=oauth_resource_metadata,
        otel_config=otel_config,
        max_stream_response_bytes=max_stream_response_bytes,
        max_externalized_response_bytes=max_externalized_response_bytes,
        enable_landing_page=False,
        **introspect_kwargs,
    )

    # Frontend: either redirect to external CDN or serve pre-rendered worker page
    frontend_url = os.environ.get("VGI_FRONTEND_URL")
    if frontend_url:
        # External frontend — redirect to CDN with ?service= param
        _FrontendRedirectResource = _make_frontend_redirect(frontend_url, prefix)
        wsgi_app.add_route(prefix or "/", _FrontendRedirectResource)
    elif describe:
        # Standardized landing surface: the shared static page + its JSON
        # contract. The page (identical across all language workers) reads the
        # ``_vgi_identity`` cookie itself, so no server-side HTML injection is
        # needed.
        from vgi.http.landing_page import ClientBundleResource, LandingPageResource

        oauth_active = (
            oauth_resource_metadata is not None and getattr(oauth_resource_metadata, "client_id", None) is not None
        )
        server_id = getattr(server, "server_id", "")
        wsgi_app.add_route(prefix or "/", LandingPageResource(worker_cls, server_id=server_id, oauth=oauth_active))
        wsgi_app.add_route(f"{prefix}/vgi-client.js", ClientBundleResource())

    return wsgi_app


def main() -> None:
    """CLI entry point for ``vgi-serve``."""
    import typer

    from vgi.logging_config import configure_worker_logging

    app = typer.Typer(
        add_completion=False,
        help="Serve a VGI worker. Stdio by default, --http for cloud deployment.",
    )

    @app.command()
    def serve(
        worker_ref: str = typer.Argument(help="Worker reference: module:Class, module, or ./file.py"),
        # Transport
        http: bool = typer.Option(False, "--http", help="Serve over HTTP instead of stdin/stdout"),
        server: str = typer.Option(
            "waitress",
            "--server",
            help=(
                "HTTP server: 'waitress' (default, pure Python) or 'granian' "
                "(Rust I/O off the GIL; needs the [granian] extra)."
            ),
        ),
        http_threads: int | None = typer.Option(
            None,
            "--http-threads",
            help=(
                "Waitress worker threads. The request path is CPU-bound Python, so more "
                "threads contend for the GIL rather than adding throughput; raise this only "
                "for functions that block on external I/O."
            ),
        ),
        http_workers: int | None = typer.Option(
            None,
            "--http-workers",
            help=(
                "Worker processes (granian only). Each is a separate interpreter, so "
                "memory scales with it. Requires VGI_SIGNING_KEY when >1 unless vgi-serve "
                "mints one for them."
            ),
        ),
        quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress startup banner (stdio mode)"),
        # Logging
        debug: bool = typer.Option(False, "--debug", help="Enable DEBUG on all vgi + vgi_rpc loggers"),
        log_level: LogLevel = typer.Option(LogLevel.INFO, "--log-level", help="Set log level"),  # noqa: B008
        log_logger: list[str] | None = typer.Option(  # noqa: B008
            None, "--log-logger", help="Target specific logger(s)"
        ),
        log_format: LogFormat = typer.Option(  # noqa: B008
            LogFormat.text, "--log-format", help="Stderr log format"
        ),
        # HTTP-only options
        host: str = typer.Option("0.0.0.0", "--host", help="HTTP bind address"),
        port: int | None = typer.Option(None, "--port", "-p", help="HTTP port (default: $PORT or 8080)"),  # noqa: B008
        prefix: str = typer.Option("", "--prefix", help="URL prefix for RPC endpoints"),
        cors_origins: str = typer.Option("*", "--cors-origins", help="Allowed CORS origins"),
        describe: bool = typer.Option(  # noqa: B008
            True, "--describe/--no-describe", help="Enable description pages (worker + RPC API)"
        ),
        max_stream_response_bytes: int | None = typer.Option(  # noqa: B008
            None,
            "--max-stream-response-bytes",
            help=(
                "HTTP-only. When set, producer-stream responses pack multiple "
                "Arrow batches into a single HTTP body up to this byte budget "
                "before emitting a continuation token. Default: one batch per response."
            ),
        ),
        max_externalized_response_bytes: int | None = typer.Option(  # noqa: B008
            None,
            "--max-externalized-response-bytes",
            help=(
                "HTTP-only. Cap on a single externalized response — the payload "
                "uploaded to blob storage and replaced on the wire by a pointer. "
                "Size it to what the load balancer or gateway in front of this "
                "worker will carry. Hard on every method type, with no continuation "
                "escape, because uploaded bytes cannot be un-uploaded. Default: no cap."
            ),
        ),
        introspect_principals: str | None = typer.Option(  # noqa: B008
            None,
            "--introspect-principals",
            help=(
                "Comma-separated principals permitted to call __introspect_token__. "
                "Only used when the worker implements resolve_token(); required in "
                "that case, with no permissive default. Env: VGI_INTROSPECT_PRINCIPALS."
            ),
        ),
        introspect_rate_limit: int | None = typer.Option(  # noqa: B008
            None,
            "--introspect-rate-limit",
            help=(
                "Introspection requests allowed per caller per second (default 20). "
                "Bounds, rather than closes, the oracle an allowlisted-but-compromised "
                "caller has. Env: VGI_INTROSPECT_RATE_LIMIT."
            ),
        ),
        access_log_sample: float | None = typer.Option(  # noqa: B008
            None,
            "--access-log-sample",
            help=(
                "Fraction of successful calls to keep in the access log (0.0-1.0). "
                "Errors are always kept, and the decision is made per call so every "
                "record of one stream shares a fate. Env: VGI_WORKER_ACCESS_LOG_SAMPLE."
            ),
        ),
        access_log_async: bool = typer.Option(
            False,
            "--access-log-async",
            help=(
                "Write access-log records from a background thread so disk latency "
                "stays out of the request path. The queue is bounded and full means "
                "drop; a crash loses whatever is queued. Env: VGI_WORKER_ACCESS_LOG_ASYNC."
            ),
        ),
        access_log_queue_size: int | None = typer.Option(  # noqa: B008
            None,
            "--access-log-queue-size",
            help=(
                "Bound on the async access-log queue (default 10000). Ignored unless "
                "--access-log-async. Env: VGI_WORKER_ACCESS_LOG_QUEUE_SIZE."
            ),
        ),
    ) -> None:
        env_debug = os.environ.get("VGI_WORKER_DEBUG", "").lower() in ("1", "true", "yes")
        effective_debug = debug or env_debug
        # VGI_WORKER_LOG_* overrides are applied inside configure_worker_logging,
        # so every worker entry point honours them identically.
        effective_level = configure_worker_logging(
            debug=effective_debug,
            log_level=log_level,
            log_loggers=log_logger,
            log_format=log_format,
            access_log_sample=access_log_sample,
            access_log_async=access_log_async,
            access_log_queue_size=access_log_queue_size,
        )
        # Before the worker class is imported, so module-import cost lands in
        # the profile too — for many workers that is the bulk of startup.
        maybe_start_profile()

        # Resolve env var overrides
        describe = _resolve_describe(describe)
        signing_key = _resolve_signing_key()

        # Initialise Sentry before constructing any RpcServer so that
        # vgi-rpc's auto-attach hook picks up the SDK.
        _maybe_init_sentry()

        worker_cls = load_worker_class(worker_ref)

        if http:
            authenticate = _resolve_authenticate()
            oauth_metadata = _resolve_oauth_resource_metadata()
            otel_config = _resolve_otel_config()
            if server not in ("waitress", "granian"):
                sys.exit(f"--server must be 'waitress' or 'granian', got {server!r}")
            if http_workers is not None and server != "granian":
                sys.exit("--http-workers only applies to --server granian")
            _serve_http(
                worker_cls,
                effective_level=effective_level,
                host=host,
                port=port,
                prefix=prefix,
                cors_origins=cors_origins,
                describe=describe,
                signing_key=signing_key,
                authenticate=authenticate,
                oauth_resource_metadata=oauth_metadata,
                otel_config=otel_config,
                max_stream_response_bytes=max_stream_response_bytes,
                max_externalized_response_bytes=max_externalized_response_bytes,
                introspect_principals=introspect_principals,
                introspect_rate_limit=introspect_rate_limit,
                server=server,
                worker_ref=worker_ref,
                http_workers=http_workers,
                http_threads=http_threads,
            )
        else:
            otel_config = _resolve_otel_config()
            worker_cls(quiet=quiet, log_level=effective_level).run(otel_config=otel_config)

    app()


SIGNING_KEY_ENV = "VGI_SIGNING_KEY"


def _resolve_signing_key() -> bytes | None:
    """Read ``VGI_SIGNING_KEY`` from the environment."""
    raw = os.environ.get(SIGNING_KEY_ENV)
    if raw:
        return raw.encode()
    return None


def resolve_shared_signing_key(*, propagate_to_children: bool) -> tuple[bytes, bool]:
    """Resolve the signing key every process in this deployment must agree on.

    The key seals HTTP state tokens and catalog opaque data. Every process
    that might serve a continuation for a stream has to hold the *same* one:
    a token sealed by one key fails the AEAD check under another, and the
    failure is load-dependent rather than deterministic. A client whose
    connection stays pinned to one process never notices; one that reconnects
    mid-stream -- ``seek_to_token``, a load balancer, a respawned worker --
    hits an intermittent 400 that looks like flakiness.

    Resolution:

    - ``VGI_SIGNING_KEY`` set: use it. Tokens survive restarts and are valid
      across every process configured with the same value. This is the only
      correct setting for a load-balanced or multi-instance deployment,
      because nothing here can reach a peer we did not start.
    - Unset: mint a random key for this deployment. Tokens are then valid for
      the life of these processes and clients re-ATTACH after a restart.

    Args:
        propagate_to_children: True when this process will start worker
            processes that import the app themselves (a pre-fork server).
            A minted key is then exported to the environment so those
            children inherit it instead of each minting its own -- which is
            the bug this function exists to prevent.

    Returns:
        ``(key, is_ephemeral)``. ``is_ephemeral`` is True when the key was
        minted here rather than configured, so callers can say so out loud.

    """
    configured = _resolve_signing_key()
    if configured is not None:
        return configured, False

    minted = secrets.token_urlsafe(32)
    if propagate_to_children:
        # Children inherit os.environ, so exporting before we start them is
        # what makes them agree. Same channel the operator would use, so this
        # adds no exposure they did not already have by setting it.
        os.environ[SIGNING_KEY_ENV] = minted
    return minted.encode(), True


def _warn_if_ephemeral_signing_key(*, is_ephemeral: bool, multiprocess: bool) -> None:
    """Say plainly when state tokens will not be portable.

    Silence here is what makes the failure mode expensive: an operator who
    scales to two instances gets a server that works until a request lands
    on the wrong one.
    """
    if not is_ephemeral:
        return
    if multiprocess:
        _logger.info(
            "No %s configured; minted one for this deployment and exported it to worker processes. "
            "State tokens are valid until restart. Set %s to keep them valid across restarts.",
            SIGNING_KEY_ENV,
            SIGNING_KEY_ENV,
        )
        return
    _logger.warning(
        "No %s configured; using a per-process key. State tokens and catalog handles are valid only "
        "for this process. If you run more than one instance behind a load balancer, set %s to the "
        "same value in every one -- otherwise a client that reconnects mid-stream gets an "
        "intermittent 400.",
        SIGNING_KEY_ENV,
        SIGNING_KEY_ENV,
    )


def _resolve_describe(cli_value: bool) -> bool:
    """Apply ``VGI_ENABLE_DESCRIBE`` env var override.

    The env var only takes effect when it is explicitly set.  Accepts
    ``1``/``true``/``yes`` (enable) and ``0``/``false``/``no`` (disable),
    case-insensitive.  The CLI flag (``--describe`` / ``--no-describe``)
    wins when Typer reports a non-default value, but since we cannot
    distinguish "user passed --describe" from "default True", the env var
    always overrides when present.
    """
    raw = os.environ.get("VGI_ENABLE_DESCRIBE")
    if raw is None:
        return cli_value
    return raw.lower() in ("1", "true", "yes")


def _resolve_authenticate() -> Callable[..., Any] | None:
    """Build an authenticate callback from environment variables.

    Supported env vars:

    - ``VGI_BEARER_TOKENS``: comma-separated ``token=principal`` pairs
      for static bearer token auth.
    - ``VGI_JWT_ISSUER`` + ``VGI_JWT_AUDIENCE``: JWT/JWKS auth
      (requires ``vgi[oauth]`` extra). Optional ``VGI_JWT_JWKS_URI``.
    - When both bearer and JWT are set, they are chained (JWT first).
    - ``VGI_PROXY_PROOF_MODE``: ``allow`` or ``require`` gates every request
      on proof that it arrived through a trusted proxy. This is a
      precondition ANDed with whichever credential above is configured, not
      an alternative to one.

    Returns:
        An authenticate callback, or None if no auth env vars are set.

    Raises:
        SystemExit: If env vars are malformed (e.g. bearer token without ``=``,
            JWT issuer without audience).

    """
    # Ordered by cost: JWT resolves a signature, bearer does a constant-time
    # scan over the token set, so the cheaper one goes last.
    candidates = [fn for fn in (_resolve_jwt_authenticate(), _resolve_bearer_authenticate()) if fn is not None]

    inner: Callable[..., Any] | None
    if len(candidates) > 1:
        from vgi_rpc.http import chain_authenticate

        inner = chain_authenticate(*candidates)
    else:
        inner = candidates[0] if candidates else None

    gate = _resolve_proxy_proof_gate()
    if gate is None:
        return inner

    # AND, not OR: chaining the gate would let any later credential satisfy
    # the request on its own, which is exactly the bypass the gate exists to
    # close. `inner` may be None — proof alone means "only my proxy may call
    # this worker", with user identity handled upstream.
    from vgi_rpc.http import require_all

    return require_all(gate, inner)


def _resolve_introspect_principals(explicit: Iterable[str] | None) -> list[str]:
    """Resolve the introspector allowlist, or exit with an actionable message.

    Env var: ``VGI_INTROSPECT_PRINCIPALS``, comma-separated.

    Fail-closed and loud rather than defaulting to "any authenticated caller":
    authenticating and introspecting are different capabilities, and a
    permissive default lets any user resolve any other user's credential to
    its owner. A worker that implements ``resolve_token`` and forgets the
    allowlist must not start.

    Args:
        explicit: Principals passed to :func:`create_app`, or None to read
            the environment.

    Returns:
        The allowlist, guaranteed non-empty.

    Raises:
        SystemExit: When neither source names a principal.

    """
    if explicit is not None:
        principals = [p.strip() for p in explicit if p.strip()]
    else:
        raw = os.environ.get("VGI_INTROSPECT_PRINCIPALS") or ""
        principals = [p.strip() for p in raw.split(",") if p.strip()]

    if not principals:
        sys.stderr.write(
            "Error: this worker implements resolve_token(), which enables the\n"
            "  POST /__introspect_token__ endpoint, but no introspector allowlist\n"
            "  was configured. Set VGI_INTROSPECT_PRINCIPALS (comma-separated) or\n"
            "  pass --introspect-principals.\n"
            "\n"
            "  There is no permissive default on purpose: introspection is a\n"
            "  separate capability from authentication, and allowing every\n"
            "  authenticated caller lets any user resolve any other user's\n"
            "  credential to its owner. Remove resolve_token() to disable the\n"
            "  endpoint entirely.\n"
        )
        sys.exit(1)
    return principals


def _resolve_introspect_rate_limit(explicit: int | None) -> int:
    """Resolve introspection requests allowed per caller per second.

    Env var: ``VGI_INTROSPECT_RATE_LIMIT``; defaults to 20.

    Args:
        explicit: Value passed to :func:`create_app`, or None to read the
            environment.

    Returns:
        A positive per-caller, per-second request ceiling.

    Raises:
        SystemExit: When the environment value is not a positive integer. A
            typo that silently became 0 would refuse every introspection with
            no diagnostic; one that became huge would remove the bound on an
            allowlisted-but-compromised caller's guessing rate.

    """
    if explicit is not None:
        value = explicit
    else:
        raw = os.environ.get("VGI_INTROSPECT_RATE_LIMIT")
        if not raw:
            return 20
        try:
            value = int(raw)
        except ValueError:
            sys.stderr.write(f"Error: VGI_INTROSPECT_RATE_LIMIT must be an integer, got {raw!r}\n")
            sys.exit(1)
    if value <= 0:
        sys.stderr.write(f"Error: introspection rate limit must be positive, got {value}\n")
        sys.exit(1)
    return value


def _resolve_proxy_proof_gate() -> Any | None:
    """Build a proxy-proof gate from ``VGI_PROXY_PROOF_*`` environment variables.

    Env vars:

    - ``VGI_PROXY_PROOF_MODE``: ``off`` (default), ``allow`` or ``require``.
    - ``VGI_PROXY_PROOF_ORIGIN_ID``: this worker's identifier. Folded into
      every MAC but never transmitted, so it must match what the proxy is
      configured to prove to.
    - ``VGI_PROXY_PROOF_SECRETS``: ``kid:hex`` pairs, comma-separated. The
      ``kid`` doubles as the proxy's label in the audit trail.
    - ``VGI_PROXY_PROOF_SKEW``: acceptance half-window in seconds (default 30).

    Returns:
        A gate for ``require_all``, or None when the feature is off.

    Raises:
        SystemExit: On any malformed value. Deliberately fail-closed: the
            secret is shared with an independently-deployed proxy, so a typo
            would otherwise silently reject every request with no diagnostic.

    """
    raw_mode = (os.environ.get("VGI_PROXY_PROOF_MODE") or "off").strip().lower()
    if raw_mode == "off":
        return None
    if raw_mode not in ("allow", "require"):
        sys.stderr.write(
            f"Error: VGI_PROXY_PROOF_MODE must be 'off', 'allow' or 'require', got {raw_mode!r}\n",
        )
        sys.exit(1)
    mode: Literal["allow", "require"] = "allow" if raw_mode == "allow" else "require"

    from vgi_rpc.http import ProxyProofConfig, parse_secrets, proxy_proof_gate

    raw_secrets = os.environ.get("VGI_PROXY_PROOF_SECRETS") or ""
    skew_raw = os.environ.get("VGI_PROXY_PROOF_SKEW") or "30"
    try:
        secrets = parse_secrets(raw_secrets)
        config = ProxyProofConfig(
            mode=mode,
            origin_id=os.environ.get("VGI_PROXY_PROOF_ORIGIN_ID") or "",
            secrets=secrets,
            skew_seconds=int(skew_raw),
        )
    except ValueError as exc:
        sys.stderr.write(
            f"Error: invalid proxy-proof configuration: {exc}\n"
            "Set VGI_PROXY_PROOF_MODE=off to disable, or fix "
            "VGI_PROXY_PROOF_ORIGIN_ID / VGI_PROXY_PROOF_SECRETS "
            "(kid:hex pairs, 64 hex chars each; generate with 'openssl rand -hex 32').\n",
        )
        sys.exit(1)
    return proxy_proof_gate(config)


def _resolve_bearer_authenticate() -> Callable[..., Any] | None:
    """Build a bearer_authenticate_static callback from VGI_BEARER_TOKENS.

    Format: ``token=principal`` pairs separated by commas.  Each entry is
    split on the *first* ``=`` only, so principals may contain ``=``
    (e.g. base64-encoded values).  However, tokens themselves **must not**
    contain ``=`` or ``,`` because those characters are used as delimiters.
    """
    raw = os.environ.get("VGI_BEARER_TOKENS")
    if not raw:
        return None

    from vgi_rpc.http import bearer_authenticate_static
    from vgi_rpc.rpc import AuthContext

    tokens: dict[str, AuthContext] = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if "=" not in entry:
            sys.stderr.write(
                f"Error: malformed VGI_BEARER_TOKENS entry: {entry!r}\n"
                "Expected format: token=principal (e.g. 'mytoken=alice')\n"
            )
            sys.exit(1)
        token, principal = entry.split("=", 1)
        tokens[token] = AuthContext(principal=principal, authenticated=True, domain="bearer")

    if not tokens:
        return None
    return bearer_authenticate_static(tokens=tokens)


def _resolve_jwt_authenticate() -> Callable[..., Any] | None:
    """Build a jwt_authenticate callback from VGI_JWT_ISSUER + VGI_JWT_AUDIENCE.

    ``VGI_JWT_ISSUER`` may be a single issuer URL or a comma-separated list
    for multi-tenant setups (e.g. Microsoft Entra with multiple tenants).
    """
    issuer_raw = os.environ.get("VGI_JWT_ISSUER")
    if not issuer_raw:
        return None

    issuers = tuple(s.strip() for s in issuer_raw.split(",") if s.strip())
    if not issuers:
        sys.stderr.write("Error: VGI_JWT_ISSUER is set but contains no valid values\n")
        sys.exit(1)

    audience_raw = os.environ.get("VGI_JWT_AUDIENCE")
    if not audience_raw:
        sys.stderr.write("Error: VGI_JWT_ISSUER is set but VGI_JWT_AUDIENCE is missing\n")
        sys.exit(1)

    audiences = tuple(s.strip() for s in audience_raw.split(",") if s.strip())
    if not audiences:
        sys.stderr.write("Error: VGI_JWT_AUDIENCE is set but contains no valid values\n")
        sys.exit(1)

    try:
        from vgi_rpc.http._oauth_jwt import jwt_authenticate
    except ImportError:
        sys.stderr.write(
            "Error: JWT auth requires the oauth extra.\n"
            "Install with: pip install vgi[oauth]  (or: uv sync --extra oauth)\n"
        )
        sys.exit(1)

    jwks_uri = os.environ.get("VGI_JWT_JWKS_URI")
    # Pass a single string when only one issuer (backwards compatible),
    # or a tuple when multiple issuers are configured.
    issuer: str | tuple[str, ...] = issuers[0] if len(issuers) == 1 else issuers
    return jwt_authenticate(issuer=issuer, audience=audiences, jwks_uri=jwks_uri)


def _resolve_oauth_resource_metadata() -> Any:
    """Build OAuthResourceMetadata from environment variables.

    Supported env vars:

    - ``VGI_OAUTH_RESOURCE``: canonical resource URL (required to enable).
    - ``VGI_OAUTH_AUTH_SERVERS``: comma-separated authorization server URLs.
    - ``VGI_OAUTH_SCOPES``: comma-separated supported scopes (optional).
    - ``VGI_OAUTH_RESOURCE_NAME``: human-readable name (optional).
    - ``VGI_OAUTH_CLIENT_ID``: client ID for MCP compatibility (optional, URL-safe chars only).
    - ``VGI_OAUTH_DEVICE_CODE_CLIENT_ID``: client ID for device-code flow (optional, URL-safe chars only).
    - ``VGI_OAUTH_DEVICE_CODE_CLIENT_SECRET``: client secret for device-code flow (optional, URL-safe chars only).
    - ``VGI_OAUTH_USE_ID_TOKEN``: when set to ``1``/``true``/``yes``, tells clients
      to use the OIDC ``id_token`` as Bearer instead of the ``access_token``.

    Returns:
        `OAuthResourceMetadata` instance, or None if not configured.

    """
    resource = os.environ.get("VGI_OAUTH_RESOURCE")
    if not resource:
        return None

    auth_servers_raw = os.environ.get("VGI_OAUTH_AUTH_SERVERS")
    if not auth_servers_raw:
        sys.stderr.write("Error: VGI_OAUTH_RESOURCE is set but VGI_OAUTH_AUTH_SERVERS is missing\n")
        sys.exit(1)

    try:
        from vgi_rpc.http import OAuthResourceMetadata
    except ImportError:
        sys.stderr.write(
            "Error: OAuth metadata requires the http extra.\n"
            "Install with: pip install vgi[http]  (or: uv sync --extra http)\n"
        )
        sys.exit(1)

    auth_servers = tuple(s.strip() for s in auth_servers_raw.split(",") if s.strip())
    scopes_raw = os.environ.get("VGI_OAUTH_SCOPES")
    scopes = tuple(s.strip() for s in scopes_raw.split(",") if s.strip()) if scopes_raw else ()
    resource_name = os.environ.get("VGI_OAUTH_RESOURCE_NAME")
    client_id = os.environ.get("VGI_OAUTH_CLIENT_ID")
    client_secret = os.environ.get("VGI_OAUTH_CLIENT_SECRET")
    device_code_client_id = os.environ.get("VGI_OAUTH_DEVICE_CODE_CLIENT_ID")
    device_code_client_secret = os.environ.get("VGI_OAUTH_DEVICE_CODE_CLIENT_SECRET")
    use_id_token = os.environ.get("VGI_OAUTH_USE_ID_TOKEN", "").lower() in ("1", "true", "yes")

    try:
        return OAuthResourceMetadata(
            resource=resource,
            authorization_servers=auth_servers,
            scopes_supported=scopes,
            resource_name=resource_name,
            client_id=client_id,
            client_secret=client_secret,
            device_code_client_id=device_code_client_id,
            device_code_client_secret=device_code_client_secret,
            use_id_token_as_bearer=use_id_token,
        )
    except ValueError as exc:
        sys.stderr.write(f"Error: invalid OAuth config: {exc}\n")
        sys.exit(1)


def _maybe_init_sentry() -> None:
    """Initialise ``sentry_sdk`` from environment when ``SENTRY_DSN`` is set.

    Reads the standard Sentry env vars (``SENTRY_DSN``, ``SENTRY_ENVIRONMENT``,
    ``SENTRY_RELEASE``, ``SENTRY_TRACES_SAMPLE_RATE``) and calls
    ``sentry_sdk.init()`` so that ``vgi-rpc``'s auto-attach hook in
    ``RpcServer.__init__`` picks up Sentry instrumentation.

    Silent no-op when ``SENTRY_DSN`` is unset or ``vgi[sentry]`` is not
    installed.
    """
    if not os.environ.get("SENTRY_DSN"):
        return
    try:
        import sentry_sdk
    except ImportError:
        sys.stderr.write(
            "Warning: SENTRY_DSN is set but sentry-sdk is not installed.\n"
            "Install with: pip install vgi[sentry]  (or: uv sync --extra sentry)\n"
        )
        return

    if sentry_sdk.is_initialized():
        return

    init_kwargs: dict[str, Any] = {}
    environment = os.environ.get("SENTRY_ENVIRONMENT")
    if environment:
        init_kwargs["environment"] = environment
    release = os.environ.get("SENTRY_RELEASE")
    if not release:
        # Fall back to the installed vgi package version so non-deploy runs
        # still get a Sentry release tag (Sentry's UI degrades when release
        # is unset).  Production deploys should set SENTRY_RELEASE to a git
        # SHA or tag for commit tracking.
        try:
            from importlib.metadata import PackageNotFoundError, version

            release = version("vgi-python")
        except PackageNotFoundError:
            release = None
    if release:
        init_kwargs["release"] = release
    sample_raw = os.environ.get("SENTRY_TRACES_SAMPLE_RATE")
    if sample_raw:
        try:
            init_kwargs["traces_sample_rate"] = float(sample_raw)
        except ValueError:
            sys.stderr.write(f"Error: SENTRY_TRACES_SAMPLE_RATE must be a float, got {sample_raw!r}\n")
            sys.exit(1)
    sentry_sdk.init(**init_kwargs)


def _resolve_otel_config() -> Any:
    """Build an ``OtelConfig`` from environment variables.

    Supported env vars:

    - ``VGI_OTEL_ENABLED``: enable OTEL (``1``/``true``/``yes``).
    - ``VGI_OTEL_CUSTOM_ATTRIBUTES``: comma-separated ``key=value`` pairs.
    - ``VGI_OTEL_CLAIM_ATTRIBUTES``: comma-separated ``claim_key=span_attr_name`` pairs.
    - ``VGI_OTEL_DISABLE_TRACING``: disable tracing only (``1``/``true``/``yes``).
    - ``VGI_OTEL_DISABLE_METRICS``: disable metrics only (``1``/``true``/``yes``).

    Returns:
        `OtelConfig` instance, or None if not enabled.

    """
    enabled = os.environ.get("VGI_OTEL_ENABLED", "").lower() in ("1", "true", "yes")
    if not enabled:
        return None

    try:
        from vgi_rpc.otel import OtelConfig
    except ImportError:
        sys.stderr.write(
            "Error: OTEL support requires the otel extra.\n"
            "Install with: pip install vgi[otel]  (or: uv sync --extra otel)\n"
        )
        sys.exit(1)

    custom_attributes: dict[str, str] = {}
    raw_custom = os.environ.get("VGI_OTEL_CUSTOM_ATTRIBUTES", "")
    if raw_custom:
        for entry in raw_custom.split(","):
            entry = entry.strip()
            if not entry:
                continue
            if "=" not in entry:
                sys.stderr.write(
                    f"Error: malformed VGI_OTEL_CUSTOM_ATTRIBUTES entry: {entry!r}\n"
                    "Expected format: key=value (e.g. 'deployment=prod')\n"
                )
                sys.exit(1)
            key, value = entry.split("=", 1)
            custom_attributes[key.strip()] = value.strip()

    claim_attributes: dict[str, str] = {}
    raw_claims = os.environ.get("VGI_OTEL_CLAIM_ATTRIBUTES", "")
    if raw_claims:
        for entry in raw_claims.split(","):
            entry = entry.strip()
            if not entry:
                continue
            if "=" not in entry:
                sys.stderr.write(
                    f"Error: malformed VGI_OTEL_CLAIM_ATTRIBUTES entry: {entry!r}\n"
                    "Expected format: claim_key=span_attr_name (e.g. 'tenant_id=rpc.vgi_rpc.auth.claim.tenant_id')\n"
                )
                sys.exit(1)
            key, value = entry.split("=", 1)
            claim_attributes[key.strip()] = value.strip()

    disable_tracing = os.environ.get("VGI_OTEL_DISABLE_TRACING", "").lower() in ("1", "true", "yes")
    disable_metrics = os.environ.get("VGI_OTEL_DISABLE_METRICS", "").lower() in ("1", "true", "yes")

    return OtelConfig(
        enable_tracing=not disable_tracing,
        enable_metrics=not disable_metrics,
        custom_attributes=custom_attributes,
        claim_attributes=claim_attributes,
    )


# ---------------------------------------------------------------------------
# Pre-fork support: rebuilding the app in a worker process
#
# waitress takes the app *object* we hand it. A pre-fork server (granian) takes
# an import path and builds the app inside each forked worker, which means the
# configuration cannot travel as Python objects -- only whatever the child
# inherits. The environment is that channel.
#
# This is a private contract between ``vgi-serve`` and its own workers, not a
# user-facing knob, so it is one opaque blob rather than a family of documented
# env vars. Secrets stay out of it: the signing key travels in VGI_SIGNING_KEY
# (see resolve_shared_signing_key), and auth/OAuth/OTel settings are already
# resolved from the environment by their own helpers, so a child reconstructs
# them exactly as the parent did.
# ---------------------------------------------------------------------------

SERVE_CONFIG_ENV = "VGI_SERVE_CONFIG"


def export_serve_config(
    *,
    worker_ref: str,
    prefix: str,
    cors_origins: str,
    describe: bool,
    log_level: int,
    max_stream_response_bytes: int | None,
    max_externalized_response_bytes: int | None,
) -> None:
    """Publish the parent's serve configuration for worker processes to read.

    Args:
        worker_ref: The worker reference string the parent was given
            (``module:Class``, ``module``, or ``./file.py``). Passed rather
            than the resolved class because a child has to import it itself.
        prefix: URL prefix for RPC endpoints.
        cors_origins: Allowed CORS origins.
        describe: Whether to enable the worker + API description pages.
        log_level: Logging level for the worker instance.
        max_stream_response_bytes: Producer-stream response budget, or None.
        max_externalized_response_bytes: Externalized-response cap, or None.

    """
    os.environ[SERVE_CONFIG_ENV] = json.dumps(
        {
            "worker_ref": worker_ref,
            "prefix": prefix,
            "cors_origins": cors_origins,
            "describe": describe,
            "log_level": log_level,
            "max_stream_response_bytes": max_stream_response_bytes,
            "max_externalized_response_bytes": max_externalized_response_bytes,
        }
    )


def wsgi_app_factory() -> Any:
    """Build the WSGI app from the environment — the pre-fork worker entry point.

    Granian is pointed at ``vgi.serve:wsgi_app_factory`` and calls this once
    per worker process. Everything it needs was published by
    :func:`export_serve_config` in the parent, plus ``VGI_SIGNING_KEY``, which
    the parent minted and exported so every worker seals state tokens with the
    same key.

    Returns:
        The Falcon WSGI application.

    Raises:
        RuntimeError: Called without a parent having exported the config,
            which means this was invoked directly rather than by ``vgi-serve``.

    """
    raw = os.environ.get(SERVE_CONFIG_ENV)
    if not raw:
        msg = (
            f"{SERVE_CONFIG_ENV} is not set. vgi.serve:wsgi_app_factory is the worker entry point "
            f"for `vgi-serve --http --server granian`; it cannot be started on its own."
        )
        raise RuntimeError(msg)
    config = json.loads(raw)

    # Resolved here, not inherited as objects: each worker re-reads the same
    # environment the parent did and arrives at the same answer.
    return create_app(
        load_worker_class(config["worker_ref"]),
        prefix=config["prefix"],
        cors_origins=config["cors_origins"],
        describe=config["describe"],
        signing_key=None,  # picked up from VGI_SIGNING_KEY
        log_level=config["log_level"],
        authenticate=_resolve_authenticate(),
        oauth_resource_metadata=_resolve_oauth_resource_metadata(),
        otel_config=_resolve_otel_config(),
        max_stream_response_bytes=config["max_stream_response_bytes"],
        max_externalized_response_bytes=config["max_externalized_response_bytes"],
    )


def _resolve_http_port(host: str, port: int | None) -> int:
    """Resolve the listen port: explicit ``--port`` > ``$PORT`` > 8080.

    ``--port 0`` binds a throwaway socket to let the OS pick, then reports the
    concrete number — process managers and test harnesses read it off stdout.
    """
    import socket

    if port is None:
        env_port = os.environ.get("PORT")
        port = int(env_port) if env_port else 8080
    if port == 0:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((host, 0))
            port = int(sock.getsockname()[1])
    return port


def _serve_http_granian(
    worker_cls: type[Worker],
    *,
    host: str,
    port: int,
    prefix: str,
    cors_origins: str,
    describe: bool,
    effective_level: int,
    max_stream_response_bytes: int | None,
    max_externalized_response_bytes: int | None,
    worker_ref: str | None,
    http_workers: int | None,
    http_threads: int | None,
) -> None:
    """Serve via granian, which forks workers that import the app themselves.

    Granian moves HTTP parsing and socket I/O into Rust, off the GIL. On this
    workload that measured 1.45x over waitress at a single worker -- the win
    comes from the I/O leaving the interpreter, not from process fan-out, so
    it does not require paying for extra interpreters.

    Two things follow from granian building the app per worker rather than
    accepting ours:

    - The configuration has to travel through the environment. See
      :func:`export_serve_config`.
    - Every worker must seal state tokens with the *same* key, or a client
      that reconnects mid-stream gets an intermittent 400. The parent mints
      and exports one here; :func:`resolve_shared_signing_key` explains why.

    Args:
        worker_cls: The resolved worker class -- used only for the startup
            banner; each granian worker imports its own from ``worker_ref``.
        host: Bind address.
        port: Bind port, already resolved.
        prefix: URL prefix for RPC endpoints.
        cors_origins: Allowed CORS origins.
        describe: Whether to enable the description pages.
        effective_level: Logging level for the worker instance.
        max_stream_response_bytes: Producer-stream response budget, or None.
        max_externalized_response_bytes: Externalized-response cap, or None.
        worker_ref: The worker reference string, required here because the
            children import it rather than inheriting a class object.
        http_workers: Number of worker processes; ``None`` means 1. These are
            separate interpreters, so memory scales with the count.
        http_threads: Python threads per worker; ``None`` means 1.

    """
    try:
        from granian import Granian
        from granian.constants import Interfaces
    except ImportError:
        sys.stderr.write(
            "Error: granian not installed.\nInstall with: pip install 'vgi[granian]'  (or: uv sync --extra granian)\n"
        )
        sys.exit(1)

    if not worker_ref:
        msg = "granian requires the worker reference string so each worker process can import it"
        raise RuntimeError(msg)

    workers = http_workers or 1
    # Mint before starting children so they inherit one key rather than each
    # generating its own. Only meaningful when we actually fork.
    _key, is_ephemeral = resolve_shared_signing_key(propagate_to_children=workers > 1)
    _warn_if_ephemeral_signing_key(is_ephemeral=is_ephemeral, multiprocess=workers > 1)

    export_serve_config(
        worker_ref=worker_ref,
        prefix=prefix,
        cors_origins=cors_origins,
        describe=describe,
        log_level=effective_level,
        max_stream_response_bytes=max_stream_response_bytes,
        max_externalized_response_bytes=max_externalized_response_bytes,
    )

    print(f"PORT:{port}", flush=True)
    _logger.info(
        "http_server_starting server=granian host=%s port=%d prefix=%s workers=%d",
        host,
        port,
        prefix,
        workers,
    )
    sys.stderr.write(f"Serving {worker_cls.__name__} on http://{host}:{port}{prefix} (granian, {workers} worker(s))\n")
    sys.stderr.flush()

    Granian(
        "vgi.serve:wsgi_app_factory",
        address=host,
        port=port,
        interface=Interfaces.WSGI,
        workers=workers,
        # Python threads inside each worker, sharing that worker's GIL --
        # distinct from `workers`, which are separate interpreters. Defaults
        # to 1 because granian already runs socket I/O in Rust, so the only
        # thing these threads overlap is app code, and for a CPU-bound
        # request path that is pure contention: measured 1387 turns/s at one
        # thread against 851 at two and 362 at eight. Functions that block on
        # external I/O want more; --http-threads raises it.
        blocking_threads=http_threads or 1,
        factory=True,
    ).serve()


def _serve_http(
    worker_cls: type[Worker],
    *,
    effective_level: int,
    host: str,
    port: int | None,
    prefix: str,
    cors_origins: str,
    describe: bool,
    signing_key: bytes | None,
    authenticate: Callable[..., Any] | None = None,
    oauth_resource_metadata: Any = None,
    otel_config: Any = None,
    max_stream_response_bytes: int | None = None,
    max_externalized_response_bytes: int | None = None,
    introspect_principals: str | None = None,
    introspect_rate_limit: int | None = None,
    server: str = "waitress",
    worker_ref: str | None = None,
    http_workers: int | None = None,
    http_threads: int | None = None,
) -> None:
    """Start the worker as an HTTP server."""
    port = _resolve_http_port(host, port)

    # Republished as environment rather than threaded through as arguments, so
    # a granian child — which rebuilds the app itself and inherits only the
    # environment — resolves the same allowlist the parent was given. See
    # export_serve_config for why the environment is the channel.
    if introspect_principals is not None:
        os.environ["VGI_INTROSPECT_PRINCIPALS"] = introspect_principals
    if introspect_rate_limit is not None:
        os.environ["VGI_INTROSPECT_RATE_LIMIT"] = str(introspect_rate_limit)

    if server == "granian":
        _serve_http_granian(
            worker_cls,
            host=host,
            port=port,
            prefix=prefix,
            cors_origins=cors_origins,
            describe=describe,
            effective_level=effective_level,
            max_stream_response_bytes=max_stream_response_bytes,
            max_externalized_response_bytes=max_externalized_response_bytes,
            worker_ref=worker_ref,
            http_workers=http_workers,
            http_threads=http_threads,
        )
        return

    try:
        import waitress  # type: ignore[import-untyped]
    except ImportError:
        sys.stderr.write(
            "Error: waitress not installed.\nInstall with: pip install vgi[http]  (or: uv sync --extra http)\n"
        )
        sys.exit(1)

    wsgi_app = create_app(
        worker_cls,
        prefix=prefix,
        cors_origins=cors_origins,
        describe=describe,
        signing_key=signing_key,
        log_level=effective_level,
        authenticate=authenticate,
        oauth_resource_metadata=oauth_resource_metadata,
        otel_config=otel_config,
        max_stream_response_bytes=max_stream_response_bytes,
        max_externalized_response_bytes=max_externalized_response_bytes,
    )

    # Machine-readable port for process managers and test harnesses
    print(f"PORT:{port}", flush=True)
    _logger.info("http_server_starting host=%s port=%d prefix=%s", host, port, prefix)
    sys.stderr.write(f"Serving {worker_cls.__name__} on http://{host}:{port}{prefix}\n")
    sys.stderr.flush()

    # Same Arrow-sized buffers the other two serve paths use. Without them
    # waitress spools multi-MiB bodies through temp files and reads sockets in
    # 8 KiB chunks; measured on this path, tuning is worth ~2x.
    from vgi_rpc.http.server._serve import _tame_queue_depth_logger, waitress_arrow_tuning

    _tame_queue_depth_logger()
    serve_kwargs: dict[str, Any] = {"host": host, "port": port, "_quiet": True}
    serve_kwargs.update(waitress_arrow_tuning())
    if http_threads is not None:
        serve_kwargs["threads"] = http_threads
    waitress.serve(wsgi_app, **serve_kwargs)


if __name__ == "__main__":
    main()
