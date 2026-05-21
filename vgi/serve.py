# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Zero-boilerplate CLI for serving VGI workers.

Loads any Worker by module reference and serves it — stdio by default
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
import logging
import os
import sys
from collections.abc import Callable
from types import ModuleType
from typing import TYPE_CHECKING, Any

from vgi.logging_config import LogFormat, LogLevel

if TYPE_CHECKING:
    import falcon
    from vgi_rpc.otel import OtelConfig
    from vgi_rpc.rpc import AuthContext

    from vgi.worker import Worker

_logger = logging.getLogger("vgi.serve")

__all__ = [
    "create_app",
    "load_worker_class",
    "main",
]


def load_worker_class(reference: str) -> type[Worker]:
    """Load a Worker subclass from a module reference string.

    Accepts several reference formats:

    - ``module:ClassName`` — import *module* and return *ClassName*
    - ``module`` — import *module* and auto-discover the single Worker subclass
    - ``./path/to/file.py`` or ``path.py`` — load from file path
    - ``./path/to/file.py:ClassName`` — load from file path, return *ClassName*

    Auto-discovery finds Worker subclasses **defined** in the module (ignores
    imported ones by checking ``__module__``).

    Args:
        reference: Module reference string.

    Returns:
        The Worker subclass.

    Raises:
        SystemExit: If the reference is invalid, module can't be loaded,
            no Worker subclass is found, or multiple are found.

    """
    from vgi.worker import Worker

    # Split off class name if present
    class_name: str | None = None
    module_ref: str = reference

    if ":" in reference:
        module_ref, class_name = reference.rsplit(":", 1)

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
    oauth_resource_metadata: Any = None,
    otel_config: OtelConfig | None = None,
    max_stream_response_bytes: int | None = None,
) -> falcon.App[Any, Any]:
    """Create a WSGI app for a VGI worker.

    Returns a standard WSGI app usable with gunicorn, uwsgi, waitress, or
    any WSGI server.

    Args:
        worker_cls: The Worker subclass to serve.
        prefix: URL prefix for RPC endpoints.
        cors_origins: Allowed CORS origins.
        describe: Enable worker + API description pages.
        signing_key: Shared signing key for state tokens.  When ``None``,
            a random per-process key is generated (tokens are invalid
            across workers).  Set via ``VGI_SIGNING_KEY`` env var or
            pass explicitly for multi-process deployments.
        log_level: Logging level for the worker instance.
        authenticate: Optional callback that validates each HTTP request
            and returns an AuthContext. When ``None``, all requests are
            anonymous.
        oauth_resource_metadata: Optional OAuthResourceMetadata for
            RFC 9728 discovery endpoint.
        otel_config: Optional OpenTelemetry configuration.  When provided,
            instruments the RPC server with tracing and/or metrics.
        max_stream_response_bytes: HTTP-only.  When set, producer stream
            responses may pack multiple Arrow batches into a single HTTP
            response up to this byte budget before emitting a continuation
            token.  Default ``None`` keeps the current one-batch-per-response
            behaviour.

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
    # key. When the operator did not configure VGI_SIGNING_KEY, generate an
    # ephemeral per-process key: sealed values are then valid for the life of
    # this process and clients re-ATTACH after a restart.
    if signing_key is None:
        signing_key = os.urandom(32)

    worker = worker_cls(quiet=True, log_level=log_level)
    worker._vgi_tracer = VgiTracer.create(otel_config)
    worker._signing_key = signing_key
    from vgi.worker import _get_vgi_version

    server = RpcServer(VgiProtocol, worker, enable_describe=describe, server_version=_get_vgi_version())
    wsgi_app = make_wsgi_app(
        server,
        prefix=prefix,
        cors_origins=cors_origins,
        token_key=signing_key,
        authenticate=authenticate,
        oauth_resource_metadata=oauth_resource_metadata,
        otel_config=otel_config,
        max_stream_response_bytes=max_stream_response_bytes,
        enable_landing_page=False,
    )

    # Frontend: either redirect to external CDN or serve pre-rendered worker page
    frontend_url = os.environ.get("VGI_FRONTEND_URL")
    if frontend_url:
        # External frontend — redirect to CDN with ?service= param
        _FrontendRedirectResource = _make_frontend_redirect(frontend_url, prefix)
        wsgi_app.add_route(prefix or "/", _FrontendRedirectResource)
    elif describe:
        from vgi.http.worker_page import WorkerPageResource

        # Inject PKCE user-info JS if OAuth PKCE is active.
        body_transform = None
        if oauth_resource_metadata is not None and getattr(oauth_resource_metadata, "client_id", None) is not None:
            try:
                from vgi_rpc.http._oauth_pkce import build_user_info_html

                user_info_html = build_user_info_html(prefix).encode()

                def body_transform(body: bytes, _html: bytes = user_info_html) -> bytes:
                    return body.replace(b"</body>", _html + b"\n</body>")
            except ImportError:
                pass
        wsgi_app.add_route(prefix or "/", WorkerPageResource(worker_cls, prefix, body_transform))

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
    ) -> None:
        env_debug = os.environ.get("VGI_WORKER_DEBUG", "").lower() in ("1", "true", "yes")
        effective_debug = debug or env_debug
        effective_level = configure_worker_logging(
            debug=effective_debug,
            log_level=log_level,
            log_loggers=log_logger,
            log_format=log_format,
        )

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
            )
        else:
            otel_config = _resolve_otel_config()
            worker_cls(quiet=quiet, log_level=effective_level).run(otel_config=otel_config)

    app()


def _resolve_signing_key() -> bytes | None:
    """Read ``VGI_SIGNING_KEY`` from the environment."""
    raw = os.environ.get("VGI_SIGNING_KEY")
    if raw:
        return raw.encode()
    return None


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

    Returns:
        An authenticate callback, or None if no auth env vars are set.

    Raises:
        SystemExit: If env vars are malformed (e.g. bearer token without ``=``,
            JWT issuer without audience).

    """
    bearer_auth = _resolve_bearer_authenticate()
    jwt_auth = _resolve_jwt_authenticate()

    if bearer_auth is not None and jwt_auth is not None:
        from vgi_rpc.http import chain_authenticate

        return chain_authenticate(jwt_auth, bearer_auth)
    return jwt_auth or bearer_auth


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
        OAuthResourceMetadata instance, or None if not configured.

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

            release = version("vgi")
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
        OtelConfig instance, or None if not enabled.

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
) -> None:
    """Start the worker as an HTTP server."""
    import socket

    try:
        import waitress  # type: ignore[import-untyped]
    except ImportError:
        sys.stderr.write(
            "Error: waitress not installed.\nInstall with: pip install vgi[http]  (or: uv sync --extra http)\n"
        )
        sys.exit(1)

    # Port resolution: explicit --port > $PORT env var > 8080
    if port is None:
        env_port = os.environ.get("PORT")
        port = int(env_port) if env_port else 8080

    if port == 0:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((host, 0))
            port = int(s.getsockname()[1])

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
    )

    # Machine-readable port for process managers and test harnesses
    print(f"PORT:{port}", flush=True)
    _logger.info("http_server_starting host=%s port=%d prefix=%s", host, port, prefix)
    sys.stderr.write(f"Serving {worker_cls.__name__} on http://{host}:{port}{prefix}\n")
    sys.stderr.flush()

    waitress.serve(wsgi_app, host=host, port=port, _quiet=True)


if __name__ == "__main__":
    main()
