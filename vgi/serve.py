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


def create_app(
    worker_cls: type[Worker],
    *,
    prefix: str = "/vgi",
    cors_origins: str = "*",
    describe: bool = True,
    signing_key: bytes | None = None,
    log_level: int = logging.INFO,
    authenticate: Callable[[falcon.Request], AuthContext] | None = None,
    oauth_resource_metadata: Any = None,
    otel_config: OtelConfig | None = None,
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

    from vgi.protocol import VgiProtocol

    worker = worker_cls(quiet=True, log_level=log_level)
    server = RpcServer(VgiProtocol, worker, enable_describe=describe)
    wsgi_app = make_wsgi_app(
        server,
        prefix=prefix,
        cors_origins=cors_origins,
        signing_key=signing_key,
        authenticate=authenticate,
        oauth_resource_metadata=oauth_resource_metadata,
        otel_config=otel_config,
    )

    if describe:
        from vgi.http.worker_page import WorkerPageResource, build_worker_page

        worker_page_body = build_worker_page(worker_cls, prefix)
        wsgi_app.add_route(f"{prefix}/worker", WorkerPageResource(worker_page_body))

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
        prefix: str = typer.Option("/vgi", "--prefix", help="URL prefix for RPC endpoints"),
        cors_origins: str = typer.Option("*", "--cors-origins", help="Allowed CORS origins"),
        describe: bool = typer.Option(  # noqa: B008
            True, "--describe/--no-describe", help="Enable description pages (worker + RPC API)"
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
            )
        else:
            worker_cls(quiet=quiet, log_level=effective_level).run()

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
    """Build a jwt_authenticate callback from VGI_JWT_ISSUER + VGI_JWT_AUDIENCE."""
    issuer = os.environ.get("VGI_JWT_ISSUER")
    if not issuer:
        return None

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
    )

    # Machine-readable port for process managers and test harnesses
    print(f"PORT:{port}", flush=True)
    _logger.info("http_server_starting host=%s port=%d prefix=%s", host, port, prefix)
    sys.stderr.write(f"Serving {worker_cls.__name__} on http://{host}:{port}{prefix}\n")
    sys.stderr.flush()

    waitress.serve(wsgi_app, host=host, port=port, _quiet=True)


if __name__ == "__main__":
    main()
