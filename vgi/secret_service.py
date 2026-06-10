# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Standalone serving harness for the VGI secret protocol (Orchard).

Orchard's secret service is an *independently-deployed microservice* — separate
from the worker/catalog ``vgi-serve`` deployable and speaking
:class:`vgi.secret_protocol.VgiSecretProtocol`. This module provides:

- :func:`create_secret_app` — build a WSGI app for any ``VgiSecretProtocol``
  implementation (usable with gunicorn/waitress/uwsgi).
- :func:`serve_secret_http` — run it under waitress (prints ``PORT:<n>`` for test
  harnesses, mirroring :mod:`vgi.serve`).
- :class:`ExampleOrchardSecretService` — a reference implementation that returns a
  canned ``s3`` credential for ``s3://test-bucket*``; the C++ integration tests
  point ``vgi-secret-serve`` at this class.
- :func:`main` — the ``vgi-secret-serve`` CLI entry point.

Auth: identity is carried by the HTTP bearer. Production deployments wire an
``authenticate`` callback (reuse the ``VGI_BEARER_TOKENS`` / ``VGI_JWT_*`` env
vars supported by :mod:`vgi.serve`). The example service ignores identity.
"""

from __future__ import annotations

import argparse
import importlib
import os
import sys
import time
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from vgi.secret_protocol import SecretLookupResponse, VgiSecretProtocol, encode_secret_values

if TYPE_CHECKING:
    from collections.abc import Callable

    import falcon


# --------------------------------------------------------------------------- #
# Reference implementation (also the integration-test fixture)
# --------------------------------------------------------------------------- #


class ExampleOrchardSecretService:
    """Reference :class:`VgiSecretProtocol` implementation for tests/demos.

    Returns a canned ``s3`` credential for any path under ``s3://test-bucket``
    with a short ``expires_at_unix`` (so the ``min(ttl, expiry)`` cache path is
    exercised) and ``secret`` marked for redaction. Everything else is a miss.
    """

    #: Seconds until the canned credential's intrinsic expiry.
    credential_lifetime_seconds: int = 30

    def secret_lookup(self, path: str, type: str) -> SecretLookupResponse:  # noqa: A002
        if type == "s3" and path.startswith("s3://test-bucket"):
            # Heterogeneous typed values: string, int64, bool, and a nested struct
            # — exercising the full Arrow→DuckDB Value bridge, not just string→string.
            values: dict[str, object] = {
                "key_id": "AKIAEXAMPLEORCHARD",
                "secret": "examplesecretvalue",
                "region": "us-east-1",
                "port": pa.array([9000], pa.int64()),
                "use_ssl": True,
                "endpoint_config": {"connect_timeout_ms": 5000, "max_retries": 3},
            }
            # When VGI_MOCK_S3_ENDPOINT is set, point httpfs at a local mock S3 so
            # a real `SELECT … FROM 's3://…'` read exercises the null-ClientContext
            # system-transaction lookup path end to end.
            mock_endpoint = os.environ.get("VGI_MOCK_S3_ENDPOINT")
            if mock_endpoint:
                values["endpoint"] = mock_endpoint  # host:port, no scheme
                values["use_ssl"] = False
                values["url_style"] = "path"
            return SecretLookupResponse(
                found=True,
                secret_type="s3",
                provider="orchard",
                name="orchard_test_bucket",
                scope=["s3://test-bucket"],
                values=encode_secret_values(values),
                redact_keys=["secret"],
                ttl_seconds=60,
                expires_at_unix=int(time.time()) + self.credential_lifetime_seconds,
            )
        return SecretLookupResponse(found=False)


# --------------------------------------------------------------------------- #
# WSGI app + HTTP server
# --------------------------------------------------------------------------- #


def create_secret_app(
    impl: object,
    *,
    prefix: str = "",
    cors_origins: str = "*",
    signing_key: bytes | None = None,
    authenticate: Callable[[falcon.Request], Any] | None = None,
    oauth_resource_metadata: Any = None,
) -> falcon.App[Any, Any]:
    """Build a WSGI app serving *impl* over :class:`VgiSecretProtocol`.

    *impl* is any object implementing ``secret_lookup(path, type)``. The default
    landing/describe pages are disabled — this is a credential endpoint, not a
    browsable worker.
    """
    try:
        from vgi_rpc.http import make_wsgi_app
    except ImportError:
        sys.stderr.write(
            "Error: HTTP dependencies not installed.\n"
            "Install with: pip install vgi[http]  (or: uv sync --extra http)\n"
        )
        sys.exit(1)

    from vgi_rpc.rpc import RpcServer

    if signing_key is None:
        signing_key = os.urandom(32)

    server = RpcServer(VgiSecretProtocol, impl, enable_describe=False)
    return make_wsgi_app(
        server,
        prefix=prefix,
        cors_origins=cors_origins,
        token_key=signing_key,
        authenticate=authenticate,
        oauth_resource_metadata=oauth_resource_metadata,
        enable_landing_page=False,
        enable_describe_page=False,
    )


def serve_secret_http(
    impl: object,
    *,
    host: str = "0.0.0.0",
    port: int | None = None,
    prefix: str = "",
    cors_origins: str = "*",
    signing_key: bytes | None = None,
    authenticate: Callable[..., Any] | None = None,
    oauth_resource_metadata: Any = None,
) -> None:
    """Serve *impl* over HTTP under waitress. Prints ``PORT:<n>`` once bound."""
    import socket

    try:
        import waitress  # type: ignore[import-untyped]
    except ImportError:
        sys.stderr.write(
            "Error: waitress not installed.\nInstall with: pip install vgi[http]  (or: uv sync --extra http)\n"
        )
        sys.exit(1)

    if port is None:
        env_port = os.environ.get("PORT")
        port = int(env_port) if env_port else 8080
    if port == 0:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((host, 0))
            port = int(s.getsockname()[1])

    wsgi_app = create_secret_app(
        impl,
        prefix=prefix,
        cors_origins=cors_origins,
        signing_key=signing_key,
        authenticate=authenticate,
        oauth_resource_metadata=oauth_resource_metadata,
    )

    print(f"PORT:{port}", flush=True)
    sys.stderr.write(f"Serving {type(impl).__name__} (VgiSecretProtocol) on http://{host}:{port}{prefix}\n")
    sys.stderr.flush()
    waitress.serve(wsgi_app, host=host, port=port, _quiet=True)


def _load_impl(reference: str) -> object:
    """Instantiate a ``VgiSecretProtocol`` implementation from ``module:Class``."""
    if ":" not in reference:
        sys.stderr.write(f"Error: expected 'module:ClassName', got {reference!r}\n")
        sys.exit(1)
    module_ref, class_name = reference.rsplit(":", 1)
    try:
        module = importlib.import_module(module_ref)
    except ImportError as exc:
        sys.stderr.write(f"Error: could not import {module_ref!r}: {exc}\n")
        sys.exit(1)
    cls = getattr(module, class_name, None)
    if cls is None or not isinstance(cls, type):
        sys.stderr.write(f"Error: {class_name!r} not found in {module_ref!r}\n")
        sys.exit(1)
    return cls()


def main() -> None:
    """CLI entry point for ``vgi-secret-serve``."""
    from vgi.serve import _resolve_authenticate, _resolve_oauth_resource_metadata, _resolve_signing_key

    parser = argparse.ArgumentParser(
        prog="vgi-secret-serve",
        description="Serve a VgiSecretProtocol implementation over HTTP (Orchard secret service).",
    )
    parser.add_argument(
        "impl",
        nargs="?",
        default="vgi.secret_service:ExampleOrchardSecretService",
        help="Implementation reference: module:ClassName "
        "(default: the built-in ExampleOrchardSecretService fixture).",
    )
    parser.add_argument("--host", default="0.0.0.0", help="HTTP bind address")
    parser.add_argument("--port", "-p", type=int, default=None, help="HTTP port (default: $PORT or 8080; 0 = ephemeral)")
    parser.add_argument("--prefix", default="", help="URL prefix for RPC endpoints")
    parser.add_argument("--cors-origins", default="*", help="Allowed CORS origins")
    args = parser.parse_args()

    impl = _load_impl(args.impl)
    serve_secret_http(
        impl,
        host=args.host,
        port=args.port,
        prefix=args.prefix,
        cors_origins=args.cors_origins,
        signing_key=_resolve_signing_key(),
        authenticate=_resolve_authenticate(),
        oauth_resource_metadata=_resolve_oauth_resource_metadata(),
    )


if __name__ == "__main__":
    main()
