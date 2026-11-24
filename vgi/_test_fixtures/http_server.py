# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Run the example worker as an HTTP server.

Usage::

    vgi-fixture-http
    vgi-fixture-http --port 9000
    vgi-fixture-http --host 0.0.0.0 --port 8080 --debug
    vgi-fixture-http --s3-bucket rusty-vgi-test
    vgi-fixture-http --demo-storage

Requires the ``http`` extra: ``pip install vgi-python[http]``
For S3 offload support, also install: ``pip install vgi-rpc[s3]``
"""

from __future__ import annotations

import os
import sys
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from vgi_rpc.external_fetch import DEFAULT_MAX_FETCH_BYTES, FetchConfig

from vgi._test_fixtures.accumulate.worker import AccumulateWorker
from vgi._test_fixtures.projection_repro.worker import ProjReproWorker
from vgi._test_fixtures.schema_reconcile.worker import SchemaReconcileWorker
from vgi._test_fixtures.worker import ExampleWorker
from vgi.logging_config import LogFormat, LogLevel, configure_worker_logging
from vgi.meta_worker import MetaWorker
from vgi.profiling import maybe_start_profile

if TYPE_CHECKING:
    from vgi_rpc.external import UploadUrl


class _SigV4S3Storage:
    """S3 external storage that forces SigV4 presigned URLs."""

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        region_name: str | None,
        endpoint_url: str | None,
        presign_expiry_seconds: int = 3600,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self.presign_expiry_seconds = presign_expiry_seconds
        self._client = None

    def _get_client(self) -> Any:
        if self._client is None:
            import boto3  # type: ignore[import-not-found,import-untyped,unused-ignore]
            from botocore.config import Config  # type: ignore[import-not-found,import-untyped,unused-ignore]

            self._client = boto3.client(
                "s3",
                region_name=self.region_name,
                endpoint_url=self.endpoint_url,
                config=Config(signature_version="s3v4"),
            )
        return self._client

    def upload(self, data: bytes, schema: Any, *, content_encoding: str | None = None) -> str:
        import uuid

        client = self._get_client()
        ext = ".arrow.zst" if content_encoding == "zstd" else ".arrow"
        key = f"{self.prefix}{uuid.uuid4().hex}{ext}"

        put_kwargs = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": data,
            "ContentType": "application/octet-stream",
        }
        if content_encoding is not None:
            put_kwargs["ContentEncoding"] = content_encoding
        client.put_object(**put_kwargs)

        url: str = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=self.presign_expiry_seconds,
        )
        return url

    def generate_upload_url(self, size: int, sha256_hex: str) -> UploadUrl:
        """Presign a PUT bound to exactly *size* bytes hashing to *sha256_hex* (upload-URL v2)."""
        import uuid

        from vgi_rpc.external import UploadUrl, sha256_b64_from_hex

        checksum = sha256_b64_from_hex(sha256_hex)
        client = self._get_client()
        key = f"{self.prefix}{uuid.uuid4().hex}.arrow"
        params = {"Bucket": self.bucket, "Key": key}
        expires_at = datetime.now(UTC) + timedelta(seconds=self.presign_expiry_seconds)

        # SigV4 signs content-length and x-amz-checksum-sha256 as headers.
        put_url = client.generate_presigned_url(
            "put_object",
            Params={**params, "ContentLength": size, "ChecksumSHA256": checksum},
            ExpiresIn=self.presign_expiry_seconds,
        )
        get_url = client.generate_presigned_url(
            "get_object",
            Params=params,
            ExpiresIn=self.presign_expiry_seconds,
        )
        return UploadUrl(
            upload_url=put_url,
            download_url=get_url,
            expires_at=expires_at,
            headers={"x-amz-checksum-sha256": checksum},
            # Protocol 3.4: the server seals the key into the grant's argument
            # reference and fetches it back with get().
            object_id=key,
        )

    def put_object(self, data: bytes) -> str:
        """Store *data* under a fresh key, size- and SHA-256-checked by S3 (``ObjectStore``)."""
        import base64
        import hashlib
        import uuid

        key = f"{self.prefix}{uuid.uuid4().hex}.bin"
        self._get_client().put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            ContentLength=len(data),
            ChecksumSHA256=base64.b64encode(hashlib.sha256(data).digest()).decode("ascii"),
            ContentType="application/octet-stream",
        )
        return key

    def get(self, object_id: str, *, max_bytes: int | None = None) -> bytes:
        """Fetch the object at key *object_id* (``ObjectStore``).

        Args:
            object_id: The object key.
            max_bytes: Largest object accepted; ``None`` means 256 MiB.

        Returns:
            The object bytes.

        Raises:
            ObjectMissingError: S3 reports ``NoSuchKey`` / 404.
            ValueError: The object is larger than *max_bytes* (default 256 MiB).

        """
        from vgi_rpc.external import ObjectMissingError

        limit = max_bytes if max_bytes is not None else DEFAULT_MAX_FETCH_BYTES
        try:
            resp = self._get_client().get_object(Bucket=self.bucket, Key=object_id)
        except Exception as exc:
            error = getattr(exc, "response", None)
            code = error.get("Error", {}).get("Code") if isinstance(error, dict) else None
            if code in ("NoSuchKey", "404", "NotFound"):
                raise ObjectMissingError("S3 object is gone") from None
            raise
        body = resp["Body"]
        try:
            length = resp.get("ContentLength")
            if isinstance(length, int) and length > limit:
                raise ValueError(f"S3 object is {length} bytes, over max_fetch_bytes ({limit})")
            data: bytes = body.read(limit + 1)
        finally:
            body.close()
        if len(data) > limit:
            raise ValueError(f"S3 object exceeds max_fetch_bytes ({limit})")
        return data


def _upload_fetch_config(max_upload_bytes: int) -> FetchConfig:
    """Inbound fetch config able to fetch back the uploads this server advertises.

    ``make_wsgi_app`` refuses a ``max_upload_bytes`` above the server's fetch
    cap (an upload it would then refuse to fetch), so ``--max-upload-bytes``
    raises the cap from its 256 MiB default, as ``VGI_RPC_MAX_UPLOAD_BYTES``
    does for the blob vendor.

    Args:
        max_upload_bytes: The advertised upload cap.

    Returns:
        A fetch config whose ``max_fetch_bytes`` is at least *max_upload_bytes*.

    """
    return FetchConfig(max_fetch_bytes=max(DEFAULT_MAX_FETCH_BYTES, max_upload_bytes))


def main() -> None:
    """Run the fixture worker as an HTTP server.

    When ``--s3-bucket`` is provided (or ``VGI_HTTP_S3_BUCKET`` is set),
    response batches larger than ``--externalize-threshold-bytes`` are uploaded
    to S3 and replaced by signed URLs on the wire.
    """
    # The test fixture server is single-process and ephemeral — there's no
    # value in fsyncing every commit through the WAL. Default to in-memory
    # storage unless the caller explicitly picked a path. Must run before
    # any Function.storage access so _DefaultStorageDescriptor caches the
    # right backend; main() runs before the WSGI app starts handling RPCs.
    os.environ.setdefault("VGI_WORKER_SQLITE_PATH", ":memory:")

    import typer

    app = typer.Typer(add_completion=False)

    @app.command()
    def _run(
        host: str = typer.Option("127.0.0.1", "--host", "-h", help="Bind address"),
        port: int = typer.Option(0, "--port", "-p", help="Bind port (0 = auto-select)"),
        prefix: str = typer.Option("", "--prefix", help="URL prefix for RPC endpoints"),
        cors_origins: str = typer.Option("*", "--cors-origins", help="Allowed CORS origins"),
        describe: bool = typer.Option(  # noqa: B008
            True, "--describe/--no-describe", help="Enable description pages (worker + RPC API)"
        ),
        debug: bool = typer.Option(False, "--debug", help="Enable DEBUG on all vgi + vgi_rpc loggers"),
        log_level: LogLevel = typer.Option(LogLevel.INFO, "--log-level", help="Set log level"),  # noqa: B008
        log_logger: list[str] | None = typer.Option(None, "--log-logger", help="Target specific logger(s)"),  # noqa: B008
        log_format: LogFormat = typer.Option(LogFormat.text, "--log-format", help="Stderr log format"),  # noqa: B008
        s3_bucket: str | None = typer.Option(
            None,
            "--s3-bucket",
            help="S3 bucket for externalized payloads (or env VGI_HTTP_S3_BUCKET)",
        ),
        s3_prefix: str = typer.Option(
            "vgi-http/",
            "--s3-prefix",
            help="S3 key prefix for externalized payloads",
        ),
        s3_region: str | None = typer.Option(None, "--s3-region", help="AWS region for S3 client"),
        s3_endpoint_url: str | None = typer.Option(
            None,
            "--s3-endpoint-url",
            help="Custom S3 endpoint URL (for MinIO/LocalStack)",
        ),
        externalize_threshold_bytes: int = typer.Option(
            4 * 1024 * 1024,
            "--externalize-threshold-bytes",
            help="Externalize batches larger than this many bytes (default: 4 MiB)",
        ),
        max_upload_bytes: int = typer.Option(
            4 * 1024 * 1024,
            "--max-upload-bytes",
            help=(
                "Advertise max direct HTTP upload bytes for server-vended upload URLs; with "
                "--s3-bucket/--demo-storage the inbound fetch cap is raised to match"
            ),
        ),
        externalize_compression: str = typer.Option(
            "none",
            "--externalize-compression",
            help="Compression for externalized batches: none, zstd, or gzip",
        ),
        demo_storage: bool = typer.Option(
            False,
            "--demo-storage",
            help="Enable in-process blob storage for externalized payloads (no S3 required)",
        ),
        max_externalized_response_bytes: int | None = typer.Option(  # noqa: B008
            None,
            "--max-externalized-response-bytes",
            help=(
                "Cap on a single externalized response. Hard on every method type, "
                "with no continuation escape. Default: no cap."
            ),
        ),
        identity: bool = typer.Option(
            False,
            "--identity",
            envvar="VGI_FIXTURE_IDENTITY",
            help=(
                "Host vgi_rpc.Identity.v2 with the vgi-rpc conformance fixture policy and its "
                "spoofable X-Conformance-Principal header authentication. Test-only; for "
                "vgi-rpc-test-hosted --identity."
            ),
        ),
        port_file: str | None = typer.Option(
            None,
            "--port-file",
            help=(
                "Write the bound port number (one line, no prefix) to this file before starting "
                "to serve. For test harnesses / process managers that need the port side-channel "
                "without parsing stdout."
            ),
        ),
    ) -> None:
        try:
            from vgi_rpc import Compression, ExternalLocationConfig
            from vgi_rpc.http import make_wsgi_app
        except ImportError:
            sys.stderr.write(
                "Error: HTTP dependencies not installed.\n"
                "Install with: pip install vgi-python[http]  (or: uv sync --extra http)\n"
            )
            sys.exit(1)

        try:
            import waitress  # type: ignore[import-untyped]
        except ImportError:
            sys.stderr.write(
                "Error: waitress not installed.\n"
                "Install with: pip install vgi-python[http]  (or: uv sync --extra http)\n"
            )
            sys.exit(1)

        env_debug = os.environ.get("VGI_WORKER_DEBUG", "").lower() in ("1", "true", "yes")
        effective_debug = debug or env_debug
        # VGI_WORKER_LOG_* overrides are applied inside configure_worker_logging,
        # so this server and `vgi-serve` are configurable the same way.
        effective_level = configure_worker_logging(
            debug=effective_debug,
            log_level=log_level,
            log_loggers=log_logger,
            log_format=log_format,
        )
        maybe_start_profile()

        bucket = s3_bucket or os.environ.get("VGI_HTTP_S3_BUCKET")
        external_location = None
        upload_url_provider: Any = None
        max_request_bytes: int | None = None
        compression_choice = externalize_compression.lower()
        if compression_choice not in {"none", "zstd", "gzip"}:
            raise typer.BadParameter("externalize-compression must be one of: none, zstd, gzip")

        def _make_compression() -> Compression | None:
            if compression_choice == "none":
                return None
            # ``Compression.level`` historically defaults to 3 (zstd-tuned);
            # leave it at the dataclass default for zstd, but use a
            # gzip-appropriate 6 when the operator picks gzip.
            if compression_choice == "gzip":
                return Compression(algorithm="gzip", level=6)
            return Compression(algorithm="zstd")

        if bucket and demo_storage:
            raise typer.BadParameter("--s3-bucket and --demo-storage are mutually exclusive")
        if bucket:
            storage = _SigV4S3Storage(
                bucket=bucket,
                prefix=s3_prefix,
                region_name=s3_region,
                endpoint_url=s3_endpoint_url,
            )
            compression = _make_compression()
            _s3_kwargs: dict[str, Any] = {}
            if s3_endpoint_url:
                from urllib.parse import urlparse as _urlparse

                if _urlparse(s3_endpoint_url).hostname in ("127.0.0.1", "localhost"):
                    # Local S3 stand-ins (LocalStack/MinIO) presign http://
                    # URLs that the https-only default validator rejects.
                    from vgi.http.demo_storage import localhost_only_validator

                    _s3_kwargs["url_validator"] = localhost_only_validator
            external_location = ExternalLocationConfig(
                storage=storage,
                externalize_threshold_bytes=externalize_threshold_bytes,
                compression=compression,
                fetch_config=_upload_fetch_config(max_upload_bytes),
                **_s3_kwargs,
            )
            upload_url_provider = storage
            sys.stderr.write(
                "S3 offload enabled: "
                f"bucket={bucket} prefix={s3_prefix} threshold={externalize_threshold_bytes} "
                f"bytes compression={compression_choice}\n"
            )
            sys.stderr.flush()
        elif demo_storage:
            from vgi.http.demo_storage import DemoBlobStorage, localhost_only_validator

            demo_blob_storage = DemoBlobStorage()
            compression = _make_compression()
            external_location = ExternalLocationConfig(
                storage=demo_blob_storage,
                externalize_threshold_bytes=externalize_threshold_bytes,
                compression=compression,
                fetch_config=_upload_fetch_config(max_upload_bytes),
                url_validator=localhost_only_validator,
            )
            upload_url_provider = demo_blob_storage
            max_request_bytes = max_upload_bytes
            sys.stderr.write(
                "Demo blob storage enabled: "
                f"threshold={externalize_threshold_bytes} bytes "
                f"compression={compression_choice}\n"
            )
            sys.stderr.flush()
        elif _storage_backend() != "none":
            # The blob vendor or a direct S3 / GCS bucket, configured as for
            # vgi-serve (VGI_STORAGE_BACKEND, VGI_BLOB_VENDOR_URL,
            # VGI_STORAGE_S3_* / VGI_STORAGE_GCS_*,
            # VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES, VGI_RPC_MAX_REQUEST_BYTES,
            # VGI_RPC_MAX_UPLOAD_BYTES): the extension's externalization lanes
            # run this server against the fake vendor or an S3-compatible store.
            from vgi.serve import resolve_blob_vendor_http_config, start_storage

            blob_http = resolve_blob_vendor_http_config()
            start_storage(blob_http, hosted=False)
            external_location = blob_http.external_location
            upload_url_provider = blob_http.upload_url_provider
            max_request_bytes = blob_http.max_request_bytes
            if blob_http.max_upload_bytes is not None:
                max_upload_bytes = blob_http.max_upload_bytes
            if blob_http.direct is not None:
                sys.stderr.write(
                    f"Direct storage enabled: {blob_http.direct.describe()} max_request_bytes={max_request_bytes}\n"
                )
            else:
                sys.stderr.write(
                    f"Blob vendor enabled: {os.environ['VGI_BLOB_VENDOR_URL']} max_request_bytes={max_request_bytes}\n"
                )
            sys.stderr.flush()

        from vgi.serve import TOKEN_TTL_SECONDS, _resolve_authenticate, _resolve_oauth_resource_metadata

        authenticate = _resolve_authenticate()
        oauth_metadata = _resolve_oauth_resource_metadata()

        introspect_principals: list[str] | None = None
        if identity:
            from vgi._test_fixtures.identity_fixture import INTROSPECT_PRINCIPALS, conformance_authenticate

            authenticate = conformance_authenticate
            introspect_principals = INTROSPECT_PRINCIPALS

        from vgi.rpc_server import build_rpc_server, resolve_grant_keys

        grant_keys = resolve_grant_keys()
        if authenticate is None:
            # Test-only OPTIONAL bearer auth. Lets the cache identity-isolation
            # test attach the same worker under different principals (alice/bob)
            # while every other test on this shared server stays anonymous:
            # no/blank/unknown bearer → anonymous (existing behaviour), a known
            # token → its principal. Never 401s, so no anonymous test breaks.
            from vgi_rpc.http import bearer_authenticate_static
            from vgi_rpc.rpc import AuthContext

            _test_tokens = {
                "vgi-test-alice": AuthContext(principal="alice", authenticated=True, domain="bearer"),
                "vgi-test-bob": AuthContext(principal="bob", authenticated=True, domain="bearer"),
            }
            _bearer_validate = bearer_authenticate_static(tokens=_test_tokens)

            def _optional_bearer(req: Any) -> AuthContext:
                header = req.get_header("Authorization") or ""
                if not header.startswith("Bearer "):
                    return AuthContext.anonymous()
                if grant_keys is not None and header.startswith("Bearer vgig1."):
                    # A sealed grant is not ours: fall through to the grant
                    # authenticator vgi-rpc appends after this one.
                    raise ValueError("sealed grant")
                try:
                    known = _bearer_validate(req)
                except Exception:
                    return AuthContext.anonymous()
                # A test bearer is a fresh login: stamping auth_time lets the
                # attach-ticket tests call issue_grant (900 s freshness rule)
                # with nothing but a bearer DuckDB can send.
                return AuthContext(
                    principal=known.principal,
                    authenticated=True,
                    domain=known.domain,
                    claims={"auth_time": time.time()},
                )

            authenticate = _optional_bearer

        # Match vgi-fixture-worker (subprocess transport): always serve the
        # base ExampleWorker plus the projection_repro, schema_reconcile,
        # accumulate and narrow_bind fixture catalogs. Add the writable catalog
        # when its extra is installed. The twin_a/twin_b catalogs (same worker,
        # one function name colliding across two catalogs) back
        # scalar/same_name_catalogs.test — catalog-qualified dispatch.
        #
        # This list and `main()` in vgi/_test_fixtures/worker.py must agree: the
        # SAME .test files run over both transports, so a catalog present in one
        # and not the other fails the file on that transport alone — which reads
        # as a transport bug and is not one. narrow_bind drifted exactly that
        # way, and it guards a client SIGSEGV, so it was the worst one to lose.
        from vgi._test_fixtures.blob_catalog import BlobTestWorker
        from vgi._test_fixtures.catalog_contents import CONTENTS_WORKERS
        from vgi._test_fixtures.narrow_bind.worker import NarrowBindWorker
        from vgi._test_fixtures.ticket_probe import TicketProbeWorker
        from vgi._test_fixtures.twin_catalogs import TwinAWorker, TwinBWorker

        worker_classes: list[type] = [
            ExampleWorker,
            ProjReproWorker,
            SchemaReconcileWorker,
            AccumulateWorker,
            NarrowBindWorker,
            TwinAWorker,
            TwinBWorker,
            *CONTENTS_WORKERS,
            TicketProbeWorker,
            BlobTestWorker,
        ]
        try:
            from vgi._test_fixtures.writable.worker import WritableWorker
        except ImportError:
            pass
        else:
            worker_classes.append(WritableWorker)
        if identity:
            # Same "example" catalog, plus the conformance identity hooks.
            from vgi._test_fixtures.identity_fixture import IdentityExampleWorker

            worker_classes[worker_classes.index(ExampleWorker)] = IdentityExampleWorker
        workers = [wc(quiet=True, log_level=effective_level) for wc in worker_classes]
        # One signing key shared by every sub-worker (which seal catalog
        # opaque-data envelopes) and the HTTP state-token machinery.
        # Configured explicitly (VGI_SIGNING_KEY) or minted for this process.
        # Only a configured key hosts vgi.attach_tickets.v1 (with grant keys).
        signing_key_configured = bool(os.environ.get("VGI_SIGNING_KEY"))
        signing_key = os.environ.get("VGI_SIGNING_KEY", "").encode() or os.urandom(32)
        worker: Any = workers[0] if len(workers) == 1 else MetaWorker(workers)
        # Key the worker that is actually served: MetaWorker's setter passes it
        # to every child, and build_rpc_server reads it from the served worker
        # to decide whether to host vgi.attach_tickets.v1. Keying only the
        # children left the MetaWorker keyless, so tickets were never hosted.
        worker._signing_key = signing_key
        server = build_rpc_server(
            worker,
            transport="http",
            describe=describe,
            external_location=external_location,
            introspect_principals=introspect_principals,
            grant_keys=grant_keys,
            signing_key_configured=signing_key_configured,
        )
        wsgi_app = make_wsgi_app(
            server,
            prefix=prefix,
            cors_origins=cors_origins,
            token_key=signing_key,
            upload_url_provider=upload_url_provider,
            max_upload_bytes=max_upload_bytes if upload_url_provider is not None else None,
            max_request_bytes=max_request_bytes,
            max_externalized_response_bytes=max_externalized_response_bytes,
            authenticate=authenticate,
            oauth_resource_metadata=oauth_metadata,
            enable_landing_page=False,
            token_ttl=TOKEN_TTL_SECONDS,
            cursor_rule="error",
        )
        # VGI-Arguments-Ref-Bytes (protocol 3.4); VGI_TEST_DISABLE_ARGUMENTS_REF=1
        # turns it off so the server behaves like a worker without argument
        # references (the extension's arguments_ref_fallback.test).
        from vgi.arguments_ref import install_arguments_ref_header

        install_arguments_ref_header(
            wsgi_app, upload_url_provider=upload_url_provider, external_location=external_location
        )

        if demo_storage:
            from vgi.http.demo_storage import add_blob_routes

            add_blob_routes(wsgi_app, demo_blob_storage, prefix=prefix)

            # vgi_rpc's make_wsgi_app installs a Falcon middleware that 413s
            # any request body over max_request_bytes, exempting only the
            # capability/upload-URL routes. The demo blob endpoint
            # (``/__blobs__/``) serves the *externalized* payloads — bodies
            # that are intentionally larger than max_request_bytes — so it
            # must be exempt too, or auto-externalization can never land its
            # upload. Real deployments point upload URLs at S3, so vgi_rpc has
            # no built-in exemption for it; we add one here for the in-process
            # demo store.
            for mw in getattr(wsgi_app, "_unprepared_middleware", []):
                if type(mw).__name__ == "_MaxRequestBytesMiddleware":
                    mw._exempt_prefixes = (*mw._exempt_prefixes, f"{prefix}/__blobs__")

        if describe:
            # Standardized landing surface: the shared static page plus the
            # browser client build it reads the catalog with.
            from vgi.http.landing_page import ClientBundleResource, LandingPageResource

            oauth_active = oauth_metadata is not None and getattr(oauth_metadata, "client_id", None) is not None
            server_id = getattr(server, "server_id", "")
            wsgi_app.add_route(
                prefix or "/", LandingPageResource(ExampleWorker, server_id=server_id, oauth=oauth_active)
            )
            wsgi_app.add_route(f"{prefix}/vgi-client.js", ClientBundleResource())

        # vgi_rpc's make_wsgi_app already advertises VGI-Max-Request-Bytes and
        # installs a Falcon middleware enforcing it with a structured 413 that
        # the client recognizes as the externalize-and-retry signal (and which
        # we exempted for /__blobs__/ above). A second WSGI-level wrapper here
        # would only re-413 with a plain-text body the client can't parse, so
        # we serve the Falcon app directly.
        serving_app: Any = wsgi_app

        # Keep the selected socket bound while publishing its port. Selecting
        # and releasing a free port first lets another test worker take it.
        # Auto-selection has always used IPv4; retain one listener for port 0.
        http_server = waitress.create_server(serving_app, host=host, port=port, ipv6=port != 0)
        if port == 0:
            port = int(http_server.effective_port)
        if demo_storage:
            demo_blob_storage.set_base_url(f"http://{host}:{port}")

        if port_file is not None:
            # Atomic side-channel publication so test harnesses can watch
            # for the file without racing a partial write. Same helper the
            # main Worker class uses via `--port-file`.
            from vgi.worker import _write_port_file

            _write_port_file(port_file, port)

        print(f"PORT:{port}", flush=True)
        sys.stderr.write(f"Serving ExampleWorker on http://{host}:{port}{prefix}\n")
        sys.stderr.flush()
        http_server.run()

    app()


def _storage_backend() -> str:
    """``VGI_STORAGE_BACKEND``, resolved as vgi-serve does (vendor / s3 / gcs / none)."""
    from vgi_rpc.storage_env import storage_backend_from_env

    return storage_backend_from_env()


if __name__ == "__main__":
    main()
