"""Run the example worker as an HTTP server.

Usage::

    vgi-example-http
    vgi-example-http --port 9000
    vgi-example-http --host 0.0.0.0 --port 8080 --debug
    vgi-example-http --s3-bucket rusty-vgi-test
    vgi-example-http --demo-storage

Requires the ``http`` extra: ``pip install vgi[http]``
For S3 offload support, also install: ``pip install vgi-rpc[s3]``
"""

from __future__ import annotations

import os
import sys
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from vgi.examples.worker import ExampleWorker
from vgi.logging_config import LogFormat, LogLevel, configure_worker_logging

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
            import boto3  # type: ignore[import-not-found]
            from botocore.config import Config  # type: ignore[import-not-found]

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

    def generate_upload_url(self, schema: Any) -> UploadUrl:
        import uuid

        from vgi_rpc.external import UploadUrl

        client = self._get_client()
        key = f"{self.prefix}{uuid.uuid4().hex}.arrow"
        params = {"Bucket": self.bucket, "Key": key}
        expires_at = datetime.now(UTC) + timedelta(seconds=self.presign_expiry_seconds)

        put_url = client.generate_presigned_url(
            "put_object",
            Params=params,
            ExpiresIn=self.presign_expiry_seconds,
        )
        get_url = client.generate_presigned_url(
            "get_object",
            Params=params,
            ExpiresIn=self.presign_expiry_seconds,
        )
        return UploadUrl(upload_url=put_url, download_url=get_url, expires_at=expires_at)


def main() -> None:
    """Run the example worker as an HTTP server.

    When ``--s3-bucket`` is provided (or ``VGI_HTTP_S3_BUCKET`` is set),
    response batches larger than ``--externalize-threshold-bytes`` are uploaded
    to S3 and replaced by signed URLs on the wire.
    """
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
            help="Advertise max direct HTTP upload bytes for server-vended upload URLs",
        ),
        externalize_compression: str = typer.Option(
            "none",
            "--externalize-compression",
            help="Compression for externalized batches: none or zstd",
        ),
        demo_storage: bool = typer.Option(
            False,
            "--demo-storage",
            help="Enable in-process blob storage for externalized payloads (no S3 required)",
        ),
    ) -> None:
        try:
            from vgi_rpc import Compression, ExternalLocationConfig, RpcServer
            from vgi_rpc.http import make_wsgi_app
        except ImportError:
            sys.stderr.write(
                "Error: HTTP dependencies not installed.\n"
                "Install with: pip install vgi[http]  (or: uv sync --extra http)\n"
            )
            sys.exit(1)

        try:
            import waitress  # type: ignore[import-untyped]
        except ImportError:
            sys.stderr.write(
                "Error: waitress not installed.\nInstall with: pip install vgi[http]  (or: uv sync --extra http)\n"
            )
            sys.exit(1)

        env_debug = os.environ.get("VGI_WORKER_DEBUG", "").lower() in ("1", "true", "yes")
        effective_debug = debug or env_debug
        effective_level = configure_worker_logging(
            debug=effective_debug,
            log_level=log_level,
            log_loggers=log_logger,
            log_format=log_format,
        )

        bucket = s3_bucket or os.environ.get("VGI_HTTP_S3_BUCKET")
        external_location = None
        upload_url_provider: Any = None
        max_request_bytes: int | None = None
        compression_choice = externalize_compression.lower()
        if compression_choice not in {"none", "zstd"}:
            raise typer.BadParameter("externalize-compression must be one of: none, zstd")
        if bucket and demo_storage:
            raise typer.BadParameter("--s3-bucket and --demo-storage are mutually exclusive")
        if bucket:
            storage = _SigV4S3Storage(
                bucket=bucket,
                prefix=s3_prefix,
                region_name=s3_region,
                endpoint_url=s3_endpoint_url,
            )
            compression = Compression() if compression_choice == "zstd" else None
            external_location = ExternalLocationConfig(
                storage=storage,
                externalize_threshold_bytes=externalize_threshold_bytes,
                compression=compression,
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
            compression = Compression() if compression_choice == "zstd" else None
            external_location = ExternalLocationConfig(
                storage=demo_blob_storage,
                externalize_threshold_bytes=externalize_threshold_bytes,
                compression=compression,
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

        import socket

        from vgi.protocol import VgiProtocol

        if port == 0:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((host, 0))
                port = int(s.getsockname()[1])

        if demo_storage:
            demo_blob_storage.set_base_url(f"http://{host}:{port}")

        from vgi.serve import _resolve_authenticate, _resolve_oauth_resource_metadata

        authenticate = _resolve_authenticate()
        oauth_metadata = _resolve_oauth_resource_metadata()

        from vgi.worker import _get_vgi_version

        worker = ExampleWorker(quiet=True, log_level=effective_level)
        server = RpcServer(
            VgiProtocol,
            worker,
            external_location=external_location,
            enable_describe=describe,
            server_version=_get_vgi_version(),
        )
        wsgi_app = make_wsgi_app(
            server,
            prefix=prefix,
            cors_origins=cors_origins,
            upload_url_provider=upload_url_provider,
            max_upload_bytes=max_upload_bytes if upload_url_provider is not None else None,
            max_request_bytes=max_request_bytes,
            authenticate=authenticate,
            oauth_resource_metadata=oauth_metadata,
        )

        if demo_storage:
            from vgi.http.demo_storage import add_blob_routes

            add_blob_routes(wsgi_app, demo_blob_storage, prefix=prefix)

        if describe:
            from vgi.http.worker_page import WorkerPageResource, build_worker_page

            worker_page_body = build_worker_page(ExampleWorker, prefix)
            wsgi_app.add_route(f"{prefix}/worker", WorkerPageResource(worker_page_body))

        # Wrap with 413 middleware after all Falcon routes are registered.
        serving_app: Any = wsgi_app
        if demo_storage:
            from vgi.http.demo_storage import MaxRequestBytesMiddleware

            serving_app = MaxRequestBytesMiddleware(wsgi_app, max_upload_bytes)

        print(f"PORT:{port}", flush=True)
        sys.stderr.write(f"Serving ExampleWorker on http://{host}:{port}{prefix}\n")
        sys.stderr.flush()
        waitress.serve(serving_app, host=host, port=port, _quiet=True)

    app()


if __name__ == "__main__":
    main()
