#!/usr/bin/env python3
"""Serve standard fixtures with contents_probe published through a local HTTP object store.

Run with the normal fixture worker arguments (stdio, --http, --unix). Object
storage lives for this worker's lifetime; durable S3 restart coverage lives in
tests/catalog/test_catalog_publication.py. Set VGI_CATALOG_PUBLICATION_PORT to
inspect /stats from a separate process. Only use this local fixture in tests.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pyarrow as pa
from vgi_rpc.external import ClientExternalConfig, Compression

from vgi._test_fixtures.catalog_contents import ContentsProbeCatalog
from vgi.catalog.snapshot import CatalogSnapshotPolicy, MemoryCatalogPublicationRepository, SnapshotIdentity


class _Storage:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str | None]] = {}
        self.gets = 0
        self.uploads = 0
        self.base = ""

    def upload(self, data: bytes, schema: pa.Schema, *, content_encoding: str | None = None) -> str:
        del schema
        key = "/" + uuid.uuid4().hex
        self.objects[key] = data, content_encoding
        self.uploads += 1
        return self.base + key


def main() -> None:
    """Configure publication before invoking the ordinary fixture CLI."""
    storage = _Storage()

    class Handler(BaseHTTPRequestHandler):
        def do_OPTIONS(self) -> None:
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "X-VGI-Accept-Encoding, Content-Type")
            self.send_header("Cross-Origin-Resource-Policy", "cross-origin")
            self.end_headers()

        def do_GET(self) -> None:
            if self.path == "/stats":
                data = json.dumps({"uploads": storage.uploads, "object_gets": storage.gets}).encode()
                encoding = None
            elif self.path in storage.objects:
                storage.gets += 1
                data, encoding = storage.objects[self.path]
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cross-Origin-Resource-Policy", "cross-origin")
            self.send_header("Content-Length", str(len(data)))
            if encoding:
                self.send_header("Content-Encoding", encoding)
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", int(os.environ.get("VGI_CATALOG_PUBLICATION_PORT", "0"))), Handler)
    storage.base = f"http://127.0.0.1:{server.server_port}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    config = ClientExternalConfig(max_retries=0, url_validator=None)
    ContentsProbeCatalog.catalog_snapshot_policy = CatalogSnapshotPolicy(
        delivery="published",
        repository=MemoryCatalogPublicationRepository(storage, profile="fixture", stable_urls=True),
        identity=lambda request: SnapshotIdentity(
            catalog_namespace="contents_probe",
            source_revision="fixture-2.2",
            visibility_scope="public-fixture",
            catalog_version=request.catalog_version,
        ),
        compression=Compression(algorithm="gzip"),
        fetch_config=config,
    )
    print(f"Catalog publication fixture storage: {storage.base}", file=sys.stderr)
    try:
        from vgi._test_fixtures.worker import main as fixture_main

        fixture_main()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
        config.fetch_config.close()


if __name__ == "__main__":
    main()
