# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Test fixture: an Orchard-style catalog worker that advertises a secret-service URL.

Serves an in-memory catalog named ``orchard`` whose ``catalog_attach`` response
carries ``tags["vgi_secret_service_url"]`` (taken from the ``VGI_ORCHARD_SECRET_URL``
environment variable). The C++ VGI extension reads that tag at ATTACH time and
auto-registers a ``VgiRemoteSecretStorage`` pointing at the secret microservice.

Run with::

    VGI_ORCHARD_SECRET_URL=http://127.0.0.1:<port>/ \
        vgi-serve vgi._test_fixtures.orchard_catalog:OrchardCatalogWorker --http
"""

from __future__ import annotations

import os

from vgi.catalog import AttachOpaqueData, SchemaInfo
from vgi._test_fixtures.catalog import CatalogData, InMemoryCatalog, SchemaData
from vgi.worker import Worker


class OrchardCatalog(InMemoryCatalog):
    """In-memory catalog with an ``orchard`` catalog tagged with the secret URL."""

    def __init__(self) -> None:
        super().__init__()
        url = os.environ.get("VGI_ORCHARD_SECRET_URL", "")
        tags = {"vgi_secret_service_url": url} if url else {}
        catalog = CatalogData(name="orchard", tags=tags)
        placeholder = AttachOpaqueData(b"\x00" * 16)
        catalog.schemas["main"] = SchemaData(
            info=SchemaInfo(attach_opaque_data=placeholder, name="main", comment=None, tags={})
        )
        self._catalogs["orchard"] = catalog


class OrchardCatalogWorker(Worker):
    """Worker serving :class:`OrchardCatalog`."""

    catalog_interface = OrchardCatalog


if __name__ == "__main__":
    OrchardCatalogWorker.main()
