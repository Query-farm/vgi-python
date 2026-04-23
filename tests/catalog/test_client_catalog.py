"""Tests for Client catalog methods using CatalogClientMixin.

These tests verify that the unified Client class can perform catalog operations
via the CatalogClientMixin. The WorkerPool keeps workers alive between calls,
so state persists within a test.

For full catalog CRUD workflows (create/get/drop tables, views, schemas),
see tests/catalog/test_integration.py which exercises all catalog operations.
"""

from vgi.client import Client

# Worker command for catalog tests
CATALOG_WORKER = "vgi-example-catalog-worker"


class TestClientCatalogStatelessOperations:
    """Test catalog operations that work independently."""

    def test_catalogs_returns_list(self) -> None:
        """Client.catalogs() returns list of catalog discovery records."""
        client = Client(CATALOG_WORKER)
        catalogs = client.catalogs()
        assert isinstance(catalogs, list)
        assert "memory" in [c.name for c in catalogs]

    def test_catalog_attach_returns_result(self) -> None:
        """Client.catalog_attach() returns CatalogAttachResult."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={}, data_version_spec=None, implementation_version=None)

        assert result.attach_id is not None
        assert len(result.attach_id) == 16  # UUID bytes
        assert result.supports_transactions is False

    def test_catalogs_works_without_start(self) -> None:
        """Catalog methods work without calling start()."""
        client = Client(CATALOG_WORKER)
        # Don't call start() - catalog methods use WorkerPool independently
        catalogs = client.catalogs()
        assert "memory" in [c.name for c in catalogs]

    def test_catalogs_works_inside_context_manager(self) -> None:
        """Catalog methods work inside context manager."""
        with Client(CATALOG_WORKER) as client:
            catalogs = client.catalogs()
            assert "memory" in [c.name for c in catalogs]

    def test_multiple_catalogs_calls(self) -> None:
        """Multiple catalogs() calls work on same Client instance."""
        client = Client(CATALOG_WORKER)

        catalogs1 = client.catalogs()
        catalogs2 = client.catalogs()

        assert "memory" in [c.name for c in catalogs1]
        assert "memory" in [c.name for c in catalogs2]

    def test_catalog_attach_includes_capabilities(self) -> None:
        """CatalogAttachResult includes capability flags."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={}, data_version_spec=None, implementation_version=None)

        # Check that capability flags are present (even if False)
        assert isinstance(result.supports_transactions, bool)
        assert isinstance(result.supports_time_travel, bool)
        assert isinstance(result.catalog_version_frozen, bool)


class TestClientCatalogProtocolIntegrity:
    """Test that the catalog protocol is working correctly."""

    def test_catalogs_returns_correct_format(self) -> None:
        """catalogs() returns a list of CatalogInfo records."""
        client = Client(CATALOG_WORKER)
        catalogs = client.catalogs()

        assert isinstance(catalogs, list)
        for info in catalogs:
            assert isinstance(info.name, str)
            assert info.implementation_version is None or isinstance(info.implementation_version, str)
            assert info.data_version_spec is None or isinstance(info.data_version_spec, str)
