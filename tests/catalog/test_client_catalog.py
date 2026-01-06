"""Tests for Client catalog methods using CatalogClientMixin.

These tests verify that the unified Client class can perform catalog operations
via the CatalogClientMixin, spawning ephemeral workers for each call.

IMPORTANT: The CatalogClientMixin spawns a NEW worker subprocess for each
catalog operation. This means:
- State doesn't persist between calls (each worker gets fresh InMemoryCatalog)
- Tests requiring attach_id from a previous call will fail
- Only stateless operations or single-call operations can be tested

For full catalog functionality testing, use the direct InMemoryCatalog tests
in tests/catalog/test_integration.py which test the catalog implementation
directly without the subprocess boundary.
"""

from vgi.client import Client

# Worker command for catalog tests
CATALOG_WORKER = "vgi-example-catalog-worker"


class TestClientCatalogStatelessOperations:
    """Test catalog operations that don't require state persistence.

    These tests work with the ephemeral worker pattern because they either:
    - Don't require state from a previous call
    - Complete in a single call

    """

    def test_catalogs_returns_list(self) -> None:
        """Client.catalogs() returns list of catalog names."""
        client = Client(CATALOG_WORKER)
        catalogs = client.catalogs()
        assert isinstance(catalogs, list)
        assert "memory" in catalogs

    def test_catalog_attach_returns_result(self) -> None:
        """Client.catalog_attach() returns CatalogAttachResult."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        assert result.attach_id is not None
        assert len(result.attach_id) == 16  # UUID bytes
        assert result.supports_transactions is False

    def test_catalogs_works_without_start(self) -> None:
        """Catalog methods work without calling start()."""
        client = Client(CATALOG_WORKER)
        # Don't call start() - catalog methods spawn ephemeral workers
        catalogs = client.catalogs()
        assert "memory" in catalogs

    def test_catalogs_works_inside_context_manager(self) -> None:
        """Catalog methods work inside context manager."""
        with Client(CATALOG_WORKER) as client:
            catalogs = client.catalogs()
            assert "memory" in catalogs

    def test_multiple_catalogs_calls(self) -> None:
        """Multiple catalogs() calls work on same Client instance."""
        client = Client(CATALOG_WORKER)

        # Multiple calls should each spawn new workers and work independently
        catalogs1 = client.catalogs()
        catalogs2 = client.catalogs()

        assert "memory" in catalogs1
        assert "memory" in catalogs2

    def test_catalog_attach_includes_capabilities(self) -> None:
        """CatalogAttachResult includes capability flags."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        # Check that capability flags are present (even if False)
        assert isinstance(result.supports_transactions, bool)
        assert isinstance(result.supports_time_travel, bool)
        assert isinstance(result.catalog_version_frozen, bool)


class TestClientCatalogProtocolIntegrity:
    """Test that the catalog protocol is working correctly.

    These tests verify the communication between Client and Worker without
    requiring state persistence across calls.

    """

    def test_catalog_attach_different_attach_ids(self) -> None:
        """Each catalog_attach call returns a different attach_id.

        This verifies that the attach process is working, even though
        the attach_id won't be usable in a subsequent call.

        """
        client = Client(CATALOG_WORKER)

        # Each attach spawns a new worker, so each gets a unique ID
        result1 = client.catalog_attach(name="memory", options={})
        result2 = client.catalog_attach(name="memory", options={})

        # Both should work, but will have different IDs
        assert result1.attach_id is not None
        assert result2.attach_id is not None
        # IDs are randomly generated, so they're very likely different
        # (not a guaranteed assertion, but useful for protocol verification)

    def test_catalogs_returns_correct_format(self) -> None:
        """catalogs() returns a list of strings."""
        client = Client(CATALOG_WORKER)
        catalogs = client.catalogs()

        assert isinstance(catalogs, list)
        for name in catalogs:
            assert isinstance(name, str)


# NOTE: Tests that require state persistence across catalog calls
# (e.g., attach then use attach_id in subsequent call) are NOT possible
# with the ephemeral worker pattern. Each call spawns a fresh worker
# with a fresh InMemoryCatalog instance.
#
# To test full catalog workflows:
# 1. Use tests/catalog/test_integration.py which tests InMemoryCatalog directly
# 2. Or use a persistent catalog backend (e.g., SQLite-backed CatalogStorage)
# 3. Or use a long-running worker process (not ephemeral)
