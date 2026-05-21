# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for CatalogStorage and CatalogStorageSqlite."""

import tempfile
from pathlib import Path

from vgi.catalog import AttachOpaqueData, CatalogStorageSqlite, TransactionOpaqueData


class TestCatalogStorageSqliteAttachments:
    """Test attachment operations."""

    def test_attach_put_and_get(self) -> None:
        """Can store and retrieve attachment state."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            attach_opaque_data = storage.generate_attach_opaque_data()
            storage.attach_put(attach_opaque_data, "my_catalog", {"key": "value"})

            result = storage.attach_get(attach_opaque_data)
            assert result is not None
            catalog_name, options = result
            assert catalog_name == "my_catalog"
            assert options == {"key": "value"}

    def test_attach_get_nonexistent(self) -> None:
        """Getting nonexistent attachment returns None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            result = storage.attach_get(AttachOpaqueData(b"nonexistent"))
            assert result is None

    def test_attach_delete(self) -> None:
        """Can delete attachment state."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            attach_opaque_data = storage.generate_attach_opaque_data()
            storage.attach_put(attach_opaque_data, "catalog", {})
            storage.attach_delete(attach_opaque_data)

            result = storage.attach_get(attach_opaque_data)
            assert result is None

    def test_attach_list(self) -> None:
        """Can list all attachment IDs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            id1 = storage.generate_attach_opaque_data()
            id2 = storage.generate_attach_opaque_data()
            storage.attach_put(id1, "catalog1", {})
            storage.attach_put(id2, "catalog2", {})

            ids = storage.attach_list()
            assert len(ids) == 2
            assert id1 in ids
            assert id2 in ids

    def test_attach_put_replaces_existing(self) -> None:
        """Putting same attach_opaque_data replaces the existing entry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            attach_opaque_data = storage.generate_attach_opaque_data()
            storage.attach_put(attach_opaque_data, "old_catalog", {"old": True})
            storage.attach_put(attach_opaque_data, "new_catalog", {"new": True})

            result = storage.attach_get(attach_opaque_data)
            assert result is not None
            catalog_name, options = result
            assert catalog_name == "new_catalog"
            assert options == {"new": True}

    def test_attach_with_complex_options(self) -> None:
        """Can store and retrieve complex options."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            attach_opaque_data = storage.generate_attach_opaque_data()
            options = {
                "string": "value",
                "number": 42,
                "float": 3.14,
                "bool": True,
                "null": None,
                "list": [1, 2, 3],
                "nested": {"a": {"b": "c"}},
            }
            storage.attach_put(attach_opaque_data, "catalog", options)

            result = storage.attach_get(attach_opaque_data)
            assert result is not None
            _, retrieved_options = result
            assert retrieved_options == options


class TestCatalogStorageSqliteTransactions:
    """Test transaction operations."""

    def test_transaction_put_and_get(self) -> None:
        """Can store and retrieve transaction state."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            attach_opaque_data = storage.generate_attach_opaque_data()
            storage.attach_put(attach_opaque_data, "catalog", {})

            tx_id = storage.generate_transaction_opaque_data()
            state = b"transaction state data"
            storage.transaction_put(tx_id, attach_opaque_data, state)

            result = storage.transaction_get(tx_id)
            assert result is not None
            retrieved_attach_opaque_data, retrieved_state = result
            assert retrieved_attach_opaque_data == attach_opaque_data
            assert retrieved_state == state

    def test_transaction_get_nonexistent(self) -> None:
        """Getting nonexistent transaction returns None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            result = storage.transaction_get(TransactionOpaqueData(b"nonexistent"))
            assert result is None

    def test_transaction_delete(self) -> None:
        """Can delete transaction state."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            attach_opaque_data = storage.generate_attach_opaque_data()
            storage.attach_put(attach_opaque_data, "catalog", {})

            tx_id = storage.generate_transaction_opaque_data()
            storage.transaction_put(tx_id, attach_opaque_data, b"state")
            storage.transaction_delete(tx_id)

            result = storage.transaction_get(tx_id)
            assert result is None

    def test_attach_delete_cascades_to_transactions(self) -> None:
        """Deleting attachment also deletes associated transactions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            attach_opaque_data = storage.generate_attach_opaque_data()
            storage.attach_put(attach_opaque_data, "catalog", {})

            tx_id = storage.generate_transaction_opaque_data()
            storage.transaction_put(tx_id, attach_opaque_data, b"state")

            # Delete the attachment
            storage.attach_delete(attach_opaque_data)

            # Transaction should also be deleted
            result = storage.transaction_get(tx_id)
            assert result is None


class TestCatalogStorageSqliteIdGeneration:
    """Test ID generation methods."""

    def test_generate_attach_opaque_data_unique(self) -> None:
        """Generated attach_opaque_data values are unique."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            ids = {storage.generate_attach_opaque_data() for _ in range(100)}
            assert len(ids) == 100  # All unique

    def test_generate_transaction_opaque_data_unique(self) -> None:
        """Generated transaction_opaque_data values are unique."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            ids = {storage.generate_transaction_opaque_data() for _ in range(100)}
            assert len(ids) == 100  # All unique

    def test_attach_opaque_data_is_16_bytes(self) -> None:
        """Generated attach_opaque_data values are 16 bytes (UUID)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            attach_opaque_data = storage.generate_attach_opaque_data()
            assert len(attach_opaque_data) == 16


class TestCatalogStorageSqliteCleanup:
    """Test cleanup operations."""

    def test_cleanup_returns_count(self) -> None:
        """Cleanup returns the count of deleted entries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            # With no entries, should return 0
            deleted = storage.cleanup_old_entries(max_age_days=0.0)
            assert deleted == 0

    def test_cleanup_preserves_recent_entries(self) -> None:
        """Cleanup with large max_age preserves recent entries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            attach_opaque_data = storage.generate_attach_opaque_data()
            storage.attach_put(attach_opaque_data, "catalog", {})

            # Cleanup with 365 days should not remove recent entry
            storage.cleanup_old_entries(max_age_days=365.0)

            result = storage.attach_get(attach_opaque_data)
            assert result is not None


class TestCatalogStorageSqlitePersistence:
    """Test persistence across storage instances."""

    def test_persistence_across_instances(self) -> None:
        """Data persists across storage instances."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            # Create and store with first instance
            storage1 = CatalogStorageSqlite(db_path)
            attach_opaque_data = storage1.generate_attach_opaque_data()
            storage1.attach_put(attach_opaque_data, "persistent_catalog", {"key": "value"})

            # Create second instance and retrieve
            storage2 = CatalogStorageSqlite(db_path)
            result = storage2.attach_get(attach_opaque_data)

            assert result is not None
            catalog_name, options = result
            assert catalog_name == "persistent_catalog"
            assert options == {"key": "value"}
