"""Tests for CatalogStorage and CatalogStorageSqlite."""

import tempfile
from pathlib import Path

from vgi.catalog import AttachId, CatalogStorageSqlite, TransactionId


class TestCatalogStorageSqliteAttachments:
    """Test attachment operations."""

    def test_attach_put_and_get(self) -> None:
        """Can store and retrieve attachment state."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            attach_id = storage.generate_attach_id()
            storage.attach_put(attach_id, "my_catalog", {"key": "value"})

            result = storage.attach_get(attach_id)
            assert result is not None
            catalog_name, options = result
            assert catalog_name == "my_catalog"
            assert options == {"key": "value"}

    def test_attach_get_nonexistent(self) -> None:
        """Getting nonexistent attachment returns None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            result = storage.attach_get(AttachId(b"nonexistent"))
            assert result is None

    def test_attach_delete(self) -> None:
        """Can delete attachment state."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            attach_id = storage.generate_attach_id()
            storage.attach_put(attach_id, "catalog", {})
            storage.attach_delete(attach_id)

            result = storage.attach_get(attach_id)
            assert result is None

    def test_attach_list(self) -> None:
        """Can list all attachment IDs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            id1 = storage.generate_attach_id()
            id2 = storage.generate_attach_id()
            storage.attach_put(id1, "catalog1", {})
            storage.attach_put(id2, "catalog2", {})

            ids = storage.attach_list()
            assert len(ids) == 2
            assert id1 in ids
            assert id2 in ids

    def test_attach_put_replaces_existing(self) -> None:
        """Putting same attach_id replaces the existing entry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            attach_id = storage.generate_attach_id()
            storage.attach_put(attach_id, "old_catalog", {"old": True})
            storage.attach_put(attach_id, "new_catalog", {"new": True})

            result = storage.attach_get(attach_id)
            assert result is not None
            catalog_name, options = result
            assert catalog_name == "new_catalog"
            assert options == {"new": True}

    def test_attach_with_complex_options(self) -> None:
        """Can store and retrieve complex options."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            attach_id = storage.generate_attach_id()
            options = {
                "string": "value",
                "number": 42,
                "float": 3.14,
                "bool": True,
                "null": None,
                "list": [1, 2, 3],
                "nested": {"a": {"b": "c"}},
            }
            storage.attach_put(attach_id, "catalog", options)

            result = storage.attach_get(attach_id)
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

            attach_id = storage.generate_attach_id()
            storage.attach_put(attach_id, "catalog", {})

            tx_id = storage.generate_transaction_id()
            state = b"transaction state data"
            storage.transaction_put(tx_id, attach_id, state)

            result = storage.transaction_get(tx_id)
            assert result is not None
            retrieved_attach_id, retrieved_state = result
            assert retrieved_attach_id == attach_id
            assert retrieved_state == state

    def test_transaction_get_nonexistent(self) -> None:
        """Getting nonexistent transaction returns None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            result = storage.transaction_get(TransactionId(b"nonexistent"))
            assert result is None

    def test_transaction_delete(self) -> None:
        """Can delete transaction state."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            attach_id = storage.generate_attach_id()
            storage.attach_put(attach_id, "catalog", {})

            tx_id = storage.generate_transaction_id()
            storage.transaction_put(tx_id, attach_id, b"state")
            storage.transaction_delete(tx_id)

            result = storage.transaction_get(tx_id)
            assert result is None

    def test_attach_delete_cascades_to_transactions(self) -> None:
        """Deleting attachment also deletes associated transactions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            attach_id = storage.generate_attach_id()
            storage.attach_put(attach_id, "catalog", {})

            tx_id = storage.generate_transaction_id()
            storage.transaction_put(tx_id, attach_id, b"state")

            # Delete the attachment
            storage.attach_delete(attach_id)

            # Transaction should also be deleted
            result = storage.transaction_get(tx_id)
            assert result is None


class TestCatalogStorageSqliteIdGeneration:
    """Test ID generation methods."""

    def test_generate_attach_id_unique(self) -> None:
        """Generated attach_ids are unique."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            ids = {storage.generate_attach_id() for _ in range(100)}
            assert len(ids) == 100  # All unique

    def test_generate_transaction_id_unique(self) -> None:
        """Generated transaction_ids are unique."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            ids = {storage.generate_transaction_id() for _ in range(100)}
            assert len(ids) == 100  # All unique

    def test_attach_id_is_16_bytes(self) -> None:
        """Generated attach_ids are 16 bytes (UUID)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            storage = CatalogStorageSqlite(db_path)

            attach_id = storage.generate_attach_id()
            assert len(attach_id) == 16


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

            attach_id = storage.generate_attach_id()
            storage.attach_put(attach_id, "catalog", {})

            # Cleanup with 365 days should not remove recent entry
            storage.cleanup_old_entries(max_age_days=365.0)

            result = storage.attach_get(attach_id)
            assert result is not None


class TestCatalogStorageSqlitePersistence:
    """Test persistence across storage instances."""

    def test_persistence_across_instances(self) -> None:
        """Data persists across storage instances."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            # Create and store with first instance
            storage1 = CatalogStorageSqlite(db_path)
            attach_id = storage1.generate_attach_id()
            storage1.attach_put(attach_id, "persistent_catalog", {"key": "value"})

            # Create second instance and retrieve
            storage2 = CatalogStorageSqlite(db_path)
            result = storage2.attach_get(attach_id)

            assert result is not None
            catalog_name, options = result
            assert catalog_name == "persistent_catalog"
            assert options == {"key": "value"}
