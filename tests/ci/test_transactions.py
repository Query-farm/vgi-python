"""Tests for CICatalog transaction operations."""

from __future__ import annotations

import pytest

from vgi.catalog import AttachId, OnConflict, SerializedSchema
from vgi.ci.catalog import CICatalog


class TestTransactionBegin:
    """Tests for catalog_transaction_begin() method."""

    def test_begin_returns_transaction_id(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Beginning transaction returns a transaction ID."""
        catalog, attach_id = attached_catalog

        tx_id = catalog.catalog_transaction_begin(attach_id=attach_id)

        assert tx_id is not None
        assert len(tx_id) == 16  # UUID bytes

    def test_begin_nested_transaction_error(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Beginning nested transaction raises error."""
        catalog, attach_id = attached_catalog
        catalog.catalog_transaction_begin(attach_id=attach_id)

        with pytest.raises(ValueError, match="already active"):
            catalog.catalog_transaction_begin(attach_id=attach_id)


class TestTransactionCommit:
    """Tests for catalog_transaction_commit() method."""

    def test_commit_succeeds(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Committing transaction succeeds."""
        catalog, attach_id = attached_catalog
        tx_id = catalog.catalog_transaction_begin(attach_id=attach_id)
        assert tx_id is not None

        catalog.catalog_transaction_commit(attach_id=attach_id, transaction_id=tx_id)

        # Can begin new transaction after commit
        new_tx = catalog.catalog_transaction_begin(attach_id=attach_id)
        assert new_tx is not None

    def test_commit_without_transaction(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Committing without active transaction raises error."""
        catalog, attach_id = attached_catalog
        from vgi.catalog import TransactionId

        with pytest.raises(ValueError, match="No transaction"):
            catalog.catalog_transaction_commit(
                attach_id=attach_id, transaction_id=TransactionId(b"fake")
            )

    def test_commit_wrong_transaction_id(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Committing with wrong transaction ID raises error."""
        catalog, attach_id = attached_catalog
        from vgi.catalog import TransactionId

        catalog.catalog_transaction_begin(attach_id=attach_id)

        with pytest.raises(ValueError, match="mismatch"):
            catalog.catalog_transaction_commit(
                attach_id=attach_id, transaction_id=TransactionId(b"wrong")
            )

    def test_commit_preserves_changes(
        self,
        attached_catalog: tuple[CICatalog, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Changes made in transaction persist after commit."""
        catalog, attach_id = attached_catalog
        tx_id = catalog.catalog_transaction_begin(attach_id=attach_id)
        assert tx_id is not None

        catalog.table_create(
            attach_id=attach_id,
            transaction_id=tx_id,
            schema_name="main",
            name="test_table",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        catalog.catalog_transaction_commit(attach_id=attach_id, transaction_id=tx_id)

        # Table should exist after commit
        table = catalog.table_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="test_table",
        )
        assert table is not None


class TestTransactionRollback:
    """Tests for catalog_transaction_rollback() method."""

    def test_rollback_succeeds(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Rolling back transaction succeeds."""
        catalog, attach_id = attached_catalog
        tx_id = catalog.catalog_transaction_begin(attach_id=attach_id)
        assert tx_id is not None

        catalog.catalog_transaction_rollback(attach_id=attach_id, transaction_id=tx_id)

        # Can begin new transaction after rollback
        new_tx = catalog.catalog_transaction_begin(attach_id=attach_id)
        assert new_tx is not None

    def test_rollback_without_transaction(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Rolling back without active transaction raises error."""
        catalog, attach_id = attached_catalog
        from vgi.catalog import TransactionId

        with pytest.raises(ValueError, match="No transaction"):
            catalog.catalog_transaction_rollback(
                attach_id=attach_id, transaction_id=TransactionId(b"fake")
            )

    def test_rollback_wrong_transaction_id(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Rolling back with wrong transaction ID raises error."""
        catalog, attach_id = attached_catalog
        from vgi.catalog import TransactionId

        catalog.catalog_transaction_begin(attach_id=attach_id)

        with pytest.raises(ValueError, match="mismatch"):
            catalog.catalog_transaction_rollback(
                attach_id=attach_id, transaction_id=TransactionId(b"wrong")
            )

    def test_rollback_reverts_table_create(
        self,
        attached_catalog: tuple[CICatalog, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Rolling back reverts table creation."""
        catalog, attach_id = attached_catalog
        tx_id = catalog.catalog_transaction_begin(attach_id=attach_id)
        assert tx_id is not None

        catalog.table_create(
            attach_id=attach_id,
            transaction_id=tx_id,
            schema_name="main",
            name="rollback_table",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        # Table exists before rollback
        assert (
            catalog.table_get(
                attach_id=attach_id,
                transaction_id=tx_id,
                schema_name="main",
                name="rollback_table",
            )
            is not None
        )

        catalog.catalog_transaction_rollback(attach_id=attach_id, transaction_id=tx_id)

        # Table should not exist after rollback
        assert (
            catalog.table_get(
                attach_id=attach_id,
                transaction_id=None,
                schema_name="main",
                name="rollback_table",
            )
            is None
        )

    def test_rollback_reverts_schema_create(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Rolling back reverts schema creation."""
        catalog, attach_id = attached_catalog
        tx_id = catalog.catalog_transaction_begin(attach_id=attach_id)
        assert tx_id is not None

        catalog.schema_create(
            attach_id=attach_id,
            transaction_id=tx_id,
            name="rollback_schema",
            comment=None,
            tags={},
        )

        # Schema exists before rollback
        assert (
            catalog.schema_get(
                attach_id=attach_id, transaction_id=tx_id, name="rollback_schema"
            )
            is not None
        )

        catalog.catalog_transaction_rollback(attach_id=attach_id, transaction_id=tx_id)

        # Schema should not exist after rollback
        assert (
            catalog.schema_get(
                attach_id=attach_id, transaction_id=None, name="rollback_schema"
            )
            is None
        )

    def test_rollback_reverts_view_create(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Rolling back reverts view creation."""
        catalog, attach_id = attached_catalog
        tx_id = catalog.catalog_transaction_begin(attach_id=attach_id)
        assert tx_id is not None

        catalog.view_create(
            attach_id=attach_id,
            transaction_id=tx_id,
            schema_name="main",
            name="rollback_view",
            definition="SELECT 1",
            on_conflict=OnConflict.ERROR,
        )

        catalog.catalog_transaction_rollback(attach_id=attach_id, transaction_id=tx_id)

        # View should not exist after rollback
        assert (
            catalog.view_get(
                attach_id=attach_id,
                transaction_id=None,
                schema_name="main",
                name="rollback_view",
            )
            is None
        )

    def test_rollback_reverts_table_drop(
        self,
        attached_catalog: tuple[CICatalog, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Rolling back reverts table drop."""
        catalog, attach_id = attached_catalog

        # Create table before transaction
        catalog.table_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="to_restore",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        tx_id = catalog.catalog_transaction_begin(attach_id=attach_id)
        assert tx_id is not None

        catalog.table_drop(
            attach_id=attach_id,
            transaction_id=tx_id,
            schema_name="main",
            name="to_restore",
            ignore_not_found=False,
        )

        # Table doesn't exist during transaction
        assert (
            catalog.table_get(
                attach_id=attach_id,
                transaction_id=tx_id,
                schema_name="main",
                name="to_restore",
            )
            is None
        )

        catalog.catalog_transaction_rollback(attach_id=attach_id, transaction_id=tx_id)

        # Table should be restored after rollback
        assert (
            catalog.table_get(
                attach_id=attach_id,
                transaction_id=None,
                schema_name="main",
                name="to_restore",
            )
            is not None
        )


class TestTransactionVisibility:
    """Tests for change visibility during transactions."""

    def test_changes_visible_during_transaction(
        self,
        attached_catalog: tuple[CICatalog, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Changes made in transaction are visible during the transaction."""
        catalog, attach_id = attached_catalog
        tx_id = catalog.catalog_transaction_begin(attach_id=attach_id)
        assert tx_id is not None

        catalog.table_create(
            attach_id=attach_id,
            transaction_id=tx_id,
            schema_name="main",
            name="visible_table",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        # Table should be visible during transaction
        table = catalog.table_get(
            attach_id=attach_id,
            transaction_id=tx_id,
            schema_name="main",
            name="visible_table",
        )
        assert table is not None

    def test_multiple_operations_in_transaction(
        self,
        attached_catalog: tuple[CICatalog, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Multiple operations in transaction are all visible."""
        catalog, attach_id = attached_catalog
        tx_id = catalog.catalog_transaction_begin(attach_id=attach_id)
        assert tx_id is not None

        # Create schema
        catalog.schema_create(
            attach_id=attach_id,
            transaction_id=tx_id,
            name="tx_schema",
            comment=None,
            tags={},
        )

        # Create table in new schema
        catalog.table_create(
            attach_id=attach_id,
            transaction_id=tx_id,
            schema_name="tx_schema",
            name="tx_table",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        # Create view
        catalog.view_create(
            attach_id=attach_id,
            transaction_id=tx_id,
            schema_name="main",
            name="tx_view",
            definition="SELECT 1",
            on_conflict=OnConflict.ERROR,
        )

        # Commit
        catalog.catalog_transaction_commit(attach_id=attach_id, transaction_id=tx_id)

        # All should exist after commit
        assert (
            catalog.schema_get(
                attach_id=attach_id, transaction_id=None, name="tx_schema"
            )
            is not None
        )
        assert (
            catalog.table_get(
                attach_id=attach_id,
                transaction_id=None,
                schema_name="tx_schema",
                name="tx_table",
            )
            is not None
        )
        assert (
            catalog.view_get(
                attach_id=attach_id,
                transaction_id=None,
                schema_name="main",
                name="tx_view",
            )
            is not None
        )
