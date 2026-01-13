"""Unit tests for AttachmentStorage."""

from __future__ import annotations

import pyarrow as pa
import pytest

from vgi.catalog import AttachId, SerializedSchema, TransactionId
from vgi.ci.storage import (
    AttachmentNotFoundError,
    AttachmentStorage,
    TransactionError,
)


class TestAttachmentLifecycle:
    """Tests for attachment create/get/delete operations."""

    def test_create_attachment(self, storage: AttachmentStorage) -> None:
        """Creating attachment returns state with default schema."""
        attach_id = AttachId(b"test-id")
        state = storage.create_attachment(attach_id, "ci")

        assert state.catalog_name == "ci"
        assert state.version == 1
        assert "main" in state.schemas

    def test_get_attachment(self, storage: AttachmentStorage) -> None:
        """Getting existing attachment returns its state."""
        attach_id = AttachId(b"test-id")
        storage.create_attachment(attach_id, "test")

        state = storage.get_attachment(attach_id)
        assert state.catalog_name == "test"

    def test_get_attachment_not_found(self, storage: AttachmentStorage) -> None:
        """Getting non-existent attachment raises error."""
        attach_id = AttachId(b"nonexistent")

        with pytest.raises(AttachmentNotFoundError):
            storage.get_attachment(attach_id)

    def test_delete_attachment(self, storage: AttachmentStorage) -> None:
        """Deleting attachment removes it from storage."""
        attach_id = AttachId(b"test-id")
        storage.create_attachment(attach_id, "ci")

        storage.delete_attachment(attach_id)

        with pytest.raises(AttachmentNotFoundError):
            storage.get_attachment(attach_id)

    def test_delete_nonexistent_attachment(self, storage: AttachmentStorage) -> None:
        """Deleting non-existent attachment doesn't raise."""
        attach_id = AttachId(b"nonexistent")
        storage.delete_attachment(attach_id)  # Should not raise

    def test_list_attachments(self, storage: AttachmentStorage) -> None:
        """List attachments returns all attachment IDs."""
        id1 = AttachId(b"test-1")
        id2 = AttachId(b"test-2")
        storage.create_attachment(id1, "ci")
        storage.create_attachment(id2, "test")

        attachments = storage.list_attachments()
        assert set(attachments) == {id1, id2}

    def test_list_empty_storage(self, storage: AttachmentStorage) -> None:
        """List attachments returns empty list when no attachments."""
        assert storage.list_attachments() == []


class TestSchemaOperations:
    """Tests for schema CRUD operations."""

    def test_create_schema(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Creating schema adds it to attachment."""
        storage, attach_id = storage_with_attachment

        storage.create_schema(attach_id, "test_schema")

        schema = storage.get_schema(attach_id, "test_schema")
        assert schema is not None
        assert schema.info.name == "test_schema"

    def test_create_schema_with_comment_and_tags(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Creating schema with comment and tags preserves them."""
        storage, attach_id = storage_with_attachment

        storage.create_schema(
            attach_id, "my_schema", comment="Test comment", tags={"env": "test"}
        )

        schema = storage.get_schema(attach_id, "my_schema")
        assert schema is not None
        assert schema.info.comment == "Test comment"
        assert schema.info.tags == {"env": "test"}

    def test_create_duplicate_schema(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Creating duplicate schema raises error."""
        storage, attach_id = storage_with_attachment
        storage.create_schema(attach_id, "dup_schema")

        with pytest.raises(ValueError, match="already exists"):
            storage.create_schema(attach_id, "dup_schema")

    def test_drop_schema(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Dropping schema removes it."""
        storage, attach_id = storage_with_attachment
        storage.create_schema(attach_id, "to_drop")

        storage.drop_schema(attach_id, "to_drop")

        assert storage.get_schema(attach_id, "to_drop") is None

    def test_drop_nonexistent_schema(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Dropping non-existent schema raises error."""
        storage, attach_id = storage_with_attachment

        with pytest.raises(ValueError, match="not found"):
            storage.drop_schema(attach_id, "nonexistent")

    def test_drop_schema_ignore_not_found(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Dropping non-existent schema with ignore_not_found doesn't raise."""
        storage, attach_id = storage_with_attachment

        storage.drop_schema(attach_id, "nonexistent", ignore_not_found=True)

    def test_drop_nonempty_schema_without_cascade(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Dropping non-empty schema without cascade raises error."""
        storage, attach_id = storage_with_attachment
        storage.create_schema(attach_id, "has_tables")
        storage.create_table(attach_id, "has_tables", "test_table", sample_schema_bytes)

        with pytest.raises(ValueError, match="not empty"):
            storage.drop_schema(attach_id, "has_tables")

    def test_drop_nonempty_schema_with_cascade(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Dropping non-empty schema with cascade succeeds."""
        storage, attach_id = storage_with_attachment
        storage.create_schema(attach_id, "has_tables")
        storage.create_table(attach_id, "has_tables", "test_table", sample_schema_bytes)

        storage.drop_schema(attach_id, "has_tables", cascade=True)

        assert storage.get_schema(attach_id, "has_tables") is None

    def test_list_schemas(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """List schemas returns all schema infos."""
        storage, attach_id = storage_with_attachment
        storage.create_schema(attach_id, "schema1")
        storage.create_schema(attach_id, "schema2")

        schemas = list(storage.list_schemas(attach_id))
        names = {s.name for s in schemas}
        assert names == {"main", "schema1", "schema2"}

    def test_version_increments_on_schema_create(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Version increments when creating schema."""
        storage, attach_id = storage_with_attachment
        initial = storage.get_attachment(attach_id).version

        storage.create_schema(attach_id, "new_schema")

        assert storage.get_attachment(attach_id).version == initial + 1


class TestTableOperations:
    """Tests for table CRUD operations."""

    def test_create_table(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Creating table adds it to schema."""
        storage, attach_id = storage_with_attachment

        storage.create_table(attach_id, "main", "test_table", sample_schema_bytes)

        table = storage.get_table(attach_id, "main", "test_table")
        assert table is not None
        assert table.info.name == "test_table"
        assert table.data.num_rows == 0

    def test_create_table_with_constraints(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Creating table with constraints preserves them."""
        storage, attach_id = storage_with_attachment

        storage.create_table(
            attach_id,
            "main",
            "constrained",
            sample_schema_bytes,
            not_null_constraints=[0],
            unique_constraints=[[0], [0, 1]],
            check_constraints=["id > 0"],
        )

        table = storage.get_table(attach_id, "main", "constrained")
        assert table is not None
        assert table.info.not_null_constraints == [0]
        assert table.info.unique_constraints == [[0], [0, 1]]
        assert table.info.check_constraints == ["id > 0"]

    def test_create_table_nonexistent_schema(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Creating table in non-existent schema raises error."""
        storage, attach_id = storage_with_attachment

        with pytest.raises(ValueError, match="Schema.*not found"):
            storage.create_table(
                attach_id, "nonexistent", "test_table", sample_schema_bytes
            )

    def test_create_duplicate_table(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Creating duplicate table raises error."""
        storage, attach_id = storage_with_attachment
        storage.create_table(attach_id, "main", "dup_table", sample_schema_bytes)

        with pytest.raises(ValueError, match="already exists"):
            storage.create_table(attach_id, "main", "dup_table", sample_schema_bytes)

    def test_drop_table(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Dropping table removes it."""
        storage, attach_id = storage_with_attachment
        storage.create_table(attach_id, "main", "to_drop", sample_schema_bytes)

        storage.drop_table(attach_id, "main", "to_drop")

        assert storage.get_table(attach_id, "main", "to_drop") is None

    def test_drop_nonexistent_table(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Dropping non-existent table raises error."""
        storage, attach_id = storage_with_attachment

        with pytest.raises(ValueError, match="not found"):
            storage.drop_table(attach_id, "main", "nonexistent")

    def test_drop_table_ignore_not_found(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Dropping non-existent table with ignore_not_found doesn't raise."""
        storage, attach_id = storage_with_attachment

        storage.drop_table(attach_id, "main", "nonexistent", ignore_not_found=True)

    def test_insert_and_scan_data(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_schema: pa.Schema,
        sample_schema_bytes: SerializedSchema,
        sample_table_data: pa.Table,
    ) -> None:
        """Inserting data makes it scannable."""
        storage, attach_id = storage_with_attachment
        storage.create_table(attach_id, "main", "data_table", sample_schema_bytes)

        storage.insert_data(attach_id, "main", "data_table", sample_table_data)

        result = storage.scan_table(attach_id, "main", "data_table")
        assert result.num_rows == 3
        assert result.to_pydict() == sample_table_data.to_pydict()

    def test_insert_multiple_times(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_schema: pa.Schema,
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Multiple inserts concatenate data."""
        storage, attach_id = storage_with_attachment
        storage.create_table(attach_id, "main", "multi_insert", sample_schema_bytes)

        data1 = pa.table(
            {"id": [1], "name": ["a"], "value": [1.0]}, schema=sample_schema
        )
        data2 = pa.table(
            {"id": [2], "name": ["b"], "value": [2.0]}, schema=sample_schema
        )

        storage.insert_data(attach_id, "main", "multi_insert", data1)
        storage.insert_data(attach_id, "main", "multi_insert", data2)

        result = storage.scan_table(attach_id, "main", "multi_insert")
        assert result.num_rows == 2

    def test_insert_nonexistent_table(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_table_data: pa.Table,
    ) -> None:
        """Inserting into non-existent table raises error."""
        storage, attach_id = storage_with_attachment

        with pytest.raises(ValueError, match="not found"):
            storage.insert_data(attach_id, "main", "nonexistent", sample_table_data)

    def test_scan_nonexistent_table(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Scanning non-existent table raises error."""
        storage, attach_id = storage_with_attachment

        with pytest.raises(ValueError, match="not found"):
            storage.scan_table(attach_id, "main", "nonexistent")

    def test_rename_table(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Renaming table updates its name."""
        storage, attach_id = storage_with_attachment
        storage.create_table(attach_id, "main", "old_name", sample_schema_bytes)

        storage.rename_table(attach_id, "main", "old_name", "new_name")

        assert storage.get_table(attach_id, "main", "old_name") is None
        table = storage.get_table(attach_id, "main", "new_name")
        assert table is not None
        assert table.info.name == "new_name"

    def test_rename_table_preserves_data(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_schema_bytes: SerializedSchema,
        sample_table_data: pa.Table,
    ) -> None:
        """Renaming table preserves data."""
        storage, attach_id = storage_with_attachment
        storage.create_table(attach_id, "main", "with_data", sample_schema_bytes)
        storage.insert_data(attach_id, "main", "with_data", sample_table_data)

        storage.rename_table(attach_id, "main", "with_data", "renamed")

        result = storage.scan_table(attach_id, "main", "renamed")
        assert result.num_rows == 3

    def test_rename_to_existing_name(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Renaming table to existing name raises error."""
        storage, attach_id = storage_with_attachment
        storage.create_table(attach_id, "main", "table1", sample_schema_bytes)
        storage.create_table(attach_id, "main", "table2", sample_schema_bytes)

        with pytest.raises(ValueError, match="already exists"):
            storage.rename_table(attach_id, "main", "table1", "table2")

    def test_set_table_comment(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Setting table comment updates metadata."""
        storage, attach_id = storage_with_attachment
        storage.create_table(attach_id, "main", "commented", sample_schema_bytes)

        storage.set_table_comment(attach_id, "main", "commented", "My comment")

        table = storage.get_table(attach_id, "main", "commented")
        assert table is not None
        assert table.info.comment == "My comment"

    def test_clear_table_comment(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Setting None clears table comment."""
        storage, attach_id = storage_with_attachment
        storage.create_table(attach_id, "main", "commented", sample_schema_bytes)
        storage.set_table_comment(attach_id, "main", "commented", "Initial")
        storage.set_table_comment(attach_id, "main", "commented", None)

        table = storage.get_table(attach_id, "main", "commented")
        assert table is not None
        assert table.info.comment is None


class TestViewOperations:
    """Tests for view CRUD operations."""

    def test_create_view(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Creating view adds it to schema."""
        storage, attach_id = storage_with_attachment

        storage.create_view(attach_id, "main", "test_view", "SELECT 1")

        view = storage.get_view(attach_id, "main", "test_view")
        assert view is not None
        assert view.info.name == "test_view"
        assert view.info.definition == "SELECT 1"

    def test_create_duplicate_view(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Creating duplicate view raises error."""
        storage, attach_id = storage_with_attachment
        storage.create_view(attach_id, "main", "dup_view", "SELECT 1")

        with pytest.raises(ValueError, match="already exists"):
            storage.create_view(attach_id, "main", "dup_view", "SELECT 2")

    def test_drop_view(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Dropping view removes it."""
        storage, attach_id = storage_with_attachment
        storage.create_view(attach_id, "main", "to_drop", "SELECT 1")

        storage.drop_view(attach_id, "main", "to_drop")

        assert storage.get_view(attach_id, "main", "to_drop") is None

    def test_drop_nonexistent_view(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Dropping non-existent view raises error."""
        storage, attach_id = storage_with_attachment

        with pytest.raises(ValueError, match="not found"):
            storage.drop_view(attach_id, "main", "nonexistent")

    def test_drop_view_ignore_not_found(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Dropping non-existent view with ignore_not_found doesn't raise."""
        storage, attach_id = storage_with_attachment

        storage.drop_view(attach_id, "main", "nonexistent", ignore_not_found=True)

    def test_get_view_nonexistent_schema(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Getting view from non-existent schema returns None."""
        storage, attach_id = storage_with_attachment

        assert storage.get_view(attach_id, "nonexistent", "test") is None


class TestTransactionOperations:
    """Tests for transaction begin/commit/rollback."""

    def test_begin_transaction(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Beginning transaction returns transaction ID."""
        storage, attach_id = storage_with_attachment

        tx_id = storage.begin_transaction(attach_id)

        assert tx_id is not None
        assert len(tx_id) == 16  # UUID bytes

    def test_commit_transaction(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Committing transaction clears transaction state."""
        storage, attach_id = storage_with_attachment
        tx_id = storage.begin_transaction(attach_id)

        storage.commit_transaction(attach_id, tx_id)

        state = storage.get_attachment(attach_id)
        assert state.pending_tx is None
        assert state.tx_snapshot is None

    def test_rollback_transaction(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Rolling back transaction restores previous state."""
        storage, attach_id = storage_with_attachment
        tx_id = storage.begin_transaction(attach_id)
        storage.create_table(attach_id, "main", "new_table", sample_schema_bytes)

        storage.rollback_transaction(attach_id, tx_id)

        # Table should not exist after rollback
        assert storage.get_table(attach_id, "main", "new_table") is None

    def test_rollback_schema_changes(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Rolling back restores schema state."""
        storage, attach_id = storage_with_attachment
        tx_id = storage.begin_transaction(attach_id)
        storage.create_schema(attach_id, "new_schema")

        storage.rollback_transaction(attach_id, tx_id)

        assert storage.get_schema(attach_id, "new_schema") is None

    def test_nested_transaction_error(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Starting nested transaction raises error."""
        storage, attach_id = storage_with_attachment
        storage.begin_transaction(attach_id)

        with pytest.raises(TransactionError, match="already active"):
            storage.begin_transaction(attach_id)

    def test_commit_without_transaction(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Committing without active transaction raises error."""
        storage, attach_id = storage_with_attachment

        with pytest.raises(TransactionError, match="No transaction"):
            storage.commit_transaction(attach_id, TransactionId(b"fake-tx-id"))

    def test_commit_wrong_transaction_id(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Committing with wrong transaction ID raises error."""
        storage, attach_id = storage_with_attachment
        storage.begin_transaction(attach_id)

        with pytest.raises(TransactionError, match="mismatch"):
            storage.commit_transaction(attach_id, TransactionId(b"wrong-tx-id"))

    def test_rollback_without_transaction(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Rolling back without active transaction raises error."""
        storage, attach_id = storage_with_attachment

        with pytest.raises(TransactionError, match="No transaction"):
            storage.rollback_transaction(attach_id, TransactionId(b"fake-tx-id"))

    def test_rollback_wrong_transaction_id(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Rolling back with wrong transaction ID raises error."""
        storage, attach_id = storage_with_attachment
        storage.begin_transaction(attach_id)

        with pytest.raises(TransactionError, match="mismatch"):
            storage.rollback_transaction(attach_id, TransactionId(b"wrong-tx-id"))

    def test_changes_visible_before_commit(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Changes are visible during transaction."""
        storage, attach_id = storage_with_attachment
        storage.begin_transaction(attach_id)
        storage.create_table(attach_id, "main", "during_tx", sample_schema_bytes)

        # Table should be visible during transaction
        assert storage.get_table(attach_id, "main", "during_tx") is not None


class TestVersionTracking:
    """Tests for version increment tracking."""

    def test_initial_version(self, storage: AttachmentStorage) -> None:
        """New attachments start at version 1."""
        attach_id = AttachId(b"test")
        state = storage.create_attachment(attach_id, "ci")
        assert state.version == 1

    def test_schema_create_increments_version(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Creating schema increments version."""
        storage, attach_id = storage_with_attachment
        v1 = storage.get_attachment(attach_id).version

        storage.create_schema(attach_id, "new")

        v2 = storage.get_attachment(attach_id).version
        assert v2 == v1 + 1

    def test_table_create_increments_version(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Creating table increments version."""
        storage, attach_id = storage_with_attachment
        v1 = storage.get_attachment(attach_id).version

        storage.create_table(attach_id, "main", "t", sample_schema_bytes)

        v2 = storage.get_attachment(attach_id).version
        assert v2 == v1 + 1

    def test_table_drop_increments_version(
        self,
        storage_with_attachment: tuple[AttachmentStorage, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Dropping table increments version."""
        storage, attach_id = storage_with_attachment
        storage.create_table(attach_id, "main", "t", sample_schema_bytes)
        v1 = storage.get_attachment(attach_id).version

        storage.drop_table(attach_id, "main", "t")

        v2 = storage.get_attachment(attach_id).version
        assert v2 == v1 + 1

    def test_view_create_increments_version(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Creating view increments version."""
        storage, attach_id = storage_with_attachment
        v1 = storage.get_attachment(attach_id).version

        storage.create_view(attach_id, "main", "v", "SELECT 1")

        v2 = storage.get_attachment(attach_id).version
        assert v2 == v1 + 1
