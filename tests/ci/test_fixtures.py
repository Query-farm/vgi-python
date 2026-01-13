"""Test that CI fixtures work correctly."""

from __future__ import annotations

import pyarrow as pa

from vgi.catalog import AttachId
from vgi.ci.catalog import CICatalog
from vgi.ci.storage import AttachmentState, AttachmentStorage, SchemaData, TableData


class TestStorageFixtures:
    """Test storage-related fixtures."""

    def test_storage_fixture(self, storage: AttachmentStorage) -> None:
        """Storage fixture provides fresh instance."""
        assert isinstance(storage, AttachmentStorage)

    def test_attach_id_fixture(self, attach_id: AttachId) -> None:
        """Attach ID fixture provides valid ID."""
        assert isinstance(attach_id, bytes)
        assert len(attach_id) > 0

    def test_storage_with_attachment(
        self, storage_with_attachment: tuple[AttachmentStorage, AttachId]
    ) -> None:
        """Storage with attachment has pre-created attachment."""
        storage, attach_id = storage_with_attachment
        state = storage.get_attachment(attach_id)
        assert state.catalog_name == "ci"


class TestCatalogFixtures:
    """Test catalog-related fixtures."""

    def test_catalog_fixture(self, catalog: CICatalog) -> None:
        """Catalog fixture provides fresh instance."""
        assert isinstance(catalog, CICatalog)

    def test_attached_catalog(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Attached catalog has active attachment."""
        catalog, attach_id = attached_catalog
        version = catalog.catalog_version(attach_id=attach_id, transaction_id=None)
        assert version >= 1


class TestSchemaFixtures:
    """Test schema-related fixtures."""

    def test_sample_schema(self, sample_schema: pa.Schema) -> None:
        """Sample schema has expected fields."""
        assert len(sample_schema) == 3
        assert sample_schema.field("id").type == pa.int64()
        assert sample_schema.field("name").type == pa.string()
        assert sample_schema.field("value").type == pa.float64()

    def test_sample_schema_bytes(self, sample_schema_bytes: bytes) -> None:
        """Sample schema bytes is valid serialized schema."""
        assert isinstance(sample_schema_bytes, bytes)
        assert len(sample_schema_bytes) > 0
        # Verify it can be deserialized
        schema = pa.ipc.read_schema(pa.py_buffer(sample_schema_bytes))
        assert len(schema) == 3

    def test_simple_schema(self, simple_schema: pa.Schema) -> None:
        """Simple schema has expected fields."""
        assert len(simple_schema) == 2
        assert simple_schema.field("id").type == pa.int64()
        assert simple_schema.field("value").type == pa.int64()


class TestTableDataFixtures:
    """Test table data fixtures."""

    def test_sample_table_data(self, sample_table_data: pa.Table) -> None:
        """Sample table data has expected content."""
        assert sample_table_data.num_rows == 3
        assert sample_table_data.num_columns == 3

    def test_sample_batch(self, sample_batch: pa.RecordBatch) -> None:
        """Sample batch has expected content."""
        assert sample_batch.num_rows == 3
        assert sample_batch.num_columns == 3

    def test_empty_table(self, empty_table: pa.Table) -> None:
        """Empty table has zero rows."""
        assert empty_table.num_rows == 0
        assert empty_table.num_columns == 3


class TestDataStructureFixtures:
    """Test internal data structure fixtures."""

    def test_table_data(self, table_data: TableData) -> None:
        """Table data fixture has info and data."""
        assert table_data.info is not None
        assert table_data.data is not None
        assert table_data.data.num_rows == 3

    def test_schema_data(self, schema_data: SchemaData) -> None:
        """Schema data fixture has empty tables/views."""
        assert schema_data.info is not None
        assert schema_data.tables == {}
        assert schema_data.views == {}

    def test_attachment_state(self, attachment_state: AttachmentState) -> None:
        """Attachment state fixture has expected defaults."""
        assert attachment_state.catalog_name == "ci"
        assert attachment_state.schemas == {}
        assert attachment_state.version == 1
        assert attachment_state.pending_tx is None
