"""Shared fixtures for CI worker tests."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from vgi.catalog import AttachId, SchemaInfo, SerializedSchema, TableInfo
from vgi.ci.catalog import CICatalog
from vgi.ci.storage import AttachmentState, AttachmentStorage, SchemaData, TableData

# =============================================================================
# Storage Fixtures
# =============================================================================


@pytest.fixture
def storage() -> AttachmentStorage:
    """Provide a fresh AttachmentStorage instance."""
    return AttachmentStorage()


@pytest.fixture
def attach_id() -> AttachId:
    """Provide a test attach ID."""
    return AttachId(b"test-attachment-id-1")


@pytest.fixture
def attach_id_2() -> AttachId:
    """Provide a second test attach ID for isolation tests."""
    return AttachId(b"test-attachment-id-2")


@pytest.fixture
def storage_with_attachment(
    storage: AttachmentStorage, attach_id: AttachId
) -> tuple[AttachmentStorage, AttachId]:
    """Provide storage with a pre-created attachment."""
    storage.create_attachment(attach_id, "ci")
    return storage, attach_id


# =============================================================================
# Catalog Fixtures
# =============================================================================


@pytest.fixture
def catalog() -> CICatalog:
    """Provide a fresh CICatalog instance."""
    return CICatalog()


@pytest.fixture
def attached_catalog(catalog: CICatalog) -> tuple[CICatalog, AttachId]:
    """Provide a catalog with an active attachment.

    Returns:
        Tuple of (catalog, attach_id) for the active attachment.

    """
    result = catalog.catalog_attach(name="ci", options={})
    return catalog, result.attach_id


# =============================================================================
# Schema Fixtures
# =============================================================================


@pytest.fixture
def sample_schema() -> pa.Schema:
    """Provide a sample Arrow schema for testing."""
    fields: list[pa.Field[Any]] = [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("value", pa.float64()),
    ]
    return pa.schema(fields)


@pytest.fixture
def sample_schema_bytes(sample_schema: pa.Schema) -> SerializedSchema:
    """Provide a serialized Arrow schema for table creation."""
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, sample_schema)
    writer.close()
    return SerializedSchema(sink.getvalue().to_pybytes())


@pytest.fixture
def simple_schema() -> pa.Schema:
    """Provide a simple Arrow schema with just id and value."""
    fields: list[pa.Field[Any]] = [
        pa.field("id", pa.int64()),
        pa.field("value", pa.int64()),
    ]
    return pa.schema(fields)


@pytest.fixture
def simple_schema_bytes(simple_schema: pa.Schema) -> SerializedSchema:
    """Provide a serialized simple schema."""
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, simple_schema)
    writer.close()
    return SerializedSchema(sink.getvalue().to_pybytes())


# =============================================================================
# Table Data Fixtures
# =============================================================================


@pytest.fixture
def sample_table_data(sample_schema: pa.Schema) -> pa.Table:
    """Provide sample table data for testing."""
    return pa.table(
        {
            "id": [1, 2, 3],
            "name": ["alice", "bob", "charlie"],
            "value": [10.5, 20.0, 30.25],
        },
        schema=sample_schema,
    )


@pytest.fixture
def sample_batch(sample_schema: pa.Schema) -> pa.RecordBatch:
    """Provide a sample record batch for testing."""
    return pa.RecordBatch.from_pydict(
        {
            "id": [1, 2, 3],
            "name": ["alice", "bob", "charlie"],
            "value": [10.5, 20.0, 30.25],
        },
        schema=sample_schema,
    )


@pytest.fixture
def empty_table(sample_schema: pa.Schema) -> pa.Table:
    """Provide an empty table with the sample schema."""
    return pa.table(
        {
            "id": [],
            "name": [],
            "value": [],
        },
        schema=sample_schema,
    )


# =============================================================================
# Schema Info Fixtures
# =============================================================================


@pytest.fixture
def main_schema_info(attach_id: AttachId) -> SchemaInfo:
    """Provide SchemaInfo for a 'main' schema."""
    return SchemaInfo(
        attach_id=attach_id,
        name="main",
        is_default=True,
        comment=None,
        tags={},
    )


@pytest.fixture
def test_schema_info(attach_id: AttachId) -> SchemaInfo:
    """Provide SchemaInfo for a 'test' schema."""
    return SchemaInfo(
        attach_id=attach_id,
        name="test",
        is_default=False,
        comment="Test schema",
        tags={"env": "test"},
    )


# =============================================================================
# Table Info Fixtures
# =============================================================================


@pytest.fixture
def users_table_info(sample_schema_bytes: SerializedSchema) -> TableInfo:
    """Provide TableInfo for a 'users' table."""
    return TableInfo(
        name="users",
        schema_name="main",
        columns=sample_schema_bytes,
        comment=None,
        not_null_constraints=[0],  # id column
        unique_constraints=[[0]],  # id column is unique
        check_constraints=[],
        tags={},
    )


# =============================================================================
# Internal Data Structure Fixtures
# =============================================================================


@pytest.fixture
def table_data(users_table_info: TableInfo, sample_table_data: pa.Table) -> TableData:
    """Provide a TableData instance for testing."""
    return TableData(
        info=users_table_info,
        data=sample_table_data,
    )


@pytest.fixture
def schema_data(main_schema_info: SchemaInfo) -> SchemaData:
    """Provide an empty SchemaData instance for testing."""
    return SchemaData(
        info=main_schema_info,
        tables={},
        views={},
    )


@pytest.fixture
def attachment_state() -> AttachmentState:
    """Provide a fresh AttachmentState instance."""
    return AttachmentState(catalog_name="ci")
