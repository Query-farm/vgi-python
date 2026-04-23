"""Tests for time travel query support."""

from __future__ import annotations

import pyarrow as pa
import pytest

from vgi.catalog.catalog_interface import (
    AttachId,
)
from vgi.catalog.descriptors import Catalog, Schema, Table
from vgi.examples.table import (
    _CURRENT_VERSION,
    _VERSIONED_DATA,
    _VERSIONED_SCHEMAS,
    resolve_version,
)
from vgi.examples.worker import ExampleWorker

# ---------------------------------------------------------------------------
# resolve_version tests
# ---------------------------------------------------------------------------


class TestResolveVersion:
    """Tests for the resolve_version helper."""

    def test_no_at_clause_returns_current(self) -> None:
        """No AT clause returns the current version."""
        assert resolve_version(None, None) == _CURRENT_VERSION

    def test_version_unit(self) -> None:
        """VERSION unit returns the exact integer version."""
        assert resolve_version("VERSION", "1") == 1
        assert resolve_version("VERSION", "2") == 2
        assert resolve_version("VERSION", "3") == 3

    def test_version_unit_case_insensitive(self) -> None:
        """VERSION unit matching is case-insensitive."""
        assert resolve_version("version", "2") == 2

    def test_timestamp_year_2020(self) -> None:
        """Timestamp in 2020 maps to version 1."""
        assert resolve_version("TIMESTAMP", "2020-06-15 00:00:00") == 1

    def test_timestamp_year_2021(self) -> None:
        """Timestamp in 2021 maps to version 2."""
        assert resolve_version("TIMESTAMP", "2021-06-15 00:00:00") == 2

    def test_timestamp_year_2022(self) -> None:
        """Timestamp in 2022 maps to version 3."""
        assert resolve_version("TIMESTAMP", "2022-06-15 00:00:00") == 3

    def test_timestamp_year_2023(self) -> None:
        """Timestamp in 2023 maps to version 3."""
        assert resolve_version("TIMESTAMP", "2023-01-01 00:00:00") == 3

    def test_unsupported_unit(self) -> None:
        """Unsupported AT unit raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported at_unit"):
            resolve_version("SNAPSHOT", "42")

    def test_timestamp_before_table_exists_errors(self) -> None:
        """Timestamp before the table existed raises ValueError."""
        with pytest.raises(ValueError, match="table did not exist before 2020"):
            resolve_version("TIMESTAMP", "1990-01-01 00:00:00")

    def test_timestamp_far_future(self) -> None:
        """Timestamp far in the future maps to current version."""
        assert resolve_version("TIMESTAMP", "2099-01-01 00:00:00") == _CURRENT_VERSION

    def test_version_zero_errors(self) -> None:
        """Version 0 does not exist and raises ValueError."""
        with pytest.raises(ValueError, match="Unknown version: 0"):
            resolve_version("VERSION", "0")

    def test_version_out_of_range_errors(self) -> None:
        """Large version does not exist and raises ValueError."""
        with pytest.raises(ValueError, match="Unknown version: 99"):
            resolve_version("VERSION", "99")

    def test_negative_version_errors(self) -> None:
        """Negative version does not exist and raises ValueError."""
        with pytest.raises(ValueError, match="Unknown version: -1"):
            resolve_version("VERSION", "-1")

    def test_timestamp_boundary_year_2020(self) -> None:
        """Timestamp exactly at 2020-01-01 maps to version 1."""
        assert resolve_version("TIMESTAMP", "2020-01-01 00:00:00") == 1


# ---------------------------------------------------------------------------
# XOR validation for at_unit/at_value pairing
# ---------------------------------------------------------------------------


class TestAtParamValidation:
    """Tests for at_unit/at_value XOR validation."""

    def test_at_unit_without_at_value_errors(self) -> None:
        """at_unit set without at_value raises ValueError."""
        worker = ExampleWorker()
        cat = worker._get_catalog()
        with pytest.raises(ValueError, match="at_unit and at_value must both be provided"):
            cat.table_get(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                schema_name="data",
                name="versioned_data",
                at_unit="VERSION",
                at_value=None,
            )

    def test_at_value_without_at_unit_errors(self) -> None:
        """at_value set without at_unit raises ValueError."""
        worker = ExampleWorker()
        cat = worker._get_catalog()
        with pytest.raises(ValueError, match="at_unit and at_value must both be provided"):
            cat.table_get(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                schema_name="data",
                name="versioned_data",
                at_unit=None,
                at_value="1",
            )

    def test_scan_at_unit_without_at_value_errors(self) -> None:
        """table_scan_function_get with at_unit but no at_value raises ValueError."""
        worker = ExampleWorker()
        cat = worker._get_catalog()
        with pytest.raises(ValueError, match="at_unit and at_value must both be provided"):
            cat.table_scan_function_get(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                schema_name="data",
                name="versioned_data",
                at_unit="VERSION",
                at_value=None,
            )


# ---------------------------------------------------------------------------
# VersionedDataFunction schema per version
# ---------------------------------------------------------------------------


class TestVersionedDataFunction:
    """Tests for VersionedDataFunction schemas and data."""

    def test_version_1_schema(self) -> None:
        """Version 1 has only the id column."""
        schema = _VERSIONED_SCHEMAS[1]
        assert schema.names == ["id"]
        assert schema.field("id").type == pa.int64()

    def test_version_2_schema(self) -> None:
        """Version 2 has id, name, score, active columns."""
        schema = _VERSIONED_SCHEMAS[2]
        assert schema.names == ["id", "name", "score", "active"]

    def test_version_3_schema(self) -> None:
        """Version 3 has id and score columns."""
        schema = _VERSIONED_SCHEMAS[3]
        assert schema.names == ["id", "score"]

    def test_version_1_data(self) -> None:
        """Version 1 has 3 rows."""
        data = _VERSIONED_DATA[1]
        assert data["id"] == [1, 2, 3]

    def test_version_2_data(self) -> None:
        """Version 2 has 5 rows with names."""
        data = _VERSIONED_DATA[2]
        assert len(data["id"]) == 5
        assert data["name"] == ["alice", "bob", "carol", "dave", "eve"]

    def test_version_3_data(self) -> None:
        """Version 3 has 4 rows with scores."""
        data = _VERSIONED_DATA[3]
        assert len(data["id"]) == 4
        assert data["score"] == [15.0, 25.0, 35.0, 45.0]


# ---------------------------------------------------------------------------
# Catalog attach with time travel flag
# ---------------------------------------------------------------------------


class TestCatalogAttachTimeTravel:
    """Tests for supports_time_travel flag on catalog_attach."""

    def test_catalog_with_time_travel_table(self) -> None:
        """Catalog with a time-travel table reports supports_time_travel=True."""
        worker = ExampleWorker()
        cat = worker._get_catalog()
        result = cat.catalog_attach(name="example", options={}, data_version_spec=None, implementation_version=None)
        assert result.supports_time_travel is True

    def test_catalog_without_time_travel_table(self) -> None:
        """Catalog with no time-travel tables reports supports_time_travel=False."""
        from vgi.catalog.catalog_interface import ReadOnlyCatalogInterface

        class NoCatalog(ReadOnlyCatalogInterface):
            catalog = Catalog(
                name="test",
                schemas=[
                    Schema(
                        name="main",
                        tables=[
                            Table(name="t1", columns=pa.schema([pa.field("x", pa.int64())])),
                        ],
                    )
                ],
            )

        cat = NoCatalog()
        result = cat.catalog_attach(name="test", options={}, data_version_spec=None, implementation_version=None)
        assert result.supports_time_travel is False


# ---------------------------------------------------------------------------
# table_get with AT params
# ---------------------------------------------------------------------------


class TestTableGetTimeTravel:
    """Tests for table_get with at_unit/at_value."""

    def test_versioned_data_version_1(self) -> None:
        """table_get returns version 1 schema when AT VERSION => 1."""
        worker = ExampleWorker()
        cat = worker._get_catalog()
        result = cat.table_get(
            attach_id=AttachId(b"test"),
            transaction_id=None,
            schema_name="data",
            name="versioned_data",
            at_unit="VERSION",
            at_value="1",
        )
        assert result is not None
        schema = pa.ipc.read_schema(pa.py_buffer(result.columns))
        assert schema.names == ["id"]

    def test_versioned_data_version_2(self) -> None:
        """table_get returns version 2 schema when AT VERSION => 2."""
        worker = ExampleWorker()
        cat = worker._get_catalog()
        result = cat.table_get(
            attach_id=AttachId(b"test"),
            transaction_id=None,
            schema_name="data",
            name="versioned_data",
            at_unit="VERSION",
            at_value="2",
        )
        assert result is not None
        schema = pa.ipc.read_schema(pa.py_buffer(result.columns))
        assert schema.names == ["id", "name", "score", "active"]

    def test_versioned_data_version_3(self) -> None:
        """table_get returns version 3 (current) schema when AT VERSION => 3."""
        worker = ExampleWorker()
        cat = worker._get_catalog()
        result = cat.table_get(
            attach_id=AttachId(b"test"),
            transaction_id=None,
            schema_name="data",
            name="versioned_data",
            at_unit="VERSION",
            at_value="3",
        )
        assert result is not None
        schema = pa.ipc.read_schema(pa.py_buffer(result.columns))
        assert schema.names == ["id", "score"]

    def test_versioned_data_no_at(self) -> None:
        """table_get without AT returns current (version 3) schema."""
        worker = ExampleWorker()
        cat = worker._get_catalog()
        result = cat.table_get(
            attach_id=AttachId(b"test"),
            transaction_id=None,
            schema_name="data",
            name="versioned_data",
        )
        assert result is not None
        schema = pa.ipc.read_schema(pa.py_buffer(result.columns))
        assert schema.names == ["id", "score"]

    def test_at_on_non_time_travel_table_errors(self) -> None:
        """AT clause on a non-time-travel table raises ValueError."""
        worker = ExampleWorker()
        cat = worker._get_catalog()
        with pytest.raises(ValueError, match="does not support time travel"):
            cat.table_get(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                schema_name="data",
                name="numbers",
                at_unit="VERSION",
                at_value="1",
            )

    def test_version_zero_errors(self) -> None:
        """Version 0 is out of bounds and raises ValueError."""
        worker = ExampleWorker()
        cat = worker._get_catalog()
        with pytest.raises(ValueError, match="Unknown version"):
            cat.table_get(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                schema_name="data",
                name="versioned_data",
                at_unit="VERSION",
                at_value="0",
            )

    def test_version_99_errors(self) -> None:
        """Version 99 is out of bounds and raises ValueError."""
        worker = ExampleWorker()
        cat = worker._get_catalog()
        with pytest.raises(ValueError, match="Unknown version"):
            cat.table_get(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                schema_name="data",
                name="versioned_data",
                at_unit="VERSION",
                at_value="99",
            )

    def test_negative_version_errors(self) -> None:
        """Negative version is out of bounds and raises ValueError."""
        worker = ExampleWorker()
        cat = worker._get_catalog()
        with pytest.raises(ValueError, match="Unknown version"):
            cat.table_get(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                schema_name="data",
                name="versioned_data",
                at_unit="VERSION",
                at_value="-1",
            )

    def test_timestamp_before_table_exists_errors(self) -> None:
        """Timestamp before the table existed raises ValueError."""
        worker = ExampleWorker()
        cat = worker._get_catalog()
        with pytest.raises(ValueError, match="table did not exist before 2020"):
            cat.table_get(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                schema_name="data",
                name="versioned_data",
                at_unit="TIMESTAMP",
                at_value="1990-01-01 00:00:00",
            )

    def test_timestamp_far_future_returns_current(self) -> None:
        """Timestamp far in the future returns current version schema."""
        worker = ExampleWorker()
        cat = worker._get_catalog()
        result = cat.table_get(
            attach_id=AttachId(b"test"),
            transaction_id=None,
            schema_name="data",
            name="versioned_data",
            at_unit="TIMESTAMP",
            at_value="2099-01-01 00:00:00",
        )
        assert result is not None
        schema = pa.ipc.read_schema(pa.py_buffer(result.columns))
        assert schema.names == ["id", "score"]


# ---------------------------------------------------------------------------
# table_scan_function_get routing
# ---------------------------------------------------------------------------


class TestTableScanFunctionGetTimeTravel:
    """Tests for table_scan_function_get time-travel routing."""

    def test_versioned_data_version(self) -> None:
        """Scan function routes to versioned_data_scan with version arg."""
        worker = ExampleWorker()
        cat = worker._get_catalog()
        result = cat.table_scan_function_get(
            attach_id=AttachId(b"test"),
            transaction_id=None,
            schema_name="data",
            name="versioned_data",
            at_unit="VERSION",
            at_value="2",
        )
        assert result.function_name == "versioned_data_scan"
        assert len(result.positional_arguments) == 1
        assert result.positional_arguments[0].as_py() == 2

    def test_versioned_data_timestamp(self) -> None:
        """Scan function routes correctly for TIMESTAMP AT clause."""
        worker = ExampleWorker()
        cat = worker._get_catalog()
        result = cat.table_scan_function_get(
            attach_id=AttachId(b"test"),
            transaction_id=None,
            schema_name="data",
            name="versioned_data",
            at_unit="TIMESTAMP",
            at_value="2020-06-15 00:00:00",
        )
        assert result.function_name == "versioned_data_scan"
        assert result.positional_arguments[0].as_py() == 1

    def test_versioned_data_no_at(self) -> None:
        """Scan function defaults to current version when no AT clause."""
        worker = ExampleWorker()
        cat = worker._get_catalog()
        result = cat.table_scan_function_get(
            attach_id=AttachId(b"test"),
            transaction_id=None,
            schema_name="data",
            name="versioned_data",
            at_unit=None,
            at_value=None,
        )
        assert result.function_name == "versioned_data_scan"
        assert result.positional_arguments[0].as_py() == _CURRENT_VERSION

    def test_at_on_non_time_travel_table_errors(self) -> None:
        """AT clause on non-time-travel table raises ValueError."""
        worker = ExampleWorker()
        cat = worker._get_catalog()
        with pytest.raises(ValueError, match="does not support time travel"):
            cat.table_scan_function_get(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                schema_name="data",
                name="numbers",
                at_unit="VERSION",
                at_value="1",
            )
