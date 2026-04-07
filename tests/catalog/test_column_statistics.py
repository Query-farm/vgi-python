# ruff: noqa: S101, D102
"""Tests for column statistics serialization, deserialization, and helpers."""

from __future__ import annotations

import pyarrow as pa
import pytest
from vgi_rpc.utils import deserialize_record_batch

from vgi.catalog.catalog_interface import (
    ColumnStatistics,
    TableColumnStatisticsResult,
    serialize_column_statistics,
)
from vgi.catalog.descriptors import ColumnStatisticsInput, Table

# ============================================================================
# serialize_column_statistics
# ============================================================================


class TestSerializeColumnStatistics:
    """Tests for the sparse-union serialization of column statistics."""

    def test_empty_stats(self) -> None:
        """Empty stats list produces a valid zero-row batch."""
        data = serialize_column_statistics([])
        batch, _ = deserialize_record_batch(data)
        assert batch.num_rows == 0
        assert "column_name" in batch.schema.names
        assert "min" in batch.schema.names
        assert "max" in batch.schema.names

    def test_single_int_column(self) -> None:
        """Single int64 column roundtrips correctly."""
        stats = [
            ColumnStatistics(
                column_name="id",
                min=pa.scalar(1, pa.int64()),
                max=pa.scalar(100, pa.int64()),
                has_null=False,
                has_not_null=True,
                distinct_count=100,
            ),
        ]
        data = serialize_column_statistics(stats)
        batch, _ = deserialize_record_batch(data)

        assert batch.num_rows == 1
        assert batch.column("column_name")[0].as_py() == "id"
        assert batch.column("has_null")[0].as_py() is False
        assert batch.column("has_not_null")[0].as_py() is True
        assert batch.column("distinct_count")[0].as_py() == 100

        # Verify min/max are in the sparse union
        assert batch.column("min")[0].as_py() == 1
        assert batch.column("max")[0].as_py() == 100

    def test_mixed_types(self) -> None:
        """Multiple columns with different types produce a multi-child sparse union."""
        stats = [
            ColumnStatistics(
                column_name="id",
                min=pa.scalar(1, pa.int64()),
                max=pa.scalar(10, pa.int64()),
                has_null=False,
                has_not_null=True,
            ),
            ColumnStatistics(
                column_name="name",
                min=pa.scalar("Alice", pa.string()),
                max=pa.scalar("Zara", pa.string()),
                has_null=True,
                has_not_null=True,
            ),
            ColumnStatistics(
                column_name="score",
                min=pa.scalar(0.5, pa.float64()),
                max=pa.scalar(99.9, pa.float64()),
                has_null=False,
                has_not_null=True,
            ),
        ]
        data = serialize_column_statistics(stats)
        batch, _ = deserialize_record_batch(data)

        assert batch.num_rows == 3
        # Verify the sparse union has 3 child types
        min_type = batch.schema.field("min").type
        assert pa.types.is_union(min_type)
        assert min_type.num_fields == 3

        # Verify values roundtrip
        names = [batch.column("column_name")[i].as_py() for i in range(3)]
        assert names == ["id", "name", "score"]
        assert batch.column("min")[0].as_py() == 1
        assert batch.column("min")[1].as_py() == "Alice"
        assert batch.column("min")[2].as_py() == pytest.approx(0.5)

    def test_all_null_minmax(self) -> None:
        """Column with no known min/max produces null union values."""
        stats = [
            ColumnStatistics(
                column_name="mystery",
                min=None,
                max=None,
                has_null=True,
                has_not_null=False,
                distinct_count=0,
            ),
        ]
        data = serialize_column_statistics(stats)
        batch, _ = deserialize_record_batch(data)

        assert batch.num_rows == 1
        assert not batch.column("min")[0].is_valid
        assert not batch.column("max")[0].is_valid

    def test_cache_metadata_present(self) -> None:
        """cache_max_age_seconds appears as IPC custom_metadata."""
        stats = [
            ColumnStatistics(
                column_name="x",
                min=pa.scalar(0, pa.int32()),
                max=pa.scalar(1, pa.int32()),
            ),
        ]
        data = serialize_column_statistics(stats, cache_max_age_seconds=3600)
        _, custom_metadata = deserialize_record_batch(data)

        assert custom_metadata is not None
        assert b"cache_max_age_seconds" in custom_metadata
        assert custom_metadata[b"cache_max_age_seconds"] == b"3600"

    def test_cache_metadata_absent(self) -> None:
        """cache_max_age_seconds=None produces no custom_metadata key."""
        stats = [
            ColumnStatistics(
                column_name="x",
                min=pa.scalar(0, pa.int32()),
                max=pa.scalar(1, pa.int32()),
            ),
        ]
        data = serialize_column_statistics(stats, cache_max_age_seconds=None)
        _, custom_metadata = deserialize_record_batch(data)

        if custom_metadata is not None:
            assert b"cache_max_age_seconds" not in custom_metadata

    def test_cache_metadata_zero(self) -> None:
        """cache_max_age_seconds=0 (no caching) is preserved."""
        stats = [
            ColumnStatistics(
                column_name="x",
                min=pa.scalar(0, pa.int32()),
                max=pa.scalar(1, pa.int32()),
            ),
        ]
        data = serialize_column_statistics(stats, cache_max_age_seconds=0)
        _, custom_metadata = deserialize_record_batch(data)

        assert custom_metadata is not None
        assert custom_metadata[b"cache_max_age_seconds"] == b"0"

    def test_string_optional_fields(self) -> None:
        """contains_unicode and max_string_length are preserved."""
        stats = [
            ColumnStatistics(
                column_name="name",
                min=pa.scalar("a", pa.string()),
                max=pa.scalar("z", pa.string()),
                contains_unicode=True,
                max_string_length=255,
            ),
        ]
        data = serialize_column_statistics(stats)
        batch, _ = deserialize_record_batch(data)

        assert batch.column("contains_unicode")[0].as_py() is True
        assert batch.column("max_string_length")[0].as_py() == 255

    def test_homogeneous_types_single_union_child(self) -> None:
        """All-int64 table produces sparse union with 1 child type."""
        stats = [
            ColumnStatistics(
                column_name=f"col{i}",
                min=pa.scalar(i, pa.int64()),
                max=pa.scalar(i * 10, pa.int64()),
            )
            for i in range(5)
        ]
        data = serialize_column_statistics(stats)
        batch, _ = deserialize_record_batch(data)

        min_type = batch.schema.field("min").type
        assert pa.types.is_union(min_type)
        assert min_type.num_fields == 1


# ============================================================================
# ColumnStatisticsInput
# ============================================================================


class TestColumnStatisticsInput:
    """Tests for ColumnStatisticsInput.resolve() with plain values and pa.Scalar."""

    def test_plain_int(self) -> None:
        si = ColumnStatisticsInput(min=1, max=100, has_null=False, distinct_count=50)
        cs = si.resolve("id", pa.int64())

        assert cs.column_name == "id"
        assert isinstance(cs.min, pa.Scalar)
        assert cs.min.type == pa.int64()
        assert cs.min.as_py() == 1
        assert cs.max.as_py() == 100  # type: ignore[union-attr]

    def test_plain_string(self) -> None:
        si = ColumnStatisticsInput(min="Alice", max="Zara")
        cs = si.resolve("name", pa.string())

        assert cs.min.type == pa.string()  # type: ignore[union-attr]
        assert cs.min.as_py() == "Alice"  # type: ignore[union-attr]

    def test_plain_float(self) -> None:
        si = ColumnStatisticsInput(min=0.5, max=99.9)
        cs = si.resolve("price", pa.float64())

        assert cs.min.type == pa.float64()  # type: ignore[union-attr]
        assert cs.min.as_py() == pytest.approx(0.5)  # type: ignore[union-attr]

    def test_explicit_scalar_passthrough(self) -> None:
        explicit_min = pa.scalar(42, pa.int32())
        si = ColumnStatisticsInput(min=explicit_min, max=pa.scalar(100, pa.int32()))
        cs = si.resolve("id", pa.int64())

        assert cs.min is explicit_min
        assert cs.min.type == pa.int32()

    def test_none_values(self) -> None:
        si = ColumnStatisticsInput(min=None, max=None)
        cs = si.resolve("x", pa.int64())

        assert cs.min is None
        assert cs.max is None


# ============================================================================
# Table descriptor integration
# ============================================================================


class TestTableDescriptorStatistics:
    """Tests for Table(statistics=...) and resolve_column_statistics()."""

    def test_inline_statistics(self) -> None:
        table = Table(
            name="test_table",
            columns=pa.schema(
                [pa.field("id", pa.int64()), pa.field("name", pa.string())]  # type: ignore[arg-type]
            ),
            statistics={
                "id": ColumnStatisticsInput(min=1, max=100, has_null=False, distinct_count=100),
                "name": ColumnStatisticsInput(min="A", max="Z", distinct_count=26),
            },
            statistics_cache_max_age_seconds=600,
        )

        result = table.resolve_column_statistics()
        assert result is not None
        assert isinstance(result, TableColumnStatisticsResult)
        assert len(result.statistics) == 2
        assert result.cache_max_age_seconds == 600

        id_stat = result.statistics[0]
        assert id_stat.column_name == "id"
        assert id_stat.min.type == pa.int64()  # type: ignore[union-attr]
        assert id_stat.min.as_py() == 1  # type: ignore[union-attr]

        name_stat = result.statistics[1]
        assert name_stat.column_name == "name"
        assert name_stat.min.type == pa.string()  # type: ignore[union-attr]

    def test_no_statistics(self) -> None:
        table = Table(name="plain", columns=pa.schema([("x", pa.int64())]))
        assert table.resolve_column_statistics() is None

    def test_supports_column_statistics_auto_derived(self) -> None:
        table_with = Table(
            name="with_stats",
            columns=pa.schema([("x", pa.int64())]),
            statistics={"x": ColumnStatisticsInput(min=0, max=1)},
        )
        table_without = Table(name="no_stats", columns=pa.schema([("x", pa.int64())]))

        assert table_with.to_table_info("main").supports_column_statistics is True
        assert table_without.to_table_info("main").supports_column_statistics is False

    def test_invalid_statistics_column_name(self) -> None:
        with pytest.raises(ValueError, match="statistics column 'nonexistent' not found"):
            Table(
                name="bad",
                columns=pa.schema([("x", pa.int64())]),
                statistics={"nonexistent": ColumnStatisticsInput(min=0, max=1)},
            )

    def test_full_roundtrip(self) -> None:
        table = Table(
            name="roundtrip",
            columns=pa.schema(
                [pa.field("a", pa.int32()), pa.field("b", pa.float64())]  # type: ignore[arg-type]
            ),
            statistics={
                "a": ColumnStatisticsInput(min=0, max=255, has_null=False, distinct_count=256),
                "b": ColumnStatisticsInput(min=-1.0, max=1.0, has_null=True, distinct_count=1000),
            },
            statistics_cache_max_age_seconds=60,
        )

        result = table.resolve_column_statistics()
        assert result is not None

        data = serialize_column_statistics(result.statistics, result.cache_max_age_seconds)
        batch, custom_metadata = deserialize_record_batch(data)

        assert batch.num_rows == 2
        assert batch.column("column_name")[0].as_py() == "a"
        assert batch.column("column_name")[1].as_py() == "b"
        assert batch.column("min")[0].as_py() == 0
        assert batch.column("min")[1].as_py() == pytest.approx(-1.0)
        assert custom_metadata is not None
        assert custom_metadata[b"cache_max_age_seconds"] == b"60"


# ============================================================================
# statistics_from_duckdb
# ============================================================================


class TestStatisticsFromDuckDB:
    """Tests for the DuckDB statistics extraction helper."""

    def test_basic_types(self) -> None:
        import duckdb

        from vgi.catalog.duckdb_statistics import statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("CREATE TABLE test (id INT, name VARCHAR, price DOUBLE)")
        conn.execute("INSERT INTO test VALUES (1, 'Apple', 1.50), (2, 'Banana', 0.75), (3, NULL, 2.00)")

        stats = statistics_from_duckdb(conn, "test")

        assert set(stats.keys()) == {"id", "name", "price"}
        assert stats["id"].has_null is False
        assert stats["id"].has_not_null is True
        assert isinstance(stats["id"].min, pa.Scalar)
        assert stats["id"].min.as_py() == 1
        assert stats["id"].max.as_py() == 3  # type: ignore[union-attr]
        assert stats["name"].has_null is True
        assert stats["name"].max_string_length == 6  # "Banana"
        assert stats["name"].contains_unicode is False
        assert stats["id"].max_string_length is None  # non-string column
        assert stats["id"].contains_unicode is None  # non-string column

    def test_all_null_column(self) -> None:
        import duckdb

        from vgi.catalog.duckdb_statistics import statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (x INT)")
        conn.execute("INSERT INTO t VALUES (NULL), (NULL)")

        stats = statistics_from_duckdb(conn, "t")
        assert stats["x"].min is None
        assert stats["x"].max is None
        assert stats["x"].has_null is True
        assert stats["x"].has_not_null is False

    def test_empty_table(self) -> None:
        import duckdb

        from vgi.catalog.duckdb_statistics import statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("CREATE TABLE empty (x INT, y VARCHAR)")

        stats = statistics_from_duckdb(conn, "empty")
        assert stats["x"].min is None
        assert stats["x"].has_null is False
        assert stats["x"].has_not_null is False
        assert stats["y"].max_string_length is None  # empty table

    def test_geometry_2d(self) -> None:
        import duckdb

        from vgi.catalog.duckdb_statistics import statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("LOAD spatial")
        conn.execute(
            "CREATE TABLE geo AS SELECT ST_Point(x::DOUBLE, y::DOUBLE)::GEOMETRY AS geom "
            "FROM range(5) t1(x), range(5) t2(y)"
        )

        stats = statistics_from_duckdb(conn, "geo")
        assert stats["geom"].min is not None
        assert stats["geom"].max is not None
        assert isinstance(stats["geom"].min, pa.Scalar)
        assert stats["geom"].min.type == pa.binary()

    def test_geometry_with_leading_null(self) -> None:
        """Geometry detection works even when the first row is NULL."""
        import duckdb

        from vgi.catalog.duckdb_statistics import statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("LOAD spatial")
        conn.execute("CREATE TABLE geo (geom GEOMETRY)")
        conn.execute("INSERT INTO geo VALUES (NULL), (ST_Point(1.0, 2.0)), (ST_Point(3.0, 4.0))")

        stats = statistics_from_duckdb(conn, "geo")
        assert stats["geom"].min is not None
        assert stats["geom"].max is not None
        assert stats["geom"].min.type == pa.binary()  # type: ignore[union-attr]
        assert stats["geom"].has_null is True

    def test_list_columns(self) -> None:
        """List columns use list_min/list_max for child element bounds."""
        import duckdb

        from vgi.catalog.duckdb_statistics import statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (tags INT[], names VARCHAR[])")
        conn.execute("INSERT INTO t VALUES ([3,1,4], ['alice','bob']), ([1,5,9], ['charlie']), (NULL, NULL)")

        stats = statistics_from_duckdb(conn, "t")

        # List stats are wrapped as single-element lists containing child extremes
        assert stats["tags"].min.as_py() == [1]  # type: ignore[union-attr]
        assert stats["tags"].max.as_py() == [9]  # type: ignore[union-attr]
        assert stats["names"].min.as_py() == ["alice"]  # type: ignore[union-attr]
        assert stats["names"].max.as_py() == ["charlie"]  # type: ignore[union-attr]
        assert stats["tags"].has_null is True

    def test_nested_list(self) -> None:
        """Nested list (INT[][]) uses list_min/list_max which naturally peels one layer."""
        import duckdb

        from vgi.catalog.duckdb_statistics import statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (nested INT[][])")
        conn.execute("INSERT INTO t VALUES ([[1,2],[3,4]]), ([[5,6],[7,8,9]])")

        stats = statistics_from_duckdb(conn, "t")

        # min/max are single-element outer lists containing the inner list extremes
        # list_min([[1,2],[3,4]]) = [1,2], list_min([[5,6],[7,8,9]]) = [5,6]
        # min of those = [1,2], wrapped → [[1,2]]
        assert stats["nested"].min.as_py() == [[1, 2]]  # type: ignore[union-attr]
        assert stats["nested"].max.as_py() == [[7, 8, 9]]  # type: ignore[union-attr]
        assert pa.types.is_list(stats["nested"].min.type)  # type: ignore[union-attr]

    def test_struct_column(self) -> None:
        """Struct columns use lexicographic min/max; FromConstant+Merge gives per-field bounds."""
        import duckdb

        from vgi.catalog.duckdb_statistics import statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (loc STRUCT(x DOUBLE, y DOUBLE))")
        conn.execute("INSERT INTO t VALUES ({'x': 5.0, 'y': 1.0}), ({'x': 1.0, 'y': 9.0}), (NULL)")

        stats = statistics_from_duckdb(conn, "t")
        # min() uses lexicographic comparison (smallest by first field)
        assert stats["loc"].min.as_py() == {"x": 1.0, "y": 9.0}  # type: ignore[union-attr]
        assert stats["loc"].max.as_py() == {"x": 5.0, "y": 1.0}  # type: ignore[union-attr]
        assert stats["loc"].has_null is True

    def test_struct_mixed_field_types(self) -> None:
        """Struct with mixed field types (int, string, float, bool)."""
        import duckdb

        from vgi.catalog.duckdb_statistics import statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (info STRUCT(age INT, name VARCHAR, score DOUBLE, active BOOLEAN))")
        conn.execute(
            "INSERT INTO t VALUES "
            "({'age': 25, 'name': 'Alice', 'score': 95.5, 'active': true}), "
            "({'age': 30, 'name': 'Bob', 'score': 87.0, 'active': false}), "
            "({'age': 22, 'name': 'Charlie', 'score': 91.2, 'active': true})"
        )

        stats = statistics_from_duckdb(conn, "t")
        min_val = stats["info"].min.as_py()  # type: ignore[union-attr]
        max_val = stats["info"].max.as_py()  # type: ignore[union-attr]

        # min() uses lexicographic struct comparison (first field = age)
        assert min_val["age"] == 22  # Charlie has lowest age
        assert max_val["age"] == 30  # Bob has highest age

        # After FromConstant+Merge on C++ side, per-field stats would be:
        # age: [22, 30], name: [Charlie..Bob by lex], score: [91.2..87.0 by lex], active: [true..false]
        # But here we just verify the Python extraction and serialization work
        assert stats["info"].has_null is False

    def test_struct_with_null_fields(self) -> None:
        """Struct where some inner fields are NULL."""
        import duckdb

        from vgi.catalog.duckdb_statistics import statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (data STRUCT(x INT, label VARCHAR))")
        conn.execute(
            "INSERT INTO t VALUES ({'x': 1, 'label': 'a'}), ({'x': NULL, 'label': 'b'}), ({'x': 3, 'label': NULL})"
        )

        stats = statistics_from_duckdb(conn, "t")
        # Struct min/max are full struct values — inner NULLs are part of the comparison
        assert stats["data"].min is not None
        assert stats["data"].max is not None
        assert stats["data"].has_null is False  # the struct column itself has no NULLs

    def test_struct_serialization_roundtrip(self) -> None:
        """Struct stats serialize through sparse union and deserialize correctly."""
        stats = [
            ColumnStatistics(
                column_name="point",
                min=pa.scalar({"x": 0.0, "y": 0.0}, type=pa.struct([("x", pa.float64()), ("y", pa.float64())])),
                max=pa.scalar({"x": 10.0, "y": 20.0}, type=pa.struct([("x", pa.float64()), ("y", pa.float64())])),
                has_null=False,
                has_not_null=True,
            ),
            ColumnStatistics(
                column_name="id",
                min=pa.scalar(1, pa.int64()),
                max=pa.scalar(100, pa.int64()),
            ),
        ]
        data = serialize_column_statistics(stats)
        batch, _ = deserialize_record_batch(data)

        assert batch.num_rows == 2
        # Struct union child should preserve structure
        min_type = batch.schema.field("min").type
        assert pa.types.is_union(min_type)
        assert min_type.num_fields == 2  # struct + int64

        assert batch.column("column_name")[0].as_py() == "point"
        assert batch.column("min")[0].as_py() == {"x": 0.0, "y": 0.0}
        assert batch.column("max")[0].as_py() == {"x": 10.0, "y": 20.0}

    def test_map_column(self) -> None:
        """MAP columns use standard min/max (lexicographic on underlying key-value list)."""
        import duckdb

        from vgi.catalog.duckdb_statistics import statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (props MAP(VARCHAR, INT))")
        conn.execute("INSERT INTO t VALUES (MAP {'a': 1, 'b': 2}), (MAP {'c': 3}), (NULL)")

        stats = statistics_from_duckdb(conn, "t")
        assert stats["props"].min.as_py() == [("a", 1), ("b", 2)]  # type: ignore[union-attr]
        assert stats["props"].max.as_py() == [("c", 3)]  # type: ignore[union-attr]
        assert stats["props"].has_null is True

    def test_map_serialization_roundtrip(self) -> None:
        """MAP stats serialize through sparse union and deserialize correctly."""
        import duckdb

        from vgi.catalog.duckdb_statistics import statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (id INT, tags MAP(VARCHAR, DOUBLE))")
        conn.execute("INSERT INTO t VALUES (1, MAP {'score': 9.5}), (2, MAP {'score': 8.0, 'weight': 1.5})")

        stats = statistics_from_duckdb(conn, "t")
        cs_list = [
            ColumnStatistics(
                column_name="id",
                min=stats["id"].min,  # type: ignore[arg-type]
                max=stats["id"].max,  # type: ignore[arg-type]
                has_null=False,
                has_not_null=True,
            ),
            ColumnStatistics(
                column_name="tags",
                min=stats["tags"].min,  # type: ignore[arg-type]
                max=stats["tags"].max,  # type: ignore[arg-type]
                has_null=False,
                has_not_null=True,
            ),
        ]
        data = serialize_column_statistics(cs_list)
        batch, _ = deserialize_record_batch(data)

        assert batch.num_rows == 2
        min_type = batch.schema.field("min").type
        assert pa.types.is_union(min_type)
        assert batch.column("column_name")[1].as_py() == "tags"
        assert batch.column("min")[1].as_py() == [("score", 8.0), ("weight", 1.5)]

    def test_fixed_size_array(self) -> None:
        """Fixed-size ARRAY columns use list_min/list_max with regular list type."""
        import duckdb

        from vgi.catalog.duckdb_statistics import statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (coords INT[3])")
        conn.execute("INSERT INTO t VALUES ([1,2,3]), ([4,5,6])")

        stats = statistics_from_duckdb(conn, "t")
        assert stats["coords"].min.as_py() == [1]  # type: ignore[union-attr]
        assert stats["coords"].max.as_py() == [6]  # type: ignore[union-attr]
        # Uses regular list type (not fixed_size_list) for the scalar
        assert pa.types.is_list(stats["coords"].min.type)  # type: ignore[union-attr]

    def test_list_all_empty(self) -> None:
        """List column with only empty lists produces None min/max."""
        import duckdb

        from vgi.catalog.duckdb_statistics import statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (vals INT[])")
        conn.execute("INSERT INTO t VALUES ([]), ([]), (NULL)")

        stats = statistics_from_duckdb(conn, "t")
        assert stats["vals"].min is None
        assert stats["vals"].max is None

    def test_resolves_for_table_descriptor(self) -> None:
        import duckdb

        from vgi.catalog.duckdb_statistics import statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("CREATE TABLE t AS SELECT unnest(range(10)) AS val")

        stats = statistics_from_duckdb(conn, "t")
        schema: pa.Schema = conn.execute("SELECT * FROM t LIMIT 0").to_arrow_table().schema

        table = Table(name="t", columns=schema, statistics=stats)
        result = table.resolve_column_statistics()
        assert result is not None
        assert len(result.statistics) == 1
        assert result.statistics[0].min.as_py() == 0  # type: ignore[union-attr]
        assert result.statistics[0].max.as_py() == 9  # type: ignore[union-attr]

    def test_dictionary_encoded_column(self) -> None:
        """Dictionary-encoded (ENUM) columns report actual values, not dictionary indices."""
        import duckdb

        from vgi.catalog.duckdb_statistics import statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("CREATE TYPE color AS ENUM ('red', 'green', 'blue')")
        conn.execute("CREATE TABLE t (c color)")
        conn.execute("INSERT INTO t VALUES ('red'), ('green'), ('blue')")

        stats = statistics_from_duckdb(conn, "t")

        # Must be actual string values, not dictionary indices (0, 1, 2).
        # ENUM ordering is by ordinal: red=0, green=1, blue=2
        assert stats["c"].min.as_py() == "red"  # type: ignore[union-attr]
        assert stats["c"].max.as_py() == "blue"  # type: ignore[union-attr]
        # Type should be the value type (string), not dictionary
        assert not pa.types.is_dictionary(stats["c"].min.type)  # type: ignore[union-attr]
        assert not pa.types.is_dictionary(stats["c"].max.type)  # type: ignore[union-attr]
        # max_string_length should be computed for ENUM columns
        assert stats["c"].max_string_length == 5  # "green" is longest

    def test_dictionary_encoded_serialization_roundtrip(self) -> None:
        """Dictionary-encoded stats serialize correctly through sparse union."""
        import duckdb

        from vgi.catalog.duckdb_statistics import column_statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("CREATE TYPE size AS ENUM ('small', 'medium', 'large')")
        conn.execute("CREATE TABLE t (id INT, sz size)")
        conn.execute("INSERT INTO t VALUES (1, 'small'), (2, 'large'), (3, 'medium')")

        cs_list = column_statistics_from_duckdb(conn, "t")
        data = serialize_column_statistics(cs_list)
        batch, _ = deserialize_record_batch(data)

        assert batch.num_rows == 2
        sz_idx = [batch.column("column_name")[i].as_py() for i in range(2)].index("sz")
        # ENUM ordering is by ordinal: small=0, medium=1, large=2
        assert batch.column("min")[sz_idx].as_py() == "small"
        assert batch.column("max")[sz_idx].as_py() == "large"

    def test_column_statistics_from_duckdb(self) -> None:
        """column_statistics_from_duckdb returns resolved ColumnStatistics list."""
        import duckdb

        from vgi.catalog.catalog_interface import TableColumnStatisticsResult
        from vgi.catalog.duckdb_statistics import column_statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (id INT, name VARCHAR, score DOUBLE)")
        conn.execute("INSERT INTO t VALUES (1, 'Alice', 95.5), (2, 'Bob', 87.0), (3, NULL, 91.2)")

        stats = column_statistics_from_duckdb(conn, "t")

        # Returns list[ColumnStatistics], not dict[str, ColumnStatisticsInput]
        assert isinstance(stats, list)
        assert len(stats) == 3
        assert all(isinstance(s, ColumnStatistics) for s in stats)

        # Values are already resolved pa.Scalar with correct types
        id_stat = next(s for s in stats if s.column_name == "id")
        assert isinstance(id_stat.min, pa.Scalar)
        assert id_stat.min.as_py() == 1
        assert id_stat.max.as_py() == 3  # type: ignore[union-attr]

        name_stat = next(s for s in stats if s.column_name == "name")
        assert name_stat.has_null is True
        assert name_stat.min.as_py() == "Alice"  # type: ignore[union-attr]

        name_stat = next(s for s in stats if s.column_name == "name")
        assert name_stat.contains_unicode is False
        assert name_stat.max_string_length == 5  # "Alice"

        # Ready to wrap in TableColumnStatisticsResult for dynamic use
        result = TableColumnStatisticsResult(statistics=stats, cache_max_age_seconds=60)
        assert result.cache_max_age_seconds == 60
        assert len(result.statistics) == 3

    def test_contains_unicode(self) -> None:
        """contains_unicode detects non-ASCII characters in string columns."""
        import duckdb

        from vgi.catalog.duckdb_statistics import statistics_from_duckdb

        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (ascii_col VARCHAR, unicode_col VARCHAR)")
        conn.execute("INSERT INTO t VALUES ('hello', 'caf\u00e9'), ('world', '\u00fc\u00f6\u00e4')")

        stats = statistics_from_duckdb(conn, "t")
        assert stats["ascii_col"].contains_unicode is False
        assert stats["unicode_col"].contains_unicode is True
