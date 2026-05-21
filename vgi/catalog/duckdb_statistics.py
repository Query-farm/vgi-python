# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Extract column statistics from DuckDB tables.

Provides a helper to query a DuckDB connection and produce
:class:`~vgi.catalog.descriptors.ColumnStatisticsInput` dicts
ready for use in ``Table(statistics=...)``.

Example::

    import duckdb
    from vgi.catalog.duckdb_statistics import statistics_from_duckdb

    conn = duckdb.connect("my_data.duckdb")
    stats = statistics_from_duckdb(conn, "my_table")

    Table(
        name="my_table",
        columns=...,
        statistics=stats,
        statistics_cache_max_age_seconds=3600,
    )

Geometry columns are handled specially: instead of meaningless ``min``/``max``
of the raw WKB blobs, the helper computes the spatial bounding box of the
dataset and sends two corner points so that DuckDB's ``GeometryStats`` can
reconstruct the correct spatial extent for filter pushdown.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

from vgi.catalog.catalog_interface import ColumnStatistics
from vgi.catalog.descriptors import ColumnStatisticsInput

if TYPE_CHECKING:
    import duckdb

__all__ = ["column_statistics_from_duckdb", "statistics_from_duckdb"]

# DuckDB type names that indicate a spatial column requiring special handling.
_GEOMETRY_TYPE_NAMES = frozenset({"GEOMETRY", "POINT_2D", "LINESTRING_2D", "POLYGON_2D", "BOX_2D"})


def _is_geometry_column(conn: duckdb.DuckDBPyConnection, qualified: str, col: str) -> bool:
    """Check if a column is a geometry type by querying DuckDB's typeof()."""
    try:
        row = conn.execute(f"SELECT typeof({col}) FROM {qualified} WHERE {col} IS NOT NULL LIMIT 1").fetchone()
        return row is not None and row[0] in _GEOMETRY_TYPE_NAMES
    except Exception:
        return False


def _geometry_stats(
    conn: duckdb.DuckDBPyConnection,
    qualified: str,
    col: str,
) -> tuple[pa.Scalar | None, pa.Scalar | None]:  # type: ignore[type-arg]
    """Compute min/max geometry scalars as bounding-box corner points.

    For geometry columns, ``min``/``max`` of the raw WKB is meaningless for
    spatial filtering.  Instead, we compute the spatial bounding box and return
    two corner-point geometries whose union covers the full extent.

    Handles all vertex types:

    - **XY** (2D): ``POINT(xmin ymin)`` / ``POINT(xmax ymax)``
    - **XYZ** (3D): ``POINT Z(xmin ymin zmin)`` / ``POINT Z(xmax ymax zmax)``
    - **XYM**: ``POINT M(xmin ymin mmin)`` / ``POINT M(xmax ymax mmax)``
    - **XYZM**: ``POINT ZM(xmin ymin zmin mmin)`` / ``POINT ZM(xmax ymax zmax mmax)``

    When the C++ side calls ``GeometryStats::Update`` on each, the resulting
    ``GeometryExtent`` is the correct overall bounding box in all dimensions.

    Returns (min_point, max_point) as Arrow binary scalars (WKB), or
    (None, None) if the column has no non-null geometries or the spatial
    extension is not loaded.
    """
    try:
        # Detect which dimensions are present by checking if Z/M functions
        # return non-NULL for any row
        dim_row = conn.execute(
            f"SELECT"
            f"  bool_or(ST_ZMin({col}) IS NOT NULL) AS has_z,"
            f"  bool_or(ST_MMin({col}) IS NOT NULL) AS has_m"
            f" FROM {qualified}"
            f" WHERE {col} IS NOT NULL"
        ).fetchone()

        if dim_row is None:
            return None, None

        has_z = bool(dim_row[0])
        has_m = bool(dim_row[1])

        # Build the aggregation query for all present dimensions
        agg_parts = [
            f"min(ST_XMin({col})) AS xmin",
            f"max(ST_XMax({col})) AS xmax",
            f"min(ST_YMin({col})) AS ymin",
            f"max(ST_YMax({col})) AS ymax",
        ]
        if has_z:
            agg_parts += [f"min(ST_ZMin({col})) AS zmin", f"max(ST_ZMax({col})) AS zmax"]
        if has_m:
            agg_parts += [f"min(ST_MMin({col})) AS mmin", f"max(ST_MMax({col})) AS mmax"]

        bounds = conn.execute(f"SELECT {', '.join(agg_parts)} FROM {qualified} WHERE {col} IS NOT NULL").fetchone()

        if bounds is None:
            return None, None

        xmin, xmax, ymin, ymax = bounds[0], bounds[1], bounds[2], bounds[3]
        if xmin is None:
            return None, None

        # Build WKT for the corner points with the correct vertex type
        idx = 4
        if has_z and has_m:
            zmin, zmax = bounds[idx], bounds[idx + 1]
            mmin, mmax = bounds[idx + 2], bounds[idx + 3]
            dim_label = "ZM"
            min_coords = f"{xmin} {ymin} {zmin} {mmin}"
            max_coords = f"{xmax} {ymax} {zmax} {mmax}"
        elif has_z:
            zmin, zmax = bounds[idx], bounds[idx + 1]
            dim_label = "Z"
            min_coords = f"{xmin} {ymin} {zmin}"
            max_coords = f"{xmax} {ymax} {zmax}"
        elif has_m:
            mmin, mmax = bounds[idx], bounds[idx + 1]
            dim_label = "M"
            min_coords = f"{xmin} {ymin} {mmin}"
            max_coords = f"{xmax} {ymax} {mmax}"
        else:
            dim_label = ""
            min_coords = f"{xmin} {ymin}"
            max_coords = f"{xmax} {ymax}"

        dim_suffix = f" {dim_label}" if dim_label else ""
        min_wkt = f"POINT{dim_suffix}({min_coords})"
        max_wkt = f"POINT{dim_suffix}({max_coords})"

        arrow_table = conn.execute(
            f"SELECT"
            f"  ST_GeomFromText('{min_wkt}')::GEOMETRY AS min_pt,"
            f"  ST_GeomFromText('{max_wkt}')::GEOMETRY AS max_pt"
        ).to_arrow_table()

        min_scalar = arrow_table.column("min_pt")[0]
        max_scalar = arrow_table.column("max_pt")[0]
        return (
            min_scalar if min_scalar.is_valid else None,
            max_scalar if max_scalar.is_valid else None,
        )
    except Exception:
        # Spatial extension not loaded, or column type doesn't support ST_ functions
        return None, None


def _list_stats(
    conn: duckdb.DuckDBPyConnection,
    qualified: str,
    col: str,
    arrow_type: pa.DataType,
) -> tuple[pa.Scalar | None, pa.Scalar | None]:  # type: ignore[type-arg]
    """Compute min/max for list columns using child element extremes.

    For list columns, ``min``/``max`` of the list values themselves is not useful
    for statistics.  Instead, we compute the min/max of the child elements across
    all lists using ``list_min``/``list_max``, then wrap them in single-element
    lists so that ``FromConstant([child_min])`` + ``Merge(FromConstant([child_max]))``
    produces the correct ``ListStats`` with child element bounds.

    Returns (min_list, max_list) as Arrow list scalars, or (None, None) if there
    are no non-null child elements.
    """
    try:
        arrow_table = conn.execute(
            f"SELECT"
            f"  [min(list_min({col}))] AS min_val,"
            f"  [max(list_max({col}))] AS max_val"
            f" FROM {qualified}"
            f" WHERE {col} IS NOT NULL"
        ).to_arrow_table()
        min_scalar = arrow_table.column("min_val")[0]
        max_scalar = arrow_table.column("max_val")[0]
        # Check if the inner element is null (all lists were empty)
        min_inner = min_scalar.as_py()
        max_inner = max_scalar.as_py()
        if min_inner is None or min_inner == [None]:
            return None, None
        if max_inner is None or max_inner == [None]:
            return None, None
        # Wrap child extremes in a regular list type (works for both LIST and ARRAY columns).
        # For fixed-size ARRAY types, we can't create a 1-element scalar with the original
        # type (size mismatch), so we use a variable-length list instead. DuckDB's
        # FromConstant handles both LIST_STATS and ARRAY_STATS identically for child bounds.
        list_type = pa.list_(arrow_type.value_type) if pa.types.is_fixed_size_list(arrow_type) else arrow_type
        return (
            pa.scalar(min_inner, type=list_type),
            pa.scalar(max_inner, type=list_type),
        )
    except Exception:
        return None, None


def statistics_from_duckdb(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    *,
    schema_name: str | None = None,
) -> dict[str, ColumnStatisticsInput]:
    """Extract column statistics from a DuckDB table.

    Queries the table for min, max, approximate distinct count, and null counts
    per column. Returns a dict mapping column names to
    :class:`ColumnStatisticsInput` instances with properly typed PyArrow scalars.

    Special column type handling:

    - **Geometry**: computes the spatial bounding box and sends two corner-point
      geometries so that DuckDB's ``GeometryStats`` can reconstruct the correct
      extent for spatial filter pushdown.
    - **List**: uses ``list_min``/``list_max`` to find child element extremes,
      then wraps them in single-element lists so DuckDB's ``ListStats`` tracks
      the correct child element bounds.

    Args:
        conn: An open DuckDB connection.
        table_name: Name of the table to query.
        schema_name: Optional schema name. If provided, the table is referenced
            as ``schema_name.table_name``.

    Returns:
        Dict mapping column names to ``ColumnStatisticsInput``, suitable for
        passing directly to ``Table(statistics=...)``.

    """
    qualified = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'

    # Get the table schema via a zero-row Arrow query
    schema: pa.Schema = conn.execute(f"SELECT * FROM {qualified} LIMIT 0").to_arrow_table().schema

    result: dict[str, ColumnStatisticsInput] = {}

    for field in schema:
        col = f'"{field.name}"'

        # Count nulls/non-nulls and distinct values (works for all types)
        count_table = conn.execute(
            f"SELECT"
            f"  approx_count_distinct({col}) AS distinct_count,"
            f"  count({col}) AS non_null_count,"
            f"  (count(*) - count({col})) AS null_count"
            f" FROM {qualified}"
        ).to_arrow_table()
        distinct_count: int = count_table.column("distinct_count")[0].as_py()
        non_null_count: int = count_table.column("non_null_count")[0].as_py()
        null_count: int = count_table.column("null_count")[0].as_py()

        # Compute min/max — dispatch by column type
        min_val: pa.Scalar | None = None  # type: ignore[type-arg]
        max_val: pa.Scalar | None = None  # type: ignore[type-arg]

        is_geom = _is_geometry_column(conn, qualified, col)
        if is_geom:
            min_val, max_val = _geometry_stats(conn, qualified, col)
        elif (
            pa.types.is_list(field.type)
            or pa.types.is_large_list(field.type)
            or pa.types.is_fixed_size_list(field.type)
        ):
            min_val, max_val = _list_stats(conn, qualified, col, field.type)
        else:
            minmax_table = conn.execute(
                f"SELECT min({col}) AS min_val, max({col}) AS max_val FROM {qualified}"
            ).to_arrow_table()
            min_scalar = minmax_table.column("min_val")[0]
            max_scalar = minmax_table.column("max_val")[0]
            min_val = min_scalar if min_scalar.is_valid else None
            max_val = max_scalar if max_scalar.is_valid else None

        # Unwrap dictionary-encoded scalars (e.g. from ENUM columns) to their
        # value type so that statistics report actual values, not dictionary indices.
        if min_val is not None and pa.types.is_dictionary(min_val.type):
            min_val = pa.scalar(min_val.as_py(), type=min_val.type.value_type)
        if max_val is not None and pa.types.is_dictionary(max_val.type):
            max_val = pa.scalar(max_val.as_py(), type=max_val.type.value_type)

        # Compute max_string_length for string/binary columns (including
        # dictionary-encoded columns with string value types like ENUMs).
        # Skip geometry columns — their Arrow type is binary but strlen/octet_length
        # don't apply to the DuckDB GEOMETRY type.
        max_string_length: int | None = None
        is_dict = pa.types.is_dictionary(field.type)
        effective_type = field.type.value_type if is_dict else field.type
        if not is_geom and (
            pa.types.is_string(effective_type)
            or pa.types.is_large_string(effective_type)
            or pa.types.is_binary(effective_type)
            or pa.types.is_large_binary(effective_type)
        ):
            # strlen returns byte length for VARCHAR; octet_length for BLOB.
            # ENUM columns need a cast to VARCHAR first.
            if pa.types.is_binary(effective_type) or pa.types.is_large_binary(effective_type):
                len_expr = f"octet_length({col})"
            elif is_dict:
                len_expr = f"strlen({col}::VARCHAR)"
            else:
                len_expr = f"strlen({col})"
            len_row = conn.execute(f"SELECT max({len_expr}) AS max_len FROM {qualified}").fetchone()
            if len_row is not None and len_row[0] is not None:
                max_string_length = int(len_row[0])

        # Compute contains_unicode for string columns: true if any value has
        # characters outside ASCII (byte length > character length).
        contains_unicode: bool | None = None
        if pa.types.is_string(effective_type) or pa.types.is_large_string(effective_type):
            if is_dict:
                unicode_expr = f"strlen({col}::VARCHAR) != length({col}::VARCHAR)"
            else:
                unicode_expr = f"strlen({col}) != length({col})"
            uni_row = conn.execute(f"SELECT bool_or({unicode_expr}) AS has_unicode FROM {qualified}").fetchone()
            contains_unicode = bool(uni_row[0]) if uni_row is not None and uni_row[0] is not None else False

        result[field.name] = ColumnStatisticsInput(
            min=min_val,
            max=max_val,
            has_null=null_count > 0,
            has_not_null=non_null_count > 0,
            distinct_count=distinct_count,
            max_string_length=max_string_length,
            contains_unicode=contains_unicode,
        )

    return result


def column_statistics_from_duckdb(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    *,
    schema_name: str | None = None,
) -> list[ColumnStatistics]:
    """Extract resolved column statistics from a DuckDB table.

    Like :func:`statistics_from_duckdb`, but returns fully resolved
    :class:`ColumnStatistics` objects with typed PyArrow scalars — ready
    to be returned from ``table_column_statistics_get()`` wrapped in a
    :class:`TableColumnStatisticsResult`.

    Example usage in a dynamic catalog::

        def table_column_statistics_get(self, *, attach_opaque_data, transaction_opaque_data, schema_name, name):
            conn = self._get_connection(attach_opaque_data)
            return TableColumnStatisticsResult(
                statistics=column_statistics_from_duckdb(conn, name, schema_name=schema_name),
                cache_max_age_seconds=60,
            )

    Args:
        conn: An open DuckDB connection.
        table_name: Name of the table to query.
        schema_name: Optional schema name.

    Returns:
        List of resolved ``ColumnStatistics`` objects.

    """
    qualified = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
    schema: pa.Schema = conn.execute(f"SELECT * FROM {qualified} LIMIT 0").to_arrow_table().schema
    stats_dict = statistics_from_duckdb(conn, table_name, schema_name=schema_name)
    return [stats_dict[field.name].resolve(field.name, field.type) for field in schema if field.name in stats_dict]
