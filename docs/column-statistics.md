# Column Statistics

Column statistics enable DuckDB's query optimizer to make cost-based decisions for VGI tables. When a worker provides min/max values, null counts, and distinct counts per column, the optimizer can eliminate unnecessary scans, improve join ordering, and push down spatial filters.

## How It Works

1. The worker declares `supports_column_statistics=True` on tables that provide statistics
2. When DuckDB plans a query, it calls the `catalog_table_column_statistics_get` RPC
3. The worker returns per-column statistics (min, max, null flags, distinct count)
4. DuckDB caches the result based on the worker's specified TTL
5. The optimizer uses the statistics for filter elimination, cardinality estimation, etc.

```sql
-- With statistics: optimizer knows id max=10, eliminates entire scan
EXPLAIN SELECT * FROM mydb.data.departments WHERE id > 100;
-- Physical Plan: EMPTY_RESULT

-- Without statistics: optimizer must scan and filter
EXPLAIN SELECT * FROM mydb.data.departments WHERE id > 100;
-- Physical Plan: FILTER → VGI_TABLE_SCAN
```

## Declarative Statistics (Recommended)

The simplest approach: add a `statistics` dict to your `Table` descriptor. Types are auto-inferred from the table's column schema.

```python
from vgi.catalog import Table, Schema, Catalog
from vgi.catalog.descriptors import ColumnStatisticsInput

catalog = Catalog(
    name="mydb",
    schemas=[
        Schema(
            name="data",
            tables=[
                Table(
                    name="products",
                    columns=pa.schema([
                        ("id", pa.int64()),
                        ("name", pa.string()),
                        ("price", pa.float64()),
                    ]),
                    statistics={
                        "id": ColumnStatisticsInput(min=1, max=10000, has_null=False, distinct_count=10000),
                        "name": ColumnStatisticsInput(min="Anvil", max="Zebra Tape", distinct_count=5000),
                        "price": ColumnStatisticsInput(min=0.99, max=999.99, has_null=False, distinct_count=800),
                    },
                    statistics_cache_max_age_seconds=3600,  # Cache for 1 hour
                ),
            ],
        ),
    ],
)
```

### ColumnStatisticsInput Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `min` | Python value or `pa.Scalar` | `None` | Minimum value (auto-converted to PyArrow scalar using column type) |
| `max` | Python value or `pa.Scalar` | `None` | Maximum value |
| `has_null` | `bool` | `True` | Whether the column contains NULL values |
| `has_not_null` | `bool` | `True` | Whether the column contains non-NULL values |
| `distinct_count` | `int \| None` | `None` | Approximate count of distinct values |
| `contains_unicode` | `bool \| None` | `None` | String columns only: contains non-ASCII characters |
| `max_string_length` | `int \| None` | `None` | String columns only: maximum byte length |

Values can be plain Python literals (`int`, `float`, `str`) which are auto-converted using the column's Arrow type, or explicit `pa.Scalar` values for precise control:

```python
# Plain Python values — types inferred from schema
ColumnStatisticsInput(min=1, max=100)

# Explicit PyArrow scalars — used as-is
ColumnStatisticsInput(min=pa.scalar(1, pa.int32()), max=pa.scalar(100, pa.int32()))
```

### Cache TTL

`statistics_cache_max_age_seconds` controls how long DuckDB caches the statistics before making another RPC call:

| Value | Behavior |
|-------|----------|
| `None` | Cache forever (default for static data) |
| `0` | Never cache — re-fetch on every query |
| `N` | Cache for N seconds |

## Dynamic Statistics from DuckDB

For workers that proxy data from a DuckDB database, use the `statistics_from_duckdb` helper to extract real statistics:

```python
import duckdb
from vgi.catalog.duckdb_statistics import statistics_from_duckdb

conn = duckdb.connect("my_data.duckdb")
stats = statistics_from_duckdb(conn, "products")

Table(
    name="products",
    columns=conn.execute("SELECT * FROM products LIMIT 0").to_arrow_table().schema,
    statistics=stats,
    statistics_cache_max_age_seconds=3600,
)
```

The helper queries `min()`, `max()`, `approx_count_distinct()`, and null counts per column using DuckDB's Arrow API, returning properly typed `pa.Scalar` values.

### Special Type Handling

| Column Type | Strategy |
|-------------|----------|
| **Geometry** | Computes spatial bounding box via `ST_XMin/ST_XMax/ST_YMin/ST_YMax`, handles XY/XYZ/XYM/XYZM vertex types |
| **List / Array** | Uses `list_min()`/`list_max()` for child element bounds, wraps in single-element lists |
| **Fixed-size Array** | Same as List, with type converted to variable-length list |
| **Struct** | Standard `min()`/`max()` (lexicographic), `FromConstant+Merge` produces per-field child stats |
| **Map** | Standard `min()`/`max()` on underlying key-value list structure |
| **Nested Lists** | `list_min()`/`list_max()` naturally peels one nesting layer per level |

## Dynamic Statistics (Override Method)

For computed or live statistics, override `table_column_statistics_get()` on your `CatalogInterface`:

```python
from vgi.catalog.catalog_interface import CatalogInterface, TableColumnStatisticsResult
from vgi.catalog.duckdb_statistics import column_statistics_from_duckdb

class MyCatalog(CatalogInterface):
    def table_column_statistics_get(
        self, *, attach_opaque_data, transaction_opaque_data, schema_name, name,
    ) -> TableColumnStatisticsResult | None:
        conn = self._get_connection(attach_opaque_data)
        return TableColumnStatisticsResult(
            statistics=column_statistics_from_duckdb(conn, name, schema_name=schema_name),
            cache_max_age_seconds=60,  # Re-fetch every minute
        )
```

`column_statistics_from_duckdb()` returns `list[ColumnStatistics]` with fully resolved PyArrow scalars — ready to wrap in `TableColumnStatisticsResult`.

## Debugging Statistics

Use the `vgi_table_statistics()` SQL function to inspect what statistics DuckDB has for a VGI table:

```sql
SELECT * FROM vgi_table_statistics('mydb', 'data', 'products');
```

| column_name | column_type | min | max | has_null | has_not_null | distinct_count |
|-------------|-------------|-----|-----|----------|--------------|----------------|
| id | BIGINT | 1 | 10000 | false | true | 10000 |
| name | VARCHAR | Anvil | Zebra Ta | false | true | 5000 |
| price | DOUBLE | 0.99 | 999.99 | false | true | 800 |

Notes:
- The `min` and `max` columns use a DuckDB `UNION` type — each column's value is in its native type
- String min/max are truncated to 8 bytes (DuckDB's internal `StringStats` limit)
- Geometry columns show the bounding box extent: `BOX(xmin ymin, xmax ymax)`
- Tables without `supports_column_statistics=True` return zero rows

## Wire Format

Statistics are transmitted via the `catalog_table_column_statistics_get` RPC method:

**Request**: standard catalog params (`attach_opaque_data`, `schema_name`, `name`, `transaction_opaque_data`)

**Response**: single RecordBatch with N rows (one per column):

| Field | Arrow Type | Description |
|-------|-----------|-------------|
| `column_name` | `utf8` | Column name |
| `min` | `sparse_union<...>` | Minimum value (union children are distinct column types) |
| `max` | `sparse_union<...>` | Maximum value |
| `has_null` | `bool` | Column contains NULLs |
| `has_not_null` | `bool` | Column contains non-NULLs |
| `distinct_count` | `int64` (nullable) | Approximate distinct count |
| `contains_unicode` | `bool` (nullable) | String columns only |
| `max_string_length` | `uint64` (nullable) | String columns only |

Cache TTL is carried as IPC batch `custom_metadata` with key `cache_max_age_seconds`.

## Capability Flags

Statistics are opt-in at two levels:

1. **Catalog level**: `CatalogAttachResult.supports_column_statistics` — global gate. If `False`, DuckDB never calls the statistics RPC.
2. **Table level**: `TableInfo.supports_column_statistics` — per-table opt-in. Mixed catalogs can have some tables with stats and others without.

When using the `Table` descriptor, both flags are auto-derived from whether the `statistics` dict is non-empty.
