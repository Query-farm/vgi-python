# Filter Pushdown Protocol

Filter pushdown allows VGI **table functions** to receive SQL WHERE clause predicates. Workers can apply filters during data generation, reducing transferred data.

VGI uses a **hybrid JSON + Arrow** format:
- **JSON** describes filter structure (operators, column references)
- **Arrow columns** store filter values (preserves exact types)

## Transport

Filters are sent in **Stream 3 (InitInput)** as a binary field containing Arrow IPC bytes:

```
InitInput Schema:
├── projection_ids: list<int32>
└── filters: binary (nullable)    -- Arrow IPC bytes
```

Table functions must declare `filter_pushdown: true` in metadata to receive filters.

## Format

The Arrow RecordBatch contains:

| Column | Name | Type | Content |
|--------|------|------|---------|
| 0 | `filter_spec` | string | JSON array of filters |
| 1+ | `_val_0`, `_val_1`, ... | varies | Values referenced by filters |

**Version metadata** on `filter_spec` field: `{"vgi_filter_version": "1"}`

## Example

SQL: `WHERE salary > 50000 AND name = 'Alice'`

**RecordBatch:**
```
filter_spec: "[{...}, {...}]"   (string)
_val_0: 50000                   (int64)
_val_1: "Alice"                 (string)
```

**filter_spec JSON:**
```json
[
  {"column_name": "salary", "column_index": 2, "type": "constant", "op": "gt", "value_ref": 0},
  {"column_name": "name", "column_index": 0, "type": "constant", "op": "eq", "value_ref": 1}
]
```

The `value_ref` points to value columns: `value_ref: 0` → column `_val_0` (index 1 in batch).

## Filter Types

### constant
Comparison filter: `col > value`

```json
{"column_name": "age", "column_index": 1, "type": "constant", "op": "ge", "value_ref": 0}
```

**Operators:** `eq` (=), `ne` (!=), `gt` (>), `ge` (>=), `lt` (<), `le` (<=)

### is_null / is_not_null
NULL check: `col IS NULL` or `col IS NOT NULL`

```json
{"column_name": "email", "column_index": 3, "type": "is_null"}
```

### in
Set membership: `col IN (v1, v2, v3)`

```json
{"column_name": "status", "column_index": 4, "type": "in", "value_ref": 0}
```

The value column is a list type containing all IN values: `_val_0: ["active", "pending", "review"]`

### and / or
Conjunction combining multiple filters on same column:

```json
{"column_name": "age", "column_index": 1, "type": "and", "children": [
  {"column_name": "age", "column_index": 1, "type": "constant", "op": "ge", "value_ref": 0},
  {"column_name": "age", "column_index": 1, "type": "constant", "op": "lt", "value_ref": 1}
]}
```

### struct
Nested field filter: `address.city = 'Seattle'`

```json
{"column_name": "address", "column_index": 5, "type": "struct",
 "child_index": 1, "child_name": "city",
 "child_filter": {"column_name": "address", "column_index": 5, "type": "constant", "op": "eq", "value_ref": 0}}
```

## Unsupported Filter Types

Some DuckDB filter types cannot be serialized for pushdown. When these are encountered, VGI skips filter pushdown entirely and the `filters` field is null:

- **DynamicFilter** - Created by TOP-N queries (`ORDER BY ... LIMIT N`). The filter value mutates during query execution.
- **BloomFilter** - Created by join optimization. Contains a large binary buffer.
- **ExpressionFilter** - Created by complex predicates like `UPPER(col) = 'X'`. Contains expression trees that may reference functions unavailable in the worker.

## Deserialization

```python
import pyarrow as pa
import json

def deserialize_filters(ipc_bytes: bytes):
    reader = pa.ipc.open_stream(ipc_bytes)
    batch = reader.read_next_batch()

    # Check version
    version = batch.schema.field(0).metadata.get(b"vgi_filter_version", b"").decode()
    assert version == "1", f"Unknown filter version: {version}"

    # Parse filters
    filters = json.loads(batch.column(0)[0].as_py())

    # Get value by ref: value_ref N → column N+1
    # Returns Arrow scalar to preserve exact type
    def get_value(ref: int) -> pa.Scalar:
        return batch.column(ref + 1)[0]

    return filters, get_value
```

## JSON Schema

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://vgi-protocol.dev/filter-pushdown/v1",
  "title": "VGI Filter Specification",
  "type": "array",
  "items": {"$ref": "#/$defs/filter"},

  "$defs": {
    "filter": {
      "type": "object",
      "required": ["column_name", "column_index", "type"],
      "properties": {
        "column_name": {"type": "string"},
        "column_index": {"type": "integer", "minimum": 0},
        "type": {"enum": ["constant", "is_null", "is_not_null", "in", "and", "or", "struct"]},
        "op": {"enum": ["eq", "ne", "gt", "ge", "lt", "le"]},
        "value_ref": {"type": "integer", "minimum": 0},
        "children": {"type": "array", "items": {"$ref": "#/$defs/filter"}},
        "child_index": {"type": "integer", "minimum": 0},
        "child_name": {"type": "string"},
        "child_filter": {"$ref": "#/$defs/filter"}
      }
    }
  }
}
```

## Worker Implementation Notes

- **Partial application OK**: Apply filters you can handle; DuckDB always re-verifies results
- **Unsupported filters**: Return all rows for that column, let DuckDB filter locally
- **Type fidelity**: Values preserve exact Arrow types (decimal, timestamp with timezone, nested types)
