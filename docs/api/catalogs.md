# Catalogs

A worker can expose a database-like catalog — schemas, tables, views, macros, indexes, secrets,
and settings — so DuckDB can `ATTACH` it. See the [Catalog Interface](../catalog-interface.md)
guide for the conceptual overview.

Column comments and tags use the existing Arrow field metadata. The helpers
preserve missing versus empty values and leave unrelated metadata intact:

```python
import pyarrow as pa
from vgi.catalog import ColumnMetadata, with_column_metadata

amount = with_column_metadata(
    pa.field("amount", pa.decimal128(12, 2)),
    ColumnMetadata(comment="", tags={"unit": "USD"}),
)
```

Use the resulting field in the CREATE/ADD schema. `None` means the annotation is
absent; `""` and `{}` are present empty values. Readers can use
`column_metadata(field)` and `validate_column_metadata(schema)`. Malformed UTF-8,
invalid tag JSON, duplicate keys and non-string tag values reject before catalog
mutation. No additional capability flag is required. Native DuckDB 1.5 exposes
comments; tags remain in the catalog's Arrow schema until the engine adapter can
map them through a public column-tags API.

::: vgi.catalog
