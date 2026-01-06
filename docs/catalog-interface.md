# VGI Catalog Interface

The Catalog Interface enables VGI workers to expose database-like structures to clients, supporting DuckDB's `ATTACH` command for external catalogs.

## Overview

While VGI functions provide computational capabilities (scalar transforms, table generation), the Catalog Interface provides **metadata management** - exposing catalogs, schemas, tables, views, and functions as first-class database objects.

```sql
-- Attach a VGI-backed catalog in DuckDB
ATTACH 'mydb' (TYPE 'vgi', LOCATION './my-worker');

-- Query tables from the attached catalog
SELECT * FROM mydb.main.users;

-- List available schemas
SELECT * FROM information_schema.schemata WHERE catalog_name = 'mydb';
```

**Key Characteristics:**

| Aspect | Functions | Catalog Interface |
|--------|-----------|-------------------|
| Purpose | Compute data | Manage metadata |
| Protocol | Bind → Init → Stream | Invoke → Stream |
| Stateful | Per-invocation | Per-attachment |
| Discovery | `Worker.functions` list | `CatalogInterface.catalogs()` |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           DuckDB / Client                           │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  ATTACH 'db1' (TYPE 'vgi', LOCATION './worker')               │  │
│  │    ↓                                                          │  │
│  │  CatalogClientMixin.catalog_attach(name='db1')                │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │ stdin/stdout                         │
│                              ▼                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                      Worker Process                           │  │
│  │  Invocation(function_type=CATALOG, function_name='...')       │  │
│  │    ↓                                                          │  │
│  │  CatalogInterface.{method_name}(**kwargs)                     │  │
│  │    ↓                                                          │  │
│  │  Arrow IPC response (streamed)                                │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Protocol

The catalog protocol is simpler than the function protocol - no bind/init phases.

### Message Flow

```
Client                                  Worker
  │                                       │
  │──── Invocation ───────────────────▶  │  function_type=CATALOG
  │     (function_name = method name)     │  function_name = "catalog_attach"
  │                                       │
  │──── Arguments Batch ──────────────▶  │  Single row with method params
  │                                       │
  │◀──── Result Batch(es) ─────────────  │  Serialized result(s)
  │◀──── Empty Batch (EOF) ────────────  │  For streaming methods
  │                                       │
  └───────────────────────────────────────┘
```

### Invocation Format

Catalog invocations use `InvocationType.CATALOG`:

| Field | Value | Description |
|-------|-------|-------------|
| `function_type` | `"catalog"` | Identifies catalog invocation |
| `function_name` | Method name | e.g., `"catalog_attach"`, `"schemas"` |
| `arguments` | Empty | Arguments sent in separate batch |

### Arguments Batch

Method arguments are sent as a single-row RecordBatch where column names match parameter names:

```python
# For catalog_attach(name='mydb', options={})
args_batch = pa.RecordBatch.from_pylist([{
    "name": "mydb",
    "options": {}
}])
```

### Result Serialization

| Return Type | Serialization |
|-------------|---------------|
| `None` | Empty batch (0 rows, 0 columns) |
| Dataclass with `serialize()` | Single serialized batch |
| `list[str]` | Single-column batch named "value" |
| `Iterable[Dataclass]` | Stream of serialized batches + empty EOF batch |

---

## Data Types

### Type Aliases

```python
from vgi.catalog import AttachId, TransactionId, SerializedSchema, SqlExpression

AttachId = NewType("AttachId", bytes)           # Unique attachment identifier
TransactionId = NewType("TransactionId", bytes)  # Transaction identifier
SerializedSchema = NewType("SerializedSchema", bytes)  # Arrow schema bytes
SqlExpression = NewType("SqlExpression", str)   # SQL expression string
```

### CatalogAttachResult

Returned by `catalog_attach()` with attachment metadata:

| Field | Type | Description |
|-------|------|-------------|
| `attach_id` | `AttachId` | Unique identifier for this attachment |
| `supports_transactions` | `bool` | Whether transactions are supported |
| `supports_time_travel` | `bool` | Whether time travel queries work |
| `catalog_version_frozen` | `bool` | Whether metadata will change |
| `catalog_version` | `int` | Current version (increments on changes) |
| `attach_id_required` | `bool` | Whether attach_id must be persisted |

### SchemaInfo

Information about a schema in a catalog:

| Field | Type | Description |
|-------|------|-------------|
| `attach_id` | `AttachId` | Parent attachment |
| `name` | `str` | Schema name |
| `is_default` | `bool` | Whether this is the default schema |
| `comment` | `str \| None` | Optional description |
| `tags` | `dict[str, str]` | Key-value metadata |

### TableInfo

Information about a table:

| Field | Type | Description |
|-------|------|-------------|
| `name` | `str` | Table name |
| `schema_name` | `str` | Parent schema name |
| `columns` | `SerializedSchema` | Column definitions as Arrow schema bytes |
| `not_null_constraints` | `list[int]` | Column indices with NOT NULL |
| `unique_constraints` | `list[list[int]]` | Column index groups for UNIQUE |
| `check_constraints` | `list[str]` | SQL check expressions |
| `comment` | `str \| None` | Optional description |
| `tags` | `dict[str, str]` | Key-value metadata |

### ViewInfo

Information about a view:

| Field | Type | Description |
|-------|------|-------------|
| `name` | `str` | View name |
| `schema_name` | `str` | Parent schema name |
| `definition` | `str` | SQL SELECT statement |
| `comment` | `str \| None` | Optional description |
| `tags` | `dict[str, str]` | Key-value metadata |

### FunctionInfo

Information about a function in a schema:

| Field | Type | Description |
|-------|------|-------------|
| `name` | `str` | Function name |
| `schema_name` | `str` | Parent schema name |
| `function_type` | `FunctionType` | `SCALAR` or `TABLE` |
| `arguments` | `SerializedSchema` | Argument schema as Arrow bytes |
| `output_schema` | `SerializedSchema` | Output schema as Arrow bytes |
| `comment` | `str \| None` | Optional description |
| `tags` | `dict[str, str]` | Key-value metadata |

### ScanFunctionResult

Information for scanning a table's data:

| Field | Type | Description |
|-------|------|-------------|
| `function_name` | `str` | VGI function to call for scanning |
| `max_processes` | `int` | Max parallel scan workers |
| `invocation_id` | `bytes \| None` | Pre-bound invocation ID |

---

## CatalogInterface

The `CatalogInterface` abstract base class defines all catalog operations. Subclass it and implement the abstract methods.

### Abstract Methods (Required)

```python
from abc import ABC, abstractmethod
from vgi.catalog import CatalogInterface, CatalogAttachResult, SchemaInfo, TableInfo, ViewInfo

class MyCatalog(CatalogInterface):
    @abstractmethod
    def catalogs(self) -> Iterable[str]:
        """List available catalog names."""

    @abstractmethod
    def catalog_attach(self, *, name: str, options: dict[str, Any]) -> CatalogAttachResult:
        """Attach to a catalog, returning attachment metadata."""

    @abstractmethod
    def schema_get(self, *, attach_id: AttachId, transaction_id: TransactionId | None, name: str) -> SchemaInfo | None:
        """Get schema info, or None if not found."""

    @abstractmethod
    def table_get(self, *, attach_id: AttachId, transaction_id: TransactionId | None, schema_name: str, name: str) -> TableInfo | None:
        """Get table info, or None if not found."""

    @abstractmethod
    def view_get(self, *, attach_id: AttachId, transaction_id: TransactionId | None, schema_name: str, name: str) -> ViewInfo | None:
        """Get view info, or None if not found."""
```

### Optional Methods (Override as Needed)

| Category | Method | Default Behavior |
|----------|--------|------------------|
| **Catalog** | `catalog_create()` | `NotImplementedError` |
| | `catalog_drop()` | `NotImplementedError` |
| | `catalog_detach()` | No-op |
| | `catalog_version()` | Returns `0` |
| **Transaction** | `catalog_transaction_begin()` | `NotImplementedError` |
| | `catalog_transaction_commit()` | `NotImplementedError` |
| | `catalog_transaction_rollback()` | `NotImplementedError` |
| **Schema** | `schemas()` | Returns `["main"]` |
| | `schema_create()` | `NotImplementedError` |
| | `schema_drop()` | `NotImplementedError` |
| | `schema_contents()` | `NotImplementedError` |
| **Table** | `table_create()` | `NotImplementedError` |
| | `table_drop()` | `NotImplementedError` |
| | `table_rename()` | `NotImplementedError` |
| | `table_comment_set()` | `NotImplementedError` |
| | `table_column_add()` | `NotImplementedError` |
| | `table_column_drop()` | `NotImplementedError` |
| | `table_column_rename()` | `NotImplementedError` |
| | `table_column_type_change()` | `NotImplementedError` |
| | `table_column_default_set()` | `NotImplementedError` |
| | `table_column_default_drop()` | `NotImplementedError` |
| | `table_not_null_set()` | `NotImplementedError` |
| | `table_not_null_drop()` | `NotImplementedError` |
| | `table_scan_function_get()` | `NotImplementedError` |
| **View** | `view_create()` | `NotImplementedError` |
| | `view_drop()` | `NotImplementedError` |
| | `view_rename()` | `NotImplementedError` |
| | `view_comment_set()` | `NotImplementedError` |

---

## ReadOnlyCatalogInterface

A convenience base class for read-only catalogs that don't support DDL operations. All modification methods raise `CatalogReadOnlyError`.

### Usage Option 1: Function-Only Catalog

The simplest way to expose VGI functions as a catalog:

```python
from vgi.catalog import ReadOnlyCatalogInterface
from vgi import ScalarFunction, TableFunctionGenerator

class MyFunctionCatalog(ReadOnlyCatalogInterface):
    catalog_name = "my_funcs"  # Name for ATTACH
    functions = [MyScalarFunction, MyTableFunction]

# Functions appear in the "main" schema:
# SELECT * FROM my_funcs.main.my_scalar_function(args);
```

### Usage Option 2: Custom Read-Only Catalog

Implement abstract methods for more control:

```python
from vgi.catalog import ReadOnlyCatalogInterface, CatalogAttachResult, SchemaInfo

class MyReadOnlyCatalog(ReadOnlyCatalogInterface):
    def catalogs(self) -> Iterable[str]:
        return ["readonly_db"]

    def catalog_attach(self, *, name: str, options: dict[str, Any]) -> CatalogAttachResult:
        return CatalogAttachResult(
            attach_id=AttachId(b"fixed-id"),
            supports_transactions=False,
            supports_time_travel=False,
            catalog_version_frozen=True,
            catalog_version=1,
            attach_id_required=False,
        )

    def schema_get(self, *, attach_id, transaction_id, name) -> SchemaInfo | None:
        if name == "main":
            return SchemaInfo(attach_id=attach_id, name="main", is_default=True, comment=None, tags={})
        return None

    # table_get, view_get return None by default
```

---

## Worker Integration

### Automatic Catalog Interface

Workers with functions automatically get a `ReadOnlyCatalogInterface`:

```python
from vgi import Worker, ScalarFunction

class MyWorker(Worker):
    functions = [MyFunction, OtherFunction]
    catalog_name = "my_catalog"  # Default: "functions"

# Automatically creates ReadOnlyCatalogInterface exposing functions
```

### Custom Catalog Interface

Set `catalog_interface` for full control:

```python
from vgi import Worker
from vgi.catalog import CatalogInterface

class MyFullCatalog(CatalogInterface):
    # ... implement abstract methods

class MyWorker(Worker):
    catalog_interface = MyFullCatalog
    functions = []  # Optional: functions can still be registered
```

### Disable Catalog

To disable the catalog interface entirely:

```python
class MyWorker(Worker):
    catalog_interface = None
    catalog_name = None  # Required to fully disable
    functions = [...]
```

---

## Client Usage

The `CatalogClientMixin` adds catalog methods to the VGI Client:

```python
from vgi.client import Client
from vgi.client.catalog_mixin import CatalogClientMixin

class CatalogClient(CatalogClientMixin, Client):
    pass

# Connect and interact with catalog
client = CatalogClient("./my-worker")

# List catalogs
catalogs = client.catalogs()  # ["my_catalog"]

# Attach to a catalog
result = client.catalog_attach(name="my_catalog", options={})
attach_id = result.attach_id

# List schemas
for schema in client.schemas(attach_id=attach_id):
    print(f"Schema: {schema.name}")

# Get schema contents (tables, views, functions)
for obj in client.schema_contents(attach_id=attach_id, name="main"):
    if isinstance(obj, TableInfo):
        print(f"Table: {obj.name}")
    elif isinstance(obj, ViewInfo):
        print(f"View: {obj.name}")
    elif isinstance(obj, FunctionInfo):
        print(f"Function: {obj.name}")

# Detach when done
client.catalog_detach(attach_id=attach_id)
```

### Available Client Methods

| Method | Description |
|--------|-------------|
| `catalogs()` | List catalog names |
| `catalog_attach()` | Attach to a catalog |
| `catalog_detach()` | Detach from a catalog |
| `catalog_create()` | Create a new catalog |
| `catalog_drop()` | Drop a catalog |
| `catalog_version()` | Get catalog version |
| `schemas()` | List schemas |
| `schema_get()` | Get schema info |
| `schema_create()` | Create a schema |
| `schema_drop()` | Drop a schema |
| `schema_contents()` | List schema contents |
| `table_get()` | Get table info |
| `table_create()` | Create a table |
| `table_drop()` | Drop a table |
| `table_rename()` | Rename a table |
| `table_comment_set()` | Set table comment |
| `table_column_add()` | Add a column |
| `table_column_drop()` | Drop a column |
| `table_column_rename()` | Rename a column |
| `table_scan_function_get()` | Get scan function |
| `view_get()` | Get view info |
| `view_create()` | Create a view |
| `view_drop()` | Drop a view |
| `view_rename()` | Rename a view |
| `view_comment_set()` | Set view comment |
| `catalog_transaction_begin()` | Begin transaction |
| `catalog_transaction_commit()` | Commit transaction |
| `catalog_transaction_rollback()` | Rollback transaction |

---

## Storage

For stateful catalogs, VGI provides `CatalogStorage` for persisting attachment and transaction state across worker processes.

### CatalogStorageSqlite

SQLite-backed storage with WAL mode for concurrent access:

```python
from vgi.catalog import CatalogStorageSqlite, AttachId

# Use default location (~/.local/state/vgi/vgi_catalog.db)
storage = CatalogStorageSqlite()

# Or specify a custom path
storage = CatalogStorageSqlite("/path/to/catalog.db")

# Store attachment
attach_id = storage.generate_attach_id()
storage.attach_put(attach_id, catalog_name="mydb", options={"key": "value"})

# Retrieve attachment
result = storage.attach_get(attach_id)  # ("mydb", {"key": "value"})

# List all attachments
all_ids = storage.attach_list()

# Delete attachment
storage.attach_delete(attach_id)
```

### Storage Protocol

```python
from typing import Protocol
from vgi.catalog import AttachId, TransactionId

class CatalogStorage(Protocol):
    # Attachment state
    def attach_put(self, attach_id: AttachId, catalog_name: str, options: dict) -> None: ...
    def attach_get(self, attach_id: AttachId) -> tuple[str, dict] | None: ...
    def attach_delete(self, attach_id: AttachId) -> None: ...
    def attach_list(self) -> list[AttachId]: ...

    # Transaction state
    def transaction_put(self, transaction_id: TransactionId, attach_id: AttachId, state: bytes) -> None: ...
    def transaction_get(self, transaction_id: TransactionId) -> tuple[AttachId, bytes] | None: ...
    def transaction_delete(self, transaction_id: TransactionId) -> None: ...
```

---

## Wire Format

### Schema Serialization

Table and function schemas are serialized using Arrow IPC:

```python
import pyarrow as pa
from vgi.catalog import SerializedSchema

# Serialize
schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.utf8()),
])
serialized: SerializedSchema = SerializedSchema(schema.serialize().to_pybytes())

# Deserialize
schema = pa.ipc.read_schema(pa.py_buffer(serialized))
```

### Dataclass Serialization

Catalog dataclasses have `serialize()` methods and `deserialize()` class methods:

```python
from vgi.catalog import CatalogAttachResult, AttachId

# Create result
result = CatalogAttachResult(
    attach_id=AttachId(b"my-id"),
    supports_transactions=False,
    supports_time_travel=False,
    catalog_version_frozen=True,
    catalog_version=1,
    attach_id_required=False,
)

# Serialize to bytes
data = result.serialize()

# Deserialize from RecordBatch
batch = pa.ipc.open_stream(pa.py_buffer(data)).read_next_batch()
restored = CatalogAttachResult.deserialize(batch)
```

---

## Transactions

Catalogs can optionally support transactions:

```python
class TransactionalCatalog(CatalogInterface):
    def catalog_attach(self, *, name, options) -> CatalogAttachResult:
        return CatalogAttachResult(
            attach_id=...,
            supports_transactions=True,  # Enable transactions
            ...
        )

    def catalog_transaction_begin(self, *, attach_id) -> TransactionId:
        txn_id = self._create_transaction(attach_id)
        return txn_id

    def catalog_transaction_commit(self, *, attach_id, transaction_id) -> None:
        self._commit_transaction(transaction_id)

    def catalog_transaction_rollback(self, *, attach_id, transaction_id) -> None:
        self._rollback_transaction(transaction_id)
```

**Transaction Guarantees:**

- Transactions MAY span multiple worker processes
- Workers MUST treat `transaction_id` as opaque bytes
- Workers MUST ensure idempotency of commit/rollback
- If `supports_transactions=False`, transaction methods won't be called

---

## Error Handling

Errors are returned as exceptions that propagate through the VGI protocol:

| Error | When Raised |
|-------|-------------|
| `ValueError` | Invalid arguments, object not found |
| `NotImplementedError` | Method not supported |
| `CatalogReadOnlyError` | DDL on read-only catalog |

Example error handling:

```python
from vgi.exceptions import CatalogReadOnlyError

class MyReadOnlyCatalog(ReadOnlyCatalogInterface):
    def table_create(self, **kwargs) -> None:
        # Automatically raises CatalogReadOnlyError
        raise CatalogReadOnlyError("Cannot create table: catalog is read-only")
```

---

## Complete Example

```python
from collections.abc import Iterable
from typing import Any
import uuid

from vgi import Worker
from vgi.catalog import (
    AttachId,
    CatalogAttachResult,
    CatalogInterface,
    SchemaInfo,
    TableInfo,
    TransactionId,
    ViewInfo,
)


class SimpleCatalog(CatalogInterface):
    """A minimal catalog with a single schema."""

    def __init__(self):
        self._attachments: dict[AttachId, str] = {}

    def catalogs(self) -> Iterable[str]:
        return ["simple_db"]

    def catalog_attach(self, *, name: str, options: dict[str, Any]) -> CatalogAttachResult:
        if name != "simple_db":
            raise ValueError(f"Unknown catalog: {name}")

        attach_id = AttachId(uuid.uuid4().bytes)
        self._attachments[attach_id] = name

        return CatalogAttachResult(
            attach_id=attach_id,
            supports_transactions=False,
            supports_time_travel=False,
            catalog_version_frozen=True,
            catalog_version=1,
            attach_id_required=False,
        )

    def catalog_detach(self, *, attach_id: AttachId) -> None:
        self._attachments.pop(attach_id, None)

    def schema_get(
        self, *, attach_id: AttachId, transaction_id: TransactionId | None, name: str
    ) -> SchemaInfo | None:
        if name == "main":
            return SchemaInfo(
                attach_id=attach_id,
                name="main",
                is_default=True,
                comment="Default schema",
                tags={},
            )
        return None

    def table_get(
        self, *, attach_id: AttachId, transaction_id: TransactionId | None,
        schema_name: str, name: str
    ) -> TableInfo | None:
        return None  # No tables

    def view_get(
        self, *, attach_id: AttachId, transaction_id: TransactionId | None,
        schema_name: str, name: str
    ) -> ViewInfo | None:
        return None  # No views


class SimpleCatalogWorker(Worker):
    catalog_interface = SimpleCatalog
    functions = []


if __name__ == "__main__":
    SimpleCatalogWorker().run()
```

---

## API Limitations

The current CatalogInterface has the following limitations:

- **Functions**: Cannot be created or dropped via catalog methods (use `Worker.functions`)
- **Tags**: Cannot be updated after object creation
- **Schema metadata**: Comments and tags cannot be updated on schemas
- **Constraints**: Only NOT NULL can be added/dropped (no ALTER for UNIQUE/CHECK)
- **Indexes**: Not supported
- **INSERT/UPDATE/DELETE**: Not yet implemented (metadata only)

---

## See Also

- [Protocol Specification](protocol.md) - VGI wire protocol details
- [Function Lifecycle](lifecycle.md) - Function execution phases
- [Function Metadata](metadata.md) - Function introspection
