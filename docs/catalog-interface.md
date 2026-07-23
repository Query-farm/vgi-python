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
| Protocol | Bind → Init → Stream | `vgi_rpc` typed dispatch |
| Stateful | Per-invocation | Per-attachment |
| Discovery | `Worker.functions` list | `CatalogInterface.catalogs()` |

---
## Protocol

Catalog methods are dispatched via `vgi_rpc` typed protocol methods. Each method has its own typed request/response defined in `vgi.protocol`, with automatic Arrow serialization handled by the RPC layer. This is simpler than the function protocol — no bind/init phases.

---

## Data Types

### Type Aliases

```python
from vgi.catalog import AttachOpaqueData, TransactionOpaqueData, SerializedSchema, SqlExpression

AttachOpaqueData = NewType("AttachOpaqueData", bytes)           # Unique attachment identifier
TransactionOpaqueData = NewType("TransactionOpaqueData", bytes)  # Transaction identifier
SerializedSchema = NewType("SerializedSchema", bytes)  # Arrow schema bytes
SqlExpression = NewType("SqlExpression", str)   # SQL expression string
```

### CatalogAttachResult

Returned by `catalog_attach()` with attachment metadata:

| Field | Type | Description |
|-------|------|-------------|
| `attach_opaque_data` | `AttachOpaqueData` | Opaque per-attachment state the implementation owns (see note below) |
| `supports_transactions` | `bool` | Whether transactions are supported |
| `supports_time_travel` | `bool` | Whether time travel queries work |
| `catalog_version_frozen` | `bool` | Whether metadata will change |
| `catalog_version` | `int` | Current version (increments on changes) |
| `attach_opaque_data_required` | `bool` | Whether attach_opaque_data must be persisted |
| `default_schema` | `str` | Name of the default schema (usually "main") |

> **`attach_opaque_data` / `transaction_opaque_data` are opaque, implementation-owned, and may carry secrets.**
> They are *not* framework identifiers — they are arbitrary `bytes` your implementation returns from
> `catalog_attach()` / `catalog_transaction_begin()`, and the client round-trips them back verbatim on
> every subsequent call. An implementation may pack connection handles, credentials, or any state into
> them. **Never log either value raw** — treat them like the `options` dict. The worker already enforces
> this for its own catalog-lifecycle logs (it short-hashes both fields at a single chokepoint), and on
> HTTP transport it additionally seals each value in an AEAD envelope bound to the caller's identity, so
> a value minted for one principal cannot be replayed by another. Your implementation only ever sees the
> plaintext; the sealing and unsealing happen transparently in the worker.

### SchemaInfo

Information about a schema in a catalog:

| Field | Type | Description |
|-------|------|-------------|
| `attach_opaque_data` | `AttachOpaqueData` | Parent attachment |
| `name` | `str` | Schema name |
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

### SchemaObjectType

Enum for filtering objects in `schema_contents()`:

| Value | Description |
|-------|-------------|
| `TABLE` | Filter to return only tables |
| `VIEW` | Filter to return only views |
| `SCALAR_FUNCTION` | Filter to return only scalar functions |
| `TABLE_FUNCTION` | Filter to return only table functions |

### ScanFunctionResult

Result from `table_scan_function_get()` that tells the VGI DuckDB extension which DuckDB function to call to obtain table data. This enables catalogs to delegate scanning to any DuckDB function (e.g., `read_parquet`, `iceberg_scan`, or a custom VGI table function) with appropriate arguments.

| Field | Type | Description |
|-------|------|-------------|
| `function_name` | `str` | The DuckDB function to call (e.g., `"read_parquet"`, `"iceberg_scan"`) |
| `positional_arguments` | `list[pa.Scalar]` | Positional arguments to pass to the function |
| `named_arguments` | `dict[str, pa.Scalar]` | Named arguments to pass to the function |
| `required_extensions` | `list[str]` | DuckDB extensions that must be loaded before calling the function |

**Example usage:**

```python
def table_scan_function_get(
    self,
    *,
    attach_opaque_data: AttachOpaqueData,
    transaction_opaque_data: TransactionOpaqueData | None,
    schema_name: str,
    name: str,
    at_unit: str | None,
    at_value: str | None,
) -> ScanFunctionResult:
    # Return a parquet scan for this table
    return ScanFunctionResult(
        function_name="read_parquet",
        positional_arguments=[pa.scalar(f"s3://bucket/{schema_name}/{name}/*.parquet")],
        named_arguments={"hive_partitioning": pa.scalar(True)},
        required_extensions=["parquet", "httpfs"],
    )
```

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
    def schema_get(self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None, name: str) -> SchemaInfo | None:
        """Get schema info, or None if not found."""

    @abstractmethod
    def table_get(self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None, schema_name: str, name: str) -> TableInfo | None:
        """Get table info, or None if not found."""

    @abstractmethod
    def view_get(self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None, schema_name: str, name: str) -> ViewInfo | None:
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
| **Observability** | `loggable_attach_options()` | Returns `{}` (no options logged — see below) |

---

## Logging Attach Options Safely

The worker emits structured `_logger.info` records and Sentry breadcrumbs for catalog lifecycle events (`catalog.attach`, `catalog.detach`, `catalog.create`, `catalog.transaction.begin`, `catalog.transaction.commit`, `catalog.transaction.rollback`). `attach_opaque_data` and `transaction_opaque_data` are short-hashed (12-char SHA-256 prefixes) before they reach the log record, the breadcrumb data, or the Sentry scope tags — the raw values never appear in observability output, since they may carry secrets. An operator correlates the short hash back to the catalog via these breadcrumbs.

The `options` dict passed to `catalog_attach()` and `catalog_create()` routinely carries credentials — passwords, tokens, OAuth secrets, connection strings. To avoid leaking these to logs and Sentry, the worker **does not** log option fields by default. Implementers opt in by overriding `loggable_attach_options()`:

```python test="skip"
class MyCatalog(CatalogInterface):
    def loggable_attach_options(self, options: Mapping[str, Any]) -> Mapping[str, Any]:
        # Allowlist the keys you know are safe.  Never include password / token / secret.
        safe_keys = {"host", "region", "bucket", "database"}
        return {k: v for k, v in options.items() if k in safe_keys}
```

When the override returns an empty mapping (the default behaviour for catalogs that haven't opted in), the `options` field is omitted from the lifecycle event entirely — fail-closed: nothing is preferred over partial-leak.

The catalog name, attach id, transaction id, and version specs are always logged regardless.

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
            attach_opaque_data=AttachOpaqueData(b"fixed-id"),
            supports_transactions=False,
            supports_time_travel=False,
            catalog_version_frozen=True,
            catalog_version=1,
            attach_opaque_data_required=False,
        )

    def schema_get(self, *, attach_opaque_data, transaction_opaque_data, name) -> SchemaInfo | None:
        if name == "main":
            return SchemaInfo(attach_opaque_data=attach_opaque_data, name="main", comment=None, tags={})
        return None

    # table_get, view_get return None by default
```

---

## Function Name Scoping

A function name is **not** a global key. The worker resolves a call by the pair
`(schema, function name)`, so the same name may be declared in more than one
schema of a catalog, and a schema-qualified call reaches the implementation in
that schema:

```python test="lint"
Catalog(
    name="example",
    default_schema="main",
    schemas=[
        Schema(name="main", functions=[ProdLookup]),   # Meta.name = "lookup"
        Schema(name="staging", functions=[StagingLookup]),  # Meta.name = "lookup"
    ],
)
```

```sql
SELECT example.main.lookup(1);     -- ProdLookup
SELECT example.staging.lookup(1);  -- StagingLookup
```

The DuckDB extension carries the owning schema on every bind request
(`BindRequest.schema_name`), taken from the schema entry the function was
registered into. Two consequences worth knowing:

- **Overloads still work.** Several classes sharing a name *within one schema*
  are overloads, disambiguated by argument signature as before. Only the
  cross-schema case is resolved by schema.
- **Callers without a schema must be unambiguous.** The pure-Python `Client`
  and the CLI send no schema. If the name is unique across the worker they
  resolve normally; if two schemas declare it, the worker raises an ambiguity
  error naming the schemas involved.

Functions declared via the legacy `Worker.functions` list have no schema of
their own, so they are registered into the catalog's `default_schema` — the
same schema DuckDB registers them into.

Across **catalogs**, the key is the attachment rather than the name: two
catalogs served by one worker process (see `vgi.meta_worker.MetaWorker`) may
each declare `main.lookup`, and each attachment's `attach_opaque_data` routes
its calls to the right catalog.

---

## Declarative Catalogs

For most use cases, the declarative catalog API provides a simpler way to define catalogs using Python dataclasses instead of implementing `CatalogInterface` directly.

### Imports

```python
from vgi import Worker, TableFunctionGenerator
from vgi.catalog import Catalog, Schema, Table, View
```

### Function-Backed Tables (Recommended)

The recommended pattern is to back tables with `TableFunctionGenerator` functions. The table schema is automatically derived from the function's `output_schema`, eliminating duplication:

```python test="skip"
import pyarrow as pa
from dataclasses import dataclass
from typing import ClassVar
from vgi import TableFunctionGenerator
from vgi.table_function import ProcessParams, OutputCollector

class UsersFunction(TableFunctionGenerator):
    """Generate user data."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([
        ("id", pa.int64()),
        ("name", pa.string()),
        ("active", pa.bool_()),
    ])

    @classmethod
    def process(cls, params, state, out: OutputCollector) -> None:
        out.emit(pa.RecordBatch.from_pydict({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "active": [True, True, False],
        }, schema=params.output_schema))
        out.finish()

# Table with auto-derived schema
users_table = Table(
    name="users",
    function=UsersFunction,  # Schema derived from FIXED_SCHEMA
    not_null=["id"],         # Constraint column names validated
    unique=[["id"]],
    comment="User accounts",
)
```

### Full Catalog Definition

```python
from vgi import Worker
from vgi.catalog import Catalog, Schema, Table, View

class MyWorker(Worker):
    catalog = Catalog(
        name="myapp",
        default_schema="main",
        schemas=[
            Schema(
                name="main",
                comment="Main application data",
                tables=[users_table],
                views=[
                    View(
                        name="active_users",
                        definition="SELECT * FROM users WHERE active = true",
                        comment="Active user accounts only",
                    ),
                ],
                functions=[UsersFunction],
            ),
            Schema(
                name="analytics",
                comment="Analytics data",
                tables=[events_table],
                functions=[AggregateFunction],
            ),
        ],
    )

if __name__ == "__main__":
    MyWorker().run()
```

### Key Benefits

| Feature | Description |
|---------|-------------|
| **No schema duplication** | Function-backed tables derive schema automatically |
| **Constraint validation** | `not_null`, `unique` column names validated at definition time |
| **Automatic scan handling** | Function-backed tables don't need `table_scan_function_get()` |
| **Type safety** | Frozen dataclasses with runtime validation |

### Tables with Explicit Columns

For tables not backed by functions, provide the schema explicitly:

```python
# Explicit columns - requires table_scan_function_get() implementation
config_table = Table(
    name="config",
    columns=pa.schema([
        ("key", pa.string()),
        ("value", pa.string()),
    ]),
    not_null=["key"],
    unique=[["key"]],
)
```

**Note:** Tables with explicit columns require the worker to implement `table_scan_function_get()` to tell DuckDB how to scan the data.

### Validation

Declarative catalogs include comprehensive validation:

```python test="skip"
# Error: missing columns or function
Table(name="bad")  # ValueError: must specify either 'columns' or 'function'

# Error: invalid constraint column
Table(
    name="users",
    columns=pa.schema([("id", pa.int64())]),
    not_null=["nonexistent"],  # ValueError: column 'nonexistent' not found
)

# Error: default_schema not in schemas
Catalog(
    name="myapp",
    default_schema="missing",
    schemas=[Schema(name="main")],  # ValueError: default_schema 'missing' not found
)
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

```python test="lint"
class MyWorker(Worker):
    catalog_interface = None
    catalog_name = None  # Required to fully disable
    functions = [...]
```

A catalog-less worker is reachable only from the pure-Python [`Client`][], which
binds by `(schema, function name)` and needs no attachment — its functions
register into the default `main` schema. It is **not** reachable from DuckDB:
`ATTACH ... (TYPE vgi)` requires a catalog, and there is no standalone
call form. If the worker should be usable from SQL, give it a catalog.

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
attach_opaque_data = result.attach_opaque_data

# List schemas
for schema in client.schemas(attach_opaque_data=attach_opaque_data):
    print(f"Schema: {schema.name}")

# Get schema contents (tables, views, functions)
for obj in client.schema_contents(attach_opaque_data=attach_opaque_data, name="main"):
    if isinstance(obj, TableInfo):
        print(f"Table: {obj.name}")
    elif isinstance(obj, ViewInfo):
        print(f"View: {obj.name}")
    elif isinstance(obj, FunctionInfo):
        print(f"Function: {obj.name}")

# Get only scalar functions using type filter
from vgi.catalog import SchemaObjectType
for obj in client.schema_contents(
    attach_opaque_data=attach_opaque_data, name="main", type=SchemaObjectType.SCALAR_FUNCTION
):
    print(f"Scalar Function: {obj.name}")

# Detach when done
client.catalog_detach(attach_opaque_data=attach_opaque_data)
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
| `schema_contents()` | List schema contents (optional `type` filter) |
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
from vgi.catalog import CatalogStorageSqlite, AttachOpaqueData

# Use default location (~/.local/state/vgi/vgi_catalog.db)
storage = CatalogStorageSqlite()

# Or specify a custom path
storage = CatalogStorageSqlite("/path/to/catalog.db")

# Store attachment
attach_opaque_data = storage.generate_attach_opaque_data()
storage.attach_put(attach_opaque_data, catalog_name="mydb", options={"key": "value"})

# Retrieve attachment
result = storage.attach_get(attach_opaque_data)  # ("mydb", {"key": "value"})

# List all attachments
all_ids = storage.attach_list()

# Delete attachment
storage.attach_delete(attach_opaque_data)
```

### Storage Protocol

```python
from typing import Protocol
from vgi.catalog import AttachOpaqueData, TransactionOpaqueData

class CatalogStorage(Protocol):
    # Attachment state
    def attach_put(self, attach_opaque_data: AttachOpaqueData, catalog_name: str, options: dict) -> None: ...
    def attach_get(self, attach_opaque_data: AttachOpaqueData) -> tuple[str, dict] | None: ...
    def attach_delete(self, attach_opaque_data: AttachOpaqueData) -> None: ...
    def attach_list(self) -> list[AttachOpaqueData]: ...

    # Transaction state
    def transaction_put(self, transaction_opaque_data: TransactionOpaqueData, attach_opaque_data: AttachOpaqueData, state: bytes) -> None: ...
    def transaction_get(self, transaction_opaque_data: TransactionOpaqueData) -> tuple[AttachOpaqueData, bytes] | None: ...
    def transaction_delete(self, transaction_opaque_data: TransactionOpaqueData) -> None: ...
```

---

## Transactions

Catalogs can optionally support transactions:

```python
class TransactionalCatalog(CatalogInterface):
    def catalog_attach(self, *, name, options) -> CatalogAttachResult:
        return CatalogAttachResult(
            attach_opaque_data=...,
            supports_transactions=True,  # Enable transactions
            ...
        )

    def catalog_transaction_begin(self, *, attach_opaque_data) -> TransactionOpaqueData:
        txn_id = self._create_transaction(attach_opaque_data)
        return txn_id

    def catalog_transaction_commit(self, *, attach_opaque_data, transaction_opaque_data) -> None:
        self._commit_transaction(transaction_opaque_data)

    def catalog_transaction_rollback(self, *, attach_opaque_data, transaction_opaque_data) -> None:
        self._rollback_transaction(transaction_opaque_data)
```

**Transaction Guarantees:**

- Transactions MAY span multiple worker processes
- Workers MUST treat `transaction_opaque_data` as opaque bytes
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
    AttachOpaqueData,
    CatalogAttachResult,
    CatalogInterface,
    SchemaInfo,
    TableInfo,
    TransactionOpaqueData,
    ViewInfo,
)


class SimpleCatalog(CatalogInterface):
    """A minimal catalog with a single schema."""

    def __init__(self):
        self._attachments: dict[AttachOpaqueData, str] = {}

    def catalogs(self) -> Iterable[str]:
        return ["simple_db"]

    def catalog_attach(self, *, name: str, options: dict[str, Any]) -> CatalogAttachResult:
        if name != "simple_db":
            raise ValueError(f"Unknown catalog: {name}")

        attach_opaque_data = AttachOpaqueData(uuid.uuid4().bytes)
        self._attachments[attach_opaque_data] = name

        return CatalogAttachResult(
            attach_opaque_data=attach_opaque_data,
            supports_transactions=False,
            supports_time_travel=False,
            catalog_version_frozen=True,
            catalog_version=1,
            attach_opaque_data_required=False,
        )

    def catalog_detach(self, *, attach_opaque_data: AttachOpaqueData) -> None:
        self._attachments.pop(attach_opaque_data, None)

    def schema_get(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None, name: str
    ) -> SchemaInfo | None:
        if name == "main":
            return SchemaInfo(
                attach_opaque_data=attach_opaque_data,
                name="main",
                comment="Default schema",
                tags={},
            )
        return None

    def table_get(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str, name: str
    ) -> TableInfo | None:
        return None  # No tables

    def view_get(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None,
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

- [Function Lifecycle](lifecycle.md) - Function execution phases
- [Function Metadata](metadata.md) - Function introspection
