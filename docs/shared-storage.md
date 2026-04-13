# Shared Storage

VGI functions running across multiple worker processes need shared storage
for coordinating work queues and aggregating partial results. The storage
backend is configured via the `VGI_WORKER_SHARED_STORAGE` environment
variable.

## Backends

| Backend | Value | Use Case | Dependencies |
|---------|-------|----------|-------------|
| SQLite | `sqlite` (default) | Local / subprocess transport | None (stdlib) |
| Azure SQL | `azure-sql` | Azure cloud deployments | `vgi[azure]` |
| Cloudflare DO | `cloudflare-do` | Edge / multi-cloud deployments | None (stdlib) |

## SQLite (Default)

Used automatically for subprocess transport. All workers share a local
SQLite database file via WAL mode.

```bash
# No configuration needed — this is the default
vgi-serve my_worker.py
```

The database is stored at the platform-specific state directory
(`~/.local/state/vgi/vgi_storage.db` on Linux). No setup required.

## Azure SQL Database

For Azure cloud deployments (App Service, Container Apps, AKS) where
workers run on separate hosts.

### Setup

1. Install the Azure extra:

```bash
pip install vgi[azure]
```

2. Create an Azure SQL Database (Serverless recommended for cost):

```bash
az sql server create --name myserver --resource-group myrg --location eastus2 \
    --admin-user vgiadmin --admin-password 'MyPassword!'
az sql db create --name vgi --server myserver --resource-group myrg \
    --edition GeneralPurpose --compute-model Serverless \
    --family Gen5 --capacity 1 --auto-pause-delay 60 --min-capacity 0.5
```

3. Create the storage tables (once, during deployment):

```python
from vgi.function_storage_azure_sql import FunctionStorageAzureSql

storage = FunctionStorageAzureSql(
    server="myserver.database.windows.net",
    database="vgi",
    user="vgiadmin",
    password="MyPassword!",
)
storage.ensure_tables()
```

4. Configure the worker via environment variables:

```bash
VGI_WORKER_SHARED_STORAGE=azure-sql
VGI_AZURE_SQL_SERVER=myserver.database.windows.net
VGI_AZURE_SQL_DATABASE=vgi
VGI_AZURE_SQL_USER=vgiadmin
VGI_AZURE_SQL_PASSWORD=MyPassword!
```

For managed identity (no username/password), omit `VGI_AZURE_SQL_USER` and
`VGI_AZURE_SQL_PASSWORD`. The client will use `DefaultAzureCredential`.

### Environment Variables

| Variable | Description |
|----------|-------------|
| `VGI_AZURE_SQL_SERVER` | Server hostname (required) |
| `VGI_AZURE_SQL_DATABASE` | Database name (required) |
| `VGI_AZURE_SQL_USER` | SQL auth username (omit for managed identity) |
| `VGI_AZURE_SQL_PASSWORD` | SQL auth password (omit for managed identity) |
| `VGI_AZURE_SQL_DEBUG_LOG` | File path for debug/timing logs |

### Programmatic Usage

```python
from vgi.function_storage_azure_sql import FunctionStorageAzureSql

storage = FunctionStorageAzureSql(
    server="myserver.database.windows.net",
    database="vgi",
    user="vgiadmin",
    password="MyPassword!",
)

class MyTableFunction(TableFunctionGenerator):
    storage = storage
```

## Cloudflare Durable Objects

For edge deployments and multi-cloud setups. Uses a Cloudflare Worker +
Durable Object running SQLite internally. The DO is single-threaded, so
all operations are inherently atomic without locking.

### Setup

1. Deploy the Cloudflare Worker (from `cloudflare/vgi-storage/`):

```bash
cd cloudflare/vgi-storage
npm install
npx wrangler deploy
```

2. Set a bearer token for authentication:

```bash
npx wrangler secret put VGI_STORAGE_TOKEN
```

3. Configure the worker via environment variables:

```bash
VGI_WORKER_SHARED_STORAGE=cloudflare-do
VGI_CF_DO_URL=https://vgi-storage.myaccount.workers.dev
VGI_CF_DO_TOKEN=my-secret-token
```

No table creation step needed — the Durable Object creates its SQLite
tables automatically on first request.

### Environment Variables

| Variable | Description |
|----------|-------------|
| `VGI_CF_DO_URL` | Cloudflare Worker URL (required) |
| `VGI_CF_DO_TOKEN` | Bearer token for authentication (optional) |
| `VGI_CF_DO_DEBUG_LOG` | File path for debug/timing logs |

### Programmatic Usage

```python
from vgi.function_storage_cf_do import FunctionStorageCfDo

storage = FunctionStorageCfDo(
    url="https://vgi-storage.myaccount.workers.dev",
    token="my-secret-token",
)

class MyTableFunction(TableFunctionGenerator):
    storage = storage
```

### How It Works

A single Durable Object instance handles all executions. Since
`execution_id` is UUID4 (globally unique), there are no collisions between
concurrent executions sharing the same DO. The DO uses the same SQLite
schema as the local `FunctionStorageSqlite` backend.

Cleanup is handled by an hourly alarm that removes entries older than
24 hours.

### Performance

The Cloudflare DO backend adds one HTTP round-trip per storage operation.
Latency depends on proximity to the nearest Cloudflare PoP:

| Location | Approx. per-operation latency |
|----------|------------------------------|
| Co-located (same region) | 5-15ms |
| Same continent | 30-60ms |
| Cross-continent | 80-150ms |

## Custom Backends

Implement the `FunctionStorage` protocol to add a new backend:

```python
from vgi.function_storage import FunctionStorage, UnknownInvocationError

class MyCustomStorage:
    def worker_put(self, execution_id: bytes, worker_id: int, state: bytes) -> None: ...
    def worker_collect(self, execution_id: bytes) -> list[bytes]: ...
    def queue_push(self, execution_id: bytes, items: list[bytes]) -> int: ...
    def queue_pop(self, execution_id: bytes) -> bytes | None: ...
    def queue_clear(self, execution_id: bytes) -> int: ...
```

Assign it to your function classes:

```python
class MyFunction(TableFunctionGenerator):
    storage = MyCustomStorage()
```
