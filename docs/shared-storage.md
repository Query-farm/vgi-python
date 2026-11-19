# Shared Storage

VGI functions running across multiple worker processes need shared storage
for coordinating work queues and aggregating partial results. The storage
backend is configured via the `VGI_WORKER_SHARED_STORAGE` environment
variable.

## Backends

| Backend | Value | Use Case | Dependencies |
|---------|-------|----------|-------------|
| SQLite | `sqlite` (default) | Local / subprocess transport | None (stdlib) |
| Azure SQL | `azure-sql` | Azure cloud deployments | `vgi-python[azure]` |
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
pip install 'vgi-python[azure]'
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

1. Deploy the Cloudflare Worker. The Worker source lives in a separate
   repository: [`vgi-cloudflare-durable-object-storage`](https://github.com/query-farm/vgi-cloudflare-durable-object-storage).

```bash
git clone https://github.com/query-farm/vgi-cloudflare-durable-object-storage
cd vgi-cloudflare-durable-object-storage
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
| `VGI_BLOB_VENDOR_URL` | Blob vendor for values over the DO's 1 MiB cap (optional; see below) |
| `VGI_BLOB_VENDOR_TOKEN` | Vendor bearer token (defaults to `VGI_CF_DO_TOKEN`) |
| `VGI_CF_DO_EXTERNALIZE_BYTES` | With the vendor, values of at least this size are stored as blobs (default 524288, at most 1048576) |

### Large Values (Blob Vendor)

The Durable Object refuses any stored value over 1 MiB. With
`VGI_BLOB_VENDOR_URL` set, a value of `VGI_CF_DO_EXTERNALIZE_BYTES` or more
(work-queue item, `state_*` value or log item) is uploaded to the blob vendor
before the DO request, and the DO stores a ~73-byte pointer. Reads fetch the
blob back and check its size and SHA-256. The `FunctionStorage` API does not
change. Without the vendor, a value over 1 MiB fails client-side with an
error naming `VGI_BLOB_VENDOR_URL`. Installing the vendor client needs
`vgi[external]`.

Every value the client writes carries a one-byte envelope tag, whether or not
the vendor is configured: `0x00 || value` inline, `0x01 || size || sha256 ||
object_id` for a blob. A value without a valid tag (for example one written by
an older release) fails to read with `StorageEnvelopeError`. Executions are
short-lived, so nothing needs migrating, but old and new clients cannot share
one running query.

Blobs are scoped to the shard and execution. `execution_clear` and
`queue_clear` delete their scope, `state_drain` the blobs it drained and
`queue_pop` the blob it claimed. Clean-up is best-effort and runs on a
background thread; the vendor's idle sweep and the bucket lifecycle rules
are the backstop.

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

## Sealed Framework Payloads

The framework stores some of its own context so that any worker process can
serve any RPC of an execution:

- a buffered table function's init request, including its resolved secrets and the attach and transaction context;
- aggregate constant arguments;
- the streaming-aggregate session.

These are always written sealed (`vgi._storage_seal`, sealed framework payload v1):

- **Cipher:** AES-256-GCM under `HKDF-SHA256(signing key, info="vgi.storage.payload.v1")`.
- **Associated data:** bound to the shard, execution id, namespace and key.
- **Envelope:** `0x01 || nonce(12) || ciphertext || tag(16)`.

A worker with no signing key (subprocess and unix transports) uses the
per-user host-local key file that secret ATTACH options use. Every process
serving one deployment must therefore share `VGI_SIGNING_KEY`, which
multi-machine HTTP deployments already require. A payload that fails to
open is an error, never a plaintext fallback.

What your functions write themselves (`process()` appends, aggregate states,
window partitions, finalize output, work-queue items) is your data and is
stored as written, unsealed.

## Profiling Storage Traffic

Set `VGI_STORAGE_PROFILE=1` to count every storage call. With it off, nothing is
wrapped and the hot path is the unprofiled code. Each call is attributed to:

- its operation (which is also the cloudflare-do endpoint);
- its call site (the first frame outside the storage modules);
- the VGI RPC it ran under;
- its attach shard and execution or transaction id.

The worker logs the following under `vgi.storage.profile`:

- a per-shard `storage_profile` JSON line every 5 s;
- a `storage_profile_execution` line when each execution is cleared;
- a summary table at a clean exit, covering:
  - calls by call site;
  - storage calls per RPC invocation (for example `Worker.table_buffering_process: 147 RPCs, 294 calls, 2.00/RPC`);
  - per-execution latency (mean, p50, p95);
  - in-flight concurrency, and the wall time with a call in flight against the execution's span.

Set `VGI_STORAGE_PROFILE_FILE=/tmp/storage-{pid}.jsonl` to also write one JSON
line per call. Then rebuild the table from those files, for example for a
server stopped by SIGTERM, which never runs `atexit`:

```bash
python -m vgi._storage_profile_report /tmp/storage-*.jsonl
```

A custom backend opts in with `vgi._storage_profile.profile_backend_class(MyCustomStorage)`.
