# VGI Command Line Interface

VGI provides CLI tools for invoking functions and managing catalogs without writing code.

## Available Commands

| Command | Description |
|---------|-------------|
| `vgi-client` | Invoke functions and manage catalogs |
| `vgi-fixture-worker` | Run the example worker with demo functions |

---

## vgi-client

The main CLI for invoking VGI functions and managing catalogs.

### Function Invocation

```bash
vgi-client [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--input FILE` | Input parquet file (omit for table functions) |
| `--output FILE` | Output file (use `-` for stdout) |
| `--format FORMAT` | Output format: `json` (default), `csv`, `parquet` |
| `--function NAME` | Function name to invoke |
| `--args JSON` | Function arguments as JSON array (default: `[]`) |
| `--worker PATH` | Worker command (default: `vgi-fixture-worker`) |
| `--type TYPE` | Function type: `auto`, `table`, `table-in-out`, `scalar` |
| `--projection-id N` | Column IDs to project (repeatable) |
| `--max-workers N` | Limit parallel workers |
| `--worker-stderr` | Show worker stderr output |

### Examples

**Table function (generates data):**

```bash
# Generate a sequence of 100 integers
vgi-client --function sequence --args '[100]'

# Output as CSV
vgi-client --function sequence --args '[10]' --format csv
```

**Table-in-out function (transforms data):**

```bash
# Echo input unchanged
vgi-client --input data.parquet --function echo

# Sum all numeric columns
vgi-client --input data.parquet --function sum_all_columns

# Repeat each row 3 times
vgi-client --input data.parquet --function repeat_inputs --args '[3]'
```

**Scalar function (per-row transform):**

```bash
# Multiply values in column "price" by 2
vgi-client --input data.parquet --function multiply --args '["price", 2]' --type scalar
```

**Output to file:**

```bash
vgi-client --function sequence --args '[1000]' --output result.parquet --format parquet
```

---

## Catalog Commands

Manage database catalogs exposed by VGI workers.

### Attach/Detach Pattern

Most catalog operations require an attach ID. Two workflows are supported:

**Explicit attach (recommended for stateful catalogs):**

```bash
# Attach and capture the attach ID
ATTACH_ID=$(vgi-client catalog attach mydb --worker ./worker.py | jq -r '.attach_opaque_data')

# Use attach ID for subsequent operations
vgi-client catalog schema list --attach-opaque-data $ATTACH_ID --worker ./worker.py

# Detach when done
vgi-client catalog detach $ATTACH_ID --worker ./worker.py
```

**Auto-attach (for stateless catalogs):**

```bash
# Specify catalog name instead of attach ID
vgi-client catalog schema list --catalog mydb --worker ./worker.py
```

### catalog list

List available catalogs from a worker.

```bash
vgi-client catalog list --worker ./worker.py
```

### catalog attach

Attach to a catalog and get an attach ID.

```bash
vgi-client catalog attach <name> --worker <worker> [--options '{}']
```

**Output:**

```json
{
  "attach_opaque_data": "a1b2c3d4",
  "supports_transactions": true,
  "catalog_version": 1
}
```

### catalog detach

Detach from a catalog.

```bash
vgi-client catalog detach <attach_opaque_data> --worker <worker>
```

### catalog create

Create a new catalog.

```bash
vgi-client catalog create <name> --worker <worker> \
    [--on-conflict {error|ignore|replace}] \
    [--options '{}']
```

### catalog drop

Drop a catalog.

```bash
vgi-client catalog drop <name> --worker <worker>
```

### catalog version

Get the current catalog version.

```bash
vgi-client catalog version --catalog <name> --worker <worker>
```

---

## Schema Commands

Manage schemas within a catalog.

### schema list

List all schemas in a catalog.

```bash
vgi-client catalog schema list --catalog <name> --worker <worker>
```

### schema get

Get schema details.

```bash
vgi-client catalog schema get <schema_name> --catalog <name> --worker <worker>
```

### schema create

Create a new schema.

```bash
vgi-client catalog schema create <schema_name> \
    --catalog <name> --worker <worker> \
    [--comment "Description"] \
    [--tags '{"key": "value"}']
```

### schema drop

Drop a schema.

```bash
vgi-client catalog schema drop <schema_name> \
    --catalog <name> --worker <worker> \
    [--ignore-not-found] [--cascade]
```

### schema contents

List all objects in a schema.

```bash
vgi-client catalog schema contents <schema_name> --catalog <name> --worker <worker>
```

---

## Table Commands

Manage tables within a schema.

### table get

Get table details.

```bash
vgi-client catalog table get <schema> <table> --catalog <name> --worker <worker>
```

### table create

Create a new table.

```bash
vgi-client catalog table create <schema> <table> \
    --catalog <name> --worker <worker> \
    --columns '[{"name": "id", "type": "int64"}, {"name": "name", "type": "string"}]' \
    [--not-null 0] \
    [--unique "0,1"] \
    [--check "id > 0"] \
    [--on-conflict {error|ignore|replace}]
```

**Supported column types:**

| Category | Types |
|----------|-------|
| Integer | `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64` |
| Float | `float16`, `float32`, `float64` |
| String | `string`, `utf8`, `large_string`, `binary`, `large_binary` |
| Boolean | `bool`, `boolean` |
| Date | `date32`, `date64` |
| Timestamp | `timestamp`, `timestamp_s`, `timestamp_ms`, `timestamp_us`, `timestamp_ns` |
| Duration | `duration`, `duration_s`, `duration_ms`, `duration_us`, `duration_ns` |
| Time | `time32`, `time64` |

### table drop

Drop a table.

```bash
vgi-client catalog table drop <schema> <table> \
    --catalog <name> --worker <worker> \
    [--ignore-not-found]
```

### table rename

Rename a table.

```bash
vgi-client catalog table rename <schema> <old_name> <new_name> \
    --catalog <name> --worker <worker>
```

### table comment

Set or clear table comment.

```bash
# Set comment
vgi-client catalog table comment <schema> <table> \
    --catalog <name> --worker <worker> \
    --set "Table description"

# Clear comment
vgi-client catalog table comment <schema> <table> \
    --catalog <name> --worker <worker> \
    --clear
```

### table scan-function

Get the scan function for a table.

```bash
vgi-client catalog table scan-function <schema> <table> \
    --catalog <name> --worker <worker>
```

---

## Column Commands

Modify table columns.

### column add

Add a column to a table.

```bash
vgi-client catalog table column add <schema> <table> \
    --catalog <name> --worker <worker> \
    --column '{"name": "email", "type": "string"}' \
    [--if-not-exists]
```

### column drop

Drop a column from a table.

```bash
vgi-client catalog table column drop <schema> <table> <column> \
    --catalog <name> --worker <worker> \
    [--if-exists] [--cascade]
```

### column rename

Rename a column.

```bash
vgi-client catalog table column rename <schema> <table> <old_name> <new_name> \
    --catalog <name> --worker <worker>
```

### column set-default

Set column default value.

```bash
vgi-client catalog table column set-default <schema> <table> <column> "0" \
    --catalog <name> --worker <worker>
```

### column drop-default

Remove column default value.

```bash
vgi-client catalog table column drop-default <schema> <table> <column> \
    --catalog <name> --worker <worker>
```

### column set-type

Change column type.

```bash
vgi-client catalog table column set-type <schema> <table> \
    --catalog <name> --worker <worker> \
    --column '{"name": "count", "type": "int64"}' \
    [--using "CAST(count AS int64)"]
```

### column set-not-null / drop-not-null

Set or remove NOT NULL constraint.

```bash
vgi-client catalog table column set-not-null <schema> <table> <column> \
    --catalog <name> --worker <worker>

vgi-client catalog table column drop-not-null <schema> <table> <column> \
    --catalog <name> --worker <worker>
```

---

## View Commands

Manage views within a schema.

### view get

Get view details.

```bash
vgi-client catalog view get <schema> <view> --catalog <name> --worker <worker>
```

### view create

Create a view.

```bash
vgi-client catalog view create <schema> <view> \
    --catalog <name> --worker <worker> \
    --definition "SELECT id, name FROM users WHERE active = true" \
    [--on-conflict {error|ignore|replace}]
```

### view drop

Drop a view.

```bash
vgi-client catalog view drop <schema> <view> \
    --catalog <name> --worker <worker> \
    [--ignore-not-found]
```

### view rename

Rename a view.

```bash
vgi-client catalog view rename <schema> <old_name> <new_name> \
    --catalog <name> --worker <worker>
```

### view comment

Set or clear view comment.

```bash
vgi-client catalog view comment <schema> <view> \
    --catalog <name> --worker <worker> \
    --set "View description"
```

---

## Transaction Commands

Manage transactions for catalogs that support them.

### transaction begin

Begin a new transaction.

```bash
TX_ID=$(vgi-client catalog transaction begin \
    --attach-opaque-data $ATTACH_ID --worker <worker> | jq -r '.transaction_opaque_data')
```

### transaction commit

Commit a transaction.

```bash
vgi-client catalog transaction commit $TX_ID \
    --attach-opaque-data $ATTACH_ID --worker <worker>
```

### transaction rollback

Rollback a transaction.

```bash
vgi-client catalog transaction rollback $TX_ID \
    --attach-opaque-data $ATTACH_ID --worker <worker>
```

### Transaction Example

```bash
# Attach to catalog
ATTACH_ID=$(vgi-client catalog attach mydb --worker ./worker.py | jq -r '.attach_opaque_data')

# Begin transaction
TX_ID=$(vgi-client catalog transaction begin \
    --attach-opaque-data $ATTACH_ID --worker ./worker.py | jq -r '.transaction_opaque_data')

# Make changes within transaction
vgi-client catalog table create main users \
    --attach-opaque-data $ATTACH_ID --transaction-opaque-data $TX_ID --worker ./worker.py \
    --columns '[{"name":"id","type":"int64"}]'

# Commit or rollback
vgi-client catalog transaction commit $TX_ID \
    --attach-opaque-data $ATTACH_ID --worker ./worker.py

# Detach
vgi-client catalog detach $ATTACH_ID --worker ./worker.py
```

---

## Worker Logging

All workers that use `Worker.main()` (including `vgi-fixture-worker`) support
logging options on the command line. Logs are written to stderr.

### Options

| Option | Description |
|--------|-------------|
| `--debug` | Enable DEBUG level on all `vgi` and `vgi_rpc` loggers |
| `--log-level LEVEL` | Set log level: `DEBUG`, `INFO` (default), `WARNING`, `ERROR` |
| `--log-logger NAME` | Target specific logger(s) instead of all defaults (repeatable) |
| `--log-format FORMAT` | Stderr format: `text` (default) or `json` |
| `--quiet` / `-q` | Suppress the interactive-terminal startup warning |

`--debug` overrides `--log-level` when both are provided.

### Examples

```bash
# Enable debug logging
vgi-fixture-worker --debug

# Set WARNING level only
vgi-fixture-worker --log-level WARNING

# Target a specific logger at DEBUG
vgi-fixture-worker --log-level DEBUG --log-logger vgi.worker

# JSON-formatted logs (for structured log pipelines)
vgi-fixture-worker --log-format json
```

### Available Loggers

| Logger | Description |
|--------|-------------|
| `vgi` | VGI root logger (all VGI messages) |
| `vgi.worker` | Worker lifecycle (startup, shutdown) |
| `vgi.client` | Client operations (spawn, bind, exchange) |
| `vgi.client.cli` | CLI front-end (argument parsing) |
| `vgi.filter_pushdown` | Filter pushdown debug (deserialization/evaluation) |
| `vgi_rpc` | vgi_rpc root logger (all vgi_rpc messages) |
| `vgi_rpc.wire.request` | RPC wire request (serialised request bytes) |
| `vgi_rpc.wire.response` | RPC wire response (serialised response bytes) |
| `vgi_rpc.wire.transport` | Transport layer (pipe/HTTP transport debug) |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `VGI_QUIET=1` | Suppress the interactive-terminal startup warning (same as `--quiet`) |
| `VGI_FILTER_DEBUG=1` | Enable filter pushdown debug logging |
| `VGI_BEARER_TOKENS` | Comma-separated `token=principal` pairs for static bearer auth (HTTP only) |
| `VGI_JWT_ISSUER` | JWT issuer URL for JWT/JWKS auth (requires `vgi[oauth]` extra) |
| `VGI_JWT_AUDIENCE` | JWT audience string, comma-separated for multiple audiences (required when `VGI_JWT_ISSUER` is set) |
| `VGI_JWT_JWKS_URI` | JWKS endpoint URL (auto-discovered if omitted) |
| `VGI_OAUTH_RESOURCE` | OAuth resource URL for RFC 9728 metadata |
| `VGI_OAUTH_AUTH_SERVERS` | Comma-separated authorization server URLs |
| `VGI_OAUTH_CLIENT_ID` | Client ID for MCP compatibility (optional, URL-safe chars only) |
| `VGI_OTEL_ENABLED` | Enable OpenTelemetry instrumentation (`1`/`true`/`yes`) |
| `VGI_OTEL_CUSTOM_ATTRIBUTES` | Comma-separated `key=value` pairs for custom span/metric attributes |
| `VGI_OTEL_CLAIM_ATTRIBUTES` | Comma-separated `claim_key=span_attr_name` pairs for claim extraction |
| `VGI_OTEL_DISABLE_TRACING` | Disable tracing only (`1`/`true`/`yes`) |
| `VGI_OTEL_DISABLE_METRICS` | Disable metrics only (`1`/`true`/`yes`) |

> **Note:** Service name, exporters, and endpoints are configured via standard `OTEL_*` SDK
> env vars (e.g. `OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`).

**OTEL usage examples:**

```bash
# Enable OTEL with standard SDK configuration
VGI_OTEL_ENABLED=1 \
OTEL_SERVICE_NAME=my-vgi-worker \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
    vgi-serve my_worker.py --http

# With custom attributes and claim extraction
VGI_OTEL_ENABLED=1 \
VGI_OTEL_CUSTOM_ATTRIBUTES="deployment=prod,region=us-east-1" \
VGI_OTEL_CLAIM_ATTRIBUTES="tenant_id=rpc.vgi_rpc.auth.claim.tenant_id" \
    vgi-serve my_worker.py --http
```

Programmatic usage:

```python test="skip"
from vgi_rpc.otel import OtelConfig
from vgi.serve import create_app, load_worker_class

app = create_app(
    load_worker_class("my_worker:MyWorker"),
    otel_config=OtelConfig(
        custom_attributes={"deployment": "prod"},
        claim_attributes={"tenant_id": "rpc.vgi_rpc.auth.claim.tenant_id"},
    ),
)
```

---

## Example Workers

### vgi-fixture-worker

Runs the built-in example worker with demo functions.

```bash
vgi-fixture-worker
```

**Available functions:**

| Function | Type | Description |
|----------|------|-------------|
| `echo` | table-in-out | Pass through input unchanged |
| `sum_all_columns` | table-in-out | Sum all numeric columns |
| `repeat_inputs` | table-in-out | Repeat each row N times |
| `buffer_input` | table-in-out | Collect all input, emit on finalize |
| `sequence` | table | Generate sequence of integers |
| `double_sequence` | table | Generate sequence of floats |
| `nested_sequence` | table | Generate sequence with nested struct/list columns |
| `partitioned_sequence` | table | Generate sequence across multiple workers |
| `projected_data` | table | Generate data with projection pushdown |
| `ten_thousand` | table | Generate 10000 integers |
| `constant_columns` | table | Generate rows with constant values from varargs |
| `named_params_echo` | table | Echo named parameter values in output columns |
| `multiply` | scalar | Multiply values by a constant factor |
| `double` | scalar | Double numeric values |
| `add_values` | scalar | Add two columns together |
| `sum_values` | scalar | Sum multiple numeric values (varargs) |
| `upper_case` | scalar | Convert string values to uppercase |
| `null_handling` | scalar | Returns value or -5000 if null |
| `random_int` | scalar | Generate random integers (VOLATILE) |
| `bernoulli` | scalar | Generate random booleans (VOLATILE) |
| `random_bytes` | scalar | Generate pseudo-random binary blobs |

### In-memory catalog example

The mutable catalog demo (`vgi/examples/catalog.py`) is no longer installed as a
console script. Run it from a source checkout via:

```bash
python -m vgi._test_fixtures.catalog
```

---

## Output Formats

### JSON (default)

Line-delimited JSON, one record per line:

```bash
vgi-client --function sequence --args '[3]' --format json
```

```json
{"n": 0}
{"n": 1}
{"n": 2}
```

### CSV

CSV with headers:

```bash
vgi-client --function sequence --args '[3]' --format csv
```

```csv
n
0
1
2
```

### Parquet

Binary Apache Parquet format (requires output file):

```bash
vgi-client --function sequence --args '[1000]' --format parquet --output data.parquet
```

---

## Common Patterns

### Piping Data

```bash
# Generate data and process it
vgi-client --function sequence --args '[100]' --format parquet --output /tmp/data.parquet
vgi-client --input /tmp/data.parquet --function sum_all_columns
```

### Using with jq

```bash
# Extract specific fields
vgi-client catalog attach mydb --worker ./worker.py | jq -r '.attach_opaque_data'

# Pretty print
vgi-client --function sequence --args '[3]' | jq .
```

### Shell Scripts

```bash
#!/bin/bash
WORKER="./my_worker.py"

# Attach
ATTACH_ID=$(vgi-client catalog attach mydb --worker $WORKER | jq -r '.attach_opaque_data')

# List schemas
vgi-client catalog schema list --attach-opaque-data $ATTACH_ID --worker $WORKER

# Cleanup
vgi-client catalog detach $ATTACH_ID --worker $WORKER
```
