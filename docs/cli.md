# VGI Command Line Interface

VGI provides CLI tools for invoking functions and managing catalogs without writing code.

## Available Commands

| Command | Description |
|---------|-------------|
| `vgi-client` | Invoke functions and manage catalogs |
| `vgi-example-worker` | Run the example worker with demo functions |
| `vgi-example-catalog-worker` | Run an in-memory catalog for testing |

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
| `--worker PATH` | Worker command (default: `vgi-example-worker`) |
| `--type TYPE` | Function type: `auto`, `table`, `table-in-out`, `scalar` |
| `--projection-id N` | Column IDs to project (repeatable) |
| `--max-workers N` | Limit parallel workers |
| `--worker-stderr` | Show worker stderr output |

### Examples

**Table function (generates data):**

```bash
# Generate a sequence of 100 integers
vgi-client --function sequence --args '[100]'

# Generate a range from 0 to 10
vgi-client --function range --args '[0, 10]'

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
# Double values in column "x"
vgi-client --input data.parquet --function double_column --args '["x"]' --type scalar
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
ATTACH_ID=$(vgi-client catalog attach mydb --worker ./worker.py | jq -r '.attach_id')

# Use attach ID for subsequent operations
vgi-client catalog schema list --attach-id $ATTACH_ID --worker ./worker.py

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
  "attach_id": "a1b2c3d4",
  "supports_transactions": true,
  "catalog_version": 1
}
```

### catalog detach

Detach from a catalog.

```bash
vgi-client catalog detach <attach_id> --worker <worker>
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
    --attach-id $ATTACH_ID --worker <worker> | jq -r '.transaction_id')
```

### transaction commit

Commit a transaction.

```bash
vgi-client catalog transaction commit $TX_ID \
    --attach-id $ATTACH_ID --worker <worker>
```

### transaction rollback

Rollback a transaction.

```bash
vgi-client catalog transaction rollback $TX_ID \
    --attach-id $ATTACH_ID --worker <worker>
```

### Transaction Example

```bash
# Attach to catalog
ATTACH_ID=$(vgi-client catalog attach mydb --worker ./worker.py | jq -r '.attach_id')

# Begin transaction
TX_ID=$(vgi-client catalog transaction begin \
    --attach-id $ATTACH_ID --worker ./worker.py | jq -r '.transaction_id')

# Make changes within transaction
vgi-client catalog table create main users \
    --attach-id $ATTACH_ID --transaction-id $TX_ID --worker ./worker.py \
    --columns '[{"name":"id","type":"int64"}]'

# Commit or rollback
vgi-client catalog transaction commit $TX_ID \
    --attach-id $ATTACH_ID --worker ./worker.py

# Detach
vgi-client catalog detach $ATTACH_ID --worker ./worker.py
```

---

## Example Workers

### vgi-example-worker

Runs the built-in example worker with demo functions.

```bash
vgi-example-worker
```

**Available functions:**

| Function | Type | Description |
|----------|------|-------------|
| `echo` | table-in-out | Pass through input unchanged |
| `sum_all_columns` | table-in-out | Sum all numeric columns |
| `repeat_inputs` | table-in-out | Repeat each row N times |
| `sequence` | table | Generate sequence of integers |
| `range` | table | Generate range of integers |
| `double_column` | scalar | Double values in a column |
| `add_columns` | scalar | Add two columns together |

### vgi-example-catalog-worker

Runs an in-memory catalog implementation for testing.

```bash
vgi-example-catalog-worker
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
vgi-client catalog attach mydb --worker ./worker.py | jq -r '.attach_id'

# Pretty print
vgi-client --function sequence --args '[3]' | jq .
```

### Shell Scripts

```bash
#!/bin/bash
WORKER="./my_worker.py"

# Attach
ATTACH_ID=$(vgi-client catalog attach mydb --worker $WORKER | jq -r '.attach_id')

# List schemas
vgi-client catalog schema list --attach-id $ATTACH_ID --worker $WORKER

# Cleanup
vgi-client catalog detach $ATTACH_ID --worker $WORKER
```
