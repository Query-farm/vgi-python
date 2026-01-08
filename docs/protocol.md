# VGI Protocol Specification

This document describes the Arrow IPC streaming protocol between VGI clients and worker processes.

## Protocol Summary

VGI uses Apache Arrow IPC streaming over stdin/stdout for communication:

| Direction | Format | Description |
|-----------|--------|-------------|
| Client → Worker | Arrow IPC | Invocation, then input batches (if any) |
| Worker → Client | Arrow IPC | OutputSpec, then output batches with status metadata |

**Key concepts:**
- All messages are Arrow RecordBatches serialized to IPC streaming format
- Status is communicated via custom metadata on output batches
- Three function types: Scalar (1:1 transform), Table (generator), Table-In-Out (transform with finalize)

---

## Function Types Overview

| Type | Base Class | Input | Output | Use Case |
|------|------------|-------|--------|----------|
| **Scalar** | `ScalarFunction` | Batches | Single column, same row count | `upper()`, `abs()`, per-row transforms |
| **Table** | `TableFunctionGenerator` | None | Multi-column batches | `range()`, `read_csv()`, data generation |
| **Table-In-Out** | `TableInOutFunction` | Batches | Multi-column batches | Filtering, aggregation, enrichment |

---

## Message Flow Diagrams

### Scalar Function Protocol

Scalar functions transform input batches to single-column output with 1:1 row mapping.

```
Client                                  Worker
  │                                       │
  │──── Invocation ───────────────────▶  │ deserialize, lookup function
  │                                       │
  │◀──── OutputSpec ──────────────────   │ single-column schema
  │                                       │
  │──── Input Batch 1 ────────────────▶  │
  │◀──── Output Batch 1 ──────────────   │ compute() → Array (same row count)
  │                                       │
  │──── Input Batch N ────────────────▶  │
  │◀──── Output Batch N ──────────────   │
  │                                       │
  │──── (close generator) ────────────▶  │ teardown()
  └───────────────────────────────────────┘
```

**Constraints:**
- Output schema: exactly 1 column named "result"
- Output row count = input row count (enforced by framework)
- No finalize phase
- No `NEED_MORE_INPUT` status (implicit: always ready)

### Table Function Protocol

Table functions generate data without receiving input batches.

```
Client                                  Worker
  │                                       │
  │──── Invocation ───────────────────▶  │ deserialize, lookup function
  │                                       │
  │◀──── OutputSpec ──────────────────   │ output schema + cardinality
  │                                       │
  │──── GlobalStateInitInput ─────────▶  │ (empty batch with projection info)
  │◀──── InitResult ──────────────────   │ initialize_global_state()
  │                                       │
  │◀──── Output Batch 1 ──────────────   │ process() yields Output(batch)
  │◀──── Output Batch 2 ──────────────   │
  │◀──── ... ─────────────────────────   │
  │◀──── Output (FINISHED) ───────────   │ generator exhausted
  │                                       │ teardown()
  └───────────────────────────────────────┘
```

### Table-In-Out Function Protocol

Table-in-out functions transform input batches with an optional finalize phase.

```
Client                                  Worker
  │                                       │
  │──── Invocation ───────────────────▶  │ deserialize, lookup function
  │                                       │
  │◀──── OutputSpec ──────────────────   │ output schema
  │                                       │
  │──── GlobalStateInitInput ─────────▶  │ (empty batch with projection)
  │◀──── InitResult ──────────────────   │ initialize_global_state()
  │                                       │
  │──── Input Batch 1 ────────────────▶  │
  │◀──── Output (NEED_MORE_INPUT) ────   │ transform() → batch
  │                                       │
  │──── Input Batch N ────────────────▶  │
  │◀──── Output (NEED_MORE_INPUT) ────   │
  │                                       │
  │──── FINALIZE (empty batch) ───────▶  │ signals end of input
  │◀──── Output Batch ────────────────   │ finish() → final batches
  │◀──── Output (FINISHED) ───────────   │
  │                                       │ teardown()
  └───────────────────────────────────────┘
```

---

## Status Values

Output batches include a status string in Arrow IPC custom metadata:

| Status | Meaning | Next Action |
|--------|---------|-------------|
| `NEED_MORE_INPUT` | Ready for next input batch | Client sends next batch |
| `HAVE_MORE_OUTPUT` | More output available | Client calls send() again |
| `FINISHED` | Processing complete | Client closes connection |

**Status metadata key:** `vgi.status` in batch metadata

---

## Message Formats

### Invocation (Client → Worker)

First message sent when invoking a function. Serialized as Arrow RecordBatch with single row.

| Field | Arrow Type | Description |
|-------|------------|-------------|
| `function_name` | `string` | Function name in worker registry |
| `arguments` | `struct` | Positional and named arguments |
| `input_schema` | `binary` | Arrow IPC serialized schema (nullable) |
| `function_type` | `string` | `"SCALAR"` or `"TABLE"` |
| `invocation_id` | `binary` | Unique ID for this invocation (nullable) |
| `correlation_id` | `string` | Request tracing/logging ID |
| `global_execution_identifier` | `binary` | Shared state ID for parallel workers (nullable) |
| `client_features` | `list<string>` | Client capability flags |
| `attach_id` | `binary` | DuckDB attachment identifier (nullable) |
| `settings` | `map<string, string>` | DuckDB settings/pragmas (nullable) |

**Arguments struct:**
```
arguments: {
  positional: list<dense_union<...Arrow scalar types...>>,
  named: map<string, dense_union<...Arrow scalar types...>>
}
```

### OutputSpec (Worker → Client)

Response to Invocation, describing output schema and execution hints.

| Field | Arrow Type | Description |
|-------|------------|-------------|
| `output_schema` | `binary` | Arrow IPC serialized output schema |
| `cardinality` | `struct` | Row count estimates (table functions) |
| `max_processes` | `int32` | Max parallel workers allowed (nullable) |
| `requires_finalize` | `bool` | Whether finalize phase is needed |
| `stability` | `string` | Output determinism hint |

**Cardinality struct:**
```
cardinality: {
  estimate: int64 (nullable),
  min: int64 (nullable),
  max: int64 (nullable)
}
```

### GlobalStateInitInput (Client → Worker)

Sent after OutputSpec to initialize global state (table/table-in-out functions).

| Field | Arrow Type | Description |
|-------|------------|-------------|
| `projected_columns` | `list<string>` | Requested output columns (nullable) |

### InitResult (Worker → Client)

Response to GlobalStateInitInput.

| Field | Arrow Type | Description |
|-------|------------|-------------|
| `global_execution_identifier` | `binary` | Identifier for shared state (nullable) |

### Data Batches

Input and output data are standard Arrow RecordBatches matching the declared schemas.

**Output batch metadata includes:**
- `vgi.status`: One of `NEED_MORE_INPUT`, `HAVE_MORE_OUTPUT`, `FINISHED`
- `vgi.log_level`: Log level if log message present (optional)
- `vgi.log_message`: Log message text (optional)
- `vgi.log_extra`: JSON-encoded additional context (optional)

---

## Error Handling

Errors are communicated via log messages with level `EXCEPTION`:

```
Output batch metadata:
  vgi.status: "FINISHED"
  vgi.log_level: "EXCEPTION"
  vgi.log_message: "Error message with stack trace"
```

**Error categories:**
- **Function not found:** Unknown function name in registry
- **Argument mismatch:** Wrong number or type of arguments
- **Schema mismatch:** Output doesn't match declared schema
- **Row count mismatch:** Scalar function output row count ≠ input
- **Runtime errors:** Exceptions during transform/compute

---

## Parallel Execution

When `max_workers > 1`, the client may spawn multiple worker processes:

**Primary Worker:**
1. Receives `global_execution_identifier = None` in Invocation
2. Runs `initialize_global_state()` → returns InitResult with new identifier
3. Runs `finalize()` after all workers complete

**Secondary Workers:**
1. Receive `global_execution_identifier` from primary's InitResult
2. Run `load_global_state()` with the identifier
3. Do NOT run `finalize()`

**State synchronization:**
- Workers use `save_state()` to persist partial results
- Primary uses `load_states()` to collect all worker states before finalize
- State storage: SQLite database at platform-specific cache directory

---

## DuckDB Settings

Functions can declare required settings in `Meta.required_settings`:

```python
class MyFunction(TableFunctionGenerator):
    class Meta:
        required_settings = ["TimeZone", "threads"]
```

Settings are passed in the Invocation and available during bind:
- Access via `self.settings` dict or `self.get_setting(name, default)`
- Available before `output_schema` property is accessed
- Missing required settings cause invocation to fail

---

## Wire Format Details

**Serialization:** Apache Arrow IPC streaming format
- Each message is a complete IPC stream (schema + batch)
- Uses `pa.ipc.new_stream()` / `pa.ipc.RecordBatchStreamReader`

**Transport:** stdin/stdout of worker subprocess
- Client writes to worker's stdin
- Client reads from worker's stdout
- Worker stderr is captured for diagnostics

**Byte order:** Native endianness (Arrow handles cross-platform)

---

## Implementation Notes

**Framework guarantees:**
- `teardown()` always called, even on exceptions
- Schema validation before sending to client
- Row count validation for scalar functions
- Automatic error wrapping with stack traces

**Client responsibilities:**
- Spawn worker subprocess with correct command
- Send Invocation as first message
- Handle all status values correctly
- Close connection gracefully

**Worker responsibilities:**
- Parse Invocation and lookup function
- Validate arguments against function signature
- Send OutputSpec with correct schema
- Handle GeneratorExit for cleanup
