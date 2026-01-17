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
- Workers support multiple invocations per process (loop until stdin EOF)

---

## IPC Stream Structure

**IMPORTANT FOR IMPLEMENTORS:** The protocol uses multiple distinct Arrow IPC streams. Each stream is a complete IPC message containing: schema → batch(es) → end-of-stream marker.

### Stream Boundaries

The protocol has two phases with different stream patterns:

#### Phase 1: Handshake (4 separate single-batch streams)

During handshake, each message is its own **complete IPC stream** containing exactly one batch:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ HANDSHAKE PHASE - Each arrow is a separate IPC stream                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Client                                              Worker                │
│     │                                                   │                   │
│     │─── Stream 1: Invocation ──────────────────────▶  │                   │
│     │    [schema + 1 batch + EOS]                       │                   │
│     │                                                   │                   │
│     │◀── Stream 2: OutputSpec ─────────────────────────│                   │
│     │    [schema + 1 batch + EOS]                       │                   │
│     │                                                   │                   │
│     │─── Stream 3: InitInput ───────────────────────▶  │                   │
│     │    [schema + 1 batch + EOS]                       │                   │
│     │                                                   │                   │
│     │◀── Stream 4: InitResult ─────────────────────────│                   │
│     │    [schema + 1 batch + EOS]                       │                   │
│     │                                                   │                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Phase 2: Data Transfer (2 long-lived multi-batch streams)

During data transfer, each direction uses a **single long-lived IPC stream** containing multiple batches:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ DATA PHASE - Two persistent streams (one per direction)                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Client                                              Worker                │
│     │                                                   │                   │
│     │═══ Stream 5: Input Data Stream ══════════════▶   │                   │
│     │    [schema]                                       │                   │
│     │         ├── batch 1 ──────────────────────────▶  │                   │
│     │         ├── batch 2 ──────────────────────────▶  │                   │
│     │         ├── ...                                   │                   │
│     │         ├── batch N ──────────────────────────▶  │                   │
│     │    [EOS - close stream]                           │                   │
│     │                                                   │                   │
│     │◀══ Stream 6: Output Data Stream ═════════════════│                   │
│     │                                       [schema]    │                   │
│     │   ◀── batch 1 (+ vgi.status metadata) ───────────│                   │
│     │   ◀── batch 2 (+ vgi.status metadata) ───────────│                   │
│     │   ◀── ...                                         │                   │
│     │   ◀── batch N (vgi.status="FINISHED") ───────────│                   │
│     │                                [EOS - close stream]                   │
│     │                                                   │                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Stream Summary Table

| Stream | Direction | Phase | Content | Batches |
|--------|-----------|-------|---------|---------|
| 1 | Client → Worker | Handshake | Invocation | 1 |
| 2 | Worker → Client | Handshake | OutputSpec (bind result) | 1 |
| 3 | Client → Worker | Handshake | InitInput | 1 |
| 4 | Worker → Client | Handshake | InitResult | 1 |
| 5 | Client → Worker | Data | Input batches | 0..N |
| 6 | Worker → Client | Data | Output batches with status | 1..N (always ≥1) |

**Notes:**
- Stream 5 is absent for Table functions (no input)
- Handshake streams use `serialize_record_batch()` → complete IPC stream bytes
- Data streams use `ipc.new_stream()` / `ipc.open_stream()` for streaming multiple batches
- All protocol batches include protocol state metadata (see [Protocol State Metadata](#protocol-state-metadata))

---

## Protocol State Metadata

**IMPORTANT:** All record batches in the VGI protocol must include protocol state metadata. This allows receivers to validate they're processing the expected message type and helps debug protocol synchronization issues.

### Protocol State Key

Protocol state is stored in batch custom metadata with the key:
```
vgi.protocol_state
```

### Protocol States

| State | Message Type | Direction |
|-------|--------------|-----------|
| `invocation` | Invocation | Client → Worker |
| `bind_result` | OutputSpec | Worker → Client |
| `init_input` | InitInput/GlobalStateInitInput | Client → Worker |
| `init_result` | InitResult | Worker → Client |
| `data` | Input data batches | Client → Worker |
| `output` | Output data batches | Worker → Client |
| `catalog_args` | Catalog operation arguments | Client → Worker |
| `catalog_result` | Catalog operation results | Worker → Client |

### Implementation

**Attaching protocol state (sender):**
```python test="skip"
from vgi.ipc_utils import protocol_state_metadata, ProtocolState

# For single-batch streams (handshake)
metadata = protocol_state_metadata(ProtocolState.INVOCATION)
bytes_data = serialize_record_batch(batch, custom_metadata=metadata)

# For multi-batch streams (data phase)
writer.write_batch(batch, custom_metadata=protocol_state_metadata(ProtocolState.OUTPUT))
```

**Validating protocol state (receiver):**
```python test="skip"
from vgi.ipc_utils import get_protocol_state, validate_single_row_batch, ProtocolState

# For single-batch protocol messages
batch, metadata = deserialize_record_batch(data)
row = validate_single_row_batch(
    batch, "Invocation",
    required_fields=["function_name", "function_type"],
    custom_metadata=metadata,
    expected_protocol_state=ProtocolState.INVOCATION
)

# For data streams, check state manually
batch, metadata = reader.read_next_batch_with_custom_metadata()
state = get_protocol_state(metadata)
if state != ProtocolState.OUTPUT:
    raise ValueError(f"Expected 'output' state, got '{state}'")
```

### Error Handling

When protocol state validation fails, a `ValueError` is raised with details:
```
Protocol state mismatch for Invocation: expected 'invocation', got 'bind_result'.
Batch fields: ['function_name', 'function_type', ...]
```

This helps identify when streams are out of sync or messages are being read in the wrong order.

---

## Function Types Overview

| Type | Base Class | Input | Output | Use Case |
|------|------------|-------|--------|----------|
| **Scalar** | `ScalarFunction` | Batches | Single column, same row count | `upper()`, `abs()`, per-row transforms |
| **Table** | `TableFunctionGenerator` | None | Multi-column batches | `range()`, `read_csv()`, data generation |
| **Table-In-Out** | `TableInOutFunction` | Batches | Multi-column batches | Filtering, aggregation, enrichment |

---

## Message Flow Diagrams

These diagrams show the logical message flow. See [IPC Stream Structure](#ipc-stream-structure) above for the underlying stream boundaries.

### Scalar Function Protocol

Scalar functions transform input batches to single-column output with 1:1 row mapping.

```
Client                                  Worker
  │                                       │
  │──── Invocation ───────────────────▶  │ deserialize, lookup function
  │      (Stream 1)                       │
  │                                       │
  │◀──── OutputSpec ──────────────────   │ single-column schema
  │      (Stream 2)                       │
  │                                       │
  │──── InitInput ────────────────────▶  │
  │      (Stream 3)                       │
  │◀──── InitResult ──────────────────   │ initialize_global_state()
  │      (Stream 4)                       │
  │                                       │
  │══════════════════════════════════════│ Data phase begins
  │                                       │
  │──── Input Batch 1 ────────────────▶  │
  │      (Stream 5)                       │
  │◀──── Output Batch 1 ──────────────   │ compute() → Array (same row count)
  │      (Stream 6)                       │
  │                                       │
  │──── Input Batch N ────────────────▶  │
  │◀──── Output Batch N ──────────────   │
  │                                       │
  │──── FINALIZE (empty batch) ───────▶  │ teardown(), return to waiting
  │      ({type: FINALIZE} metadata)      │
  └───────────────────────────────────────┘
```

**Constraints:**
- Output schema: exactly 1 column named "result"
- Output row count = input row count (enforced by framework)
- Finalize: empty batch with `{type: FINALIZE}` metadata, no response expected
- No `NEED_MORE_INPUT` status (implicit: always ready)

### Table Function Protocol

Table functions generate data without receiving input batches. **No Stream 5** (no input).

```
Client                                  Worker
  │                                       │
  │──── Invocation ───────────────────▶  │ deserialize, lookup function
  │      (Stream 1)                       │
  │                                       │
  │◀──── OutputSpec ──────────────────   │ output schema + cardinality
  │      (Stream 2)                       │
  │                                       │
  │──── InitInput ────────────────────▶  │ (empty batch with projection info)
  │      (Stream 3)                       │
  │◀──── InitResult ──────────────────   │ initialize_global_state()
  │      (Stream 4)                       │
  │                                       │
  │══════════════════════════════════════│ Data phase begins (output only)
  │                                       │
  │◀──── Output Batch 1 ──────────────   │ process() yields Output(batch)
  │      (Stream 6)                       │
  │◀──── Output Batch 2 ──────────────   │
  │◀──── ... ─────────────────────────   │
  │◀──── Output (FINISHED) ───────────   │ generator exhausted
  │                                       │ teardown()
  └───────────────────────────────────────┘
```

**Empty output handling:**
- If `process()` yields no batches, the worker emits an empty batch with `FINISHED` status
- This ensures the client receives a completion signal and doesn't block waiting for data
- The client filters out empty batches before yielding to callers (protocol detail not exposed to users)

### Table-In-Out Function Protocol

Table-in-out functions transform input batches with an optional finalize phase.

```
Client                                  Worker
  │                                       │
  │──── Invocation ───────────────────▶  │ deserialize, lookup function
  │      (Stream 1)                       │
  │                                       │
  │◀──── OutputSpec ──────────────────   │ output schema
  │      (Stream 2)                       │
  │                                       │
  │──── InitInput ────────────────────▶  │ (empty batch with projection)
  │      (Stream 3)                       │
  │◀──── InitResult ──────────────────   │ initialize_global_state()
  │      (Stream 4)                       │
  │                                       │
  │══════════════════════════════════════│ Data phase begins
  │                                       │
  │──── Input Batch 1 ────────────────▶  │
  │      (Stream 5)                       │
  │◀──── Output (NEED_MORE_INPUT) ────   │ transform() → batch
  │      (Stream 6)                       │
  │                                       │
  │──── Input Batch N ────────────────▶  │
  │◀──── Output (NEED_MORE_INPUT) ────   │
  │                                       │
  │──── (close Stream 5) ─────────────▶  │ signals end of input
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

**Protocol state:** `invocation`

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

**Protocol state:** `bind_result`

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

**Protocol state:** `init_input`

| Field | Arrow Type | Description |
|-------|------------|-------------|
| `projected_columns` | `list<string>` | Requested output columns (nullable) |

### InitResult (Worker → Client)

Response to GlobalStateInitInput.

**Protocol state:** `init_result`

| Field | Arrow Type | Description |
|-------|------------|-------------|
| `global_execution_identifier` | `binary` | Identifier for shared state (nullable) |

### Data Batches

Input and output data are standard Arrow RecordBatches matching the declared schemas.

**Input batch protocol state:** `data`

**Output batch protocol state:** `output`

**Output batch metadata includes:**
- `vgi.protocol_state`: Always `output`
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

Each IPC stream consists of:
1. **Schema message** - Arrow schema with field types and metadata
2. **RecordBatch message(s)** - Data batches conforming to schema
3. **End-of-stream marker** - Signals stream completion

**Handshake streams (Streams 1-4):**
- Serialized via `serialize_record_batch()` which produces complete IPC stream bytes
- Each contains exactly 1 batch
- Written/read atomically (full stream bytes at once)

**Data streams (Streams 5-6):**
- Created via `pa.ipc.new_stream(sink, schema)` / `pa.ipc.open_stream(source)`
- Schema written once at stream start
- Multiple batches written via `writer.write_batch(batch, custom_metadata=...)`
- Stream closed by closing the writer or reaching end-of-stream

**Custom Metadata:**
- Attached per-batch via `write_batch(batch, custom_metadata=pa.KeyValueMetadata({...}))`
- Read via `reader.read_next_batch_with_custom_metadata()` → `(batch, metadata)`
- Keys are bytes in metadata dict (e.g., `b"vgi.status"`, `b"vgi.protocol_state"`)
- **Protocol state is required** on all batches (see [Protocol State Metadata](#protocol-state-metadata))

**Transport:** stdin/stdout of worker subprocess
- Client writes to worker's stdin (Streams 1, 3, 5)
- Client reads from worker's stdout (Streams 2, 4, 6)
- Worker stderr is captured for diagnostics

**Byte order:** Native endianness (Arrow handles cross-platform)

---

## Worker Lifecycle

Workers support multiple invocations within a single process. After completing one invocation (all 6 streams), the worker loops back to wait for another invocation on stdin.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ WORKER LIFECYCLE - Multiple invocations per process                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Worker Process                                                            │
│     │                                                                       │
│     ├──▶ Wait for Invocation on stdin                                       │
│     │         │                                                             │
│     │         ├── EOF detected ──────────────▶ Exit cleanly                 │
│     │         │                                                             │
│     │         ▼                                                             │
│     │    Process Invocation (Streams 1-6)                                   │
│     │         │                                                             │
│     │         ├── Handshake: Invocation → OutputSpec → Init                 │
│     │         ├── Data: Input batches → Output batches                      │
│     │         ├── Finalize (if applicable)                                  │
│     │         │                                                             │
│     │         ▼                                                             │
│     └──── Loop back to wait for next invocation                             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Worker exit conditions:**
- **EOF on stdin:** When the client closes stdin, the worker detects EOF while waiting for the next invocation and exits cleanly
- **Fatal error:** Unrecoverable errors cause immediate exit with non-zero status

**Client responsibilities for worker shutdown:**
- After closing the IPC data stream (`data_writer.close()`), the client must also close `proc.stdin` to send EOF
- Closing only the IPC stream does not close the underlying pipe - the worker would block forever waiting for the next invocation

**Benefits of multi-invocation:**
- Reduced process spawn overhead for repeated calls
- Potential for connection pooling / worker reuse (future)
- Warm JIT / cached state between invocations

---

## Implementation Notes

**Framework guarantees:**
- `teardown()` always called, even on exceptions
- Schema validation before sending to client
- Row count validation for scalar functions
- Automatic error wrapping with stack traces
- Table functions always emit at least one batch (empty if no output) to signal completion
- Protocol state metadata attached to all serialized messages

**Client responsibilities:**
- Spawn worker subprocess with correct command
- Send Invocation as first message
- Handle all status values correctly
- Close stdin (not just IPC stream) to signal worker shutdown
- Filter out empty batches from table functions before yielding to callers
- Validate protocol state on received messages (bind_result, init_result, output)
- Attach protocol state metadata when sending messages (invocation, init_input, data)

**Worker responsibilities:**
- Parse Invocation and lookup function
- Validate arguments against function signature
- Send OutputSpec with correct schema
- Handle GeneratorExit for cleanup
- Loop to handle multiple invocations until EOF on stdin
- Validate protocol state on received messages (invocation, init_input, data)
- Attach protocol state metadata when sending messages (bind_result, init_result, output)
