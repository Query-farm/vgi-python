# VGI Protocol Flow

This document describes the Arrow IPC protocol between client and worker.

## Scalar Function (row transform)

Scalar functions transform input batches to single-column output with 1:1 row mapping.
They are used for per-row computations like `upper()`, `abs()`, or `concat()`.

```
Client                                  Worker
  │                                       │
  │──── Invocation (function, args) ─────▶│
  │                                       │ instantiate function
  │◀──── OutputSpec (output schema) ──────│ (single column)
  │                                       │
  │──── Input Batch 1 ───────────────────▶│
  │◀──── Output Batch 1 ─────────────────│ compute() / process()
  │                                       │ (same row count)
  │──── Input Batch 2 ───────────────────▶│
  │◀──── Output Batch 2 ─────────────────│
  │                                       │
  │──── (generator closed) ──────────────▶│
  │                                       │ teardown()
```

**Key differences from Table-In-Out:**
- Output schema must have exactly one column
- Output row count must equal input row count (1:1 mapping)
- No finalize phase - processing ends when input stream ends
- No `NEED_MORE_INPUT` status (always expects more input until closed)

## Table Function (no input)

Table functions generate data without receiving input batches.

```
Client                                  Worker
  │                                       │
  │──── Invocation (function, args) ─────▶│
  │                                       │ instantiate function
  │◀──── OutputSpec (output schema) ──────│
  │                                       │
  │──── GlobalStateInitInput ────────────▶│
  │◀──── InitResult ────────────────│ initialize_global_state()
  │                                       │
  │◀──── Output Batch 1 ──────────────────│ process() yields
  │◀──── Output Batch 2 ──────────────────│
  │◀──── ... ─────────────────────────────│
  │◀──── Final Output (FINISHED) ─────────│
  │                                       │
```

## Table-In-Out Function (with input)

Table-in-out functions transform input batches to output batches.

```
Client                                  Worker
  │                                       │
  │──── Invocation (function, args) ─────▶│
  │                                       │ instantiate function
  │◀──── OutputSpec (output schema) ──────│
  │                                       │
  │──── GlobalStateInitInput ────────────▶│
  │◀──── InitResult ────────────────│ initialize_global_state()
  │                                       │
  │──── Input Batch 1 ───────────────────▶│
  │◀──── Output Batch 1 (NEED_MORE_INPUT)─│ transform() / process()
  │                                       │
  │──── Input Batch 2 ───────────────────▶│
  │◀──── Output Batch 2 (NEED_MORE_INPUT)─│
  │                                       │
  │──── FINALIZE (empty batch) ──────────▶│
  │◀──── Final Output (FINISHED) ─────────│ finish() / finalize()
  │                                       │
```

## Status Values (in IPC metadata)

| Status | Meaning |
|--------|---------|
| `NEED_MORE_INPUT` | Ready for next input batch |
| `HAVE_MORE_OUTPUT` | Call send() again for more output |
| `FINISHED` | Processing complete |

## Invocation Fields

The Invocation message contains the following fields:

| Field | Type | Description |
|-------|------|-------------|
| `function_name` | string | Name of the function to invoke |
| `arguments` | struct | Positional and named arguments |
| `input_schema` | binary | Arrow IPC serialized schema (nullable) |
| `function_type` | string | `SCALAR` or `TABLE` |
| `invocation_id` | binary | Unique ID for this binding (nullable) |
| `correlation_id` | string | For request tracing/logging |
| `global_execution_identifier` | binary | Shared state ID for parallel workers |
| `client_features` | list\<string\> | Feature flags from client |
| `attach_id` | binary | DuckDB attachment identifier (nullable) |
| `duckdb_settings` | map\<string, string\> | DuckDB settings/pragmas (nullable) |

## DuckDB Settings

Functions can declare required DuckDB settings via `Meta.required_settings`. These
settings are passed from client to worker in the Invocation during the bind phase.

```python
class MyFunction(TableFunctionGenerator):
    class Meta:
        required_settings = ["TimeZone", "threads"]

    @property
    def output_schema(self) -> pa.Schema:
        # Settings available during bind
        tz = self.get_setting("TimeZone", "UTC")
        return pa.schema([("timestamp", pa.timestamp("us", tz=tz))])
```

The worker validates that all required settings are present before instantiating
the function. Missing settings result in an error.
