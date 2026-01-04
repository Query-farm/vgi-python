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
  │◀──── InitResult ────────────────│ perform_init()
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
  │◀──── InitResult ────────────────│ perform_init()
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
