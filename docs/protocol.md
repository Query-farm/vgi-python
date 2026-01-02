# VGI Protocol Flow

This document describes the Arrow IPC protocol between client and worker.

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
  │◀──── GlobalInitResult ────────────────│ perform_init()
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
  │◀──── GlobalInitResult ────────────────│ perform_init()
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
