# Function Lifecycle

Understanding when lifecycle methods are called is critical for resource management and distributed processing.

## Scalar Function Lifecycle

Scalar functions have a simplified lifecycle with no finalize phase. Processing ends when input is exhausted.

```
┌─────────────────────────────────────────────────────────────────┐
│  bind(request) → BindResponse                                   │
│    ↓                                                            │
│  init(request) → Stream with ScalarExchangeState                │
│    ↓                                                            │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  exchange(batch1) → compute(batch1) → Array             │    │
│  │    ↓                                                    │    │
│  │  [return output with same row count]                    │    │
│  │    ↓                                                    │    │
│  │  exchange(batch2) → compute(batch2) → Array             │    │
│  │    ↓                                                    │    │
│  │  ... (repeat for all batches)                           │    │
│  │    ↓                                                    │    │
│  │  Input stream ends (stream closed)                      │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

**Key differences from Table-In-Out:**
- No `finalize()` phase - processing ends when input ends
- No distributed state via `params.storage` - not designed for distributed aggregation
- Output must have exactly 1 column with same row count as input

## Table-In-Out Single-Worker Lifecycle (max_workers=1)

```
┌─────────────────────────────────────────────────────────────────┐
│  bind(request) → BindResponse                                   │
│    ↓                                                            │
│  init(phase=INPUT) → Stream with TableInOutExchangeState        │
│    ↓                                                            │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  exchange(batch1) → process/transform(batch1, out)      │    │
│  │    ↓                                                    │    │
│  │  [out.emit() produces output]                           │    │
│  │    ↓                                                    │    │
│  │  exchange(batch2) → process/transform(batch2, out)      │    │
│  │    ↓                                                    │    │
│  │  ... (repeat for all batches)                           │    │
│  │    ↓                                                    │    │
│  │  Input stream ends (stream closed)                      │    │
│  └─────────────────────────────────────────────────────────┘    │
│    ↓                                                            │
│  init(phase=FINALIZE) → Stream with TableInOutFinalizeState     │
│    ↓                                                            │
│  finalize(params, states, out) → out.emit() / out.finish()      │
└─────────────────────────────────────────────────────────────────┘
```

## Multi-Worker Lifecycle (max_workers > 1)

When `max_workers > 1`, the client spawns multiple worker processes.
One becomes the **primary worker** (runs finalize), others are **secondary workers**.

**Primary Worker:**
```
bind → init(INPUT) → exchange batches → init(FINALIZE) → finalize
```

**Secondary Workers:**
```
bind → init(INPUT, execution_id=...) → exchange batches → stop
                                                       ↓
                                               (NO finalize!)
```

**Key Differences:**

| Aspect | Primary Worker | Secondary Workers |
|--------|---------------|-------------------|
| `global_init()` called? | Yes | No (uses execution_id from primary) |
| `finalize()` called? | Yes | No |
| Receives all batches? | Subset (round-robin) | Subset (round-robin) |

## Lifecycle with Distributed State (Distributed Aggregation)

For distributed aggregations, state flows from secondary workers to primary
via `params.storage` (a `BoundStorage` backed by `FunctionStorageSqlite` by default):

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         SECONDARY WORKERS                                 │
│  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐  │
│  │ Worker 1           │  │ Worker 2           │  │ Worker N           │  │
│  │ exchange(batches)  │  │ exchange(batches)  │  │ exchange(batches)  │  │
│  │ storage.put() ─────┼──┼─────────┬──────────┼──┼→ params.storage    │  │
│  └────────────────────┘  └─────────│──────────┘  └────────────────────┘  │
│                                    ↓                                      │
│                         ┌──────────────────────┐                          │
│                         │   PRIMARY WORKER     │                          │
│                         │ exchange(batches)    │                          │
│                         │ storage.put() ───────┼→ params.storage          │
│                         │ storage.collect() ←──┼─ (collects ALL states)   │
│                         │ finalize()           │                          │
│                         └──────────────────────┘                          │
└──────────────────────────────────────────────────────────────────────────┘
```

**Timing Guarantees:**

1. `params.storage.put()` is called after all input batches are processed
2. Primary's `params.storage.collect()` receives states from ALL workers (including itself)

## When to Use Each Lifecycle Hook

| Hook | Use For | Example |
|------|---------|---------|
| `on_bind()` | Validate arguments, set output schema | Schema based on settings |
| `process()` / `transform()` | Transform/accumulate data per batch | Main processing logic |
| `params.storage.put()` | Persist partial results (distributed) | Serialize aggregation state |
| `params.storage.collect()` | Collect all worker states (primary only) | Combine partial aggregations |
| `finish()` / `finalize()` | Emit final results | Output aggregation results |
