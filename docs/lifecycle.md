# Function Lifecycle

Understanding when lifecycle methods are called is critical for resource management and distributed processing.

## Scalar Function Lifecycle

Scalar functions have a simplified lifecycle with no finalize phase. Processing ends when input is exhausted.

```
┌─────────────────────────────────────────────────────────────────┐
│  __init__(invocation, logger)                                   │
│    ↓                                                            │
│  output_schema (property accessed, must be single column)       │
│    ↓                                                            │
│  setup()  ← Acquire resources here                              │
│    ↓                                                            │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  process(batch1) → compute(batch1) → Array              │    │
│  │    ↓                                                    │    │
│  │  [return output with same row count]                    │    │
│  │    ↓                                                    │    │
│  │  process(batch2) → compute(batch2) → Array              │    │
│  │    ↓                                                    │    │
│  │  ... (repeat for all batches)                           │    │
│  │    ↓                                                    │    │
│  │  Input stream ends (generator closed)                   │    │
│  └─────────────────────────────────────────────────────────┘    │
│    ↓                                                            │
│  teardown()  ← Release resources (always called)                │
└─────────────────────────────────────────────────────────────────┘
```

**Key differences from Table-In-Out:**
- No `finalize()` phase - processing ends when input ends
- No `save_state()` / `load_states()` - not designed for distributed aggregation
- Output must have exactly 1 column with same row count as input

## Table-In-Out Single-Process Lifecycle (max_processes=1)

```
┌─────────────────────────────────────────────────────────────────┐
│  __init__(invocation, logger)                                   │
│    ↓                                                            │
│  output_schema (property accessed)                              │
│    ↓                                                            │
│  perform_init(init_batch) → InitResult                    │
│    ↓                                                            │
│  setup()  ← Acquire resources here (DB connections, files)      │
│    ↓                                                            │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  process(batch1) → OutputGenerator                      │    │
│  │    ↓                                                    │    │
│  │  [yield outputs for batch1]                             │    │
│  │    ↓                                                    │    │
│  │  process receives batch2 via yield                      │    │
│  │    ↓                                                    │    │
│  │  [yield outputs for batch2]                             │    │
│  │    ↓                                                    │    │
│  │  ... (repeat for all batches)                           │    │
│  │    ↓                                                    │    │
│  │  process receives None (end of input)                   │    │
│  └─────────────────────────────────────────────────────────┘    │
│    ↓                                                            │
│  finalize() → OutputGenerator                                   │
│    ↓                                                            │
│  [yield final outputs]                                          │
│    ↓                                                            │
│  teardown()  ← Release resources here (always called)           │
└─────────────────────────────────────────────────────────────────┘
```

## Multi-Process Lifecycle (max_processes > 1)

When `max_processes() > 1`, the client spawns multiple worker processes.
One becomes the **primary worker** (runs finalize), others are **secondary workers**.

**Primary Worker:**
```
__init__ → output_schema → perform_init → setup → process → finalize → teardown
```

**Secondary Workers:**
```
__init__ → output_schema → retrieve_init → setup → process → teardown
                                                      ↓
                                              (NO finalize!)
```

**Key Differences:**

| Aspect | Primary Worker | Secondary Workers |
|--------|---------------|-------------------|
| `perform_init()` called? | Yes | No |
| `retrieve_init()` called? | No | Yes |
| `finalize()` called? | Yes | No |
| `teardown()` called? | Yes (after finalize) | Yes (after process ends) |
| Receives all batches? | Subset (round-robin) | Subset (round-robin) |

## Lifecycle with save_state/load_states (Distributed Aggregation)

For distributed aggregations, state flows from secondary workers to primary:

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         SECONDARY WORKERS                                 │
│  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐  │
│  │ Worker 1           │  │ Worker 2           │  │ Worker N           │  │
│  │ setup()            │  │ setup()            │  │ setup()            │  │
│  │ process(batches)   │  │ process(batches)   │  │ process(batches)   │  │
│  │ save_state() ──────┼──┼─────────┬──────────┼──┼→ SQLite Storage    │  │
│  │ teardown()         │  │ teardown()         │  │ teardown()         │  │
│  └────────────────────┘  └─────────│──────────┘  └────────────────────┘  │
│                                    ↓                                      │
│                         ┌──────────────────────┐                          │
│                         │   PRIMARY WORKER     │                          │
│                         │ setup()              │                          │
│                         │ process(batches)     │                          │
│                         │ save_state() ────────┼→ SQLite Storage          │
│                         │ load_states() ←──────┼─ (collects ALL states)   │
│                         │ finalize()           │                          │
│                         │ teardown()           │                          │
│                         └──────────────────────┘                          │
└──────────────────────────────────────────────────────────────────────────┘
```

**Timing Guarantees:**

1. `save_state()` is called automatically when the process generator closes
2. Secondary workers' `teardown()` completes BEFORE primary's `load_states()`
3. Primary's `load_states()` receives states from ALL workers (including itself)
4. `teardown()` is ALWAYS called, even if an exception occurs

## Resource Management Best Practices

```python
class MyFunction(TableInOutFunction):
    def setup(self) -> None:
        """Acquire resources. Called once per worker."""
        self.db_conn = sqlite3.connect("my.db")
        self.temp_file = tempfile.NamedTemporaryFile()

    def teardown(self) -> None:
        """Release resources. ALWAYS called, even on error."""
        if hasattr(self, 'db_conn'):
            self.db_conn.close()
        if hasattr(self, 'temp_file'):
            self.temp_file.close()

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        # Safe to use self.db_conn here
        return batch
```

**Anti-Pattern: Don't acquire resources in __init__:**
```python
# ❌ WRONG - resources acquired before setup()
def __init__(self, invocation, logger):
    super().__init__(invocation, logger)
    self.db_conn = sqlite3.connect("my.db")  # Too early!

# ✅ CORRECT - acquire in setup()
def setup(self) -> None:
    self.db_conn = sqlite3.connect("my.db")
```

## When to Use Each Lifecycle Hook

| Hook | Use For | Example |
|------|---------|---------|
| `__init__` | Parse arguments, initialize simple state | `self.total = 0` |
| `setup()` | Acquire external resources | DB connections, file handles |
| `process()` | Transform/accumulate data | Main processing logic |
| `save_state()` | Persist partial results (distributed) | Serialize aggregation state |
| `load_states()` | Merge worker states (primary only) | Combine partial aggregations |
| `finalize()` | Emit final results | Output aggregation results |
| `teardown()` | Release external resources | Close connections, delete temp files |
