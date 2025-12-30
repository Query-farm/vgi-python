# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

This project uses `uv` for Python package management.

```bash
# Install dependencies (required before running any commands)
uv sync --all-extras

# Run tests
uv run pytest

# Lint code
uv run ruff check .

# Format code
uv run ruff format .

# Type check
uv run mypy vgi/
```

## Project Overview

VGI (Vector Gateway Interface) provides an Apache Arrow-based protocol for connecting DuckDB to external programs. It enables user-defined functions to run in separate processes, communicating via stdin/stdout using Arrow IPC streaming.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           DuckDB / Client                           │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │ Client spawns worker subprocess, sends FunctionInvocation, streams      │  │
│  │ input batches, receives output batches via Arrow IPC          │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │ stdin/stdout                         │
│                              ▼                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                      Worker Process                           │  │
│  │  ┌─────────────────────────────────────────────────────────┐  │  │
│  │  │ TableInOutGeneratorFunction.process() / finalize()      │  │  │
│  │  │ - process(): Generator receiving RecordBatch via yield  │  │  │
│  │  │ - finalize(): Generator emitting final results          │  │  │
│  │  │ - Yields Output with output RecordBatches        │  │  │
│  │  └─────────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Components

- **Worker** (`vgi/worker.py`): Subprocess that hosts functions, handles protocol
- **Client** (`vgi/client.py`): Spawns workers, streams data through functions
- **TableInOutGeneratorFunction** (`vgi/table_in_out_function.py`): Base class for table-in-out functions
- **FunctionInvocation/FunctionOutputSpec** (`vgi/function.py`): Protocol messages for initialization
- **GlobalInitResult** (`vgi/function.py`): Shared state for parallel workers

## Protocol Flow

```
Client                                  Worker
  │                                       │
  │──── FunctionInvocation (function, args) ───────▶│
  │                                       │ instantiate function
  │◀──── FunctionOutputSpec (output schema) ──────│
  │                                       │
  │──── GlobalStateInitInput ────────────▶│
  │◀──── GlobalInitResult ────────────────│ perform_init()
  │                                       │
  │──── Input Batch 1 ───────────────────▶│
  │◀──── Output Batch 1 (NEED_MORE_INPUT)─│ process() yields
  │                                       │
  │──── Input Batch 2 ───────────────────▶│
  │◀──── Output Batch 2 (NEED_MORE_INPUT)─│
  │                                       │
  │──── FINALIZE (empty batch) ──────────▶│
  │◀──── Final Output (FINISHED) ─────────│ finalize() yields
  │                                       │
```

## Project Structure

```
vgi/
  __init__.py              # Package exports and module docstring
  function.py              # FunctionInvocation, FunctionOutputSpec, Arguments, GlobalInitResult
  table_function.py        # CardinalityInfo, TableFunction base class
  table_in_out_function.py # TableInOutGeneratorFunction, Output, OutputGenerator
  worker.py                # Worker base class
  client.py                # Client class and CLI
  util.py                  # Serialization utilities
  examples/
    table_in_out.py        # Example functions (Echo, BufferInput, SumAllColumns, etc.)
    worker.py              # ExampleWorker with registry
```

## CLI Commands

```bash
# Run example worker (has echo, buffer_input, repeat_inputs, sum_all_columns)
vgi-example-worker

# Send data through a function
vgi-client --input data.parquet --function echo --server vgi-example-worker
vgi-client --input data.parquet --function sum_all_columns --server vgi-example-worker
vgi-client --input data.parquet --function repeat_inputs --args '[3]' --server vgi-example-worker
```

## Creating a Custom Function (Simple API - Recommended)

Use `TableInOutFunction` for most use cases. Override `transform()` for per-batch processing and `finish()` for final output:

```python
import pyarrow as pa
import pyarrow.compute as pc
import structlog

from vgi import TableInOutFunction, Invocation


class MyFunction(TableInOutFunction):
    """Transform each batch by doubling numeric values."""

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        # Called for each input batch. Return transformed batch.
        doubled = pc.multiply(batch.column(0), 2)
        return batch.set_column(0, batch.schema[0].name, doubled)


class SumFunction(TableInOutFunction):
    """Aggregate: sum all values, emit single result."""

    def __init__(self, invocation: Invocation, logger: structlog.stdlib.BoundLogger):
        super().__init__(invocation, logger)
        self.total = 0

    @property
    def output_schema(self) -> pa.Schema:
        return pa.schema([("sum", pa.int64())])

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        self.total += pc.sum(batch.column(0)).as_py()
        return self.empty_output_batch  # No output during processing

    def finish(self) -> list[pa.RecordBatch]:
        return [pa.RecordBatch.from_pydict(
            {"sum": [self.total]},
            schema=self.output_schema
        )]

    def max_processes(self) -> int:
        return 1  # Aggregations must be single-process
```

### TableInOutFunction Methods

| Method | When to Override | Default |
|--------|------------------|---------|
| `transform(batch)` | Per-batch transformation | Returns batch unchanged |
| `finish()` | Final output after all input | Returns empty list |
| `output_schema` | Different output columns | Returns input_schema |
| `log(level, msg)` | N/A - call to emit logs | N/A |
| `save_state()` | Distributed processing | Returns None |
| `load_states(states)` | Distributed processing | No-op |

### Logging Example

```python
from vgi.log import Level

class LoggingFunction(TableInOutFunction):
    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        self.log(Level.INFO, f"Processing {batch.num_rows} rows")
        return batch
```

### Distributed Aggregation Example

```python
from vgi.ipc_utils import RecordBatchState

class DistributedSum(TableInOutFunction):
    def __init__(self, invocation, logger):
        super().__init__(invocation, logger)
        self.total = 0

    @property
    def output_schema(self) -> pa.Schema:
        return pa.schema([("sum", pa.int64())])

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        self.total += pc.sum(batch.column(0)).as_py()
        return self.empty_output_batch

    def save_state(self) -> RecordBatchState:
        return RecordBatchState(batch=pa.RecordBatch.from_pydict(
            {"partial": [self.total]}, schema=self.output_schema
        ))

    def load_states(self, states: list[RecordBatchState]) -> None:
        table = pa.Table.from_batches([s.batch for s in states])
        self.total = pc.sum(table.column(0)).as_py()

    def finish(self) -> list[pa.RecordBatch]:
        return [pa.RecordBatch.from_pydict(
            {"sum": [self.total]}, schema=self.output_schema
        )]
```

### When to Use Each Base Class

| Use Case | Base Class |
|----------|------------|
| Transform each batch independently | `TableInOutFunction` |
| Aggregate to single result | `TableInOutFunction` + `finish()` |
| Buffer all input, emit on finalize | `TableInOutFunction` + `finish()` |
| Multiple outputs per input | `TableInOutFunction` (return list) |
| Distributed aggregation | `TableInOutFunction` + `save_state()/load_states()` |
| Need GeneratorExit handling | `TableInOutGeneratorFunction` |
| Fine-grained streaming control | `TableInOutGeneratorFunction` |

## Creating a Custom Function (Generator API - Advanced)

For advanced streaming control, use `TableInOutGeneratorFunction` with generators:

```python
import pyarrow as pa
import structlog

from vgi.function import Invocation
from vgi.table_in_out_function import (
    OutputGenerator,
    Output,
    TableInOutGeneratorFunction,
)


class MyFunction(TableInOutGeneratorFunction):
    def __init__(self, invocation: Invocation, logger: structlog.stdlib.BoundLogger):
        super().__init__(invocation, logger)
        # Access arguments using self.arguments.get()
        # self.my_arg = self.arguments.get(0)              # positional
        # self.my_kwarg = self.arguments.get("name", default="value")  # named
        # Access input schema via self.input_schema

    @property
    def output_schema(self) -> pa.Schema:
        # Override to define output schema
        # Default: returns self.input_schema (passthrough)
        return self.input_schema

    def process(self, batch: pa.RecordBatch) -> OutputGenerator:
        # Priming yield (framework calls send(None) to advance past this)
        _ = yield None

        # Process batches - first batch comes from parameter, rest via yield
        while True:
            # Transform batch and yield output
            yield Output(batch)
            batch = yield None
            if batch is None:
                break

    # Optional: override finalize() only if you need to emit final results
    # def finalize(self) -> OutputGenerator | None:
    #     _ = yield None
    #     yield Output(final_batch)
```

## Creating a Custom Worker

```python
from vgi.worker import Worker

class MyWorker(Worker):
    registry = {
        "my_function": MyFunction,
        "another_function": AnotherFunction,
    }

if __name__ == "__main__":
    MyWorker().run()
```

## Key Patterns

### 1. Passthrough (Echo)
```python
class EchoFunction(TableInOutGeneratorFunction):
    pass  # Default process() passes input unchanged
```

### 2. Aggregation (emit on finalize)
```python
class SumFunction(TableInOutGeneratorFunction):
    @property
    def output_schema(self):
        return pa.schema([pa.field("sum", pa.int64())])

    def process(self, batch: pa.RecordBatch) -> OutputGenerator:
        self.total = 0
        _ = yield None

        while True:
            self.total += sum(batch.column("value").to_pylist())
            batch = yield None
            if batch is None:
                break

    def finalize(self) -> OutputGenerator:
        _ = yield None
        yield Output(
            pa.RecordBatch.from_pydict(
                {"sum": [self.total]}, schema=self.output_schema
            )
        )
```

### 3. Multiple outputs per input (has_more=True)
```python
def process(self, batch: pa.RecordBatch) -> OutputGenerator:
    _ = yield None

    while True:
        # Emit the same batch 3 times
        for _ in range(3):
            yield Output(batch, has_more=True)
        batch = yield None
        if batch is None:
            break
```

### 4. Logging (yield Message directly)
```python
from vgi.log import Level, Message

def process(self, batch: pa.RecordBatch) -> OutputGenerator:
    _ = yield None

    while True:
        # Emit log message - input will be re-sent after logging
        yield Message(Level.INFO, f"Processing {batch.num_rows} rows")
        # Process and emit result
        yield Output(transformed_batch)
        batch = yield None
        if batch is None:
            break
```

## Common Mistakes

### 1. Forgetting the priming yield

The generator MUST start with `_ = yield None`. This is required by the framework.

```python
# ❌ WRONG - will raise TypeError on first send()
def process(self, batch: pa.RecordBatch) -> OutputGenerator:
    while True:
        yield Output(batch)
        batch = yield None
        if batch is None:
            break

# ✅ CORRECT
def process(self, batch: pa.RecordBatch) -> OutputGenerator:
    _ = yield None  # Required priming yield
    while True:
        yield Output(batch)
        batch = yield None
        if batch is None:
            break
```

### 2. Not checking for None at end of loop

When input is exhausted, `yield None` returns `None`. You must check for this.

```python
# ❌ WRONG - infinite loop when input ends
def process(self, batch: pa.RecordBatch) -> OutputGenerator:
    _ = yield None
    while True:
        yield Output(batch)
        batch = yield None
        # Missing: if batch is None: break

# ✅ CORRECT
def process(self, batch: pa.RecordBatch) -> OutputGenerator:
    _ = yield None
    while True:
        yield Output(batch)
        batch = yield None
        if batch is None:
            break
```

### 3. Using walrus operator incorrectly

The compact form `while batch := (yield ...)` is error-prone. Prefer the explicit pattern.

```python
# ⚠️ COMPACT but confusing - avoid unless you understand it well
def process(self, batch: pa.RecordBatch) -> OutputGenerator:
    _ = yield None
    while batch := (yield Output(batch)):
        pass

# ✅ RECOMMENDED - explicit and clear
def process(self, batch: pa.RecordBatch) -> OutputGenerator:
    _ = yield None
    while True:
        yield Output(batch)
        batch = yield None
        if batch is None:
            break
```

### 4. Initializing state in process() instead of __init__

State that persists across batches should be initialized in `__init__`, not at the start of `process()`.

```python
# ⚠️ PROBLEMATIC - self.total reset on each process() call if generator restarts
def process(self, batch: pa.RecordBatch) -> OutputGenerator:
    self.total = 0  # This runs once per generator, which is usually fine
    _ = yield None
    # ...

# ✅ CLEARER - initialize in __init__
def __init__(self, invocation, logger):
    super().__init__(invocation, logger)
    self.total = 0

def process(self, batch: pa.RecordBatch) -> OutputGenerator:
    _ = yield None
    # ...
```

### 5. Forgetting to call super().__init__()

Always call the parent constructor when overriding `__init__`.

```python
# ❌ WRONG - missing super().__init__()
def __init__(self, invocation: Invocation, logger):
    self.my_value = invocation.arguments.positional[0]

# ✅ CORRECT
def __init__(self, invocation: Invocation, logger):
    super().__init__(invocation=invocation, logger=logger)
    self.my_value = invocation.arguments.positional[0]
```

### 6. Returning instead of yielding in finalize()

`finalize()` must be a generator (using yield) or return None.

```python
# ❌ WRONG - returning a batch instead of yielding
def finalize(self) -> OutputGenerator:
    return Output(final_batch)  # This doesn't work!

# ✅ CORRECT
def finalize(self) -> OutputGenerator:
    _ = yield None
    yield Output(final_batch)

# ✅ ALSO CORRECT - if no finalization needed
def finalize(self) -> OutputGenerator | None:
    return None
```

## LLM Quick Reference

### Import Cheatsheet

```python
# Simple API (recommended for most uses)
from vgi import TableInOutFunction, Invocation, Arg, Worker

# Schema helpers (for output_schema definitions)
from vgi import schema, schema_like

# Generator API (advanced)
from vgi import TableInOutGeneratorFunction, Output, OutputGenerator, Invocation, Arg, Worker

# Logging
from vgi.log import Level, Message

# Cardinality hints (optional)
from vgi.table_function import CardinalityInfo

# Distributed state (for parallel functions)
from vgi.ipc_utils import RecordBatchState

# Client for invoking functions
from vgi.client import Client
```

### Schema Helpers

Use `schema()` and `schema_like()` to define output schemas with minimal boilerplate.

**schema() - Build from scratch:**

```python
from vgi import schema, TableInOutFunction
import pyarrow as pa

class MyFunction(TableInOutFunction):
    @property
    def output_schema(self) -> pa.Schema:
        # Concise: keyword arguments map names to types
        return schema(sum=pa.int64(), count=pa.int64(), avg=pa.float64())

        # Equivalent verbose form:
        # return pa.schema([
        #     pa.field("sum", pa.int64()),
        #     pa.field("count", pa.int64()),
        #     pa.field("avg", pa.float64()),
        # ])
```

**schema_like() - Derive from input:**

```python
from vgi import schema_like, TableInOutFunction
import pyarrow as pa

class MyFunction(TableInOutFunction):
    @property
    def output_schema(self) -> pa.Schema:
        # Add a column to input schema
        return schema_like(self.input_schema, add={"total": pa.int64()})

        # Remove columns
        return schema_like(self.input_schema, remove=["temp", "debug"])

        # Rename columns
        return schema_like(self.input_schema, rename={"old_name": "new_name"})

        # Change column type (keeps position)
        return schema_like(self.input_schema, replace={"count": pa.float64()})

        # Combine operations (order: remove → rename → replace → add)
        return schema_like(
            self.input_schema,
            remove=["temp"],
            rename={"val": "value"},
            replace={"count": pa.float64()},
            add={"computed": pa.int64()},
        )
```

**Common Schema Patterns:**

```python
# Aggregation output (different from input)
output_schema = schema(sum=pa.int64(), count=pa.int64())

# Passthrough with extra column
output_schema = schema_like(self.input_schema, add={"processed": pa.bool_()})

# From a dict (programmatic)
fields = {"a": pa.int64(), "b": pa.string()}
output_schema = schema(fields)

# Type promotion for aggregation
output_schema = schema_like(
    self.input_schema,
    replace={"value": pa.float64()},  # int32 → float64 for avg
)
```

### Type Summary

| Type | Description | Module |
|------|-------------|--------|
| `TableInOutFunction` | Callback-based API (recommended) | `vgi.table_in_out_function` |
| `TableInOutGeneratorFunction` | Generator-based API (advanced) | `vgi.table_in_out_function` |
| `Output` | Yielded from process()/finalize() | `vgi.table_in_out_function` |
| `OutputGenerator` | Return type for process()/finalize() | `vgi.table_in_out_function` |
| `Invocation` | Function invocation request | `vgi.function` |
| `Arguments` | Positional and named arguments | `vgi.function` |
| `Arg` | Descriptor for declarative argument parsing | `vgi.function` |
| `Worker` | Base class for worker processes | `vgi.worker` |
| `Client` | Invokes functions on workers | `vgi.client` |
| `Level` | Log severity enum | `vgi.log` |
| `Message` | Log message object | `vgi.log` |
| `CardinalityInfo` | Row count estimates | `vgi.table_function` |
| `RecordBatchState` | State wrapper for distributed functions | `vgi.ipc_utils` |
| `schema` | Build schemas from keyword arguments | `vgi.schema_utils` |
| `schema_like` | Derive schemas with modifications | `vgi.schema_utils` |

### Accessing Arguments

**Option 1: Declarative with `Arg` descriptor (recommended)**

Declare arguments as class attributes - no `__init__` override needed:

```python
from vgi import TableInOutFunction, Arg

class MyFunction(TableInOutFunction):
    # Required positional argument (index 0)
    count = Arg[int](0)

    # Optional positional with default
    multiplier = Arg[int](1, default=1)

    # Required named argument
    column = Arg[str]("column")

    # Optional named with default
    format = Arg[str]("format", default="json")

    def transform(self, batch):
        # self.count, self.multiplier, etc. are available
        # IDE knows: self.count is int, self.format is str
        return batch
```

**Option 2: Manual with `self.arguments.get()`**

Parse arguments in `__init__`:

```python
# Positional arguments (by index)
count = self.arguments.get(0)                      # Required, raises if missing
name = self.arguments.get(1, default="unnamed")    # Optional with default

# Named arguments (by string)
separator = self.arguments.get("sep", default=",")
threshold = self.arguments.get("threshold")        # Required

# With Arrow type validation (optional)
count = self.arguments.get(0, type=pa.int64())     # Raises TypeError if wrong type
```

### Function Skeleton Template (Simple API - Recommended)

```python
import pyarrow as pa
from vgi import TableInOutFunction, Arg

class MyFunction(TableInOutFunction):
    """One-line description.

    Detailed description of what this function does.
    """

    # Declare arguments as class attributes (no __init__ needed)
    # count = Arg[int](0)                        # Required positional
    # separator = Arg[str]("sep", default=",")   # Optional named

    @property
    def output_schema(self) -> pa.Schema:
        """Return output schema (default: passthrough)."""
        return self.input_schema  # Or build custom schema

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Transform each input batch."""
        # Your transformation here
        return batch

    def finish(self) -> list[pa.RecordBatch]:
        """Emit final results (optional)."""
        return []  # Or return list of batches
```

### Function Skeleton Template (Generator API - Advanced)

```python
import pyarrow as pa
from vgi import TableInOutGeneratorFunction, Output, OutputGenerator, Arg

class MyFunction(TableInOutGeneratorFunction):
    """One-line description.

    Detailed description of what this function does.
    """

    # Declare arguments as class attributes (no __init__ needed)
    # count = Arg[int](0)                        # Required positional
    # separator = Arg[str]("sep", default=",")   # Optional named

    @property
    def output_schema(self) -> pa.Schema:
        """Return output schema (default: passthrough)."""
        return self.input_schema  # Or build custom schema

    def process(self, batch: pa.RecordBatch) -> OutputGenerator:
        """Process input batches."""
        _ = yield None  # REQUIRED: priming yield

        while True:
            # Transform batch here
            result = batch  # Your transformation
            yield Output(result)

            batch = yield None
            if batch is None:
                break

    def finalize(self) -> OutputGenerator | None:
        """Emit final results (optional)."""
        return None  # Or implement if needed
```

### Pattern Decision Tree

```
Need to implement a VGI function?
│
├─ No transformation needed?
│  └─ class Echo(TableInOutFunction): pass
│
├─ Transform each batch independently?
│  └─ Override transform() → returns pa.RecordBatch
│
├─ Produce multiple outputs per input?
│  └─ Override transform() → returns list[pa.RecordBatch]
│
├─ Aggregate across all batches?
│  └─ Accumulate in transform(), emit in finish()
│      └─ Set max_processes() -> 1
│
├─ Buffer all input, emit on finalize?
│  └─ Buffer in transform(), return in finish()
│      └─ Set max_processes() -> 1
│
├─ Need GeneratorExit handling or distributed state?
│  └─ Use TableInOutGeneratorFunction (generator API)
│
└─ Need fine-grained streaming control?
   └─ Use TableInOutGeneratorFunction (generator API)
```

### Status Values (in IPC metadata)

| Status | Meaning |
|--------|---------|
| `NEED_MORE_INPUT` | Ready for next input batch |
| `HAVE_MORE_OUTPUT` | Call send() again for more output |
| `FINISHED` | Processing complete |

### Method Override Summary

| Method | When to Override | Default Behavior |
|--------|------------------|------------------|
| `__init__` | Parse arguments, init state | Sets input_schema, arguments |
| `output_schema` | Change output columns | Returns input_schema |
| `process()` | Transform data | Passthrough |
| `finalize()` | Emit final/aggregated data | Returns None |
| `max_processes()` | Limit parallelism | Returns 99999 |
| `cardinality()` | Provide row estimates | Returns None |
| `setup()` | Acquire resources | No-op |
| `teardown()` | Release resources | No-op |

### Function Lifecycle

Understanding when lifecycle methods are called is critical for resource management
and distributed processing.

#### Single-Process Lifecycle (max_processes=1)

```
┌─────────────────────────────────────────────────────────────────┐
│  __init__(invocation, logger)                                   │
│    ↓                                                            │
│  output_schema (property accessed)                              │
│    ↓                                                            │
│  perform_init(init_batch) → GlobalInitResult                    │
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

#### Multi-Process Lifecycle (max_processes > 1)

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

#### Lifecycle with save_state/load_states (Distributed Aggregation)

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

#### Resource Management Best Practices

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

#### When to Use Each Lifecycle Hook

| Hook | Use For | Example |
|------|---------|---------|
| `__init__` | Parse arguments, initialize simple state | `self.total = 0` |
| `setup()` | Acquire external resources | DB connections, file handles |
| `process()` | Transform/accumulate data | Main processing logic |
| `save_state()` | Persist partial results (distributed) | Serialize aggregation state |
| `load_states()` | Merge worker states (primary only) | Combine partial aggregations |
| `finalize()` | Emit final results | Output aggregation results |
| `teardown()` | Release external resources | Close connections, delete temp files |

