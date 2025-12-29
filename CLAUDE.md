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
│  │ Client spawns worker subprocess, sends FunctionRequest, streams      │  │
│  │ input batches, receives output batches via Arrow IPC          │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │ stdin/stdout                         │
│                              ▼                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                      Worker Process                           │  │
│  │  ┌─────────────────────────────────────────────────────────┐  │  │
│  │  │ Function.process() / finalize()               │  │  │
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
- **Function** (`vgi/table_in_out_function.py`): Base class for functions
- **FunctionRequest/FunctionOutputSpec** (`vgi/function.py`): Protocol messages for initialization
- **GlobalInitResult** (`vgi/function.py`): Shared state for parallel workers

## Protocol Flow

```
Client                                  Worker
  │                                       │
  │──── FunctionRequest (function, args) ───────▶│
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
  function.py              # FunctionRequest, FunctionOutputSpec, Arguments, GlobalInitResult
  table_function.py        # CardinalityInfo, Function base class
  table_in_out_function.py # Function, Output, OutputGenerator
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

## Creating a Custom Function

```python
import pyarrow as pa
import structlog

from vgi.function import FunctionRequest
from vgi.table_in_out_function import (
    OutputGenerator,
    Output,
    Function,
)


class MyFunction(Function):
    def __init__(self, invocation: FunctionRequest, logger: structlog.stdlib.BoundLogger):
        super().__init__(invocation, logger)
        # Access arguments via self.arguments.positional and self.arguments.named
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
class EchoFunction(Function):
    pass  # Default process() passes input unchanged
```

### 2. Aggregation (emit on finalize)
```python
class SumFunction(Function):
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

### 3. Multiple outputs per input (continue_from_current_input=True)
```python
def process(self, batch: pa.RecordBatch) -> OutputGenerator:
    _ = yield None

    while True:
        # Emit the same batch 3 times
        for _ in range(3):
            yield Output(batch, continue_from_current_input=True)
        batch = yield None
        if batch is None:
            break
```

### 4. Logging (yield LogMessage directly)
```python
from vgi.function import LogLevel, LogMessage

def process(self, batch: pa.RecordBatch) -> OutputGenerator:
    _ = yield None

    while True:
        # Emit log message - input will be re-sent after logging
        yield LogMessage(LogLevel.INFO, f"Processing {batch.num_rows} rows")
        # Process and emit result
        yield Output(transformed_batch)
        batch = yield None
        if batch is None:
            break
```

Alternatively, attach log messages to Output:
```python
def process(self, batch: pa.RecordBatch) -> OutputGenerator:
    _ = yield None

    while True:
        yield Output(
            transformed_batch,
            log_message=LogMessage(LogLevel.INFO, f"Processed {batch.num_rows} rows")
        )
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
def __init__(self, invocation: FunctionRequest, logger):
    self.my_value = invocation.arguments.positional[0]

# ✅ CORRECT
def __init__(self, invocation: FunctionRequest, logger):
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

