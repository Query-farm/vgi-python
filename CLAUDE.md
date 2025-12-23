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
│  │ Client spawns worker subprocess, sends CallData, streams      │  │
│  │ input batches, receives output batches via Arrow IPC          │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │ stdin/stdout                         │
│                              ▼                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                      Worker Process                           │  │
│  │  ┌─────────────────────────────────────────────────────────┐  │  │
│  │  │ TableInOutFunction.process_batch(init_data, batch, ...)│  │  │
│  │  │ - Receives input RecordBatches                          │  │  │
│  │  │ - Returns output RecordBatches via ProcessResult        │  │  │
│  │  └─────────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Components

- **Worker** (`vgi/worker.py`): Subprocess that hosts functions, handles protocol
- **Client** (`vgi/client.py`): Spawns workers, streams data through functions
- **TableInOutFunction** (`vgi/table_in_out_function.py`): Base class for functions
- **CallData/BindResult** (`vgi/function.py`): Protocol messages for initialization
- **GlobalInitResult** (`vgi/function.py`): Shared state for parallel workers

## Protocol Flow

```
Client                                  Worker
  │                                       │
  │──── CallData (function, args) ───────▶│
  │                                       │ instantiate function
  │◀──── BindResult (output schema) ──────│
  │                                       │
  │──── GlobalStateInitInput ────────────▶│
  │◀──── GlobalInitResult ────────────────│ process_init()
  │                                       │
  │──── Input Batch 1 ───────────────────▶│
  │◀──── Output Batch 1 (NEED_MORE_INPUT)─│ process_batch()
  │                                       │
  │──── Input Batch 2 ───────────────────▶│
  │◀──── Output Batch 2 (NEED_MORE_INPUT)─│
  │                                       │
  │──── FINALIZE (empty batch) ──────────▶│
  │◀──── Final Output (FINISHED) ─────────│ process_batch(is_finalize=True)
  │                                       │
```

## Project Structure

```
vgi/
  __init__.py              # Package exports and module docstring
  function.py              # CallData, BindResult, Arguments, GlobalInitResult
  table_function.py        # CardinalityInfo, TableFunction base class
  table_in_out_function.py # TableInOutFunction, ProcessResult, FunctionInput/Output
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
from vgi.function import CallData, GlobalInitResult
from vgi.table_in_out_function import TableInOutFunction, ProcessResult
import pyarrow as pa

class MyFunction(TableInOutFunction):
    def __init__(self, call_data: CallData):
        super().__init__(call_data)
        # Access arguments via self.arguments.positional and self.arguments.named
        # Access input schema via self.input_schema

    def _output_schema(self) -> pa.Schema:
        # Called once to determine output schema
        # Default: returns self.input_schema (passthrough)
        return self.input_schema

    def process_batch(
        self,
        init_data: GlobalInitResult,
        batch: pa.RecordBatch,
        is_finalize: bool,
    ) -> ProcessResult:
        if is_finalize:
            # Called after all input; emit buffered/aggregated results
            return ProcessResult(None)
        # Process batch and return output
        return ProcessResult(batch)
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
class EchoFunction(TableInOutFunction):
    pass  # Default process_batch returns input unchanged
```

### 2. Aggregation (emit on finalize)
```python
class SumFunction(TableInOutFunction):
    def __init__(self, call_data):
        super().__init__(call_data)
        self.total = 0

    def _output_schema(self):
        return pa.schema([pa.field("sum", pa.int64())])

    def process_batch(self, init_data, batch, is_finalize):
        if is_finalize:
            return ProcessResult(
                pa.RecordBatch.from_pydict({"sum": [self.total]}, schema=self.output_schema)
            )
        self.total += sum(batch.column("value").to_pylist())
        return ProcessResult(None)
```

### 3. Multiple outputs per input (has_more=True)
```python
def process_batch(self, init_data, batch, is_finalize):
    self.repeat_count += 1
    has_more = self.repeat_count < 3
    if not has_more:
        self.repeat_count = 0
    return ProcessResult(batch, has_more=has_more)
```

## OutputStatus Values

- `NEED_MORE_INPUT`: Ready for next input batch
- `HAVE_MORE_OUTPUT`: Call again to get more output from current input
- `FINISHED`: Processing complete (only during finalize phase)
