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
│  │  │ TableInOutFunction.process_batches()                    │  │  │
│  │  │ - Generator receiving ProcessInput via yield            │  │  │
│  │  │ - Yields ProcessResult with output RecordBatches        │  │  │
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
  │◀──── Output Batch 1 (NEED_MORE_INPUT)─│ process_batches() yields
  │                                       │
  │──── Input Batch 2 ───────────────────▶│
  │◀──── Output Batch 2 (NEED_MORE_INPUT)─│
  │                                       │
  │──── FINALIZE (empty batch) ──────────▶│
  │◀──── Final Output (FINISHED) ─────────│ input.is_finalize=True
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
from collections.abc import Generator

import pyarrow as pa
import structlog

from vgi.function import CallData
from vgi.table_in_out_function import (
    ProcessInput,
    ProcessResult,
    TableInOutFunction,
)


class MyFunction(TableInOutFunction):
    def __init__(self, call_data: CallData, logger: structlog.stdlib.BoundLogger):
        super().__init__(call_data, logger)
        # Access arguments via self.arguments.positional and self.arguments.named
        # Access input schema via self.input_schema

    @property
    def output_schema(self) -> pa.Schema:
        # Override to define output schema
        # Default: returns self.input_schema (passthrough)
        return self.input_schema

    def process_batches(
        self,
    ) -> Generator[ProcessResult, ProcessInput | None, None]:
        # Initial priming yield
        _ = yield ProcessResult(None)

        while True:
            input = yield ProcessResult(None)
            if input is None:
                raise ValueError("Expected ProcessInput, got None")
            if input.is_finalize:
                # Called after all input; emit buffered/aggregated results
                break
            # Process input.batch and yield output
            yield ProcessResult(input.batch)
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
    pass  # Default process_batches passes input unchanged
```

### 2. Aggregation (emit on finalize)
```python
class SumFunction(TableInOutFunction):
    @property
    def output_schema(self):
        return pa.schema([pa.field("sum", pa.int64())])

    def process_batches(self) -> Generator[ProcessResult, ProcessInput | None, None]:
        total = 0
        _ = yield ProcessResult(None)

        while True:
            input = yield ProcessResult(None)
            if input is None:
                raise ValueError("Expected ProcessInput")
            if input.is_finalize:
                yield ProcessResult(
                    pa.RecordBatch.from_pydict(
                        {"sum": [total]}, schema=self.output_schema
                    )
                )
                break
            total += sum(input.batch.column("value").to_pylist())
```

### 3. Multiple outputs per input (has_more=True)
```python
def process_batches(self) -> Generator[ProcessResult, ProcessInput | None, None]:
    _ = yield ProcessResult(None)
    while True:
        input = yield ProcessResult(None)
        if input is None:
            raise ValueError("Expected ProcessInput")
        if input.is_finalize:
            break
        # Emit the same batch 3 times
        for i in range(3):
            has_more = i < 2
            yield ProcessResult(input.batch, has_more=has_more)
```

## OutputStatus Values

- `NEED_MORE_INPUT`: Ready for next input batch
- `HAVE_MORE_OUTPUT`: Call again to get more output from current input
- `FINISHED`: Processing complete (only during finalize phase)
