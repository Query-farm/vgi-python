# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

This project uses `uv` for Python package management.

```bash
# Install dependencies
uv sync

# Install with dev dependencies
uv sync --dev

# Run tests
uv run pytest

# Lint code
uv run ruff check .

# Format code
uv run ruff format .

# Type check
uv run mypy vgi/
```

Before you can use ruff or mypy you need to run `uv sync --all-extras`

## Project Overview

VGI (Vector Gateway Interface) provides an Apache Arrow-based protocol for connecting DuckDB to external programs.

### Architecture

- **Worker**: A subprocess that hosts user-defined functions, communicating via stdin/stdout using Arrow IPC
- **Client**: Spawns a worker subprocess and sends data through functions
- **TableInOutFunction**: Base class for functions that accept and return tables

### Project Structure

```
vgi/
  worker.py                  # Worker base class
  client.py                  # CLI client
  table_in_out_function.py   # TableInOutFunction base class
  examples/
    table_in_out.py          # Example functions (Echo, BufferInput, etc.)
    worker.py                # ExampleWorker
```

### CLI Commands

```bash
# Run example worker (has echo, buffer_input, repeat_inputs, sum_all_columns)
vgi-example-worker

# Send data through a function
vgi-client --input data.parquet --function echo --server ./my_worker.py
```

### Creating a Custom Worker

```python
from vgi.worker import Worker
from vgi.table_in_out_function import TableInOutFunction, table_in_out_function, ProcessResult

@table_in_out_function
class MyFunction(TableInOutFunction):
    def process_batch(self, batch, is_finalize):
        if is_finalize:
            return ProcessResult(None)
        return ProcessResult(batch)

class MyWorker(Worker):
    registry = {
        "my_function": MyFunction,
    }

if __name__ == "__main__":
    MyWorker().run()
```
