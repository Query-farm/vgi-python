# Function API Reference (Advanced)

This document covers the callback-based APIs for scalar, table, and table-in-out functions.

## Scalar Function (Row Transform)

Use `ScalarFunction` with a `compute()` callback for scalar transforms.
For full control over per-batch processing, use `ScalarFunctionGenerator` with its `process()` classmethod.

### Basic Template

```python
from typing import Annotated
import pyarrow as pa
import pyarrow.compute as pc
from vgi import ScalarFunction, Param, Returns

class MyScalarFunction(ScalarFunction):
    """Transform each row to a single output value."""

    @staticmethod
    def compute(
        col: Annotated[pa.Int64Array, Param(doc="Input column")],
    ) -> Annotated[pa.Int64Array, Returns(doc="Doubled value")]:
        return pc.multiply(col, 2)
```

### ScalarFunction Methods

| Method | When to Override | Default |
|--------|------------------|---------|
| `compute()` | Always - transform input | Required |
| `on_bind()` | Customize output schema | Inferred from `compute()` return type |

### Key Constraints

1. **Single-column output**: Output schema has exactly one column named "result"
2. **1:1 row mapping**: Output `num_rows` must equal input `num_rows`
3. **No finalize phase**: Processing ends when input stream ends

### ScalarFunctionGenerator (Per-Batch Control)

For cases where you need per-batch control (e.g., custom init/response handling):

```python
import pyarrow as pa
from vgi import ScalarFunctionGenerator

class MyScalarGen(ScalarFunctionGenerator):
    """Per-batch scalar processing with full control."""

    @classmethod
    def process(cls, *, batch, init_call, init_response, storage) -> pa.RecordBatch:
        """Process one input batch. Return output batch with same row count."""
        result = pa.RecordBatch.from_arrays(
            [batch.column(0)],
            schema=pa.schema([("result", batch.schema[0].type)])
        )
        return result
```

---

## Table Function (No Input)

Use `TableFunctionGenerator` when you need to generate data without receiving input.

### Basic Template

```python
from typing import Annotated
import pyarrow as pa
from vgi import TableFunctionGenerator, Arg
from vgi.table_function import TableCardinality

from dataclasses import dataclass
from typing import ClassVar

@dataclass(slots=True, frozen=True)
class MyTableFunctionArgs:
    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]

@dataclass
class MyTableState:
    remaining: int
    offset: int = 0

class MyTableFunction(TableFunctionGenerator[MyTableFunctionArgs, MyTableState]):
    """Generate data without input."""

    FunctionArguments = MyTableFunctionArgs

    class Meta:
        name = "my_table_function"

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([("value", pa.int64())])
    BATCH_SIZE: ClassVar[int] = 1000

    @classmethod
    def cardinality(cls, params) -> TableCardinality:
        """Provide row count estimate."""
        return TableCardinality(estimate=params.args.count, max=params.args.count)

    @classmethod
    def initial_state(cls, params) -> MyTableState:
        return MyTableState(remaining=params.args.count)

    @classmethod
    def process(cls, params, state, out) -> None:
        """Generate output batches. Call out.emit() then out.finish()."""
        if state.remaining <= 0:
            out.finish()
            return
        size = min(state.remaining, cls.BATCH_SIZE)
        values = list(range(state.offset, state.offset + size))
        out.emit(pa.RecordBatch.from_pydict(
            {"value": values}, schema=params.output_schema
        ))
        state.offset += size
        state.remaining -= size
```

### TableFunctionGenerator Methods

| Method | When to Override | Default |
|--------|------------------|---------|
| `process(params, state, out)` | Always - generate output | Required |
| `initial_state(params)` | Initialize per-worker state | Returns None |
| `FIXED_SCHEMA` or `on_bind(params)` | Define output columns | Required |
| `cardinality(params)` | Provide row count estimates | Returns None |

### Table Function Patterns

**Simple generation with state:**
```python
from dataclasses import dataclass

@dataclass
class CounterState:
    index: int = 0
    remaining: int = 0

class CounterFunction(TableFunctionGenerator):
    @classmethod
    def initial_state(cls, params):
        return CounterState(remaining=params.args.count)

    @classmethod
    def process(cls, params, state, out):
        if state.remaining <= 0:
            out.finish()
            return
        out.emit(pa.RecordBatch.from_pydict(
            {"n": [state.index]}, schema=params.output_schema
        ))
        state.index += 1
        state.remaining -= 1
```

**In-band logging:**
```python
from vgi_rpc.log import Level

@classmethod
def process(cls, params, state, out):
    if state.index == 0:
        out.client_log(Level.INFO, "Starting generation")
    if state.remaining <= 0:
        out.client_log(Level.INFO, "Generation complete")
        out.finish()
        return
    out.emit(batch)
    state.index += 1
```

---

## Table-In-Out Function (Input Transform)

For transforming input data, use `TableInOutFunction` (recommended) or `TableInOutGenerator` (advanced).

### TableInOutFunction (Recommended)

The simplest API with `transform()` and `finish()` callbacks and explicit `TState`:

```python
import pyarrow as pa
from vgi import TableInOutFunction
from vgi.table_function import BindParams, ProcessParams

class MyFunction(TableInOutFunction[MyArgs, MyState]):
    """Transform input batches."""

    @classmethod
    def on_bind(cls, params: BindParams[MyArgs]) -> BindResponse:
        assert params.bind_call.input_schema is not None
        return BindResponse(output_schema=params.bind_call.input_schema)

    @classmethod
    def initial_state(cls, params: ProcessParams[MyArgs]) -> MyState | None:
        return MyState(...)

    @classmethod
    def transform(
        cls,
        batch: pa.RecordBatch,
        params: ProcessParams[MyArgs],
        state: MyState | None,
    ) -> pa.RecordBatch:
        """Transform one input batch. Return output batch."""
        return batch

    @classmethod
    def finish(
        cls,
        params: ProcessParams[MyArgs],
        states: list[MyState],
    ) -> list[pa.RecordBatch]:
        """Emit final output after all input is processed."""
        return []
```

### TableInOutGenerator (Advanced)

For full control over per-batch processing with `OutputCollector`:

```python
import pyarrow as pa
from vgi import TableInOutGenerator
from vgi.table_function import BindParams, ProcessParams
from vgi_rpc.rpc import OutputCollector

class MyFunction(TableInOutGenerator[MyArgs, MyState]):
    """Advanced table-in-out with OutputCollector."""

    @classmethod
    def on_bind(cls, params: BindParams[MyArgs]) -> BindResponse:
        assert params.bind_call.input_schema is not None
        return BindResponse(output_schema=params.bind_call.input_schema)

    @classmethod
    def process(cls, params: ProcessParams[MyArgs], state: MyState | None, batch: pa.RecordBatch, out: OutputCollector) -> None:
        """Process one input batch. Call out.emit() for output."""
        out.emit(batch)

    @classmethod
    def finalize(cls, params: ProcessParams[MyArgs]) -> list[pa.RecordBatch]:
        """Produce final output. Return list of batches."""
        return []
```

### Key Patterns

**Passthrough (Echo):**
```python
class EchoFunction(TableInOutGenerator):
    pass  # Default process() passes input unchanged
```

**Aggregation (emit on finalize):**
```python test="skip"
class SumFunction(TableInOutFunction):
    @property
    def output_schema(self):
        return pa.schema([pa.field("sum", pa.int64())])

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        self.total = getattr(self, 'total', 0)
        self.total += sum(batch.column("value").to_pylist())
        return self.empty_output_batch  # No output during processing

    def finish(self) -> list[pa.RecordBatch]:
        return [pa.RecordBatch.from_pydict(
            {"sum": [self.total]}, schema=self.output_schema
        )]
```

**Multiple outputs per input (HAVE_MORE_OUTPUT status):**
```python
from vgi_rpc.rpc import OutputCollector

def process(cls, params, state, batch, out: OutputCollector) -> None:
    # Emit multiple batches for one input by setting vgi.status metadata
    out.emit(batch, custom_metadata={b"vgi.status": b"HAVE_MORE_OUTPUT"})
```

**In-band logging via OutputCollector:**
```python
from vgi_rpc.rpc import OutputCollector
from vgi_rpc.log import Level

def process(cls, params, state, batch, out: OutputCollector) -> None:
    out.client_log(Level.INFO, f"Processing {batch.num_rows} rows")
    out.emit(batch)
```

---

## Common Mistakes

### 1. Forgetting out.finish() in table functions

```python test="skip"
# WRONG - function never signals completion, client hangs
@classmethod
def process(cls, params, state, out):
    if state.remaining <= 0:
        return  # Missing out.finish()!
    out.emit(batch)

# CORRECT
@classmethod
def process(cls, params, state, out):
    if state.remaining <= 0:
        out.finish()
        return
    out.emit(batch)
```

### 2. Forgetting to call super().__init__()

```python test="skip"
# WRONG
def __init__(self, invocation, logger):
    self.my_value = invocation.arguments.get(0)

# CORRECT
def __init__(self, invocation, logger):
    super().__init__(invocation=invocation, logger=logger)
    self.my_value = self.invocation.arguments.get(0)
```

### 3. Not updating state in table function process()

```python test="skip"
# WRONG - infinite loop, state never advances
@classmethod
def process(cls, params, state, out):
    out.emit(batch)
    # Missing: state.remaining -= 1

# CORRECT
@classmethod
def process(cls, params, state, out):
    if state.remaining <= 0:
        out.finish()
        return
    out.emit(batch)
    state.remaining -= 1
```
