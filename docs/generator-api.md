# Generator API (Advanced)

This document covers the generator-based APIs for scalar, table, and table-in-out functions.

## Scalar Function Generator (Row Transform)

Use `ScalarFunctionGenerator` when you need full generator control for scalar transforms,
including yielding log messages during processing. For most use cases, prefer the simpler
`ScalarFunction` with its `compute()` callback.

### Basic Template

```python
import pyarrow as pa
import pyarrow.compute as pc
from vgi import ScalarFunctionGenerator, Output, Arg
from vgi.log import Level, Message

class MyScalarFunction(ScalarFunctionGenerator):
    """Transform each row to a single output value."""

    column = Arg[str](0, doc="Column to transform")

    @property
    def output_schema(self) -> pa.Schema:
        # Must have exactly one column
        return pa.schema([("result", pa.int64())])

    def process(self, batch: pa.RecordBatch) -> ScalarOutputGenerator:
        # Priming yield - REQUIRED
        _ = yield Output(self.empty_output_batch)

        while True:
            # Optional: yield log messages
            yield Message(Level.INFO, f"Processing {batch.num_rows} rows")

            # Compute result - must have same row count as input
            result = pc.multiply(batch.column(self.column), 2)
            output = pa.RecordBatch.from_arrays([result], schema=self.output_schema)

            batch = yield Output(output)
            if batch is None:
                break
```

### ScalarFunctionGenerator Methods

| Method | When to Override | Default |
|--------|------------------|---------|
| `process(batch)` | Always - transform input | Required |
| `output_schema` | Define single-column output | Required |
| `setup()` | Acquire resources | No-op |
| `teardown()` | Release resources | No-op |

### Key Constraints

1. **Single-column output**: `output_schema` must have exactly one column
2. **1:1 row mapping**: Output `num_rows` must equal input `num_rows`
3. **No finalize phase**: Processing ends when input stream ends
4. **Priming yield required**: Generator must start with `_ = yield Output(...)`

### When to Use Generator vs Callback API

| Feature | ScalarFunctionGenerator | ScalarFunction |
|---------|------------------------|----------------|
| API style | Generator with `process()` | Callback with `compute()` |
| Logging | Yield `Message` directly | Call `self.log(level, msg)` |
| Complexity | More control, more code | Simpler, less code |
| Use when | Need log messages mid-batch | Standard transforms |

---

## Table Function Generator (No Input)

Use `TableFunctionGenerator` when you need to generate data without receiving input.

### Basic Template

```python
import pyarrow as pa
from vgi import TableFunctionGenerator, Output, Arg
from vgi.table_function import TableCardinality

class MyTableFunction(TableFunctionGenerator):
    """Generate data without input."""

    class Meta:
        name = "my_table_function"
        max_workers = 1  # Or None for parallel

    count = Arg[int](0, doc="Number of rows to generate")
    BATCH_SIZE = 1000

    @property
    def output_schema(self) -> pa.Schema:
        return pa.schema([("value", pa.int64())])

    @property
    def cardinality(self) -> TableCardinality:
        """Optional: provide row count estimate."""
        return TableCardinality(estimate=self.count, max=self.count)

    def process(self):
        """Generate output batches."""
        for start in range(0, self.count, self.BATCH_SIZE):
            end = min(start + self.BATCH_SIZE, self.count)
            values = list(range(start, end))
            yield Output(
                pa.RecordBatch.from_pydict(
                    {"value": values}, schema=self.output_schema
                )
            )
```

### TableFunctionGenerator Methods

| Method | When to Override | Default |
|--------|------------------|---------|
| `process()` | Always - generate output | Required |
| `output_schema` | Define output columns | Required |
| `cardinality()` | Provide row count estimates | Returns None |
| `setup()` | Acquire resources | No-op |
| `teardown()` | Release resources | No-op |
| `initialize_global_state()` | Distributed init (primary) | Default impl |
| `load_global_state()` | Distributed init (secondary) | Default impl |

### Table Function Patterns

**Simple sequence:**
```python
def process(self):
    for i in range(self.count):
        yield Output(pa.RecordBatch.from_pydict(
            {"n": [i]}, schema=self.output_schema
        ))
```

**Batched output (recommended for large outputs):**
```python
BATCH_SIZE = 1000

def process(self):
    for start in range(0, self.count, self.BATCH_SIZE):
        end = min(start + self.BATCH_SIZE, self.count)
        values = list(range(start, end))
        yield Output(pa.RecordBatch.from_pydict(
            {"n": values}, schema=self.output_schema
        ))
```

**Parallel generation with work queue:**
```python
def initialize_global_state(self, init_input):
    # Primary worker: populate work queue
    work_items = [chunk.serialize() for chunk in self.create_chunks()]
    self.enqueue_work(work_items)
    return InitResult(self.execution_identifier)

def process(self):
    # All workers: pull from queue until empty
    while True:
        work = self.dequeue_work()
        if work is None:
            break
        for batch in self.generate_chunk(work):
            yield Output(batch)
```

---

## Table-In-Out Generator Function (Advanced)

For advanced streaming control with input data, use `TableInOutGeneratorFunction`. Most users should prefer `TableInOutFunction` instead.

### When to Use Generator API

- Need `GeneratorExit` handling
- Need fine-grained streaming control
- Need to yield multiple outputs before receiving next input

### Basic Template

```python
import pyarrow as pa
from vgi import TableInOutGeneratorFunction, Output, OutputGenerator, Arg

class MyFunction(TableInOutGeneratorFunction):
    """One-line description."""

    @property
    def output_schema(self) -> pa.Schema:
        return self.input_schema

    def process(self, batch: pa.RecordBatch) -> OutputGenerator:
        _ = yield None  # REQUIRED: priming yield

        while True:
            yield Output(batch)
            batch = yield None
            if batch is None:
                break

    def finalize(self) -> OutputGenerator | None:
        return None  # Or implement if needed
```

### Key Patterns

**Passthrough (Echo):**
```python
class EchoFunction(TableInOutGeneratorFunction):
    pass  # Default process() passes input unchanged
```

**Aggregation (emit on finalize):**
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

**Multiple outputs per input (has_more=True):**
```python
def process(self, batch: pa.RecordBatch) -> OutputGenerator:
    _ = yield None

    while True:
        for _ in range(3):
            yield Output(batch, has_more=True)
        batch = yield None
        if batch is None:
            break
```

**Logging (yield Message directly):**
```python
from vgi.log import Level, Message

def process(self, batch: pa.RecordBatch) -> OutputGenerator:
    _ = yield None

    while True:
        yield Message(Level.INFO, f"Processing {batch.num_rows} rows")
        yield Output(transformed_batch)
        batch = yield None
        if batch is None:
            break
```

---

## Common Mistakes

### 1. Forgetting the priming yield (TableInOutGeneratorFunction only)

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

```python
# ❌ WRONG - infinite loop when input ends
def process(self, batch: pa.RecordBatch) -> OutputGenerator:
    _ = yield None
    while True:
        yield Output(batch)
        batch = yield None
        # Missing: if batch is None: break
```

### 3. Returning instead of yielding in finalize()

```python
# ❌ WRONG
def finalize(self) -> OutputGenerator:
    return Output(final_batch)  # This doesn't work!

# ✅ CORRECT
def finalize(self) -> OutputGenerator:
    _ = yield None
    yield Output(final_batch)
```

### 4. Forgetting to call super().__init__()

```python
# ❌ WRONG
def __init__(self, invocation: Invocation, logger):
    self.my_value = invocation.arguments.get(0)

# ✅ CORRECT
def __init__(self, invocation: Invocation, logger):
    super().__init__(invocation=invocation, logger=logger)
    self.my_value = self.invocation.arguments.get(0)
```

### 5. Initializing state in process() instead of __init__

```python
# ⚠️ PROBLEMATIC
def process(self, batch: pa.RecordBatch) -> OutputGenerator:
    self.total = 0  # Runs once per generator
    _ = yield None
    # ...

# ✅ CLEARER
def __init__(self, invocation, logger):
    super().__init__(invocation, logger)
    self.total = 0

def process(self, batch: pa.RecordBatch) -> OutputGenerator:
    _ = yield None
    # ...
```
