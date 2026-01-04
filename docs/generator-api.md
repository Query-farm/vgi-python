# Generator API (Advanced)

This document covers the generator-based APIs for both table functions and table-in-out functions.

## Table Function Generator (No Input)

Use `TableFunctionGenerator` when you need to generate data without receiving input.

### Basic Template

```python
import pyarrow as pa
from vgi import TableFunctionGenerator, Output, Arg
from vgi.table_function import CardinalityInfo

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

    def cardinality(self) -> CardinalityInfo:
        """Optional: provide row count estimate."""
        return CardinalityInfo(estimate=self.count, max=self.count)

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
| `perform_init()` | Distributed init (primary) | Default impl |
| `retrieve_init()` | Distributed init (secondary) | Default impl |

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
def perform_init(self, init_input):
    # Primary worker: populate work queue
    work_items = [chunk.serialize() for chunk in self.create_chunks()]
    self.enqueue_work(work_items)
    return InitResult(self.init_identifier)

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
