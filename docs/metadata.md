# Function Metadata

Functions can define a nested `Meta` class to provide introspection metadata. No inheritance is required - just define the attributes you need.

## Basic Usage

Metadata works with all function types: `ScalarFunction`, `TableFunctionGenerator`, and `TableInOutFunction`.

```python
from vgi import TableInOutFunction, Arg

class SumColumnsFunction(TableInOutFunction):
    """Sum all numeric columns in the input."""

    class Meta:
        name = "sum_columns"  # Registration name (default: snake_case of class)
        description = "Sum all numeric columns and return a single row"
        categories = ["aggregation", "numeric"]
        max_workers = 1  # Single-threaded (used by max_processes property)
        supports_distributed = True

    columns = Arg[list]("columns", default=None, doc="Columns to sum")

    def transform(self, batch):
        ...
```

```python
from vgi import ScalarFunction, Arg
import pyarrow as pa
import pyarrow.compute as pc

class DoubleColumn(ScalarFunction):
    """Double the values in a numeric column."""

    class Meta:
        name = "double"
        categories = ["numeric", "transform"]

    column = Arg[str](0, doc="Column to double")

    @property
    def output_type(self) -> pa.DataType:
        return self.input_schema.field(self.column).type

    def compute(self, batch):
        return pc.multiply(batch.column(self.column), 2)
```

## Accessing Metadata

```python
# Get resolved metadata
meta = SumColumnsFunction.get_metadata()
print(meta.name)        # "sum_columns"
print(meta.max_workers) # 1
print(meta.parameters)  # [ParameterInfo(name='columns', ...)]

# Get as JSON-serializable dict
info = SumColumnsFunction.describe()
```

## Available Meta Attributes

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | Class name → snake_case | Function registration name |
| `description` | `str` | First docstring line | Human-readable description |
| `categories` | `list[str]` | `[]` | Classification tags |
| `examples` | `list` | `[]` | SQL examples (str or FunctionExample) |
| `max_workers` | `int\|None` | `None` (unlimited) | Max parallel workers |
| `stability` | `FunctionStability` | `CONSISTENT` | Output determinism |
| `projection_pushdown` | `bool` | `True` | Enable column pruning |
| `filter_pushdown` | `bool` | `False` | Enable filter pushdown |
| `preserves_order` | `OrderPreservation` | `PRESERVES_ORDER` | Row order guarantee |
| `supports_distributed` | `bool` | `False` | Enable distributed execution |
| `internal` | `bool` | `False` | Mark as internal function |

## Metadata Inheritance

Meta attributes are inherited from parent classes:

```python
class FilterFunction(TableInOutFunction):
    class Meta:
        categories = ["filter"]
        preserves_order = OrderPreservation.PRESERVES_ORDER

class PositiveFilter(FilterFunction):
    class Meta:
        description = "Keep only positive values"
    # Inherits categories=["filter"] from parent
```

## Arrow Serialization (Worker Registration)

Metadata can be serialized to Arrow for worker registration:

```python
from vgi import functions_to_arrow
from vgi.metadata import arrow_to_functions

# Worker sends available functions to client
batch = functions_to_arrow([EchoFunction, SumFunction])

# Client deserializes
function_infos = arrow_to_functions(batch)
for info in function_infos:
    print(f"{info.name}: {info.description}")
```
