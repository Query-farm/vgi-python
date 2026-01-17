# Function Metadata

Functions can define a nested `Meta` class to provide introspection metadata. No inheritance is required - just define the attributes you need.

## Basic Usage

Metadata works with all function types: `ScalarFunction`, `TableFunctionGenerator`, and `TableInOutFunction`.

```python
from typing import Annotated
from vgi import TableInOutFunction, Arg

class SumColumnsFunction(TableInOutFunction):
    """Sum all numeric columns in the input."""

    class Meta:
        name = "sum_columns"  # Registration name (default: snake_case of class)
        description = "Sum all numeric columns and return a single row"
        categories = ["aggregation", "numeric"]
        max_workers = 1  # Single-threaded (used by max_processes property)

    column_name: Annotated[str | None, Arg("column", default=None, doc="Value to sum")]

    def transform(self, batch):
        ...
```

```python
from typing import Annotated
from vgi import ScalarFunction, Arg
from vgi.arguments import AnyArrow
import pyarrow as pa
import pyarrow.compute as pc

class DoubleValues(ScalarFunction):
    """Double the values in a numeric column."""

    class Meta:
        name = "double"
        output_type = AnyArrow  # Dynamic type - depends on input
        categories = ["numeric", "transform"]

    col_name: Annotated[str, Arg(0, doc="Numeric value to double")]

    @property
    def output_type(self) -> pa.DataType:
        return self.input_schema.field(self.col_name).type

    def compute(self, batch):
        return pc.multiply(batch.column(self.col_name), 2)
```

## Accessing Metadata

```python
# Get resolved metadata
meta = SumColumnsFunction.get_metadata()
print(meta.name)        # "sum_columns"
print(meta.max_workers) # 1
print(meta.parameters)  # [ParameterInfo(name='column_name', ...)]

# Get as JSON-serializable dict
info = SumColumnsFunction.describe()
```

## Available Meta Attributes

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | Class name → snake_case | Function registration name |
| `description` | `str` | First docstring line | Human-readable description |
| `categories` | `list[str]` | `[]` | Classification tags |
| `tags` | `dict[str, str]` | `{}` | Custom key-value tags |
| `examples` | `list` | `[]` | SQL examples (str or FunctionExample) |
| `max_workers` | `int\|None` | `None` (unlimited) | Max parallel workers |
| `stability` | `FunctionStability` | `CONSISTENT` | Output determinism |
| `null_handling` | `NullHandling` | `DEFAULT` | NULL input behavior |
| `required_settings` | `list[str]` | `[]` | Required DuckDB settings |
| `projection_pushdown` | `bool` | `True` | Enable column pruning |
| `filter_pushdown` | `bool` | `False` | Enable filter pushdown |
| `preserves_order` | `OrderPreservation` | `PRESERVES_ORDER` | Row order guarantee |
| `order_dependent` | `OrderDependence` | `NOT_ORDER_DEPENDENT` | Aggregate order sensitivity |
| `distinct_dependent` | `DistinctDependence` | `NOT_DISTINCT_DEPENDENT` | Aggregate DISTINCT sensitivity |
| `output_type` | `pa.DataType\|AnyArrow` | Required for ScalarFunction | Scalar output type |

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
