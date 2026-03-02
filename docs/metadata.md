# Function Metadata

Functions can define a nested `Meta` class to provide introspection metadata. No inheritance is required - just define the attributes you need.

## Basic Usage

Metadata works with all function types: `ScalarFunction`, `TableFunctionGenerator`, and `TableInOutFunction`.

```python
from typing import Annotated, Any
from vgi import ScalarFunction, Param, Returns
import pyarrow as pa
import pyarrow.compute as pc

class MultiplyFunction(ScalarFunction):
    """Multiplies a value by a constant factor."""

    class Meta:
        name = "multiply"
        description = "Multiplies a value by a constant factor"
        categories = ["numeric", "transform"]

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.Int64Array, Param(doc="Integer value to multiply")],
        factor: Annotated[int, ConstParam("Multiplication factor")],
    ) -> Annotated[pa.Int64Array, Returns()]:
        return pc.multiply(value, factor)
```

```python
from typing import Annotated, Any
from vgi import ScalarFunction, Param, Returns
from vgi.scalar_function import BindParameters, BindResult
import pyarrow as pa
import pyarrow.compute as pc

class DoubleFunction(ScalarFunction):
    """Double numeric values."""

    class Meta:
        name = "double"
        description = "Doubles numeric values"
        categories = ["numeric", "transform"]

    @classmethod
    def on_bind(cls, params: BindParameters) -> BindResult:
        field = params.arguments_schema.field(0)
        return BindResult(field.type)

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.Array[Any], Param(doc="Numeric value to double")],
    ) -> Annotated[pa.Array[Any], Returns()]:
        return pc.multiply(value, 2)
```

## Accessing Metadata

```python
# Get resolved metadata
meta = SumValuesFunction.get_metadata()
print(meta.name)        # "sum_values"
print(meta.max_workers) # 1
print(meta.parameters)  # [ParameterInfo(name='column_name', ...)]

# Get as JSON-serializable dict
info = SumValuesFunction.describe()
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
