# Polars Scalar Functions

This guide explains how to create scalar functions using Polars with the
expression-based `PolarsScalarFunction` API.

## Overview

`PolarsScalarFunction` provides:

- **Expression-based API**: Return `pl.Expr` instead of computing Series directly
- **Zero-copy conversion**: Arrow ↔ Polars without data copying
- **Named column references**: Reference columns by parameter name
- **Type safety**: Optional type bounds for dynamic types

## Quick Start

```python
from typing import Annotated
import polars as pl
from vgi import PolarsScalarFunction, Param

class UpperCase(PolarsScalarFunction):
    """Convert text to uppercase."""

    # 1. Declare parameter with position and Polars type
    text: Annotated[pl.Utf8, Param(position=0, doc="Input string")]

    # 2. Declare output type in Meta
    class Meta:
        output_type = pl.Utf8

    # 3. Return a Polars expression
    def compute_polars(self) -> pl.Expr:
        return pl.col("text").str.to_uppercase()
```

## Parameter Declaration

Parameters are declared as class attributes using `Annotated[type, Param(...)]`:

```python
class MyFunction(PolarsScalarFunction):
    # Single parameter at position 0
    value: Annotated[pl.Float64, Param(position=0, doc="Input value")]

    # Multiple parameters with different positions
    left: Annotated[pl.Int64, Param(position=0, doc="Left operand")]
    right: Annotated[pl.Int64, Param(position=1, doc="Right operand")]
```

### Param Options

| Option | Type | Description |
|--------|------|-------------|
| `position` | `int` | Column position in input batch (required) |
| `doc` | `str` | Documentation string |
| `varargs` | `bool` | Collect all remaining columns |
| `type_bound` | `Callable` | Type constraint for dynamic types |

## Writing Expressions

In `compute_polars()`, reference columns by their parameter name:

```python
def compute_polars(self) -> pl.Expr:
    # Reference the "value" parameter as pl.col("value")
    return pl.col("value") * 2
```

### Multiple Columns

```python
class AddColumns(PolarsScalarFunction):
    left: Annotated[pl.Float64, Param(position=0, doc="First")]
    right: Annotated[pl.Float64, Param(position=1, doc="Second")]

    class Meta:
        output_type = pl.Float64

    def compute_polars(self) -> pl.Expr:
        return pl.col("left") + pl.col("right")
```

### Using Polars Methods

```python
def compute_polars(self) -> pl.Expr:
    # String operations
    return pl.col("text").str.to_uppercase()

    # Numeric operations
    return pl.col("value").abs().sqrt()

    # Conditional logic
    return pl.when(pl.col("x") > 0).then(1).otherwise(-1)

    # Aggregations (computed per-batch)
    col = pl.col("value")
    return (col - col.mean()) / col.std()
```

## Output Types

### Static Output Type

When output type is known at definition time:

```python
class Meta:
    output_type = pl.Float64  # or pl.Utf8, pl.Int64, etc.
```

### Dynamic Output Type

When output type depends on input (e.g., preserving input type):

```python
from typing import Any
import pyarrow.types as pat
from vgi import AnyPolars

class Double(PolarsScalarFunction):
    value: Annotated[
        Any,  # Accept any type
        Param(
            position=0,
            doc="Value to double",
            # Constrain to numeric types
            type_bound=[pat.is_integer, pat.is_floating],
        ),
    ]

    class Meta:
        output_type = AnyPolars  # Dynamic type marker

    @property
    def output_polars_type(self) -> pl.DataType:
        # Return input type to preserve it
        return self.polars_schema[self.input_schema.field(0).name]

    def compute_polars(self) -> pl.Expr:
        return pl.col("value") * 2
```

### Type Bounds

Type bounds constrain what input types are accepted:

```python
import pyarrow.types as pat

# Single predicate
type_bound=pat.is_integer

# Multiple predicates (OR logic - any must match)
type_bound=[pat.is_integer, pat.is_floating]

# Available predicates from pyarrow.types:
# - pat.is_integer, pat.is_floating, pat.is_numeric
# - pat.is_string, pat.is_binary, pat.is_boolean
# - pat.is_temporal, pat.is_date, pat.is_time, pat.is_timestamp
```

If validation fails, you get a clear error:
```
SchemaValidationError: Column 'value' has type string,
but type_bound requires: is_integer, is_floating
```

## Variable Arguments (Varargs)

Accept any number of columns with `varargs=True`:

```python
class SumAll(PolarsScalarFunction):
    values: Annotated[
        pl.Float64,
        Param(position=0, doc="Values to sum", varargs=True)
    ]

    class Meta:
        output_type = pl.Float64

    def compute_polars(self) -> pl.Expr:
        # Vararg columns are renamed to values_0, values_1, etc.
        # Use regex to match all of them
        return pl.sum_horizontal(pl.col("^values_.*$"))
```

### How Varargs Work

1. Input columns: `["a", "b", "c"]`
2. After rename: `["values_0", "values_1", "values_2"]`
3. Match with: `pl.col("^values_.*$")`

## Constant Arguments

Access scalar values passed in SQL (not from table columns):

```python
class Multiply(PolarsScalarFunction):
    value: Annotated[pl.Float64, Param(position=0, doc="Column")]

    class Meta:
        output_type = pl.Float64

    @property
    def factor(self) -> float:
        """Get constant from SQL arguments."""
        return self.invocation.arguments.positional[0].as_py()

    def compute_polars(self) -> pl.Expr:
        return pl.col("value") * self.factor
```

SQL usage: `SELECT polars_multiply(price, 1.1) FROM products`

## Meta Class Options

```python
class Meta:
    # Output type (required)
    output_type = pl.Float64

    # Function name for SQL (defaults to class name in snake_case)
    name = "my_custom_function"

    # Description for catalogs
    description = "Multiplies values by a factor"

    # Example queries
    examples = [
        FunctionExample(
            sql="SELECT my_func(col) FROM table",
            description="Basic usage example",
        ),
    ]
```

## Available Instance Attributes

Inside your function methods, you have access to:

| Attribute | Type | Description |
|-----------|------|-------------|
| `self.input_schema` | `pa.Schema` | Arrow schema of input |
| `self.polars_schema` | `Mapping[str, pl.DataType]` | Polars schema |
| `self.output_schema` | `pa.Schema` | Arrow output schema |
| `self.invocation` | `Invocation` | Full invocation details |
| `self.empty_output_batch` | `pa.RecordBatch` | Empty output batch |

## Lifecycle Methods

```python
class MyFunction(PolarsScalarFunction):
    def bind(self) -> None:
        """Called after input_schema is set. Override to validate or compute."""
        super().bind()
        # Access self.input_schema, self.polars_schema here

    def setup(self) -> None:
        """Called before processing. Acquire resources."""
        pass

    def teardown(self) -> None:
        """Called after processing. Release resources."""
        pass
```

## Complete Example

```python
from typing import Annotated, Any
import polars as pl
import pyarrow.types as pat
from vgi import PolarsScalarFunction, Param, AnyPolars
from vgi.metadata import FunctionExample

class ZScoreNormalize(PolarsScalarFunction):
    """Compute z-score normalization: (value - mean) / std.

    Accepts any numeric type and preserves it in the output.
    """

    value: Annotated[
        Any,
        Param(
            position=0,
            doc="Numeric column to normalize",
            type_bound=[pat.is_integer, pat.is_floating],
        ),
    ]

    class Meta:
        name = "zscore_normalize"
        description = "Z-score normalization (standardization)"
        output_type = AnyPolars
        examples = [
            FunctionExample(
                sql="SELECT zscore_normalize(score) FROM exams",
                description="Normalize exam scores",
            ),
        ]

    @property
    def output_polars_type(self) -> pl.DataType:
        # Always output Float64 for normalized values
        return pl.Float64

    def compute_polars(self) -> pl.Expr:
        col = pl.col("value").cast(pl.Float64)
        return (col - col.mean()) / col.std()
```

## See Also

- [vgi/examples/scalar_polars.py](../vgi/examples/scalar_polars.py) - Example implementations
- [vgi/scalar_function_polars.py](../vgi/scalar_function_polars.py) - Base class source
