# Argument Specification Serialization

This document describes how VGI function argument specifications are serialized
to Apache Arrow schemas for IPC transmission and DuckDB function registration.

## Quick Reference

| Metadata Key | Value | Meaning |
|--------------|-------|---------|
| `vgi_arg` | `named` | Named argument (not positional) |
| `vgi_type` | `table` | Table input argument |
| `vgi_type` | `any` | Any Arrow type argument |
| `vgi_varargs` | `true` | Variable arguments |

## Schema Format

Arguments are serialized as a **single Arrow schema** where each field
represents one argument.

### Field Order

1. **Positional arguments** come first, in order (field index = position index)
2. **Named arguments** follow, marked with metadata

### Field Components

| Component | Source |
|-----------|--------|
| Field name | Python attribute name |
| Field type | Exact Arrow data type |
| Field metadata | Markers for named, table, any, varargs |

## Positional Arguments

Positional arguments have no special metadata. Their position index is
determined by their order in the schema.

```python
# Arg[int](0) becomes:
pa.field("count", pa.int64())

# Arg[str](1) becomes:
pa.field("name", pa.utf8())
```

## Named Arguments

Named arguments have `vgi_arg=named` metadata. The field name is the
argument key used in SQL.

```python
# Arg[str]("format") becomes:
pa.field("format", pa.utf8(), metadata={b"vgi_arg": b"named"})
```

## Special Types

### Table Input

Table input arguments (`Arg[TableInput]`) receive streaming RecordBatches
rather than scalar values.

- **Arrow type**: `pa.null()`
- **Metadata**: `{b"vgi_type": b"table"}`

```python
# Arg[TableInput](1) becomes:
pa.field("data", pa.null(), metadata={b"vgi_type": b"table"})
```

### Any Type

Any-type arguments (`Arg[AnyArrow]`) accept any valid Arrow scalar type
at runtime.

- **Arrow type**: `pa.null()`
- **Metadata**: `{b"vgi_type": b"any"}`

```python
# Arg[AnyArrow](0) becomes:
pa.field("value", pa.null(), metadata={b"vgi_type": b"any"})
```

### Variable Arguments

Varargs arguments (`varargs=True`) collect all remaining positional
arguments from their position onwards.

- **Arrow type**: The element type (e.g., `pa.int64()` for int varargs)
- **Metadata**: `{b"vgi_varargs": b"true"}`

```python
# Arg[str](0, varargs=True) becomes:
pa.field("columns", pa.utf8(), metadata={b"vgi_varargs": b"true"})
```

## Combined Metadata

Fields can have multiple metadata keys. For example, a named argument
that accepts any type:

```python
# Arg[AnyArrow]("threshold") becomes:
pa.field("threshold", pa.null(), metadata={
    b"vgi_arg": b"named",
    b"vgi_type": b"any",
})
```

## Complete Examples

### Example 1: Simple Function

```python
class MyFunction(TableInOutFunction):
    count = Arg[int](0)           # Positional 0
    name = Arg[str](1)            # Positional 1
    verbose = Arg[bool]("verbose") # Named

# Serializes to:
schema = pa.schema([
    pa.field("count", pa.int64()),
    pa.field("name", pa.utf8()),
    pa.field("verbose", pa.bool_(), metadata={b"vgi_arg": b"named"}),
])
```

### Example 2: Function with Table Input

```python
class TransformFunction(TableInOutFunction):
    multiplier = Arg[float](0)
    data: TableInput = Arg[TableInput](1)

# Serializes to:
schema = pa.schema([
    pa.field("multiplier", pa.float64()),
    pa.field("data", pa.null(), metadata={b"vgi_type": b"table"}),
])
```

### Example 3: Function with Varargs

```python
class SumColumnsFunction(TableInOutFunction):
    columns = Arg[str](0, varargs=True)

# Serializes to:
schema = pa.schema([
    pa.field("columns", pa.utf8(), metadata={b"vgi_varargs": b"true"}),
])
```

### Example 4: Complex Function

```python
class ComplexFunction(TableInOutFunction):
    count = Arg[int](0)
    data: TableInput = Arg[TableInput](1)
    extra = Arg[float](2, varargs=True)
    format = Arg[str]("format")
    threshold: AnyArrow = Arg[AnyArrow]("threshold")

# Serializes to:
schema = pa.schema([
    pa.field("count", pa.int64()),
    pa.field("data", pa.null(), metadata={b"vgi_type": b"table"}),
    pa.field("extra", pa.float64(), metadata={b"vgi_varargs": b"true"}),
    pa.field("format", pa.utf8(), metadata={b"vgi_arg": b"named"}),
    pa.field("threshold", pa.null(), metadata={
        b"vgi_arg": b"named",
        b"vgi_type": b"any",
    }),
])
```

## Serialization Code

### Serialize to Bytes

```python
from vgi.argument_spec import argument_specs_to_schema

# Create schema from specs
schema = argument_specs_to_schema(specs)

# Serialize to bytes
schema_bytes = schema.serialize().to_pybytes()
```

### Deserialize from Bytes

```python
import pyarrow as pa
from vgi.argument_spec import schema_to_argument_specs

# Deserialize schema
schema = pa.ipc.read_schema(pa.py_buffer(schema_bytes))

# Convert to ArgumentSpec objects
specs = schema_to_argument_specs(schema)
```

## Parsing Algorithm

To parse a schema back to argument specifications:

1. Initialize `position_index = 0`
2. For each field in schema:
   - Check if field has `vgi_arg=named` metadata
   - If named: `position = field.name` (string)
   - If positional: `position = position_index`, then increment `position_index`
   - Check for `vgi_type` metadata (`table` or `any`)
   - Check for `vgi_varargs` metadata
   - Create `ArgumentSpec` with extracted info

```python
def parse_schema(schema):
    specs = []
    position_index = 0

    for field in schema:
        metadata = field.metadata or {}

        # Determine position type
        if metadata.get(b"vgi_arg") == b"named":
            position = field.name  # Named argument
        else:
            position = position_index  # Positional argument
            position_index += 1

        # Check special types
        vgi_type = metadata.get(b"vgi_type")
        is_table_input = (vgi_type == b"table")
        is_any_type = (vgi_type == b"any")
        is_varargs = (metadata.get(b"vgi_varargs") == b"true")

        specs.append(ArgumentSpec(
            name=field.name,
            position=position,
            arrow_type=field.type,
            is_table_input=is_table_input,
            is_any_type=is_any_type,
            is_varargs=is_varargs,
        ))

    return specs
```

## Not Included

The following are **not** serialized in the schema:

- **Default values** - handled at runtime by the `Arg` descriptor
- **Validation constraints** (`ge`, `le`, `choices`, `pattern`) - Python-side validation
- **Documentation strings** - available via `ParameterInfo` in metadata

These are implementation details of the Python function runtime, not part
of the argument type specification needed for function registration.
