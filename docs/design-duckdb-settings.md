# Design: DuckDB Settings/Pragmas Access for VGI Functions

## Overview

VGI functions need access to DuckDB settings/pragmas to:
1. Determine output schema during bind phase (e.g., timezone affects datetime output)
2. Influence processing behavior (e.g., thread limits, memory settings)
3. Maintain consistency with DuckDB's execution environment

This design adds support for functions to declare required settings and access their values.

## Design Decisions

### 1. Settings Declaration in Meta Class

Functions declare required settings in their `Meta` class:

```python
class TimezoneAwareFunction(TableInOutFunction):
    class Meta:
        required_settings = ["TimeZone", "Calendar"]  # List of DuckDB setting names
        max_workers = 1

    @property
    def output_schema(self) -> pa.Schema:
        # Settings available during bind - can influence output schema
        tz = self.get_setting("TimeZone", "UTC")
        return pa.schema([
            ("timestamp", pa.timestamp("us", tz=tz)),
        ])
```

**Changes to `vgi/metadata.py`:**
- Add `"required_settings"` to `_VALID_META_ATTRIBUTES`
- Add `required_settings: list[str] = field(default_factory=list)` to `ResolvedMetadata`
- Update `resolve_metadata()` to extract the field
- Update Arrow serialization schema and methods

### 2. Settings in Invocation

Settings are passed as a dict in the `Invocation`:

```python
@dataclass(frozen=True, slots=True)
class Invocation:
    # ... existing fields ...
    duckdb_settings: dict[str, str] | None = None  # New field
```

**Changes to `vgi/invocation.py`:**
- Add `duckdb_settings: dict[str, str] | None = None` field
- Serialize as `pa.map_(pa.utf8(), pa.utf8())` type in Arrow IPC
- Deserialize with backward compatibility (None if field missing)

**Serialization Format:**
```python
# In serialize():
pa.field("duckdb_settings", pa.map_(pa.utf8(), pa.utf8()), nullable=True)

# Value encoding:
"duckdb_settings": (
    list(self.duckdb_settings.items()) if self.duckdb_settings else None
)
```

### 3. Settings Accessor API

Functions access settings via the `Function` base class:

```python
class Function:
    @property
    def settings(self) -> dict[str, str]:
        """All DuckDB settings passed to this function."""
        return dict(self.invocation.duckdb_settings or {})

    def get_setting(self, name: str, default: str | None = None) -> str | None:
        """Get a specific DuckDB setting value.

        Args:
            name: DuckDB setting name (e.g., "TimeZone", "threads")
            default: Value to return if setting not present

        Returns:
            Setting value or default
        """
        if self.invocation.duckdb_settings is None:
            return default
        return self.invocation.duckdb_settings.get(name, default)
```

**Changes to `vgi/function.py`:**
- Add `settings` property
- Add `get_setting()` method

### 4. Worker Validation

The worker validates required settings during bind:

```python
# In Worker._validate_required_settings():
def _validate_required_settings(
    self,
    func_cls: type[Function],
    invocation: Invocation
) -> None:
    """Validate that all required settings are present."""
    meta = func_cls.get_metadata()
    required = set(meta.required_settings)

    if not required:
        return  # No settings required

    provided = set(invocation.duckdb_settings.keys()) if invocation.duckdb_settings else set()
    missing = required - provided

    if missing:
        raise ValueError(
            f"Function '{meta.name}' requires settings {sorted(missing)} "
            f"but they were not provided. Provided: {sorted(provided)}"
        )
```

**Changes to `vgi/worker.py`:**
- Add `_validate_required_settings()` method
- Call it after function class resolution, before instantiation

### 5. Client Support

The client passes settings when creating invocations:

```python
# In Client methods:
def invoke(
    self,
    function_name: str,
    ...,
    duckdb_settings: dict[str, str] | None = None,  # New parameter
) -> ...:
```

**Changes to `vgi/client/client.py`:**
- Add `duckdb_settings` parameter to `_initialize_stream_common()` and related methods
- Include in `Invocation` creation

## Protocol Flow

```
Client                                    Worker
  │                                         │
  │  Invocation                             │
  │  ├─ function_name: "timezone_func"      │
  │  ├─ arguments: {...}                    │
  │  ├─ input_schema: {...}                 │
  │  └─ duckdb_settings: {                  │
  │       "TimeZone": "America/New_York",   │
  │       "threads": "4"                    │
  │     }                                   │
  │─────────────────────────────────────────▶│
  │                                         │ 1. Lookup function class
  │                                         │ 2. Validate required_settings
  │                                         │ 3. Instantiate function
  │                                         │    (settings available via self.settings)
  │                                         │ 4. Get output_schema (may use settings)
  │◀─────────────────────────────────────────│
  │  OutputSpec                             │
  │  └─ output_schema: {...}                │
  │                                         │
```

## Example Function

```python
from vgi import TableInOutFunction, Arg
import pyarrow as pa

class DebugOutputFunction(TableInOutFunction):
    """Function that optionally includes debug columns based on setting."""

    class Meta:
        required_settings = ["vgi_debug_mode"]
        max_workers = 1

    @property
    def output_schema(self) -> pa.Schema:
        # Base schema from input
        fields = list(self.input_schema)

        # Add debug column if debug mode enabled
        if self.get_setting("vgi_debug_mode") == "true":
            fields.append(pa.field("_debug_worker_pid", pa.int32()))

        return pa.schema(fields)

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        if self.get_setting("vgi_debug_mode") == "true":
            # Add debug column
            import os
            debug_col = pa.array([os.getpid()] * batch.num_rows, type=pa.int32())
            return pa.RecordBatch.from_arrays(
                list(batch.columns) + [debug_col],
                schema=self.output_schema
            )
        return batch
```

## Files to Modify

| File | Changes |
|------|---------|
| `vgi/metadata.py` | Add `required_settings` to Meta, ResolvedMetadata, Arrow schema |
| `vgi/invocation.py` | Add `duckdb_settings` field, serialization |
| `vgi/function.py` | Add `settings` property, `get_setting()` method |
| `vgi/worker.py` | Add settings validation during bind |
| `vgi/client/client.py` | Add `duckdb_settings` parameter |
| `docs/protocol.md` | Document settings in protocol |
| `docs/metadata.md` | Document `required_settings` |
| `CLAUDE.md` | Add settings usage example |

## Test Cases

1. **Serialization roundtrip**: Invocation with settings serializes/deserializes correctly
2. **Empty settings**: Function with no required_settings works without settings
3. **Required settings present**: Function receives and can access settings
4. **Missing required settings**: Worker rejects with clear error
5. **Settings affect output schema**: Verify bind returns different schema based on setting
6. **Settings affect processing**: Verify transform behavior changes based on setting
7. **Backward compatibility**: Old clients without settings field work with new workers

## Implementation Order

1. `vgi/metadata.py` - Add required_settings to Meta
2. `vgi/invocation.py` - Add duckdb_settings field
3. `vgi/function.py` - Add settings accessor
4. `vgi/worker.py` - Add validation
5. `vgi/client/client.py` - Add settings parameter
6. Example function
7. Tests
8. Documentation
