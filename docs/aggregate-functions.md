# Aggregate Functions

Aggregate functions accumulate input rows into per-group state and produce one result per group. They power SQL expressions like `SELECT my_agg(col) FROM t GROUP BY category`.

## Architecture

VGI aggregate functions use an all-unary RPC design. Each DuckDB callback (bind, update, combine, finalize, destructor) maps to one RPC call. Per-group state lives in `FunctionStorage` (SQLite-backed), keyed by a globally unique `group_id` assigned by C++.

```
DuckDB                          Python Worker
──────                          ─────────────
aggregate_bind()    ──RPC──►    on_bind()         → execution_id + output_schema
initialize()        (local)     (assigns group_id)
aggregate_update()  ──RPC──►    update()          → accumulate rows into states
aggregate_combine() ──RPC──►    combine()         → merge parallel worker states
aggregate_finalize()──RPC──►    finalize()        → produce result per group
aggregate_destructor()─RPC──►   (cleanup)         → clear FunctionStorage
```

**Key design decisions:**

- **Globally unique group_ids**: C++ assigns group_ids from a shared atomic counter on `ExecState`, so IDs never collide across parallel threads.
- **Lazy initialization**: `initial_state()` is called on first encounter during `update()`, not during C++ `initialize()`.
- **State in FunctionStorage**: All per-group state is serialized via `ArrowSerializableDataclass` and stored in SQLite. This makes the design HTTP-transport compatible.
- **Single destructor call**: C++ tracks a `destroy_counter` and only sends the cleanup RPC when all states have been destroyed.

## Quick Start

```python
from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi.aggregate_function import AggregateFunction
from vgi.arguments import Param, Returns
from vgi.metadata import NullHandling
from vgi.table_function import ProcessParams


@dataclass(kw_only=True)
class SumState(ArrowSerializableDataclass):
    total: Annotated[int, ArrowType(pa.int64())] = 0


class SumFunction(AggregateFunction[SumState]):
    class Meta:
        name = "vgi_sum"
        description = "Sum integer values"
        null_handling = NullHandling.DEFAULT

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> SumState:
        return SumState()

    @classmethod
    def update(
        cls,
        states: dict[int, SumState],
        group_ids: pa.Int64Array,
        value: Annotated[pa.Int64Array, Param(doc="Column to sum")],
    ) -> None:
        table = pa.table({"gid": group_ids, "value": value})
        grouped = table.group_by("gid").aggregate([("value", "sum")])
        for i in range(grouped.num_rows):
            gid = grouped.column("gid")[i].as_py()
            val = grouped.column("value_sum")[i].as_py()
            if val is not None:
                states[gid] = SumState(total=states[gid].total + val)

    @classmethod
    def combine(
        cls, source: SumState, target: SumState, params: ProcessParams[None]
    ) -> SumState:
        return SumState(total=source.total + target.total)

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, SumState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns(pa.int64())]:
        results = [states[gid.as_py()].total for gid in group_ids]
        return pa.record_batch({"result": pa.array(results, type=pa.int64())})
```

## State Class

The state class must be a `dataclass` extending `ArrowSerializableDataclass`. Each field needs an `ArrowType` annotation for serialization:

```python
@dataclass(kw_only=True)
class AvgState(ArrowSerializableDataclass):
    total: Annotated[float, ArrowType(pa.float64())] = 0.0
    count: Annotated[int, ArrowType(pa.int64())] = 0
```

States are serialized to bytes and stored in `FunctionStorage` between RPC calls. Use simple scalar types (int, float, str, bytes) for efficient serialization.

## Method Reference

### `initial_state(params) -> TState`

Called when a group_id is first encountered during `update()`. Returns the identity element for the aggregation (e.g., 0 for sum, empty string for concatenation).

### `update(states, group_ids, ...columns) -> None`

Accumulates input rows into per-group state. Called once per batch of input rows.

- `states`: `dict[int, TState]` — pre-populated with `initial_state()` for new group_ids
- `group_ids`: `pa.Int64Array` — parallel to each column array, identifies which group each row belongs to
- Additional parameters: declared via `Param` annotations, receive `pa.Array` column data

The `states` dict is mutable — update values in-place. The framework saves all modified states to `FunctionStorage` after each call.

### `combine(source, target, params) -> TState`

Merges two partial states from parallel workers. Called during DuckDB's hash aggregate combine phase.

- `source`: state to merge from (will be removed after combine)
- `target`: state to merge into
- Returns: the merged state (replaces `target`)

### `finalize(group_ids, states, params) -> RecordBatch`

Produces final results. Must return a `RecordBatch` with one row per `group_id`, in the same order as `group_ids`.

Annotate the return type with `Returns(arrow_type)` to declare the output type:

```python
def finalize(cls, ...) -> Annotated[pa.RecordBatch, Returns(pa.float64())]:
```

### `on_bind(params) -> BindResponse` (optional)

Override for dynamic output types or bind-time validation. Settings and secrets are available here via `params.settings` and `params.secrets`.

## Meta Class Options

```python
class Meta:
    name = "vgi_my_agg"                                    # SQL function name
    description = "Description for catalog"                # Optional
    null_handling = NullHandling.DEFAULT                    # DEFAULT or SPECIAL
    order_dependent = OrderDependence.ORDER_DEPENDENT       # For order-sensitive aggs
    distinct_dependent = DistinctDependence.DISTINCT_DEPENDENT  # For DISTINCT
```

- `NullHandling.DEFAULT`: NULL inputs are skipped (never passed to `update`)
- `NullHandling.SPECIAL`: NULL inputs are passed through (needed for `COUNT(*)`)
- `OrderDependence.ORDER_DEPENDENT`: result depends on input order (e.g., `LISTAGG`)
- `DistinctDependence.DISTINCT_DEPENDENT`: `DISTINCT` modifier changes result

## Input Parameters

Input columns are declared on `update()` using `Param` annotations, following the same pattern as `ScalarFunction.compute()`:

```python
@classmethod
def update(
    cls,
    states: dict[int, MyState],
    group_ids: pa.Int64Array,
    value: Annotated[pa.DoubleArray, Param(doc="Values")],
    weight: Annotated[pa.DoubleArray, Param(doc="Weights")],
) -> None:
```

### Constant Parameters (ConstParam)

For parameters that are constant across all rows (e.g., a percentile threshold), use `ConstParam`. These are constant-folded at bind time and stored in `FunctionStorage`:

```python
@classmethod
def update(
    cls,
    states: dict[int, MyState],
    group_ids: pa.Int64Array,
    value: Annotated[pa.DoubleArray, Param(doc="Values")],
    percentile: Annotated[float, ConstParam("Percentile (0-1)", phase="finalize")] = 0.5,
) -> None:
```

The `phase` parameter controls when the constant is injected:

| Phase | Injected in `update()` | Injected in `finalize()` |
|-------|----------------------|------------------------|
| `"all"` (default) | Yes | Yes |
| `"update"` | Yes | No |
| `"finalize"` | No | Yes |

Use `phase="finalize"` to avoid serializing large constants on every update batch — they're only loaded when `finalize()` needs them.

In `finalize()`, access constant values via `params.args.positional`:

```python
@classmethod
def finalize(cls, group_ids, states, params):
    pct = params.args.positional[0].as_py() if params.args and params.args.positional else 0.5
```

### Varargs

For aggregate functions accepting a variable number of columns, use `Param(varargs=True)`. The parameter receives a list of arrays:

```python
@classmethod
def update(
    cls,
    states: dict[int, MyState],
    group_ids: pa.Int64Array,
    columns: Annotated[pa.Array, Param(doc="Columns to sum", varargs=True)],
) -> None:
    for i in range(len(group_ids)):
        gid = group_ids[i].as_py()
        for col in columns:
            val = col[i].as_py()
            if val is not None:
                states[gid].total += float(val)
```

SQL: `SELECT vgi_sum_all(a, b, c) FROM t GROUP BY category`

## Dynamic Output Type (ANY)

For aggregate functions where the output type depends on the input, use `Returns()` without an arrow type and override `on_bind()`:

```python
class GenericSum(AggregateFunction[GenericSumState]):
    @classmethod
    def on_bind(cls, params, **kwargs):
        if params.bind_call and params.bind_call.input_schema:
            input_type = params.bind_call.input_schema.field(0).type
            return BindResponse(output_schema=pa.schema([("result", input_type)]))
        return BindResponse(output_schema=pa.schema([("result", pa.float64())]))

    @classmethod
    def finalize(cls, group_ids, states, params) -> Annotated[pa.RecordBatch, Returns()]:
        output_type = params.output_schema.field(0).type if params.output_schema else pa.float64()
        results = [states[gid.as_py()].total for gid in group_ids]
        return pa.record_batch({"result": pa.array(results, type=output_type)})
```

## Lifecycle

```
1. BIND (once per query)
   └─ on_bind() → output_schema + execution_id
   └─ const args stored in FunctionStorage at group_id=-2

2. UPDATE (per batch, possibly parallel)
   └─ C++ assigns group_ids from shared atomic counter
   └─ States loaded from FunctionStorage (or created via initial_state())
   └─ update() called with states dict + column arrays
   └─ Modified states saved back to FunctionStorage

3. COMBINE (merge parallel results)
   └─ Source + target states loaded from FunctionStorage
   └─ combine() merges source into target
   └─ Target state saved, source state removed

4. FINALIZE (produce results)
   └─ States loaded from FunctionStorage
   └─ finalize() returns RecordBatch with one row per group_id

5. DESTRUCTOR (cleanup)
   └─ Called once when all states have been destroyed
   └─ Clears FunctionStorage for this execution_id
```

## Registration

Register aggregate functions in your worker alongside scalar and table functions:

```python
worker = Worker(
    functions=[
        SumFunction,
        AvgFunction,
        # ... other functions
    ],
)
```

The framework automatically detects `AggregateFunction` subclasses and registers them with the correct function type in the catalog.

## Example Functions

See `vgi/examples/aggregate.py` for complete implementations:

| Function | Demonstrates |
|----------|-------------|
| `CountFunction` | Nullary aggregate (no inputs), `NullHandling.SPECIAL` |
| `SumFunction` | Single input, basic grouping |
| `AvgFunction` | Multi-field state (sum + count) |
| `WeightedSumFunction` | Multiple input columns |
| `ListAggFunction` | Order-dependent aggregate |
| `PercentileFunction` | `ConstParam` with `phase="finalize"` |
| `GenericSumFunction` | ANY type, dynamic output via `on_bind()` |
| `SumAllFunction` | Varargs aggregate |
