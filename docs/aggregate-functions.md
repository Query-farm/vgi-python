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

States are serialized to bytes and stored in `FunctionStorage` between RPC calls. The
framework requires exactly two things of a state type — `serialize_to_bytes()` and
`deserialize_from_bytes()`, the [`StreamStateCodec`][vgi.function.StreamStateCodec]
protocol — and treats the bytes as opaque.

The usual answer is a `dataclass` extending `ArrowSerializableDataclass`, which writes
both methods for you. Each field needs an `ArrowType` annotation for serialization:

```python
from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType


@dataclass(kw_only=True)
class AvgState(ArrowSerializableDataclass):
    total: Annotated[float, ArrowType(pa.float64())] = 0.0
    count: Annotated[int, ArrowType(pa.int64())] = 0
```

Use simple scalar types (int, float, str, bytes) for efficient serialization.

You are not required to go through Arrow. A state that is a couple of integers pays for
a schema message, a batch message and an end-of-stream marker on every group, every
batch — packing it yourself is both smaller and faster, and it lets a Python worker match
the state encoding of a sibling VGI implementation in another language:

```python
import struct
from dataclasses import dataclass
from typing import ClassVar


@dataclass(kw_only=True)
class SumState:
    """Same two counters, packed as little-endian int64s."""

    total: int = 0
    count: int = 0

    _STRUCT: ClassVar[struct.Struct] = struct.Struct("<qq")

    def serialize_to_bytes(self) -> bytes:
        return self._STRUCT.pack(self.total, self.count)

    @classmethod
    def deserialize_from_bytes(cls, data: bytes) -> "SumState":
        total, count = cls._STRUCT.unpack(data)
        return cls(total=total, count=count)
```

The codec must round-trip exactly: `T.deserialize_from_bytes(s.serialize_to_bytes())` has
to equal `s` for every state the aggregate can produce, including `initial_state()`. The
framework cannot check that, and a lossy codec surfaces as wrong aggregate results rather
than as an error. A `TState` that has neither method is rejected at class-definition time.

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

## Invoking From Python

Real users reach aggregates through DuckDB, which drives the all-unary aggregate RPCs from its hash-aggregate operator. Non-DuckDB callers drive the same protocol through `Client`.

`aggregate_function()` is the convenience shape — it keys the groups client-side, pumps every batch through `update`, and finalizes in chunks:

```python test="skip"
import pyarrow as pa

from vgi.client import Client

batch = pa.RecordBatch.from_pydict({"cat": ["a", "b", "a"], "value": [1, 10, 2]})

with Client("vgi-fixture-worker") as client:
    result = client.aggregate_function(
        function_name="vgi_sum",
        schema_name="main",
        input=[batch],
        group_by=["cat"],
    )
    # {'cat': ['a', 'b'], 'result': [3, 10]}
```

Every input column not named in `group_by` is passed to the aggregate as a value column, in batch order — so order the columns to match the declared `Param`s. Omitting `group_by` gives the global-aggregate shape (`SELECT vgi_sum(x) FROM t`), which returns one row even for empty input.

For the raw protocol — caller-allocated group ids, `combine`, and the optional window RPCs — use `aggregate_session()`:

```python test="skip"
with Client("vgi-fixture-worker") as client:
    with client.aggregate_session(
        function_name="vgi_sum",
        schema_name="main",
        input_schema=pa.schema([pa.field("value", pa.int64())]),
    ) as session:
        session.update(
            group_ids=[0, 0, 1],
            batch=pa.RecordBatch.from_pydict({"value": [1, 2, 100]}),
        )
        session.combine(source_group_ids=[1], target_group_ids=[0])
        session.finalize([0, 1])  # -> {'result': [103, 100]}
```

The session is destroyed on exit, so worker-side group state never outlives the `with` block. Window-capable aggregates add `window_init()` / `window()` / `window_batch()` / `window_destroy()` on the same session; streaming-partitioned aggregates use `client.aggregate_streaming(...)` instead (see below).

## Streaming-Partitioned Variant

For `OVER (PARTITION BY ... ORDER BY ...)` queries against unbounded inputs (e.g. running aggregates across years of trade history), the standard windowed path materializes each partition in DuckDB memory before the aggregate sees it — fine for bounded data, OOMs at scale.

The `streaming_partitioned` opt-in routes those queries through a custom physical operator in the VGI DuckDB extension: input chunks pipe directly to the worker, the worker maintains concurrent per-partition state in a hash map keyed by partition tuple, and each input chunk produces a same-length output array of cumulative snapshots. No DuckDB-side partition materialization; memory is bounded by `partitions × state_per_partition`, not by row count.

```python
class MyRunningAgg(AggregateFunction[MyState]):
    class Meta:
        name = "my_running_agg"
        streaming_partitioned = True   # opt-in
        # supports_window may also be set; the optimizer chooses the
        # streaming path for eligible queries and falls back to the
        # windowed path otherwise.

    @classmethod
    def streaming_open(cls, params: ProcessParams[None]) -> dict[str, Any]:
        # Build cross-partition session state. Returned object lives in
        # an in-process cache for the duration of the session and is
        # also persisted to FunctionStorage so chunk RPCs landing on a
        # different pool worker can rehydrate.
        return {"partition_states": {}}

    @classmethod
    def streaming_chunk(
        cls,
        chunk: pa.RecordBatch,
        streaming_state: dict[str, Any],
        partition_key_count: int,
        order_key_count: int,
        params: ProcessParams[None],
    ) -> pa.Array:
        # Column layout in `chunk`:
        #   [partition_key_cols..., order_key_cols..., value_cols...]
        # Return one output value per input row (cumulative snapshot
        # at that row's position in its partition's order).
        ...

    @classmethod
    def streaming_close(cls, streaming_state, params) -> None:
        # Cleanup hook (called once per session). Default: no-op.
        ...
```

**Eligibility for the streaming path** is decided by the extension's optimizer rule and requires:

- `streaming_partitioned = True` on the function's Meta.
- A cumulative frame: `ROWS/RANGE/GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` (or the implicit cumulative frame DuckDB emits when only `ORDER BY` is given).
- No `EXCLUDE`, `DISTINCT`, `FILTER (WHERE ...)`, or aggregate-arg `ORDER BY`.
- The worker function declares no const-arg parameters (v1 limitation).

Queries that don't satisfy all of these fall back to the standard windowed path automatically. The streaming path is opt-in and additive — it does not replace `update`/`combine`/`finalize`, which still service `GROUP BY` queries normally.

From Python, `Client.aggregate_streaming()` opens the session and closes it on exit. The chunk schema is positional: partition keys first, then order keys, then value columns.

```python test="skip"
schema = pa.schema(
    [pa.field("k", pa.string()), pa.field("ts", pa.int64()), pa.field("v", pa.int64())]
)

with Client("vgi-fixture-worker") as client:
    with client.aggregate_streaming(
        function_name="vgi_streaming_sum",
        schema_name="main",
        input_schema=schema,
        partition_key_count=1,
        order_key_count=1,
    ) as session:
        chunk = pa.RecordBatch.from_pydict(
            {"k": ["a", "b", "a", "b"], "ts": [1, 1, 2, 2], "v": [1, 10, 2, 20]},
            schema=schema,
        )
        session.chunk(chunk)  # -> {'result': [1, 10, 3, 30]}
```

**When pre-aggregation is the better answer.** For most analytics shapes — "EOD positions per book per day, carrying forward across days" — pre-aggregating the input is the cleanest pattern in plain SQL:

```sql
WITH per_period_net AS (
  SELECT book, period_key, symbol, SUM(quantity) AS quantity
  FROM trades GROUP BY book, period_key, symbol
)
SELECT book, period_key,
       my_running_agg(symbol, quantity)
         OVER (PARTITION BY book ORDER BY period_key) AS running
FROM per_period_net;
```

The pre-aggregate collapses fills within each period before the OVER sees them, so the per-row output cardinality of the OVER matches the user's actual intent. The streaming path is the right tool when pre-aggregation isn't viable: per-fill running views, very high symbol cardinality per partition, or aggregates whose state isn't algebraically reducible by a pre-aggregate.

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
