"""make_pairs_*, repeat_value_*, and constant_columns generators."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, Transient
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table._common import (
    _cardinality_from_count,
)
from vgi.arguments import Arg
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.schema_utils import schema
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)


@dataclass(slots=True, frozen=True)
class ConstantColumnsFunctionArguments:
    """Arguments for ConstantColumnsFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]
    values: Annotated[
        tuple[Any, ...],
        Arg(
            1,
            varargs=True,
            doc="Values to fill each column (at least one required)",
        ),
    ]


@dataclass(kw_only=True)
class ConstantColumnsState(ArrowSerializableDataclass):
    """Mutable state for ConstantColumnsFunction."""

    remaining: int
    full_batch: Annotated[pa.RecordBatch | None, Transient()] = field(repr=False, default=None)


@init_single_worker
@_cardinality_from_count
class ConstantColumnsFunction(TableFunctionGenerator[ConstantColumnsFunctionArguments, ConstantColumnsState]):
    """Generates a table with constant values in each column based on varargs.

    USE CASE
    --------
    Demonstrates varargs with AnyArrow type where the output schema is
    determined by the types of the values provided. Each vararg value
    becomes a column filled with that constant value for all rows.

    This shows how varargs can accept mixed types and produce a dynamic
    output schema based on the argument types.

    SCHEMA
    ------
    Output schema is dynamic based on the types of provided values.
    Column names are auto-generated as col_0, col_1, col_2, etc.

    Example: constant_columns(3, 42, 'hello', 3.14)
    Output schema: {"col_0": int64, "col_1": string, "col_2": double}

    Example:
    -------
    SELECT * FROM constant_columns(3, 42, 'hello')
    Returns: [{"col_0": 42, "col_1": "hello"},
              {"col_0": 42, "col_1": "hello"},
              {"col_0": 42, "col_1": "hello"}]

    SELECT * FROM constant_columns(2, 1, 2, 3, 'apple')
    Returns: [{"col_0": 1, "col_1": 2, "col_2": 3, "col_3": "apple"},
              {"col_0": 1, "col_1": 2, "col_2": 3, "col_3": "apple"}]

    """

    class Meta:
        """Metadata for ConstantColumnsFunction."""

        name = "constant_columns"
        description = "Generates rows with constant values from varargs"
        categories = ["generator", "utility"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM constant_columns(5, 42, 'hello')",
                description="Generate 5 rows with columns containing 42 and 'hello'",
            ),
            FunctionExample(
                sql="SELECT * FROM constant_columns(3, 1, 2, 3, 'test')",
                description="Generate 3 rows with 4 columns of mixed types",
            ),
        ]

    BATCH_SIZE: ClassVar[int] = 2048

    @classmethod
    def on_bind(cls, params: BindParams[ConstantColumnsFunctionArguments]) -> BindResponse:
        """Return output schema with one column per vararg, typed by value."""
        return BindResponse(output_schema=schema({f"col_{i}": v.type for i, v in enumerate(params.args.values)}))

    @classmethod
    def initial_state(cls, params: ProcessParams[ConstantColumnsFunctionArguments]) -> ConstantColumnsState:
        """Create initial state with pre-built full batch."""
        arrays = [pa.repeat(scalar, cls.BATCH_SIZE) for scalar in params.args.values]
        full_batch = pa.RecordBatch.from_arrays(arrays, schema=params.output_schema)
        return ConstantColumnsState(remaining=params.args.count, full_batch=full_batch)

    @classmethod
    def process(
        cls,
        params: ProcessParams[ConstantColumnsFunctionArguments],
        state: ConstantColumnsState,
        out: OutputCollector,
    ) -> None:
        """Generate rows with constant values in each column."""
        if state.remaining <= 0:
            out.finish()
            return

        if state.full_batch is None:
            arrays = [pa.repeat(scalar, cls.BATCH_SIZE) for scalar in params.args.values]
            state.full_batch = pa.RecordBatch.from_arrays(arrays, schema=params.output_schema)
        if state.remaining >= cls.BATCH_SIZE:
            out.emit(state.full_batch)
            state.remaining -= cls.BATCH_SIZE
        else:
            out.emit(state.full_batch.slice(0, state.remaining))
            state.remaining = 0


# ============================================================================

MAKE_PAIRS_INT_SCHEMA = schema(a=pa.int64(), b=pa.int64())
MAKE_PAIRS_STR_SCHEMA = schema(a=pa.string(), b=pa.string())


@dataclass(kw_only=True)
class MakePairsIntArgs:
    """Arguments for integer make_pairs."""

    start: Annotated[int, Arg(0, doc="Start value")]
    stop: Annotated[int, Arg(1, doc="Stop value")]


@dataclass(kw_only=True)
class MakePairsStrArgs:
    """Arguments for string make_pairs."""

    prefix: Annotated[str, Arg(0, doc="Prefix for column a")]
    suffix: Annotated[str, Arg(1, doc="Suffix for column b")]


@dataclass(kw_only=True)
class MakePairsIntState(ArrowSerializableDataclass):
    """State for integer make_pairs."""

    a_vals: list[int] = field(default_factory=list)
    b_vals: list[int] = field(default_factory=list)
    done: bool = False


@dataclass(kw_only=True)
class MakePairsStrState(ArrowSerializableDataclass):
    """State for string make_pairs."""

    a_vals: list[str] = field(default_factory=list)
    b_vals: list[str] = field(default_factory=list)
    done: bool = False


@init_single_worker
@bind_fixed_schema
class MakePairsIntFunction(TableFunctionGenerator[MakePairsIntArgs, MakePairsIntState]):
    """Generate integer pairs (i, i*2) from start to stop-1.

    Example:
        SELECT * FROM make_pairs(1, 4)
        Returns: (1,2), (2,4), (3,6)

    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MAKE_PAIRS_INT_SCHEMA

    class Meta:
        """Function metadata."""

        name = "make_pairs"
        description = "Generate integer pairs (i, i*2)"

    @classmethod
    def initial_state(cls, params: ProcessParams[MakePairsIntArgs]) -> MakePairsIntState:
        """Build integer pairs."""
        vals = list(range(params.args.start, params.args.stop))
        return MakePairsIntState(a_vals=vals, b_vals=[v * 2 for v in vals])

    @classmethod
    def process(cls, params: ProcessParams[MakePairsIntArgs], state: MakePairsIntState, out: OutputCollector) -> None:
        """Emit pairs batch."""
        if state.done:
            out.finish()
            return
        state.done = True
        out.emit(pa.RecordBatch.from_pydict({"a": state.a_vals, "b": state.b_vals}, schema=MAKE_PAIRS_INT_SCHEMA))


@init_single_worker
@bind_fixed_schema
class MakePairsStrFunction(TableFunctionGenerator[MakePairsStrArgs, MakePairsStrState]):
    """Generate string pairs (prefix+i, suffix+i) for i in 0..4.

    Example:
        SELECT * FROM make_pairs('row_', '_end')
        Returns: ('row_0','_end0'), ('row_1','_end1'), ...

    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MAKE_PAIRS_STR_SCHEMA

    class Meta:
        """Function metadata."""

        name = "make_pairs"
        description = "Generate string pairs with prefix and suffix"

    @classmethod
    def initial_state(cls, params: ProcessParams[MakePairsStrArgs]) -> MakePairsStrState:
        """Build string pairs."""
        return MakePairsStrState(
            a_vals=[f"{params.args.prefix}{i}" for i in range(5)],
            b_vals=[f"{params.args.suffix}{i}" for i in range(5)],
        )

    @classmethod
    def process(cls, params: ProcessParams[MakePairsStrArgs], state: MakePairsStrState, out: OutputCollector) -> None:
        """Emit pairs batch."""
        if state.done:
            out.finish()
            return
        state.done = True
        out.emit(pa.RecordBatch.from_pydict({"a": state.a_vals, "b": state.b_vals}, schema=MAKE_PAIRS_STR_SCHEMA))


# ============================================================================

MAKE_PAIRS_MIXED_SCHEMA = pa.schema(
    [("a", pa.int64()), ("b", pa.string())]  # type: ignore[arg-type]  # PyArrow mixed-type tuple typing
)


@dataclass(kw_only=True)
class MakePairsIntStrArgs:
    """Arguments for mixed-type make_pairs."""

    start: Annotated[int, Arg(0, doc="Start integer value")]
    label: Annotated[str, Arg(1, doc="Label prefix for string column")]


@dataclass(kw_only=True)
class MakePairsIntStrState(ArrowSerializableDataclass):
    """State for mixed-type make_pairs."""

    a_vals: list[int] = field(default_factory=list)
    b_vals: list[str] = field(default_factory=list)
    done: bool = False


@init_single_worker
@bind_fixed_schema
class MakePairsIntStrFunction(TableFunctionGenerator[MakePairsIntStrArgs, MakePairsIntStrState]):
    """Generate mixed int/string pairs (start+i, label+str(i)) for i in 0..4.

    Example:
        SELECT * FROM make_pairs(10, 'item_')
        Returns: (10, 'item_0'), (11, 'item_1'), ..., (14, 'item_4')

    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MAKE_PAIRS_MIXED_SCHEMA

    class Meta:
        """Function metadata."""

        name = "make_pairs"
        description = "Generate mixed int/string pairs"

    @classmethod
    def initial_state(cls, params: ProcessParams[MakePairsIntStrArgs]) -> MakePairsIntStrState:
        """Build mixed-type pairs."""
        return MakePairsIntStrState(
            a_vals=[params.args.start + i for i in range(5)],
            b_vals=[f"{params.args.label}{i}" for i in range(5)],
        )

    @classmethod
    def process(
        cls, params: ProcessParams[MakePairsIntStrArgs], state: MakePairsIntStrState, out: OutputCollector
    ) -> None:
        """Emit pairs batch."""
        if state.done:
            out.finish()
            return
        state.done = True
        out.emit(pa.RecordBatch.from_pydict({"a": state.a_vals, "b": state.b_vals}, schema=MAKE_PAIRS_MIXED_SCHEMA))


# ============================================================================


@dataclass(kw_only=True)
class RepeatValueIntArgs:
    """Arguments for integer repeat_value."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate")]
    values: Annotated[list[int], Arg(1, varargs=True, arrow_type=pa.int64(), doc="Integer values to repeat")]


@dataclass(kw_only=True)
class RepeatValueStrArgs:
    """Arguments for string repeat_value."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate")]
    values: Annotated[list[str], Arg(1, varargs=True, arrow_type=pa.string(), doc="String values to repeat")]


@dataclass(kw_only=True)
class RepeatValueIntState(ArrowSerializableDataclass):
    """State for integer repeat_value."""

    rows: list[list[int]] = field(default_factory=list)
    done: bool = False


@dataclass(kw_only=True)
class RepeatValueStrState(ArrowSerializableDataclass):
    """State for string repeat_value."""

    rows: list[list[str]] = field(default_factory=list)
    done: bool = False


@init_single_worker
class RepeatValueIntFunction(TableFunctionGenerator[RepeatValueIntArgs, RepeatValueIntState]):
    """Repeat integer values for count rows.

    Example:
        SELECT * FROM repeat_value(3, 10, 20)
        Returns 3 rows with columns v0=10, v1=20

    """

    class Meta:
        """Function metadata."""

        name = "repeat_value"
        description = "Repeat integer values for N rows"

    @classmethod
    def on_bind(cls, params: BindParams[RepeatValueIntArgs]) -> BindResponse:
        """Build output schema from varargs count."""
        num_values = len(params.args.values)
        fields = [pa.field(f"v{i}", pa.int64()) for i in range(num_values)]
        return BindResponse(output_schema=pa.schema(fields))

    @classmethod
    def initial_state(cls, params: ProcessParams[RepeatValueIntArgs]) -> RepeatValueIntState:
        """Build repeated rows."""
        return RepeatValueIntState(
            rows=[[v] * params.args.count for v in params.args.values],
        )

    @classmethod
    def process(
        cls, params: ProcessParams[RepeatValueIntArgs], state: RepeatValueIntState, out: OutputCollector
    ) -> None:
        """Emit repeated values."""
        if state.done:
            out.finish()
            return
        state.done = True
        data = {f"v{i}": col for i, col in enumerate(state.rows)}
        out_schema = schema({f"v{i}": pa.int64() for i in range(len(state.rows))})
        out.emit(pa.RecordBatch.from_pydict(data, schema=out_schema))


@init_single_worker
class RepeatValueStrFunction(TableFunctionGenerator[RepeatValueStrArgs, RepeatValueStrState]):
    """Repeat string values for count rows.

    Example:
        SELECT * FROM repeat_value(3, 'a', 'b')
        Returns 3 rows with columns v0='a', v1='b'

    """

    class Meta:
        """Function metadata."""

        name = "repeat_value"
        description = "Repeat string values for N rows"

    @classmethod
    def on_bind(cls, params: BindParams[RepeatValueStrArgs]) -> BindResponse:
        """Build output schema from varargs count."""
        num_values = len(params.args.values)
        fields = [pa.field(f"v{i}", pa.string()) for i in range(num_values)]
        return BindResponse(output_schema=pa.schema(fields))

    @classmethod
    def initial_state(cls, params: ProcessParams[RepeatValueStrArgs]) -> RepeatValueStrState:
        """Build repeated rows."""
        return RepeatValueStrState(
            rows=[[v] * params.args.count for v in params.args.values],
        )

    @classmethod
    def process(
        cls, params: ProcessParams[RepeatValueStrArgs], state: RepeatValueStrState, out: OutputCollector
    ) -> None:
        """Emit repeated values."""
        if state.done:
            out.finish()
            return
        state.done = True
        data = {f"v{i}": col for i, col in enumerate(state.rows)}
        out_schema = schema({f"v{i}": pa.string() for i in range(len(state.rows))})
        out.emit(pa.RecordBatch.from_pydict(data, schema=out_schema))
