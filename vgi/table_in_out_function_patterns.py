"""Specialized base classes for common table function patterns.

This module provides abstract base classes that simplify implementing common
data processing patterns. Each class handles the boilerplate for its pattern,
letting you focus on the core logic.

AVAILABLE PATTERNS
------------------
AggregationFunction
    Reduces all input batches to a summary output (e.g., sum, count, mean).
    Supports distributed aggregation across multiple workers.

FilterFunction
    Filters rows based on a boolean predicate.
    Stateless and inherently parallelizable.

MapFunction
    Transforms columns independently per row.
    Stateless and inherently parallelizable.

CHOOSING A PATTERN
------------------
Use AggregationFunction when:
    - You need to see all data before producing output
    - Output is a summary (fewer rows than input, often just one)
    - Examples: sum, count, mean, min/max, histogram

Use FilterFunction when:
    - You're removing rows that don't match criteria
    - Output schema is same as input schema
    - Examples: WHERE clauses, null filtering, range filtering

Use MapFunction when:
    - You're transforming column values row-by-row
    - Each output row depends only on its input row
    - Examples: type conversion, arithmetic, string manipulation

For other patterns, use TableInOutFunction directly.

"""

from abc import abstractmethod
from typing import final

import pyarrow as pa
import pyarrow.compute as pc

from vgi.ipc_utils import RecordBatchState
from vgi.log import Level
from vgi.table_in_out_function import TableInOutFunction

__all__ = [
    "AggregationFunction",
    "FilterFunction",
    "MapFunction",
]


class AggregationFunction(TableInOutFunction):
    """Base class for aggregation functions that reduce input to summary output.

    Aggregation functions collect data from all input batches and produce a
    summary result (typically a single row). This class handles the accumulation
    pattern and supports distributed aggregation across multiple workers.

    METHODS TO OVERRIDE
    -------------------
    output_schema -> pa.Schema (property, required)
        Define the schema of the output batch.

    state_schema -> pa.Schema (property, required)
        Define the schema for partial state batches used in distributed mode.
        This schema describes the intermediate state that gets saved/loaded
        between workers.

    accumulate(batch: pa.RecordBatch) -> None (required)
        Process an input batch and update internal state. Called once per
        input batch. Use Arrow compute functions to update state efficiently.

    get_accumulated_state() -> pa.RecordBatch (required)
        Return current accumulated state as a RecordBatch conforming to
        state_schema. Called when saving state for distributed aggregation.

    merge_accumulated_states(states: pa.Table) -> None (required)
        Merge partial states from all workers (including this one) into
        this instance's state. The states parameter is a Table containing
        all partial state batches concatenated together.

    compute_result() -> pa.RecordBatch (required)
        Compute the final output from accumulated state. Called once during
        finalization after all states have been merged.

    DISTRIBUTED AGGREGATION
    -----------------------
    When max_processes() > 1, the aggregation runs in parallel:

    1. Each worker calls accumulate() for its assigned batches
    2. Each worker's state is saved via get_accumulated_state()
    3. Primary worker loads all states and calls merge_accumulated_states()
    4. Primary worker calls compute_result() to produce final output

    For single-worker mode, the flow is simpler:
    1. accumulate() called for all batches
    2. compute_result() called to produce output

    LOGGING
    -------
    Use self.log(Level, message) to emit log messages:

        def accumulate(self, batch: pa.RecordBatch) -> None:
            self.log(Level.DEBUG, f"Processing {batch.num_rows} rows")
            ...

    Examples
    --------
    Sum all numeric columns:

        class SumColumns(AggregationFunction):
            def __init__(self, invocation: Invocation, logger):
                super().__init__(invocation, logger)
                self._sums: dict[str, pa.Scalar] = {}

            @property
            def output_schema(self) -> pa.Schema:
                # Promote integers to int64, floats to float64
                fields = []
                for field in self.input_schema:
                    if pa.types.is_integer(field.type):
                        fields.append(pa.field(field.name, pa.int64()))
                    elif pa.types.is_floating(field.type):
                        fields.append(pa.field(field.name, pa.float64()))
                return pa.schema(fields)

            @property
            def state_schema(self) -> pa.Schema:
                # State has same schema as output
                return self.output_schema

            def accumulate(self, batch: pa.RecordBatch) -> None:
                for field in self.output_schema:
                    col_sum = pc.sum(batch.column(field.name))
                    if field.name in self._sums:
                        self._sums[field.name] = pc.add(
                            self._sums[field.name], col_sum
                        )
                    else:
                        self._sums[field.name] = col_sum

            def get_accumulated_state(self) -> pa.RecordBatch:
                return pa.RecordBatch.from_pydict(
                    {name: [scalar.as_py()] for name, scalar in self._sums.items()},
                    schema=self.state_schema,
                )

            def merge_accumulated_states(self, states: pa.Table) -> None:
                self._sums = {}
                for field in self.output_schema:
                    self._sums[field.name] = pc.sum(states.column(field.name))

            def compute_result(self) -> pa.RecordBatch:
                return pa.RecordBatch.from_pydict(
                    {name: [scalar.as_py()] for name, scalar in self._sums.items()},
                    schema=self.output_schema,
                )

    """

    @property
    @abstractmethod
    def state_schema(self) -> pa.Schema:
        """Schema for partial state batches used in distributed aggregation.

        This schema defines the structure of batches returned by
        get_accumulated_state() and consumed by merge_accumulated_states().

        Returns:
            Arrow schema for the intermediate state representation.

        """
        ...

    @abstractmethod
    def accumulate(self, batch: pa.RecordBatch) -> None:
        """Accumulate data from an input batch into internal state.

        Called once for each input batch during the processing phase.
        Update instance attributes to track accumulated values.

        Args:
            batch: Input RecordBatch to accumulate. Conforms to input_schema.

        Example:
            def accumulate(self, batch: pa.RecordBatch) -> None:
                col_sum = pc.sum(batch.column("value"))
                if col_sum.is_valid:
                    self._total = pc.add(self._total, col_sum)

        """
        ...

    @abstractmethod
    def get_accumulated_state(self) -> pa.RecordBatch:
        """Return current accumulated state as a RecordBatch.

        Called when saving state for distributed aggregation. The returned
        batch must conform to state_schema.

        Returns:
            RecordBatch containing current accumulated state.

        Example:
            def get_accumulated_state(self) -> pa.RecordBatch:
                return pa.RecordBatch.from_pydict(
                    {"total": [self._total.as_py()]},
                    schema=self.state_schema,
                )

        """
        ...

    @abstractmethod
    def merge_accumulated_states(self, states: pa.Table) -> None:
        """Merge partial states from all workers into this instance's state.

        Called on the primary worker during finalization. The states Table
        contains one row per worker (including this one). Update instance
        state to reflect the merged result.

        Args:
            states: Table containing all partial states concatenated.
                    Schema matches state_schema.

        Example:
            def merge_accumulated_states(self, states: pa.Table) -> None:
                self._total = pc.sum(states.column("total"))

        """
        ...

    @abstractmethod
    def compute_result(self) -> pa.RecordBatch:
        """Compute final output from accumulated state.

        Called once during finalization after all states have been merged.
        Returns the final aggregation result.

        Returns:
            RecordBatch containing the aggregation result.
            Must conform to output_schema.

        Example:
            def compute_result(self) -> pa.RecordBatch:
                return pa.RecordBatch.from_pydict(
                    {"total": [self._total.as_py()]},
                    schema=self.output_schema,
                )

        """
        ...

    @final
    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Accumulate batch data. Do not override.

        Delegates to accumulate() and returns an empty batch.
        Override accumulate() to implement your accumulation logic.

        """
        self.log(Level.DEBUG, f"Accumulating {batch.num_rows} rows")
        self.accumulate(batch)
        return self.empty_output_batch

    @final
    def save_state(self) -> RecordBatchState | None:
        """Save accumulated state for distributed aggregation. Do not override.

        Delegates to get_accumulated_state() and wraps in RecordBatchState.

        """
        state_batch = self.get_accumulated_state()
        if state_batch.num_rows == 0:
            return None
        return RecordBatchState(batch=state_batch)

    @final
    def load_states(self, states: list[RecordBatchState]) -> None:
        """Load and merge states from all workers. Do not override.

        Concatenates all state batches into a Table and delegates to
        merge_accumulated_states().

        """
        if not states:
            return
        state_table = pa.Table.from_batches(
            [s.batch for s in states],
            schema=self.state_schema,
        )
        self.log(
            Level.DEBUG,
            f"Merging {len(states)} partial states ({state_table.num_rows} rows)",
        )
        self.merge_accumulated_states(state_table)

    @final
    def finish(self) -> list[pa.RecordBatch]:
        """Compute and return final result. Do not override.

        Delegates to compute_result() and wraps the result in a list.

        """
        result = self.compute_result()
        self.log(Level.DEBUG, f"Aggregation complete: {result.num_rows} output rows")
        return [result]


class FilterFunction(TableInOutFunction):
    """Base class for row filtering functions.

    Filter functions evaluate a boolean predicate for each row and keep only
    rows where the predicate is True. The output schema is identical to the
    input schema.

    This class is stateless and inherently parallelizable. Multiple workers
    can filter batches independently.

    METHODS TO OVERRIDE
    -------------------
    predicate(batch: pa.RecordBatch) -> pa.Array (required)
        Return a boolean Array indicating which rows to keep.
        True = keep row, False = drop row.

    LOGGING
    -------
    Filtering statistics are logged automatically at DEBUG level.
    Use self.log(Level, message) for additional logging:

        def predicate(self, batch: pa.RecordBatch) -> pa.Array:
            self.log(Level.INFO, "Applying custom filter")
            return pc.greater(batch.column("value"), 0)

    Examples
    --------
    Keep rows where value is positive:

        class PositiveFilter(FilterFunction):
            def predicate(self, batch: pa.RecordBatch) -> pa.Array:
                return pc.greater(batch.column("value"), 0)

    Keep rows where name is not null:

        class NotNullFilter(FilterFunction):
            column_name = Arg[str](0)

            def predicate(self, batch: pa.RecordBatch) -> pa.Array:
                return pc.is_valid(batch.column(self.column_name))

    Combine multiple conditions:

        class RangeFilter(FilterFunction):
            min_val = Arg[int](0)
            max_val = Arg[int](1)

            def predicate(self, batch: pa.RecordBatch) -> pa.Array:
                col = batch.column("value")
                above_min = pc.greater_equal(col, self.min_val)
                below_max = pc.less_equal(col, self.max_val)
                return pc.and_(above_min, below_max)

    """

    @abstractmethod
    def predicate(self, batch: pa.RecordBatch) -> pa.Array:
        """Return boolean array indicating which rows to keep.

        Args:
            batch: Input RecordBatch to evaluate.

        Returns:
            Boolean Array with same length as batch.
            True = keep row, False = drop row.

        Example:
            def predicate(self, batch: pa.RecordBatch) -> pa.Array:
                # Keep rows where 'status' equals 'active'
                return pc.equal(batch.column("status"), "active")

        """
        ...

    @final
    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Apply predicate filter to batch. Do not override.

        Evaluates predicate() and filters the batch accordingly.
        Logs filtering statistics at DEBUG level.

        """
        mask = self.predicate(batch)
        result = pc.filter(batch, mask)

        # Calculate and log filtering stats
        # pc.sum on boolean array counts True values
        kept_scalar = pc.sum(mask)
        kept = kept_scalar.as_py() if kept_scalar.is_valid else 0
        dropped = batch.num_rows - kept

        if dropped > 0:
            self.log(
                Level.DEBUG,
                f"Filtered batch: {kept} rows kept, {dropped} rows dropped",
            )

        # pc.filter returns a ChunkedArray for RecordBatch, need to handle this
        if isinstance(result, pa.ChunkedArray):
            # This shouldn't happen with RecordBatch input, but handle it
            result = result.combine_chunks()

        return result


class MapFunction(TableInOutFunction):
    """Base class for column transformation functions.

    Map functions transform column values independently per row. Each output
    row depends only on its corresponding input row. The output schema is
    the same as the input schema (columns are transformed in place).

    This class is stateless and inherently parallelizable. Multiple workers
    can transform batches independently.

    METHODS TO OVERRIDE
    -------------------
    map_columns(batch: pa.RecordBatch) -> dict[str, pa.Array] (required)
        Return a dictionary mapping column names to transformed arrays.
        Only include columns that are being modified.

    output_schema -> pa.Schema (optional)
        Override if transformed columns have different types than input.
        Default returns input_schema unchanged.

    LOGGING
    -------
    Use self.log(Level, message) to emit log messages:

        def map_columns(self, batch: pa.RecordBatch) -> dict[str, pa.Array]:
            self.log(Level.DEBUG, f"Transforming {batch.num_rows} rows")
            return {"value": pc.multiply(batch.column("value"), 2)}

    Examples
    --------
    Double all values in a column:

        class DoubleValues(MapFunction):
            def map_columns(self, batch: pa.RecordBatch) -> dict[str, pa.Array]:
                return {"value": pc.multiply(batch.column("value"), 2)}

    Convert string column to uppercase:

        class UpperCase(MapFunction):
            def map_columns(self, batch: pa.RecordBatch) -> dict[str, pa.Array]:
                return {"name": pc.utf8_upper(batch.column("name"))}

    Transform multiple columns:

        class NormalizeData(MapFunction):
            def map_columns(self, batch: pa.RecordBatch) -> dict[str, pa.Array]:
                return {
                    "price": pc.multiply(batch.column("price"), 100),  # to cents
                    "name": pc.utf8_trim(batch.column("name")),
                }

    Change column type (requires output_schema override):

        class CastToFloat(MapFunction):
            @property
            def output_schema(self) -> pa.Schema:
                # Change 'value' from int64 to float64
                return pa.schema([
                    pa.field("id", pa.int64()),
                    pa.field("value", pa.float64()),
                ])

            def map_columns(self, batch: pa.RecordBatch) -> dict[str, pa.Array]:
                return {"value": batch.column("value").cast(pa.float64())}

    """

    @abstractmethod
    def map_columns(self, batch: pa.RecordBatch) -> dict[str, pa.Array]:
        """Return dictionary mapping column names to transformed arrays.

        Only include columns that are being modified. Columns not in the
        returned dictionary are passed through unchanged.

        Args:
            batch: Input RecordBatch to transform.

        Returns:
            Dictionary mapping column names to transformed pa.Array values.
            Each array must have the same length as the input batch.

        Example:
            def map_columns(self, batch: pa.RecordBatch) -> dict[str, pa.Array]:
                return {
                    "value": pc.multiply(batch.column("value"), 2),
                    "name": pc.utf8_lower(batch.column("name")),
                }

        """
        ...

    @final
    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Apply column transformations to batch. Do not override.

        Evaluates map_columns() and updates the specified columns.
        Columns not returned by map_columns() are passed through unchanged.

        """
        updates = self.map_columns(batch)

        if not updates:
            return batch

        result = batch
        for name, array in updates.items():
            idx = result.schema.get_field_index(name)
            # Get the field from output_schema to handle type changes
            output_field = self.output_schema.field(
                self.output_schema.get_field_index(name)
            )
            result = result.set_column(idx, output_field, array)

        return result
