"""Extended bind results for table functions with cardinality hints.

This module provides:
- CardinalityInfo: Row count estimates for query optimization
- TableFunctionBindResult: BindResult subclass with cardinality support
- TableFunction: Base class for table functions with cardinality

These classes are used by TableInOutFunction and can be used directly
for custom table function implementations.
"""

from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import structlog

import vgi.function
import vgi.util

__all__ = ["TableFunctionBindResult", "CardinalityInfo", "TableFunction"]


@dataclass(frozen=True, slots=True)
class CardinalityInfo:
    """Cardinality hints for query optimization.

    Provides optional row count estimates that can help query planners make
    better decisions about join ordering, memory allocation, and parallelization.

    Attributes:
        estimate: Estimated number of output rows, or None if unknown.
        max: Maximum possible output rows, or None if unbounded.

    Example:
        # Function that filters ~10% of rows, with known input size
        CardinalityInfo(estimate=1000, max=10000)

        # Aggregation that always produces exactly one row
        CardinalityInfo(estimate=1, max=1)

        # Unknown output size
        CardinalityInfo(estimate=None, max=None)
    """

    estimate: int | None
    max: int | None


@dataclass(frozen=True, slots=True)
class GlobalStateInitInput:
    """Input sent to initialize global state for a TableInOutFunction.

    Attributes:
        projection_ids: Optional list of column indices to project, or None for all.
    """

    projection_ids: list[int] | None = None

    def serialize(self) -> bytes:
        """Serialize GlobalStateInitInput to bytes."""
        batch = pa.RecordBatch.from_arrays(
            [pa.array([self.projection_ids], type=pa.list_(pa.int32()))],
            schema=pa.schema([pa.field("projection_ids", pa.list_(pa.int32()))]),
        )
        return vgi.util.recordbatch_to_bytes(batch)

    @staticmethod
    def deserialize(batch: pa.RecordBatch) -> "GlobalStateInitInput":
        """Deserialize GlobalStateInitInput from a RecordBatch"""
        values = batch.to_pylist()[0]
        return GlobalStateInitInput(**values)


@dataclass(frozen=True, slots=True)
class TableFunctionBindResult(vgi.function.BindResult):
    """Extended bind result for table functions with cardinality information.

    Extends BindResult with optional cardinality estimates that help query
    planners optimize execution strategies.

    Attributes:
        output_schema: Arrow schema describing the structure of output batches.
        max_processes: Maximum parallel processes this function can utilize.
        call_identifier: Unique bytes identifying this function invocation.
        cardinality: Optional row count estimates for query optimization.
            None indicates no cardinality information is available.

    Example:
        import uuid
        TableFunctionBindResult(
            output_schema=pa.schema([pa.field("id", pa.int64())]),
            max_processes=4,
            call_identifier=uuid.uuid4().bytes,
            cardinality=CardinalityInfo(estimate=1000, max=10000),
        )
    """

    cardinality: CardinalityInfo | None

    def serialize_schema(self) -> pa.Schema:
        """Extend parent schema with cardinality fields."""
        return (
            super(TableFunctionBindResult, self)
            .serialize_schema()
            .append(pa.field("cardinality_estimated", pa.int64(), nullable=True))
            .append(pa.field("cardinality_max", pa.int64(), nullable=True))
        )

    def serialize_dict(self) -> dict[str, Any]:
        """Extend parent dict with cardinality values."""
        return super(TableFunctionBindResult, self).serialize_dict() | {
            "cardinality_estimated": (
                self.cardinality.estimate if self.cardinality else None
            ),
            "cardinality_max": (self.cardinality.max if self.cardinality else None),
        }


class TableFunction(vgi.function.Function):
    """Base class for table functions with cardinality estimation.

    Extends Function with optional cardinality hints that help query planners
    optimize execution. Override cardinality() to provide row count estimates.

    See Also:
        vgi.table_in_out_function.TableInOutFunction: Full streaming implementation
            that extends this class with the complete DATA/FINALIZE protocol.
    """

    # This is the init data that may be been read.
    init_data: GlobalStateInitInput | None = None

    def __init__(
        self, *, call_data: vgi.function.CallData, logger: structlog.stdlib.BoundLogger
    ):
        """Initialize the table function with call data.

        Args:
            call_data: Complete invocation request including function name,
                arguments, and input schema.
            logger: Logger instance for structured logging.
        """
        super().__init__(call_data=call_data, logger=logger)

    def cardinality(self) -> CardinalityInfo | None:
        """Return optional cardinality estimate for the output.

        Override to provide row count estimates that help query planners
        make better decisions about join ordering and memory allocation.

        Returns:
            CardinalityInfo with estimate and/or max, or None if unknown.
        """
        return None

    def perform_init(self, input: pa.RecordBatch) -> vgi.function.GlobalInitResult:
        """Perform a new init call and store it in the storage."""
        self.init_data = GlobalStateInitInput.deserialize(input)
        return vgi.function.GlobalInitResult(self.init_storage.create(self.init_data))

    def retrieve_init(self, input: vgi.function.GlobalInitResult) -> None:
        """Retrieve and store init data from the storage."""
        assert input.global_init_identifier is not None
        self.init_data = self.init_storage.get(input.global_init_identifier)

    def apply_projection(self, schema: pa.Schema) -> pa.Schema:
        """Apply any projection specified in the init data to the schema.

        Args:
            schema: Original output schema before projection.
        Returns:
            Projected schema according to init data, or original if no projection.
        """
        if self.init_data and self.init_data.projection_ids is not None:
            projected_fields = []
            for proj_id in self.init_data.projection_ids:
                field = schema.field(proj_id)
                projected_fields.append(field)
            return pa.schema(projected_fields)
        return schema
