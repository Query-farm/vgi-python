from dataclasses import dataclass
from typing import Any

import pyarrow as pa

import vgi.function

__all__ = ["TableFunctionBindResult", "CardinalityInfo"]


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
    def __init__(self, call_data: vgi.function.CallData):
        super().__init__(call_data)

    def cardinality(self) -> CardinalityInfo | None:
        """Optional cardinality estimate for the output."""
        return None
