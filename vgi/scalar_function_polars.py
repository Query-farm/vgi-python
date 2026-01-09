"""Polars-based scalar functions with zero-copy Arrow integration.

This module provides PolarsScalarFunction, a base class for scalar functions
that use Polars for data processing. It handles the zero-copy conversion
between Arrow and Polars automatically.

Zero-Copy Pattern
-----------------
Polars can work directly with Arrow data without copying:

    # Arrow -> Polars (zero-copy)
    df = pl.from_arrow(batch)

    # Polars operations
    result_series = df.select(pl.col("x") * 2).to_series()

    # Polars -> Arrow (zero-copy)
    result_array = result_series.to_arrow()

PolarsScalarFunction automates this pattern:

    class DoubleColumn(PolarsScalarFunction):
        class Meta:
            output_type = pl.Int64  # Polars type

        column = Arg[str](0, doc="Column to double")

        def compute_polars(self, df: pl.DataFrame) -> pl.Series:
            return df[self.column] * 2

Example:
-------
Simple uppercase function::

    class UpperCase(PolarsScalarFunction):
        class Meta:
            output_type = pl.Utf8

        column = Arg[str](0, doc="Column to uppercase")

        def compute_polars(self, df: pl.DataFrame) -> pl.Series:
            return df[self.column].str.to_uppercase()

Dynamic output type (depends on input)::

    class DoubleColumn(PolarsScalarFunction):
        class Meta:
            output_type = AnyPolars  # Dynamic type

        column = Arg[str](0, doc="Column to double")

        @property
        def output_polars_type(self) -> pl.DataType:
            # Determine output type from input column
            return self.polars_schema[self.column]

        def compute_polars(self, df: pl.DataFrame) -> pl.Series:
            return df[self.column] * 2

"""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, cast

import polars as pl
import pyarrow as pa
import structlog
from polars.datatypes.classes import DataTypeClass

import vgi.invocation
import vgi.log
from vgi.scalar_function import ScalarFunctionGenerator, ScalarOutputGenerator
from vgi.table_function import Output

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = [
    "AnyPolars",
    "PolarsScalarFunction",
    "PolarsOutputType",
]


class AnyPolars:
    """Marker class indicating dynamic Polars output type.

    Use this as Meta.output_type when the output type depends on input schema
    and will be determined at runtime via the output_polars_type property.
    """

    pass


# Type alias for Polars scalar function output type declarations.
# Polars types can be either DataType instances (pl.Utf8()) or DataTypeClass (pl.Utf8)
PolarsOutputType = pl.DataType | DataTypeClass | type[AnyPolars]


def _normalize_polars_type(polars_type: pl.DataType | DataTypeClass) -> pl.DataType:
    """Normalize a Polars type to a DataType instance.

    Polars types can be specified as either:
    - DataType instances: pl.Utf8(), pl.Float64()
    - DataTypeClass (type classes): pl.Utf8, pl.Float64

    This function ensures we have a DataType instance.
    """
    if isinstance(polars_type, DataTypeClass):
        # DataTypeClass is callable and returns a DataType instance
        return cast(pl.DataType, polars_type())
    return polars_type


def _polars_to_arrow_type(polars_type: pl.DataType | DataTypeClass) -> pa.DataType:
    """Convert a Polars data type to an Arrow data type.

    Args:
        polars_type: The Polars data type to convert (instance or class).

    Returns:
        The equivalent Arrow data type.

    """
    # Normalize to DataType instance
    dtype = _normalize_polars_type(polars_type)

    # Create a minimal series of the given type and convert to Arrow
    # This lets Polars handle the type mapping correctly
    dummy = pl.Series("x", [], dtype=dtype)
    arrow_array = dummy.to_arrow()
    return cast(pa.DataType, arrow_array.type)


class PolarsScalarFunction(ScalarFunctionGenerator):
    """Base class for scalar functions using Polars.

    This class handles the zero-copy conversion between Arrow and Polars,
    letting you work with Polars DataFrames in compute_polars().

    Methods/Attributes to Override
    ------------------------------
    Meta.output_type : pl.DataType | type[AnyPolars] (required)
        Declare the Polars output type for catalog introspection.
        Use a pl.DataType for static output, or AnyPolars if output
        type depends on input schema.

    compute_polars(df) : pl.Series
        Transform the input DataFrame to a single output Series.
        Must return a Series with exactly df.height elements.

    output_polars_type : pl.DataType (property, optional)
        Override only if Meta.output_type is AnyPolars.
        Default implementation uses Meta.output_type.

    setup() : None
        Called before processing. Acquire resources here.

    teardown() : None
        Called after processing. Release resources here.

    Available Attributes
    --------------------
    self.invocation : Invocation
        The complete invocation request with function name and arguments.

    self.input_schema : pa.Schema
        Arrow schema of input batches.

    self.polars_schema : dict[str, pl.DataType]
        Polars schema derived from input_schema.

    self.output_schema : pa.Schema
        Arrow schema of output batches (single column named "result").

    self.empty_output_batch : pa.RecordBatch
        Empty batch conforming to output_schema.

    Example:
    -------
    A function that converts a column to uppercase::

        class PolarsUpperCase(PolarsScalarFunction):
            class Meta:
                output_type = pl.Utf8

            column = Arg[str](0, doc="Column to uppercase")

            def compute_polars(self, df: pl.DataFrame) -> pl.Series:
                return df[self.column].str.to_uppercase()

    """

    _pending_messages: list[vgi.log.Message]
    _polars_schema: Mapping[str, pl.DataType] | None

    def __init__(
        self,
        invocation: vgi.invocation.Invocation,
        logger: structlog.stdlib.BoundLogger,
    ):
        """Initialize the Polars scalar function."""
        self._pending_messages = []
        self._polars_schema = None
        super().__init__(invocation=invocation, logger=logger)

    @property
    def polars_schema(self) -> Mapping[str, pl.DataType]:
        """Return the Polars schema derived from input_schema.

        This property lazily converts the Arrow input_schema to a Polars
        schema mapping (column name -> Polars data type).
        """
        if self._polars_schema is None:
            # Convert Arrow schema to Polars schema by creating an empty
            # DataFrame with the Arrow schema
            columns = {
                field.name: pa.array([], type=field.type) for field in self.input_schema
            }
            empty_table = pa.table(columns)
            df = cast(pl.DataFrame, pl.from_arrow(empty_table))
            self._polars_schema = df.schema
        return self._polars_schema

    def log(self, level: vgi.log.Level, message: str) -> None:
        """Queue a log message to be emitted with the output.

        Messages are yielded before the compute_polars() result.

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR).
            message: Log message text.

        """
        self._pending_messages.append(vgi.log.Message(level=level, message=message))

    @classmethod
    def _get_meta_output_type(cls) -> PolarsOutputType:
        """Get output_type from Meta class.

        Walks the MRO to find a Meta class with output_type defined.

        Returns:
            The output_type value from Meta (pl.DataType or AnyPolars).

        Raises:
            TypeError: If Meta.output_type is not defined in the class hierarchy.

        """
        for klass in cls.__mro__:
            if "Meta" in klass.__dict__:
                meta = klass.__dict__["Meta"]
                if hasattr(meta, "output_type"):
                    return meta.output_type  # type: ignore[no-any-return]
        raise TypeError(
            f"{cls.__name__} must define Meta.output_type "
            f"(pl.DataType for static type, or AnyPolars for dynamic)"
        )

    @classmethod
    def catalog_output_schema(cls) -> pa.Schema:
        """Return output schema for catalog introspection.

        Returns the output schema with a single "result" field. If
        Meta.output_type is AnyPolars, the field has type null() with
        metadata indicating it's a dynamic "any" type.
        """
        output_type = cls._get_meta_output_type()
        if output_type is AnyPolars:
            # Use null type with metadata to indicate "any" type
            field = pa.field("result", pa.null(), metadata={b"vgi:any": b"true"})
            return pa.schema([field])
        # Type is a Polars DataType or DataTypeClass (not AnyPolars)
        polars_type = cast("pl.DataType | DataTypeClass", output_type)
        arrow_type = _polars_to_arrow_type(polars_type)
        return pa.schema([pa.field("result", arrow_type)])

    @property
    def output_polars_type(self) -> pl.DataType:
        """Return the Polars type for the output column.

        Default implementation uses Meta.output_type. Override only if
        Meta.output_type is AnyPolars and you need to compute the type
        from input schema at runtime.

        Example:
            @property
            def output_polars_type(self) -> pl.DataType:
                return self.polars_schema[self.column]

        """
        result = self._get_meta_output_type()
        if result is AnyPolars:
            raise NotImplementedError(
                f"{type(self).__name__}.output_polars_type must be overridden when "
                f"Meta.output_type is AnyPolars"
            )
        # Normalize DataTypeClass to DataType instance (not AnyPolars)
        polars_type = cast("pl.DataType | DataTypeClass", result)
        return _normalize_polars_type(polars_type)

    @property
    def output_type(self) -> pa.DataType:
        """Return the Arrow type for the output column.

        Converts the Polars output type to Arrow.
        """
        return _polars_to_arrow_type(self.output_polars_type)

    @property
    def output_schema(self) -> pa.Schema:
        """Return single-column output schema."""
        return pa.schema([pa.field("result", self.output_type)])

    @abstractmethod
    def compute_polars(self, df: pl.DataFrame) -> pl.Series:
        """Compute output Series from input DataFrame.

        Override this method to implement your scalar transformation
        using Polars operations.

        Args:
            df: Input DataFrame (zero-copy from Arrow).

        Returns:
            Series with exactly df.height elements.

        Example:
            def compute_polars(self, df: pl.DataFrame) -> pl.Series:
                return df[self.column].str.to_uppercase()

        """
        ...

    def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Transform Arrow batch through Polars. Do not override.

        This method handles the Arrow <-> Polars conversion and calls
        compute_polars() for the actual transformation.
        """
        # Zero-copy conversion to Polars DataFrame
        df = cast(pl.DataFrame, pl.from_arrow(batch))

        # Call user's Polars implementation
        result_series = self.compute_polars(df)

        # Zero-copy conversion back to Arrow
        return result_series.to_arrow()

    def _yield_pending_messages(self) -> ScalarOutputGenerator:
        """Yield all pending log messages. Helper for process()."""
        while self._pending_messages:
            msg = self._pending_messages.pop(0)
            _ = yield msg

    def process(self, batch: pa.RecordBatch) -> ScalarOutputGenerator:
        """Convert compute() to generator protocol. Do not override.

        This method implements the generator protocol by calling your
        compute_polars() method for each input batch.
        """
        # Priming yield
        _ = yield Output(self.empty_output_batch)

        while True:
            result = self.compute(batch)

            # Yield any pending log messages first
            yield from self._yield_pending_messages()

            # Create output batch from result array
            output = pa.RecordBatch.from_arrays([result], schema=self.output_schema)
            received = yield Output(output)

            if received is None:
                break
            batch = received
