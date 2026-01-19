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

import inspect
from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any, cast, get_args, get_origin

import polars as pl
import pyarrow as pa
import structlog
from polars.datatypes.classes import DataTypeClass

import vgi.invocation
import vgi.log
from vgi.arguments import ConstParam, Param, is_polars_type
from vgi.scalar_function import ScalarFunctionGenerator, ScalarOutputGenerator
from vgi.table_function import Output

if TYPE_CHECKING:
    from collections.abc import Mapping


__all__ = [
    "AnyPolars",
    "PolarsParamInfo",
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


@dataclass(frozen=True, slots=True)
class PolarsParamInfo:
    """Information about a Polars function parameter.

    Attributes:
        name: The parameter name (used for column reference in expressions).
        polars_type: The Polars data type, or None if dynamic (Any).
        position: The position in the input batch columns.
        doc: Documentation string for this parameter.
        varargs: True if this parameter collects remaining columns.
        type_bound: Optional type constraint predicate(s) for dynamic types.
        is_const: True if this is a ConstParam (scalar, not array).

    """

    name: str
    polars_type: pl.DataType | None  # None means dynamic (Any)
    position: int
    doc: str
    varargs: bool = False
    type_bound: Any = None  # TypeBoundPredicate | tuple[TypeBoundPredicate, ...] | None
    is_const: bool = False


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
    # Class-level attributes set by __init_subclass__
    _polars_params: dict[str, PolarsParamInfo]  # Param/ConstParam info by name
    _has_dynamic_types: bool  # True if any param uses Any
    _class_output_type: pl.DataType | None  # Output type if static

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Extract Param/ConstParam annotations from class attributes.

        This method processes class-level Annotated[type, Param(...)] declarations
        to build the parameter specification for Polars functions.

        Example class attribute:
            text: Annotated[pl.Utf8, Param(position=0, doc="String value")]

        Sets class attributes:
            _polars_params: Dict of PolarsParamInfo by parameter name
            _has_dynamic_types: True if any param uses Any (requires bind-time type)
            _class_output_type: Output type if all types are static, None otherwise
        """
        super().__init_subclass__(**kwargs)

        # Skip abstract classes
        if inspect.isabstract(cls):
            return

        # Initialize class attributes
        cls._polars_params = {}
        cls._has_dynamic_types = False
        cls._class_output_type = None

        # Get annotations from the class (not inherited)
        annotations = getattr(cls, "__annotations__", {})
        if not annotations:
            return

        # Build evaluation namespace from module globals
        module = __import__(cls.__module__, fromlist=[""])
        globalns = getattr(module, "__dict__", {})
        # Add common imports that might be needed for annotation evaluation
        globalns.setdefault("Annotated", Annotated)
        globalns.setdefault("pl", pl)
        globalns.setdefault("Any", Any)
        # Add pyarrow.types for type_bound predicates
        import pyarrow.types as pat

        globalns.setdefault("pat", pat)

        for attr_name, annotation in annotations.items():
            # Evaluate string annotation if needed (from __future__ import annotations)
            if isinstance(annotation, str):
                try:
                    hint = eval(annotation, globalns)  # noqa: S307
                except Exception:
                    # Can't evaluate this annotation, skip it
                    continue
            else:
                hint = annotation

            # Skip if not Annotated
            if get_origin(hint) is not Annotated:
                continue

            # Get the base type and metadata from Annotated[BaseType, metadata...]
            args = get_args(hint)
            if not args:
                continue

            base_type = args[0]
            metadata = args[1:]

            # Look for Param or ConstParam in the metadata
            for meta in metadata:
                if isinstance(meta, Param):
                    # Extract parameter info
                    polars_type: pl.DataType | None = None

                    # Determine the Polars type from base_type
                    if base_type is Any:
                        cls._has_dynamic_types = True
                    elif is_polars_type(base_type):
                        polars_type = _normalize_polars_type(base_type)
                    else:
                        # Could be a Python type or something else - skip
                        continue

                    # Get position from Param (either position attr or from arrow_type)
                    position = meta.position
                    if position is None:
                        # Position must be specified for Polars params
                        raise TypeError(
                            f"{cls.__name__}.{attr_name}: Param must specify position "
                            f"(e.g., Param(position=0, doc='...'))"
                        )

                    # Extract type_bound
                    type_bound = meta.type_bound
                    if isinstance(type_bound, (list, tuple)):
                        type_bound = tuple(type_bound)

                    cls._polars_params[attr_name] = PolarsParamInfo(
                        name=attr_name,
                        polars_type=polars_type,
                        position=position,
                        doc=meta.doc,
                        varargs=meta.varargs,
                        type_bound=type_bound,
                        is_const=False,
                    )
                    break

                elif isinstance(meta, ConstParam):
                    # ConstParam - extract scalar parameter info
                    # Position must be specified
                    position = getattr(meta, "position", None)
                    if position is None:
                        raise TypeError(
                            f"{cls.__name__}.{attr_name}: ConstParam must specify "
                            f"position for Polars functions"
                        )

                    cls._polars_params[attr_name] = PolarsParamInfo(
                        name=attr_name,
                        polars_type=None,  # Const params don't have Polars types
                        position=position,
                        doc=meta.doc,
                        varargs=False,
                        type_bound=None,
                        is_const=True,
                    )
                    break

    def __init__(
        self,
        invocation: vgi.invocation.Invocation,
        logger: structlog.stdlib.BoundLogger,
    ):
        """Initialize the Polars scalar function."""
        self._pending_messages = []
        self._polars_schema = None
        self._inferred_output_type: pl.DataType | None = None
        super().__init__(invocation=invocation, logger=logger)

    def bind(self) -> None:
        """Validate type bounds and prepare for dynamic type inference.

        This method:
        1. Validates type_bounds for params with dynamic types (Any)
        2. For dynamic output types, prepares for inference (actual inference
           happens when compute_polars() is called for the first time)

        Raises:
            SchemaValidationError: If any type_bound constraint is not satisfied.

        """
        from vgi.exceptions import SchemaValidationError

        super().bind()

        # Validate type bounds for params with type_bound specified
        for name, param_info in self._polars_params.items():
            if param_info.type_bound is None:
                continue
            if param_info.is_const:
                continue  # Const params don't have type bounds

            # Get the actual type from input schema
            if param_info.position >= self.input_schema.__len__():
                raise SchemaValidationError(
                    f"Parameter '{name}' at position {param_info.position} "
                    f"but input has only {self.input_schema.__len__()} columns"
                )

            field = self.input_schema.field(param_info.position)
            field_type = field.type

            # Normalize type_bound to sequence
            type_bound = param_info.type_bound
            predicates = [type_bound] if callable(type_bound) else list(type_bound)

            # OR logic: at least one predicate must pass
            if not any(predicate(field_type) for predicate in predicates):
                predicate_names = [getattr(p, "__name__", str(p)) for p in predicates]
                raise SchemaValidationError(
                    f"Column '{name}' has type {field_type}, "
                    f"but type_bound requires: {', '.join(predicate_names)}"
                )

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
    def compute_polars(self) -> pl.Expr:
        """Return a Polars expression for the scalar transformation.

        Override this method to implement your scalar transformation
        as a Polars expression. Reference columns by their declared
        param names using pl.col("param_name").

        Returns:
            A Polars expression that computes the output.

        Example:
            def compute_polars(self) -> pl.Expr:
                return pl.col("text").str.to_uppercase()

        Example with multiple params:
            def compute_polars(self) -> pl.Expr:
                return pl.col("left") + pl.col("right")

        Example with constant:
            def compute_polars(self) -> pl.Expr:
                return pl.col("value") * self.factor

        """
        ...

    def _build_column_rename_map(self, batch: pa.RecordBatch) -> dict[str, str]:
        """Build mapping from input column names to declared param names.

        For regular params: col_0 -> "text", col_1 -> "right"
        For varargs: col_0 -> "values_0", col_1 -> "values_1", etc.

        Args:
            batch: Input RecordBatch with columns to rename.

        Returns:
            Dict mapping original column names to param names.

        """
        rename_map: dict[str, str] = {}

        # Collect all params sorted by position
        params_by_pos: list[tuple[int, str, PolarsParamInfo]] = []
        for name, param in self._polars_params.items():
            if not param.is_const:  # Skip const params (not columns)
                params_by_pos.append((param.position, name, param))
        params_by_pos.sort(key=lambda x: x[0])

        for pos, name, param in params_by_pos:
            if param.varargs:
                # Varargs: map remaining columns as name_0, name_1, etc.
                for vararg_idx, col_idx in enumerate(range(pos, batch.num_columns)):
                    col_name = batch.schema.field(col_idx).name
                    rename_map[col_name] = f"{name}_{vararg_idx}"
            else:
                # Regular param: map single column to param name
                if pos < batch.num_columns:
                    col_name = batch.schema.field(pos).name
                    rename_map[col_name] = name

        return rename_map

    def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Transform Arrow batch through Polars. Do not override.

        This method handles the Arrow <-> Polars conversion and calls
        compute_polars() for the actual transformation.
        """
        # Zero-copy conversion to Polars DataFrame
        df = cast(pl.DataFrame, pl.from_arrow(batch))

        # Rename columns to declared param names if using new Param API
        if self._polars_params:
            rename_map = self._build_column_rename_map(batch)
            if rename_map:
                df = df.rename(rename_map)

        # Get the expression and evaluate it
        expr = self.compute_polars()
        result_series = df.select(expr.alias("result"))["result"]

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
