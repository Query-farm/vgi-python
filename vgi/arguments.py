"""Argument parsing and validation for VGI functions.

This module provides classes for handling function arguments in VGI:

Classes:
    Arguments: Container for positional and named function arguments.
    ArgumentValidationError: Raised when an argument fails validation.
    Arg: Descriptor for declarative argument parsing with optional validation.
    AnyArrow: Sentinel type for arguments accepting multiple Arrow types.
    AnyArrowValue: Wrapper returned when accessing AnyArrow arguments.

Example using Annotated (recommended):
    from typing import Annotated
    from vgi import Arg, AnyArrowValue

    class MyFunction(TableInOutFunction):
        count: Annotated[int, Arg(0)]  # Required positional
        name: Annotated[str, Arg("name", default="unnamed")]  # Optional named
        column: Annotated[AnyArrowValue, Arg(0, type_bound=pa.types.is_integer)]

Example using legacy Arg[T] syntax:
    class MyFunction(TableInOutFunction):
        count = Arg[int](0)  # Required positional
        name = Arg[str]("name", default="unnamed")  # Optional named

Example using Arguments.get() for manual parsing:
    count = args.get(0)
    name = args.get("name", default="unnamed")

"""

import re
import warnings
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, TypeVar, cast, overload

import pyarrow as pa

if TYPE_CHECKING:
    from pyarrow import Scalar

# Note: Param.arrow_type also accepts Polars types (pl.DataType, pl.Utf8, etc.)
# These are detected at runtime by is_polars_type() and converted to Arrow types


# Python type to Arrow type mapping for Arg type hints
PYTHON_TO_ARROW: dict[type, pa.DataType] = {
    int: pa.int64(),
    str: pa.utf8(),
    float: pa.float64(),
    bool: pa.bool_(),
    bytes: pa.binary(),
}

# Private mapping used by _python_to_arrow() helper
_PYTHON_TO_ARROW: dict[type, pa.DataType] = {
    int: pa.int64(),
    float: pa.float64(),
    str: pa.string(),
    bool: pa.bool_(),
    bytes: pa.binary(),
}

# Arrow type to Python scalar type mapping
# Keys are the type class of Arrow DataType instances (e.g., type(pa.int8()))
_ARROW_TO_PYTHON: dict[type, type] = {
    # Primitives - integers
    type(pa.int8()): int,
    type(pa.int16()): int,
    type(pa.int32()): int,
    type(pa.int64()): int,
    type(pa.uint8()): int,
    type(pa.uint16()): int,
    type(pa.uint32()): int,
    type(pa.uint64()): int,
    # Primitives - floats
    type(pa.float16()): float,
    type(pa.float32()): float,
    type(pa.float64()): float,
    # Primitives - strings
    type(pa.string()): str,
    type(pa.large_string()): str,
    # Primitives - boolean
    type(pa.bool_()): bool,
    # Primitives - binary
    type(pa.binary()): bytes,
    type(pa.large_binary()): bytes,
    # Nested types
    type(pa.struct([])): dict,
    type(pa.list_(pa.int32())): list,
    type(pa.large_list(pa.int32())): list,
    type(pa.list_(pa.int32(), 3)): list,  # FixedSizeListType
    type(pa.map_(pa.string(), pa.int32())): dict,
}


def _python_to_arrow(py_type: type) -> pa.DataType:
    """Convert a Python type to the corresponding Arrow type.

    Args:
        py_type: Python type (int, float, str, bool, bytes).

    Returns:
        Corresponding Arrow data type.

    Raises:
        TypeError: If py_type is not a supported Python type.

    Example:
        >>> _python_to_arrow(int)
        DataType(int64)
        >>> _python_to_arrow(str)
        DataType(string)

    """
    if py_type in _PYTHON_TO_ARROW:
        return _PYTHON_TO_ARROW[py_type]

    supported = ", ".join(t.__name__ for t in _PYTHON_TO_ARROW)
    raise TypeError(
        f"Cannot convert Python type '{py_type.__name__}' to Arrow type. "
        f"Supported types: {supported}. "
        f"Example: _python_to_arrow(int) -> pa.int64()"
    )


# =============================================================================
# PyArrow Array Class to DataType Mapping (for type inference)
# =============================================================================
#
# These mappings enable inferring Arrow types from array class annotations:
#   Annotated[pa.Int64Array, Param(doc="...")] -> pa.int64()
#
# Only simple (non-parameterized) types are included. Complex types that require
# parameters (e.g., StructArray, ListArray, Decimal128Array) need explicit
# arrow_type specification.

# Simple array classes that can be inferred automatically
ARRAY_CLASS_TO_DATATYPE: dict[type, pa.DataType] = {
    # Integers
    pa.Int8Array: pa.int8(),
    pa.Int16Array: pa.int16(),
    pa.Int32Array: pa.int32(),
    pa.Int64Array: pa.int64(),
    pa.UInt8Array: pa.uint8(),
    pa.UInt16Array: pa.uint16(),
    pa.UInt32Array: pa.uint32(),
    pa.UInt64Array: pa.uint64(),
    # Floats
    pa.HalfFloatArray: pa.float16(),
    pa.FloatArray: pa.float32(),
    pa.DoubleArray: pa.float64(),
    # Strings/Binary
    pa.StringArray: pa.string(),
    pa.LargeStringArray: pa.large_string(),
    pa.BinaryArray: pa.binary(),
    pa.LargeBinaryArray: pa.large_binary(),
    # Boolean
    pa.BooleanArray: pa.bool_(),
    # Dates (no params needed)
    pa.Date32Array: pa.date32(),
    pa.Date64Array: pa.date64(),
    # Null
    pa.NullArray: pa.null(),
}

# Complex array classes that require explicit arrow_type (parameterized types)
# Using these without arrow_type will raise a helpful error
COMPLEX_ARRAY_CLASSES: set[type] = {
    # Nested types
    pa.StructArray,
    pa.ListArray,
    pa.LargeListArray,
    pa.FixedSizeListArray,
    pa.MapArray,
    pa.UnionArray,
    # Parameterized types
    pa.DictionaryArray,
    pa.Decimal128Array,
    pa.Decimal256Array,
    pa.FixedSizeBinaryArray,
    # Temporal types with units (require explicit unit specification)
    pa.Time32Array,
    pa.Time64Array,
    pa.TimestampArray,
    pa.DurationArray,
}


# =============================================================================
# Polars Type Detection and Conversion
# =============================================================================


def is_polars_type(obj: Any) -> bool:
    """Check if an object is a Polars data type.

    Detects both Polars DataType instances (pl.Utf8()) and DataTypeClass
    (pl.Utf8). Works without importing polars at module level.

    Args:
        obj: Object to check.

    Returns:
        True if obj is a Polars DataType or DataTypeClass, False otherwise.

    """
    # Check by module name to avoid importing polars
    obj_type = type(obj)
    module = getattr(obj_type, "__module__", "")

    # DataType instances: polars.datatypes.classes module
    if module.startswith("polars.datatypes"):
        return True

    # DataTypeClass (type classes like pl.Utf8): check if it's a class
    # with a polars module that's callable and returns a DataType
    if isinstance(obj, type):
        obj_module = getattr(obj, "__module__", "")
        if obj_module.startswith("polars.datatypes"):
            return True

    return False


def polars_type_to_arrow(polars_type: Any) -> pa.DataType:
    """Convert a Polars data type to an Arrow data type.

    Args:
        polars_type: A Polars DataType instance (pl.Utf8()) or DataTypeClass (pl.Utf8).

    Returns:
        The equivalent Arrow data type.

    Raises:
        TypeError: If polars is not installed or conversion fails.

    Example:
        >>> import polars as pl
        >>> polars_type_to_arrow(pl.Utf8)
        DataType(string)
        >>> polars_type_to_arrow(pl.Int64())
        DataType(int64)

    """
    try:
        import polars as pl
    except ImportError as e:
        raise TypeError(
            f"Cannot convert Polars type '{polars_type}' - polars is not installed"
        ) from e

    # Normalize DataTypeClass to DataType instance
    # DataTypeClass types (like pl.Utf8) are callable to produce instances
    if isinstance(polars_type, type) and getattr(
        polars_type, "__module__", ""
    ).startswith("polars.datatypes"):
        polars_type = polars_type()  # Call to get instance

    if not isinstance(polars_type, pl.DataType):
        raise TypeError(
            f"Expected Polars DataType, got {type(polars_type).__name__}: {polars_type}"
        )

    # Create a minimal series of the given type and convert to Arrow
    # This lets Polars handle the type mapping correctly
    dummy = pl.Series("x", [], dtype=polars_type)
    arrow_array = dummy.to_arrow()
    return cast(pa.DataType, arrow_array.type)


def _arrow_type_to_python(arrow_type: pa.DataType) -> type:
    """Convert an Arrow type to the corresponding Python scalar type.

    Args:
        arrow_type: Arrow data type instance.

    Returns:
        Corresponding Python type for scalar values.
        Returns Any (object) for unknown Arrow types.

    Example:
        >>> _arrow_type_to_python(pa.int64())
        <class 'int'>
        >>> _arrow_type_to_python(pa.string())
        <class 'str'>

    """
    arrow_type_class = type(arrow_type)
    return _ARROW_TO_PYTHON.get(arrow_type_class, object)


# Sentinel for missing default value - proper type pattern
class _MissingType:
    """Sentinel type for missing default values.

    This provides better type safety than using `Any` for the sentinel.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return "<MISSING>"

    def __bool__(self) -> bool:
        return False


_MISSING: Final = _MissingType()

__all__ = [
    "AnyArrow",
    "AnyArrowValue",
    "ARRAY_CLASS_TO_DATATYPE",
    "Arg",
    "ArgumentValidationError",
    "Arguments",
    "COMPLEX_ARRAY_CLASSES",
    "ConstParam",
    "Param",
    "PYTHON_TO_ARROW",
    "Returns",
    "TableInput",
    "TypeBoundPredicate",
    "is_polars_type",
    "polars_type_to_arrow",
]


class TableInput:
    """Sentinel type for table input parameters in table-in-out functions.

    Use this as the type parameter for Arg to declare which argument receives
    the streaming table input. Every TableInOutFunction must have exactly one
    TableInput argument, and it must be positional (not named).

    The TableInput argument determines which table expression feeds the function
    when called from SQL. It doesn't correspond to an actual Arrow value - the
    table data arrives as streaming RecordBatches via process().

    Example:
        class MyFunction(TableInOutFunction):
            # Other args come first, table input last (by convention)
            repeat_count = Arg[int](0, doc="Number of repetitions")
            data = Arg[TableInput](1, doc="Input table to process")

        # SQL: SELECT * FROM my_function(3, input_table)
        #      repeat_count=3, data receives rows from input_table

    """

    pass


@dataclass(frozen=True, slots=True)
class AnyArrowValue:
    """Wrapper for AnyArrow argument values with metadata.

    When an Arg returns an AnyArrow type, accessing the attribute returns
    an AnyArrowValue instead of just the raw value. This provides access to
    both the value and the argument's position/name for schema lookups.

    Attributes:
        value: The Python value (from scalar.as_py()).
        position: The positional index from the Arg definition (int for positional,
            str for named arguments).
        name: The Python attribute name of the Arg.

    Example using Annotated (recommended):
        from typing import Annotated

        class MyFunction(ScalarFunction):
            col1: Annotated[AnyArrowValue, Arg(0, doc="First column")]

            def bind(self) -> None:
                # self.col1 is an AnyArrowValue
                field = self.input_schema.field(self.col1.position)
                # Or access the value directly
                print(self.col1.value)

    Example using legacy Arg[AnyArrow] syntax:
        class MyFunction(ScalarFunction):
            col1 = Arg[AnyArrow](0, doc="First column")  # type: ignore[assignment]

    """

    value: Any
    position: int | str
    name: str


class AnyArrow:
    """Sentinel type for arguments accepting multiple Arrow types.

    Use this with ``AnyArrowValue`` in the Annotated pattern when an argument
    should accept multiple valid Arrow types, validated via the ``type_bound``
    parameter. When accessed, returns an AnyArrowValue containing the value
    plus metadata (position and name).

    Choosing Between Specific Types and AnyArrowValue
    -------------------------------------------------
    - **Single required type**: Use ``Annotated[str, Arg(...)]`` or similar.
      The argument will only accept that exact type.

    - **Multiple valid types**: Use ``Annotated[AnyArrowValue, Arg(...)]`` with
      ``type_bound`` to specify which types are acceptable. For example, numeric
      operations that work on integers, floats, and decimals should use AnyArrowValue.

    The ``type_bound`` parameter is ONLY meaningful for ``AnyArrowValue`` arguments.
    Using it with other types will emit a warning.

    Examples using Annotated (recommended):
        from typing import Annotated
        from vgi import Arg, AnyArrowValue

        # Single type: function only works with strings
        class UpperCaseFunction(ScalarFunction):
            column: Annotated[str, Arg(0, doc="String column to uppercase")]

        # Multiple types: function works with any numeric type
        class DoubleFunction(ScalarFunction):
            column: Annotated[
                AnyArrowValue,
                Arg(0, type_bound=[pa.types.is_integer, pa.types.is_floating])
            ]

            def bind(self) -> None:
                # Access column metadata for dynamic output type
                field = self.input_schema.field(self.column.value)
                self._output_type = field.type

        # Any type: function works with all types
        class IdentityFunction(ScalarFunction):
            column: Annotated[AnyArrowValue, Arg(0, doc="Column to pass through")]

    Accessing Values:
        When using AnyArrowValue, access the value via the ``.value`` attribute::

            val = self.column.value  # The column name as a string
            field = self.input_schema.field(self.column.value)

    Note:
        Unlike TableInput, AnyArrow arguments have actual Arrow values -
        they are just not constrained to a specific Arrow type.

    """

    # Type stubs for static analysis - at runtime, Arg[AnyArrow] returns AnyArrowValue
    value: Any
    position: int | str
    name: str


@dataclass(frozen=True, slots=True)
class Arguments:
    """Container for function arguments.

    Access arguments using get() for Python values:

        # Positional arguments (by index)
        count = args.get(0)                      # First argument
        name = args.get(1, default="unnamed")    # With default

        # Named arguments (by string)
        separator = args.get("sep", default=",")
        threshold = args.get("threshold")

        # With type validation (optional, for strict checking)
        count = args.get(0, type=pa.int64())

    For direct Arrow Scalar access, use positional/named attributes:

        scalar = args.positional[0]              # pa.Scalar | None
        scalar = args.named["sep"]               # pa.Scalar

    Attributes:
        positional: Tuple of positional argument values as pa.Scalar.
        named: Dictionary mapping argument names to pa.Scalar values.

    """

    positional: tuple["Scalar[Any] | None", ...] = ()
    named: dict[str, "Scalar[Any]"] | None = None

    def get(
        self,
        key: int | str,
        *,
        type: pa.DataType | None = None,
        default: Any = _MISSING,
    ) -> Any:
        """Get argument as Python value.

        Args:
            key: Positional index (int) or argument name (str).
            type: Expected Arrow type. Raises TypeError if mismatch.
            default: Value to return if argument is missing or null.
                If not provided, raises an exception for missing/null args.

        Returns:
            The argument value as a Python object.

        Raises:
            IndexError: Positional argument not found (no default provided).
            KeyError: Named argument not found (no default provided).
            ValueError: Argument is null (no default provided).
            TypeError: Argument type doesn't match `type` parameter.

        Examples:
            # Get required positional argument
            count = args.get(0)

            # Get optional argument with default
            separator = args.get("sep", default=",")
            page_size = args.get(1, default=100)

            # Get with type validation
            ratio = args.get(0, type=pa.float64())

            # Get optional with type validation
            limit = args.get("limit", type=pa.int64(), default=1000)

        """
        # Get the scalar based on key type
        if isinstance(key, int):
            # Positional argument
            if key < 0 or key >= len(self.positional):
                if default is not _MISSING:
                    return default
                raise IndexError(
                    f"Argument {key}: index out of range "
                    f"(have {len(self.positional)} positional arguments)"
                )
            scalar = self.positional[key]
        else:
            # Named argument
            if self.named is None or key not in self.named:
                if default is not _MISSING:
                    return default
                raise KeyError(f"Argument '{key}': not found")
            scalar = self.named[key]

        # Handle null values
        if scalar is None or not scalar.is_valid:
            if default is not _MISSING:
                return default
            if isinstance(key, int):
                raise ValueError(f"Argument {key}: value is null")
            else:
                raise ValueError(f"Argument '{key}': value is null")

        # Type validation (if requested)
        if type is not None and scalar.type != type:
            if isinstance(key, int):
                raise TypeError(f"Argument {key}: expected {type}, got {scalar.type}")
            else:
                raise TypeError(f"Argument '{key}': expected {type}, got {scalar.type}")

        return scalar.as_py()

    def get_varargs(
        self,
        start: int,
        *,
        type: pa.DataType | None = None,
    ) -> tuple[Any, ...]:
        """Get all positional arguments from start position onwards.

        Args:
            start: Starting positional index (inclusive).
            type: Expected Arrow type for all values. Raises TypeError if mismatch.

        Returns:
            Tuple of argument values as Python objects.

        Examples:
            # Get all args from position 2 onwards
            extra_values = args.get_varargs(2)  # Returns tuple

            # With type validation
            numbers = args.get_varargs(1, type=pa.int64())

        """
        if start < 0:
            raise ValueError(f"start must be non-negative, got {start}")

        values: list[Any] = []
        for i in range(start, len(self.positional)):
            scalar = self.positional[i]

            # Handle null values - varargs don't support nulls
            if scalar is None or not scalar.is_valid:
                raise ValueError(
                    f"Argument {i}: value is null (varargs cannot contain nulls)"
                )

            # Type validation (if requested)
            if type is not None and scalar.type != type:
                raise TypeError(f"Argument {i}: expected {type}, got {scalar.type}")

            values.append(scalar.as_py())

        return tuple(values)

    def encoded_dict(self) -> dict[str, "Scalar[Any] | None"]:
        """Convert arguments to a dictionary suitable for serialization.

        Positional arguments are stored with keys "positional_0", "positional_1", etc.
        Named arguments are stored with their actual names prefixed by "named_".

        The reason why a dictionary is used is to facilitate serialization with Arrow,
        which can easily handle flat structures, but doesn't handle variable typed
        arrays of arbitrary objects.

        Returns:
            Dictionary mapping argument names to their values.

        """
        return {
            f"positional_{index}": value for index, value in enumerate(self.positional)
        } | (
            {f"named_{name}": value for name, value in self.named.items()}
            if self.named
            else {}
        )

    def schema(self) -> pa.Schema:
        """Return Arrow schema for serializing these Arguments.

        Creates a schema with one field per argument: "positional_0", "positional_1",
        etc. for positional args, and "named_<name>" for named args. Field types
        are inferred from the argument values.

        Returns:
            Arrow schema matching the structure returned by encoded_dict().

        """
        return pa.RecordBatch.from_pylist([self.encoded_dict()]).schema

    @staticmethod
    def decode(data: pa.StructScalar) -> "Arguments":
        """Decode Arguments from a serialized dictionary.

        Args:
            data: Dictionary containing serialized argument fields.

        Returns:
            Deserialized Arguments instance.

        """
        positional: list[Scalar[Any] | None] = []
        named: dict[str, Scalar[Any]] = {}
        for key, value in data.items():
            if key.startswith("positional_"):
                index = int(key[len("positional_") :])
                while len(positional) <= index:
                    positional.append(None)
                positional[index] = value
            elif key.startswith("named_"):
                name = key[len("named_") :]
                named[name] = value
        return Arguments(positional=tuple(positional), named=named or None)


class ArgumentValidationError(ValueError):
    """Raised when an argument fails validation.

    This exception provides detailed context about what went wrong and
    suggests how to fix the issue.

    Attributes:
        arg_name: Name of the argument that failed validation.
        value: The invalid value that was provided.
        constraint: Description of the constraint that was violated.
        doc: Documentation string for the argument (if provided).
        valid_range: Human-readable description of valid values.
        default: Default value (if any) that could be used instead.
        suggestions: List of valid values close to the provided value.

    """

    def __init__(
        self,
        message: str,
        *,
        arg_name: str | None = None,
        position: int | str | None = None,
        value: Any = None,
        constraint: str | None = None,
        doc: str | None = None,
        valid_range: str | None = None,
        default: Any = _MISSING,
        choices: Sequence[Any] | None = None,
    ) -> None:
        """Initialize with rich context for helpful error messages.

        Args:
            message: Base error message.
            arg_name: Attribute name of the Arg descriptor.
            position: Positional index or named key.
            value: The value that failed validation.
            constraint: What constraint was violated (e.g., "must be >= 1").
            doc: Documentation for what this argument does.
            valid_range: Description of valid values.
            default: Default value if any.
            choices: List of valid choices if applicable.

        """
        self.arg_name = arg_name
        self.position = position
        self.value = value
        self.constraint = constraint
        self.doc = doc
        self.valid_range = valid_range
        self.default = default
        self.choices = choices

        # Build detailed message
        full_message = self._build_message(message)
        super().__init__(full_message)

    def _build_message(self, base_message: str) -> str:
        """Build a detailed, helpful error message."""
        lines = [base_message, ""]

        # Add position info
        if self.position is not None:
            if isinstance(self.position, int):
                lines.append(f"  Argument: positional argument {self.position}")
            else:
                lines.append(f"  Argument: named argument '{self.position}'")

        # Always show attribute name if set (helps identify where in code to fix)
        if self.arg_name:
            lines.append(f"  Attribute: self.{self.arg_name}")

        # Add value info
        if self.value is not None:
            lines.append(f"  Value: {self.value!r}")

        # Add constraint info
        if self.constraint:
            lines.append(f"  Constraint: {self.constraint}")

        # Add documentation
        if self.doc:
            lines.append("")
            lines.append(f"  Purpose: {self.doc}")

        # Add valid range
        if self.valid_range:
            lines.append(f"  Valid values: {self.valid_range}")

        # Add suggestions for choices
        if self.choices:
            suggestions = self._suggest_similar_choices()
            if suggestions:
                lines.append("")
                lines.append("  Did you mean:")
                for suggestion in suggestions[:3]:
                    lines.append(f"    - {suggestion!r}")

        # Add default value hint
        if self.default is not _MISSING:
            lines.append("")
            lines.append(
                f"  Tip: Omit this argument to use default value: {self.default!r}"
            )

        return "\n".join(lines)

    def _suggest_similar_choices(self) -> list[Any]:
        """Find choices similar to the provided value."""
        if not self.choices or self.value is None:
            return []

        # For strings, find similar by edit distance or prefix
        if isinstance(self.value, str):
            value_lower = self.value.lower()
            scored: list[tuple[int, Any]] = []

            for choice in self.choices:
                if isinstance(choice, str):
                    choice_lower = choice.lower()
                    # Prioritize prefix matches
                    if choice_lower.startswith(value_lower):
                        scored.append((0, choice))
                    elif value_lower.startswith(choice_lower):
                        scored.append((1, choice))
                    # Then substring matches
                    elif value_lower in choice_lower or choice_lower in value_lower:
                        scored.append((2, choice))
                    else:
                        # Simple character overlap score
                        overlap = len(set(value_lower) & set(choice_lower))
                        if overlap > len(value_lower) // 2:
                            scored.append((10 - overlap, choice))

            scored.sort(key=lambda x: x[0])
            return [choice for _, choice in scored]

        # For numbers, find closest values
        if isinstance(self.value, int | float):
            try:
                numeric_choices = [
                    c for c in self.choices if isinstance(c, int | float)
                ]
                numeric_choices.sort(key=lambda c: abs(c - self.value))
                return numeric_choices
            except TypeError:
                pass

        return list(self.choices)


# TypeVar for Arg generic type
ArgT = TypeVar("ArgT")

# Type alias for type bound predicates (e.g., pa.types.is_integer)
TypeBoundPredicate = Callable[[pa.DataType], bool]


class _ArgFactory:
    """Factory returned by Arg[type] to capture the type parameter.

    This allows Arg[str](0) to create an Arg instance with _type_param=str,
    which can be used by extract_argument_specs to infer the Arrow type.
    """

    __slots__ = ("_type_param",)

    def __init__(self, type_param: type) -> None:
        self._type_param = type_param

    def __call__(
        self,
        position: int | str,
        *,
        default: Any = _MISSING,
        doc: str = "",
        ge: float | int | None = None,
        le: float | int | None = None,
        gt: float | int | None = None,
        lt: float | int | None = None,
        choices: Sequence[Any] | None = None,
        pattern: str | None = None,
        varargs: bool = False,
        arrow_type: pa.DataType | None = None,
        type_bound: "TypeBoundPredicate | Sequence[TypeBoundPredicate] | None" = None,
        const: bool = False,
        is_any: bool = False,
    ) -> "Arg[Any]":
        """Create an Arg instance with the captured type parameter."""
        arg: Arg[Any] = Arg.__new__(Arg)
        # Manually call __init__ logic since we're using __new__
        # Validate constraint combinations
        if ge is not None and gt is not None:
            raise ValueError("Cannot specify both 'ge' and 'gt'")
        if le is not None and lt is not None:
            raise ValueError("Cannot specify both 'le' and 'lt'")
        if varargs:
            if isinstance(position, str):
                raise ValueError(
                    "varargs=True requires a positional argument (int), not named"
                )
            if default is not _MISSING:
                raise ValueError(
                    "varargs=True cannot have a default value "
                    "(requires at least 1 value)"
                )

        # Warn if type_bound is used with non-AnyArrow type
        # Check both _type_param (legacy API) and is_any (new Param API)
        if type_bound is not None and self._type_param is not AnyArrow and not is_any:
            type_name = getattr(self._type_param, "__name__", str(self._type_param))
            warnings.warn(
                f"type_bound is only meaningful for Arg[AnyArrow], "
                f"but was specified for Arg[{type_name}]",
                UserWarning,
                stacklevel=2,
            )

        arg.position = position
        arg.default = default
        arg.doc = doc
        arg.ge = ge
        arg.le = le
        arg.gt = gt
        arg.lt = lt
        arg.choices = choices
        arg.pattern = pattern
        arg.varargs = varargs
        arg.arrow_type = arrow_type
        arg.type_bound = type_bound
        arg.const = const
        arg.is_any = is_any
        arg._name = None
        arg._compiled_pattern = None
        arg._type_param = self._type_param
        # Set based on legacy Arg[AnyArrow] pattern
        arg._returns_any_arrow_value = self._type_param is AnyArrow
        # Resolution index for value lookup (may differ from position for const params)
        arg._resolution_index = None

        if pattern is not None:
            arg._compiled_pattern = re.compile(pattern)

        return arg


class Arg[ArgT]:
    """Descriptor for declarative argument parsing with optional validation.

    Use as a class attribute to declare function arguments that are automatically
    parsed from self.arguments when accessed. This eliminates the need to override
    __init__ for simple argument parsing.

    Attributes:
        position: Positional index (int) or named key (str).
        default: Default value if argument not provided. Omit for required arguments.
        doc: Documentation string for this argument.
        ge: Value must be >= this (for numeric types).
        le: Value must be <= this (for numeric types).
        gt: Value must be > this (for numeric types).
        lt: Value must be < this (for numeric types).
        choices: Value must be one of these options.
        pattern: Value must match this regex pattern (for strings).

    Examples:
        class MyFunction(TableInOutFunction):
            # Required positional argument (index 0)
            count = Arg[int](0)

            # Optional positional with default
            multiplier = Arg[int](1, default=1)

            # Required named argument
            column = Arg[str]("column")

            # Optional named with default
            format = Arg[str]("format", default="json")

            # With validation constraints
            count = Arg[int](0, ge=1, le=100, doc="Count must be 1-100")
            ratio = Arg[float](1, gt=0.0, lt=1.0, doc="Ratio in (0, 1)")
            mode = Arg[str]("mode", choices=["fast", "slow", "auto"])
            name = Arg[str]("name", pattern=r"^[a-z_][a-z0-9_]*$")

            def transform(self, batch):
                # self.count, self.multiplier, etc. are available
                # IDE knows: self.count is int, self.format is str
                # Validation happens automatically on first access
                ...

    Note:
        For named arguments (string position), the Python attribute name should
        match the SQL key. This is the standard convention::

            format = Arg[str]("format")  # Recommended: attribute == key

        Avoid using different names::

            output_format = Arg[str]("format")  # Not recommended

        While this works at runtime, it can cause issues with metadata
        serialization where only one name is preserved.

    """

    __slots__ = (
        "position",
        "default",
        "doc",
        "ge",
        "le",
        "gt",
        "lt",
        "choices",
        "pattern",
        "varargs",
        "arrow_type",
        "type_bound",
        "const",
        "is_any",
        "_name",
        "_compiled_pattern",
        "_type_param",
        "_returns_any_arrow_value",
        "_resolution_index",
    )

    def __init__(
        self,
        position: int | str,
        *,
        default: ArgT | Any = _MISSING,
        doc: str = "",
        ge: float | int | None = None,
        le: float | int | None = None,
        gt: float | int | None = None,
        lt: float | int | None = None,
        choices: Sequence[ArgT] | None = None,
        pattern: str | None = None,
        varargs: bool = False,
        arrow_type: pa.DataType | None = None,
        type_bound: "TypeBoundPredicate | Sequence[TypeBoundPredicate] | None" = None,
        const: bool = False,
        is_any: bool = False,
    ) -> None:
        """Initialize an Arg descriptor with optional validation.

        Args:
            position: Positional index (int) or named key (str).
            default: Default value if argument not provided. Omit for required.
            doc: Documentation string for this argument.
            ge: Minimum value (inclusive). Value must be >= this.
            le: Maximum value (inclusive). Value must be <= this.
            gt: Minimum value (exclusive). Value must be > this.
            lt: Maximum value (exclusive). Value must be < this.
            choices: Allowed values. Value must be one of these.
            pattern: Regex pattern for string validation.
            varargs: If True, collect all remaining positional arguments from this
                position onwards. Returns tuple[ArgT, ...]. Requires at least 1 value.
                Must be positional (not named).
            arrow_type: Explicit Arrow type for this argument. If not provided,
                type is inferred from the type hint using PYTHON_TO_ARROW.
            type_bound: Type predicate(s) for Arg[AnyArrow] column type validation.
                Accepts a single predicate (e.g., pa.types.is_integer) or a sequence
                of predicates where any match is valid (OR logic). Only meaningful
                for Arg[AnyArrow] arguments; issues a warning if used with other types.
            const: If True, marks this argument as constant-folded (ConstParam).
                Constant arguments have their values known at planning time.
            is_any: If True, indicates this argument accepts any Arrow type (AnyArrow).
                Used for tracking when AnyArrow was specified in the type hint.

        Raises:
            ValueError: If conflicting constraints are specified (e.g., ge and gt).

        """
        # Validate constraint combinations
        if ge is not None and gt is not None:
            raise ValueError("Cannot specify both 'ge' and 'gt'")
        if le is not None and lt is not None:
            raise ValueError("Cannot specify both 'le' and 'lt'")

        # Validate varargs constraints
        if varargs:
            if isinstance(position, str):
                raise ValueError(
                    "varargs=True requires a positional argument (int), not named"
                )
            if default is not _MISSING:
                raise ValueError(
                    "varargs=True cannot have a default value "
                    "(requires at least 1 value)"
                )

        self.position = position
        self.default = default
        self.doc = doc
        self.ge = ge
        self.le = le
        self.gt = gt
        self.lt = lt
        self.choices = choices
        self.pattern = pattern
        self.varargs = varargs
        self.arrow_type = arrow_type
        self.type_bound = type_bound
        self.const = const
        self.is_any = is_any
        self._name: str | None = None
        self._compiled_pattern: re.Pattern[str] | None = None
        self._type_param: type | None = None
        # Set by __init_subclass__ when using Annotated[AnyArrowValue, Arg(...)]
        self._returns_any_arrow_value: bool = False
        # Resolution index for value lookup (may differ from position for const params)
        # When set, _resolve() uses this instead of position for Arguments.get()
        self._resolution_index: int | None = None

        # Pre-compile pattern for efficiency
        if pattern is not None:
            self._compiled_pattern = re.compile(pattern)

    def __class_getitem__(cls, item: type) -> "_ArgFactory":
        """Support Arg[type] syntax to capture the type parameter at runtime.

        When you write Arg[str](0), this method is called first with item=str,
        and returns an _ArgFactory that will create Arg instances with
        _type_param set to str.
        """
        return _ArgFactory(item)

    def __set_name__(self, owner: type, name: str) -> None:
        """Store the attribute name when assigned to a class."""
        self._name = name

    @overload
    def __get__(self, obj: None, objtype: type) -> "Arg[ArgT]": ...

    @overload
    def __get__(self, obj: object, objtype: type | None = None) -> ArgT: ...

    def __get__(
        self, obj: object | None, objtype: type | None = None
    ) -> "Arg[ArgT] | ArgT":
        """Get the argument value, parsing and caching on first access."""
        if obj is None:
            return self  # Class-level access returns descriptor

        # Instance access - parse and cache
        if self._name is None:
            raise RuntimeError(
                "Arg descriptor was not properly initialized. "
                "This typically means the descriptor was accessed before __set_name__ "
                "was called. Ensure Arg is used as a class attribute, not instantiated "
                "dynamically."
            )

        if self._name not in obj.__dict__:
            obj.__dict__[self._name] = self._resolve(obj)
        return obj.__dict__[self._name]  # type: ignore[no-any-return]

    def _resolve(self, obj: object) -> ArgT:
        """Parse argument from obj.invocation.arguments and validate."""
        invocation = getattr(obj, "invocation", None)
        if invocation is None:
            raise RuntimeError(
                f"Cannot resolve Arg '{self._name}': object {type(obj).__name__} does "
                f"not have an 'invocation' attribute. Arg descriptors can only be used "
                f"on classes that have an 'invocation' attribute (e.g., "
                f"TableInOutFunction, TableFunctionGenerator)."
            )
        arguments = invocation.arguments

        # Use _resolution_index if set (for const params with separate tracking)
        # Otherwise fall back to position
        lookup_pos: int | str
        if self._resolution_index is not None:
            lookup_pos = self._resolution_index
        else:
            lookup_pos = self.position

        if self.varargs:
            # Collect all positional arguments from this position onwards
            # position is guaranteed to be int (validated in __init__)
            assert isinstance(lookup_pos, int)  # Validated in __init__
            values = arguments.get_varargs(lookup_pos)
            if len(values) == 0:
                raise ArgumentValidationError(
                    f"Argument '{self._name}' requires at least 1 value.",
                    arg_name=self._name,
                    position=self.position,
                    constraint="varargs requires at least 1 value",
                    doc=self.doc if self.doc else None,
                )
            # Validate each element
            for i, val in enumerate(values):
                self._validate_single(val, index=i)
            return values  # type: ignore[no-any-return]  # varargs returns tuple

        if self.default is _MISSING:
            value: ArgT = arguments.get(lookup_pos)
        else:
            value = arguments.get(lookup_pos, default=self.default)

        # Apply validation
        self._validate(value)

        # Wrap AnyArrow values with metadata for schema lookups
        if self._returns_any_arrow_value:
            assert self._name is not None  # Set by __set_name__
            return AnyArrowValue(value, self.position, self._name)  # type: ignore[return-value]

        return value

    def _describe_valid_range(self) -> str | None:
        """Build a human-readable description of valid values."""
        parts = []

        # Numeric bounds
        if self.ge is not None:
            parts.append(f">= {self.ge}")
        if self.gt is not None:
            parts.append(f"> {self.gt}")
        if self.le is not None:
            parts.append(f"<= {self.le}")
        if self.lt is not None:
            parts.append(f"< {self.lt}")

        if parts:
            # Format as range if we have both bounds
            if len(parts) == 2:
                lower = parts[0]
                upper = parts[1]
                return f"{lower} and {upper}"
            return " and ".join(parts)

        # Choices
        if self.choices is not None:
            if len(self.choices) <= 5:
                return ", ".join(repr(c) for c in self.choices)
            else:
                shown = ", ".join(repr(c) for c in list(self.choices)[:4])
                return f"{shown}, ... ({len(self.choices)} total options)"

        # Pattern
        if self.pattern is not None:
            return f"string matching pattern: {self.pattern}"

        return None

    def _validate(self, value: ArgT) -> None:
        """Validate value against all constraints.

        Args:
            value: The value to validate.

        Raises:
            ArgumentValidationError: If any constraint is violated.

        """
        arg_name = self._name or str(self.position)
        valid_range = self._describe_valid_range()

        # Numeric range validation
        # Note: type: ignore needed because ArgT is generic - comparisons only valid
        # for numeric types, but we can't express "ArgT when constraints are set"
        if self.ge is not None and value < self.ge:  # type: ignore[operator]
            raise ArgumentValidationError(
                f"Argument '{arg_name}' is too small.",
                arg_name=self._name,
                position=self.position,
                value=value,
                constraint=f"must be >= {self.ge}",
                doc=self.doc if self.doc else None,
                valid_range=valid_range,
                default=self.default,
            )

        if self.le is not None and value > self.le:  # type: ignore[operator]
            raise ArgumentValidationError(
                f"Argument '{arg_name}' is too large.",
                arg_name=self._name,
                position=self.position,
                value=value,
                constraint=f"must be <= {self.le}",
                doc=self.doc if self.doc else None,
                valid_range=valid_range,
                default=self.default,
            )

        if self.gt is not None and value <= self.gt:  # type: ignore[operator]
            raise ArgumentValidationError(
                f"Argument '{arg_name}' is too small.",
                arg_name=self._name,
                position=self.position,
                value=value,
                constraint=f"must be > {self.gt}",
                doc=self.doc if self.doc else None,
                valid_range=valid_range,
                default=self.default,
            )

        if self.lt is not None and value >= self.lt:  # type: ignore[operator]
            raise ArgumentValidationError(
                f"Argument '{arg_name}' is too large.",
                arg_name=self._name,
                position=self.position,
                value=value,
                constraint=f"must be < {self.lt}",
                doc=self.doc if self.doc else None,
                valid_range=valid_range,
                default=self.default,
            )

        # Choices validation
        if self.choices is not None and value not in self.choices:
            raise ArgumentValidationError(
                f"Argument '{arg_name}' has an invalid value.",
                arg_name=self._name,
                position=self.position,
                value=value,
                constraint="must be one of the allowed choices",
                doc=self.doc if self.doc else None,
                valid_range=valid_range,
                default=self.default,
                choices=self.choices,
            )

        # Pattern validation (for strings)
        if self._compiled_pattern is not None:
            if not isinstance(value, str):
                raise ArgumentValidationError(
                    f"Argument '{arg_name}' must be a string for pattern validation.",
                    arg_name=self._name,
                    position=self.position,
                    value=value,
                    constraint=f"must be a string matching pattern '{self.pattern}'",
                    doc=self.doc if self.doc else None,
                    valid_range=valid_range,
                    default=self.default,
                )
            if not self._compiled_pattern.match(value):
                raise ArgumentValidationError(
                    f"Argument '{arg_name}' does not match the required pattern.",
                    arg_name=self._name,
                    position=self.position,
                    value=value,
                    constraint=f"must match pattern '{self.pattern}'",
                    doc=self.doc if self.doc else None,
                    valid_range=valid_range,
                    default=self.default,
                )

    def _validate_single(self, value: Any, *, index: int) -> None:
        """Validate a single value from varargs against all constraints.

        Args:
            value: The value to validate.
            index: Index within the varargs tuple (for error messages).

        Raises:
            ArgumentValidationError: If any constraint is violated.

        """
        arg_name = self._name or str(self.position)
        valid_range = self._describe_valid_range()
        display_pos = f"{self.position}[{index}]"

        # Numeric range validation
        if self.ge is not None and value < self.ge:
            raise ArgumentValidationError(
                f"Argument '{arg_name}' element {index} is too small.",
                arg_name=self._name,
                position=display_pos,
                value=value,
                constraint=f"must be >= {self.ge}",
                doc=self.doc if self.doc else None,
                valid_range=valid_range,
            )

        if self.le is not None and value > self.le:
            raise ArgumentValidationError(
                f"Argument '{arg_name}' element {index} is too large.",
                arg_name=self._name,
                position=display_pos,
                value=value,
                constraint=f"must be <= {self.le}",
                doc=self.doc if self.doc else None,
                valid_range=valid_range,
            )

        if self.gt is not None and value <= self.gt:
            raise ArgumentValidationError(
                f"Argument '{arg_name}' element {index} is too small.",
                arg_name=self._name,
                position=display_pos,
                value=value,
                constraint=f"must be > {self.gt}",
                doc=self.doc if self.doc else None,
                valid_range=valid_range,
            )

        if self.lt is not None and value >= self.lt:
            raise ArgumentValidationError(
                f"Argument '{arg_name}' element {index} is too large.",
                arg_name=self._name,
                position=display_pos,
                value=value,
                constraint=f"must be < {self.lt}",
                doc=self.doc if self.doc else None,
                valid_range=valid_range,
            )

        # Choices validation
        if self.choices is not None and value not in self.choices:
            raise ArgumentValidationError(
                f"Argument '{arg_name}' element {index} has an invalid value.",
                arg_name=self._name,
                position=display_pos,
                value=value,
                constraint="must be one of the allowed choices",
                doc=self.doc if self.doc else None,
                valid_range=valid_range,
                choices=self.choices,
            )

        # Pattern validation (for strings)
        if self._compiled_pattern is not None:
            if not isinstance(value, str):
                raise ArgumentValidationError(
                    f"Argument '{arg_name}' element {index} must be a string.",
                    arg_name=self._name,
                    position=display_pos,
                    value=value,
                    constraint=f"must be a string matching pattern '{self.pattern}'",
                    doc=self.doc if self.doc else None,
                    valid_range=valid_range,
                )
            if not self._compiled_pattern.match(value):
                raise ArgumentValidationError(
                    f"Argument '{arg_name}' element {index} does not match pattern.",
                    arg_name=self._name,
                    position=display_pos,
                    value=value,
                    constraint=f"must match pattern '{self.pattern}'",
                    doc=self.doc if self.doc else None,
                    valid_range=valid_range,
                )

    def format_error(self, message: str) -> str:
        """Format an error message with argument context.

        Use this method when performing custom validation to produce
        error messages that include the argument's position and name.

        Args:
            message: The error message describing what went wrong.

        Returns:
            Formatted error message prefixed with argument context.

        Example:
            col_arg = type(self).column  # Get the Arg descriptor
            if not is_valid(self.column):
                raise SchemaValidationError(
                    col_arg.format_error("must be numeric")
                )

            # Output for positional: "Argument 'col1': must be numeric"
            # Output for named:      "Argument 'column': must be numeric"

        """
        # Use the attribute name if available
        name = self._name or str(self.position)
        return f"Argument '{name}': {message}"

    def validate_type_bound(self, field_type: pa.DataType) -> None:
        """Validate that the field type satisfies the type bound predicate(s).

        This method is called during function initialization for Arg[AnyArrow]
        arguments that have type_bound specified.

        If multiple predicates are provided, uses OR logic (any match is valid).

        Args:
            field_type: The Arrow type of the column to validate.

        Raises:
            SchemaValidationError: If the type bound is not satisfied.

        """
        from vgi.exceptions import SchemaValidationError

        if self.type_bound is None:
            return

        # Normalize to sequence
        if callable(self.type_bound):
            predicates: list[TypeBoundPredicate] = [self.type_bound]
        else:
            predicates = list(self.type_bound)

        # OR logic: at least one predicate must pass
        if not any(predicate(field_type) for predicate in predicates):
            predicate_names = [getattr(p, "__name__", str(p)) for p in predicates]
            raise SchemaValidationError(
                self.format_error(
                    f"column type {field_type} does not match any of: "
                    f"{', '.join(predicate_names)}"
                )
            )

    def __repr__(self) -> str:
        """Return a string representation of this Arg."""
        parts = [repr(self.position)]

        if self.default is not _MISSING:
            parts.append(f"default={self.default!r}")
        if self.doc:
            parts.append(f"doc={self.doc!r}")
        if self.ge is not None:
            parts.append(f"ge={self.ge!r}")
        if self.le is not None:
            parts.append(f"le={self.le!r}")
        if self.gt is not None:
            parts.append(f"gt={self.gt!r}")
        if self.lt is not None:
            parts.append(f"lt={self.lt!r}")
        if self.choices is not None:
            parts.append(f"choices={self.choices!r}")
        if self.pattern is not None:
            parts.append(f"pattern={self.pattern!r}")
        if self.varargs:
            parts.append("varargs=True")
        if self.arrow_type is not None:
            parts.append(f"arrow_type={self.arrow_type!r}")
        if self.type_bound is not None:
            if callable(self.type_bound):
                name = getattr(self.type_bound, "__name__", str(self.type_bound))
                parts.append(f"type_bound={name}")
            else:
                names = [getattr(p, "__name__", str(p)) for p in self.type_bound]
                parts.append(f"type_bound=[{', '.join(names)}]")
        if self.const:
            parts.append("const=True")
        if self.is_any:
            parts.append("is_any=True")

        return f"Arg({', '.join(parts)})"


# =============================================================================
# Param, ConstParam, Returns - Dataclasses for Scalar Function Annotations
# =============================================================================
#
# These dataclasses follow the Pydantic v2 pattern: use inside Annotated[]
# for native mypy support without # type: ignore comments.
#
# Example:
#     def compute(
#         self,
#         column: Annotated[pa.Array, Param(pa.int64(), "Input column")],
#         factor: Annotated[int, ConstParam("Multiplication factor")],
#     ) -> Annotated[pa.Array, Returns(pa.int64())]:
#         return pc.multiply(column, factor)
# =============================================================================


@dataclass(frozen=True, slots=True)
class Param:
    """Metadata for columnar parameters in compute() or class-level declarations.

    Use with Annotated to declare parameters that receive pa.Array values
    at runtime. The type information is used for catalog registration and
    argument validation.

    For ScalarFunction compute() methods, position is inferred from parameter order.
    For PolarsScalarFunction class-level attributes, specify position explicitly.

    Args:
        position: Explicit column position (for class-level attributes).
            None means position is inferred from method signature order.
        arrow_type: The Arrow data type, Polars data type, Python type
            (int/str/float/bool/bytes), or None for AnyArrow (accepts any type).
            Polars types (pl.Utf8, pl.Int64, etc.) are automatically converted
            to Arrow types internally.
        doc: Documentation string describing this parameter.
        type_bound: Type predicate(s) for validating input column types.
            Only meaningful when arrow_type is None (AnyArrow).
        varargs: If True, this parameter collects all remaining positional
            arguments as a list of arrays.

    Example (ScalarFunction compute() - position inferred):
        class AddColumns(ScalarFunction):
            def compute(
                self,
                left: Annotated[pa.Array, Param(pa.int64(), "First value")],
                right: Annotated[pa.Array, Param(pa.int64(), "Second value")],
            ) -> Annotated[pa.Array, Returns(pa.int64())]:
                return pc.add(left, right)

    Example (PolarsScalarFunction class-level - explicit position):
        class UpperCase(PolarsScalarFunction):
            text: Annotated[pl.Utf8, Param(position=0, doc="String to uppercase")]

            def compute_polars(self) -> pl.Expr:
                return pl.col("text").str.to_uppercase()

    Example (AnyArrow with type_bound):
        class Double(ScalarFunction):
            def compute(
                self,
                value: Annotated[pa.Array, Param(doc="Numeric value",
                                                  type_bound=pa.types.is_numeric)],
            ) -> Annotated[pa.Array, Returns()]:
                return pc.multiply(value, 2)

    """

    # Keep arrow_type first for backwards compatibility with Param(pa.int64(), "doc")
    # Also accepts Polars types (pl.Utf8, pl.Int64, etc.) - detected at runtime
    arrow_type: "pa.DataType | type | Any" = None
    doc: str = ""
    type_bound: "TypeBoundPredicate | Sequence[TypeBoundPredicate] | None" = None
    varargs: bool = False
    # position is keyword-only for class-level attributes (PolarsScalarFunction)
    position: int | None = None


@dataclass(frozen=True, slots=True)
class ConstParam:
    """Metadata for constant scalar parameters in compute().

    Use with Annotated to declare parameters that receive constant (non-columnar)
    values known at planning time. The type is inferred from the Annotated first
    argument (e.g., `Annotated[int, ConstParam(...)]` infers pa.int64()).

    Args:
        doc: Documentation string describing this parameter.
        arrow_type: Optional explicit Arrow type. If not provided, type is
            inferred from the Annotated first argument.
        position: Position in the argument list (required for PolarsScalarFunction,
            optional for ScalarFunction where position is inferred from signature).

    Example:
        class FormatNumber(ScalarFunction):
            def compute(
                self,
                value: Annotated[pa.Array, Param(pa.float64(), "Number to format")],
                precision: Annotated[int, ConstParam("Decimal places")],
            ) -> Annotated[pa.Array, Returns(pa.string())]:
                # precision is an int, not an array
                fmt = f"%.{precision}f"
                return pa.array([fmt % v for v in value.to_pylist()])

    Example for PolarsScalarFunction:
        class Multiply(PolarsScalarFunction):
            value: Annotated[pl.Float64, Param(position=0, doc="Value to multiply")]
            factor: Annotated[float, ConstParam("Factor", position=0)]

            def compute_polars(self) -> pl.Expr:
                return pl.col("value") * self.factor

    """

    doc: str = ""
    arrow_type: pa.DataType | type | None = None
    # Position in the argument list (for PolarsScalarFunction class-level attributes)
    position: int | None = None


@dataclass(frozen=True, slots=True)
class Returns:
    """Metadata for compute() return type.

    Use with Annotated to declare the output Arrow type for catalog registration.
    The annotation indicates that compute() returns a pa.Array of the specified type.

    Args:
        arrow_type: The Arrow data type of the output, or None for AnyArrow
            (dynamic output type determined at bind time).

    Example:
        class DoubleValue(ScalarFunction):
            def compute(
                self,
                value: Annotated[pa.Array, Param(pa.int64(), "Input value")],
            ) -> Annotated[pa.Array, Returns(pa.int64())]:
                return pc.multiply(value, 2)

        # With AnyArrow for dynamic output type (use None or omit arrow_type):
        class Identity(ScalarFunction):
            @property
            def output_type(self) -> pa.DataType:
                return self.input_schema.field(0).type

            def compute(
                self,
                value: Annotated[pa.Array, Param(doc="Value to pass through")],
            ) -> Annotated[pa.Array, Returns()]:
                return value

    """

    arrow_type: pa.DataType | None = None
