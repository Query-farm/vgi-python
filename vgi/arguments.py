# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Argument parsing and validation for VGI functions.

This module provides classes for handling function arguments in VGI:

Classes:
    [`Arguments`][]: Container for positional and named function arguments.
    [`ArgumentValidationError`][]: Raised when an argument fails validation.
    [`Arg`][]: Descriptor for declarative argument parsing with optional validation.
    [`AnyArrow`][]: Sentinel type for arguments accepting multiple Arrow types.
    [`AnyArrowValue`][]: Wrapper returned when accessing `AnyArrow` arguments.

"""

import re
import types
import typing
import warnings
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, TypeVar, overload

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass

if TYPE_CHECKING:
    from pyarrow import Scalar

# Python type to Arrow type mapping for Arg type hints
PYTHON_TO_ARROW: dict[type, pa.DataType] = {
    int: pa.int64(),
    str: pa.utf8(),
    float: pa.float64(),
    bool: pa.bool_(),
    bytes: pa.binary(),
}

# Python type to Arrow type mapping (imported by vgi.scalar_function).
_PYTHON_TO_ARROW: dict[type, pa.DataType] = {
    int: pa.int64(),
    float: pa.float64(),
    str: pa.string(),
    bool: pa.bool_(),
    bytes: pa.binary(),
}


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


def _accepts_none(annotated_inner_type: Any) -> bool:
    """Whether a declared Arg type allows ``None``.

    ``annotated_inner_type`` is the first type-arg of an
    ``Annotated[T, Arg(...)]`` hint — i.e. the user's declared type for
    the field. Returns True iff the type is a union that includes
    ``NoneType`` (e.g. ``int | None``, ``Optional[int]``,
    ``Union[int, None]``). Used by argument resolvers to reject SQL NULL
    when the user did not opt in to nullable arguments.
    """
    if annotated_inner_type is type(None):
        return True
    origin = typing.get_origin(annotated_inner_type)
    if origin is typing.Union or origin is types.UnionType:
        return type(None) in typing.get_args(annotated_inner_type)
    return False


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
    "TaggedUnion",
    "TypeBoundPredicate",
    "OutputLength",
    "Setting",
    "Secret",
    "SecretLookupEntry",
    "_extract_setting_secret_params",
]


@dataclass(frozen=True, slots=True)
class TaggedUnion:
    """A decoded union-typed argument: which member is set (``tag``) and its ``value``.

    DuckDB ``UNION`` / Arrow union arguments are *tagged*: the discriminator
    (which member is present) lives in the Arrow ``UnionScalar.type_code``, not
    in the member value. Plain ``Scalar.as_py()`` returns only the member value
    and drops that tag, so union arguments are decoded into this wrapper
    instead — ``tag`` is the active member's field name and ``value`` is its
    Python value.

    Example::

        config: Annotated[TaggedUnion, Arg("config", arrow_type=pa.sparse_union([...]))]
        ...
        cfg = params.args.config            # TaggedUnion(tag=..., value=...)
        if cfg.tag == "random_forest_classifier":
            grid = cfg.value                # the member struct, as a dict

    """

    tag: str | None
    value: Any


def _scalar_to_py(scalar: "Scalar") -> Any:
    """Convert an argument scalar to a Python value, preserving union tags.

    Identical to ``scalar.as_py()`` for every type except unions: a
    ``UnionScalar`` is decoded to a [`TaggedUnion`][] so the member
    discriminator (which ``as_py()`` discards) is retained.
    """
    if isinstance(scalar, pa.UnionScalar):
        union_type = scalar.type
        tag = next(
            (
                union_type.field(i).name
                for i in range(union_type.num_fields)
                if union_type.type_codes[i] == scalar.type_code
            ),
            None,
        )
        inner = scalar.value
        return TaggedUnion(tag=tag, value=inner.as_py() if inner is not None else None)
    return scalar.as_py()


class TableInput:
    """Sentinel type for table input parameters in table-in-out functions.

    Use this as the type parameter for [`Arg`][] to declare which argument
    receives the streaming table input. Every [`TableInOutFunction`][] must have
    exactly one [`TableInput`][] argument, and it must be positional (not named).

    The `TableInput` argument determines which table expression feeds the
    function when called from SQL. It doesn't correspond to an actual Arrow
    value - the table data arrives as streaming `RecordBatch`es via `process()`.

    """

    pass


@dataclass(frozen=True, slots=True)
class AnyArrowValue:
    """Wrapper for [`AnyArrow`][] argument values with metadata.

    When an [`Arg`][] returns an `AnyArrow` type, accessing the attribute returns
    an [`AnyArrowValue`][] instead of just the raw value. This provides access to
    both the value and the argument's position/name for schema lookups.

    Attributes:
        value: The Python value (from `scalar.as_py()`).
        position: The positional index from the `Arg` definition (`int` for positional,
            `str` for named arguments).
        name: The Python attribute name of the `Arg`.

    Example using Annotated (recommended):
        from typing import Annotated

        class MyFunction(TableFunctionGenerator):
            col1: Annotated[AnyArrowValue, Arg(0, doc="First column")]

            def on_bind(self) -> None:
                # self.col1 is an AnyArrowValue
                print(self.col1.value)     # The column name
                print(self.col1.position)  # The positional index

    Example using legacy Arg[AnyArrow] syntax:
        class MyFunction(TableFunctionGenerator):
            col1 = Arg[AnyArrow](0, doc="First column")  # type: ignore[assignment]

    """

    value: Any
    position: int | str
    name: str


class AnyArrow:
    """Sentinel type for arguments accepting multiple Arrow types.

    Use this with `[`AnyArrowValue`][]` in the Annotated pattern when an argument
    should accept multiple valid Arrow types, validated via the ``type_bound``
    parameter. When accessed, returns an `AnyArrowValue` containing the value
    plus metadata (position and name).

    Choosing Between Specific Types and AnyArrowValue
    -------------------------------------------------
    - **Single required type**: Use ``Annotated[str, Arg(...)]`` or similar.
      The argument will only accept that exact type.

    - **Multiple valid types**: Use ``Annotated[AnyArrowValue, Arg(...)]`` with
      ``type_bound`` to specify which types are acceptable. For example, numeric
      operations that work on integers, floats, and decimals should use `AnyArrowValue`.

    The ``type_bound`` parameter is ONLY meaningful for ``AnyArrowValue`` arguments.
    Using it with other types will emit a warning.

    Examples using Annotated (recommended):
        from typing import Annotated
        from vgi import Arg, AnyArrowValue

        # Single type: function only works with strings
        class UpperCaseFunction(TableFunctionGenerator):
            column: Annotated[str, Arg(0, doc="String column to uppercase")]

        # Multiple types: function works with any numeric type
        class DoubleFunction(TableFunctionGenerator):
            column: Annotated[
                AnyArrowValue,
                Arg(0, type_bound=[pa.types.is_integer, pa.types.is_floating])
            ]

            def on_bind(self) -> None:
                # Access column metadata for dynamic output type
                self._output_type = self.column.value

        # Any type: function works with all types
        class IdentityFunction(TableFunctionGenerator):
            column: Annotated[AnyArrowValue, Arg(0, doc="Column to pass through")]

    Accessing Values:
        When using `AnyArrowValue`, access the value via the ``.value`` attribute::

            val = self.column.value     # The column name as a string
            pos = self.column.position  # The positional index

    Note:
        Unlike [`TableInput`][], [`AnyArrow`][] arguments have actual Arrow values -
        they are just not constrained to a specific Arrow type.

    Attributes:
        value: The resolved Arrow value of the argument.
        position: The argument's positional index or name used to resolve it.
        name: The argument's name.
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

        SQL NULL is a real value, distinct from "argument not provided".
        ``default`` is consulted only when the caller omitted the argument
        entirely; an explicit SQL NULL returns ``None``.

        Args:
            key: Positional index (int) or argument name (str).
            type: Expected Arrow type. Raises TypeError if mismatch.
            default: Value to return if argument is omitted (not provided
                by the caller). If not provided, raises an exception for
                missing args. ``default`` is *not* consulted for explicit
                SQL NULL — that case returns ``None``.

        Returns:
            The argument value as a Python object. ``None`` if the caller
            passed an explicit SQL NULL.

        Raises:
            IndexError: Positional argument not provided (no default).
            KeyError: Named argument not provided (no default).
            TypeError: Argument type doesn't match `type` parameter.

        """
        # Get the scalar based on key type. Note: an absent argument means
        # the caller did not write it at all; the C++ extension only ships
        # fields the user supplied, so absence shows up as out-of-range
        # (positional) or missing key (named). A scalar that is present
        # but invalid is an *explicit* SQL NULL passed by the caller.
        if isinstance(key, int):
            # Positional argument
            if key < 0 or key >= len(self.positional) or self.positional[key] is None:
                if default is not _MISSING:
                    return default
                raise IndexError(
                    f"Argument {key}: index out of range (have {len(self.positional)} positional arguments)"
                )
            scalar = self.positional[key]
            assert scalar is not None  # narrowed above
        else:
            # Named argument
            if self.named is None or key not in self.named:
                if default is not _MISSING:
                    return default
                raise KeyError(f"Argument '{key}': not found")
            scalar = self.named[key]

        # Type validation (if requested)
        if type is not None and scalar.type != type:
            if isinstance(key, int):
                raise TypeError(f"Argument {key}: expected {type}, got {scalar.type}")
            else:
                raise TypeError(f"Argument '{key}': expected {type}, got {scalar.type}")

        return _scalar_to_py(scalar)

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

        """
        if start < 0:
            raise ValueError(f"start must be non-negative, got {start}")

        values: list[Any] = []
        for i in range(start, len(self.positional)):
            scalar = self.positional[i]

            # Handle null values - varargs don't support nulls
            if scalar is None or not scalar.is_valid:
                raise ValueError(f"Argument {i}: value is null (varargs cannot contain nulls)")

            # Type validation (if requested)
            if type is not None and scalar.type != type:
                raise TypeError(f"Argument {i}: expected {type}, got {scalar.type}")

            values.append(_scalar_to_py(scalar))

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
        return {f"positional_{index}": value for index, value in enumerate(self.positional)} | (
            {f"named_{name}": value for name, value in self.named.items()} if self.named else {}
        )

    def schema(self) -> pa.Schema:
        """Return Arrow schema for serializing these [`Arguments`][].

        Creates a schema with one field per argument: "positional_0", "positional_1",
        etc. for positional args, and "named_<name>" for named args. Field types
        are taken directly from scalar values to handle Arrow extension types.

        Returns:
            Arrow schema matching the structure returned by `encoded_dict()`.

        """
        args_dict = self.encoded_dict()
        fields: list[pa.Field[Any]] = []
        for key, scalar in args_dict.items():
            if scalar is None:
                fields.append(pa.field(key, pa.null()))
            else:
                fields.append(pa.field(key, scalar.type))
        return pa.schema(fields)

    @staticmethod
    def decode(data: pa.StructScalar) -> "Arguments":
        """Decode [`Arguments`][] from a serialized dictionary.

        Args:
            data: Dictionary containing serialized argument fields.

        Returns:
            Deserialized `Arguments` instance.

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

    def serialize_to_bytes(self) -> bytes:
        """Serialize [`Arguments`][] to bytes using Arrow IPC format.

        Creates a single-row `RecordBatch` with the arguments encoded as
        a struct column, then serializes it to IPC stream bytes.

        Builds the batch with explicit types from scalar values to handle
        Arrow extension types (e.g., HUGEINT) that ``from_pylist()`` cannot infer.

        Returns:
            Serialized bytes containing the `Arguments`.

        """
        args_dict = self.encoded_dict()
        fields: list[pa.Field[Any]] = []
        arrays: list[pa.Array[Any]] = []
        for key, scalar in args_dict.items():
            if scalar is None:
                fields.append(pa.field(key, pa.null()))
                arrays.append(pa.nulls(1))
            else:
                fields.append(pa.field(key, scalar.type))
                arrays.append(pa.repeat(scalar, 1))  # type: ignore[call-overload]
        if fields:
            struct_array: pa.StructArray = pa.StructArray.from_arrays(arrays, fields=fields)
        else:
            # Empty args: create a length-1 struct array with no fields
            struct_type = pa.struct([])
            struct_array = pa.array([{}], type=struct_type)  # type: ignore[assignment]
        batch = pa.RecordBatch.from_arrays([struct_array], names=["args"])
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        return sink.getvalue().to_pybytes()

    @staticmethod
    def deserialize_from_bytes(data: bytes, ipc_validation: Any = None) -> "Arguments":
        """Deserialize [`Arguments`][] from bytes.

        Args:
            data: Bytes serialized via `serialize_to_bytes()`.
            ipc_validation: Unused, accepted for compatibility with
                `ArrowSerializableDataclass._convert_value_for_deserialization`.

        Returns:
            Deserialized `Arguments` instance.

        """
        reader = pa.ipc.open_stream(data)
        batch = reader.read_next_batch()
        return Arguments.decode(batch.column("args")[0])


class ArgumentValidationError(ValueError):
    """Raised when an argument fails validation.

    This exception provides detailed context about what went wrong and
    suggests how to fix the issue.

    Attributes:
        arg_name: Name of the argument that failed validation.
        position: Positional index or named key of the argument.
        value: The invalid value that was provided.
        constraint: Description of the constraint that was violated.
        doc: Documentation string for the argument (if provided).
        valid_range: Human-readable description of valid values.
        default: Default value (if any) that could be used instead.
        choices: Valid choices, if the argument is constrained to a set.

    """

    arg_name: str | None
    position: int | str | None
    value: Any
    constraint: str | None
    doc: str | None
    valid_range: str | None
    default: Any
    choices: Sequence[Any] | None

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
            lines.append(f"  Tip: Omit this argument to use default value: {self.default!r}")

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
                numeric_choices = [c for c in self.choices if isinstance(c, int | float)]
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
    """Factory returned by `Arg[type]` to capture the type parameter.

    This allows `Arg[str](0)` to create an [`Arg`][] instance with _type_param=str,
    which can be used by `extract_argument_specs` to infer the Arrow type.
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
                raise ValueError("varargs=True requires a positional argument (int), not named")
            if default is not _MISSING:
                raise ValueError("varargs=True cannot have a default value (requires at least 1 value)")

        # Positional args cannot have defaults — DuckDB's binder always
        # requires the positional argument, so the default would never fire.
        # To make an argument optional, use a named argument (string position).
        if isinstance(position, int) and default is not _MISSING:
            raise ValueError(
                f"Arg(position={position}, default=...): positional arguments cannot "
                f"have a default value. DuckDB's binder always requires the positional "
                f"argument, so the default would never fire. To make this argument "
                f'optional, use a named argument: Arg("{{name}}", default=...).'
            )

        # Warn if type_bound is used with non-AnyArrow type
        # Check both _type_param (legacy API) and is_any (new Param API)
        if type_bound is not None and self._type_param is not AnyArrow and not is_any:
            type_name = getattr(self._type_param, "__name__", str(self._type_param))
            warnings.warn(
                f"type_bound is only meaningful for Arg[AnyArrow], but was specified for Arg[{type_name}]",
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
    parsed from `self.arguments` when accessed. This eliminates the need to override
    `__init__` for simple argument parsing.

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

    Note:
        For named arguments (string position), the Python attribute name should
        match the SQL key. This is the standard convention::

            format = Arg[str]("format")  # Recommended: attribute == key

        Avoid using different names::

            output_format = Arg[str]("format")  # Not recommended

        While this works at runtime, it can cause issues with metadata
        serialization where only one name is preserved.

    """

    # Bare annotations (storage is provided by __slots__) so the documented
    # attributes are recognized; order matches the Attributes: section.
    position: int | str
    default: ArgT | Any
    doc: str
    ge: float | int | None
    le: float | int | None
    gt: float | int | None
    lt: float | int | None
    choices: Sequence[ArgT] | None
    pattern: str | None

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
            position: Positional index (int) or named key (str). Positional
                arguments are always required (DuckDB's binder always supplies
                them); to make an argument optional, pass a string key instead.
            default: Default value if argument not provided. Only valid for
                named (string-position) arguments — passing a default with an
                integer position raises ValueError. Omit for required.
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
                type is inferred from the type hint using `PYTHON_TO_ARROW`.
            type_bound: Type predicate(s) for `Arg[AnyArrow]` column type validation.
                Accepts a single predicate (e.g., pa.types.is_integer) or a sequence
                of predicates where any match is valid (OR logic). Only meaningful
                for `Arg[AnyArrow]` arguments; issues a warning if used with other types.
            const: If True, marks this argument as constant-folded ([`ConstParam`][]).
                Constant arguments have their values known at planning time.
            is_any: If True, indicates this argument accepts any Arrow type ([`AnyArrow`][]).
                Used for tracking when `AnyArrow` was specified in the type hint.

        Raises:
            ValueError: If conflicting constraints are specified (e.g., ge and gt),
                or if a default value is supplied with an integer (positional)
                position.

        """
        # Validate constraint combinations
        if ge is not None and gt is not None:
            raise ValueError("Cannot specify both 'ge' and 'gt'")
        if le is not None and lt is not None:
            raise ValueError("Cannot specify both 'le' and 'lt'")

        # Validate varargs constraints
        if varargs:
            if isinstance(position, str):
                raise ValueError("varargs=True requires a positional argument (int), not named")
            if default is not _MISSING:
                raise ValueError("varargs=True cannot have a default value (requires at least 1 value)")

        # Positional args cannot have defaults. DuckDB's binder always requires
        # a positional argument to be supplied; the default is never consulted
        # through the SQL path. To make an argument optional, declare it as a
        # named argument by passing a string position (e.g. Arg("count", default=10)).
        if isinstance(position, int) and default is not _MISSING:
            raise ValueError(
                f"Arg(position={position}, default=...): positional arguments cannot "
                f"have a default value. DuckDB's binder always requires the positional "
                f"argument, so the default would never fire. To make this argument "
                f'optional, use a named argument: Arg("{{name}}", default=...).'
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
        """Support `Arg[type]` syntax to capture the type parameter at runtime.

        When you write `Arg[str](0)`, this method is called first with item=str,
        and returns an `_ArgFactory` that will create [`Arg`][] instances with
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

    def __get__(self, obj: object | None, objtype: type | None = None) -> "Arg[ArgT] | ArgT":
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
        """Parse argument from `obj.invocation.arguments` and validate."""
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
        lookup_pos: int | str = self._resolution_index if self._resolution_index is not None else self.position

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

        # Skip validation for None — either an explicit SQL NULL the caller
        # passed, or default=None for a nullable Arg. Numeric/choice/pattern
        # constraints don't apply to None and would otherwise TypeError.
        if value is not None:
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

    def _reject_none(self) -> "ArgumentValidationError":
        """Build the error raised when SQL NULL is passed to a non-Optional Arg.

        Callers ``_parse_arguments`` (table_function.py) and ``_resolve``
        (this module) hit ``_validate`` with a None value when the user
        wrote e.g. ``my_func(NULL)``. ``_validate``'s numeric/choice/pattern
        comparisons would then crash with a Python ``TypeError`` deep in
        the worker — which surfaces in the C++ extension as an opaque
        traceback rather than a clean argument error. Callers use this
        helper to emit a structured error before reaching ``_validate``.
        """
        arg_name = self._name or str(self.position)
        return ArgumentValidationError(
            f"Argument '{arg_name}' cannot be NULL.",
            arg_name=self._name,
            position=self.position,
            value=None,
            constraint="must not be NULL (declare type as `T | None` to accept SQL NULL)",
            doc=self.doc if self.doc else None,
            valid_range=self._describe_valid_range(),
            default=self.default,
        )

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

        """
        # Use the attribute name if available
        name = self._name or str(self.position)
        return f"Argument '{name}': {message}"

    def validate_type_bound(self, field_type: pa.DataType) -> None:
        """Validate that the field type satisfies the type bound predicate(s).

        This method is called during function initialization for `Arg[AnyArrow]`
        arguments that have type_bound specified.

        If multiple predicates are provided, uses OR logic (any match is valid).

        Args:
            field_type: The Arrow type of the column to validate.

        Raises:
            [`SchemaValidationError`][]: If the type bound is not satisfied.

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
                self.format_error(f"column type {field_type} does not match any of: {', '.join(predicate_names)}")
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
#     @classmethod
#     def compute(
#         cls,
#         column: Annotated[pa.Array, Param(pa.int64(), "Input column")],
#         factor: Annotated[int, ConstParam("Multiplication factor")],
#     ) -> Annotated[pa.Array, Returns(pa.int64())]:
#         return pc.multiply(column, factor)
# =============================================================================


@dataclass(frozen=True, slots=True)
class Param:
    """Metadata for columnar parameters in `compute()` or class-level declarations.

    Use with Annotated to declare parameters that receive `pa.Array` values
    at runtime. The type information is used for catalog registration and
    argument validation.

    For [`ScalarFunction`][] `compute()` methods, position is inferred from parameter order.

    Attributes:
        arrow_type: The Arrow data type, Python type
            (int/str/float/bool/bytes), or None for [`AnyArrow`][] (accepts any type).
        doc: Documentation string describing this parameter.
        type_bound: Type predicate(s) for validating input column types.
            Only meaningful when arrow_type is None (`AnyArrow`).
        varargs: If True, this parameter collects all remaining positional
            arguments as a list of arrays.
        position: Explicit column position (for class-level attributes).
            None means position is inferred from method signature order.

    Example (ScalarFunction compute() - position inferred):
        class AddColumns(ScalarFunction):
            @classmethod
            def compute(
                cls,
                left: Annotated[pa.Array, Param(pa.int64(), "First value")],
                right: Annotated[pa.Array, Param(pa.int64(), "Second value")],
            ) -> Annotated[pa.Array, Returns(pa.int64())]:
                return pc.add(left, right)

    Example (AnyArrow with type_bound):
        class Double(ScalarFunction):
            @classmethod
            def compute(
                cls,
                value: Annotated[pa.Array, Param(doc="Numeric value",
                                                  type_bound=pa.types.is_numeric)],
            ) -> Annotated[pa.Array, Returns()]:
                return pc.multiply(value, 2)

    """

    # Keep arrow_type first for backwards compatibility with Param(pa.int64(), "doc")
    arrow_type: pa.DataType | type | None = None
    doc: str = ""
    type_bound: "TypeBoundPredicate | Sequence[TypeBoundPredicate] | None" = None
    varargs: bool = False
    position: int | None = None


@dataclass(frozen=True, slots=True)
class ConstParam:
    """Metadata for constant scalar parameters in `compute()`.

    Use with Annotated to declare parameters that receive constant (non-columnar)
    values known at planning time. The type is inferred from the Annotated first
    argument (e.g., `Annotated[int, ConstParam(...)]` infers pa.int64()).

    Attributes:
        doc: Documentation string describing this parameter.
        arrow_type: Optional explicit Arrow type. If not provided, type is
            inferred from the Annotated first argument.
        position: Position in the argument list
            (optional for [`ScalarFunction`][] where position is inferred from signature).
        phase: Phase when this const param is needed (aggregate functions only).
            ``"all"`` = every callback, ``"update"`` = only update,
            ``"finalize"`` = only finalize.

    """

    doc: str = ""
    arrow_type: pa.DataType | type | None = None
    position: int | None = None
    phase: str = "all"


@dataclass(frozen=True, slots=True)
class Setting:
    """Metadata for settings parameter in `compute()`.

    Use with Annotated to declare parameters that receive setting values
    from the DuckDB session. Settings are string key-value pairs.

    Attributes:
        key: The setting key name. If not provided, uses the parameter name.

    """

    key: str | None = None


@dataclass(frozen=True, slots=True)
class Secret:
    """Metadata for secrets parameter in `compute()` or `on_bind()`.

    Use with Annotated to declare parameters that receive secret values
    from the DuckDB `SecretManager`. Secrets contain multiple key-value pairs
    where keys are strings and values can be any DuckDB type.

    Attributes:
        secret_type: The secret type to look up (e.g., "vgi_example", "s3").
            Required — C++ enforces type matching.
        name: Optional secret name for name-based lookup.
        scope: Optional static scope for pre-resolution (resolved before first bind call).

    Examples:
        Secret("vgi_example")                    — unscoped lookup by type
        Secret("s3", name="my_cred")             — type + name-based lookup
        Secret("s3", scope="s3://bucket/")       — type + scope (pre-resolved)
        Secret("s3", name="my_cred", scope="s3://bucket/")  — all three

    """

    secret_type: str
    name: str | None = None
    scope: str | None = None


@dataclass(frozen=True, slots=True)
class SecretLookupEntry(ArrowSerializableDataclass):
    """A request to look up a specific secret.

    Used both in function metadata (static requirements from annotations)
    and in runtime requests (dynamic scoped lookups).  Also used directly
    as the catalog-level secret requirement type (replacing the former
    ``CatalogSecretRequirement`` which had identical fields).

    Extends ``ArrowSerializableDataclass`` so it can be serialized in
    catalog `[`FunctionInfo`][]` payloads.

    secret_type is required — C++ enforces type matching.

    Supported lookup patterns:
    - By type only:           SecretLookupEntry(secret_type="s3")
    - By type + scope:        SecretLookupEntry(secret_type="s3", scope="s3://bucket/")
    - By type + name:         SecretLookupEntry(secret_type="s3", secret_name="my_cred")
    - By type + scope + name: all three fields set

    Attributes:
        secret_type: The DuckDB secret type to match (required; C++ enforces
            type matching).
        scope: Optional URI prefix the secret must apply to.
        secret_name: Optional name of the specific secret to resolve.
    """

    secret_type: str
    scope: str | None = None
    secret_name: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        """Convert to dictionary for serialization."""
        return {
            "secret_type": self.secret_type,
            "secret_name": self.secret_name,
            "scope": self.scope,
        }

    @staticmethod
    def from_dict(d: dict[str, Any]) -> "SecretLookupEntry":
        """Create from dictionary."""
        return SecretLookupEntry(
            secret_type=d["secret_type"],
            secret_name=d.get("secret_name"),
            scope=d.get("scope"),
        )


def _extract_setting_secret_params(
    method: Any,
) -> tuple[dict[str, str], dict[str, Secret]]:
    """Extract Setting/Secret annotations from a method signature.

    Parses the method's type hints to find parameters annotated with
    `Setting()` or `Secret()`, returning mappings from parameter name to key/[`Secret`][].

    Handles ``from __future__ import annotations`` (string annotations)
    using an eval-with-namespace fallback.

    Args:
        method: The method to inspect (e.g., compute, on_bind).

    Returns:
        Tuple of (setting_params, secret_params) where:
        - setting_params: dict mapping ``param_name -> setting_key``
        - secret_params: dict mapping ``param_name -> Secret`` instance

    """
    import contextlib
    import inspect
    from typing import get_type_hints

    sig = inspect.signature(method)

    # Try to get type hints (handles PEP 563 string annotations)
    hints: dict[str, Any] = {}
    with contextlib.suppress(Exception):
        hints = get_type_hints(method, include_extras=True)

    # Fallback for `from __future__ import annotations`
    if not hints:
        import pyarrow as pa

        raw_annotations = getattr(method, "__annotations__", {})
        from typing import Annotated

        # Create a mock pa module with subscriptable Scalar for eval
        # (pa.Scalar[Any] isn't subscriptable in PyArrow at runtime)
        class _MockScalar:
            def __class_getitem__(cls, _item: Any) -> Any:
                return Any

        class _MockPa:
            Scalar = _MockScalar

            def __getattr__(self, attr_name: str) -> Any:
                return getattr(pa, attr_name)

        eval_namespace = {
            **getattr(method, "__globals__", {}),
            "Annotated": Annotated,
            "Setting": Setting,
            "Secret": Secret,
            "pa": _MockPa(),
        }
        for name, annotation in raw_annotations.items():
            if isinstance(annotation, str):
                with contextlib.suppress(Exception):
                    hints[name] = eval(annotation, eval_namespace)  # noqa: S307
            else:
                hints[name] = annotation

    setting_params: dict[str, str] = {}
    secret_params: dict[str, Secret] = {}

    for name in sig.parameters:
        if name in ("self", "cls"):
            continue

        hint = hints.get(name)
        if hint is None or not hasattr(hint, "__metadata__"):
            continue

        for meta in hint.__metadata__:
            if isinstance(meta, Setting):
                setting_key = meta.key if meta.key is not None else name
                setting_params[name] = setting_key
                break
            if isinstance(meta, Secret):
                secret_params[name] = meta
                break

    return setting_params, secret_params


@dataclass(frozen=True, slots=True)
class Auth:
    """Metadata for auth context parameter in `compute()`.

    Use with Annotated to declare a parameter that receives the `AuthContext`
    for the current request. Returns `AuthContext.anonymous()` when no
    authentication is configured (including stdio transport).

    """


@dataclass(frozen=True, slots=True)
class OutputLength:
    """Metadata for output length parameter in `compute()`.

    Use with Annotated to declare a parameter that receives the number of rows
    in the input batch. This is useful for scalar functions that don't take
    any column arguments but need to know how many output values to produce.

    """

    pass


@dataclass(frozen=True, slots=True)
class Returns:
    """Metadata for `compute()` return type.

    Use with Annotated to declare the output Arrow type for catalog registration.
    The annotation indicates that `compute()` returns a `pa.Array` of the specified type.

    Attributes:
        arrow_type: The Arrow data type of the output, or None for [`AnyArrow`][]
            (dynamic output type determined at bind time).

    """

    arrow_type: pa.DataType | None = None
