"""Argument parsing and validation for VGI functions.

This module provides classes for handling function arguments in VGI:

Classes:
    Arguments: Container for positional and named function arguments.
    ArgumentValidationError: Raised when an argument fails validation.
    Arg: Descriptor for declarative argument parsing with optional validation.

Example:
    # Using Arg descriptor for declarative parsing
    class MyFunction(TableInOutFunction):
        count = Arg[int](0)  # Required positional
        name = Arg[str]("name", default="unnamed")  # Optional named

    # Using Arguments.get() for manual parsing
    count = args.get(0)
    name = args.get("name", default="unnamed")

"""

import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, TypeVar, overload

import pyarrow as pa

# Sentinel for missing default value
_MISSING: Any = object()

__all__ = [
    "Arg",
    "ArgumentValidationError",
    "Arguments",
    "TableInput",
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

    positional: tuple[pa.Scalar | None, ...] = ()
    named: dict[str, pa.Scalar] | None = None

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

    def encoded_dict(self) -> dict[str, pa.Scalar | None]:
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
        positional: list[pa.Scalar | None] = []
        named: dict[str, pa.Scalar] = {}
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
        "_name",
        "_compiled_pattern",
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

        Raises:
            ValueError: If conflicting constraints are specified (e.g., ge and gt).

        """
        # Validate constraint combinations
        if ge is not None and gt is not None:
            raise ValueError("Cannot specify both 'ge' and 'gt'")
        if le is not None and lt is not None:
            raise ValueError("Cannot specify both 'le' and 'lt'")

        self.position = position
        self.default = default
        self.doc = doc
        self.ge = ge
        self.le = le
        self.gt = gt
        self.lt = lt
        self.choices = choices
        self.pattern = pattern
        self._name: str | None = None
        self._compiled_pattern: re.Pattern[str] | None = None

        # Pre-compile pattern for efficiency
        if pattern is not None:
            self._compiled_pattern = re.compile(pattern)

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
            raise RuntimeError("Arg descriptor was not properly initialized")

        if self._name not in obj.__dict__:
            obj.__dict__[self._name] = self._resolve(obj)
        return obj.__dict__[self._name]  # type: ignore[no-any-return]

    def _resolve(self, obj: object) -> ArgT:
        """Parse argument from obj.invocation.arguments and validate."""
        invocation = getattr(obj, "invocation", None)
        if invocation is None:
            raise RuntimeError("Object does not have 'invocation' attribute")
        arguments = invocation.arguments

        if self.default is _MISSING:
            value: ArgT = arguments.get(self.position)
        else:
            value = arguments.get(self.position, default=self.default)

        # Apply validation
        self._validate(value)

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

        return f"Arg({', '.join(parts)})"
