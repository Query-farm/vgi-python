"""Function metadata for introspection, documentation, and DuckDB registration.

This module provides declarative metadata classes that enable functions to
describe themselves. Metadata is used for:

1. Documentation generation
2. Worker registration (serialized to Arrow for IPC)
3. DuckDB function catalog integration
4. Tooling and discovery

DESIGN
------
Users define a nested `Meta` class with attributes. No inheritance required:

    class MyFunction(TableInOutFunction):
        class Meta:
            name = "my_function"
            description = "Transform data in some way"
            categories = ["transform"]
            max_workers = 4

        count = Arg[int](0, doc="Number of iterations")

        def transform(self, batch):
            ...

The system automatically:
- Resolves metadata from the class hierarchy (inheritance works)
- Extracts parameter info from Arg descriptors
- Infers function name from class name if not specified
- Uses docstring as description fallback

ARROW SERIALIZATION
-------------------
For worker registration, metadata can be serialized to Arrow:

    from vgi.metadata import functions_to_arrow, arrow_to_functions

    # Worker sends available functions to client
    batch = functions_to_arrow([MyFunction, OtherFunction])

    # Client receives and deserializes
    function_infos = arrow_to_functions(batch)

"""

from __future__ import annotations

import functools
import json
import re
import warnings
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, get_type_hints

import pyarrow as pa

from vgi.arguments import _MISSING, TableInput

if TYPE_CHECKING:
    from vgi.arguments import Arg

__all__ = [
    # Enums
    "FunctionStability",
    "FunctionType",
    "NullHandling",
    "OrderPreservation",
    "OrderDependence",
    "DistinctDependence",
    # Data classes
    "ParameterInfo",
    "FunctionExample",
    "ResolvedMetadata",
    # Resolution
    "resolve_metadata",
    "extract_parameters",
    # Exceptions
    "FunctionTypeError",
    "TableInputValidationError",
    # Arrow serialization
    "metadata_to_arrow",
    "metadatas_to_arrow",
    "arrow_to_metadata",
    "functions_to_arrow",
    "arrow_to_functions",
    # Mixin
    "MetadataMixin",
]


# =============================================================================
# Enums
# =============================================================================


class FunctionType(Enum):
    """Type of function for DuckDB registration."""

    SCALAR = auto()
    """Scalar function: one output per input row."""

    AGGREGATE = auto()
    """Aggregate function: many inputs → one output."""

    TABLE = auto()
    """Table function: returns a table."""


class FunctionStability(Enum):
    """Function output stability classification.

    Maps to DuckDB's FunctionStability enum.
    """

    CONSISTENT = auto()
    """Same input always produces same output (deterministic)."""

    VOLATILE = auto()
    """Output may change per row even with same input (e.g., random())."""

    CONSISTENT_WITHIN_QUERY = auto()
    """Same within a query, but may vary across queries (e.g., now())."""


class NullHandling(Enum):
    """NULL input handling behavior.

    Maps to DuckDB's FunctionNullHandling enum.
    """

    DEFAULT = auto()
    """NULL in → NULL out (standard SQL behavior)."""

    SPECIAL = auto()
    """Function handles NULLs specially (e.g., COALESCE, IFNULL)."""


class OrderPreservation(Enum):
    """Row order preservation behavior.

    Maps to DuckDB's OrderPreservationType enum.
    """

    PRESERVES_ORDER = auto()
    """Output rows are in same order as input rows."""

    NO_ORDER_GUARANTEE = auto()
    """Output order is undefined (may be reordered)."""


class OrderDependence(Enum):
    """Aggregate order sensitivity.

    Maps to DuckDB's AggregateOrderDependent enum.
    """

    ORDER_DEPENDENT = auto()
    """Result changes based on row order (e.g., FIRST, LAST, LISTAGG)."""

    NOT_ORDER_DEPENDENT = auto()
    """Result is the same regardless of order (e.g., SUM, COUNT)."""


class DistinctDependence(Enum):
    """Aggregate DISTINCT modifier sensitivity.

    Maps to DuckDB's AggregateDistinctDependent enum.
    """

    DISTINCT_DEPENDENT = auto()
    """DISTINCT changes the result (e.g., COUNT DISTINCT)."""

    NOT_DISTINCT_DEPENDENT = auto()
    """DISTINCT has no effect (e.g., MAX, MIN)."""


# =============================================================================
# Data Classes
# =============================================================================


@dataclass(frozen=True)
class ParameterInfo:
    """Metadata about a function parameter.

    Automatically extracted from Arg descriptors.

    Attributes:
        name: Parameter name (attribute name from class).
        position: Positional index (int) or named key (str).
        type_name: Type name as string (e.g., "int", "str", "TableInput").
        description: Documentation from Arg.doc.
        required: True if no default value.
        default: Default value, or None if required.
        constraints: Validation constraints as dict.
        is_table_input: True if this is the table input parameter.

    """

    name: str
    position: int | str
    type_name: str | None = None
    description: str = ""
    required: bool = True
    default: Any = None
    constraints: dict[str, Any] = field(default_factory=dict)
    is_table_input: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "position": self.position if isinstance(self.position, int) else None,
            "position_name": self.position if isinstance(self.position, str) else None,
            "type_name": self.type_name,
            "description": self.description,
            "required": self.required,
            "default": repr(self.default) if self.default is not None else None,
            "constraints": json.dumps(self.constraints) if self.constraints else None,
            "is_table_input": self.is_table_input,
        }

    @staticmethod
    def from_dict(d: dict[str, Any]) -> ParameterInfo:
        """Create from dictionary."""
        position: int | str
        if d.get("position") is not None:
            position = d["position"]
        elif d.get("position_name") is not None:
            position = d["position_name"]
        else:
            position = 0

        constraints = {}
        if d.get("constraints"):
            constraints = json.loads(d["constraints"])

        return ParameterInfo(
            name=d["name"],
            position=position,
            type_name=d.get("type_name"),
            description=d.get("description", ""),
            required=d.get("required", True),
            default=d.get("default"),
            constraints=constraints,
            is_table_input=d.get("is_table_input", False),
        )


@dataclass(frozen=True)
class FunctionExample:
    """An example usage of a function.

    Attributes:
        sql: SQL query demonstrating the function.
        description: What this example demonstrates.
        expected_output: Optional expected result description.

    """

    sql: str
    description: str = ""
    expected_output: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "sql": self.sql,
            "description": self.description,
            "expected_output": self.expected_output,
        }

    @staticmethod
    def from_dict(d: dict[str, Any]) -> FunctionExample:
        """Create from dictionary."""
        return FunctionExample(
            sql=d["sql"],
            description=d.get("description", ""),
            expected_output=d.get("expected_output"),
        )


@dataclass
class ResolvedMetadata:
    """Fully resolved metadata for a function.

    This is the result of resolving a Meta class hierarchy and extracting
    parameter information from Arg descriptors.

    """

    # Identity
    name: str
    class_name: str
    function_type: FunctionType

    # Documentation
    description: str = ""
    examples: list[FunctionExample] = field(default_factory=list)
    categories: list[str] = field(default_factory=list)
    parameters: list[ParameterInfo] = field(default_factory=list)

    # Behavior (all functions)
    stability: FunctionStability = FunctionStability.CONSISTENT
    null_handling: NullHandling = NullHandling.DEFAULT

    # DuckDB settings required by the function
    required_settings: list[str] = field(default_factory=list)

    # Table function specific
    projection_pushdown: bool = True
    filter_pushdown: bool = False
    preserves_order: OrderPreservation = OrderPreservation.PRESERVES_ORDER
    max_workers: int | None = None

    # Aggregate function specific
    order_dependent: OrderDependence = OrderDependence.NOT_ORDER_DEPENDENT
    distinct_dependent: DistinctDependence = DistinctDependence.NOT_DISTINCT_DEPENDENT

    # Scalar function specific
    return_type: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "class_name": self.class_name,
            "function_type": self.function_type.name,
            "description": self.description,
            "examples": [ex.to_dict() for ex in self.examples],
            "categories": self.categories,
            "parameters": [p.to_dict() for p in self.parameters],
            "stability": self.stability.name,
            "null_handling": self.null_handling.name,
            "required_settings": self.required_settings,
            "projection_pushdown": self.projection_pushdown,
            "filter_pushdown": self.filter_pushdown,
            "preserves_order": self.preserves_order.name,
            "max_workers": self.max_workers,
            "order_dependent": self.order_dependent.name,
            "distinct_dependent": self.distinct_dependent.name,
            "return_type": self.return_type,
        }

    @staticmethod
    def from_dict(d: dict[str, Any]) -> ResolvedMetadata:
        """Create from dictionary."""
        return ResolvedMetadata(
            name=d["name"],
            class_name=d["class_name"],
            function_type=FunctionType[d["function_type"]],
            description=d.get("description", ""),
            examples=[FunctionExample.from_dict(ex) for ex in d.get("examples", [])],
            categories=d.get("categories", []),
            parameters=[ParameterInfo.from_dict(p) for p in d.get("parameters", [])],
            stability=FunctionStability[d.get("stability", "CONSISTENT")],
            null_handling=NullHandling[d.get("null_handling", "DEFAULT")],
            required_settings=d.get("required_settings", []),
            projection_pushdown=d.get("projection_pushdown", True),
            filter_pushdown=d.get("filter_pushdown", False),
            preserves_order=OrderPreservation[
                d.get("preserves_order", "PRESERVES_ORDER")
            ],
            max_workers=d.get("max_workers"),
            order_dependent=OrderDependence[
                d.get("order_dependent", "NOT_ORDER_DEPENDENT")
            ],
            distinct_dependent=DistinctDependence[
                d.get("distinct_dependent", "NOT_DISTINCT_DEPENDENT")
            ],
            return_type=d.get("return_type"),
        )


# =============================================================================
# Parameter Extraction from Arg Descriptors
# =============================================================================


def _get_arg_type_info(cls: type, attr_name: str) -> tuple[str | None, bool]:
    """Extract type name and TableInput status from type hints for an Arg attribute.

    Returns:
        Tuple of (type_name, is_table_input).

    """
    try:
        hints = get_type_hints(cls)
    except (NameError, AttributeError):
        # NameError: Forward references can't be resolved (common with TYPE_CHECKING)
        # AttributeError: Issues accessing class attributes during resolution
        return (None, False)

    if attr_name not in hints:
        return (None, False)

    hint = hints[attr_name]

    # Check if it's TableInput
    if hint is TableInput:
        return ("TableInput", True)

    # Extract type name
    if hasattr(hint, "__name__"):
        return (hint.__name__, False)

    return (str(hint), False)


class TableInputValidationError(ValueError):
    """Raised when TableInput parameter validation fails."""


def _build_constraints(arg: Arg[Any]) -> dict[str, Any]:
    """Extract validation constraints from an Arg descriptor."""
    constraints: dict[str, Any] = {}

    # Numeric bounds
    for name in ("ge", "le", "gt", "lt"):
        value = getattr(arg, name)
        if value is not None:
            constraints[name] = value

    # Other constraints
    if arg.choices is not None:
        constraints["choices"] = list(arg.choices)
    if arg.pattern is not None:
        constraints["pattern"] = arg.pattern

    return constraints


def extract_parameters(
    cls: type, *, validate_table_input: bool = True
) -> list[ParameterInfo]:
    """Extract parameter information from Arg descriptors on a class.

    Walks the class and its bases to find all Arg descriptors and converts
    them to ParameterInfo objects.

    Args:
        cls: The function class to extract parameters from.
        validate_table_input: If True, validates TableInput requirements for
            TableInOutFunction subclasses.

    Returns:
        List of ParameterInfo objects, sorted by position.

    Raises:
        TableInputValidationError: If TableInput validation fails.

    """
    # Import here to avoid circular imports
    from vgi.arguments import Arg

    parameters: list[ParameterInfo] = []
    seen_names: set[str] = set()

    # Walk MRO to find all Arg descriptors
    for klass in cls.__mro__:
        if klass is object:
            continue

        for attr_name, attr_value in vars(klass).items():
            if attr_name.startswith("_"):
                continue
            if attr_name in seen_names:
                continue

            if isinstance(attr_value, Arg):
                seen_names.add(attr_name)
                arg: Arg[Any] = attr_value
                required = arg.default is _MISSING
                type_name, is_table_input = _get_arg_type_info(cls, attr_name)

                parameters.append(
                    ParameterInfo(
                        name=attr_name,
                        position=arg.position,
                        type_name=type_name,
                        description=arg.doc,
                        required=required,
                        default=None if required else arg.default,
                        constraints=_build_constraints(arg),
                        is_table_input=is_table_input,
                    )
                )

    # Sort: positional args by index first, then named args alphabetically
    def sort_key(p: ParameterInfo) -> tuple[int, int | str]:
        if isinstance(p.position, int):
            return (0, p.position)
        return (1, p.position)

    sorted_params = sorted(parameters, key=sort_key)

    # Validate TableInput constraints if any are present
    if validate_table_input:
        _validate_table_input(cls, sorted_params)

    return sorted_params


def _validate_table_input(cls: type, parameters: list[ParameterInfo]) -> None:
    """Validate TableInput parameter constraints.

    If a function has TableInput parameters, validates that:
    - There is exactly one TableInput parameter
    - The TableInput parameter is positional (not named)

    Args:
        cls: The function class being validated.
        parameters: Extracted parameters.

    Raises:
        TableInputValidationError: If validation fails.

    """
    table_inputs = [p for p in parameters if p.is_table_input]

    if len(table_inputs) == 0:
        return  # No TableInput parameters, nothing to validate

    if len(table_inputs) > 1:
        names = [p.name for p in table_inputs]
        raise TableInputValidationError(
            f"{cls.__name__}: Functions can have at most one Arg[TableInput] "
            f"parameter, but found {len(table_inputs)}: {names}"
        )

    table_input = table_inputs[0]

    # TableInput must be positional (not named)
    if isinstance(table_input.position, str):
        raise TableInputValidationError(
            f"{cls.__name__}: TableInput parameter '{table_input.name}' must be "
            f"positional (int), not named. Change from "
            f"Arg[TableInput]('{table_input.position}') to "
            f"Arg[TableInput](<position_index>)"
        )


# =============================================================================
# Metadata Resolution
# =============================================================================


def _normalize_examples(
    examples: list[FunctionExample | str],
) -> list[FunctionExample]:
    """Convert string examples to FunctionExample objects."""
    return [FunctionExample(sql=ex) if isinstance(ex, str) else ex for ex in examples]


# Mapping from base class names to FunctionType.
# Using a dict avoids typos and provides O(1) lookup.
# Class names are used (not classes) to avoid circular imports.
# Note: Functions with an Arg[TableInput] parameter receive table input.
_CLASS_NAME_TO_FUNCTION_TYPE: dict[str, FunctionType] = {
    # Table functions (including those with table input)
    "TableFunctionBase": FunctionType.TABLE,
    # Future function types (not yet implemented)
    "AggregateFunction": FunctionType.AGGREGATE,
    "ScalarFunction": FunctionType.SCALAR,
}

# Valid Meta class attribute names (for typo detection)
_VALID_META_ATTRIBUTES: frozenset[str] = frozenset(
    {
        # Common
        "name",
        "description",
        "examples",
        "categories",
        "stability",
        "null_handling",
        "required_settings",  # DuckDB settings/pragmas required by function
        # Table function specific
        "projection_pushdown",
        "filter_pushdown",
        "preserves_order",
        "max_workers",
        # Aggregate function specific
        "order_dependent",
        "distinct_dependent",
        # Scalar function specific
        "return_type",
    }
)


class FunctionTypeError(TypeError):
    """Raised when a function's type cannot be determined from its class hierarchy."""


def _infer_function_type(cls: type) -> FunctionType:
    """Infer the function type from the class hierarchy.

    Raises:
        FunctionTypeError: If no recognized base class is found in the MRO.

    """
    for klass in cls.__mro__:
        if klass.__name__ in _CLASS_NAME_TO_FUNCTION_TYPE:
            return _CLASS_NAME_TO_FUNCTION_TYPE[klass.__name__]
    recognized_bases = sorted(_CLASS_NAME_TO_FUNCTION_TYPE.keys())
    raise FunctionTypeError(
        f"Cannot determine function type for {cls.__name__}. "
        f"Class must inherit from one of: {recognized_bases}"
    )


@functools.lru_cache(maxsize=256)
def resolve_metadata(cls: type) -> ResolvedMetadata:
    """Resolve metadata for a function class.

    Results are cached since class metadata doesn't change at runtime.

    This function:
    1. Walks the class hierarchy to find and merge Meta classes
    2. Extracts parameter info from Arg descriptors
    3. Infers function name from class name if not specified
    4. Uses docstring as description fallback

    Args:
        cls: The function class to resolve metadata for.

    Returns:
        ResolvedMetadata with all resolved values.

    Example:
        class MyFunction(TableInOutFunction):
            class Meta:
                description = "My function"
                max_workers = 2

            count = Arg[int](0, doc="Count parameter")

        meta = resolve_metadata(MyFunction)
        print(meta.name)  # "my" (snake_case, suffix removed)
        print(meta.class_name)  # "MyFunction"
        print(meta.description)  # "My function"

    """
    # Collect all attributes from Meta classes in MRO
    attrs: dict[str, Any] = {}

    # Walk MRO in reverse so derived classes override base classes
    for klass in reversed(cls.__mro__):
        if klass is object:
            continue

        # Check for nested Meta class defined directly on this class
        if "Meta" not in klass.__dict__:
            continue

        meta_class = klass.__dict__["Meta"]

        # Extract class attributes defined directly on this Meta class
        for attr_name, value in vars(meta_class).items():
            if attr_name.startswith("_"):
                continue
            # Skip methods
            if callable(value) and not isinstance(value, type):
                continue
            attrs[attr_name] = value

    # Warn about unknown Meta attributes (likely typos)
    unknown_attrs = set(attrs.keys()) - _VALID_META_ATTRIBUTES
    if unknown_attrs:
        warnings.warn(
            f"{cls.__name__}.Meta has unknown attributes: {sorted(unknown_attrs)}. "
            f"Valid attributes are: {sorted(_VALID_META_ATTRIBUTES)}",
            stacklevel=2,
        )

    # Infer function type from class hierarchy
    function_type = _infer_function_type(cls)

    # Use class name as default name, converting to snake_case
    class_name = cls.__name__
    if "name" in attrs and attrs["name"]:
        name = attrs["name"]
    else:
        # Convert CamelCase to snake_case
        name = re.sub(r"(?<!^)(?=[A-Z])", "_", class_name).lower()
        # Remove common suffixes
        for suffix in ["_function", "_func"]:
            if name.endswith(suffix):
                name = name[: -len(suffix)]
                break

    # Use docstring as fallback description
    description = attrs.get("description", "")
    if not description and cls.__doc__:
        description = cls.__doc__.strip().split("\n")[0]

    # Normalize examples
    examples = _normalize_examples(attrs.get("examples", []))

    # Extract parameters from Arg descriptors
    parameters = extract_parameters(cls)

    return ResolvedMetadata(
        name=name,
        class_name=class_name,
        function_type=function_type,
        description=description,
        examples=examples,
        categories=attrs.get("categories", []),
        parameters=parameters,
        stability=attrs.get("stability", FunctionStability.CONSISTENT),
        null_handling=attrs.get("null_handling", NullHandling.DEFAULT),
        required_settings=attrs.get("required_settings", []),
        projection_pushdown=attrs.get("projection_pushdown", True),
        filter_pushdown=attrs.get("filter_pushdown", False),
        preserves_order=attrs.get("preserves_order", OrderPreservation.PRESERVES_ORDER),
        max_workers=attrs.get("max_workers"),
        order_dependent=attrs.get(
            "order_dependent", OrderDependence.NOT_ORDER_DEPENDENT
        ),
        distinct_dependent=attrs.get(
            "distinct_dependent", DistinctDependence.NOT_DISTINCT_DEPENDENT
        ),
        return_type=attrs.get("return_type"),
    )


# =============================================================================
# Arrow Serialization
# =============================================================================

# Nested struct type for function examples
_EXAMPLE_STRUCT = pa.struct(
    [
        pa.field("sql", pa.string()),
        pa.field("description", pa.string()),
        pa.field("expected_output", pa.string(), nullable=True),
    ]
)

# Nested struct type for function parameters
_PARAMETER_STRUCT = pa.struct(
    [
        pa.field("name", pa.string()),
        pa.field("position", pa.int32(), nullable=True),
        pa.field("position_name", pa.string(), nullable=True),
        pa.field("type_name", pa.string(), nullable=True),
        pa.field("description", pa.string()),
        pa.field("required", pa.bool_()),
        pa.field("default", pa.string(), nullable=True),
        pa.field("constraints", pa.string(), nullable=True),  # JSON for flexibility
        pa.field("is_table_input", pa.bool_()),
    ]
)

# Schema for serializing function metadata
_METADATA_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string()),
        pa.field("class_name", pa.string()),
        pa.field("function_type", pa.string()),
        pa.field("description", pa.string()),
        pa.field("examples", pa.list_(_EXAMPLE_STRUCT)),
        pa.field("categories", pa.list_(pa.string())),
        pa.field("parameters", pa.list_(_PARAMETER_STRUCT)),
        pa.field("stability", pa.string()),
        pa.field("null_handling", pa.string()),
        pa.field("required_settings", pa.list_(pa.string())),
        pa.field("projection_pushdown", pa.bool_()),
        pa.field("filter_pushdown", pa.bool_()),
        pa.field("preserves_order", pa.string()),
        pa.field("max_workers", pa.int32(), nullable=True),
        pa.field("order_dependent", pa.string()),
        pa.field("distinct_dependent", pa.string()),
        pa.field("return_type", pa.string(), nullable=True),
    ]
)

# Fields that contain lists and need None -> [] conversion during deserialization
_LIST_FIELDS: frozenset[str] = frozenset(
    {"examples", "categories", "parameters", "required_settings"}
)


def _extract_arrow_row(columns: dict[str, list[Any]], index: int) -> dict[str, Any]:
    """Extract a single row from Arrow columnar data as a dict.

    Handles None values for list fields (converts None to []).
    """
    return {
        field: (values[index] or [] if field in _LIST_FIELDS else values[index])
        for field, values in columns.items()
    }


def metadata_to_arrow(metadata: ResolvedMetadata) -> pa.RecordBatch:
    """Serialize a single ResolvedMetadata to Arrow RecordBatch.

    Args:
        metadata: The metadata to serialize.

    Returns:
        RecordBatch with one row containing the metadata.

    """
    row = metadata.to_dict()
    # Wrap each value in a list for single-row batch
    data = {field: [value] for field, value in row.items()}
    return pa.RecordBatch.from_pydict(data, schema=_METADATA_SCHEMA)


def arrow_to_metadata(batch: pa.RecordBatch) -> ResolvedMetadata:
    """Deserialize Arrow RecordBatch to ResolvedMetadata.

    Args:
        batch: RecordBatch with one row containing metadata.

    Returns:
        Deserialized ResolvedMetadata.

    """
    if batch.num_rows != 1:
        raise ValueError(f"Expected 1 row, got {batch.num_rows}")

    columns = batch.to_pydict()
    row = _extract_arrow_row(columns, 0)
    return ResolvedMetadata.from_dict(row)


def metadatas_to_arrow(metadatas: Sequence[ResolvedMetadata]) -> pa.RecordBatch:
    """Serialize multiple ResolvedMetadata objects to Arrow RecordBatch.

    Args:
        metadatas: Sequence of ResolvedMetadata objects to serialize.

    Returns:
        RecordBatch with one row per metadata object.

    """
    if not metadatas:
        return pa.RecordBatch.from_pydict(
            {field.name: [] for field in _METADATA_SCHEMA}, schema=_METADATA_SCHEMA
        )

    # Collect all data into columnar lists
    data: dict[str, list[Any]] = {field.name: [] for field in _METADATA_SCHEMA}

    for meta in metadatas:
        row = meta.to_dict()
        for key, value in row.items():
            data[key].append(value)

    return pa.RecordBatch.from_pydict(data, schema=_METADATA_SCHEMA)


def functions_to_arrow(function_classes: Sequence[type]) -> pa.RecordBatch:
    """Serialize multiple function classes to Arrow RecordBatch.

    Convenience function that resolves metadata for each class, then serializes.
    For pre-resolved metadata, use metadatas_to_arrow() directly.

    Args:
        function_classes: Sequence of function classes to serialize.

    Returns:
        RecordBatch with one row per function.

    Example:
        # Worker registration
        batch = functions_to_arrow([EchoFunction, SumFunction])
        # Send batch to client...

    """
    return metadatas_to_arrow([resolve_metadata(cls) for cls in function_classes])


def arrow_to_functions(batch: pa.RecordBatch) -> list[ResolvedMetadata]:
    """Deserialize Arrow RecordBatch to list of ResolvedMetadata.

    Args:
        batch: RecordBatch with one row per function.

    Returns:
        List of deserialized ResolvedMetadata objects.

    """
    columns = batch.to_pydict()
    return [
        ResolvedMetadata.from_dict(_extract_arrow_row(columns, i))
        for i in range(batch.num_rows)
    ]


# =============================================================================
# Mixin for Function Classes
# =============================================================================


class MetadataMixin:
    """Mixin that provides metadata access for function classes.

    Add this to the base Function class to enable metadata resolution.

    Example:
        class Function(MetadataMixin):
            ...

        class MyFunction(Function):
            class Meta:
                description = "My function"
                max_workers = 2

        meta = MyFunction.get_metadata()
        print(meta.description)  # "My function"
        print(meta.max_workers)  # 2

    """

    @classmethod
    def get_metadata(cls) -> ResolvedMetadata:
        """Get the resolved metadata for this function class."""
        return resolve_metadata(cls)  # type: ignore[arg-type]

    @classmethod
    def describe(cls) -> dict[str, Any]:
        """Get metadata as a dictionary (for JSON serialization)."""
        return cls.get_metadata().to_dict()
