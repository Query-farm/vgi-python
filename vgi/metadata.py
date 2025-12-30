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

import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, get_type_hints

import pyarrow as pa

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
    # Meta base classes (for documentation/IDE support only)
    "FunctionMeta",
    "ScalarFunctionMeta",
    "AggregateFunctionMeta",
    "TableFunctionMeta",
    "TableInOutFunctionMeta",
    # Resolution
    "resolve_metadata",
    "extract_parameters",
    # Arrow serialization
    "metadata_to_arrow",
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

    TABLE_IN_OUT = auto()
    """Table-in-out function: streaming table transformation."""


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
        type_name: Type name as string (e.g., "int", "str").
        description: Documentation from Arg.doc.
        required: True if no default value.
        default: Default value, or None if required.
        constraints: Validation constraints as dict.

    """

    name: str
    position: int | str
    type_name: str | None = None
    description: str = ""
    required: bool = True
    default: Any = None
    constraints: dict[str, Any] = field(default_factory=dict)

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
    function_type: FunctionType = FunctionType.TABLE_IN_OUT

    # Documentation
    description: str = ""
    examples: list[FunctionExample] = field(default_factory=list)
    categories: list[str] = field(default_factory=list)
    parameters: list[ParameterInfo] = field(default_factory=list)

    # Behavior (all functions)
    stability: FunctionStability = FunctionStability.CONSISTENT
    null_handling: NullHandling = NullHandling.DEFAULT
    internal: bool = False

    # Table function specific
    projection_pushdown: bool = True
    filter_pushdown: bool = False
    preserves_order: OrderPreservation = OrderPreservation.PRESERVES_ORDER
    max_workers: int | None = None

    # VGI-specific
    streaming: bool = True
    supports_distributed: bool = False

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
            "internal": self.internal,
            "projection_pushdown": self.projection_pushdown,
            "filter_pushdown": self.filter_pushdown,
            "preserves_order": self.preserves_order.name,
            "max_workers": self.max_workers,
            "streaming": self.streaming,
            "supports_distributed": self.supports_distributed,
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
            internal=d.get("internal", False),
            projection_pushdown=d.get("projection_pushdown", True),
            filter_pushdown=d.get("filter_pushdown", False),
            preserves_order=OrderPreservation[
                d.get("preserves_order", "PRESERVES_ORDER")
            ],
            max_workers=d.get("max_workers"),
            streaming=d.get("streaming", True),
            supports_distributed=d.get("supports_distributed", False),
            order_dependent=OrderDependence[
                d.get("order_dependent", "NOT_ORDER_DEPENDENT")
            ],
            distinct_dependent=DistinctDependence[
                d.get("distinct_dependent", "NOT_DISTINCT_DEPENDENT")
            ],
            return_type=d.get("return_type"),
        )


# =============================================================================
# Meta Base Classes (for IDE support and documentation)
#
# These are NOT required for inheritance. Users can just write `class Meta:`
# and define attributes directly. These classes exist for:
# 1. IDE autocomplete when users DO choose to inherit
# 2. Documentation of available attributes
# 3. Type hints
# =============================================================================


class FunctionMeta:
    """Base metadata attributes available for all functions.

    Users don't need to inherit from this class. Just define attributes
    directly in a nested Meta class:

        class MyFunction(TableInOutFunction):
            class Meta:
                name = "my_func"
                description = "Does something"

    Available Attributes:
        name: Function name for registration (default: class name).
        description: Human-readable description.
        examples: List of FunctionExample or SQL strings.
        categories: Classification tags (e.g., ["math", "aggregate"]).
        stability: FunctionStability enum value.
        null_handling: NullHandling enum value.
        internal: Whether this is an internal/system function.

    """

    name: str | None = None
    description: str = ""
    examples: list[FunctionExample | str] = []
    categories: list[str] = []
    stability: FunctionStability = FunctionStability.CONSISTENT
    null_handling: NullHandling = NullHandling.DEFAULT
    internal: bool = False


class ScalarFunctionMeta(FunctionMeta):
    """Metadata for scalar functions (one output per input row).

    Additional Attributes:
        return_type: Return type description (e.g., "int64", "string").

    """

    return_type: str | None = None


class AggregateFunctionMeta(FunctionMeta):
    """Metadata for aggregate functions (many inputs → one output).

    Additional Attributes:
        order_dependent: Whether row order affects the result.
        distinct_dependent: Whether DISTINCT modifier affects the result.
        return_type: Return type description.

    """

    order_dependent: OrderDependence = OrderDependence.NOT_ORDER_DEPENDENT
    distinct_dependent: DistinctDependence = DistinctDependence.NOT_DISTINCT_DEPENDENT
    return_type: str | None = None


class TableFunctionMeta(FunctionMeta):
    """Metadata for table functions (returns a table).

    Additional Attributes:
        projection_pushdown: Whether column projection can be pushed down.
        filter_pushdown: Whether row filters can be pushed down.
        preserves_order: Whether output order matches input order.
        max_workers: Maximum parallel workers (None = unlimited).

    """

    projection_pushdown: bool = True
    filter_pushdown: bool = False
    preserves_order: OrderPreservation = OrderPreservation.PRESERVES_ORDER
    max_workers: int | None = None


class TableInOutFunctionMeta(TableFunctionMeta):
    """Metadata for table-in-out functions (VGI's primary function type).

    Additional Attributes:
        streaming: Whether the function processes data in streaming fashion.
        supports_distributed: Whether function supports distributed execution
            via save_state/load_states.

    """

    streaming: bool = True
    supports_distributed: bool = False


# =============================================================================
# Parameter Extraction from Arg Descriptors
# =============================================================================

# Sentinel to detect missing defaults
_MISSING: Any = object()


def _get_arg_type_name(cls: type, attr_name: str) -> str | None:
    """Try to extract type name from type hints for an Arg attribute."""
    try:
        hints = get_type_hints(cls)
        if attr_name in hints:
            hint = hints[attr_name]
            # Handle common cases
            if hasattr(hint, "__name__"):
                return str(hint.__name__)
            return str(hint)
    except Exception:
        pass
    return None


def extract_parameters(cls: type) -> list[ParameterInfo]:
    """Extract parameter information from Arg descriptors on a class.

    Walks the class and its bases to find all Arg descriptors and converts
    them to ParameterInfo objects.

    Args:
        cls: The function class to extract parameters from.

    Returns:
        List of ParameterInfo objects, sorted by position.

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

                # Build constraints dict
                constraints: dict[str, Any] = {}
                if arg.ge is not None:
                    constraints["ge"] = arg.ge
                if arg.le is not None:
                    constraints["le"] = arg.le
                if arg.gt is not None:
                    constraints["gt"] = arg.gt
                if arg.lt is not None:
                    constraints["lt"] = arg.lt
                if arg.choices is not None:
                    constraints["choices"] = list(arg.choices)
                if arg.pattern is not None:
                    constraints["pattern"] = arg.pattern

                # Check if required (no default)
                required = arg.default is _MISSING

                # Try to get type name from hints
                type_name = _get_arg_type_name(cls, attr_name)

                parameters.append(
                    ParameterInfo(
                        name=attr_name,
                        position=arg.position,
                        type_name=type_name,
                        description=arg.doc,
                        required=required,
                        default=None if required else arg.default,
                        constraints=constraints,
                    )
                )

    # Sort: positional args by index first, then named args alphabetically
    def sort_key(p: ParameterInfo) -> tuple[int, int | str]:
        if isinstance(p.position, int):
            return (0, p.position)
        return (1, p.position)

    return sorted(parameters, key=sort_key)


# =============================================================================
# Metadata Resolution
# =============================================================================


def _normalize_examples(
    examples: list[FunctionExample | str],
) -> list[FunctionExample]:
    """Convert string examples to FunctionExample objects."""
    result = []
    for ex in examples:
        if isinstance(ex, str):
            result.append(FunctionExample(sql=ex))
        else:
            result.append(ex)
    return result


def _infer_function_type(cls: type) -> FunctionType:
    """Infer the function type from the class hierarchy."""
    # Check class names in MRO to determine type
    # This avoids importing the actual classes (circular import issues)
    for klass in cls.__mro__:
        name = klass.__name__
        if name == "TableInOutFunction" or name == "TableInOutGeneratorFunction":
            return FunctionType.TABLE_IN_OUT
        if name == "TableFunction":
            return FunctionType.TABLE
        if name == "AggregateFunction":
            return FunctionType.AGGREGATE
        if name == "ScalarFunction":
            return FunctionType.SCALAR
    return FunctionType.TABLE_IN_OUT  # Default


def resolve_metadata(cls: type) -> ResolvedMetadata:
    """Resolve metadata for a function class.

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
        print(meta.name)  # "MyFunction"
        print(meta.description)  # "My function"
        print(meta.parameters)  # [ParameterInfo(name="count", ...)]

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

    # Infer function type from class hierarchy
    function_type = _infer_function_type(cls)

    # Use class name as default name, converting to snake_case
    class_name = cls.__name__
    if "name" in attrs and attrs["name"]:
        name = attrs["name"]
    else:
        # Convert CamelCase to snake_case
        import re

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
        internal=attrs.get("internal", False),
        projection_pushdown=attrs.get("projection_pushdown", True),
        filter_pushdown=attrs.get("filter_pushdown", False),
        preserves_order=attrs.get("preserves_order", OrderPreservation.PRESERVES_ORDER),
        max_workers=attrs.get("max_workers"),
        streaming=attrs.get("streaming", True),
        supports_distributed=attrs.get("supports_distributed", False),
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

# Schema for serializing function metadata
_METADATA_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string()),
        pa.field("class_name", pa.string()),
        pa.field("function_type", pa.string()),
        pa.field("description", pa.string()),
        pa.field("examples_json", pa.string()),  # JSON array
        pa.field("categories_json", pa.string()),  # JSON array
        pa.field("parameters_json", pa.string()),  # JSON array
        pa.field("stability", pa.string()),
        pa.field("null_handling", pa.string()),
        pa.field("internal", pa.bool_()),
        pa.field("projection_pushdown", pa.bool_()),
        pa.field("filter_pushdown", pa.bool_()),
        pa.field("preserves_order", pa.string()),
        pa.field("max_workers", pa.int32(), nullable=True),
        pa.field("streaming", pa.bool_()),
        pa.field("supports_distributed", pa.bool_()),
        pa.field("order_dependent", pa.string()),
        pa.field("distinct_dependent", pa.string()),
        pa.field("return_type", pa.string(), nullable=True),
    ]
)


def metadata_to_arrow(metadata: ResolvedMetadata) -> pa.RecordBatch:
    """Serialize a single ResolvedMetadata to Arrow RecordBatch.

    Args:
        metadata: The metadata to serialize.

    Returns:
        RecordBatch with one row containing the metadata.

    """
    data = {
        "name": [metadata.name],
        "class_name": [metadata.class_name],
        "function_type": [metadata.function_type.name],
        "description": [metadata.description],
        "examples_json": [json.dumps([ex.to_dict() for ex in metadata.examples])],
        "categories_json": [json.dumps(metadata.categories)],
        "parameters_json": [json.dumps([p.to_dict() for p in metadata.parameters])],
        "stability": [metadata.stability.name],
        "null_handling": [metadata.null_handling.name],
        "internal": [metadata.internal],
        "projection_pushdown": [metadata.projection_pushdown],
        "filter_pushdown": [metadata.filter_pushdown],
        "preserves_order": [metadata.preserves_order.name],
        "max_workers": [metadata.max_workers],
        "streaming": [metadata.streaming],
        "supports_distributed": [metadata.supports_distributed],
        "order_dependent": [metadata.order_dependent.name],
        "distinct_dependent": [metadata.distinct_dependent.name],
        "return_type": [metadata.return_type],
    }
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

    row = batch.to_pydict()

    return ResolvedMetadata(
        name=row["name"][0],
        class_name=row["class_name"][0],
        function_type=FunctionType[row["function_type"][0]],
        description=row["description"][0],
        examples=[
            FunctionExample.from_dict(ex)
            for ex in json.loads(row["examples_json"][0] or "[]")
        ],
        categories=json.loads(row["categories_json"][0] or "[]"),
        parameters=[
            ParameterInfo.from_dict(p)
            for p in json.loads(row["parameters_json"][0] or "[]")
        ],
        stability=FunctionStability[row["stability"][0]],
        null_handling=NullHandling[row["null_handling"][0]],
        internal=row["internal"][0],
        projection_pushdown=row["projection_pushdown"][0],
        filter_pushdown=row["filter_pushdown"][0],
        preserves_order=OrderPreservation[row["preserves_order"][0]],
        max_workers=row["max_workers"][0],
        streaming=row["streaming"][0],
        supports_distributed=row["supports_distributed"][0],
        order_dependent=OrderDependence[row["order_dependent"][0]],
        distinct_dependent=DistinctDependence[row["distinct_dependent"][0]],
        return_type=row["return_type"][0],
    )


def functions_to_arrow(function_classes: Sequence[type]) -> pa.RecordBatch:
    """Serialize multiple function classes to Arrow RecordBatch.

    Resolves metadata for each class and creates a batch with one row per function.

    Args:
        function_classes: Sequence of function classes to serialize.

    Returns:
        RecordBatch with one row per function.

    Example:
        # Worker registration
        batch = functions_to_arrow([EchoFunction, SumFunction])
        # Send batch to client...

    """
    if not function_classes:
        return pa.RecordBatch.from_pydict(
            {field.name: [] for field in _METADATA_SCHEMA}, schema=_METADATA_SCHEMA
        )

    # Collect all data into lists
    data: dict[str, list[Any]] = {field.name: [] for field in _METADATA_SCHEMA}

    for cls in function_classes:
        meta = resolve_metadata(cls)
        data["name"].append(meta.name)
        data["class_name"].append(meta.class_name)
        data["function_type"].append(meta.function_type.name)
        data["description"].append(meta.description)
        data["examples_json"].append(
            json.dumps([ex.to_dict() for ex in meta.examples])
        )
        data["categories_json"].append(json.dumps(meta.categories))
        data["parameters_json"].append(
            json.dumps([p.to_dict() for p in meta.parameters])
        )
        data["stability"].append(meta.stability.name)
        data["null_handling"].append(meta.null_handling.name)
        data["internal"].append(meta.internal)
        data["projection_pushdown"].append(meta.projection_pushdown)
        data["filter_pushdown"].append(meta.filter_pushdown)
        data["preserves_order"].append(meta.preserves_order.name)
        data["max_workers"].append(meta.max_workers)
        data["streaming"].append(meta.streaming)
        data["supports_distributed"].append(meta.supports_distributed)
        data["order_dependent"].append(meta.order_dependent.name)
        data["distinct_dependent"].append(meta.distinct_dependent.name)
        data["return_type"].append(meta.return_type)

    return pa.RecordBatch.from_pydict(data, schema=_METADATA_SCHEMA)


def arrow_to_functions(batch: pa.RecordBatch) -> list[ResolvedMetadata]:
    """Deserialize Arrow RecordBatch to list of ResolvedMetadata.

    Args:
        batch: RecordBatch with one row per function.

    Returns:
        List of deserialized ResolvedMetadata objects.

    """
    result = []
    rows = batch.to_pydict()
    num_rows = batch.num_rows

    for i in range(num_rows):
        result.append(
            ResolvedMetadata(
                name=rows["name"][i],
                class_name=rows["class_name"][i],
                function_type=FunctionType[rows["function_type"][i]],
                description=rows["description"][i],
                examples=[
                    FunctionExample.from_dict(ex)
                    for ex in json.loads(rows["examples_json"][i] or "[]")
                ],
                categories=json.loads(rows["categories_json"][i] or "[]"),
                parameters=[
                    ParameterInfo.from_dict(p)
                    for p in json.loads(rows["parameters_json"][i] or "[]")
                ],
                stability=FunctionStability[rows["stability"][i]],
                null_handling=NullHandling[rows["null_handling"][i]],
                internal=rows["internal"][i],
                projection_pushdown=rows["projection_pushdown"][i],
                filter_pushdown=rows["filter_pushdown"][i],
                preserves_order=OrderPreservation[rows["preserves_order"][i]],
                max_workers=rows["max_workers"][i],
                streaming=rows["streaming"][i],
                supports_distributed=rows["supports_distributed"][i],
                order_dependent=OrderDependence[rows["order_dependent"][i]],
                distinct_dependent=DistinctDependence[rows["distinct_dependent"][i]],
                return_type=rows["return_type"][i],
            )
        )

    return result


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
        return resolve_metadata(cls)

    @classmethod
    def describe(cls) -> dict[str, Any]:
        """Get metadata as a dictionary (for JSON serialization)."""
        return cls.get_metadata().to_dict()


# =============================================================================
# Demo / Test
# =============================================================================

if __name__ == "__main__":
    import json as json_module

    # Simulated VGI imports
    from vgi.arguments import Arg

    # Simulate VGI class hierarchy
    class Function(MetadataMixin):
        """Base function class."""

        pass

    class TableFunction(Function):
        """Table function base."""

        pass

    class TableInOutFunction(TableFunction):
        """Table-in-out function base."""

        pass

    # Example 1: Full metadata with Arg descriptors
    class SumColumnsFunction(TableInOutFunction):
        """Sum all numeric columns in the input table."""

        class Meta:
            name = "sum_columns"
            description = "Sum all numeric columns and return a single row"
            examples = [
                FunctionExample(
                    sql="SELECT * FROM sum_columns(input_table)",
                    description="Sum all columns",
                ),
                "SELECT * FROM sum_columns(input_table, columns=['a', 'b'])",
            ]
            categories = ["aggregation", "numeric"]
            max_workers = 1
            supports_distributed = True

        # Arg descriptors - automatically extracted as parameters
        columns = Arg[list](  # type: ignore[type-arg]
            "columns",
            default=None,
            doc="Column names to sum (default: all numeric)",
        )

    # Example 2: Minimal metadata (uses docstring, class name)
    class EchoFunction(TableInOutFunction):
        """Pass through all input rows unchanged."""

        class Meta:
            categories = ["utility", "debug"]

    # Example 3: Inheritance
    class FilterFunction(TableInOutFunction):
        """Base class for filter functions."""

        class Meta:
            categories = ["filter"]
            preserves_order = OrderPreservation.PRESERVES_ORDER

    class PositiveFilter(FilterFunction):
        """Filter to keep only positive values."""

        class Meta:
            description = "Keep only rows where value > 0"
            examples = ["SELECT * FROM positive_filter(data)"]

        threshold = Arg[float](0, default=0.0, doc="Minimum threshold", ge=0.0)

    # Test resolution
    print("=== SumColumnsFunction ===")
    meta = SumColumnsFunction.get_metadata()
    print(f"Name: {meta.name}")
    print(f"Class: {meta.class_name}")
    print(f"Description: {meta.description}")
    print(f"Parameters: {meta.parameters}")
    print(f"Max Workers: {meta.max_workers}")
    print()

    print("=== EchoFunction ===")
    meta = EchoFunction.get_metadata()
    print(f"Name: {meta.name}")  # Should be "echo" (auto-converted)
    print(f"Description: {meta.description}")  # From docstring
    print()

    print("=== PositiveFilter (inheritance) ===")
    meta = PositiveFilter.get_metadata()
    print(f"Name: {meta.name}")
    print(f"Categories: {meta.categories}")  # Should inherit ["filter"]
    print(f"Parameters: {meta.parameters}")  # Should have threshold
    print()

    # Test Arrow serialization
    print("=== Arrow Serialization ===")
    functions = [SumColumnsFunction, EchoFunction, PositiveFilter]
    batch = functions_to_arrow(functions)
    print(f"Serialized {batch.num_rows} functions to Arrow")
    print(f"Schema: {batch.schema}")
    print()

    # Deserialize
    restored = arrow_to_functions(batch)
    print("Restored functions:")
    for meta in restored:
        print(f"  - {meta.name}: {meta.description[:50]}...")
    print()

    # JSON output
    print("=== JSON Output ===")
    print(json_module.dumps(SumColumnsFunction.describe(), indent=2))
