# Copyright 2025, 2026 Query Farm LLC - https://query.farm

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
from typing import TYPE_CHECKING, Annotated, Any, get_args, get_origin, get_type_hints

import pyarrow as pa

from vgi.arguments import _MISSING, AnyArrow, Secret, SecretLookupEntry, TableInput

if TYPE_CHECKING:
    from vgi.arguments import Arg

# Default max_workers when not explicitly specified (effectively unlimited)
DEFAULT_MAX_WORKERS = 99999

__all__ = [
    # Constants
    "DEFAULT_MAX_WORKERS",
    # Enums
    "FunctionStability",
    "CatalogFunctionType",
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
    "VarargsValidationError",
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


class CatalogFunctionType(Enum):
    """Type of function for DuckDB registration."""

    SCALAR = auto()
    """Scalar function: one output per input row."""

    AGGREGATE = auto()
    """Aggregate function: many inputs → one output."""

    TABLE = auto()
    """Table function: returns a table (streaming producer or streaming exchange)."""

    TABLE_BUFFERING = auto()
    """Buffered table function: Sink+Source PhysicalOperator that sees all
    input before producing output. Dispatched to the custom
    ``PhysicalVgiTableBufferingFunction`` operator instead of the streaming
    ``in_out_function`` registration. The class hierarchy is the dispatch
    key — set automatically for ``TableBufferingFunction`` subclasses."""


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

    Maps to DuckDB's ``OrderPreservationType`` enum:

    * ``PRESERVES_ORDER`` → ``OrderPreservationType::INSERTION_ORDER``
      (DuckDB default — operator maintains child operator order).
    * ``NO_ORDER_GUARANTEE`` → ``OrderPreservationType::NO_ORDER``
      (operator may freely reorder its input/output).
    * ``FIXED_ORDER`` → ``OrderPreservationType::FIXED_ORDER``
      (operator outputs rows in a fixed, mandatory order — DuckDB
      serializes the pipeline so a single worker produces all rows).
    """

    PRESERVES_ORDER = auto()
    """Output rows are in same order as input rows (DuckDB INSERTION_ORDER)."""

    NO_ORDER_GUARANTEE = auto()
    """Output order is undefined; may be reordered (DuckDB NO_ORDER)."""

    FIXED_ORDER = auto()
    """Output is in a fixed mandatory order; DuckDB serializes the pipeline
    (single worker) to preserve it (DuckDB FIXED_ORDER)."""


class PartitionKind(Enum):
    """Partition shape declared by a table function.

    Declared over its ``vgi.partition_column``-annotated bind-schema fields.

    Mirrors DuckDB's ``TablePartitionInfo`` at
    ``duckdb/src/include/duckdb/function/partition_stats.hpp:20``.

    The C++ extension returns this from ``TableFunction::get_partition_info``;
    DuckDB's planner currently consumes only ``SINGLE_VALUE_PARTITIONS``
    (to plan ``PhysicalPartitionedAggregate`` over ``PhysicalHashAggregate``;
    see ``plan_aggregate.cpp:109``). The other values are declarable
    so the protocol is future-proof; today they fall back to
    ``HASH_GROUP_BY``.

    Only set this to a non-default value when at least one field in
    the bind schema is annotated with
    ``{b"vgi.partition_column": b"true"}`` (use
    :func:`vgi.schema_utils.partition_field` to construct such fields).
    The reverse is also required — annotated fields without a
    matching ``partition_kind`` raise at worker startup.
    """

    NOT_PARTITIONED = auto()
    """Function does not declare partitioning over the annotated columns
    (default; same effect as leaving fields un-annotated)."""

    SINGLE_VALUE_PARTITIONS = auto()
    """Each emitted chunk has exactly one distinct value per partition
    column. Unlocks ``PhysicalPartitionedAggregate`` for ``GROUP BY``
    over those columns."""

    OVERLAPPING_PARTITIONS = auto()
    """Partitions overlap only at boundaries (bounds = [1,2][2,3][3,4]).
    Wire-level declarable; DuckDB has no consumer today."""

    DISJOINT_PARTITIONS = auto()
    """Partitions are pairwise disjoint (bounds = [1,2][3,4][5,6]).
    Wire-level declarable; DuckDB has no consumer today."""


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
        is_varargs: True if this accepts multiple trailing values.
        is_const: True if this is a constant parameter (ConstParam).

    """

    name: str
    position: int | str
    type_name: str | None = None
    description: str = ""
    required: bool = True
    default: Any = None
    constraints: dict[str, Any] = field(default_factory=dict)
    is_table_input: bool = False
    is_varargs: bool = False
    is_const: bool = False

    def to_dict(self) -> dict[str, str | int | bool | None]:
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
            "is_varargs": self.is_varargs,
            "is_const": self.is_const,
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
            is_varargs=d.get("is_varargs", False),
            is_const=d.get("is_const", False),
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

    def to_dict(self) -> dict[str, str | None]:
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
    function_type: CatalogFunctionType

    # Documentation
    description: str = ""
    examples: list[FunctionExample] = field(default_factory=list)
    categories: list[str] = field(default_factory=list)
    tags: dict[str, str] = field(default_factory=dict)
    parameters: list[ParameterInfo] = field(default_factory=list)

    # Behavior (all functions)
    stability: FunctionStability = FunctionStability.CONSISTENT
    null_handling: NullHandling = NullHandling.DEFAULT

    # settings required by the function
    required_settings: list[str] = field(default_factory=list)

    # secrets required by the function (each entry has secret_type, secret_name, scope)
    required_secrets: list[SecretLookupEntry] = field(default_factory=list)

    # Table function specific
    projection_pushdown: bool = False
    filter_pushdown: bool = False
    sampling_pushdown: bool = False
    # When True, the table function participates in DuckDB's late-materialization
    # optimizer: TOP_N/LIMIT/SAMPLE over the scan is rewritten into a SEMI join on
    # the rowid virtual column, and surviving rowids are pushed back to the wide
    # scan as a filter. Requires a unique, deterministic, snapshot-stable rowid
    # column (is_row_id) plus projection_pushdown + filter_pushdown. See the C++
    # extension's late-materialization gating for the worker contract.
    late_materialization: bool = False
    supported_expression_filters: list[str] = field(default_factory=list)
    preserves_order: OrderPreservation = OrderPreservation.PRESERVES_ORDER
    max_workers: int | None = None
    supports_batch_index: bool = False
    partition_kind: PartitionKind = PartitionKind.NOT_PARTITIONED

    # Aggregate function specific
    order_dependent: OrderDependence = OrderDependence.NOT_ORDER_DEPENDENT
    distinct_dependent: DistinctDependence = DistinctDependence.NOT_DISTINCT_DEPENDENT
    supports_window: bool = False
    streaming_partitioned: bool = False

    # Table-in-out specific: True if the function has a meaningful finalize phase
    # (override of finalize()/finish()). Used by the C++ extension to decide
    # whether to register in_out_function_final, which DuckDB disallows alongside
    # LATERAL-projected input.
    has_finalize: bool = False

    # When True (only meaningful when ``function_type == TABLE_BUFFERING``),
    # the source phase is single-threaded and finalize_state_ids are drained
    # in the order combine() returned them. The default (False) enables
    # parallel finalize.
    source_order_dependent: bool = False

    # When True (only meaningful when ``function_type == TABLE_BUFFERING``),
    # the SINK phase runs single-threaded — every process() call arrives in
    # source order on one worker. The default (False) parallelizes ingest.
    # Mutually exclusive with requires_input_batch_index (single-thread
    # already orders; no batch_index needed).
    sink_order_dependent: bool = False

    # When True (only meaningful when ``function_type == TABLE_BUFFERING``), the C++ Sink
    # operator declares RequiredPartitionInfo()=BatchIndex(), causing DuckDB
    # to thread a globally-unique monotonic batch_index from the source
    # into every process() call. Workers can accumulate (batch_index,
    # payload) tuples and sort in combine() to reconstruct source order
    # under parallel ingest. Requires the source to support batch_index
    # (parquet/csv/temp-table-scan do; range() does not — bind fails).
    # Mutually exclusive with sink_order_dependent.
    requires_input_batch_index: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "class_name": self.class_name,
            "function_type": self.function_type.name,
            "description": self.description,
            "examples": [ex.to_dict() for ex in self.examples],
            "categories": self.categories,
            "tags": self.tags,
            "parameters": [p.to_dict() for p in self.parameters],
            "stability": self.stability.name,
            "null_handling": self.null_handling.name,
            "required_settings": self.required_settings,
            "required_secrets": [e.to_dict() for e in self.required_secrets],
            "projection_pushdown": self.projection_pushdown,
            "filter_pushdown": self.filter_pushdown,
            "sampling_pushdown": self.sampling_pushdown,
            "late_materialization": self.late_materialization,
            "supported_expression_filters": self.supported_expression_filters,
            "preserves_order": self.preserves_order.name,
            "max_workers": self.max_workers,
            "supports_batch_index": self.supports_batch_index,
            "partition_kind": self.partition_kind.name,
            "order_dependent": self.order_dependent.name,
            "distinct_dependent": self.distinct_dependent.name,
            "supports_window": self.supports_window,
            "streaming_partitioned": self.streaming_partitioned,
            "has_finalize": self.has_finalize,
            "source_order_dependent": self.source_order_dependent,
            "sink_order_dependent": self.sink_order_dependent,
            "requires_input_batch_index": self.requires_input_batch_index,
        }

    @staticmethod
    def from_dict(d: dict[str, Any]) -> ResolvedMetadata:
        """Create from dictionary."""
        return ResolvedMetadata(
            name=d["name"],
            class_name=d["class_name"],
            function_type=CatalogFunctionType[d["function_type"]],
            description=d.get("description", ""),
            examples=[FunctionExample.from_dict(ex) for ex in d.get("examples", [])],
            categories=d.get("categories", []),
            tags=dict(d.get("tags", {})),
            parameters=[ParameterInfo.from_dict(p) for p in d.get("parameters", [])],
            stability=FunctionStability[d.get("stability", "CONSISTENT")],
            null_handling=NullHandling[d.get("null_handling", "DEFAULT")],
            required_settings=d.get("required_settings", []),
            required_secrets=[SecretLookupEntry.from_dict(e) for e in d.get("required_secrets", [])],
            projection_pushdown=d.get("projection_pushdown", False),
            filter_pushdown=d.get("filter_pushdown", False),
            sampling_pushdown=d.get("sampling_pushdown", False),
            late_materialization=d.get("late_materialization", False),
            supported_expression_filters=d.get("supported_expression_filters", []),
            preserves_order=OrderPreservation[d.get("preserves_order", "PRESERVES_ORDER")],
            max_workers=d.get("max_workers"),
            supports_batch_index=d.get("supports_batch_index", False),
            partition_kind=PartitionKind[d.get("partition_kind", "NOT_PARTITIONED")],
            order_dependent=OrderDependence[d.get("order_dependent", "NOT_ORDER_DEPENDENT")],
            distinct_dependent=DistinctDependence[d.get("distinct_dependent", "NOT_DISTINCT_DEPENDENT")],
            supports_window=d.get("supports_window", False),
            streaming_partitioned=d.get("streaming_partitioned", False),
            has_finalize=d.get("has_finalize", False),
            source_order_dependent=d.get("source_order_dependent", False),
            sink_order_dependent=d.get("sink_order_dependent", False),
            requires_input_batch_index=d.get("requires_input_batch_index", False),
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

    # Check if it's AnyArrow (any Arrow type accepted)
    if hint is AnyArrow:
        return ("AnyArrow", False)

    # Extract type name
    if hasattr(hint, "__name__"):
        return (hint.__name__, False)

    return (str(hint), False)


class TableInputValidationError(ValueError):
    """Raised when TableInput parameter validation fails."""


class VarargsValidationError(ValueError):
    """Raised when varargs parameter validation fails."""


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


def extract_parameters(cls: type, *, validate_table_input: bool = True) -> list[ParameterInfo]:
    """Extract parameter information from Arg descriptors on a class.

    Walks the class and its bases to find all Arg descriptors and converts
    them to ParameterInfo objects. Also handles the new Param/ConstParam API
    for ScalarFunction subclasses.

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

    # Check for new Param/ConstParam API (ScalarFunction and AggregateFunction subclasses)
    # These are stored in _compute_params and _const_params class attributes
    compute_params: dict[str, Arg[Any]] = getattr(cls, "_compute_params", {})
    const_params: dict[str, Arg[Any]] = getattr(cls, "_const_params", {})

    for name, arg in compute_params.items():
        seen_names.add(name)
        required = arg.default is _MISSING
        # For new API, use arrow_type if available
        compute_type_name = str(arg.arrow_type) if arg.arrow_type else "any"

        parameters.append(
            ParameterInfo(
                name=name,
                position=arg.position,
                type_name=compute_type_name,
                description=arg.doc,
                required=required,
                default=None if required else arg.default,
                constraints=_build_constraints(arg),
                is_table_input=False,
                is_varargs=arg.varargs,
            )
        )

    for name, arg in const_params.items():
        seen_names.add(name)
        required = arg.default is _MISSING
        const_type_name = str(arg.arrow_type) if arg.arrow_type else "any"

        parameters.append(
            ParameterInfo(
                name=name,
                position=arg.position,
                type_name=const_type_name,
                description=arg.doc,
                required=required,
                default=None if required else arg.default,
                constraints=_build_constraints(arg),
                is_table_input=False,
                is_varargs=arg.varargs,
                is_const=arg.const,
            )
        )

    # Check for FunctionArguments dataclass (typed generic pattern)
    # e.g., class MyFunc(TableFunctionGenerator[MyArgs]):
    #   where MyArgs has fields like: count: Annotated[int, Arg(0, doc="...")]
    func_args_class = getattr(cls, "FunctionArguments", None)
    if func_args_class is not None:
        try:
            func_args_hints = get_type_hints(func_args_class, include_extras=True)
        except (NameError, AttributeError):
            func_args_hints = {}

        for field_name, field_hint in func_args_hints.items():
            if field_name.startswith("_") or field_name in seen_names:
                continue

            if get_origin(field_hint) is not Annotated:
                continue

            # Extract Arg from Annotated metadata
            type_args = get_args(field_hint)
            base_type = type_args[0]
            arg_instance: Arg[Any] | None = None
            for meta in type_args[1:]:
                if isinstance(meta, Arg):
                    arg_instance = meta
                    break

            if arg_instance is None:
                continue

            seen_names.add(field_name)

            is_table_input = base_type is TableInput
            if base_type is TableInput:
                type_name = "TableInput"
            elif base_type is AnyArrow:
                type_name = "AnyArrow"
            elif hasattr(base_type, "__name__"):
                type_name = base_type.__name__
            else:
                type_name = str(base_type)

            required = arg_instance.default is _MISSING

            parameters.append(
                ParameterInfo(
                    name=field_name,
                    position=arg_instance.position,
                    type_name=type_name,
                    description=arg_instance.doc,
                    required=required,
                    default=None if required else arg_instance.default,
                    constraints=_build_constraints(arg_instance),
                    is_table_input=is_table_input,
                    is_varargs=arg_instance.varargs,
                )
            )

    # Walk MRO to find all Arg descriptors (legacy API)
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
                arg = attr_value
                required = arg.default is _MISSING
                legacy_type_name, is_table_input = _get_arg_type_info(cls, attr_name)

                parameters.append(
                    ParameterInfo(
                        name=attr_name,
                        position=arg.position,
                        type_name=legacy_type_name or "any",
                        description=arg.doc,
                        required=required,
                        default=None if required else arg.default,
                        constraints=_build_constraints(arg),
                        is_table_input=is_table_input,
                        is_varargs=arg.varargs,
                    )
                )

    # Sort: positional args by index first, then named args alphabetically
    def sort_key(p: ParameterInfo) -> tuple[int, int | str]:
        if isinstance(p.position, int):
            return (0, p.position)
        return (1, p.position)

    sorted_params = sorted(parameters, key=sort_key)

    # Validate TableInput and varargs constraints
    if validate_table_input:
        _validate_table_input(cls, sorted_params)
        _validate_varargs(cls, sorted_params)

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


def _validate_varargs(cls: type, parameters: list[ParameterInfo]) -> None:
    """Validate varargs parameter constraints.

    If a function has varargs parameters, validates that:
    - There is at most one varargs parameter
    - The varargs parameter is positional (not named) - enforced by Arg.__init__
    - The varargs parameter is the last positional arg (before TableInput if present)

    Args:
        cls: The function class being validated.
        parameters: Extracted parameters.

    Raises:
        VarargsValidationError: If validation fails.

    """
    varargs_params = [p for p in parameters if p.is_varargs]

    if len(varargs_params) == 0:
        return  # No varargs parameters, nothing to validate

    if len(varargs_params) > 1:
        names = [p.name for p in varargs_params]
        raise VarargsValidationError(
            f"{cls.__name__}: Functions can have at most one varargs parameter, "
            f"but found {len(varargs_params)}: {names}"
        )

    varargs_param = varargs_params[0]

    # Get all positional parameters (excluding TableInput)
    positional_params = [p for p in parameters if isinstance(p.position, int) and not p.is_table_input]

    if not positional_params:
        return  # Should not happen if varargs exists, but be safe

    # Find the maximum position among non-varargs positional params
    # All positions here are int (filtered above), but mypy doesn't know
    non_varargs_positional = [p for p in positional_params if not p.is_varargs]
    if non_varargs_positional:
        # All positions are int (filtered by isinstance(p.position, int) above)
        int_positions = [p.position for p in non_varargs_positional if isinstance(p.position, int)]
        max_non_varargs_pos = max(int_positions)
        # varargs position must be int (enforced by Arg.__init__)
        assert isinstance(varargs_param.position, int)
        if varargs_param.position < max_non_varargs_pos:
            raise VarargsValidationError(
                f"{cls.__name__}: Varargs parameter '{varargs_param.name}' at "
                f"position {varargs_param.position} must be the last positional "
                f"argument, but there are positional arguments after it"
            )


# =============================================================================
# Metadata Resolution
# =============================================================================


def _normalize_examples(
    examples: list[FunctionExample | str],
) -> list[FunctionExample]:
    """Convert string examples to FunctionExample objects."""
    return [FunctionExample(sql=ex) if isinstance(ex, str) else ex for ex in examples]


# Mapping from base class names to CatalogFunctionType.
# Using a dict avoids typos and provides O(1) lookup.
# Class names are used (not classes) to avoid circular imports.
# Note: Functions with an Arg[TableInput] parameter receive table input.
_CLASS_NAME_TO_FUNCTION_TYPE: dict[str, CatalogFunctionType] = {
    # Buffered table function (Sink+Source). Must come before "TableFunctionBase"
    # in the MRO walk — ``_infer_function_type`` returns on the first match, so
    # the more-specific entry wins for TableBufferingFunction subclasses.
    "TableBufferingFunction": CatalogFunctionType.TABLE_BUFFERING,
    # Streaming table functions (TableFunctionGenerator + TableInOutGenerator).
    "TableFunctionBase": CatalogFunctionType.TABLE,
    "AggregateFunction": CatalogFunctionType.AGGREGATE,
    "ScalarFunction": CatalogFunctionType.SCALAR,
    "ScalarFunctionGenerator": CatalogFunctionType.SCALAR,
}

# Valid Meta class attribute names (for typo detection)
_VALID_META_ATTRIBUTES: frozenset[str] = frozenset(
    {
        # Common
        "name",
        "description",
        "examples",
        "categories",
        "tags",
        "stability",
        "null_handling",
        "required_settings",  # settings/pragmas required by function
        "required_secrets",  # secrets required by function
        # Table function specific
        "projection_pushdown",
        "filter_pushdown",
        "sampling_pushdown",
        "late_materialization",  # Participate in DuckDB late-materialization rewrite
        "supported_expression_filters",
        "auto_apply_filters",  # Auto-apply pushdown filters to output batches
        "preserves_order",
        "max_workers",
        "supports_batch_index",  # opt-in to per-batch batch_index tagging (parallel + ordered sink)
        "partition_kind",  # opt-in to PartitionColumns mode for Hive-style partitioning
        # Table-in-out specific: explicit override for the has_finalize auto-detection.
        # Set to True or False to force the emitted ``in_out_function_final``
        # registration bit; leave unset (None) to auto-detect from finish/finalize.
        "has_finalize",
        # Buffered table function knobs (only meaningful when the class is a
        # TableBufferingFunction subclass — function_type == TABLE_BUFFERING).
        # When True, source phase is single-threaded and finalize_state_ids
        # drain in combine-returned order.
        "source_order_dependent",
        # When True, the SINK phase runs single-threaded — process() calls
        # arrive in source order on one worker.
        "sink_order_dependent",
        # When True, DuckDB threads a globally-unique monotonic batch_index
        # from the source into every process() call. Worker can reconstruct
        # source order in combine() by sorting accumulated (batch_index,
        # payload) tuples.
        "requires_input_batch_index",
        # Aggregate function specific
        "order_dependent",
        "distinct_dependent",
        "supports_window",
        "streaming_partitioned",
        # Scalar function specific
        "output_type",  # pa.DataType | type[AnyArrow] for scalar functions
    }
)


class FunctionTypeError(TypeError):
    """Raised when a function's type cannot be determined from its class hierarchy."""


def _infer_function_type(cls: type) -> CatalogFunctionType:
    """Infer the function type from the class hierarchy.

    Raises:
        FunctionTypeError: If no recognized base class is found in the MRO.

    """
    for klass in cls.__mro__:
        if klass.__name__ in _CLASS_NAME_TO_FUNCTION_TYPE:
            return _CLASS_NAME_TO_FUNCTION_TYPE[klass.__name__]
    recognized_bases = sorted(_CLASS_NAME_TO_FUNCTION_TYPE.keys())
    raise FunctionTypeError(
        f"Cannot determine function type for {cls.__name__}. Class must inherit from one of: {recognized_bases}"
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

    # Infer function type from class hierarchy. TableBufferingFunction
    # subclasses resolve to ``CatalogFunctionType.TABLE_BUFFERING`` — that's
    # the single source of truth for the C++ optimizer rewriter, not a
    # separate Meta flag.
    function_type = _infer_function_type(cls)
    is_buffering = function_type is CatalogFunctionType.TABLE_BUFFERING

    # Cross-flag validation for the buffered table path.
    if attrs.get("source_order_dependent") and not is_buffering:
        raise TypeError(
            f"{cls.__name__}: Meta.source_order_dependent is only meaningful on TableBufferingFunction subclasses"
        )
    if attrs.get("sink_order_dependent") and not is_buffering:
        raise TypeError(
            f"{cls.__name__}: Meta.sink_order_dependent is only meaningful on TableBufferingFunction subclasses"
        )
    if attrs.get("requires_input_batch_index") and not is_buffering:
        raise TypeError(
            f"{cls.__name__}: Meta.requires_input_batch_index is only meaningful on TableBufferingFunction subclasses"
        )
    if attrs.get("sink_order_dependent") and attrs.get("requires_input_batch_index"):
        raise TypeError(
            f"{cls.__name__}: Meta.sink_order_dependent and "
            f"Meta.requires_input_batch_index are mutually exclusive — "
            f"single-threaded sink already orders process() calls; "
            f"batch_index is only useful under parallel ingest"
        )

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

    # Merge annotation-derived setting/secret keys into required_settings/required_secrets
    meta_required_settings: list[str] = list(attrs.get("required_settings", []))

    # Build required_secrets from Meta and annotations
    meta_required_secrets_raw = attrs.get("required_secrets", [])
    meta_required_secrets: list[SecretLookupEntry] = []
    for entry in meta_required_secrets_raw:
        if isinstance(entry, SecretLookupEntry):
            meta_required_secrets.append(entry)
        elif isinstance(entry, dict):
            meta_required_secrets.append(SecretLookupEntry.from_dict(entry))

    # Auto-populate from _setting_params / _secret_params class vars (set by __init_subclass__)
    annotation_setting_keys: set[str] = set()

    setting_params: dict[str, str] = getattr(cls, "_setting_params", {})
    secret_params: dict[str, Secret] = getattr(cls, "_secret_params", {})
    annotation_setting_keys.update(setting_params.values())

    # Union with Meta-declared keys, deduped, preserving order
    existing_settings = set(meta_required_settings)
    for key in sorted(annotation_setting_keys):
        if key not in existing_settings:
            meta_required_settings.append(key)

    # Add annotation-derived secret requirements
    existing_secret_types = {e.secret_type for e in meta_required_secrets}
    for secret in secret_params.values():
        if secret.secret_type not in existing_secret_types:
            meta_required_secrets.append(
                SecretLookupEntry(
                    secret_type=secret.secret_type,
                    secret_name=secret.name,
                    scope=secret.scope,
                )
            )
            existing_secret_types.add(secret.secret_type)

    return ResolvedMetadata(
        name=name,
        class_name=class_name,
        function_type=function_type,
        description=description,
        examples=examples,
        categories=attrs.get("categories", []),
        tags=dict(attrs.get("tags", {})),
        parameters=parameters,
        stability=attrs.get("stability", FunctionStability.CONSISTENT),
        null_handling=attrs.get("null_handling", NullHandling.DEFAULT),
        required_settings=meta_required_settings,
        required_secrets=meta_required_secrets,
        projection_pushdown=attrs.get("projection_pushdown", False),
        filter_pushdown=attrs.get("filter_pushdown", False),
        sampling_pushdown=attrs.get("sampling_pushdown", False),
        late_materialization=bool(attrs.get("late_materialization", False)),
        supported_expression_filters=attrs.get("supported_expression_filters", []),
        preserves_order=attrs.get("preserves_order", OrderPreservation.PRESERVES_ORDER),
        max_workers=attrs.get("max_workers"),
        supports_batch_index=bool(attrs.get("supports_batch_index", False)),
        partition_kind=_validate_partition_kind(cls, attrs.get("partition_kind", PartitionKind.NOT_PARTITIONED)),
        order_dependent=attrs.get("order_dependent", OrderDependence.NOT_ORDER_DEPENDENT),
        distinct_dependent=attrs.get("distinct_dependent", DistinctDependence.NOT_DISTINCT_DEPENDENT),
        supports_window=bool(attrs.get("supports_window", False)),
        streaming_partitioned=bool(attrs.get("streaming_partitioned", False)),
        # TABLE_BUFFERING implies has_finalize — the buffered path always
        # invokes the worker's finalize phase (it's the whole point).
        has_finalize=(_detect_has_finalize(cls, function_type) or is_buffering),
        source_order_dependent=bool(attrs.get("source_order_dependent", False)),
        sink_order_dependent=bool(attrs.get("sink_order_dependent", False)),
        requires_input_batch_index=bool(attrs.get("requires_input_batch_index", False)),
    )


def _validate_partition_kind(cls: type, kind: PartitionKind) -> PartitionKind:
    """Cross-check ``Meta.partition_kind`` against the bind schema.

    When the class exposes a static ``FIXED_SCHEMA`` ``ClassVar``
    (the common pattern in test fixtures), we can verify at
    registration time that:

    * ``kind != NOT_PARTITIONED`` ⇒ at least one field carries the
      ``vgi.partition_column`` metadata key (via
      :func:`vgi.schema_utils.partition_field`).
    * The reverse: any field annotated as a partition column ⇒
      ``kind != NOT_PARTITIONED``.

    For functions that compute their bind schema dynamically (no
    ``FIXED_SCHEMA`` available at class-resolution time), the check
    is deferred to the framework's bind path — the C++ extension's
    bind-time walk also raises ``BinderException`` on mismatch.

    Returns the validated kind unchanged.
    """
    # Static-schema fast path. ``FIXED_SCHEMA`` is the established
    # pattern for fixed-output table functions (see e.g.
    # ``PartitionedBatchIndexFunction.FIXED_SCHEMA``).
    fixed_schema = getattr(cls, "FIXED_SCHEMA", None)
    if not isinstance(fixed_schema, pa.Schema):
        # Dynamic schema or not a table function — defer to bind-time
        # validation in the C++ extension.
        return kind

    from vgi.schema_utils import VGI_PARTITION_COLUMN_KEY

    annotated_fields: list[str] = []
    for fld in fixed_schema:
        md = fld.metadata
        if md is not None and md.get(VGI_PARTITION_COLUMN_KEY) == b"true":
            annotated_fields.append(fld.name)

    if kind == PartitionKind.NOT_PARTITIONED and annotated_fields:
        raise ValueError(
            f"{cls.__name__}: bind schema has partition-annotated field(s) "
            f"{annotated_fields!r} but Meta.partition_kind is NOT_PARTITIONED. "
            f"Set Meta.partition_kind to a non-default PartitionKind, or "
            f"remove the partition_field() annotations."
        )
    if kind != PartitionKind.NOT_PARTITIONED and not annotated_fields:
        raise ValueError(
            f"{cls.__name__}: Meta.partition_kind is {kind.name} but no bind "
            f"schema field is annotated with vgi.partition_column. Use "
            f"vgi.schema_utils.partition_field(name, type) to mark the "
            f"column(s) that satisfy the partition contract, or set "
            f"Meta.partition_kind back to NOT_PARTITIONED."
        )

    return kind


def _detect_has_finalize(cls: type, function_type: CatalogFunctionType) -> bool:
    """Route to the TableInOut base class's ``has_finalize_override`` hook.

    For non-TableInOut function types always returns ``False``. The actual
    detection logic lives on the base class so users can subclass and
    override the heuristic, and so the Meta-level ``has_finalize`` flag is
    handled in one place.
    """
    if function_type is CatalogFunctionType.TABLE_BUFFERING:
        # The Sink+Source path is, by construction, an exchange that emits
        # output exclusively in the Source phase — has_finalize is always True
        # and is not detected from the user's class.
        return True
    if function_type is not CatalogFunctionType.TABLE:
        return False
    # Lazy import to avoid a circular dependency.
    try:
        from vgi.table_in_out_function import TableInOutGenerator
    except ImportError:  # pragma: no cover
        return False
    if not issubclass(cls, TableInOutGenerator):
        return False
    return cls.has_finalize_override()


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

# Nested struct type for secret requirements
_SECRET_REQUIREMENT_STRUCT = pa.struct(
    [
        pa.field("secret_type", pa.string()),
        pa.field("secret_name", pa.string(), nullable=True),
        pa.field("scope", pa.string(), nullable=True),
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
        pa.field("is_varargs", pa.bool_()),
        pa.field("is_const", pa.bool_()),
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
        pa.field("tags", pa.map_(pa.string(), pa.string())),
        pa.field("parameters", pa.list_(_PARAMETER_STRUCT)),
        pa.field("stability", pa.string()),
        pa.field("null_handling", pa.string()),
        pa.field("required_settings", pa.list_(pa.string())),
        pa.field("required_secrets", pa.list_(_SECRET_REQUIREMENT_STRUCT)),
        pa.field("projection_pushdown", pa.bool_()),
        pa.field("filter_pushdown", pa.bool_()),
        pa.field("sampling_pushdown", pa.bool_()),
        pa.field("late_materialization", pa.bool_()),
        pa.field("supported_expression_filters", pa.list_(pa.string())),
        pa.field("preserves_order", pa.string()),
        pa.field("max_workers", pa.int32(), nullable=True),
        pa.field("supports_batch_index", pa.bool_()),
        pa.field("partition_kind", pa.string()),
        pa.field("order_dependent", pa.string()),
        pa.field("distinct_dependent", pa.string()),
        pa.field("supports_window", pa.bool_()),
        pa.field("streaming_partitioned", pa.bool_()),
        pa.field("has_finalize", pa.bool_()),
        pa.field("source_order_dependent", pa.bool_()),
        pa.field("sink_order_dependent", pa.bool_()),
        pa.field("requires_input_batch_index", pa.bool_()),
    ]
)

# Fields that contain lists and need None -> [] conversion during deserialization
_LIST_FIELDS: frozenset[str] = frozenset(
    {"examples", "categories", "parameters", "required_settings", "required_secrets", "supported_expression_filters"}
)

# Fields that contain maps and need None -> {} conversion during deserialization
_MAP_FIELDS: frozenset[str] = frozenset({"tags"})


def _extract_arrow_row(columns: dict[str, list[Any]], index: int) -> dict[str, Any]:
    """Extract a single row from Arrow columnar data as a dict.

    Handles None values for list fields (converts None to [])
    and map fields (converts None to {}).
    """
    result: dict[str, Any] = {}
    for field_name, values in columns.items():
        value = values[index]
        if value is None:
            if field_name in _LIST_FIELDS:
                result[field_name] = []
            elif field_name in _MAP_FIELDS:
                result[field_name] = {}
            else:
                result[field_name] = value
        else:
            result[field_name] = value
    return result


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
        return pa.RecordBatch.from_pydict({field.name: [] for field in _METADATA_SCHEMA}, schema=_METADATA_SCHEMA)

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
    return [ResolvedMetadata.from_dict(_extract_arrow_row(columns, i)) for i in range(batch.num_rows)]


# =============================================================================
# Mixin for Function Classes
# =============================================================================


class MetadataMixin:
    """Mixin that provides metadata access for function classes.

    Add this to the base Function class to enable metadata resolution.
    """

    @classmethod
    def get_metadata(cls) -> ResolvedMetadata:
        """Get the resolved metadata for this function class."""
        return resolve_metadata(cls)  # type: ignore[arg-type]

    @classmethod
    def describe(cls) -> dict[str, Any]:
        """Get metadata as a dictionary (for JSON serialization)."""
        return cls.get_metadata().to_dict()
