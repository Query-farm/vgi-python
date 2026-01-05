"""Arrow-based serialization of function argument specifications.

This module provides classes and functions for serializing function argument
specifications to Apache Arrow schemas. This enables functions to describe
their argument signatures (types, positions, special markers) in a format
that can be transmitted over IPC and understood by DuckDB for function
registration.

The serialization uses a single Arrow schema where:
- Positional arguments come first (field order = position index)
- Named arguments follow (marked with metadata)
- Special types (TableInput, AnyArrow, varargs) use field metadata markers

Example:
    # Define argument specs
    specs = [
        ArgumentSpec(name="count", position=0, arrow_type=pa.int64()),
        ArgumentSpec(
            name="data", position=1, arrow_type=pa.null(), is_table_input=True
        ),
        ArgumentSpec(name="format", position="format", arrow_type=pa.utf8()),
    ]

    # Serialize to Arrow schema
    schema = argument_specs_to_schema(specs)

    # Serialize schema to bytes for IPC
    schema_bytes = schema.serialize().to_pybytes()

    # Deserialize
    schema = pa.ipc.read_schema(pa.py_buffer(schema_bytes))
    specs = schema_to_argument_specs(schema)

"""

import warnings
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, get_type_hints

import pyarrow as pa

from vgi.arguments import AnyArrow, Arg, TableInput

__all__ = [
    "ArgumentSpec",
    "argument_specs_to_schema",
    "extract_argument_specs",
    "schema_to_argument_specs",
    # Metadata constants for parsing schemas
    "VGI_ARG_KEY",
    "VGI_ARG_NAMED",
    "VGI_TYPE_KEY",
    "VGI_TYPE_TABLE",
    "VGI_TYPE_ANY",
    "VGI_VARARGS_KEY",
    "VGI_VARARGS_TRUE",
]

# =============================================================================
# Metadata Keys
# =============================================================================

# Key indicating a named argument (not positional)
VGI_ARG_KEY = b"vgi_arg"
VGI_ARG_NAMED = b"named"

# Key indicating special argument types
VGI_TYPE_KEY = b"vgi_type"
VGI_TYPE_TABLE = b"table"
VGI_TYPE_ANY = b"any"

# Key indicating varargs (collects remaining positional arguments)
VGI_VARARGS_KEY = b"vgi_varargs"
VGI_VARARGS_TRUE = b"true"


def _argument_spec_sort_key(spec: "ArgumentSpec") -> tuple[int, int | str]:
    """Sort key: positional first (by index), then named (alphabetically)."""
    if isinstance(spec.position, int):
        return (0, spec.position)
    return (1, spec.position)


# =============================================================================
# ArgumentSpec Dataclass
# =============================================================================


@dataclass(frozen=True, slots=True)
class ArgumentSpec:
    """Specification for a single function argument.

    This represents one argument in a function's signature, capturing:
    - The argument's name and position (positional index or named key)
    - The exact Arrow data type
    - Special markers for table input, any-type, and varargs

    Attributes:
        name: Python attribute name for the argument.
        position: Positional index (int) for positional args, or the named key
            (str) for named arguments.
        arrow_type: The Arrow data type. Use pa.null() for TableInput and
            AnyArrow types.
        is_table_input: True if this argument receives streaming table input
            (Arg[TableInput]).
        is_any_type: True if this argument accepts any Arrow type
            (Arg[AnyArrow]).
        is_varargs: True if this argument collects all remaining positional
            arguments (varargs=True).

    Note:
        For named arguments, the Python attribute name (``name``) and the SQL
        key (``position``) are assumed to be identical. This is the standard
        convention::

            format = Arg[str]("format")  # name="format", position="format"

        If they differ, the ``position`` value will be lost during schema
        round-trip serialization, as only ``name`` is stored in the Arrow
        schema field name.

    """

    name: str
    position: int | str
    arrow_type: pa.DataType
    is_table_input: bool = False
    is_any_type: bool = False
    is_varargs: bool = False


# =============================================================================
# Serialization Functions
# =============================================================================


def argument_specs_to_schema(specs: Sequence[ArgumentSpec]) -> pa.Schema:
    """Convert ArgumentSpecs to a single Arrow schema.

    The schema encodes the argument specifications as follows:
    - Positional arguments come first, in order (field index = position index)
    - Named arguments follow, each with metadata {b"vgi_arg": b"named"}
    - Special types are indicated via metadata:
        - TableInput: {b"vgi_type": b"table"}
        - AnyArrow: {b"vgi_type": b"any"}
        - varargs: {b"vgi_varargs": b"true"}

    Args:
        specs: Sequence of ArgumentSpec objects to serialize.

    Returns:
        Arrow schema with one field per argument.

    Example:
        specs = [
            ArgumentSpec(name="count", position=0, arrow_type=pa.int64()),
            ArgumentSpec(name="format", position="format", arrow_type=pa.utf8()),
        ]
        schema = argument_specs_to_schema(specs)
        # schema has fields: count (int64), format (utf8 with vgi_arg=named)

    """
    sorted_specs = sorted(specs, key=_argument_spec_sort_key)

    # Validate contiguous positional indices
    positional_indices = [
        spec.position for spec in sorted_specs if isinstance(spec.position, int)
    ]
    if positional_indices:
        expected = list(range(len(positional_indices)))
        if positional_indices != expected:
            warnings.warn(
                f"Positional argument indices are not contiguous starting from 0. "
                f"Found: {positional_indices}, expected: {expected}. "
                f"This may indicate a bug.",
                stacklevel=2,
            )

    fields: list[pa.Field[Any]] = []
    for spec in sorted_specs:
        # Build metadata dict
        metadata: dict[bytes, bytes] = {}

        if isinstance(spec.position, str):
            metadata[VGI_ARG_KEY] = VGI_ARG_NAMED

        if spec.is_table_input:
            metadata[VGI_TYPE_KEY] = VGI_TYPE_TABLE
        elif spec.is_any_type:
            metadata[VGI_TYPE_KEY] = VGI_TYPE_ANY

        if spec.is_varargs:
            metadata[VGI_VARARGS_KEY] = VGI_VARARGS_TRUE

        # Create field with or without metadata
        field = pa.field(
            spec.name,
            spec.arrow_type,
            metadata=metadata if metadata else None,
        )
        fields.append(field)

    return pa.schema(fields)


def schema_to_argument_specs(schema: pa.Schema) -> list[ArgumentSpec]:
    """Convert Arrow schema back to ArgumentSpecs.

    Parses the schema fields and their metadata to reconstruct the original
    ArgumentSpec objects.

    Args:
        schema: Arrow schema with argument fields.

    Returns:
        List of ArgumentSpec objects in schema field order.

    Example:
        schema = pa.schema([
            pa.field("count", pa.int64()),
            pa.field("format", pa.utf8(), metadata={b"vgi_arg": b"named"}),
        ])
        specs = schema_to_argument_specs(schema)
        # specs[0].position == 0, specs[1].position == "format"

    """
    specs: list[ArgumentSpec] = []
    position_index = 0

    for field in schema:
        metadata = field.metadata or {}

        # Determine position
        is_named = metadata.get(VGI_ARG_KEY) == VGI_ARG_NAMED
        if is_named:
            position: int | str = field.name
        else:
            position = position_index
            position_index += 1

        # Check special type markers
        vgi_type = metadata.get(VGI_TYPE_KEY)
        is_table_input = vgi_type == VGI_TYPE_TABLE
        is_any_type = vgi_type == VGI_TYPE_ANY

        # Check varargs
        is_varargs = metadata.get(VGI_VARARGS_KEY) == VGI_VARARGS_TRUE

        specs.append(
            ArgumentSpec(
                name=field.name,
                position=position,
                arrow_type=field.type,
                is_table_input=is_table_input,
                is_any_type=is_any_type,
                is_varargs=is_varargs,
            )
        )

    return specs


# =============================================================================
# Extraction from Function Classes
# =============================================================================


def extract_argument_specs(
    cls: type,
    arg_types: Mapping[str, pa.DataType],
) -> list[ArgumentSpec]:
    """Extract ArgumentSpecs from a function class with Arg descriptors.

    Walks the class hierarchy to find all Arg descriptors and creates
    ArgumentSpec objects with the provided Arrow types.

    Args:
        cls: Function class with Arg descriptors.
        arg_types: Mapping from argument attribute names to their Arrow types.
            For TableInput and AnyArrow arguments, use pa.null().

    Returns:
        List of ArgumentSpec objects, sorted by position (positional first,
        then named).

    Example:
        class MyFunction(TableInOutFunction):
            count = Arg[int](0)
            format = Arg[str]("format")

        arg_types = {"count": pa.int64(), "format": pa.utf8()}
        specs = extract_argument_specs(MyFunction, arg_types)

    """
    specs: list[ArgumentSpec] = []
    seen_names: set[str] = set()

    # Get type hints for detecting TableInput/AnyArrow
    try:
        hints = get_type_hints(cls)
    except (NameError, AttributeError):
        hints = {}

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

                # Get Arrow type from provided mapping
                arrow_type: pa.DataType
                if attr_name not in arg_types:
                    warnings.warn(
                        f"Missing type for argument '{attr_name}' in arg_types "
                        f"mapping; defaulting to pa.null(). This may indicate a bug.",
                        stacklevel=2,
                    )
                    arrow_type = pa.null()
                else:
                    arrow_type = arg_types[attr_name]

                # Check type hint for special types
                hint = hints.get(attr_name)
                is_table_input = hint is TableInput
                is_any_type = hint is AnyArrow

                # Check varargs flag
                is_varargs = arg.varargs

                specs.append(
                    ArgumentSpec(
                        name=attr_name,
                        position=arg.position,
                        arrow_type=arrow_type,
                        is_table_input=is_table_input,
                        is_any_type=is_any_type,
                        is_varargs=is_varargs,
                    )
                )

    return sorted(specs, key=_argument_spec_sort_key)
