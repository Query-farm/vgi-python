# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Arrow-based serialization of function argument specifications.

This module provides classes and functions for serializing function argument
specifications to Apache Arrow schemas. This enables functions to describe
their argument signatures (types, positions, special markers) in a format
that can be transmitted over IPC and understood by DuckDB for function
registration.

The serialization uses a single Arrow schema where:
- Positional arguments come first (field order = position index)
- Named arguments follow (marked with metadata)
- Special types ([`TableInput`][], [`AnyArrow`][], varargs) use field metadata markers
"""

import warnings
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Annotated, Any, get_args, get_origin, get_type_hints

import pyarrow as pa

from vgi.arguments import PYTHON_TO_ARROW, AnyArrow, AnyArrowValue, Arg, TableInput

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
    "VGI_CONST_KEY",
    "VGI_CONST_TRUE",
    "VGI_DOC_KEY",
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

# Key indicating constant-folded argument (scalar value, not array)
VGI_CONST_KEY = b"vgi_const"
VGI_CONST_TRUE = b"true"

# Key carrying the per-argument description (UTF-8 text). Presence-only: the key
# is omitted entirely when there is no doc (absent = undocumented). The
# ``vgi_doc_*`` prefix is reserved for future per-argument doc variants.
VGI_DOC_KEY = b"vgi_doc"


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
        arrow_type: The Arrow data type. Use pa.null() for [`TableInput`][] and
            [`AnyArrow`][] types.
        is_table_input: True if this argument receives streaming table input
            (`Arg[TableInput]`).
        is_any_type: True if this argument accepts any Arrow type
            (`Arg[AnyArrow]`).
        is_varargs: True if this argument collects all remaining positional
            arguments (varargs=True).
        is_const: True if this argument is constant-folded ([`ConstParam`][]).
            Constant arguments are scalar values known at planning time,
            rather than columnar data processed at runtime.
        doc: Optional human/agent-facing description of the argument. Surfaced
            through the catalog as the ``vgi_doc`` Arrow field metadata key
            (UTF-8); empty string means undocumented.

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
    is_const: bool = False
    doc: str = ""

    def __repr__(self) -> str:
        """Return concise repr showing key attributes."""
        # Build position display: integer or quoted string
        pos = self.position if isinstance(self.position, int) else f'"{self.position}"'

        # Build flags list (only show if True)
        flags = []
        if self.is_table_input:
            flags.append("table_input")
        if self.is_any_type:
            flags.append("any_type")
        if self.is_varargs:
            flags.append("varargs")
        if self.is_const:
            flags.append("const")

        flags_str = f", flags=[{', '.join(flags)}]" if flags else ""

        return f'ArgumentSpec(name="{self.name}", pos={pos}, type={self.arrow_type}{flags_str})'


# =============================================================================
# Serialization Functions
# =============================================================================


def argument_specs_to_schema(specs: Sequence[ArgumentSpec]) -> pa.Schema:
    """Convert [`ArgumentSpec`][]s to a single Arrow schema.

    The schema encodes the argument specifications as follows:
    - Positional arguments come first, in order (field index = position index)
    - Named arguments follow, each with metadata {b"vgi_arg": b"named"}
    - Special types are indicated via metadata:
        - [`TableInput`][]: {b"vgi_type": b"table"}
        - [`AnyArrow`][]: {b"vgi_type": b"any"}
        - varargs: {b"vgi_varargs": b"true"}

    Args:
        specs: Sequence of `ArgumentSpec` objects to serialize.

    Returns:
        Arrow schema with one field per argument.

    """
    sorted_specs = sorted(specs, key=_argument_spec_sort_key)

    # Validate contiguous positional indices
    positional_indices = [spec.position for spec in sorted_specs if isinstance(spec.position, int)]
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

        if spec.is_const:
            metadata[VGI_CONST_KEY] = VGI_CONST_TRUE

        # Per-argument description (UTF-8; presence-only — omit when empty)
        if spec.doc:
            metadata[VGI_DOC_KEY] = spec.doc.encode("utf-8")

        # Create field with or without metadata
        field = pa.field(
            spec.name,
            spec.arrow_type,
            metadata=metadata if metadata else None,
        )
        fields.append(field)

    return pa.schema(fields)


def schema_to_argument_specs(schema: pa.Schema) -> list[ArgumentSpec]:
    """Convert Arrow schema back to [`ArgumentSpec`][]s.

    Parses the schema fields and their metadata to reconstruct the original
    `ArgumentSpec` objects.

    Args:
        schema: Arrow schema with argument fields.

    Returns:
        List of `ArgumentSpec` objects in schema field order.

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

        # Check const
        is_const = metadata.get(VGI_CONST_KEY) == VGI_CONST_TRUE

        # Per-argument description (UTF-8; absent = undocumented)
        doc_bytes = metadata.get(VGI_DOC_KEY)
        doc = doc_bytes.decode("utf-8") if doc_bytes else ""

        specs.append(
            ArgumentSpec(
                name=field.name,
                position=position,
                arrow_type=field.type,
                is_table_input=is_table_input,
                is_any_type=is_any_type,
                is_varargs=is_varargs,
                is_const=is_const,
                doc=doc,
            )
        )

    return specs


# =============================================================================
# Extraction from Function Classes
# =============================================================================


def extract_argument_specs(
    cls: type,
) -> list[ArgumentSpec]:
    """Extract [`ArgumentSpec`][]s from a function class with [`Arg`][] descriptors.

    Walks the class hierarchy to find all `Arg` descriptors and creates
    `ArgumentSpec` objects with Arrow types determined by:
    1. Explicit arrow_type on `Arg` (highest priority)
    2. Type annotation with `PYTHON_TO_ARROW` mapping
    3. Default to pa.null() with warning for unknown types

    Args:
        cls: Function class with `Arg` descriptors.

    Returns:
        List of `ArgumentSpec` objects, sorted by position (positional first,
        then named).

    """
    specs: list[ArgumentSpec] = []
    seen_names: set[str] = set()

    # Get type hints for type inference and detecting TableInput/AnyArrow
    try:
        hints = get_type_hints(cls)
    except (NameError, AttributeError):
        hints = {}

    # Check for new Param/ConstParam API (ScalarFunction subclasses)
    # These are stored in _compute_params and _const_params class attributes
    compute_params: dict[str, Arg[Any]] = getattr(cls, "_compute_params", {})
    const_params: dict[str, Arg[Any]] = getattr(cls, "_const_params", {})

    for param_name, param_arg in compute_params.items():
        seen_names.add(param_name)
        # Use arrow_type from Param() which stores the pa.DataType
        param_arrow_type = param_arg.arrow_type if param_arg.arrow_type is not None else pa.null()
        specs.append(
            ArgumentSpec(
                name=param_name,
                position=param_arg.position,
                arrow_type=param_arrow_type,
                is_table_input=False,
                is_any_type=param_arg.is_any,
                is_varargs=param_arg.varargs,
                is_const=False,
                doc=param_arg.doc or "",
            )
        )

    for const_name, const_arg in const_params.items():
        seen_names.add(const_name)
        # ConstParam stores arrow_type from the Python type mapping
        const_arrow_type = const_arg.arrow_type if const_arg.arrow_type is not None else pa.null()
        specs.append(
            ArgumentSpec(
                name=const_name,
                position=const_arg.position,
                arrow_type=const_arrow_type,
                is_table_input=False,
                is_any_type=const_arg.is_any,
                is_varargs=const_arg.varargs,
                is_const=True,
                doc=const_arg.doc or "",
            )
        )

    # Check for FunctionArguments dataclass (typed generic pattern)
    # e.g., class MyFunc(TableFunctionGenerator[MyArgs]):
    #   where MyArgs has fields like: count: Annotated[int, Arg(0, doc="...")]
    args_class = getattr(cls, "FunctionArguments", None)
    if args_class is not None:
        try:
            args_hints = get_type_hints(args_class, include_extras=True)
        except (NameError, AttributeError):
            args_hints = {}

        for field_name, field_hint in args_hints.items():
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

            # For varargs, unwrap list[T] to get the element type T for inference
            infer_type = base_type
            if arg_instance.varargs and get_origin(base_type) is list:
                type_args = get_args(base_type)
                if type_args:
                    infer_type = type_args[0]

            is_table_input = infer_type is TableInput
            is_any_type = infer_type is AnyArrow or infer_type is AnyArrowValue

            # Determine Arrow type
            arrow_type: pa.DataType
            if arg_instance.arrow_type is not None:
                arrow_type = arg_instance.arrow_type
            elif is_table_input or is_any_type:
                arrow_type = pa.null()
            elif infer_type in PYTHON_TO_ARROW:
                arrow_type = PYTHON_TO_ARROW[infer_type]
            else:
                arrow_type = pa.null()

            specs.append(
                ArgumentSpec(
                    name=field_name,
                    position=arg_instance.position,
                    arrow_type=arrow_type,
                    is_table_input=is_table_input,
                    is_any_type=is_any_type,
                    is_varargs=arg_instance.varargs,
                    is_const=getattr(arg_instance, "const", False),
                    doc=getattr(arg_instance, "doc", "") or "",
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
                arg_legacy: Arg[Any] = attr_value

                # Check for special types (AnyArrow, TableInput)
                # Priority: Arg subscript type (Arg[AnyArrow]) > class type hint
                # Also check _returns_any_arrow_value for Annotated[AnyArrowValue, ...]
                hint = hints.get(attr_name)
                type_param = getattr(arg_legacy, "_type_param", None)
                is_table_input = type_param is TableInput or hint is TableInput
                is_any_type = (
                    type_param is AnyArrow
                    or hint is AnyArrow
                    or hint is AnyArrowValue
                    or getattr(arg_legacy, "_returns_any_arrow_value", False)
                )

                # Determine Arrow type using priority order:
                # 1. Explicit arrow_type on Arg
                # 2. Type parameter from Arg[type] subscript (e.g., Arg[str])
                # 3. Type hint with PYTHON_TO_ARROW mapping
                # 4. Default to pa.null() with warning
                legacy_arrow_type: pa.DataType
                if arg_legacy.arrow_type is not None:
                    legacy_arrow_type = arg_legacy.arrow_type
                elif is_table_input or is_any_type:
                    legacy_arrow_type = pa.null()
                elif (
                    hasattr(arg_legacy, "_type_param")
                    and arg_legacy._type_param is not None
                    and arg_legacy._type_param in PYTHON_TO_ARROW
                ):
                    # Use type from Arg[type] subscript
                    legacy_arrow_type = PYTHON_TO_ARROW[arg_legacy._type_param]
                elif hint is not None and hint in PYTHON_TO_ARROW:
                    legacy_arrow_type = PYTHON_TO_ARROW[hint]
                else:
                    warnings.warn(
                        f"Cannot determine Arrow type for argument '{attr_name}'. "
                        f"Add explicit arrow_type to Arg or add type annotation. "
                        f"Defaulting to pa.null().",
                        stacklevel=2,
                    )
                    legacy_arrow_type = pa.null()

                # Check varargs flag
                is_varargs = arg_legacy.varargs

                # Check const flag
                is_const = getattr(arg_legacy, "const", False)

                specs.append(
                    ArgumentSpec(
                        name=attr_name,
                        position=arg_legacy.position,
                        arrow_type=legacy_arrow_type,
                        is_table_input=is_table_input,
                        is_any_type=is_any_type,
                        is_varargs=is_varargs,
                        is_const=is_const,
                        doc=getattr(arg_legacy, "doc", "") or "",
                    )
                )

    return sorted(specs, key=_argument_spec_sort_key)
