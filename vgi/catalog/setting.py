"""Setting descriptor for declarative worker settings.

This module provides the Setting descriptor class for defining worker settings
using Python's Annotated type hints, similar to how Arg works for function arguments.

Example:
    from typing import Annotated
    import pyarrow as pa
    from vgi import Worker
    from vgi.catalog import Setting

    class MyWorker(Worker):
        class Settings:
            # Simple Python types - Arrow type inferred
            verbose_mode: Annotated[bool, Setting(desc="Enable verbose output")] = False
            batch_size: Annotated[int, Setting(desc="Batch size")] = 1000

            # Complex Arrow types - specify directly in annotation
            allowed_ids: Annotated[pa.list_(pa.int64()), Setting(desc="IDs")] = []

"""

from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    get_args,
    get_origin,
    get_type_hints,
)

import pyarrow as pa

import vgi.ipc_utils

if TYPE_CHECKING:
    from typing import Self

__all__ = [
    "Setting",
    "SettingSpec",
    "extract_setting_specs",
]


@dataclass(frozen=True)
class SettingSpec:
    """Extracted setting metadata for catalog serialization.

    This is the resolved form of a Setting, with all types inferred and
    ready for serialization.

    Attributes:
        name: The setting name (from the class attribute name).
        desc: Human-readable description.
        type: The Arrow data type for this setting.
        default: The default value (Python object).

    """

    name: str
    desc: str
    type: pa.DataType
    default: Any

    ARROW_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("name", pa.string(), nullable=False),
            pa.field("description", pa.string(), nullable=False),
            pa.field("type", pa.binary(), nullable=False),
            pa.field("default_value", pa.binary(), nullable=True),
        ]  # type: ignore[arg-type]
    )

    def serialize(self) -> bytes:
        """Serialize to Arrow IPC bytes."""
        # Serialize type as a single-field schema
        type_schema = pa.schema([pa.field("value", self.type)])
        type_bytes = type_schema.serialize().to_pybytes()

        # Serialize default value if present
        default_bytes: bytes | None = None
        if self.default is not None:
            default_batch = pa.RecordBatch.from_pydict(
                {"value": [self.default]}, schema=type_schema
            )
            default_bytes = vgi.ipc_utils.serialize_record_batch(default_batch)

        batch = pa.RecordBatch.from_pylist(
            [
                {
                    "name": self.name,
                    "description": self.desc,
                    "type": type_bytes,
                    "default_value": default_bytes,
                }
            ],
            schema=self.ARROW_SCHEMA,
        )
        return vgi.ipc_utils.serialize_record_batch(batch)

    @classmethod
    def deserialize(cls, batch: pa.RecordBatch) -> "Self":
        """Deserialize from Arrow RecordBatch."""
        row = vgi.ipc_utils.validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=["name", "description", "type"],
        )
        # Deserialize type from schema bytes
        type_schema = pa.ipc.read_schema(pa.py_buffer(row["type"]))
        data_type = type_schema.field("value").type

        # Deserialize default value if present
        default: Any = None
        if row["default_value"] is not None:
            default_batch = vgi.ipc_utils.deserialize_record_batch(row["default_value"])
            default = default_batch.column("value")[0].as_py()

        return cls(
            name=row["name"],
            desc=row["description"],
            type=data_type,
            default=default,
        )


# Python type to Arrow type mapping
_PYTHON_TO_ARROW: dict[type, pa.DataType] = {
    bool: pa.bool_(),
    int: pa.int64(),
    float: pa.float64(),
    str: pa.string(),
    bytes: pa.binary(),
}


def _resolve_arrow_type(type_hint: Any) -> pa.DataType:
    """Resolve Arrow type from either a Python type or Arrow DataType.

    Args:
        type_hint: A Python type (bool, int, float, str, bytes) or Arrow DataType.

    Returns:
        The resolved Arrow DataType.

    Raises:
        TypeError: If the type cannot be resolved.

    """
    # If already an Arrow DataType, use it directly
    if isinstance(type_hint, pa.DataType):
        return type_hint

    # Map Python types to Arrow types
    if type_hint in _PYTHON_TO_ARROW:
        return _PYTHON_TO_ARROW[type_hint]

    raise TypeError(
        f"Cannot resolve Arrow type from: {type_hint}. "
        "Use a Python type (bool, int, float, str, bytes) or Arrow DataType."
    )


@dataclass
class Setting:
    """Descriptor for declarative setting definitions using Annotated.

    Use with Annotated type hints to declare settings in a Worker's Settings class.
    The Arrow type is resolved from the base type in the Annotated hint.

    Attributes:
        desc: Human-readable description of the setting.
        arrow_type: Optional explicit Arrow type (overrides inference from annotation).

    Examples:
        class MyWorker(Worker):
            class Settings:
                # Type inferred from Python type annotation
                verbose: Annotated[bool, Setting(desc="Enable verbose")] = False
                count: Annotated[int, Setting(desc="Count")] = 100

                # Complex Arrow type specified directly
                ids: Annotated[pa.list_(pa.int64()), Setting(desc="IDs")] = []

    """

    desc: str = ""
    arrow_type: pa.DataType | None = None

    # Internal fields set during class creation
    _name: str = field(default="", init=False, repr=False)

    def __set_name__(self, owner: type, name: str) -> None:
        """Store the attribute name when assigned to a class."""
        self._name = name

    def __get__(self, obj: object | None, objtype: type | None = None) -> Any:
        """Get the setting value.

        When accessed on the class, returns the descriptor itself.
        When accessed on an instance, returns the default value.
        """
        if obj is None:
            return self
        # Return the class-level default
        return getattr(type(obj), self._name, None)


def extract_setting_specs(settings_cls: type) -> list[SettingSpec]:
    """Extract SettingSpec objects from a Settings class.

    Parses a Settings class with Annotated type hints and extracts
    SettingSpec objects for each setting definition.

    Args:
        settings_cls: A class with Annotated[type, Setting(...)] attributes.

    Returns:
        List of SettingSpec objects extracted from the class.

    Raises:
        TypeError: If a setting's Arrow type cannot be resolved.

    Example:
        class Settings:
            verbose: Annotated[bool, Setting(desc="Verbose mode")] = False
            count: Annotated[int, Setting(desc="Count")] = 10

        specs = extract_setting_specs(Settings)
        # specs[0].name == "verbose", specs[0].type == pa.bool_()

    """
    specs: list[SettingSpec] = []

    # Get type hints with extras (preserves Annotated)
    try:
        hints = get_type_hints(settings_cls, include_extras=True)
    except Exception:
        # If type hints can't be resolved, return empty list
        return specs

    for name, hint in hints.items():
        # Skip non-Annotated hints
        if get_origin(hint) is not Annotated:
            continue

        args = get_args(hint)
        if len(args) < 2:
            continue

        base_type = args[0]

        # Find Setting in the annotation args
        setting = None
        for arg in args[1:]:
            if isinstance(arg, Setting):
                setting = arg
                break

        if setting is None:
            continue

        # Get default value from class attribute
        default = getattr(settings_cls, name, None)

        # Resolve Arrow type: explicit Setting.type takes precedence,
        # otherwise resolve from base_type (Python type or Arrow DataType)
        if setting.arrow_type is not None:
            arrow_type = setting.arrow_type
        else:
            arrow_type = _resolve_arrow_type(base_type)

        specs.append(
            SettingSpec(
                name=name,
                desc=setting.desc,
                type=arrow_type,
                default=default,
            )
        )

    return specs
