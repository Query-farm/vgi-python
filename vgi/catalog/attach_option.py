"""AttachOption descriptor for declarative worker attach-time options.

This module provides the AttachOption descriptor class for declaring options
that workers accept at ATTACH time (delivered once via catalog_attach, distinct
from session-level Settings resent on every call).

The declaration mirrors ``vgi.catalog.setting.Setting`` almost verbatim — same
Arrow IPC spec format, same Python type → Arrow mapping, same extractor shape.
"""

from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

import pyarrow as pa
from vgi_rpc.utils import deserialize_record_batch, serialize_record_batch_bytes

from vgi.schema_utils import schema

if TYPE_CHECKING:
    from typing import Self

__all__ = [
    "AttachOption",
    "AttachOptionSpec",
    "extract_attach_option_specs",
]


@dataclass(frozen=True)
class AttachOptionSpec:
    """Extracted attach-option metadata for catalog discovery serialization.

    Attributes:
        name: The option name (from the class attribute name).
        desc: Human-readable description.
        type: The Arrow data type for this option.
        default: The default value (Python object) or ``None`` if unset.

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
        type_schema = schema(value=self.type)
        type_bytes = type_schema.serialize().to_pybytes()

        default_bytes: bytes | None = None
        if self.default is not None:
            default_batch = pa.RecordBatch.from_pydict({"value": [self.default]}, schema=type_schema)
            default_bytes = serialize_record_batch_bytes(default_batch)

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
        return serialize_record_batch_bytes(batch)

    @classmethod
    def deserialize(cls, batch: pa.RecordBatch) -> "Self":
        """Deserialize from Arrow RecordBatch."""
        from vgi_rpc.utils import _validate_single_row_batch

        row = _validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=["name", "description", "type"],
        )
        type_schema = pa.ipc.read_schema(pa.py_buffer(cast(bytes, row["type"])))
        data_type = type_schema.field("value").type

        default: Any = None
        if row["default_value"] is not None:
            default_batch, _ = deserialize_record_batch(cast(bytes, row["default_value"]))
            default = default_batch.column("value")[0].as_py()

        return cls(
            name=cast(str, row["name"]),
            desc=cast(str, row["description"]),
            type=data_type,
            default=default,
        )


_PYTHON_TO_ARROW: dict[type, pa.DataType] = {
    bool: pa.bool_(),
    int: pa.int64(),
    float: pa.float64(),
    str: pa.string(),
    bytes: pa.binary(),
}


def _resolve_arrow_type(type_hint: type | pa.DataType) -> pa.DataType:
    """Resolve Arrow type from either a Python type or an Arrow DataType."""
    if isinstance(type_hint, pa.DataType):
        return type_hint
    if type_hint in _PYTHON_TO_ARROW:
        return _PYTHON_TO_ARROW[type_hint]
    raise TypeError(
        f"Cannot resolve Arrow type from: {type_hint}. "
        "Use a Python type (bool, int, float, str, bytes) or Arrow DataType."
    )


@dataclass
class AttachOption:
    """Descriptor for declarative attach-option definitions using Annotated.

    Use with Annotated type hints to declare options in a Worker's
    AttachOptions inner class. The Arrow type is resolved from the base type
    in the Annotated hint.

    Attributes:
        desc: Human-readable description of the option.
        arrow_type: Optional explicit Arrow type (overrides inference).

    """

    desc: str = ""
    arrow_type: pa.DataType | None = None

    _name: str = field(default="", init=False, repr=False)

    def __set_name__(self, owner: type, name: str) -> None:
        """Capture the attribute name when bound to a class."""
        self._name = name

    def __get__(self, obj: object | None, objtype: type | None = None) -> Any:
        """Return the descriptor itself on class access; the bound value on instance access."""
        if obj is None:
            return self
        return getattr(type(obj), self._name, None)


def extract_attach_option_specs(options_cls: type) -> list[AttachOptionSpec]:
    """Extract AttachOptionSpec objects from an AttachOptions class."""
    specs: list[AttachOptionSpec] = []

    try:
        hints = get_type_hints(options_cls, include_extras=True)
    except Exception:
        return specs

    for name, hint in hints.items():
        if get_origin(hint) is not Annotated:
            continue

        args = get_args(hint)
        if len(args) < 2:
            continue

        base_type = args[0]

        opt: AttachOption | None = None
        for arg in args[1:]:
            if isinstance(arg, AttachOption):
                opt = arg
                break

        if opt is None:
            continue

        default = getattr(options_cls, name, None)
        arrow_type = opt.arrow_type if opt.arrow_type is not None else _resolve_arrow_type(base_type)

        specs.append(
            AttachOptionSpec(
                name=name,
                desc=opt.desc,
                type=arrow_type,
                default=default,
            )
        )

    return specs
