# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Shared base classes for declarative descriptor/spec pairs.

[`Setting`][vgi.catalog.setting.Setting] (session-level, resent every call) and
[`AttachOption`][vgi.catalog.attach_option.AttachOption] (delivered once at
``catalog_attach``) are declared the same way — an `Annotated`-hint descriptor
plus a serializable spec with an identical Arrow IPC wire format. This module
holds the machinery both share; the two public modules subclass it so their
names, wire format, and behaviour are unchanged.
"""

from __future__ import annotations

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
    from collections.abc import Callable
    from typing import Self


# Python type to Arrow type mapping shared by every declarative descriptor.
_PYTHON_TO_ARROW: dict[type, pa.DataType] = {
    bool: pa.bool_(),
    int: pa.int64(),
    float: pa.float64(),
    str: pa.string(),
    bytes: pa.binary(),
}


def _resolve_arrow_type(type_hint: type | pa.DataType) -> pa.DataType:
    """Resolve Arrow type from either a Python type or Arrow DataType.

    Args:
        type_hint: A Python type (bool, int, float, str, bytes) or Arrow DataType.

    Returns:
        The resolved Arrow DataType.

    Raises:
        TypeError: If the type cannot be resolved.

    """
    if isinstance(type_hint, pa.DataType):
        return type_hint
    if type_hint in _PYTHON_TO_ARROW:
        return _PYTHON_TO_ARROW[type_hint]
    raise TypeError(
        f"Cannot resolve Arrow type from: {type_hint}. "
        "Use a Python type (bool, int, float, str, bytes) or Arrow DataType."
    )


@dataclass(frozen=True)
class _SpecBase:
    """Resolved descriptor metadata for catalog serialization.

    The resolved form of a declarative descriptor, with all types inferred and
    ready for serialization over the wire.

    Attributes:
        name: The attribute name (from the class attribute name).
        desc: Human-readable description.
        type: The Arrow data type for this entry.
        default: The default value (Python object) or ``None`` if unset.
        ARROW_SCHEMA: Arrow IPC schema used to (de)serialize this spec over the wire.

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
        type_schema = schema(value=self.type)
        type_bytes = type_schema.serialize().to_pybytes()

        # Serialize default value if present
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
    def deserialize(cls, batch: pa.RecordBatch) -> Self:
        """Deserialize from Arrow RecordBatch."""
        from vgi_rpc.utils import _validate_single_row_batch

        row = _validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=["name", "description", "type"],
        )
        # Deserialize type from schema bytes
        type_schema = pa.ipc.read_schema(pa.py_buffer(cast(bytes, row["type"])))
        data_type = type_schema.field("value").type

        # Deserialize default value if present
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


@dataclass
class _DescriptorBase:
    """Base for declarative descriptors defined via `Annotated` hints.

    The Arrow type is resolved from the base type in the `Annotated` hint, or
    overridden by an explicit ``arrow_type``.

    Attributes:
        desc: Human-readable description.
        arrow_type: Optional explicit Arrow type (overrides inference).

    """

    desc: str = ""
    arrow_type: pa.DataType | None = None

    # Internal field set during class creation
    _name: str = field(default="", init=False, repr=False)

    def __set_name__(self, owner: type, name: str) -> None:
        """Store the attribute name when assigned to a class."""
        self._name = name

    def __get__(self, obj: object | None, objtype: type | None = None) -> Any:
        """Return the descriptor on class access; the class-level default on instance access."""
        if obj is None:
            return self
        return getattr(type(obj), self._name, None)


def _extract_specs[D: _DescriptorBase, S: _SpecBase](
    declaring_cls: type,
    *,
    descriptor_type: type[D],
    spec_factory: Callable[..., S],
) -> list[S]:
    """Extract specs from a class whose attributes are ``descriptor_type`` instances.

    Parses ``Annotated[type, descriptor_type(...)]`` attributes and resolves each
    into a spec built by ``spec_factory``.

    Args:
        declaring_cls: The class declaring the descriptors (Settings / AttachOptions).
        descriptor_type: The descriptor class to match in the annotation metadata.
        spec_factory: Callable building a spec from ``name``/``desc``/``type``/``default``.

    Returns:
        List of specs extracted from the class.

    Raises:
        TypeError: If an entry's Arrow type cannot be resolved.

    """
    specs: list[S] = []

    # Get type hints with extras (preserves Annotated)
    try:
        hints = get_type_hints(declaring_cls, include_extras=True)
    except Exception:
        # If type hints can't be resolved, return empty list
        return specs

    for name, hint in hints.items():
        if get_origin(hint) is not Annotated:
            continue

        args = get_args(hint)
        if len(args) < 2:
            continue

        base_type = args[0]

        descriptor: D | None = None
        for arg in args[1:]:
            if isinstance(arg, descriptor_type):
                descriptor = arg
                break

        if descriptor is None:
            continue

        default = getattr(declaring_cls, name, None)
        arrow_type = descriptor.arrow_type if descriptor.arrow_type is not None else _resolve_arrow_type(base_type)

        specs.append(
            spec_factory(
                name=name,
                desc=descriptor.desc,
                type=arrow_type,
                default=default,
            )
        )

    return specs
